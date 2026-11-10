"""Unit and integration test suite for Raw Generation, Bronze Ingestion, and Silver Core Cleaning."""

import json
import os
from pathlib import Path
from typing import Dict

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import NYC_BOUNDS, QUALITY_THRESHOLDS, get_env_paths
from src.bronze.ingest_bronze import METADATA_COLUMNS, run_bronze_ingestion
from src.silver.clean_quarantine import RULE_NAMES, run_silver_cleaning
from src.utils.data_generator import generate_raw_batches
from src.utils.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a shared local SparkSession for the test suite."""
    session = get_spark_session("DAT535_Test_Bronze_Silver_Core")
    yield session


@pytest.fixture()
def isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Dict[str, str]:
    """Configure an isolated temporary base directory for test execution."""
    base_dir = str(tmp_path / "spark-lab-data")
    monkeypatch.setenv("DAT535_DATA_DIR", base_dir)
    monkeypatch.setenv("DAT535_ENV", "test")
    paths = get_env_paths("test")
    return paths


def test_config_paths_and_thresholds(isolated_env: Dict[str, str]) -> None:
    """Verify environment path resolution and quality threshold constants."""
    expected_keys = {
        "raw_dir",
        "bronze_trips",
        "bronze_stations",
        "bronze_weather",
        "bronze_receipt",
        "silver_cleaned",
        "silver_quarantine",
        "silver_enriched",
        "silver_quality_report",
        "gold_rebalancing",
        "gold_weather_impact",
        "gold_ready_signal",
    }
    assert set(isolated_env.keys()) == expected_keys
    assert isolated_env["raw_dir"].endswith("/test/raw_landing")
    assert QUALITY_THRESHOLDS["max_classic_speed_kmh"] == 45.0
    assert QUALITY_THRESHOLDS["max_electric_speed_kmh"] == 55.0
    assert NYC_BOUNDS["lat_min"] == 40.45


def test_generate_raw_batches_deterministic(isolated_env: Dict[str, str]) -> None:
    """Verify deterministic raw batch generation for stations, weather, and trip files."""
    summary = generate_raw_batches(
        raw_dir=isolated_env["raw_dir"], num_trips=2000, seed=42
    )

    assert summary["stations_count"] == 120
    assert summary["total_trip_rows"] == 2000
    assert os.path.exists(summary["stations_file"])
    assert os.path.exists(summary["weather_file"])
    assert len(summary["trip_files"]) == 2
    for trip_file in summary["trip_files"]:
        assert os.path.exists(trip_file)

    with open(summary["stations_file"], "r", encoding="utf-8") as f_st:
        stations = json.load(f_st)
    assert len(stations) == 120
    first_station = stations[0]
    for col in (
        "station_id",
        "station_name",
        "capacity",
        "region_id",
        "station_lat",
        "station_lng",
        "is_active",
    ):
        assert col in first_station
    assert all(15 <= s["capacity"] <= 60 for s in stations)


def test_bronze_ingestion_and_silver_cleaning_end_to_end(
    spark: SparkSession, isolated_env: Dict[str, str]
) -> None:
    """Verify Bronze metadata & delivery receipt and Silver deduplication, imputation, and 7-rule quarantine."""
    run_id = "pytest_run_001"
    num_trips = 2500

    # 1. Generate raw batches
    gen_summary = generate_raw_batches(
        raw_dir=isolated_env["raw_dir"], num_trips=num_trips, seed=42
    )
    assert gen_summary["total_trip_rows"] == num_trips

    # 2. Run Bronze ingestion
    receipt = run_bronze_ingestion(spark=spark, env="test", run_id=run_id)

    assert os.path.exists(isolated_env["bronze_receipt"])
    with open(isolated_env["bronze_receipt"], "r", encoding="utf-8") as f_rcpt:
        disk_receipt = json.load(f_rcpt)

    assert disk_receipt["run_id"] == run_id
    assert disk_receipt["schema_validation_status"] == "PASSED"
    assert disk_receipt["expected_file_count"] == 4
    assert disk_receipt["received_file_count"] == 4
    assert disk_receipt["row_counts"]["trips"] == num_trips
    assert disk_receipt["row_counts"]["stations"] == 120
    assert disk_receipt["row_counts"]["weather"] > 0

    for table_key in ("bronze_trips", "bronze_stations", "bronze_weather"):
        df = spark.read.parquet(isolated_env[table_key])
        for meta_col in METADATA_COLUMNS:
            assert meta_col in df.columns
        sample_row = df.select("_run_id", "_schema_version").first()
        assert sample_row is not None
        assert sample_row["_run_id"] == run_id
        assert sample_row["_schema_version"] == "v1.0"

    # 3. Run Silver cleaning & quarantine
    metrics = run_silver_cleaning(spark=spark, env="test", run_id=run_id)

    assert metrics["bronze_input_rows"] == num_trips
    assert metrics["duplicates_removed"] == gen_summary["duplicate_trip_rows"]
    assert metrics["duplicates_removed"] > 0
    assert metrics["imputed_coords_count"] == gen_summary["imputed_coord_rows"]
    assert metrics["imputed_coords_count"] > 0

    # Conservation of rows: deduplicated == valid_cleaned + quarantined
    dedup_total = metrics["bronze_input_rows"] - metrics["duplicates_removed"]
    assert dedup_total == metrics["valid_cleaned_rows"] + metrics["quarantined_rows"]

    # Verify all 7 quarantine rules captured anomalies
    breakdown = metrics["quarantine_breakdown_by_rule"]
    for rule_name in RULE_NAMES:
        assert rule_name in breakdown
        assert breakdown[rule_name] > 0, f"Expected > 0 quarantined rows for {rule_name}"

    cleaned_df = spark.read.parquet(isolated_env["silver_cleaned"])
    quarantine_df = spark.read.parquet(isolated_env["silver_quarantine"])

    # Verify zero duplicate ride_ids in silver_cleaned
    distinct_rides = cleaned_df.select("ride_id").distinct().count()
    assert distinct_rides == metrics["valid_cleaned_rows"]

    # Verify coordinate imputation succeeded and saved imputed trips into silver_cleaned
    imputed_in_clean = cleaned_df.filter(
        F.col("dq_handling_flag") == "IMPUTED_END_COORDS"
    ).count()
    assert imputed_in_clean == gen_summary["imputed_coord_rows"]
    zero_coords_in_clean = cleaned_df.filter(
        (F.col("end_lat") == 0.0) | (F.col("end_lng") == 0.0)
    ).count()
    assert zero_coords_in_clean == 0

    # Verify Haversine speed filter enforced in silver_cleaned
    speed_violations_in_clean = cleaned_df.filter(
        ((F.col("rideable_type") == "classic_bike") & (F.col("speed_kmh") > 45.0))
        | ((F.col("rideable_type") == "electric_bike") & (F.col("speed_kmh") > 55.0))
    ).count()
    assert speed_violations_in_clean == 0

    # Verify quarantine table contains dq_issue and _rejection_reason (including multi-rule violations)
    assert "dq_issue" in quarantine_df.columns
    assert "_rejection_reason" in quarantine_df.columns
    multi_rule_count = quarantine_df.filter(
        F.col("_rejection_reason").contains(",")
    ).count()
    assert multi_rule_count > 0
