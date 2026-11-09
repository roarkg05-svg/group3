"""Unit and integration tests for the Gold marts and their orchestrator."""

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import get_env_paths
from src.bronze.ingest_bronze import run_bronze_ingestion
from src.gold import build_gold as build_gold_module
from src.gold.build_gold import run_gold_transformation
from src.gold.mart_station_flow import build_station_flow
from src.gold.mart_weather_ridership import join_trips_with_weather
from src.silver.clean_quarantine import run_silver_cleaning
from src.utils.data_generator import generate_raw_batches
from src.utils.spark_session import get_spark_session

ENV = "test"


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return get_spark_session("DAT535_Test_Gold")


@pytest.fixture(scope="module")
def pipeline(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Dict[str, Any]]:
    """Run raw generation through Gold once in an isolated data directory."""
    base_dir = str(tmp_path_factory.mktemp("gold") / "spark-lab-data")
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("DAT535_DATA_DIR", base_dir)
        paths = get_env_paths(ENV)
        generate_raw_batches(paths["raw_dir"], num_trips=2500, seed=42)
        run_bronze_ingestion(spark, env=ENV, run_id="pytest_gold")
        run_silver_cleaning(spark, env=ENV, run_id="pytest_gold")
        metrics = run_gold_transformation(spark, ENV, "pytest_gold")
        tables = build_gold_module._gold_table_paths(paths)
        yield {"base_dir": base_dir, "paths": paths, "tables": tables, "metrics": metrics}


def _trip(
    spark: SparkSession, start_station: str, end_station: str, started_at: datetime
) -> DataFrame:
    ended_at = started_at.replace(minute=30)
    return spark.createDataFrame(
        [(start_station, end_station, started_at, ended_at, 2.0, 12.0)],
        "start_station_id string, end_station_id string, started_at timestamp, "
        "ended_at timestamp, distance_km double, speed_kmh double",
    )


def _stations(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("A", "Station A", "R1", 20), ("B", "Station B", "R1", 40)],
        "station_id string, station_name string, region_id string, capacity long",
    )


def test_station_flow_conservation(spark: SparkSession, pipeline: Dict[str, Any]) -> None:
    flow = spark.read.parquet(pipeline["tables"]["station_flow"])
    silver_rows = spark.read.parquet(pipeline["paths"]["silver_cleaned"]).count()

    totals = flow.agg(
        F.sum("outbound_trips").alias("outbound"), F.sum("inbound_trips").alias("inbound")
    ).first()
    assert totals["outbound"] == totals["inbound"] == silver_rows
    assert flow.filter(F.col("net_flow") != F.col("inbound_trips") - F.col("outbound_trips")).count() == 0
    assert flow.filter(F.col("turnover_ratio") < 0.0).count() == 0
    assert flow.count() == flow.select("station_id", "trip_date").distinct().count()


def test_station_flow_keeps_stations_missing_from_catalog(
    spark: SparkSession, pipeline: Dict[str, Any]
) -> None:
    flow = spark.read.parquet(pipeline["tables"]["station_flow"])
    unknown = flow.filter(~F.col("is_catalog_station"))

    assert unknown.count() > 0
    assert unknown.filter(F.col("turnover_ratio").isNotNull()).count() == 0
    assert flow.filter(F.col("is_catalog_station") & F.col("turnover_ratio").isNull()).count() == 0


def test_peak_departure_hour_tie_keeps_one_row_per_station_day(spark: SparkSession) -> None:
    trips = _trip(spark, "A", "B", datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)).unionByName(
        _trip(spark, "A", "B", datetime(2026, 10, 1, 17, 0, tzinfo=timezone.utc))
    )

    rows = build_station_flow(trips, _stations(spark)).filter(F.col("station_id") == "A").collect()

    assert len(rows) == 1
    assert rows[0]["peak_departure_hour"] == 8
    assert rows[0]["outbound_trips"] == 2
    assert rows[0]["net_flow"] == -2
    assert rows[0]["turnover_ratio"] == pytest.approx(0.1)


def test_weather_join_integrity(spark: SparkSession) -> None:
    trips = _trip(spark, "A", "B", datetime(2026, 10, 1, 8, 15, tzinfo=timezone.utc))
    # The same hour reported twice must not double the trip, and 0.05 mm must get a bin.
    weather = spark.createDataFrame(
        [
            ("2026-10-01 08:00:00", "12.5", "0.05", "f1", datetime(2026, 11, 1)),
            ("2026-10-01 08:00:00", "12.5", "0.05", "f2", datetime(2026, 11, 2)),
            ("2026-10-01 09:00:00", "not-a-number", "", "f1", datetime(2026, 11, 1)),
        ],
        "weather_timestamp string, temp_c string, precipitation_mm string, "
        "_source_file string, _ingestion_timestamp timestamp",
    )

    rows = join_trips_with_weather(trips, weather).collect()

    assert len(rows) == 1
    assert rows[0]["temperature_bin"] == "Mild (10-20°C)"
    assert rows[0]["rain_bin"] == "Light Rain (>0-5.0mm)"


def test_weather_mart_covers_clean_trips(spark: SparkSession, pipeline: Dict[str, Any]) -> None:
    weather = spark.read.parquet(pipeline["tables"]["weather_ridership"])
    silver_rows = spark.read.parquet(pipeline["paths"]["silver_cleaned"]).count()

    assert weather.agg(F.sum("total_trips")).first()[0] == silver_rows
    assert weather.filter(F.col("temperature_bin").isNull() | F.col("rain_bin").isNull()).count() == 0
    assert weather.filter((F.col("temperature_bin") == "Unknown") | (F.col("rain_bin") == "Unknown")).count() == 0


def test_user_behavior_kpis(spark: SparkSession, pipeline: Dict[str, Any]) -> None:
    kpis = spark.read.parquet(pipeline["tables"]["user_kpis"])
    corridors = spark.read.parquet(pipeline["tables"]["top_corridors"])
    silver_rows = spark.read.parquet(pipeline["paths"]["silver_cleaned"]).count()

    assert kpis.agg(F.sum("total_trips")).first()[0] == silver_rows
    members = kpis.filter(F.col("member_casual") == "member").collect()
    assert members
    for row in members:
        assert 1.0 <= row["avg_duration_min"] <= 1440.0
        assert 0.0 < row["avg_speed_kmh"] <= 55.0

    for tier in ("member", "casual"):
        tier_rows = corridors.filter(F.col("member_casual") == tier)
        assert tier_rows.count() == 10
        assert tier_rows.select("corridor_rank").distinct().count() == 10
    assert corridors.filter(F.col("start_station_name").isNull()).count() == 0


def test_gold_parquet_artifacts_exist(pipeline: Dict[str, Any]) -> None:
    for table_path in pipeline["tables"].values():
        assert list(Path(table_path).rglob("*.parquet")), f"no Parquet files in {table_path}"

    with open(pipeline["paths"]["gold_ready_signal"], encoding="utf-8") as signal_file:
        signal = json.load(signal_file)
    assert signal["status"] == "SUCCESS"
    assert all(signal["checks"].values())


def test_failed_reconciliation_blocks_ready_signal(
    spark: SparkSession, pipeline: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_env = "gate"
    shutil.copytree(
        os.path.join(pipeline["base_dir"], ENV), os.path.join(pipeline["base_dir"], gate_env)
    )
    real_builder = build_gold_module.build_station_flow
    monkeypatch.setattr(
        build_gold_module, "build_station_flow", lambda *args: real_builder(*args).limit(1)
    )

    with pytest.raises(RuntimeError, match="outbound_matches_silver"):
        run_gold_transformation(spark, gate_env, "pytest_gate")

    with open(get_env_paths(gate_env)["gold_ready_signal"], encoding="utf-8") as signal_file:
        assert json.load(signal_file)["status"] == "FAILED"
