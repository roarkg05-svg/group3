"""Bronze layer batch ingestion implementing 'Load First, Interpret Later' and 'Payload Plus Evidence'."""

import glob
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import get_env_paths
from src.utils.data_generator import generate_raw_batches

EXPECTED_COLUMNS = {
    "trips": {
        "ride_id",
        "bike_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_id",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual",
    },
    "stations": {
        "station_id",
        "station_name",
        "capacity",
        "region_id",
        "station_lat",
        "station_lng",
        "is_active",
    },
    "weather": {
        "weather_timestamp",
        "temp_c",
        "precipitation_mm",
        "wind_kmh",
        "condition",
    },
}

METADATA_COLUMNS = [
    "_source_file",
    "_dataset_type",
    "_ingestion_timestamp",
    "_run_id",
    "_schema_version",
]


def _append_audit_metadata(
    df: DataFrame, dataset_type: str, run_id: str, schema_version: str = "v1.0"
) -> DataFrame:
    """Attach mandatory Bronze lineage and audit metadata columns to a raw DataFrame."""
    return (
        df.withColumn("_source_file", F.input_file_name())
        .withColumn("_dataset_type", F.lit(dataset_type))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_schema_version", F.lit(schema_version))
    )


def run_bronze_ingestion(spark: SparkSession, env: str, run_id: str) -> Dict[str, Any]:
    """Ingest raw landing batches into Bronze Parquet tables and emit a JSON delivery receipt."""
    paths = get_env_paths(env)
    raw_dir = paths["raw_dir"]

    trips_raw_dir = os.path.join(raw_dir, "trips")
    stations_raw_dir = os.path.join(raw_dir, "stations")
    weather_raw_dir = os.path.join(raw_dir, "weather")

    # Auto-bootstrap raw landing files if missing
    if not os.path.exists(trips_raw_dir) or not glob.glob(os.path.join(trips_raw_dir, "*.csv")):
        generate_raw_batches(raw_dir=raw_dir)

    received_trip_files = sorted(glob.glob(os.path.join(trips_raw_dir, "*.csv")))
    received_station_files = sorted(glob.glob(os.path.join(stations_raw_dir, "*.json")))
    received_weather_files = sorted(glob.glob(os.path.join(weather_raw_dir, "*.csv")))

    # Read raw datasets preserving source payload fidelity
    raw_trips_df = spark.read.option("header", "true").csv(trips_raw_dir)
    if "bike_id" not in raw_trips_df.columns:
        raw_trips_df = raw_trips_df.withColumn("bike_id", F.lit(None).cast("string"))
    raw_stations_df = spark.read.option("multiLine", "true").json(stations_raw_dir)
    raw_weather_df = spark.read.option("header", "true").csv(weather_raw_dir)

    # Append Payload-Plus-Evidence audit columns
    bronze_trips_df = _append_audit_metadata(raw_trips_df, "trips", run_id)
    bronze_stations_df = _append_audit_metadata(raw_stations_df, "stations", run_id)
    bronze_weather_df = _append_audit_metadata(raw_weather_df, "weather", run_id)

    # Persist Bronze Parquet tables in overwrite mode
    bronze_trips_df.write.mode("overwrite").parquet(paths["bronze_trips"])
    bronze_stations_df.write.mode("overwrite").parquet(paths["bronze_stations"])
    bronze_weather_df.write.mode("overwrite").parquet(paths["bronze_weather"])

    # Compute row counts from persisted Bronze tables
    trips_count = spark.read.parquet(paths["bronze_trips"]).count()
    stations_count = spark.read.parquet(paths["bronze_stations"]).count()
    weather_count = spark.read.parquet(paths["bronze_weather"]).count()

    # Validate expected source + metadata columns
    trips_schema_ok = EXPECTED_COLUMNS["trips"].issubset(set(bronze_trips_df.columns)) and set(
        METADATA_COLUMNS
    ).issubset(set(bronze_trips_df.columns))
    stations_schema_ok = EXPECTED_COLUMNS["stations"].issubset(
        set(bronze_stations_df.columns)
    ) and set(METADATA_COLUMNS).issubset(set(bronze_stations_df.columns))
    weather_schema_ok = EXPECTED_COLUMNS["weather"].issubset(
        set(bronze_weather_df.columns)
    ) and set(METADATA_COLUMNS).issubset(set(bronze_weather_df.columns))

    schema_valid = bool(trips_schema_ok and stations_schema_ok and weather_schema_ok)

    expected_files = {"trips": 2, "stations": 1, "weather": 1, "total": 4}
    received_files = {
        "trips": len(received_trip_files),
        "stations": len(received_station_files),
        "weather": len(received_weather_files),
        "total": len(received_trip_files)
        + len(received_station_files)
        + len(received_weather_files),
    }

    receipt: Dict[str, Any] = {
        "run_id": run_id,
        "env": env,
        "schema_version": "v1.0",
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "SUCCESS"
        if schema_valid and received_files["total"] >= expected_files["total"]
        else "WARNING",
        "schema_validation_status": "PASSED" if schema_valid else "FAILED",
        "schema_valid": schema_valid,
        "expected_file_count": expected_files["total"],
        "received_file_count": received_files["total"],
        "expected_files": expected_files,
        "received_files": received_files,
        "row_counts": {
            "trips": trips_count,
            "stations": stations_count,
            "weather": weather_count,
        },
        "datasets": {
            "trips": {
                "expected_files": expected_files["trips"],
                "received_files": received_files["trips"],
                "row_count": trips_count,
                "target_path": paths["bronze_trips"],
                "schema_valid": trips_schema_ok,
                "columns": bronze_trips_df.columns,
            },
            "stations": {
                "expected_files": expected_files["stations"],
                "received_files": received_files["stations"],
                "row_count": stations_count,
                "target_path": paths["bronze_stations"],
                "schema_valid": stations_schema_ok,
                "columns": bronze_stations_df.columns,
            },
            "weather": {
                "expected_files": expected_files["weather"],
                "received_files": received_files["weather"],
                "row_count": weather_count,
                "target_path": paths["bronze_weather"],
                "schema_valid": weather_schema_ok,
                "columns": bronze_weather_df.columns,
            },
        },
    }

    receipt_path = paths["bronze_receipt"]
    os.makedirs(os.path.dirname(receipt_path), exist_ok=True)
    with open(receipt_path, "w", encoding="utf-8") as f_rcpt:
        json.dump(receipt, f_rcpt, indent=2)

    # Maintain backward compatibility with alternative receipt naming
    alt_receipt_path = os.path.join(
        os.path.dirname(receipt_path),
        "_delivery_receipt.json" if receipt_path.endswith("bronze_receipt.json") else "bronze_receipt.json",
    )
    with open(alt_receipt_path, "w", encoding="utf-8") as f_rcpt:
        json.dump(receipt, f_rcpt, indent=2)

    return receipt


if __name__ == "__main__":
    from src.utils.spark_session import get_spark_session

    env_name = os.getenv("DAT535_ENV", "dev")
    spark_sess = get_spark_session()
    out_receipt = run_bronze_ingestion(spark_sess, env=env_name, run_id="manual_bronze_run")
    print(json.dumps(out_receipt, indent=2))
