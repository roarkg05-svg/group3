"""Silver layer enrichment and conformance module.

Conforms cleaned trips by attaching GBFS station catalog dimensions (station names,
borough/region, dock capacity) and temporal calendar attributes (trip date, hour of day,
day of week, weekend indicator, and catalog presence flags).
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import get_env_paths


def run_silver_enrichment(
    spark: SparkSession, env: str = "dev", run_id: str = "manual_silver_enrichment"
) -> Dict[str, Any]:
    """Enrich cleaned Silver trips with GBFS station dimensions and write silver_enriched."""
    paths = get_env_paths(env)

    trips_df = spark.read.parquet(paths["silver_cleaned"])
    stations_df = spark.read.parquet(paths["bronze_stations"])

    # Prepare deduplicated station dimension table
    station_dim = (
        stations_df.select(
            F.trim(F.col("station_id")).alias("dim_station_id"),
            F.col("station_name"),
            F.col("region_id"),
            F.col("capacity").cast("long").alias("capacity"),
        )
        .filter(F.col("dim_station_id").isNotNull())
        .dropDuplicates(["dim_station_id"])
    )

    start_dim = station_dim.select(
        F.col("dim_station_id").alias("_start_dim_id"),
        F.col("station_name").alias("start_station_name"),
        F.col("region_id").alias("start_region_id"),
        F.col("capacity").alias("start_capacity"),
    )

    end_dim = station_dim.select(
        F.col("dim_station_id").alias("_end_dim_id"),
        F.col("station_name").alias("end_station_name"),
        F.col("region_id").alias("end_region_id"),
        F.col("capacity").alias("end_capacity"),
    )

    # Prepare cleaned trips, dropping any existing station metadata columns to avoid ambiguous references
    trips_base_df = trips_df
    for col_to_drop in [
        "start_station_name",
        "end_station_name",
        "start_region_id",
        "end_region_id",
        "start_capacity",
        "end_capacity",
    ]:
        if col_to_drop in trips_base_df.columns:
            trips_base_df = trips_base_df.drop(col_to_drop)

    enriched_df = (
        trips_base_df.join(
            F.broadcast(start_dim),
            trips_base_df["start_station_id"] == start_dim["_start_dim_id"],
            how="left",
        )
        .join(
            F.broadcast(end_dim),
            trips_base_df["end_station_id"] == end_dim["_end_dim_id"],
            how="left",
        )
        .drop("_start_dim_id", "_end_dim_id")
        .withColumn("trip_date", F.to_date(F.col("started_at")))
        .withColumn("hour_of_day", F.hour(F.col("started_at")))
        .withColumn("day_of_week", F.dayofweek(F.col("started_at")))
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1, 7), F.lit(True)).otherwise(F.lit(False)),
        )
        .withColumn(
            "is_start_catalog_station",
            F.col("start_station_name").isNotNull(),
        )
        .withColumn(
            "is_end_catalog_station",
            F.col("end_station_name").isNotNull(),
        )
    )

    # Persist Silver Enriched table
    enriched_path = paths["silver_enriched"]
    enriched_df.write.mode("overwrite").parquet(enriched_path)

    persisted_enriched_df = spark.read.parquet(enriched_path)
    enriched_count = persisted_enriched_df.count()

    metrics: Dict[str, Any] = {
        "run_id": run_id,
        "env": env,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "input_cleaned_rows": trips_df.count(),
        "enriched_rows": enriched_count,
        "target_path": enriched_path,
    }
    return metrics


if __name__ == "__main__":
    from src.utils.spark_session import get_spark_session

    env_name = os.getenv("DAT535_ENV", "dev")
    spark_sess = get_spark_session("DAT535_Silver_Enrichment")
    out_metrics = run_silver_enrichment(spark_sess, env=env_name)
    print(json.dumps(out_metrics, indent=2))
