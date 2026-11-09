"""Silver layer cleaning, coordinate imputation, deduplication, and 7-rule quarantine engine."""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from config.settings import NYC_BOUNDS, QUALITY_THRESHOLDS, get_env_paths

RULE_NAMES = [
    "MISSING_CRITICAL_KEY",
    "NEGATIVE_OR_ZERO_DURATION",
    "FALSE_UNLOCK_UNDER_60S",
    "LOST_OR_STOLEN_OVER_24H",
    "CIRCULAR_ABANDONED_TRIP",
    "IMPOSSIBLE_PHYSICAL_SPEED",
    "OUT_OF_NYC_BOUNDS",
]


def _trim_or_null(col_name: str) -> F.Column:
    """Trim string column and normalize empty strings to null."""
    trimmed = F.trim(F.col(col_name).cast(StringType()))
    return F.when(trimmed == "", F.lit(None).cast(StringType())).otherwise(trimmed)


def _normalize_coord(col_name: str) -> F.Column:
    """Trim coordinate string, normalize empty strings / whitespace / 0.0 to null, cast to double."""
    raw_str = F.trim(F.col(col_name).cast(StringType()))
    empty_or_null = raw_str.isNull() | (raw_str == "")
    casted = F.when(empty_or_null, F.lit(None).cast(DoubleType())).otherwise(
        raw_str.cast(DoubleType())
    )
    return F.when((casted.isNull()) | (casted == 0.0), F.lit(None).cast(DoubleType())).otherwise(
        casted
    )


def _apply_shape_and_imputation(
    bronze_trips_df: DataFrame, bronze_stations_df: DataFrame
) -> DataFrame:
    """Method 1 (Shape), Station Coordinate Imputation, and Haversine Physics Calculation."""
    shaped_df = (
        bronze_trips_df.withColumn("ride_id", _trim_or_null("ride_id"))
        .withColumn("bike_id", _trim_or_null("bike_id"))
        .withColumn("rideable_type", _trim_or_null("rideable_type"))
        .withColumn("start_station_id", _trim_or_null("start_station_id"))
        .withColumn("end_station_id", _trim_or_null("end_station_id"))
        .withColumn("member_casual", _trim_or_null("member_casual"))
        .withColumn("started_at", F.to_timestamp(F.trim(F.col("started_at"))))
        .withColumn("ended_at", F.to_timestamp(F.trim(F.col("ended_at"))))
        .withColumn("start_lat", _normalize_coord("start_lat"))
        .withColumn("start_lng", _normalize_coord("start_lng"))
        .withColumn("end_lat", _normalize_coord("end_lat"))
        .withColumn("end_lng", _normalize_coord("end_lng"))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp(F.col("ended_at")) - F.unix_timestamp(F.col("started_at")))
            / F.lit(60.0),
        )
    )

    # Prepare deduplicated station coordinate reference for end_station_id lookup
    station_lookup_df = (
        bronze_stations_df.select(
            F.trim(F.col("station_id").cast(StringType())).alias("_ref_station_id"),
            F.col("station_lat").cast(DoubleType()).alias("station_lat"),
            F.col("station_lng").cast(DoubleType()).alias("station_lng"),
        )
        .filter(F.col("_ref_station_id").isNotNull())
        .dropDuplicates(["_ref_station_id"])
    )

    joined_df = shaped_df.join(
        F.broadcast(station_lookup_df),
        shaped_df["end_station_id"] == station_lookup_df["_ref_station_id"],
        how="left",
    )

    coord_imputed_cond = (
        (F.col("end_lat").isNull() | F.col("end_lng").isNull())
        & F.col("station_lat").isNotNull()
        & F.col("station_lng").isNotNull()
    )

    imputed_df = (
        joined_df.withColumn(
            "_coord_imputed",
            coord_imputed_cond,
        )
        .withColumn(
            "dq_handling_flag",
            F.when(coord_imputed_cond, F.lit("IMPUTED_END_COORDS")).otherwise(
                F.lit("ORIGINAL_COORDS")
            ),
        )
        .withColumn(
            "end_lat",
            F.coalesce(F.col("end_lat"), F.col("station_lat")),
        )
        .withColumn(
            "end_lng",
            F.coalesce(F.col("end_lng"), F.col("station_lng")),
        )
        .drop("_ref_station_id", "station_lat", "station_lng")
    )

    # Haversine great-circle distance (km) and physical speed (km/h)
    earth_radius_km = 6371.0
    dlat = F.radians(F.col("end_lat") - F.col("start_lat"))
    dlng = F.radians(F.col("end_lng") - F.col("start_lng"))
    a_expr = F.pow(F.sin(dlat / F.lit(2.0)), 2) + (
        F.cos(F.radians(F.col("start_lat")))
        * F.cos(F.radians(F.col("end_lat")))
        * F.pow(F.sin(dlng / F.lit(2.0)), 2)
    )
    a_clamped = F.least(F.lit(1.0), F.greatest(F.lit(0.0), a_expr))
    distance_km_expr = F.lit(2.0 * earth_radius_km) * F.asin(F.sqrt(a_clamped))

    physics_df = imputed_df.withColumn("distance_km", distance_km_expr).withColumn(
        "speed_kmh",
        F.when(
            (F.col("trip_duration_min").isNotNull())
            & (F.col("trip_duration_min") > 0.0)
            & (F.col("distance_km").isNotNull()),
            F.col("distance_km") / (F.col("trip_duration_min") / F.lit(60.0)),
        ).otherwise(F.lit(0.0)),
    )

    return physics_df


def run_silver_cleaning(spark: SparkSession, env: str, run_id: str) -> Dict[str, Any]:
    """Execute Silver Methods 1 (Shape), 2 (Identity), and 3 (7-Rule Quarantine Engine)."""
    paths = get_env_paths(env)

    bronze_trips_df = spark.read.parquet(paths["bronze_trips"])
    bronze_stations_df = spark.read.parquet(paths["bronze_stations"])

    bronze_input_rows = bronze_trips_df.count()

    # Step 1: Shape, Coordinate Imputation, and Haversine Physics
    shaped_df = _apply_shape_and_imputation(bronze_trips_df, bronze_stations_df)

    # Step 2: Method 2 (Identity - Grain Deduplication on ride_id ordered by _ingestion_timestamp DESC)
    # Preserve null ride_id rows so they are routed to Quarantine Rule 1 rather than collapsed
    partition_ride_key = F.when(
        F.col("ride_id").isNotNull(), F.col("ride_id")
    ).otherwise(
        F.concat(F.lit("__NULL_RIDE_"), F.monotonically_increasing_id().cast(StringType()))
    )
    dedup_window = Window.partitionBy(partition_ride_key).orderBy(
        F.col("_ingestion_timestamp").desc(), F.col("_source_file").desc()
    )

    dedup_df = (
        shaped_df.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    deduplicated_rows = dedup_df.count()
    duplicates_removed_count = bronze_input_rows - deduplicated_rows
    imputed_coords_count = dedup_df.filter(
        (F.col("dq_handling_flag") == "IMPUTED_END_COORDS")
        | (F.col("_coord_imputed") == True)
    ).count()

    # Step 3: Method 3 (Trust - 7-Rule Quarantine Engine, "Flag Don't Delete")
    cond_missing_key = (
        F.col("ride_id").isNull()
        | F.col("started_at").isNull()
        | F.col("ended_at").isNull()
        | F.col("start_station_id").isNull()
    )

    cond_neg_or_zero_dur = F.col("trip_duration_min").isNotNull() & (
        F.col("trip_duration_min") <= 0.0
    )

    cond_false_unlock = (
        F.col("trip_duration_min").isNotNull()
        & (F.col("trip_duration_min") > 0.0)
        & (F.col("trip_duration_min") < F.lit(QUALITY_THRESHOLDS["min_duration_min"]))
    )

    cond_over_24h = F.col("trip_duration_min").isNotNull() & (
        F.col("trip_duration_min") > F.lit(QUALITY_THRESHOLDS["max_duration_min"])
    )

    cond_circular = (
        F.col("start_station_id").isNotNull()
        & F.col("end_station_id").isNotNull()
        & (F.col("start_station_id") == F.col("end_station_id"))
        & F.col("trip_duration_min").isNotNull()
        & (F.col("trip_duration_min") > 0.0)
        & (F.col("trip_duration_min") < 3.0)
        & F.col("distance_km").isNotNull()
        & (F.col("distance_km") < 0.05)
    )

    cond_impossible_speed = F.col("speed_kmh").isNotNull() & (
        (
            (F.col("rideable_type") == "classic_bike")
            & (F.col("speed_kmh") > F.lit(QUALITY_THRESHOLDS["max_classic_speed_kmh"]))
        )
        | (
            (F.col("rideable_type") == "electric_bike")
            & (F.col("speed_kmh") > F.lit(QUALITY_THRESHOLDS["max_electric_speed_kmh"]))
        )
    )

    cond_out_of_bounds = (
        F.col("start_lat").isNull()
        | F.col("start_lng").isNull()
        | F.col("end_lat").isNull()
        | F.col("end_lng").isNull()
        | (F.col("start_lat") < F.lit(NYC_BOUNDS["lat_min"]))
        | (F.col("start_lat") > F.lit(NYC_BOUNDS["lat_max"]))
        | (F.col("end_lat") < F.lit(NYC_BOUNDS["lat_min"]))
        | (F.col("end_lat") > F.lit(NYC_BOUNDS["lat_max"]))
        | (F.col("start_lng") < F.lit(NYC_BOUNDS["lng_min"]))
        | (F.col("start_lng") > F.lit(NYC_BOUNDS["lng_max"]))
        | (F.col("end_lng") < F.lit(NYC_BOUNDS["lng_min"]))
        | (F.col("end_lng") > F.lit(NYC_BOUNDS["lng_max"]))
    )

    rejection_reason_expr = F.concat_ws(
        ",",
        F.when(cond_missing_key, F.lit("MISSING_CRITICAL_KEY")),
        F.when(cond_neg_or_zero_dur, F.lit("NEGATIVE_OR_ZERO_DURATION")),
        F.when(cond_false_unlock, F.lit("FALSE_UNLOCK_UNDER_60S")),
        F.when(cond_over_24h, F.lit("LOST_OR_STOLEN_OVER_24H")),
        F.when(cond_circular, F.lit("CIRCULAR_ABANDONED_TRIP")),
        F.when(cond_impossible_speed, F.lit("IMPOSSIBLE_PHYSICAL_SPEED")),
        F.when(cond_out_of_bounds, F.lit("OUT_OF_NYC_BOUNDS")),
    )

    primary_dq_issue_expr = (
        F.when(cond_missing_key, F.lit("MISSING_CRITICAL_KEY"))
        .when(cond_neg_or_zero_dur, F.lit("NEGATIVE_OR_ZERO_DURATION"))
        .when(cond_false_unlock, F.lit("FALSE_UNLOCK_UNDER_60S"))
        .when(cond_over_24h, F.lit("LOST_OR_STOLEN_OVER_24H"))
        .when(cond_circular, F.lit("CIRCULAR_ABANDONED_TRIP"))
        .when(cond_impossible_speed, F.lit("IMPOSSIBLE_PHYSICAL_SPEED"))
        .when(cond_out_of_bounds, F.lit("OUT_OF_NYC_BOUNDS"))
        .otherwise(F.lit(None).cast(StringType()))
    )

    evaluated_df = dedup_df.withColumn("dq_issue", primary_dq_issue_expr).withColumn(
        "_rejection_reason", rejection_reason_expr
    )

    quarantine_df = evaluated_df.filter(F.col("dq_issue").isNotNull())
    cleaned_df = evaluated_df.filter(F.col("dq_issue").isNull()).drop(
        "dq_issue", "_rejection_reason"
    )

    # Persist Silver Cleaned and Silver Quarantine Parquet datasets
    cleaned_df.write.mode("overwrite").parquet(paths["silver_cleaned"])
    quarantine_df.write.mode("overwrite").parquet(paths["silver_quarantine"])

    # Maintain backward compatibility with alternative silver directory conventions
    silver_base_dir = os.path.dirname(paths["silver_cleaned"].rstrip("/\\"))
    if not paths["silver_cleaned"].endswith("trips_cleaned"):
        cleaned_df.write.mode("overwrite").parquet(os.path.join(silver_base_dir, "trips_cleaned"))
    if not paths["silver_quarantine"].endswith("trips_quarantine"):
        quarantine_df.write.mode("overwrite").parquet(os.path.join(silver_base_dir, "trips_quarantine"))

    # Compute exact metrics from persisted outputs
    persisted_cleaned_df = spark.read.parquet(paths["silver_cleaned"])
    persisted_quarantine_df = spark.read.parquet(paths["silver_quarantine"])

    valid_cleaned_rows = persisted_cleaned_df.count()
    quarantined_rows = persisted_quarantine_df.count()

    breakdown_rows = (
        persisted_quarantine_df.groupBy("dq_issue").count().collect()
    )
    quarantine_breakdown_by_rule: Dict[str, int] = {rule: 0 for rule in RULE_NAMES}
    for row in breakdown_rows:
        issue_name = row["dq_issue"]
        if issue_name:
            quarantine_breakdown_by_rule[issue_name] = int(row["count"])

    quarantine_rate = (
        round(quarantined_rows / float(deduplicated_rows), 4)
        if deduplicated_rows > 0
        else 0.0
    )

    cleaning_metrics: Dict[str, Any] = {
        "run_id": run_id,
        "env": env,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bronze_input_rows": bronze_input_rows,
        "deduplicated_rows": deduplicated_rows,
        "duplicates_removed": duplicates_removed_count,
        "duplicates_removed_count": duplicates_removed_count,
        "imputed_coords_count": imputed_coords_count,
        "valid_cleaned_rows": valid_cleaned_rows,
        "quarantined_rows": quarantined_rows,
        "quarantine_rate": quarantine_rate,
        "max_quarantine_threshold": QUALITY_THRESHOLDS["max_quarantine_rate"],
        "quality_gate_passed": quarantine_rate <= QUALITY_THRESHOLDS["max_quarantine_rate"],
        "quarantine_breakdown_by_rule": quarantine_breakdown_by_rule,
    }

    # Write preliminary Silver quality report (can be enriched by downstream Silver conformance)
    quality_report_path = paths["silver_quality_report"]
    os.makedirs(os.path.dirname(quality_report_path), exist_ok=True)
    with open(quality_report_path, "w", encoding="utf-8") as f_rep:
        json.dump(cleaning_metrics, f_rep, indent=2)

    alt_quality_report = os.path.join(
        os.path.dirname(quality_report_path),
        "_quality_report.json"
        if quality_report_path.endswith("silver_quality_report.json")
        else "silver_quality_report.json",
    )
    with open(alt_quality_report, "w", encoding="utf-8") as f_rep:
        json.dump(cleaning_metrics, f_rep, indent=2)

    return cleaning_metrics


if __name__ == "__main__":
    from src.bronze.ingest_bronze import run_bronze_ingestion
    from src.utils.spark_session import get_spark_session

    env_name = os.getenv("DAT535_ENV", "dev")
    run_identifier = f"silver_core_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    spark_sess = get_spark_session()
    run_bronze_ingestion(spark_sess, env=env_name, run_id=run_identifier)
    metrics = run_silver_cleaning(spark_sess, env=env_name, run_id=run_identifier)
    print(json.dumps(metrics, indent=2))
