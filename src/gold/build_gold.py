"""Gold layer orchestrator: builds every mart, reconciles it with Silver, then signals readiness."""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import get_env_paths
from src.gold.mart_station_flow import build_station_flow
from src.gold.mart_user_behavior import build_top_corridors, build_user_kpis
from src.gold.mart_weather_ridership import build_weather_ridership


def _gold_table_paths(paths: Dict[str, str]) -> Dict[str, str]:
    """Return the Parquet location of each Gold table."""
    user_behavior = paths["gold_rebalancing"].replace(
        "station_rebalancing_mart", "user_behavior_mart"
    )
    return {
        "station_flow": paths["gold_rebalancing"],
        "user_kpis": f"{user_behavior}/kpis",
        "top_corridors": f"{user_behavior}/top_corridors",
        "weather_ridership": paths["gold_weather_impact"],
    }


def _sum_column(df: DataFrame, column: str) -> int:
    return int(df.agg(F.coalesce(F.sum(column), F.lit(0))).first()[0])


def _write_signal(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as signal_file:
        json.dump(payload, signal_file, indent=2)


def run_gold_transformation(spark: SparkSession, env: str, run_id: str) -> Dict[str, Any]:
    """Execute all Gold marts and write a JSON summary signal.

    The signal says SUCCESS only when every mart reconciles with Silver. Otherwise
    it says FAILED and a RuntimeError stops the pipeline.
    """
    paths = get_env_paths(env)
    tables = _gold_table_paths(paths)

    # A signal left by an earlier run must not vouch for the tables rewritten below.
    if os.path.exists(paths["gold_ready_signal"]):
        os.remove(paths["gold_ready_signal"])

    trips_df = spark.read.parquet(paths["silver_cleaned"])
    stations_df = spark.read.parquet(paths["bronze_stations"])
    weather_df = spark.read.parquet(paths["bronze_weather"])

    build_station_flow(trips_df, stations_df).write.mode("overwrite").partitionBy(
        "year", "month"
    ).parquet(tables["station_flow"])
    build_user_kpis(trips_df).write.mode("overwrite").parquet(tables["user_kpis"])
    build_top_corridors(trips_df, stations_df).write.mode("overwrite").parquet(
        tables["top_corridors"]
    )
    build_weather_ridership(trips_df, weather_df).write.mode("overwrite").parquet(
        tables["weather_ridership"]
    )

    station_flow = spark.read.parquet(tables["station_flow"])
    user_kpis = spark.read.parquet(tables["user_kpis"])
    top_corridors = spark.read.parquet(tables["top_corridors"])
    weather_ridership = spark.read.parquet(tables["weather_ridership"])

    silver_rows = trips_df.count()
    outbound_total = _sum_column(station_flow, "outbound_trips")
    inbound_total = _sum_column(station_flow, "inbound_trips")
    kpi_trips = _sum_column(user_kpis, "total_trips")
    weather_trips = _sum_column(weather_ridership, "total_trips")

    checks = {
        "outbound_matches_silver": outbound_total == silver_rows,
        "inbound_matches_silver": inbound_total == silver_rows,
        "user_kpis_match_silver": kpi_trips == silver_rows,
        "weather_trips_not_multiplied": weather_trips <= silver_rows,
    }
    status = "SUCCESS" if all(checks.values()) else "FAILED"

    metrics: Dict[str, Any] = {
        "status": status,
        "run_id": run_id,
        "env": env,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "silver_rows": silver_rows,
        "station_flow_rows": station_flow.count(),
        "user_kpi_rows": user_kpis.count(),
        "top_corridor_rows": top_corridors.count(),
        "weather_ridership_rows": weather_ridership.count(),
        "weather_matched_trips": weather_trips,
        "weather_unmatched_trips": silver_rows - weather_trips,
        "unknown_station_rows": station_flow.filter(~F.col("is_catalog_station")).count(),
        "checks": checks,
    }
    _write_signal(paths["gold_ready_signal"], metrics)

    if status != "SUCCESS":
        failed = [name for name, passed in checks.items() if not passed]
        raise RuntimeError(f"Gold reconciliation against Silver failed: {failed}")

    return metrics
