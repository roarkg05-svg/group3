#!/usr/bin/env python3
"""Reproducible PySpark performance benchmark for DAT535 Medallion Pipeline.

Compares two Spark execution configurations on the same analytical workload
(joining Silver trips with the Bronze station catalog + daily station-flow
aggregation and peak departure hour calculation):

  - Configuration A (Untuned Baseline):
      spark.sql.shuffle.partitions = 200
      spark.sql.autoBroadcastJoinThreshold = -1 (forces SortMergeJoin)

  - Configuration B (Optimized Project Configuration):
      spark.sql.shuffle.partitions = 8
      Explicit F.broadcast(stations_df) (forces BroadcastHashJoin)

Usage:
    python scripts/benchmark_spark.py
    python scripts/benchmark_spark.py --env dev --runs 3
"""

import argparse
import io
import os
import re
import sys
import time
from contextlib import redirect_stdout
from typing import Any, Dict, List

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from config.settings import get_env_paths
from src.bronze.ingest_bronze import run_bronze_ingestion
from src.silver.clean_quarantine import run_silver_cleaning
from src.utils.data_generator import generate_raw_batches
from src.utils.spark_session import get_spark_session


def _ensure_dev_data(spark: SparkSession, env: str) -> Dict[str, str]:
    """Ensure Silver trips and Bronze stations exist for the benchmark."""
    paths = get_env_paths(env)
    if not os.path.exists(paths["silver_cleaned"]) or not os.path.exists(paths["bronze_stations"]):
        print(f"[Bootstrap] Generating and ingesting {env} dataset for benchmark...")
        generate_raw_batches(paths["raw_dir"], num_trips=35000, seed=42)
        run_bronze_ingestion(spark, env=env, run_id=f"bench_bootstrap_{env}")
        run_silver_cleaning(spark, env=env, run_id=f"bench_bootstrap_{env}")
    return paths


def _build_workload_df(
    trips_df: DataFrame, stations_df: DataFrame, use_broadcast: bool
) -> DataFrame:
    """Build the station-join + daily aggregation + peak-hour workload DataFrame."""
    catalog = (
        stations_df.select(
            F.col("station_id"),
            F.col("station_name"),
            F.col("region_id"),
            F.col("capacity").cast("long").alias("capacity"),
        )
        .filter(F.col("station_id").isNotNull())
        .dropDuplicates(["station_id"])
    )

    outbound = (
        trips_df.filter(F.col("start_station_id").isNotNull())
        .withColumn("trip_date", F.to_date("started_at"))
        .groupBy(F.col("start_station_id").alias("station_id"), "trip_date")
        .agg(F.count(F.lit(1)).alias("outbound_trips"))
    )

    inbound = (
        trips_df.filter(F.col("end_station_id").isNotNull())
        .withColumn("trip_date", F.to_date("ended_at"))
        .groupBy(F.col("end_station_id").alias("station_id"), "trip_date")
        .agg(F.count(F.lit(1)).alias("inbound_trips"))
    )

    hourly = (
        trips_df.filter(F.col("start_station_id").isNotNull())
        .withColumn("trip_date", F.to_date("started_at"))
        .withColumn("departure_hour", F.hour("started_at"))
        .groupBy("start_station_id", "trip_date", "departure_hour")
        .agg(F.count(F.lit(1)).alias("peak_departure_trips"))
    )
    peak_window = Window.partitionBy("start_station_id", "trip_date").orderBy(
        F.col("peak_departure_trips").desc(), F.col("departure_hour").asc()
    )
    peak_hours = (
        hourly.withColumn("_rank", F.row_number().over(peak_window))
        .filter(F.col("_rank") == 1)
        .select(
            F.col("start_station_id").alias("station_id"),
            F.col("trip_date"),
            F.col("departure_hour").alias("peak_departure_hour"),
            F.col("peak_departure_trips"),
        )
    )

    dim_target = F.broadcast(catalog) if use_broadcast else catalog

    result_df = (
        outbound.join(inbound, on=["station_id", "trip_date"], how="full_outer")
        .fillna(0, subset=["outbound_trips", "inbound_trips"])
        .join(dim_target, on="station_id", how="left")
        .join(peak_hours, on=["station_id", "trip_date"], how="left")
        .withColumn("net_flow", F.col("inbound_trips") - F.col("outbound_trips"))
    )
    return result_df


def _capture_explain_formatted(df: DataFrame) -> str:
    """Capture the output of df.explain(mode='formatted') as a string."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


def _extract_key_operators(explain_text: str) -> List[str]:
    """Extract physical join and exchange operators from formatted explain output."""
    operators: List[str] = []
    for line in explain_text.splitlines():
        if any(
            op in line
            for op in (
                "SortMergeJoin",
                "BroadcastHashJoin",
                "BroadcastExchange",
                "Exchange",
                "HashAggregate",
                "Window",
            )
        ):
            cleaned = re.sub(r"\s+", " ", line).strip()
            if cleaned and cleaned not in operators:
                operators.append(cleaned)
    return operators


def _run_benchmark_config(
    spark: SparkSession,
    trips_df: DataFrame,
    stations_df: DataFrame,
    name: str,
    shuffle_partitions: int,
    auto_broadcast_threshold: int,
    use_broadcast: bool,
    adaptive_enabled: bool,
    num_runs: int = 3,
) -> Dict[str, Any]:
    """Run warm-up + N timed iterations for a specific Spark configuration."""
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(auto_broadcast_threshold))
    spark.conf.set("spark.sql.adaptive.enabled", "true" if adaptive_enabled else "false")

    # Warm-up pass (1 run)
    warmup_df = _build_workload_df(trips_df, stations_df, use_broadcast=use_broadcast)
    _ = warmup_df.count()

    # Capture physical plan and partition count
    explain_str = _capture_explain_formatted(warmup_df)
    key_ops = _extract_key_operators(explain_str)
    num_partitions = warmup_df.rdd.getNumPartitions()

    # Timed runs
    run_times: List[float] = []
    row_count = 0
    for i in range(1, num_runs + 1):
        spark.catalog.clearCache()
        t0 = time.perf_counter()
        df = _build_workload_df(trips_df, stations_df, use_broadcast=use_broadcast)
        row_count = df.count()
        elapsed = time.perf_counter() - t0
        run_times.append(elapsed)
        print(f"  [{name}] Run {i}/{num_runs}: {elapsed:.3f}s (rows={row_count})")

    avg_time = sum(run_times) / len(run_times)
    return {
        "name": name,
        "shuffle_partitions_configured": shuffle_partitions,
        "output_partitions": num_partitions,
        "auto_broadcast_threshold": auto_broadcast_threshold,
        "use_broadcast": use_broadcast,
        "run_times": run_times,
        "avg_time_sec": avg_time,
        "row_count": row_count,
        "key_operators": key_ops,
        "explain_formatted": explain_str,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="DAT535 Spark Configuration Benchmark")
    parser.add_argument("--env", default="dev", choices=["dev", "test", "prod"])
    parser.add_argument("--runs", type=int, default=3, help="Number of timed runs after 1 warm-up")
    args = parser.parse_args()

    spark = get_spark_session("DAT535_Performance_Benchmark")
    paths = _ensure_dev_data(spark, args.env)

    trips_df = spark.read.parquet(paths["silver_cleaned"])
    stations_df = spark.read.parquet(paths["bronze_stations"])

    trips_count = trips_df.count()
    stations_count = stations_df.count()

    print("\n" + "=" * 80)
    print("DAT535 PYSPARK PERFORMANCE BENCHMARK (BASELINE VS. OPTIMIZED)")
    print("=" * 80)
    print(f"Environment        : {args.env}")
    print(f"Silver Clean Trips : {trips_count:,} rows")
    print(f"Bronze Stations    : {stations_count:,} rows")
    print(f"Protocol           : 1 warm-up run + {args.runs} timed runs per configuration")
    print("-" * 80)

    print("\n>>> Executing Configuration A: Baseline (shuffle.partitions=200, SortMergeJoin)...")
    res_a = _run_benchmark_config(
        spark=spark,
        trips_df=trips_df,
        stations_df=stations_df,
        name="Config A (Baseline)",
        shuffle_partitions=200,
        auto_broadcast_threshold=-1,
        use_broadcast=False,
        adaptive_enabled=False,
        num_runs=args.runs,
    )

    print("\n>>> Executing Configuration B: Optimized (shuffle.partitions=8, BroadcastHashJoin)...")
    res_b = _run_benchmark_config(
        spark=spark,
        trips_df=trips_df,
        stations_df=stations_df,
        name="Config B (Optimized)",
        shuffle_partitions=8,
        auto_broadcast_threshold=10485760,
        use_broadcast=True,
        adaptive_enabled=True,
        num_runs=args.runs,
    )

    improvement_pct = (
        ((res_a["avg_time_sec"] - res_b["avg_time_sec"]) / res_a["avg_time_sec"]) * 100.0
        if res_a["avg_time_sec"] > 0
        else 0.0
    )
    speedup_factor = (
        res_a["avg_time_sec"] / res_b["avg_time_sec"]
        if res_b["avg_time_sec"] > 0
        else 1.0
    )

    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY RESULTS")
    print("=" * 80)
    print(
        f"{'Metric':<35} | {'Config A (Baseline)':<20} | {'Config B (Optimized)':<20}"
    )
    print("-" * 80)
    print(
        f"{'spark.sql.shuffle.partitions':<35} | {res_a['shuffle_partitions_configured']:<20} | {res_b['shuffle_partitions_configured']:<20}"
    )
    print(
        f"{'Join Strategy':<35} | {'SortMergeJoin':<20} | {'BroadcastHashJoin':<20}"
    )
    print(
        f"{'Resulting Output Partitions':<35} | {res_a['output_partitions']:<20} | {res_b['output_partitions']:<20}"
    )
    print(
        f"{'Average Execution Time (3 runs)':<35} | {res_a['avg_time_sec']:<19.3f}s | {res_b['avg_time_sec']:<19.3f}s"
    )
    print("-" * 80)
    print(f"Latency Reduction / Improvement : {improvement_pct:.2f}% faster ({speedup_factor:.2f}x speedup)")
    print("=" * 80)

    print("\n--- Key Physical Plan Operators: Configuration A (Baseline) ---")
    for op in res_a["key_operators"][:10]:
        print(f"  {op}")

    print("\n--- Key Physical Plan Operators: Configuration B (Optimized) ---")
    for op in res_b["key_operators"][:10]:
        print(f"  {op}")

    print("\n--- Full Formatted Physical Plan (Configuration B - Optimized) ---")
    print(res_b["explain_formatted"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
