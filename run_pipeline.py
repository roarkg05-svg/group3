#!/usr/bin/env python3
"""DAT535 Medallion Architecture Pipeline Orchestrator.

Executes the end-to-end CityBikes Medallion pipeline (Bronze Ingestion,
Silver Core Cleaning & Quarantine, Silver Enrichment, and Gold Marts).

Usage:
    python run_pipeline.py --env dev
    python run_pipeline.py --env dev --source real --scope jc
    python run_pipeline.py --env dev --source real --scope nyc
    python run_pipeline.py --env test
    python run_pipeline.py --env prod
    python run_pipeline.py --help

Archived Coursework Labs (Optional):
    python run_pipeline.py lab2
    python run_pipeline.py lab3
    python run_pipeline.py all
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

# Ensure labs directory is discoverable if archived labs are invoked
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
LABS_DIR = os.path.join(REPO_ROOT, "labs")
if LABS_DIR not in sys.path:
    sys.path.insert(0, LABS_DIR)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("DAT535_Orchestrator")


def run_medallion(
    env: str = "dev",
    source: str = "synthetic",
    scope: str = "jc",
    month: str = "202409",
) -> Dict[str, Any]:
    """Execute the core Medallion pipeline: Raw Generation/Download, Bronze Ingestion, Silver Cleaning, and Gold Marts."""
    from config.settings import get_env_paths
    from src.bronze.ingest_bronze import run_bronze_ingestion
    from src.gold.build_gold import run_gold_transformation
    from src.silver.clean_quarantine import run_silver_cleaning
    from src.silver.enrich_conform import run_silver_enrichment
    from src.utils.data_generator import generate_raw_batches
    from src.utils.real_data_downloader import download_all_real_data
    from src.utils.spark_session import get_spark_session

    start_time = time.time()
    run_id = f"pipeline_{env}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    paths = get_env_paths(env)

    logger.info("=" * 70)
    logger.info(
        f"STARTING MEDALLION PIPELINE [ENV: {env.upper()}] [SOURCE: {source.upper()}] [RUN_ID: {run_id}]"
    )
    logger.info("=" * 70)

    # 1. Raw Batch Bootstrap (Synthetic or Real)
    if source.lower() == "real":
        logger.info(
            f"Step 1: Downloading real Citi Bike ({scope.upper()}) & NOAA weather data for {month}..."
        )
        gen_info = download_all_real_data(raw_dir=paths["raw_dir"], scope=scope, month=month)
        logger.info(
            f"Real data ready: {gen_info['trips_count']} trips, "
            f"{gen_info['stations_count']} stations, {gen_info['weather_count']} weather records."
        )
    else:
        logger.info("Step 1: Checking raw data landing directory (synthetic generator)...")
        for sub in ("trips", "stations", "weather"):
            sub_dir = os.path.join(paths["raw_dir"], sub)
            if os.path.exists(sub_dir):
                for fname in os.listdir(sub_dir):
                    fpath = os.path.join(sub_dir, fname)
                    if os.path.isfile(fpath):
                        os.remove(fpath)
        gen_info = generate_raw_batches(paths["raw_dir"], num_trips=35000, seed=42)
        logger.info(
            f"Raw data ready: {gen_info['total_trip_rows']} trips, "
            f"{gen_info['stations_count']} stations, {gen_info['weather_rows']} weather records."
        )

    # 2. Spark Session Initialization
    spark = get_spark_session(f"DAT535_Medallion_{env.capitalize()}")

    # 3. Bronze Layer Ingestion
    logger.info("Step 2: Executing Bronze Layer Ingestion...")
    bronze_receipt = run_bronze_ingestion(spark, env=env, run_id=run_id)
    logger.info(
        f"Bronze Ingestion Status: {bronze_receipt['status']} "
        f"(receipt created at {paths['bronze_receipt']}, {bronze_receipt['row_counts']['trips']} trips ingested)."
    )

    # 4. Silver Layer Core Cleaning, Coordinate Imputation, and 7-Rule Quarantine
    logger.info("Step 3: Executing Silver Core Cleaning & Quarantine...")
    silver_metrics = run_silver_cleaning(spark, env=env, run_id=run_id)
    logger.info(
        f"Silver Cleaning Complete: {silver_metrics['valid_cleaned_rows']} valid rows, "
        f"{silver_metrics['quarantined_rows']} quarantined ({silver_metrics['quarantine_rate']*100:.2f}%), "
        f"{silver_metrics['duplicates_removed']} duplicates removed, "
        f"{silver_metrics['imputed_coords_count']} coordinates imputed, "
        f"7-rule quarantine split: {silver_metrics['quarantine_breakdown_by_rule']}."
    )

    # 4b. Silver Conformance & Station Enrichment
    logger.info("Step 3b: Executing Silver Conformance & Station Enrichment...")
    silver_enrichment_metrics = run_silver_enrichment(spark, env=env, run_id=run_id)
    logger.info(
        f"Silver Enrichment Complete: {silver_enrichment_metrics['enriched_rows']} trips conformed at {paths['silver_enriched']}."
    )

    # 5. Gold Layer Marts
    logger.info("Step 4: Executing Gold Layer Marts...")
    gold_metrics = run_gold_transformation(spark, env, run_id)
    logger.info(
        f"Gold Marts Complete: {gold_metrics['station_flow_rows']} station-flow rows, "
        f"{gold_metrics['user_kpi_rows']} user KPI rows, "
        f"{gold_metrics['top_corridor_rows']} top corridors, "
        f"{gold_metrics['weather_ridership_rows']} weather rows."
    )

    elapsed_time = time.time() - start_time
    logger.info(f"Pipeline executed successfully in {elapsed_time:.2f}s.")

    # 6. Publish Summary to GitHub Actions if available
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("## DAT535 Medallion Pipeline Summary\n\n")
            f.write(f"- **Run ID:** `{run_id}`\n")
            f.write(f"- **Environment:** `{env}`\n")
            f.write(f"- **Bronze Schema Validation:** `{bronze_receipt['schema_validation_status']}`\n")
            f.write(f"- **Bronze Trips Ingested:** `{bronze_receipt['row_counts']['trips']}`\n")
            f.write(f"- **Silver Duplicates Removed:** `{silver_metrics['duplicates_removed']}`\n")
            f.write(f"- **Silver Imputed Coordinates:** `{silver_metrics['imputed_coords_count']}`\n")
            f.write(f"- **Silver Valid Cleaned Rows:** `{silver_metrics['valid_cleaned_rows']}`\n")
            f.write(f"- **Silver Quarantined Rows:** `{silver_metrics['quarantined_rows']}` ({silver_metrics['quarantine_rate']*100:.2f}%)\n")
            f.write(f"- **Gold Layer Status:** `{gold_metrics['status']}`\n\n")
            f.write("### Quarantine Breakdown by Rule\n\n")
            f.write("| Rule | Quarantined Rows |\n|---|---:|\n")
            for rule, count in silver_metrics["quarantine_breakdown_by_rule"].items():
                f.write(f"| `{rule}` | {count} |\n")
            f.write("\n### Gold Marts Summary\n\n")
            f.write(f"- Station Rebalancing Records: `{gold_metrics['station_flow_rows']}`\n")
            f.write(f"- User Behavior Records: `{gold_metrics['user_kpi_rows'] + gold_metrics['top_corridor_rows']}`\n")
            f.write(f"- Weather Ridership Records: `{gold_metrics['weather_ridership_rows']}`\n")

    # 5b. Display Visual Data Tables to Console
    print("\n" + "=" * 80)
    print("📊 GOLD ANALYTICAL MARTS & SILVER QUALITY PREVIEW")
    print("=" * 80)

    try:
        from pyspark.sql import functions as F

        # Table 1: User Behavior KPIs
        print("\n🔹 MART 1: USER BEHAVIOR KPIS (Member vs Casual)")
        user_kpis_path = paths["gold_rebalancing"].replace(
            "station_rebalancing_mart", "user_behavior_mart/kpis"
        )
        if os.path.exists(user_kpis_path):
            kpis_df = spark.read.parquet(user_kpis_path)
            kpis_df.select(
                "member_casual",
                "rideable_type",
                "total_trips",
                F.round("avg_duration_min", 1).alias("avg_dur_min"),
                F.round("median_duration_min", 1).alias("med_dur_min"),
                F.round("avg_speed_kmh", 1).alias("speed_kmh"),
                F.round("avg_distance_km", 2).alias("dist_km"),
            ).show(truncate=False)

        # Table 2: Top 5 Corridors
        print("🔹 MART 2: TOP 5 COMMUTER & LEISURE CORRIDORS")
        corridors_path = paths["gold_rebalancing"].replace(
            "station_rebalancing_mart", "user_behavior_mart/top_corridors"
        )
        if os.path.exists(corridors_path):
            corr_df = spark.read.parquet(corridors_path)
            corr_df.filter(F.col("corridor_rank") <= 5).select(
                "member_casual",
                "corridor_rank",
                "start_station_name",
                "end_station_name",
                "trips",
            ).show(truncate=False)

        # Table 3: Weather Ridership Mart
        print("🔹 MART 3: WEATHER IMPACT ON COMMUTE RIDERSHIP")
        if os.path.exists(paths["gold_weather_impact"]):
            weather_df = spark.read.parquet(paths["gold_weather_impact"])
            weather_df.select(
                "temperature_bin",
                "rain_bin",
                "total_trips",
                "hours_with_trips",
                F.round("avg_trips_per_hour", 1).alias("trips_per_hr"),
                F.round("avg_speed_kmh", 1).alias("speed_kmh"),
            ).show(truncate=False)

        # Table 4: Station Rebalancing Top 5
        print("🔹 MART 4: STATION FLOW & REBALANCING (Top 5 Stations by Volume)")
        if os.path.exists(paths["gold_rebalancing"]):
            flow_df = spark.read.parquet(paths["gold_rebalancing"])
            flow_summary = (
                flow_df.withColumn(
                    "station_display",
                    F.coalesce(
                        F.col("station_name"),
                        F.concat(F.lit("Station "), F.col("station_id")),
                    ),
                )
                .groupBy("station_id", "station_display", "region_id")
                .agg(
                    F.sum("outbound_trips").alias("outbound"),
                    F.sum("inbound_trips").alias("inbound"),
                    F.sum("net_flow").alias("net_flow"),
                    F.round(F.avg("turnover_ratio"), 1).alias("avg_turnover"),
                )
                .select(
                    "station_id",
                    F.col("station_display").alias("station_name"),
                    "region_id",
                    "outbound",
                    "inbound",
                    "net_flow",
                    "avg_turnover",
                )
                .orderBy(F.col("outbound").desc())
                .limit(5)
            )
            flow_summary.show(truncate=False)

        # Table 5: Silver Quality Evidence Before vs After
        print("🔹 SILVER QUALITY EVIDENCE (BEFORE VS AFTER CLEANING)")
        comp = silver_metrics.get("before_and_after_comparison", {})
        if comp:
            comp_rows = []
            for metric_key, vals in comp.items():
                if "before_bronze" in vals:
                    comp_rows.append((
                        metric_key.replace("_", " ").title(),
                        str(vals["before_bronze"]),
                        str(vals["after_silver_clean"]),
                        str(vals["action_taken"]),
                    ))
            comp_df = spark.createDataFrame(
                comp_rows,
                ["Quality Check", "Bronze (Before)", "Silver (After)", "Action Taken"],
            )
            comp_df.show(truncate=False)

        print("=" * 80 + "\n")
    except Exception as e:
        logger.warning(f"Could not print visual preview tables: {e}")

    print("Medallion pipeline completed successfully.")
    return {
        "status": "success",
        "run_id": run_id,
        "elapsed_seconds": elapsed_time,
        "bronze": bronze_receipt,
        "silver": silver_metrics,
        "gold": gold_metrics,
    }


def run_lab2() -> Dict[str, Any]:
    """Run archived Lab 2 from labs/ directory."""
    logger.info("Running archived Lab 2 pipeline from labs/...")
    from lab2_pipeline import Lab2Pipeline

    pipeline = Lab2Pipeline()
    return pipeline.run()


def run_lab3() -> Dict[str, Any]:
    """Run archived Lab 3 from labs/ directory."""
    logger.info("Running archived Lab 3 pipeline from labs/...")
    from lab3_pipeline import Lab3Pipeline

    pipeline = Lab3Pipeline()
    return pipeline.run()


def run_all_labs() -> Dict[str, Any]:
    """Run archived Lab 2 & Lab 3 in sequence."""
    logger.info("Running all archived coursework labs...")
    results = {}
    results["lab2"] = run_lab2()
    if results["lab2"].get("status") == "success":
        results["lab3"] = run_lab3()
    else:
        results["lab3"] = {"status": "skipped"}
    return results


def main() -> int:
    """Parse CLI arguments and dispatch pipeline execution."""
    parser = argparse.ArgumentParser(
        description="DAT535 Medallion Pipeline & Lab Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "pipeline",
        nargs="?",
        choices=["medallion", "lab2", "lab3", "all"],
        default=None,
        help="Optional specific pipeline or archived lab to run",
    )
    parser.add_argument(
        "--env",
        default=os.getenv("DAT535_ENV", "dev"),
        choices=["dev", "test", "prod"],
        help="Target environment (default: dev)",
    )
    parser.add_argument(
        "--source",
        default="synthetic",
        choices=["synthetic", "real"],
        help="Data source mode: synthetic generator or real Citi Bike & NOAA data (default: synthetic)",
    )
    parser.add_argument(
        "--scope",
        default="jc",
        choices=["jc", "nyc"],
        help="Scope for real Citi Bike data: jc (Jersey City ~100k trips) or nyc (NYC ~3.5M trips) (default: jc)",
    )
    parser.add_argument(
        "--month",
        default="202409",
        help="Year and month for real data in YYYYMM format (default: 202409)",
    )

    args = parser.parse_args()

    try:
        if args.pipeline == "lab2":
            res = run_lab2()
            return 0 if res.get("status") == "success" else 1
        elif args.pipeline == "lab3":
            res = run_lab3()
            return 0 if res.get("status") == "success" else 1
        elif args.pipeline == "all":
            res = run_all_labs()
            return 0 if all(r.get("status") == "success" for r in res.values()) else 1
        else:
            # Default: run the core Medallion pipeline
            res = run_medallion(
                env=args.env,
                source=args.source,
                scope=args.scope,
                month=args.month,
            )
            return 0 if res.get("status") == "success" else 1
    except Exception as exc:
        logger.error(f"Pipeline execution failed: {exc}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
