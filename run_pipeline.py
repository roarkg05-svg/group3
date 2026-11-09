#!/usr/bin/env python3
"""DAT535 Medallion Architecture Pipeline Orchestrator.

Executes the end-to-end CityBikes Medallion pipeline (Bronze Ingestion,
Silver Core Cleaning & Quarantine, and scaffolded Gold Marts).

Usage:
    python run_pipeline.py --env dev
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


def run_medallion(env: str = "dev") -> Dict[str, Any]:
    """Execute the core Medallion pipeline: Raw Generation, Bronze Ingestion, Silver Cleaning, and Gold Marts."""
    from config.settings import get_env_paths
    from src.bronze.ingest_bronze import run_bronze_ingestion
    from src.silver.clean_quarantine import run_silver_cleaning
    from src.utils.data_generator import generate_raw_batches
    from src.utils.spark_session import get_spark_session

    start_time = time.time()
    run_id = f"pipeline_{env}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    paths = get_env_paths(env)

    logger.info("=" * 70)
    logger.info(f"STARTING MEDALLION PIPELINE [ENV: {env.upper()}] [RUN_ID: {run_id}]")
    logger.info("=" * 70)

    # 1. Raw Batch Bootstrap
    logger.info("Step 1: Checking raw data landing directory...")
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
        f"({bronze_receipt['row_counts']['trips']} trips ingested)."
    )

    # 4. Silver Layer Core Cleaning, Coordinate Imputation, and 7-Rule Quarantine
    logger.info("Step 3: Executing Silver Core Cleaning & Quarantine...")
    silver_metrics = run_silver_cleaning(spark, env=env, run_id=run_id)
    logger.info(
        f"Silver Cleaning Complete: {silver_metrics['valid_cleaned_rows']} valid rows, "
        f"{silver_metrics['quarantined_rows']} quarantined ({silver_metrics['quarantine_rate']*100:.2f}%), "
        f"{silver_metrics['imputed_coords_count']} coordinates imputed."
    )

    # 5. Gold Layer Execution (Scaffolded for Oriol)
    logger.info("Step 4: Checking Gold Layer Marts...")
    gold_status = "SCAFFOLDED"
    try:
        from src.gold import run_gold_pipeline  # type: ignore

        gold_metrics = run_gold_pipeline(spark, env=env, run_id=run_id)
        gold_status = "COMPLETED"
        logger.info(f"Gold marts produced: {gold_metrics}")
    except (ImportError, AttributeError):
        logger.info("Gold layer analytics currently scaffolded (pending Oriol's implementation).")

    elapsed_time = time.time() - start_time
    logger.info(f"Pipeline executed successfully in {elapsed_time:.2f}s.")

    # 6. Publish Summary to GitHub Actions if available
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("## DAT535 Dev CI - Bronze & Silver Core Summary\n\n")
            f.write(f"- **Run ID:** `{run_id}`\n")
            f.write(f"- **Environment:** `{env}`\n")
            f.write(f"- **Bronze Schema Validation:** `{bronze_receipt['schema_validation_status']}`\n")
            f.write(f"- **Bronze Trips Ingested:** `{bronze_receipt['row_counts']['trips']}`\n")
            f.write(f"- **Silver Duplicates Removed:** `{silver_metrics['duplicates_removed']}`\n")
            f.write(f"- **Silver Imputed Coordinates:** `{silver_metrics['imputed_coords_count']}`\n")
            f.write(f"- **Silver Valid Cleaned Rows:** `{silver_metrics['valid_cleaned_rows']}`\n")
            f.write(f"- **Silver Quarantined Rows:** `{silver_metrics['quarantined_rows']}` ({silver_metrics['quarantine_rate']*100:.2f}%)\n")
            f.write(f"- **Gold Layer Status:** `{gold_status}`\n\n")
            f.write("### Quarantine Breakdown by Rule\n\n")
            f.write("| Rule | Quarantined Rows |\n|---|---:|\n")
            for rule, count in silver_metrics["quarantine_breakdown_by_rule"].items():
                f.write(f"| `{rule}` | {count} |\n")

    print("Bronze + Silver Core verification completed successfully.")
    return {
        "status": "success",
        "run_id": run_id,
        "elapsed_seconds": elapsed_time,
        "bronze": bronze_receipt,
        "silver": silver_metrics,
        "gold_status": gold_status,
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
            res = run_medallion(env=args.env)
            return 0 if res.get("status") == "success" else 1
    except Exception as exc:
        logger.error(f"Pipeline execution failed: {exc}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
