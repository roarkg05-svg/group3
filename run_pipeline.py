#!/usr/bin/env python3
"""
DAT535 Lab Pipeline Orchestrator
=================================

Run individual labs or all labs in sequence.

Usage:
    python run_pipeline.py lab2          # Run Lab 2 only
    python run_pipeline.py lab3          # Run Lab 3 only
    python run_pipeline.py all           # Run all labs (Lab 2 then Lab 3)
    python run_pipeline.py --help        # Show help
"""

import sys
import argparse
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_lab2():
    """Run Lab 2: Spark Fundamentals & the Medallion Architecture."""
    logger.info("=" * 60)
    logger.info("RUNNING LAB 2: SPARK FUNDAMENTALS & MEDALLION ARCHITECTURE")
    logger.info("=" * 60)
    
    from lab2_pipeline import Lab2Pipeline
    pipeline = Lab2Pipeline()
    return pipeline.run()


def run_lab3():
    """Run Lab 3: Advanced Spark & Production Patterns.

    Requires Lab 2 to have already run, since Lab 3 loads the shared Silver-layer
    dataset that Lab 2 produces.
    """
    logger.info("=" * 60)
    logger.info("RUNNING LAB 3: ADVANCED SPARK & PRODUCTION PATTERNS")
    logger.info("=" * 60)
    
    from lab3_pipeline import Lab3Pipeline
    pipeline = Lab3Pipeline()
    return pipeline.run()


def run_medallion(env="dev"):
    """Run Medallion pipeline (Bronze + Silver Core) for the given environment."""
    import os
    from datetime import datetime, timezone
    from config.settings import get_env_paths
    from src.utils.spark_session import get_spark_session
    from src.utils.data_generator import generate_raw_batches
    from src.bronze.ingest_bronze import run_bronze_ingestion
    from src.silver.clean_quarantine import run_silver_cleaning

    run_id = f"pipeline_{env}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    paths = get_env_paths(env)

    logger.info(f"Generating raw batches for environment '{env}'...")
    generate_raw_batches(paths["raw_dir"], num_trips=35000, seed=42)

    spark = get_spark_session(f"DAT535_Medallion_{env.capitalize()}")
    logger.info("Running Bronze ingestion...")
    bronze_receipt = run_bronze_ingestion(spark, env=env, run_id=run_id)

    logger.info("Running Silver core cleaning & quarantine...")
    silver_metrics = run_silver_cleaning(spark, env=env, run_id=run_id)

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
            f.write(f"- **Silver Quarantined Rows:** `{silver_metrics['quarantined_rows']}` ({silver_metrics['quarantine_rate']*100:.2f}%)\n\n")
            f.write("### Quarantine Breakdown by Rule\n\n")
            for rule, count in silver_metrics["quarantine_breakdown_by_rule"].items():
                f.write(f"| `{rule}` | {count} |\n")
    print("Bronze + Silver Core verification completed successfully.")
    return {'status': 'success'}


def run_all():
    """Run all labs in sequence (Lab 2 must complete before Lab 3)."""
    logger.info("=" * 60)
    logger.info("RUNNING ALL LABS")
    logger.info("=" * 60)
    
    results = {}
    total_start = time.time()
    
    # Lab 2 (generates and saves the shared dataset used by Lab 3)
    try:
        results['lab2'] = run_lab2()
        logger.info("✓ Lab 2 completed successfully")
    except Exception as e:
        logger.error(f"✗ Lab 2 failed: {e}")
        results['lab2'] = {'status': 'failed', 'error': str(e)}
    
    # Lab 3 (only attempt if Lab 2 succeeded, since it depends on Lab 2's output)
    if results['lab2'].get('status') == 'success':
        try:
            results['lab3'] = run_lab3()
            logger.info("✓ Lab 3 completed successfully")
        except Exception as e:
            logger.error(f"✗ Lab 3 failed: {e}")
            results['lab3'] = {'status': 'failed', 'error': str(e)}
    else:
        logger.warning("Skipping Lab 3 because Lab 2 did not complete successfully")
        results['lab3'] = {'status': 'skipped'}
    
    total_time = time.time() - total_start
    
    # Summary
    logger.info("=" * 60)
    logger.info("ALL LABS SUMMARY")
    logger.info("=" * 60)
    
    for lab, result in results.items():
        status = result.get('status', 'unknown')
        elapsed = result.get('elapsed_time', 0)
        logger.info(f"  {lab}: {status} ({elapsed:.2f}s)")
    
    logger.info(f"  Total time: {total_time:.2f}s")
    
    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="DAT535 Lab Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_pipeline.py lab2          # Run Lab 2 only
    python run_pipeline.py lab3          # Run Lab 3 only
    python run_pipeline.py all           # Run all labs

Labs:
    lab2    Spark Fundamentals & Medallion Architecture - DataFrame/RDD/SQL
            conversions, MapReduce, I/O, Bronze/Silver/Gold layers
    lab3    Advanced Spark & Production Patterns - window functions, joins,
            partitioning, caching, UDFs, streaming, optimization
            (requires Lab 2's shared dataset to have been generated first)
        """
    )
    
    parser.add_argument(
        'pipeline',
        nargs='?',
        choices=['lab2', 'lab3', 'all', 'medallion'],
        default=None,
        help='Pipeline to run'
    )
    parser.add_argument(
        '--env',
        default='dev',
        help='Environment name (e.g. dev, prod, test)'
    )
    
    args = parser.parse_args()
    
    # Dispatch to appropriate pipeline
    pipeline_map = {
        'lab2': run_lab2,
        'lab3': run_lab3,
        'all': run_all,
        'medallion': lambda: run_medallion(args.env),
    }
    
    try:
        if args.pipeline in ('lab2', 'lab3', 'all'):
            result = pipeline_map[args.pipeline]()
        else:
            result = run_medallion(args.env)
        
        # Check status
        if args.pipeline == 'all':
            all_success = all(
                r.get('status') == 'success' 
                for r in result.values()
            )
            return 0 if all_success else 1
        else:
            return 0 if result.get('status') == 'success' else 1
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())