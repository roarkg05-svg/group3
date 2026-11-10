"""SparkSession factory configured for the DAT535 self-hosted Linux runner."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DAT535_CitiBike_Medallion") -> SparkSession:
    """Create or retrieve a configured SparkSession for Medallion pipeline workloads."""
    from pyspark.sql import SparkSession

    master_url = os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Ensure runtime SQL configurations are enforced even when reusing an active session
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sparkContext.setLogLevel("WARN")
    return spark
