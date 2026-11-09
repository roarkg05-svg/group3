"""Utility helpers for SparkSession lifecycle and synthetic raw batch generation."""

from src.utils.spark_session import get_spark_session

try:
    from src.utils.data_generator import generate_raw_batches
except ImportError:  # pragma: no cover - before Step 2 commit
    generate_raw_batches = None  # type: ignore[assignment]

__all__ = ["get_spark_session", "generate_raw_batches"]
