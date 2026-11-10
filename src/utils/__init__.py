"""Utility helpers for SparkSession lifecycle, synthetic raw batch generation, and real data download."""

from src.utils.spark_session import get_spark_session

try:
    from src.utils.data_generator import generate_raw_batches
except ImportError:  # pragma: no cover - before Step 2 commit
    generate_raw_batches = None  # type: ignore[assignment]

try:
    from src.utils.real_data_downloader import download_all_real_data
except ImportError:  # pragma: no cover
    download_all_real_data = None  # type: ignore[assignment]

__all__ = ["get_spark_session", "generate_raw_batches", "download_all_real_data"]

