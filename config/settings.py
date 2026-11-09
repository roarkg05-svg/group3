"""Global configuration, path resolution, geographic bounds, and data quality thresholds."""

import os
from typing import Dict

_FALLBACK_BASE_DIR = "/tmp/spark-lab-data"
DEFAULT_BASE_DIR: str = os.getenv("DAT535_DATA_DIR", _FALLBACK_BASE_DIR)

NYC_BOUNDS: Dict[str, float] = {
    "lat_min": 40.45,
    "lat_max": 40.95,
    "lng_min": -74.30,
    "lng_max": -73.65,
}

QUALITY_THRESHOLDS: Dict[str, float] = {
    "min_duration_min": 1.0,
    "max_duration_min": 1440.0,
    "max_classic_speed_kmh": 45.0,
    "max_electric_speed_kmh": 55.0,
    "max_quarantine_rate": 0.25,
}


def get_env_paths(env: str = "dev") -> Dict[str, str]:
    """Return canonical Medallion storage paths for the target environment (`dev` or `prod`)."""
    target_env = (env or os.getenv("DAT535_ENV", "dev")).strip()
    env_override = os.getenv("DAT535_DATA_DIR")
    if env_override:
        base = env_override.rstrip("/\\")
    else:
        base = DEFAULT_BASE_DIR.rstrip("/\\")

    return {
        "raw_dir": f"{base}/{target_env}/raw_landing",
        "bronze_trips": f"{base}/{target_env}/bronze/trips",
        "bronze_stations": f"{base}/{target_env}/bronze/stations",
        "bronze_weather": f"{base}/{target_env}/bronze/weather",
        "bronze_receipt": f"{base}/{target_env}/bronze/_delivery_receipt.json",
        "silver_cleaned": f"{base}/{target_env}/silver/trips_cleaned",
        "silver_quarantine": f"{base}/{target_env}/silver/trips_quarantine",
        "silver_enriched": f"{base}/{target_env}/silver/trips_enriched",
        "silver_quality_report": f"{base}/{target_env}/silver/_quality_report.json",
        "gold_rebalancing": f"{base}/{target_env}/gold/station_rebalancing_mart",
        "gold_weather_impact": f"{base}/{target_env}/gold/weather_commute_impact",
        "gold_ready_signal": f"{base}/{target_env}/gold/_READY_SIGNAL.json",
    }
