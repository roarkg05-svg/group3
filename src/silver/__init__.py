"""Silver layer cleaning, quarantine, and conformance package."""

from src.silver.clean_quarantine import run_silver_cleaning

try:
    from src.silver.enrich_conform import run_silver_enrichment
except ImportError:  # pragma: no cover - added in Part 2 (Oriol's half)
    run_silver_enrichment = None  # type: ignore[assignment]

__all__ = ["run_silver_cleaning", "run_silver_enrichment"]
