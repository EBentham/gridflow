"""Gold builder for demand features dataset (placeholder)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from gridflow.gold.base import BaseGoldBuilder


class DemandFeaturesBuilder(BaseGoldBuilder):
    """Build demand features gold dataset (Phase 3 — placeholder)."""

    gold_dataset = "demand_features"

    def build(self, start_date: date, end_date: date) -> pl.DataFrame:
        # Placeholder for Phase 3 implementation
        return pl.DataFrame()
