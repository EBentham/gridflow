"""Gold builder for demand features dataset (placeholder)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from gridflow.gold.base import BaseGoldBuilder

if TYPE_CHECKING:
    from datetime import date


class DemandFeaturesBuilder(BaseGoldBuilder):
    """Build demand features gold dataset (Phase 3 — placeholder)."""

    gold_dataset = "demand_features"

    def build(self, start_date: date, end_date: date) -> pl.DataFrame:
        # Placeholder for Phase 3 implementation
        return pl.DataFrame()
