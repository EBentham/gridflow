"""Gold builder for merit order / power stack dataset (placeholder)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from gridflow.gold.base import BaseGoldBuilder


class MeritOrderBuilder(BaseGoldBuilder):
    """Build merit order gold dataset (Phase 3 — placeholder)."""

    gold_dataset = "merit_order"

    def build(self, start_date: date, end_date: date) -> pl.DataFrame:
        # Placeholder for Phase 3 implementation
        return pl.DataFrame()
