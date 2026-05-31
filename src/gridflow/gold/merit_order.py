"""Gold builder for merit order / power stack dataset (placeholder)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from gridflow.gold.base import BaseGoldBuilder

if TYPE_CHECKING:
    from datetime import date


class MeritOrderBuilder(BaseGoldBuilder):
    """Build merit order gold dataset (Phase 3 — placeholder)."""

    gold_dataset = "merit_order"

    def build(self, start_date: date, end_date: date) -> pl.DataFrame:
        # Placeholder for Phase 3 implementation
        return pl.DataFrame()
