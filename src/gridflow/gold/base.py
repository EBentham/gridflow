"""Abstract base class for gold-layer dataset builders."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import polars as pl

from gridflow.storage.parquet import write_parquet

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

logger = logging.getLogger(__name__)


class BaseGoldBuilder(ABC):
    """Base class for gold-layer dataset builders.

    Gold datasets are rebuilt from silver. They are not incrementally updated.
    """

    gold_dataset: str

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.gold_dir = data_dir / "gold" / self.gold_dataset

    @abstractmethod
    def build(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Build the gold dataset for a date range from silver data.

        Returns the built DataFrame.
        """
        ...

    def run(self, start_date: date, end_date: date) -> int:
        """Execute the gold build for a date range.

        Returns the total number of rows written.
        """
        df = self.build(start_date, end_date)
        if df.is_empty():
            logger.warning(f"Gold build produced 0 rows for {self.gold_dataset}")
            return 0

        total_rows = 0

        # Write one file per date
        if "settlement_date" in df.columns:
            date_col = "settlement_date"
        elif "date" in df.columns:
            date_col = "date"
        else:
            # Write as single file
            out_path = self.gold_dir / f"{self.gold_dataset}.parquet"
            write_parquet(df, out_path)
            logger.info(f"Gold write: {self.gold_dataset} -> {len(df)} rows (single file)")
            return len(df)

        for target_date in df[date_col].unique().sort().to_list():
            day_df = df.filter(pl.col(date_col) == target_date)
            out_dir = self.gold_dir / f"year={target_date.year}"
            filename = f"{self.gold_dataset}_{target_date.strftime('%Y%m%d')}.parquet"
            write_parquet(day_df, out_dir / filename)
            total_rows += len(day_df)

        logger.info(
            f"Gold write: {self.gold_dataset} {start_date} to {end_date} -> {total_rows} rows"
        )
        return total_rows
