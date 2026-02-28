"""Abstract base class for silver-layer transformers."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

import polars as pl

from gridflow.storage.parquet import write_parquet

logger = logging.getLogger(__name__)


class BaseSilverTransformer(ABC):
    """Base class for bronze -> silver transformations.

    Subclasses implement:
    - source: the data source name
    - dataset: the dataset name
    - read_bronze(): read and parse raw bronze files
    - transform(): apply normalisation, validation, deduplication
    """

    source: str
    dataset: str

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.bronze_dir = data_dir / "bronze" / self.source / self.dataset
        self.silver_dir = data_dir / "silver" / self.source / self.dataset

    @abstractmethod
    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Read and parse all bronze files for a given date.
        Returns a raw DataFrame before validation."""
        ...

    @abstractmethod
    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Apply source-specific normalisation, validation, and deduplication.
        Returns a clean DataFrame matching the silver schema."""
        ...

    def run(self, target_date: date) -> int:
        """Execute the full bronze -> silver pipeline for one date.

        Returns the number of rows written.
        """
        # Read bronze
        raw_df = self.read_bronze(target_date)
        if raw_df.is_empty():
            logger.warning(f"No bronze data for {self.source}/{self.dataset} on {target_date}")
            return 0

        # Transform
        clean_df = self.transform(raw_df)
        if clean_df.is_empty():
            logger.warning(f"Transform produced 0 rows for {target_date}")
            return 0

        # Write silver (atomic: write to temp, then rename)
        self._write_silver(clean_df, target_date)

        logger.info(
            f"Silver write: {self.source}/{self.dataset} {target_date} -> {len(clean_df)} rows"
        )
        return len(clean_df)

    def _write_silver(self, df: pl.DataFrame, target_date: date) -> None:
        """Write DataFrame to partitioned Parquet, replacing existing file."""
        out_dir = self.silver_dir / f"year={target_date.year}" / f"month={target_date.month:02d}"
        filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}.parquet"
        final_path = out_dir / filename
        write_parquet(df, final_path)
