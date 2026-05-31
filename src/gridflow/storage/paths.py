"""Path builder for bronze/silver/gold directories.

Encapsulates all filesystem path logic. Transforms never construct paths directly.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date


class PathBuilder:
    """Builds standardised paths for all data layers."""

    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)

    # --- Bronze ---

    def bronze_dir(self, source: str, dataset: str) -> Path:
        return self.data_dir / "bronze" / source / dataset

    def bronze_date_dir(self, source: str, dataset: str, target_date: date) -> Path:
        return (
            self.bronze_dir(source, dataset)
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )

    # --- Silver ---

    def silver_dir(self, source: str, dataset: str) -> Path:
        return self.data_dir / "silver" / source / dataset

    def silver_partition_dir(self, source: str, dataset: str, target_date: date) -> Path:
        return (
            self.silver_dir(source, dataset)
            / f"year={target_date.year}"
            / f"month={target_date.month:02d}"
        )

    def silver_file(self, source: str, dataset: str, target_date: date) -> Path:
        filename = f"{dataset}_{target_date.strftime('%Y%m%d')}.parquet"
        return self.silver_partition_dir(source, dataset, target_date) / filename

    def silver_glob_pattern(self, source: str, dataset: str) -> str:
        """Return a glob pattern for all silver parquet files."""
        return str(self.silver_dir(source, dataset) / "**" / "*.parquet")

    # --- Gold ---

    def gold_dir(self, gold_dataset: str) -> Path:
        return self.data_dir / "gold" / gold_dataset

    def gold_partition_dir(self, gold_dataset: str, target_date: date) -> Path:
        return self.gold_dir(gold_dataset) / f"year={target_date.year}"

    def gold_file(self, gold_dataset: str, target_date: date) -> Path:
        filename = f"{gold_dataset}_{target_date.strftime('%Y%m%d')}.parquet"
        return self.gold_partition_dir(gold_dataset, target_date) / filename

    def gold_glob_pattern(self, gold_dataset: str) -> str:
        """Return a glob pattern for all gold parquet files."""
        return str(self.gold_dir(gold_dataset) / "**" / "*.parquet")

    # --- DuckDB ---

    def duckdb_path(self) -> Path:
        return self.data_dir / "gridflow.duckdb"
