"""Abstract base class for silver-layer transformers."""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
from pathlib import Path
from typing import ClassVar

import polars as pl

from gridflow.storage.parquet import write_parquet
from gridflow.utils.time import settlement_period_to_utc

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
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = ()

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

    def run(
        self,
        target_date: date,
        run_id: str | None = None,
        reingest: bool = False,
    ) -> int:
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

        resolved_run_id = run_id or f"adhoc-{datetime.now(UTC).isoformat()}"
        available_at = (
            self._available_at_from_bronze(target_date)
            if reingest
            else datetime.now(UTC)
        )
        clean_df = self._add_bitemporal_columns(
            clean_df,
            target_date=target_date,
            run_id=resolved_run_id,
            available_at=available_at,
        )

        # Write silver (atomic: write to temp, then rename)
        self._write_silver(clean_df, target_date)
        self._write_csv(clean_df, target_date)

        logger.info(
            f"Silver write: {self.source}/{self.dataset} {target_date} -> {len(clean_df)} rows"
        )
        return len(clean_df)

    def _add_bitemporal_columns(
        self,
        df: pl.DataFrame,
        target_date: date,
        run_id: str,
        available_at: datetime,
    ) -> pl.DataFrame:
        """Add modelling lineage columns before silver output is persisted."""
        if available_at.tzinfo is None:
            available_at = available_at.replace(tzinfo=UTC)
        else:
            available_at = available_at.astimezone(UTC)

        return df.with_columns(
            [
                self._event_time_expr(df, target_date),
                pl.lit(available_at).cast(pl.Datetime("us", "UTC")).alias("available_at"),
                pl.lit(run_id).alias("source_run_id"),
                pl.lit(self.DATASET_VERSION).alias("dataset_version"),
            ]
        )

    def _event_time_expr(self, df: pl.DataFrame, target_date: date) -> pl.Expr:
        """Return the expression used for the row's semantic event time."""
        column = self._event_time_column()
        if column in df.columns:
            return pl.col(column).cast(pl.Datetime("us", "UTC")).alias("event_time")

        if {"settlement_date", "settlement_period"}.issubset(df.columns):
            return (
                pl.struct(["settlement_date", "settlement_period"])
                .map_elements(
                    lambda row: settlement_period_to_utc(
                        row["settlement_date"],
                        row["settlement_period"],
                    ),
                    return_dtype=pl.Datetime("us", "UTC"),
                )
                .alias("event_time")
            )

        logger.debug(
            "Falling back to target-date event_time for %s/%s on %s",
            self.source,
            self.dataset,
            target_date,
        )
        return (
            pl.lit(datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC))
            .cast(pl.Datetime("us", "UTC"))
            .alias("event_time")
        )

    def _event_time_column(self) -> str:
        """Name of the transform output column that represents event time."""
        return "timestamp_utc"

    def _available_at_from_bronze(self, target_date: date) -> datetime:
        """Reconstruct historical availability from bronze sidecar metadata."""
        timestamps: list[datetime] = []
        for date_dir in self._bronze_date_dirs(target_date):
            for meta_path in sorted(date_dir.glob("raw_*.meta.json")):
                timestamp = self._timestamp_from_sidecar(meta_path)
                if timestamp is not None:
                    timestamps.append(timestamp)

        if timestamps:
            return max(timestamps)

        fallback = datetime.now(UTC)
        logger.warning(
            "No bronze sidecar timestamp found for %s/%s on %s; using %s",
            self.source,
            self.dataset,
            target_date,
            fallback.isoformat(),
        )
        return fallback

    def _bronze_date_dirs(self, target_date: date) -> list[Path]:
        """Candidate bronze date directories for this dataset/date."""
        suffix = (
            Path(str(target_date.year))
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        candidates = [self.bronze_dir / suffix]

        # Some aggregate transformers read from explicit sibling partitions, e.g.
        # open_meteo/historical reads bronze/open_meteo/historical_london.
        parent = self.bronze_dir.parent
        if parent.exists():
            for sibling_dataset in self.BRONZE_SIBLING_DATASETS:
                candidates.append(parent / sibling_dataset / suffix)

        return [path for path in candidates if path.exists()]

    @staticmethod
    def _timestamp_from_sidecar(meta_path: Path) -> datetime | None:
        """Extract the most useful timestamp field from a bronze sidecar."""
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Failed to parse bronze sidecar %s: %s", meta_path, exc)
            return None

        for key in ("available_at", "response_received_at", "fetched_at", "written_at"):
            raw_value = meta.get(key)
            if raw_value:
                timestamp = BaseSilverTransformer._parse_timestamp(raw_value)
                if timestamp is not None:
                    return timestamp
        return None

    @staticmethod
    def _parse_timestamp(raw_value: object) -> datetime | None:
        if isinstance(raw_value, datetime):
            value = raw_value
        elif isinstance(raw_value, str):
            normalized = raw_value.replace("Z", "+00:00")
            try:
                value = datetime.fromisoformat(normalized)
            except ValueError:
                logger.warning("Could not parse bronze sidecar timestamp: %s", raw_value)
                return None
        else:
            return None

        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _write_silver(self, df: pl.DataFrame, target_date: date) -> None:
        """Write DataFrame to partitioned Parquet, replacing existing file."""
        out_dir = self.silver_dir / f"year={target_date.year}" / f"month={target_date.month:02d}"
        filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}.parquet"
        final_path = out_dir / filename
        write_parquet(df, final_path)

    def _write_csv(self, df: pl.DataFrame, target_date: date) -> None:
        """Write DataFrame to CSV at data/silver/{source}/{dataset}/{dataset}_{YYYYMMDD}.csv."""
        csv_dir = self.silver_dir
        csv_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}.csv"
        final_path = csv_dir / filename
        tmp_path = csv_dir / f".tmp_{filename}"
        df.write_csv(tmp_path)
        os.replace(tmp_path, final_path)
        logger.debug(f"Wrote CSV: {len(df)} rows to {final_path}")
