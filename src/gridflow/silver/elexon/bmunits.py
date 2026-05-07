"""Silver transformer for Elexon BM Unit reference data."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.storage.parquet import write_parquet

logger = logging.getLogger(__name__)


class BMUnitsTransformer(BaseSilverTransformer):
    """Transform Elexon BM Unit reference data from bronze to silver.

    This is reference data (no date dimension), so it writes a single
    file rather than date-partitioned Parquet.

    Note on timestamps: ``available_at`` is the authoritative bitemporal
    publication timestamp added by ``BaseSilverTransformer``; ``ingested_at``
    is retained for backward compatibility as the local processing
    timestamp. Under ``--reingest`` the two diverge.
    """

    source = "elexon"
    dataset = "bmunits_reference"
    DATASET_VERSION: ClassVar[str] = "1.0.0"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        # Reference data has no date partitioning; read latest file from any date dir
        if not self.bronze_dir.exists():
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        # Search all date directories for the most recent file
        for f in sorted(self.bronze_dir.rglob("raw_*.json"), reverse=True):
            if f.name.endswith(".meta.json"):
                continue
            try:
                data = json.loads(f.read_text())
                records = data.get("data", []) if isinstance(data, dict) else data
                rows.extend(records)
                break  # Use only the most recent file
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse bronze file {f}: {e}")
                continue

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, infer_schema_length=None)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "bmUnit": "bm_unit_id",
            "elexonBmUnit": "bm_unit_id",
            "name": "bm_unit_name",
            "bmUnitName": "bm_unit_name",
            "fuelType": "fuel_type",
            "registeredCapacity": "registered_capacity_mw",
            "generationCapacity": "registered_capacity_mw",
            "companyName": "company_name",
            "leadPartyName": "company_name",
            "gspGroupId": "gsp_group_id",
            "nationalGridBmUnit": "national_grid_bm_unit",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        if "bm_unit_id" not in raw_df.columns:
            logger.error("Missing required column 'bm_unit_id' in BMUnits")
            return pl.DataFrame()

        df = raw_df.with_columns(pl.col("bm_unit_id").cast(pl.Utf8))

        if "registered_capacity_mw" in df.columns:
            df = df.with_columns(pl.col("registered_capacity_mw").cast(pl.Float64))

        df = df.unique(subset=["bm_unit_id"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("elexon").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "bm_unit_id", "bm_unit_name", "fuel_type",
            "registered_capacity_mw", "company_name",
            "gsp_group_id", "national_grid_bm_unit",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("bm_unit_id")

    def _write_silver(
        self,
        df: pl.DataFrame,
        target_date: date,
        available_at: datetime,
    ) -> None:
        """Override: write a single reference file (not date-partitioned)."""
        final_path = self.silver_dir / "bmunits_reference.parquet"
        write_parquet(df, final_path)


register_transformer("elexon", "bmunits_reference", BMUnitsTransformer)
