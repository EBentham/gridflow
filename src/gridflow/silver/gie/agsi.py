"""Silver transformer for GIE AGSI+ gas storage data."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


def _safe_float(val: Any) -> float | None:
    """Parse a string or numeric value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


class GasStorageTransformer(BaseSilverTransformer):
    """Transform GIE AGSI+ gas storage data from bronze to silver."""

    source = "gie_agsi"
    dataset = "storage"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.json")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                payload = json.loads(f.read_text())
                # GIE API wraps records in a "data" array
                records = payload.get("data", []) if isinstance(payload, dict) else []
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as exc:
                logger.warning("Failed to parse GIE AGSI bronze file %s: %s", f, exc)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        # GIE field names (camelCase from API)
        required = ["gasDayStart"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in GIE AGSI: %s", missing)
            return pl.DataFrame()

        df = raw_df

        # Parse gas_day
        df = df.with_columns(
            pl.col("gasDayStart")
            .str.to_date(format="%Y-%m-%d", strict=False)
            .alias("gas_day")
        )

        # Map columns
        rename_map: dict[str, str] = {}
        field_map = {
            "gasInStorage": "gas_in_storage_gwh",
            "withdrawal": "withdrawal_gwh",
            "injection": "injection_gwh",
            "workingGasVolume": "working_gas_volume_gwh",
            "full": "storage_pct_full",
            "trend": "trend",
            "code": "country_code",
            "name": "country_name",
        }
        for src_col, dst_col in field_map.items():
            if src_col in df.columns:
                rename_map[src_col] = dst_col

        if rename_map:
            df = df.rename(rename_map)

        # Cast numeric columns (GIE returns strings)
        float_cols = [
            "gas_in_storage_gwh", "withdrawal_gwh", "injection_gwh",
            "working_gas_volume_gwh", "storage_pct_full", "trend",
        ]
        for col in float_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8).cast(pl.Float64, strict=False)
                )

        # Ensure country_code exists
        if "country_code" not in df.columns and "countryCode" in df.columns:
            df = df.rename({"countryCode": "country_code"})

        dedup_cols = ["gas_day"]
        if "country_code" in df.columns:
            dedup_cols = ["gas_day", "country_code"]
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("gie_agsi").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "gas_day", "country_code", "country_name",
            "gas_in_storage_gwh", "withdrawal_gwh", "injection_gwh",
            "working_gas_volume_gwh", "storage_pct_full", "trend",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        sort_cols = ["gas_day"]
        if "country_code" in df.columns:
            sort_cols.append("country_code")
        return df.select(available).sort(sort_cols)


register_transformer("gie_agsi", "storage", GasStorageTransformer)
