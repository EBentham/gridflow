"""Silver transformer for GIE ALSI LNG terminal data."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class LNGTerminalTransformer(BaseSilverTransformer):
    """Transform GIE ALSI LNG terminal data from bronze to silver."""

    source = "gie_alsi"
    dataset = "lng"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.json")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                payload = json.loads(f.read_text())
                records = payload.get("data", []) if isinstance(payload, dict) else []
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as exc:
                logger.warning("Failed to parse GIE ALSI bronze file %s: %s", f, exc)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["gasDayStart"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in GIE ALSI: %s", missing)
            return pl.DataFrame()

        df = raw_df

        # Parse gas_day
        df = df.with_columns(
            pl.col("gasDayStart")
            .str.to_date(format="%Y-%m-%d", strict=False)
            .alias("gas_day")
        )

        # ALSI field names (may differ slightly from AGSI)
        rename_map: dict[str, str] = {}
        field_map = {
            "lngInventory": "lng_in_storage_gwh",
            "gasInStorage": "lng_in_storage_gwh",  # fallback name
            "sendOut": "send_out_gwh",
            "withdrawal": "send_out_gwh",  # fallback
            "injection": "injection_gwh",
            "dtrs": "dtrs_pct_full",
            "full": "dtrs_pct_full",  # fallback
            "trend": "trend",
            "code": "country_code",
            "name": "country_name",
        }
        for src_col, dst_col in field_map.items():
            # Only map if source exists and destination not yet set
            if src_col in df.columns and dst_col not in rename_map.values():
                rename_map[src_col] = dst_col

        if rename_map:
            df = df.rename(rename_map)

        float_cols = [
            "lng_in_storage_gwh", "send_out_gwh", "injection_gwh",
            "dtrs_pct_full", "trend",
        ]
        for col in float_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8).cast(pl.Float64, strict=False)
                )

        if "country_code" not in df.columns and "countryCode" in df.columns:
            df = df.rename({"countryCode": "country_code"})

        dedup_cols = ["gas_day"]
        if "country_code" in df.columns:
            dedup_cols = ["gas_day", "country_code"]
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("gie_alsi").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "gas_day", "country_code", "country_name",
            "lng_in_storage_gwh", "send_out_gwh", "injection_gwh",
            "dtrs_pct_full", "trend",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        sort_cols = ["gas_day"]
        if "country_code" in df.columns:
            sort_cols.append("country_code")
        return df.select(available).sort(sort_cols)


register_transformer("gie_alsi", "lng", LNGTerminalTransformer)
