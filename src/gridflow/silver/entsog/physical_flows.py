"""Silver transformer for ENTSO-G physical gas flows."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)

# Unit normalisation to GWh/day
# ENTSO-G commonly returns kWh/d; divide by 1e6 → GWh/d
# Some endpoints return kWh/h; multiply by 24 then divide by 1e6
_KWH_D_TO_GWH_D = 1e-6
_KWH_H_TO_GWH_D = 24.0 * 1e-6


def _normalise_to_gwh_day(value: float, unit: str) -> float:
    """Normalise a gas flow value to GWh/day."""
    unit_lower = unit.lower().strip()
    if "kwh/h" in unit_lower or "kwh/hr" in unit_lower:
        return value * _KWH_H_TO_GWH_D
    # Default: assume kWh/d
    return value * _KWH_D_TO_GWH_D


class PhysicalFlowsTransformer(BaseSilverTransformer):
    """Transform ENTSO-G physical flow JSON from bronze to silver."""

    source = "entsog"
    dataset = "physical_flows"

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
                # ENTSO-G wraps records in "operationalData" array
                records = (
                    payload.get("operationalData", [])
                    if isinstance(payload, dict)
                    else []
                )
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as exc:
                logger.warning(
                    "Failed to parse ENTSO-G bronze file %s: %s", f, exc
                )

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["periodFrom", "pointKey"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(
                "Missing required columns in ENTSO-G physical flows: %s", missing
            )
            return pl.DataFrame()

        df = raw_df

        # Filter to Physical Flow indicator only (if indicator column present)
        if "indicator" in df.columns:
            df = df.filter(pl.col("indicator") == "Physical Flow")

        if df.is_empty():
            return pl.DataFrame()

        # Parse periodFrom as UTC timestamp
        df = df.with_columns(
            pl.col("periodFrom")
            .str.to_datetime(
                format="%Y-%m-%d %H:%M:%S",
                time_unit="us",
                strict=False,
            )
            .dt.replace_time_zone("UTC")
            .alias("timestamp_utc")
        )

        # Rename API camelCase columns
        rename_map: dict[str, str] = {}
        col_map = {
            "pointKey": "point_key",
            "pointLabel": "point_label",
            "operatorKey": "operator_key",
            "operatorLabel": "operator_label",
            "directionKey": "direction_key",
            "unit": "unit",
        }
        for src, dst in col_map.items():
            if src in df.columns:
                rename_map[src] = dst
        if rename_map:
            df = df.rename(rename_map)

        # Normalise flow value to GWh/day
        if "value" in df.columns and "unit" in df.columns:
            df = df.with_columns(pl.col("value").cast(pl.Float64).alias("value"))
            # Apply unit normalisation row-by-row
            df = df.with_columns(
                pl.struct(["value", "unit"])
                .map_elements(
                    lambda row: _normalise_to_gwh_day(
                        row["value"] or 0.0, row["unit"] or ""
                    ),
                    return_dtype=pl.Float64,
                )
                .alias("flow_gwh_per_day")
            )
        elif "value" in df.columns:
            df = df.with_columns(
                (pl.col("value").cast(pl.Float64) * _KWH_D_TO_GWH_D).alias(
                    "flow_gwh_per_day"
                )
            )
        else:
            df = df.with_columns(pl.lit(0.0).alias("flow_gwh_per_day"))

        dedup_cols = ["timestamp_utc", "point_key"]
        if "direction_key" in df.columns:
            dedup_cols.append("direction_key")
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("entsog").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "timestamp_utc", "point_key", "point_label",
            "operator_key", "operator_label", "direction_key",
            "flow_gwh_per_day", "unit",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        sort_cols = ["timestamp_utc", "point_key"]
        if "direction_key" in df.columns:
            sort_cols.append("direction_key")
        return df.select(available).sort(sort_cols)


register_transformer("entsog", "physical_flows", PhysicalFlowsTransformer)
