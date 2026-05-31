"""Silver transformer for ENTSO-G physical gas flows."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsog.datetime import (
    filter_records_to_target_date,
    parse_entsog_datetime_expr,
)
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)

# Unit normalisation to GWh/day.
# ENTSO-G commonly returns kWh/d; divide by 1e6 to get GWh/d.
# Some endpoints return kWh/h; multiply by 24 then divide by 1e6.
_KWH_D_TO_GWH_D = 1e-6
_KWH_H_TO_GWH_D = 24.0 * 1e-6

# Issue 05 #3 (code-review-2026-05): explicit unit -> GWh/day factor table.
# The previous `else` fallthrough multiplied EVERY non-kWh/h unit by 1e-6,
# silently mis-scaling already-GWh/d values by 1e6 and MWh/d by 1e3. An
# unrecognised unit must be rejected, never assumed kWh/d. Keys are
# lower-cased and stripped; daily and hourly spellings both covered.
_UNIT_TO_GWH_DAY: dict[str, float] = {
    "kwh/d": _KWH_D_TO_GWH_D,
    "kwh/day": _KWH_D_TO_GWH_D,
    "kwh/h": _KWH_H_TO_GWH_D,
    "kwh/hr": _KWH_H_TO_GWH_D,
    "mwh/d": 1e-3,
    "mwh/day": 1e-3,
    "mwh/h": 24.0 * 1e-3,
    "mwh/hr": 24.0 * 1e-3,
    "gwh/d": 1.0,
    "gwh/day": 1.0,
    "gwh/h": 24.0,
    "gwh/hr": 24.0,
}


def _normalise_to_gwh_day(value: float, unit: str) -> float:
    """Normalise a gas flow value to GWh/day.

    Raises:
        ValueError: if ``unit`` is not in the explicit factor table. Callers
            must handle this (the transformer log-and-drops the row) rather
            than letting an unknown unit be silently mis-scaled.
    """
    unit_key = unit.lower().strip()
    factor = _UNIT_TO_GWH_DAY.get(unit_key)
    if factor is None:
        raise ValueError(f"Unrecognised ENTSO-G flow unit: {unit!r}")
    return value * factor


class PhysicalFlowsTransformer(BaseSilverTransformer):
    """Transform ENTSO-G physical flow JSON from bronze to silver."""

    source = "entsog"
    dataset = "physical_flows"

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
                records = payload.get("operationalData", []) if isinstance(payload, dict) else []
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as exc:
                logger.warning("Failed to parse ENTSO-G bronze file %s: %s", f, exc)

        if not rows:
            return pl.DataFrame()
        rows = filter_records_to_target_date(rows, target_date, ("periodFrom",))
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, infer_schema_length=None)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["periodFrom", "pointKey"]
        missing = [column for column in required if column not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in ENTSO-G physical flows: %s", missing)
            return pl.DataFrame()

        df = raw_df

        if "indicator" in df.columns:
            df = df.filter(pl.col("indicator") == "Physical Flow")

        if df.is_empty():
            return pl.DataFrame()

        df = df.with_columns(parse_entsog_datetime_expr("periodFrom").alias("timestamp_utc"))

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

        if "value" in df.columns and "unit" in df.columns:
            df = df.with_columns(pl.col("value").cast(pl.Float64, strict=False).alias("value"))
            # Issue 05 #3: drop rows whose unit is not in the explicit factor
            # table BEFORE normalising, with a logged count, rather than
            # silently mis-scaling them by assuming kWh/d. "Never silently
            # dropped" (CLAUDE.md): the drop is surfaced via a WARNING.
            recognised = pl.col("unit").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
            is_known = recognised.is_in(list(_UNIT_TO_GWH_DAY.keys()))
            unknown = df.filter(~is_known)
            if not unknown.is_empty():
                bad_units = sorted({u for u in unknown["unit"].to_list() if u is not None})
                logger.warning(
                    "Dropping %d ENTSO-G physical-flow row(s) with unrecognised "
                    "unit(s) %s (not mis-scaling as kWh/d)",
                    unknown.height,
                    bad_units,
                )
                df = df.filter(is_known)
            if df.is_empty():
                return pl.DataFrame()
            df = df.with_columns(
                pl.struct(["value", "unit"])
                .map_elements(
                    lambda row: _normalise_to_gwh_day(row["value"] or 0.0, row["unit"] or ""),
                    return_dtype=pl.Float64,
                )
                .alias("flow_gwh_per_day")
            )
        elif "value" in df.columns:
            df = df.with_columns(
                (
                    pl.col("value").cast(pl.Float64, strict=False).fill_null(0.0) * _KWH_D_TO_GWH_D
                ).alias("flow_gwh_per_day")
            )
        else:
            df = df.with_columns(pl.lit(0.0).alias("flow_gwh_per_day"))

        dedup_cols = ["timestamp_utc", "point_key"]
        if "operator_key" in df.columns:
            dedup_cols.append("operator_key")
        if "direction_key" in df.columns:
            dedup_cols.append("direction_key")
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsog").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "timestamp_utc",
            "point_key",
            "point_label",
            "operator_key",
            "operator_label",
            "direction_key",
            "flow_gwh_per_day",
            "unit",
            "data_provider",
            "ingested_at",
        ]
        available = [column for column in output_cols if column in df.columns]
        sort_cols = ["timestamp_utc", "point_key"]
        if "direction_key" in df.columns:
            sort_cols.append("direction_key")
        return df.select(available).sort(sort_cols)


register_transformer("entsog", "physical_flows", PhysicalFlowsTransformer)
