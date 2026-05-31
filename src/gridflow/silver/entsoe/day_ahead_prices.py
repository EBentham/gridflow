"""Silver transformer for ENTSO-E day-ahead prices."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class DayAheadPricesTransformer(BaseSilverTransformer):
    """Transform ENTSO-E day-ahead price XML from bronze to silver."""

    source = "entsoe"
    dataset = "day_ahead_prices"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.xml")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                records = parse_timeseries_xml(f.read_bytes(), value_tag="price.amount")
                rows.extend(records)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to parse ENTSO-E XML %s: %s", f, exc)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["timestamp_utc", "value", "in_domain"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in day_ahead_prices: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "price_eur_mwh", "in_domain": "area_code"})

        # Cast timestamp_utc if it came in as Python datetime objects
        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(pl.col("price_eur_mwh").cast(pl.Float64))

        # Issue 05 #1: carry the explicit source currency (parsed from
        # <currency_Unit.name>) so a GBP price is never silently trusted as
        # EUR on the strength of the `price_eur_mwh` column name alone. The
        # value column name is retained for backward compatibility; `currency`
        # is the authoritative denomination. Empty -> "EUR" fallback only when
        # the source omitted currency_Unit (continental default).
        if "currency_unit" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("currency_unit").cast(pl.Utf8).str.strip_chars() != "")
                .then(pl.col("currency_unit").cast(pl.Utf8).str.strip_chars())
                .otherwise(pl.lit("EUR"))
                .alias("currency")
            )
        else:
            df = df.with_columns(pl.lit("EUR").alias("currency"))

        df = df.unique(subset=["timestamp_utc", "area_code"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "timestamp_utc",
            "area_code",
            "price_eur_mwh",
            "currency",
            "resolution",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "area_code")


register_transformer("entsoe", "day_ahead_prices", DayAheadPricesTransformer)
