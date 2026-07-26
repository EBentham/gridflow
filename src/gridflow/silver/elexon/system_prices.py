"""Silver transformer for Elexon system sell/buy prices."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonSystemPrice
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


class SystemPriceTransformer(BaseSilverTransformer):
    """Transform Elexon system price data from bronze to silver."""

    source = "elexon"
    dataset = "system_prices"
    schema_cls = ElexonSystemPrice

    # Run type precedence — higher number wins
    APPEND_ONLY: ClassVar[bool] = True
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = True

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Read all bronze JSON files for a given date."""
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
                data = json.loads(f.read_text())
                # Elexon Insights API returns {"data": [...]}
                records = data.get("data", []) if isinstance(data, dict) else data
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse bronze file {f}: {e}")
                continue

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame(rows)

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Read one Elexon system-price bronze response."""
        try:
            data = json.loads(raw_path.read_text())
            records = data.get("data", []) if isinstance(data, dict) else data
        except (OSError, json.JSONDecodeError, AttributeError) as exc:
            logger.warning(f"Failed to parse bronze file {raw_path}: {exc}")
            return pl.DataFrame()

        return pl.DataFrame(records) if records else pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Normalise, validate, and deduplicate system price data."""
        # Rename API fields to snake_case.
        #
        # `settlementRunType` (legacy field, when present) → `run_type`.
        # `priceDerivationCode` is a SEPARATE concept — it describes how
        # the SBP/SSP was derived for the period (live values include
        # 'N' and 'P'), not the BSC settlement run. V2-FIX-04 fixed the
        # earlier conflation that fed `priceDerivationCode` into
        # `run_type`, then failed downstream Pydantic validation
        # (regex `^(II|SF|R[1-3]|RF|DF)$`).
        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "systemSellPrice": "system_sell_price",
            "systemBuyPrice": "system_buy_price",
            "netImbalanceVolume": "net_imbalance_volume",
            "settlementRunType": "run_type",
            "priceDerivationCode": "price_derivation_code",
        }

        # Only rename columns that exist
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        # Ensure required columns exist. run_type and price_derivation_code are
        # optional at the RAW-FIELD layer only — depend on which Elexon
        # endpoint produced the bronze (settlementRunType / priceDerivationCode
        # may each be absent). The SILVER columns are NOT optional (F-13):
        # gold/views/uk_imbalance_context.sql SELECTs price_derivation_code
        # unconditionally, so a raw-field absence must not silently drop the
        # silver column the view depends on. schemas/elexon.py:48-49 already
        # declares both as `str | None = None`, so always emitting them
        # (typed-null when absent) moves physical output TOWARD the declared
        # contract, not away from it.
        required = [
            "settlement_date",
            "settlement_period",
            "system_sell_price",
            "system_buy_price",
            "net_imbalance_volume",
        ]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            return pl.DataFrame()

        # Inject any missing optional column as a typed-null Utf8 column
        # BEFORE casting, so the cast below is unconditional. Never bare
        # `pl.lit(None)`: a Null-dtype run_type makes select_latest_vintage's
        # replace_strict raise (latent R1-F02 limb 2) — a typed Utf8 null
        # column round-trips through parquet and that codepath cleanly.
        for optional_col in ("run_type", "price_derivation_code"):
            if optional_col not in raw_df.columns:
                raw_df = raw_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(optional_col))

        # Cast types (run_type / price_derivation_code always exist now).
        casts = [
            pl.col("settlement_date").cast(pl.Date),
            pl.col("settlement_period").cast(pl.Int32),
            pl.col("system_sell_price").cast(pl.Float64),
            pl.col("system_buy_price").cast(pl.Float64),
            pl.col("net_imbalance_volume").cast(pl.Float64),
            pl.col("run_type").cast(pl.Utf8),
            pl.col("price_derivation_code").cast(pl.Utf8),
        ]
        df = raw_df.with_columns(casts)

        # Derive UTC timestamp from settlement date + period
        df = df.with_columns(
            pl.struct(["settlement_date", "settlement_period"])
            .map_elements(
                lambda row: settlement_period_to_utc(
                    row["settlement_date"], row["settlement_period"]
                ),
                return_dtype=pl.Datetime("us", "UTC"),
            )
            .alias("timestamp_utc")
        )

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        # Select final columns in order, UNCONDITIONALLY (F-13): run_type and
        # price_derivation_code are always present at this point (injected
        # typed-null above when the raw field was absent), so a drift that
        # somehow removed one before this point now fails LOUD
        # (ColumnNotFoundError) instead of silently narrowing the selection —
        # that silent narrowing is precisely why the P1.5 guard could not fail
        # before this fix.
        output_cols = [
            "settlement_date",
            "settlement_period",
            "timestamp_utc",
            "system_sell_price",
            "system_buy_price",
            "net_imbalance_volume",
            "run_type",
            "price_derivation_code",
            "data_provider",
            "ingested_at",
        ]

        return df.select(output_cols).sort("timestamp_utc")


# Register this transformer
register_transformer("elexon", "system_prices", SystemPriceTransformer)
