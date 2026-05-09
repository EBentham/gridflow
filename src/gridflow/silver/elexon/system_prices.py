"""Silver transformer for Elexon system sell/buy prices."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class SystemPriceTransformer(BaseSilverTransformer):
    """Transform Elexon system price data from bronze to silver."""

    source = "elexon"
    dataset = "system_prices"

    # Run type precedence — higher number wins
    RUN_PRECEDENCE = {"II": 1, "SF": 2, "R1": 3, "R2": 4, "R3": 5, "RF": 6, "DF": 7}

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

        rows: list[dict] = []
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

        # Ensure required columns exist (run_type and price_derivation_code
        # are both optional — depend on which Elexon endpoint produced
        # the bronze).
        required = [
            "settlement_date", "settlement_period",
            "system_sell_price", "system_buy_price",
            "net_imbalance_volume",
        ]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            return pl.DataFrame()

        # Cast types
        casts = [
            pl.col("settlement_date").cast(pl.Date),
            pl.col("settlement_period").cast(pl.Int32),
            pl.col("system_sell_price").cast(pl.Float64),
            pl.col("system_buy_price").cast(pl.Float64),
            pl.col("net_imbalance_volume").cast(pl.Float64),
        ]
        if "run_type" in raw_df.columns:
            casts.append(pl.col("run_type").cast(pl.Utf8))
        if "price_derivation_code" in raw_df.columns:
            casts.append(pl.col("price_derivation_code").cast(pl.Utf8))
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

        # Resolve settlement runs: keep only the latest per SP
        if "run_type" in df.columns:
            df = self._resolve_runs(df)
        else:
            df = df.unique(subset=["settlement_date", "settlement_period"], keep="last")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("elexon").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        # Select final columns in order
        output_cols = [
            "settlement_date", "settlement_period", "timestamp_utc",
            "system_sell_price", "system_buy_price", "net_imbalance_volume",
            "run_type", "price_derivation_code", "data_provider", "ingested_at",
        ]
        available_cols = [c for c in output_cols if c in df.columns]

        return df.select(available_cols).sort("timestamp_utc")

    def _resolve_runs(self, df: pl.DataFrame) -> pl.DataFrame:
        """Keep only the latest run type per settlement period."""
        return (
            df.with_columns(
                pl.col("run_type")
                .replace_strict(self.RUN_PRECEDENCE, default=0)
                .alias("_run_rank")
            )
            .sort("_run_rank", descending=True)
            .group_by(["settlement_date", "settlement_period"])
            .first()
            .drop("_run_rank")
        )


# Register this transformer
register_transformer("elexon", "system_prices", SystemPriceTransformer)
