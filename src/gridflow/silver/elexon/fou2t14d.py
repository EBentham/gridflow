"""Silver transformer for Elexon 2-14 Day Generation Availability by Fuel Type (FOU2T14D)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonFOU2T14D
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class FOU2T14DTransformer(BaseSilverTransformer):
    """Transform Elexon FOU2T14D data from bronze to silver.

    F7 makes the dataset append-only: each daily run writes a run-suffixed
    file so revised forward availability forecasts coexist with prior runs.
    Silver retains EVERY vendor publication that bronze carries for a key
    (F-06, v0.18 R1-C); latest-vintage selection is a read-time concern,
    served by ``silver_elexon_fou2t14d_latest`` (``latest_views.py:104-107``,
    ADR-025 §2) and by the models-side ``latest_only``/``partition_columns``
    fetch (see ADR-019 in the gridflow_models repo).

    ``available_at`` is per-row ``coalesce(published_at, ingest_scalar)``
    (ADR-025 §3, ``base.py:404-420``), so distinct publications inside one
    bronze fetch day are distinguishable within one run-suffixed file.
    Known residual, named explicitly: ``VINTAGE_PER_BRONZE_FILE`` is not set
    on this transformer, so rows with a null ``published_at`` still share one
    coarse per-fetch-day availability scalar (X1-F07, MEDIUM, deferred --
    not fixed here).

    Note on timestamps: ``available_at`` is the authoritative bitemporal
    publication timestamp added by ``BaseSilverTransformer``; ``ingested_at``
    is retained for backward compatibility as the local processing
    timestamp. Under ``--reingest`` the two diverge.
    """

    source = "elexon"
    dataset = "fou2t14d"
    schema_cls = ElexonFOU2T14D
    APPEND_ONLY: ClassVar[bool] = True
    DATASET_VERSION: ClassVar[str] = "1.0.0"

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
                data = json.loads(f.read_text())
                records = data.get("data", []) if isinstance(data, dict) else data
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse bronze file {f}: {e}")
                continue

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "forecastDate": "settlement_date",
            "publishDateTime": "published_at",
            "publishTime": "published_at",
            "fuelType": "fuel_type",
            "outputUsable": "output_usable_mw",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "fuel_type", "output_usable_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in FOU2T14D: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("fuel_type").cast(pl.Utf8),
                pl.col("output_usable_mw").cast(pl.Float64),
            ]
        )

        # settlement_period may not exist for forecast data (forecastDate only)
        if "settlement_period" in df.columns:
            df = df.with_columns(pl.col("settlement_period").cast(pl.Int32))
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
        else:
            # Use settlement_date at midnight UTC as timestamp
            df = df.with_columns(
                pl.col("settlement_date")
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("UTC")
                .alias("timestamp_utc")
            )

        # G6 (W2.2 pattern): cast `published_at` to UTC datetime so the
        # column survives to silver well-typed.
        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
            )
        else:
            # WHY: the silver schema declares published_at as a nullable contract
            # column. Emit it as typed-null when bronze lacks the publish field so the
            # silver schema is deterministic and partition globs don't drift across
            # history (a missing column breaks SELECT * reads spanning files that do
            # carry it).
            df = df.with_columns(pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at"))

        # WHY (F-06, v0.18 R1-C): published_at is the vendor publication vintage
        # and this dataset is APPEND_ONLY, so a dedup key without it collapsed
        # 95.7% (176,035/184,015) of the vintages inside transform() (v0.17
        # review). Origin `66bfcdd` (published_at not yet in output_cols, so the
        # key WAS the full silver grain); made a defect by `7fccb1f` (G6), which
        # added published_at to output_cols without updating this key. What
        # remains deduped is the intra-vintage class: one vendor response cannot
        # carry two truths for one key at one publication instant. Rows with a
        # null published_at still collapse (nulls compare equal in `unique`) --
        # correct, because no vintage axis exists for them.
        #
        # The append is deliberately UNCONDITIONAL, diverging from
        # demand_forecast.py:148-151 / wind_forecast.py:128-133, which guard
        # with `if "published_at" in df.columns`. published_at is guaranteed
        # present here by construction (both branches above emit it, cast or
        # typed-null), so an unconditional key raises loudly (ColumnNotFoundError)
        # if that guarantee is ever refactored away, instead of silently
        # reinstating the collapse (OQ-1).
        dedup_cols = ["settlement_date", "fuel_type"]
        if "settlement_period" in df.columns:
            dedup_cols.insert(1, "settlement_period")
        dedup_cols.append("published_at")
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "settlement_date",
            "settlement_period",
            "timestamp_utc",
            "fuel_type",
            "output_usable_mw",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "fuel_type")


register_transformer("elexon", "fou2t14d", FOU2T14DTransformer)
