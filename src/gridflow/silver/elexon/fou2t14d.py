"""Silver transformer for Elexon 2-14 Day Generation Availability by Fuel Type (FOU2T14D)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonFOU2T14D
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc_expr

logger = logging.getLogger(__name__)


class FOU2T14DTransformer(ElexonTransformer):
    """Transform Elexon FOU2T14D data from bronze to silver.

    F7 makes the dataset append-only: each daily run writes a run-suffixed
    file so revised forward availability forecasts coexist with prior runs.
    Silver retains EVERY vendor publication that bronze carries for a key
    (F-06, v0.18 R1-C). The dedup key includes ``published_at``
    UNCONDITIONALLY — deliberately diverging from the presence-guarded
    siblings (``demand_forecast``, ``wind_forecast``): if the column ever
    drifts out of the output, this transformer raises loudly rather than
    silently reverting to the vintage-collapsing business-key dedup that
    F-06 fixed. Latest-vintage selection is a read-time concern,
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
    DATASET_VERSION: ClassVar[str] = "2.0.0"
    # D-8: published_at is UNCONDITIONAL here (see the F-06 dedup_cols note
    # below) -- this is the revision-preserving grain, distinct from
    # LATEST_VIEW_SPECS's coarser business key (latest_views.py, no
    # published_at) used to pick ONE winning vintage per business key.
    ENTITY_KEY_COLUMNS = ("settlement_date", "fuel_type", "published_at")
    OPTIONAL_ENTITY_KEY_COLUMNS = ("settlement_period",)

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
                settlement_period_to_utc_expr("settlement_date", "settlement_period").alias(
                    "timestamp_utc"
                )
            )
        else:
            # forecastDate is a UK local day (the payload says Europe/London):
            # stamp the start of settlement period 1.
            df = df.with_columns(
                settlement_period_to_utc_expr("settlement_date").alias("timestamp_utc")
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

        df = self.dedup_to_grain(df)

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
