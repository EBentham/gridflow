"""Silver transformer for Elexon REMIT Outage and Unavailability Messages."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class REMITTransformer(BaseSilverTransformer):
    """Transform Elexon REMIT data from bronze to silver.

    F7 changes the silver semantics: every revision of an outage message is
    preserved instead of collapsing to the latest revision per ``mrid``.
    Latest-revision selection is now a read-time concern handled by
    ``GridflowDataSource.fetch(latest_only=True, partition_columns=["mrid"])``.
    See ``.planning/phases/F7-stack-model-data-infrastructure/F7-PLAN.md``
    for the broader F7 phase summary.

    Note on timestamps: ``available_at`` is the bitemporal processing/write
    timestamp added by ``BaseSilverTransformer`` (``datetime.now`` on a live
    run, or the bronze ``written_at`` sidecar value under ``--reingest``). It
    is the leakage-barrier key, not a publication timestamp. Publication time
    is carried separately by ``published_at`` (from ``publishTime``) and the
    semantic ``event_time``; reason about what a model could see at delivery
    on those columns, not on ``available_at``. ``ingested_at`` is retained for
    backward compatibility as the local processing timestamp. Under
    ``--reingest`` the two diverge — ``available_at`` is reconstructed from
    bronze sidecars while ``ingested_at`` stamps the current run.
    """

    source = "elexon"
    dataset = "remit"
    APPEND_ONLY: ClassVar[bool] = True
    DATASET_VERSION: ClassVar[str] = "2.0.0"

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
            "mrid": "mrid",
            "revisionNumber": "revision_number",
            "publishTime": "published_at",
            "createdTime": "created_time",
            "messageType": "message_type",
            "messageHeading": "message_heading",
            "eventType": "event_type",
            "unavailabilityType": "unavailability_type",
            "participantId": "participant_id",
            "registrationCode": "registration_code",
            "assetId": "asset_id",
            "assetType": "asset_type",
            "affectedUnit": "affected_unit",
            "affectedUnitEIC": "affected_unit_eic",
            "biddingZone": "bidding_zone",
            "fuelType": "fuel_type",
            "normalCapacity": "normal_capacity_mw",
            "availableCapacity": "available_capacity_mw",
            "unavailableCapacity": "unavailable_capacity_mw",
            "eventStatus": "event_status",
            "eventStartTime": "event_start_time",
            "eventEndTime": "event_end_time",
            "cause": "cause",
            "relatedInformation": "related_information",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["mrid", "published_at"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in REMIT: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            pl.col("published_at")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
            .dt.replace_time_zone("UTC")
            .alias("timestamp_utc")
        )

        for col in ["event_start_time", "event_end_time", "created_time"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                    .dt.replace_time_zone("UTC")
                    .alias(col)
                )

        for col in ["normal_capacity_mw", "available_capacity_mw", "unavailable_capacity_mw"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        if "revision_number" in df.columns:
            df = df.with_columns(pl.col("revision_number").cast(pl.Int32))

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "mrid",
            "revision_number",
            "timestamp_utc",
            "message_type",
            "message_heading",
            "event_type",
            "unavailability_type",
            "participant_id",
            "registration_code",
            "asset_id",
            "asset_type",
            "affected_unit",
            "affected_unit_eic",
            "bidding_zone",
            "fuel_type",
            "normal_capacity_mw",
            "available_capacity_mw",
            "unavailable_capacity_mw",
            "event_status",
            "event_start_time",
            "event_end_time",
            "cause",
            "related_information",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "remit", REMITTransformer)
