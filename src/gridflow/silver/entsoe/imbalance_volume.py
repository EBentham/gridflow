"""Silver transformer for ENTSO-E imbalance volumes (A86/A16)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeImbalanceVolume
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._enum_maps import UNMAPPED_SENTINEL
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ImbalanceVolumeTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E imbalance volumes (A86/A16) bronze XML → silver Parquet.

    Maps flow_direction A01→"long", A02→"short" via replace_strict.
    Deduplicates on (timestamp_utc, area_code, flow_direction).
    """

    source = "entsoe"
    dataset = "imbalance_volume"
    schema_cls = EntsoeImbalanceVolume
    DATASET_VERSION = "2.0.0"
    # The raw codes, not their labels: two unknown codes stay two rows (ADR-022).
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "flow_direction")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """B4 (N-9): FILTER_SAFE, vault-recorded live FR 2025-04-01: real
    populated data, PT15M (R3-RESEARCH.md:68); window-bound comparison not
    shown in the abridged vault text, see
    ADR-026 Appendix A."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        available = set(raw_df.columns)
        required = {"timestamp_utc", "value", "control_area_domain", "flow_direction", "resolution"}
        if not required.issubset(available):
            missing = required - available
            logger.error("Missing columns in bronze data: %s", missing)
            return pl.DataFrame()

        now = datetime.now(UTC)
        df = raw_df.rename(
            {
                "value": "volume_mwh",
                "control_area_domain": "area_code",
            }
        ).with_columns(
            pl.col("flow_direction")
            .replace_strict(
                {"A01": "long", "A02": "short"},
                default=UNMAPPED_SENTINEL,
                return_dtype=pl.Utf8,
            )
            .alias("direction")
        )

        # F-02 fix: derive published_at from document_created_at BEFORE the
        # column-dropping select below, which otherwise discards
        # document_created_at and forces the typed-null branch permanently.
        df = with_published_at(df, dataset=self.dataset)

        df = (
            df.select(
                [
                    "timestamp_utc",
                    "area_code",
                    "flow_direction",
                    "direction",
                    "volume_mwh",
                    "resolution",
                    "published_at",
                ]
            )
            .pipe(self.dedup_to_grain)
            .sort(["timestamp_utc", "area_code", "direction"])
            .with_columns(
                [
                    pl.lit("entsoe").alias("data_provider"),
                    pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
                    pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
                ]
            )
        )

        output_cols = [
            "timestamp_utc",
            "area_code",
            "flow_direction",
            "direction",
            "volume_mwh",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available_cols = [c for c in output_cols if c in df.columns]
        df = df.select(available_cols)

        self.last_unmapped_count = int(df.filter(pl.col("direction") == UNMAPPED_SENTINEL).height)
        if self.last_unmapped_count > 0:
            raw_codes = raw_df.get_column("flow_direction").unique().to_list()
            unmapped_codes = sorted(str(c) for c in raw_codes if c not in {"A01", "A02"})
            logger.warning(
                "%s/%s: %d unmapped flow_direction row(s) labelled %r; unmapped raw codes: %s",
                self.source,
                self.dataset,
                self.last_unmapped_count,
                UNMAPPED_SENTINEL,
                unmapped_codes,
            )

        return df


register_transformer("entsoe", "imbalance_volume", ImbalanceVolumeTransformer)
