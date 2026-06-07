"""Silver transformer for ENTSO-E imbalance volumes (A86/A16)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import EntsoeImbalanceVolume
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsoe._enum_maps import UNMAPPED_SENTINEL
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ImbalanceVolumeTransformer(BaseSilverTransformer):
    """Transform ENTSO-E imbalance volumes (A86/A16) bronze XML → silver Parquet.

    Maps flow_direction A01→"long", A02→"short" via replace_strict.
    Deduplicates on (timestamp_utc, area_code, direction).
    """

    source = "entsoe"
    dataset = "imbalance_volume"
    schema_cls = EntsoeImbalanceVolume

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        records: list[dict[str, Any]] = []
        for xml_file in sorted(bronze_path.glob("raw_*.xml")):
            records.extend(parse_timeseries_xml(xml_file.read_bytes(), value_tag="quantity"))

        if not records:
            return pl.DataFrame()

        return pl.DataFrame(records).with_columns(
            pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
        )

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
        df = (
            raw_df.rename(
                {
                    "value": "volume_mwh",
                    "control_area_domain": "area_code",
                }
            )
            .with_columns(
                pl.col("flow_direction")
                .replace_strict(
                    {"A01": "long", "A02": "short"},
                    default=UNMAPPED_SENTINEL,
                    return_dtype=pl.Utf8,
                )
                .alias("direction")
            )
            .select(
                [
                    "timestamp_utc",
                    "area_code",
                    "direction",
                    "volume_mwh",
                    "resolution",
                ]
            )
            .unique(subset=["timestamp_utc", "area_code", "direction"], keep="last")
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
            "direction",
            "volume_mwh",
            "resolution",
            "data_provider",
            "ingested_at",
        ]
        available_cols = [c for c in output_cols if c in df.columns]
        df = df.select(available_cols)

        self.last_unmapped_count = int(df.filter(pl.col("direction") == UNMAPPED_SENTINEL).height)
        if self.last_unmapped_count > 0:
            raw_codes = raw_df.get_column("flow_direction").unique().to_list()
            unmapped_codes = sorted(c for c in raw_codes if c not in {"A01", "A02"})
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
