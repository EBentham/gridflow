"""Generic silver transformers for ENTSO-G JSON endpoints."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

import polars as pl

from gridflow.connectors.entsog.endpoints import ENDPOINTS, EntsogEndpoint
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsog.datetime import (
    filter_records_to_target_date,
    parse_entsog_datetime_expr,
)
from gridflow.silver.registry import register_transformer
from gridflow.storage.parquet import write_parquet

logger = logging.getLogger(__name__)

_DATETIME_COLUMNS = {
    "period_from",
    "period_to",
    "original_period_from",
    "last_update_date_time",
    "publication_date_time",
    "event_start",
    "event_stop",
    "auction_from",
    "auction_to",
    "capacity_from",
    "capacity_to",
    "product_period_from",
    "product_period_to",
    "valid_from",
    "valid_to",
}
_TIMESTAMP_PRIORITY = [
    "period_from",
    "publication_date_time",
    "event_start",
    "auction_from",
    "capacity_from",
    "valid_from",
    "last_update_date_time",
]
_NUMERIC_SUFFIXES = (
    "_value",
    "_price",
    "_volume",
    "_capacity",
    "_count",
    "_rate",
    "_x",
    "_y",
)
_NUMERIC_NAMES = {
    "value",
    "auction_premium",
    "cleared_price",
    "reserve_price",
    "requested_volume",
    "allocated_volume",
    "unallocated_volume",
    "unavailable_capacity",
    "available_capacity",
    "technical_capacity",
    "count_point_presents",
    "adjacent_systems_count",
}


class GenericEntsogJsonTransformer(BaseSilverTransformer):
    """Transform flat ENTSOG JSON records into deterministic silver output."""

    source = "entsog"
    dataset: str
    response_key: str
    reference_dataset: bool = False
    date_window_dataset: bool = False

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        files = self._bronze_files(target_date)
        rows: list[dict[str, Any]] = []
        for path in files:
            try:
                payload = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                logger.warning("Failed to parse ENTSO-G bronze file %s: %s", path, exc)
                continue
            rows.extend(_extract_records(payload, self.response_key))

        if not rows:
            return pl.DataFrame()
        if self.date_window_dataset:
            rows = filter_records_to_target_date(rows, target_date, _RAW_TIMESTAMP_PRIORITY)
            if not rows:
                return pl.DataFrame()
        return pl.DataFrame(rows, infer_schema_length=None)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        df = _normalise_column_names(raw_df)

        for column in list(df.columns):
            if column in _DATETIME_COLUMNS:
                df = df.with_columns(parse_entsog_datetime_expr(column).alias(column))

        timestamp_source = next((col for col in _TIMESTAMP_PRIORITY if col in df.columns), None)
        if timestamp_source and "timestamp_utc" not in df.columns:
            df = df.with_columns(pl.col(timestamp_source).alias("timestamp_utc"))

        for column in list(df.columns):
            if _looks_numeric(column):
                df = df.with_columns(pl.col(column).cast(pl.Float64, strict=False))

        dedup_subset = ["id"] if "id" in df.columns else [
            column for column in df.columns if column not in {"timestamp_utc"}
        ]
        if dedup_subset:
            df = df.unique(subset=dedup_subset, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("entsog").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        preferred = [
            "timestamp_utc",
            "period_from",
            "period_to",
            "indicator",
            "period_type",
            "operator_key",
            "operator_label",
            "tso_eic_code",
            "point_key",
            "point_label",
            "direction_key",
            "country_key",
            "country_label",
            "bz_key",
            "bz_label",
            "unit",
            "value",
            "id",
        ]
        ordered = [col for col in preferred if col in df.columns]
        ordered.extend(col for col in df.columns if col not in ordered)

        sort_cols = [
            col
            for col in ["timestamp_utc", "point_key", "operator_key", "id"]
            if col in df.columns
        ]
        result = df.select(ordered)
        return result.sort(sort_cols) if sort_cols else result

    def _bronze_files(self, target_date: date) -> list[Path]:
        if self.reference_dataset:
            if not self.bronze_dir.exists():
                return []
            return [
                path
                for path in sorted(self.bronze_dir.rglob("raw_*.json"), reverse=True)
                if not path.name.endswith(".meta.json")
            ][:1]

        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            return []
        return [
            path
            for path in sorted(bronze_path.glob("raw_*.json"))
            if not path.name.endswith(".meta.json")
        ]

    def _write_silver(
        self,
        df: pl.DataFrame,
        target_date: date,
        available_at: datetime,
    ) -> None:
        if not self.reference_dataset:
            super()._write_silver(df, target_date, available_at=available_at)
            return
        write_parquet(df, self.silver_dir / f"{self.dataset}.parquet")

    def _write_csv(self, df: pl.DataFrame, target_date: date) -> None:
        if not self.reference_dataset:
            super()._write_csv(df, target_date)
            return
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        final_path = self.silver_dir / f"{self.dataset}.csv"
        tmp_path = self.silver_dir / f".tmp_{self.dataset}.csv"
        df.write_csv(tmp_path)
        os.replace(tmp_path, final_path)


def register_generic_entsog_transformers() -> None:
    """Register generic transformers for all non-specialised ENTSOG datasets."""
    for dataset, endpoint in ENDPOINTS.items():
        if dataset == "physical_flows":
            continue
        register_transformer("entsog", dataset, _make_transformer_class(dataset, endpoint))


def _make_transformer_class(
    dataset: str,
    endpoint: EntsogEndpoint,
) -> type[GenericEntsogJsonTransformer]:
    class_name = "".join(part.title() for part in dataset.split("_")) + "Transformer"
    return type(
        class_name,
        (GenericEntsogJsonTransformer,),
        {
            "dataset": dataset,
            "response_key": endpoint.response_key,
            "reference_dataset": endpoint.reference,
            "date_window_dataset": endpoint.requires_dates,
            "__module__": __name__,
        },
    )


def _extract_records(payload: Any, response_key: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return payload if isinstance(payload, list) else []

    records = payload.get(response_key)
    if isinstance(records, list):
        return [row for row in records if isinstance(row, dict)]

    for key, value in payload.items():
        if key == "meta":
            continue
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _camel_to_snake(value: str) -> str:
    value = value.replace(" ", "_").replace("-", "_")
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return re.sub(r"_+", "_", value).strip("_").lower()


def _normalise_column_names(df: pl.DataFrame) -> pl.DataFrame:
    normalised: dict[str, list[str]] = {}
    for column in df.columns:
        normalised.setdefault(_camel_to_snake(column), []).append(column)

    result = df
    collisions = {
        target: columns
        for target, columns in normalised.items()
        if len(columns) > 1
    }
    for target, columns in collisions.items():
        result = result.with_columns(
            pl.coalesce([pl.col(column) for column in columns]).alias(target)
        )
        result = result.drop([
            column
            for column in columns
            if column != target and column in result.columns
        ])

    rename_map = {
        columns[0]: target
        for target, columns in normalised.items()
        if len(columns) == 1 and columns[0] != target
    }
    return result.rename(rename_map)


def _looks_numeric(column: str) -> bool:
    if column in _NUMERIC_NAMES:
        return True
    return column.endswith(_NUMERIC_SUFFIXES)


_RAW_TIMESTAMP_PRIORITY = (
    "periodFrom",
    "publicationDateTime",
    "eventStart",
    "auctionFrom",
    "capacityFrom",
    "validFrom",
    "lastUpdateDateTime",
)
