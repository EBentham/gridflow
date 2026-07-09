"""Silver transformers for NESO Carbon Intensity API data."""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.schemas.common import BaseSchema

import polars as pl

from gridflow.connectors.neso.endpoints import ENDPOINTS, ParserFamily
from gridflow.schemas.neso import (
    CarbonIntensity,
    CarbonIntensityFactor,
    CarbonIntensityStats,
    GenerationMix,
    RegionalIntensity,
)
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.storage.parquet import write_parquet

logger = logging.getLogger(__name__)

# VTA-SCHEMA-01: one class serves all NESO datasets and dispatches by
# ``parser_family``, so the Pydantic contract is selected per family rather than
# per dataset. ``schema_cls`` is set as a CLASS attribute (via this map) on each
# generated transformer subtype and on ``CarbonIntensityTransformer`` — never
# assigned through ``self`` (that would breach the ClassVar declared on the ABC).
_FAMILY_SCHEMA: dict[ParserFamily, type[BaseSchema]] = {
    ParserFamily.INTENSITY: CarbonIntensity,
    ParserFamily.STATS: CarbonIntensityStats,
    ParserFamily.FACTORS: CarbonIntensityFactor,
    ParserFamily.GENERATION: GenerationMix,
    ParserFamily.REGIONAL: RegionalIntensity,
}


class GenericNesoJsonTransformer(BaseSilverTransformer):
    """Transform one NESO JSON response family into deterministic silver output."""

    source = "neso"
    dataset: str
    parser_family: ParserFamily
    reference_dataset = False

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        for path in self._bronze_files(target_date):
            try:
                payload = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                logger.warning("Failed to parse NESO bronze file %s: %s", path, exc)
                continue
            rows.extend(_extract_rows(payload, self.parser_family))

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, infer_schema_length=None)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        match self.parser_family:
            case ParserFamily.INTENSITY:
                return _transform_intensity(raw_df)
            case ParserFamily.FACTORS:
                return _transform_factors(raw_df)
            case ParserFamily.STATS:
                return _transform_stats(raw_df)
            case ParserFamily.GENERATION:
                return _transform_generation(raw_df)
            case ParserFamily.REGIONAL:
                return _transform_regional(raw_df)

    def _bronze_files(self, target_date: date) -> list[Path]:
        if self.reference_dataset:
            if not self.bronze_dir.exists():
                return []
            return [
                path
                for path in sorted(self.bronze_dir.rglob("raw_*.json"), reverse=True)
                if not path.name.endswith(".meta.json")
            ][:1]

        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
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


class CarbonIntensityTransformer(GenericNesoJsonTransformer):
    """Backward-compatible transformer for the legacy national range dataset."""

    dataset = "carbon_intensity"
    parser_family = ParserFamily.INTENSITY
    schema_cls = CarbonIntensity


def register_neso_transformers() -> None:
    """Register transformers for every NESO endpoint in the catalog."""
    for dataset, endpoint in ENDPOINTS.items():
        transformer = (
            CarbonIntensityTransformer
            if dataset == "carbon_intensity"
            else _make_transformer_class(dataset, endpoint.parser_family, endpoint.reference)
        )
        register_transformer("neso", dataset, transformer)


def _make_transformer_class(
    dataset: str,
    parser_family: ParserFamily,
    reference_dataset: bool,
) -> type[GenericNesoJsonTransformer]:
    class_name = "".join(part.title() for part in dataset.split("_")) + "Transformer"
    return type(
        class_name,
        (GenericNesoJsonTransformer,),
        {
            "dataset": dataset,
            "parser_family": parser_family,
            "reference_dataset": reference_dataset,
            "schema_cls": _FAMILY_SCHEMA[parser_family],
            "__module__": __name__,
        },
    )


def _extract_rows(payload: Any, family: ParserFamily) -> list[dict[str, Any]]:
    match family:
        case ParserFamily.INTENSITY:
            return _extract_intensity_rows(payload)
        case ParserFamily.FACTORS:
            return _extract_factor_rows(payload)
        case ParserFamily.STATS:
            return _extract_stats_rows(payload)
        case ParserFamily.GENERATION:
            return _extract_generation_rows(payload)
        case ParserFamily.REGIONAL:
            return _extract_regional_rows(payload)


def _data_entries(payload: Any) -> list[Any]:
    if not isinstance(payload, dict):
        return payload if isinstance(payload, list) else []
    data = payload.get("data", [])
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _extract_intensity_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in _data_entries(payload):
        if not isinstance(record, dict):
            continue
        intensity = record.get("intensity", {}) or {}
        rows.append(
            {
                "from": record.get("from"),
                "to": record.get("to"),
                "forecast": intensity.get("forecast"),
                "actual": intensity.get("actual"),
                "index": intensity.get("index", ""),
            }
        )
    return rows


def _extract_stats_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in _data_entries(payload):
        if not isinstance(record, dict):
            continue
        intensity = record.get("intensity", {}) or {}
        rows.append(
            {
                "from": record.get("from"),
                "to": record.get("to"),
                "max": intensity.get("max"),
                "average": intensity.get("average"),
                "min": intensity.get("min"),
                "index": intensity.get("index", ""),
            }
        )
    return rows


def _extract_factor_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in _data_entries(payload):
        if not isinstance(record, dict):
            continue
        for fuel, factor in record.items():
            rows.append({"fuel": fuel, "factor_gco2_kwh": factor})
    return rows


def _extract_generation_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in _data_entries(payload):
        if not isinstance(record, dict):
            continue
        for mix in _generation_mix_rows(record):
            rows.append(
                {
                    "from": record.get("from"),
                    "to": record.get("to"),
                    "fuel": mix.get("fuel"),
                    "perc": mix.get("perc"),
                }
            )
    return rows


def _extract_regional_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in _data_entries(payload):
        if not isinstance(entry, dict):
            continue

        if isinstance(entry.get("regions"), list):
            rows.extend(_rows_from_period_regions(entry, entry["regions"]))
        elif isinstance(entry.get("data"), list):
            rows.extend(_rows_from_region_container(entry))
    return rows


def _rows_from_period_regions(period: dict[str, Any], regions: list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for region in regions:
        if not isinstance(region, dict):
            continue
        rows.extend(_rows_from_region_period(region, period))
    return rows


def _rows_from_region_container(region: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for period in region.get("data", []):
        if isinstance(period, dict):
            rows.extend(_rows_from_region_period(region, period))
    return rows


def _rows_from_region_period(
    region: dict[str, Any],
    period: dict[str, Any],
) -> list[dict[str, Any]]:
    # Live API places `intensity` and `generationmix` on the *region* dict
    # for the period-keyed branch (/regional, /regional/intensity/.../{fw24h
    # |fw48h|pt24h|to}) and on the *period* dict for the region-keyed branch
    # (/regional/{name}, /regional/{postcode|regionid}/X). Read from
    # whichever level holds the data so both shapes stay correct.
    base = {
        "regionid": region.get("regionid"),
        "dnoregion": region.get("dnoregion"),
        "shortname": region.get("shortname"),
        "postcode": region.get("postcode"),
        "from": period.get("from"),
        "to": period.get("to"),
    }
    intensity = region.get("intensity") or period.get("intensity") or {}
    base.update(
        {
            "forecast": intensity.get("forecast"),
            "actual": intensity.get("actual"),
            "index": intensity.get("index", ""),
        }
    )

    mixes = _generation_mix_rows(region) or _generation_mix_rows(period)
    if not mixes:
        return [base]
    return [
        {
            **base,
            "fuel": mix.get("fuel"),
            "perc": mix.get("perc"),
        }
        for mix in mixes
    ]


def _generation_mix_rows(record: dict[str, Any]) -> list[dict[str, Any]]:
    generation_mix = record.get("generationmix", [])
    return (
        [row for row in generation_mix if isinstance(row, dict)]
        if isinstance(generation_mix, list)
        else []
    )


def _transform_intensity(raw_df: pl.DataFrame) -> pl.DataFrame:
    if "from" not in raw_df.columns:
        logger.error("Missing required column in NESO intensity data: from")
        return pl.DataFrame()

    df = raw_df.with_columns(
        [
            _parse_neso_datetime("from").alias("timestamp_utc"),
            _parse_neso_datetime("to").alias("period_end_utc"),
            _float_or_null(raw_df, "forecast").alias("forecast_gco2_kwh"),
            _float_or_null(raw_df, "actual").alias("actual_gco2_kwh"),
            _string_or_empty(raw_df, "index").alias("intensity_index"),
        ]
    )
    df = _add_common_columns(df)
    return (
        df.unique(subset=["timestamp_utc"], keep="last")
        .select(
            [
                "timestamp_utc",
                "period_end_utc",
                "forecast_gco2_kwh",
                "actual_gco2_kwh",
                "intensity_index",
                "data_provider",
                "ingested_at",
            ]
        )
        .sort("timestamp_utc")
    )


def _transform_stats(raw_df: pl.DataFrame) -> pl.DataFrame:
    if "from" not in raw_df.columns:
        logger.error("Missing required column in NESO stats data: from")
        return pl.DataFrame()

    df = raw_df.with_columns(
        [
            _parse_neso_datetime("from").alias("timestamp_utc"),
            _parse_neso_datetime("to").alias("period_end_utc"),
            _float_or_null(raw_df, "max").alias("max_gco2_kwh"),
            _float_or_null(raw_df, "average").alias("average_gco2_kwh"),
            _float_or_null(raw_df, "min").alias("min_gco2_kwh"),
            _string_or_empty(raw_df, "index").alias("intensity_index"),
        ]
    )
    df = _add_common_columns(df)
    return (
        df.unique(subset=["timestamp_utc", "period_end_utc"], keep="last")
        .select(
            [
                "timestamp_utc",
                "period_end_utc",
                "max_gco2_kwh",
                "average_gco2_kwh",
                "min_gco2_kwh",
                "intensity_index",
                "data_provider",
                "ingested_at",
            ]
        )
        .sort("timestamp_utc")
    )


def _transform_factors(raw_df: pl.DataFrame) -> pl.DataFrame:
    if "fuel" not in raw_df.columns:
        logger.error("Missing required column in NESO factors data: fuel")
        return pl.DataFrame()

    df = raw_df.with_columns(
        [
            pl.col("fuel")
            .cast(pl.Utf8)
            .str.to_lowercase()
            .str.replace_all(r"[^a-z0-9]+", "_")
            .str.strip_chars("_")
            .alias("fuel"),
            _float_or_null(raw_df, "factor_gco2_kwh").alias("factor_gco2_kwh"),
        ]
    )
    df = _add_common_columns(df)
    return (
        df.unique(subset=["fuel"], keep="last")
        .select(["fuel", "factor_gco2_kwh", "data_provider", "ingested_at"])
        .sort("fuel")
    )


def _transform_generation(raw_df: pl.DataFrame) -> pl.DataFrame:
    if "from" not in raw_df.columns:
        logger.error("Missing required column in NESO generation data: from")
        return pl.DataFrame()

    df = raw_df.with_columns(
        [
            _parse_neso_datetime("from").alias("timestamp_utc"),
            _parse_neso_datetime("to").alias("period_end_utc"),
            _string_or_empty(raw_df, "fuel").alias("fuel"),
            _float_or_null(raw_df, "perc").alias("generation_percentage"),
        ]
    )
    df = _add_common_columns(df)
    return (
        df.unique(subset=["timestamp_utc", "fuel"], keep="last")
        .select(
            [
                "timestamp_utc",
                "period_end_utc",
                "fuel",
                "generation_percentage",
                "data_provider",
                "ingested_at",
            ]
        )
        .sort(["timestamp_utc", "fuel"])
    )


def _transform_regional(raw_df: pl.DataFrame) -> pl.DataFrame:
    if "from" not in raw_df.columns:
        logger.error("Missing required column in NESO regional data: from")
        return pl.DataFrame()

    df = raw_df.with_columns(
        [
            _parse_neso_datetime("from").alias("timestamp_utc"),
            _parse_neso_datetime("to").alias("period_end_utc"),
            _float_or_null(raw_df, "forecast").alias("forecast_gco2_kwh"),
            _float_or_null(raw_df, "actual").alias("actual_gco2_kwh"),
            _string_or_empty(raw_df, "index").alias("intensity_index"),
            _string_or_empty(raw_df, "fuel").alias("fuel"),
            _float_or_null(raw_df, "perc").alias("generation_percentage"),
        ]
    )
    if "regionid" in df.columns:
        df = df.with_columns(pl.col("regionid").cast(pl.Int64, strict=False))
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("regionid"))

    for column in ["dnoregion", "shortname", "postcode"]:
        if column not in df.columns:
            df = df.with_columns(pl.lit("").alias(column))
        else:
            df = df.with_columns(pl.col(column).fill_null("").cast(pl.Utf8))

    df = _add_common_columns(df)
    return (
        df.unique(
            subset=["timestamp_utc", "regionid", "shortname", "postcode", "fuel"],
            keep="last",
        )
        .select(
            [
                "timestamp_utc",
                "period_end_utc",
                "regionid",
                "dnoregion",
                "shortname",
                "postcode",
                "forecast_gco2_kwh",
                "actual_gco2_kwh",
                "intensity_index",
                "fuel",
                "generation_percentage",
                "data_provider",
                "ingested_at",
            ]
        )
        .sort(["timestamp_utc", "regionid", "fuel"])
    )


def _parse_neso_datetime(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.to_datetime(format="%Y-%m-%dT%H:%MZ", time_unit="us", strict=False)
        .dt.replace_time_zone("UTC")
    )


def _float_or_null(df: pl.DataFrame, column: str) -> pl.Expr:
    if column not in df.columns:
        return pl.lit(None).cast(pl.Float64)
    return pl.col(column).cast(pl.Float64, strict=False)


def _string_or_empty(df: pl.DataFrame, column: str) -> pl.Expr:
    if column not in df.columns:
        return pl.lit("")
    return pl.col(column).fill_null("").cast(pl.Utf8)


def _add_common_columns(df: pl.DataFrame) -> pl.DataFrame:
    now = datetime.now(UTC)
    return df.with_columns(
        [
            pl.lit("neso").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ]
    )


register_neso_transformers()
