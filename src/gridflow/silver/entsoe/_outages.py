"""Shared outage-document handling for the ENTSO-E unavailability tables (A76-A80).

Outage documents describe one unavailability as an A03 curve whose PT1M Period
can run for months, and every daily request returns every document that
overlaps that day. Silver therefore:

* parses each declared point as one step with an explicit end
  (``parse_timeseries_xml(..., a03_as_steps=True)``), never a row per minute;
* clips each step to the partition's UTC day, so a daily file owns only that
  day's part of an outage and ``query()`` over several days returns each
  outage-minute once (the repeats across daily files were 13al);
* keeps only the latest revision of each document within a partition, and keys
  rows on the document rather than on time and unit, so two different
  documents for the same unit no longer collapse in parse order (13ak).

The outage's own start and end survive unclipped as ``outage_start_utc`` and
``outage_end_utc``, so durations stay recoverable.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

logger = logging.getLogger(__name__)

_UTC_US = pl.Datetime("us", "UTC")

#: Columns every outage silver table carries, in output order, after the
#: table-specific identity columns.
OUTAGE_COMMON_COLUMNS: tuple[str, ...] = (
    "outage_type",
    "unavailable_mw",
    "business_type",
    "document_mrid",
    "revision_number",
    "document_status",
    "timeseries_mrid",
    "outage_start_utc",
    "outage_end_utc",
    "resolution",
    "published_at",
    "data_provider",
    "ingested_at",
)

_STRING_DEFAULTS: tuple[str, ...] = (
    "business_type",
    "document_mrid",
    "revision_number",
    "document_status",
    "timeseries_mrid",
    "resolution",
    "unit_mrid",
    "unit_name",
    "production_type",
    "asset_mrid",
    "asset_name",
    "generation_unit_mrid",
    "generation_unit_name",
    "nominal_p",
)


def parse_outage_xml(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Parse one outage document into step records.

    Args:
        xml_bytes: Raw ENTSO-E Unavailability_MarketDocument body.

    Returns:
        One record per declared point, each with ``step_end_utc``,
        ``period_start_utc`` and ``period_end_utc``.
    """
    return parse_timeseries_xml(xml_bytes, value_tag="quantity", a03_as_steps=True)


def read_outage_partition(bronze_path: Path, target_date: date) -> pl.DataFrame:
    """Parse every raw body in one bronze partition and clip it to that day.

    Args:
        bronze_path: The partition directory holding ``raw_*.xml`` bodies.
        target_date: The partition's UTC day.

    Returns:
        Step rows overlapping ``target_date``, clipped to it; empty when none.
    """
    rows: list[dict[str, Any]] = []
    for xml_file in sorted(bronze_path.glob("raw_*.xml")):
        try:
            rows.extend(parse_outage_xml(xml_file.read_bytes()))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse ENTSO-E XML %s: %s", xml_file, exc)
    if not rows:
        return pl.DataFrame()
    return clip_to_day(pl.DataFrame(rows, infer_schema_length=None), target_date)


def clip_to_day(df: pl.DataFrame, target_date: date) -> pl.DataFrame:
    """Keep the part of each step that falls inside ``target_date`` (UTC).

    Args:
        df: Step rows with ``timestamp_utc`` and ``step_end_utc``.
        target_date: The partition's UTC day.

    Returns:
        Rows overlapping ``[day start, next day start)``, with both bounds
        clipped to that window.
    """
    if df.is_empty():
        return df
    day_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    start = pl.lit(day_start).cast(_UTC_US)
    end = pl.lit(day_end).cast(_UTC_US)
    df = df.with_columns(
        pl.col("timestamp_utc").cast(_UTC_US),
        pl.col("step_end_utc").cast(_UTC_US),
    )
    overlaps = (pl.col("step_end_utc") > start) & (pl.col("timestamp_utc") < end)
    return df.filter(overlaps).with_columns(
        pl.max_horizontal(pl.col("timestamp_utc"), start).alias("timestamp_utc"),
        pl.min_horizontal(pl.col("step_end_utc"), end).alias("step_end_utc"),
    )


def prepare_outage_frame(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Normalise parsed outage rows: types, defaults and the outage interval.

    Args:
        raw_df: Rows from :func:`parse_outage_xml` (clipped or not).

    Returns:
        The frame with every optional string column present, timestamps
        tz-aware, ``unavailable_mw`` and ``nominal_mw`` as floats,
        ``revision_number`` as an integer, ``outage_type`` mapped from the
        business type, and ``outage_start_utc`` / ``outage_end_utc`` taken
        from the series' Periods.
    """
    df = raw_df.rename({"value": "unavailable_mw"})
    missing = [pl.lit("").alias(c) for c in _STRING_DEFAULTS if c not in df.columns]
    if missing:
        df = df.with_columns(missing)
    for column in ("period_start_utc", "period_end_utc"):
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).cast(_UTC_US).alias(column))
    series = ["document_mrid", "revision_number", "timeseries_mrid"]
    return df.with_columns(
        pl.col("timestamp_utc").cast(_UTC_US),
        pl.col("step_end_utc").cast(_UTC_US),
        pl.col("unavailable_mw").cast(pl.Float64),
        pl.col("nominal_p").cast(pl.Float64, strict=False).alias("nominal_mw"),
        pl.col("revision_number").cast(pl.Int64, strict=False),
        pl.col("period_start_utc").cast(_UTC_US).min().over(series).alias("outage_start_utc"),
        pl.col("period_end_utc").cast(_UTC_US).max().over(series).alias("outage_end_utc"),
        pl.when(pl.col("business_type") == "A53")
        .then(pl.lit("planned"))
        .when(pl.col("business_type") == "A54")
        .then(pl.lit("unplanned"))
        .otherwise(pl.col("business_type"))
        .alias("outage_type"),
        pl.lit("entsoe").alias("data_provider"),
        pl.lit(datetime.now(UTC)).cast(_UTC_US).alias("ingested_at"),
    )


def latest_revision_steps(df: pl.DataFrame, identity: Sequence[str]) -> pl.DataFrame:
    """Keep each document's latest revision and one row per step.

    A later revision supersedes the whole document, so older revisions are
    dropped by document, not point by point. Repeated fetches of the same
    revision collapse on ``identity`` plus the step start.

    Args:
        df: Output of :func:`prepare_outage_frame`.
        identity: Columns that, with ``timestamp_utc``, name one step of one
            document (always including ``document_mrid``).

    Returns:
        The de-duplicated frame, sorted by document and time.
    """
    latest = pl.col("revision_number").fill_null(-1)
    df = df.filter(latest == latest.max().over("document_mrid"))
    key = [*identity, "timestamp_utc"]
    order = [c for c in ("document_created_at",) if c in df.columns]
    if order:
        df = df.sort(order, nulls_last=False)
    return df.unique(subset=key, keep="last").sort(key)
