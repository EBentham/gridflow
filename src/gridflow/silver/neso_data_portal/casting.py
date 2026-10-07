"""Typing, exclusion and the capture-wide pass of the NESO generic engine.

ADR-034 P-4 and P-5, in that order and nowhere else:

- :func:`type_child` runs **once per child table**, on the vendor header. It is
  the only typing pass and the only exclusion site. Its exclusion mask is built
  from the matched epoch's column specs only (**I-2**), so a row is never
  judged by another epoch's rules, and every excluded row is tallied.
- :func:`finish_capture` runs once per (capture, family) over the concatenated
  children: it accounts the exclusions, derives ``timestamp_utc`` and
  ``published_at``, stamps the capture identity and checks the entity key.

Every function here is pure over its inputs (no filesystem, no clock): the
clocks come from the :class:`~gridflow.silver.neso_data_portal.completion.
CaptureContext`, i.e. from the bronze sidecar and the record.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import polars as pl

from gridflow.connectors.neso_data_portal.registry.record import has_issue_time, silver_columns
from gridflow.schemas.neso_data_portal import is_valid_settlement_period
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from gridflow.connectors.neso_data_portal.registry.record import (
        ColumnSpec,
        HeaderEpoch,
        SchemaRecord,
    )
    from gridflow.silver.neso_data_portal.completion import CaptureContext
    from gridflow.silver.neso_data_portal.readers import ChildTable

logger = logging.getLogger(__name__)

__all__ = [
    "CAPTURE_STAMP_COLUMNS",
    "AllRowsExcludedError",
    "DuplicateEntityKeyError",
    "ExclusionTally",
    "HeaderEpochError",
    "IssueTimeError",
    "epoch_for",
    "TypedChild",
    "finish_capture",
    "record_columns",
    "record_dtypes",
    "type_child",
]

CAPTURE_STAMP_COLUMNS: tuple[str, ...] = (
    "timestamp_utc",
    "published_at",
    "bronze_capture_id",
    "capture_written_at",
)
"""The columns P-5 adds after the typed record columns, in this order."""

_SAMPLE_LIMIT = 5
_UTC_DATETIME = pl.Datetime("us", "UTC")
_POLARS_TYPES: dict[str, pl.DataType] = {
    "string": pl.Utf8(),
    "int64": pl.Int64(),
    "float64": pl.Float64(),
    "date": pl.Date(),
    "datetime": _UTC_DATETIME,
}


class HeaderEpochError(Exception):
    """A table's header matches none of the record's epochs."""


class IssueTimeError(Exception):
    """An epoch with an issue recipe yielded a null issue time (B2)."""


class AllRowsExcludedError(Exception):
    """A body with rows had every row excluded."""


class DuplicateEntityKeyError(Exception):
    """Two rows of one capture share the entity key: the key is wrong."""


@dataclass
class ExclusionTally:
    """Excluded-row counts per rule plus up to five sample keys.

    Attributes:
        counts: Rule -> rows excluded (``null``, ``range``, ``settlement_period``);
            each row counts once, under the first rule it fails.
        samples: Up to five ``col=value`` renderings of excluded entity keys.
    """

    counts: dict[str, int] = field(default_factory=dict)
    samples: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Rows excluded under every rule."""
        return sum(self.counts.values())

    def merge(self, other: ExclusionTally) -> None:
        """Add ``other``'s counts and samples (samples stay capped)."""
        for rule, count in other.counts.items():
            self.counts[rule] = self.counts.get(rule, 0) + count
        room = _SAMPLE_LIMIT - len(self.samples)
        if room > 0:
            self.samples.extend(other.samples[:room])


@dataclass(frozen=True)
class TypedChild:
    """One child table after P-4: typed, renamed, issue-stamped, filtered."""

    frame: pl.DataFrame
    tally: ExclusionTally


def record_dtypes(record: SchemaRecord) -> dict[str, str]:
    """Every output column of P-4/P-5 -> its record dtype, in output order.

    The typed silver columns in first-appearance order across epochs, then
    ``issue_time`` (when any epoch declares one), ``child_id`` (container
    readers), then :data:`CAPTURE_STAMP_COLUMNS`.
    """
    from gridflow.silver.neso_data_portal.readers import CONTAINER_READERS

    out: dict[str, str] = {name: spec.dtype for name, spec in silver_columns(record).items()}
    if has_issue_time(record):
        out["issue_time"] = "datetime"
    if record.reader in CONTAINER_READERS:
        out["child_id"] = "string"
    out.update(
        {
            "timestamp_utc": "datetime",
            "published_at": "datetime",
            "bronze_capture_id": "string",
            "capture_written_at": "datetime",
        }
    )
    return out


def record_columns(record: SchemaRecord) -> tuple[str, ...]:
    """The ordered column tuple of every P-5 output of ``record`` (P-11/P-12 reuse it)."""
    return tuple(record_dtypes(record))


def epoch_for(record: SchemaRecord, header: tuple[str, ...]) -> HeaderEpoch:
    """Return the epoch whose header equals ``header`` exactly (P-4 step 1)."""
    for epoch in record.epochs:
        if epoch.header == header:
            return epoch
    raise HeaderEpochError(
        f"header {list(header)} matches none of the record's {len(record.epochs)} epoch(s): "
        f"{[list(epoch.header) for epoch in record.epochs]}"
    )


def _cast(spec: ColumnSpec) -> pl.Expr:
    """The strict cast of one vendor column (null tokens already applied)."""
    col = pl.col(spec.source)
    if spec.dtype == "string":
        return col.cast(pl.Utf8)
    if spec.dtype == "int64":
        return col.cast(pl.Int64, strict=True)
    if spec.dtype == "float64":
        return col.cast(pl.Float64, strict=True)
    assert spec.format is not None
    if spec.dtype == "date":
        return col.str.strptime(pl.Date, spec.format, strict=True)
    if spec.zone is None:
        parsed = col.str.strptime(pl.Datetime("us"), spec.format, strict=True)
        return parsed.dt.convert_time_zone("UTC").cast(_UTC_DATETIME)
    naive = col.str.strptime(pl.Datetime("us"), spec.format, strict=True)
    if spec.zone == "UTC":
        return naive.dt.replace_time_zone("UTC").cast(_UTC_DATETIME)
    assert spec.ambiguous is not None
    return (
        naive.dt.replace_time_zone(spec.zone, ambiguous=spec.ambiguous, non_existent="raise")
        .dt.convert_time_zone("UTC")
        .cast(_UTC_DATETIME)
    )


def _null_tokens(spec: ColumnSpec) -> pl.Expr:
    col = pl.col(spec.source)
    if not spec.null_tokens:
        return col
    return pl.when(col.is_in(list(spec.null_tokens))).then(None).otherwise(col).alias(spec.source)


def _issue_time(epoch: HeaderEpoch, ctx: CaptureContext) -> pl.Expr:
    issue = epoch.issue
    if issue.kind == "data_column":
        assert issue.column is not None
        return pl.col(issue.column).cast(_UTC_DATETIME)
    if issue.kind == "filename_token":
        assert issue.pattern is not None and issue.format is not None
        match = re.fullmatch(issue.pattern, ctx.resource_filename)
        token: datetime | None = None
        if match is not None:
            try:
                token = datetime.strptime(match.group(1), issue.format).replace(tzinfo=UTC)
            except ValueError:
                token = None
        if token is None:
            raise IssueTimeError(
                f"{ctx.capture_id}: resource_filename {ctx.resource_filename!r} yields no "
                f"issue time under {issue.pattern!r} / {issue.format!r}"
            )
        return pl.lit(token).cast(_UTC_DATETIME)
    return pl.lit(None, dtype=_UTC_DATETIME)


def _sample(frame: pl.DataFrame, key: tuple[str, ...]) -> list[str]:
    present = [column for column in key if column in frame.columns]
    rows = frame.head(_SAMPLE_LIMIT).select(present).to_dicts()
    return [", ".join(f"{name}={row[name]}" for name in present) for row in rows]


def type_child(table: ChildTable, record: SchemaRecord, ctx: CaptureContext) -> TypedChild:
    """Type one child table under its own epoch and exclude its invalid rows (P-4).

    Args:
        table: An all-``Utf8`` table from a reader.
        record: The family's record.
        ctx: The capture's context (filename for a token issue time).

    Returns:
        The typed frame (columns in record order, see :func:`record_dtypes`
        minus the P-5 stamps) and its exclusion tally.

    Raises:
        HeaderEpochError: The header matches no epoch.
        IssueTimeError: A null issue time under a recipe other than ``none``.
        polars.exceptions.PolarsError: A value outside the declared null
            tokens did not cast (D-41: the capture fails, nothing is coerced).
    """
    epoch = epoch_for(record, table.header)
    frame = table.frame
    tokened = [_null_tokens(spec) for spec in epoch.columns if spec.null_tokens]
    if tokened:
        frame = frame.with_columns(tokened)
    frame = frame.select([_cast(spec).alias(spec.name) for spec in epoch.columns])

    ordered = silver_columns(record)
    present = set(frame.columns)
    frame = frame.select(
        [
            pl.col(name)
            if name in present
            else pl.lit(None, dtype=_POLARS_TYPES[spec.dtype]).alias(name)
            for name, spec in ordered.items()
        ]
    )

    if has_issue_time(record):
        frame = frame.with_columns(_issue_time(epoch, ctx).alias("issue_time"))
        if epoch.issue.kind != "none" and frame["issue_time"].null_count():
            raise IssueTimeError(
                f"{ctx.capture_id}: {frame['issue_time'].null_count()} row(s) have no issue "
                f"time under recipe {epoch.issue.kind!r}"
            )

    frame, tally = _exclude(frame, epoch, record)

    from gridflow.silver.neso_data_portal.readers import CONTAINER_READERS

    if record.reader in CONTAINER_READERS:
        frame = frame.with_columns(pl.lit(table.child_id, dtype=pl.Utf8).alias("child_id"))
    return TypedChild(frame=frame, tally=tally)


def _exclude(
    frame: pl.DataFrame, epoch: HeaderEpoch, record: SchemaRecord
) -> tuple[pl.DataFrame, ExclusionTally]:
    """I-2: one vectorised mask from THIS epoch's specs; tally by first failing rule."""
    tally = ExclusionTally()
    if frame.height == 0:
        return frame, tally
    null_terms = [pl.col(spec.name).is_null() for spec in epoch.columns if not spec.nullable]
    range_terms: list[pl.Expr] = []
    for spec in epoch.columns:
        if spec.min is not None:
            range_terms.append(pl.col(spec.name).is_not_null() & (pl.col(spec.name) < spec.min))
        if spec.max is not None:
            range_terms.append(pl.col(spec.name).is_not_null() & (pl.col(spec.name) > spec.max))
    null_mask = pl.any_horizontal(null_terms) if null_terms else pl.lit(False)
    range_mask = pl.any_horizontal(range_terms) if range_terms else pl.lit(False)
    rule = (
        pl.when(null_mask)
        .then(pl.lit("null"))
        .when(range_mask)
        .then(pl.lit("range"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )
    frame = frame.with_columns(rule.alias("__exclusion_rule"))

    temporal = record.temporal
    if temporal.kind == "sp_pair":
        date_col, period_col = temporal.date_column, temporal.period_column
        assert date_col is not None and period_col is not None
        pairs = (
            frame.filter(pl.col("__exclusion_rule").is_null()).select(date_col, period_col).unique()
        )
        invalid = [
            (row[date_col], row[period_col])
            for row in pairs.to_dicts()
            if not is_valid_settlement_period(row[date_col], row[period_col])
        ]
        if invalid:
            bad = pl.DataFrame(
                {
                    date_col: [pair[0] for pair in invalid],
                    period_col: [pair[1] for pair in invalid],
                    "__bad_pair": [True] * len(invalid),
                },
                schema={date_col: pl.Date, period_col: pl.Int64, "__bad_pair": pl.Boolean},
            )
            frame = (
                frame.join(bad, on=[date_col, period_col], how="left", maintain_order="left")
                .with_columns(
                    pl.when(pl.col("__exclusion_rule").is_null() & pl.col("__bad_pair"))
                    .then(pl.lit("settlement_period"))
                    .otherwise(pl.col("__exclusion_rule"))
                    .alias("__exclusion_rule")
                )
                .drop("__bad_pair")
            )

    excluded = frame.filter(pl.col("__exclusion_rule").is_not_null())
    if excluded.height:
        counts = excluded.group_by("__exclusion_rule").len()
        tally.counts = {str(row["__exclusion_rule"]): int(row["len"]) for row in counts.to_dicts()}
        tally.samples = _sample(excluded, record.entity_key)
    kept = frame.filter(pl.col("__exclusion_rule").is_null()).drop("__exclusion_rule")
    return kept, tally


def finish_capture(
    frames: list[pl.DataFrame],
    tally: ExclusionTally,
    record: SchemaRecord,
    ctx: CaptureContext,
    key: str,
) -> pl.DataFrame:
    """The capture-wide pass over one (capture, family)'s typed children (P-5).

    Args:
        frames: The P-4 frames of every child, in reader order.
        tally: Their merged exclusion tally.
        record: The family's record.
        ctx: The capture's context.
        key: The family key (log context).

    Returns:
        The frame with columns :func:`record_columns`, ready for the
        bitemporal stamp.

    Raises:
        AllRowsExcludedError: The body had rows and every one was excluded.
        DuplicateEntityKeyError: Two rows share the entity key.
    """
    frame = pl.concat(frames, how="vertical") if len(frames) > 1 else frames[0]
    if tally.total:
        logger.warning(
            "neso_data_portal/%s: capture %s excluded %d row(s) by rule %s; sample keys: %s",
            key,
            ctx.capture_id,
            tally.total,
            dict(sorted(tally.counts.items())),
            tally.samples,
        )
    if frame.height == 0:
        raise AllRowsExcludedError(
            f"neso_data_portal/{key}: every row of capture {ctx.capture_id} was excluded "
            f"({dict(sorted(tally.counts.items()))}); nothing is written"
        )

    frame = _with_timestamp_utc(frame, record, ctx)
    if record.vintage == "issue_time_evidenced":
        published = pl.col("issue_time").cast(_UTC_DATETIME)
    else:
        published = pl.lit(ctx.published_at, dtype=_UTC_DATETIME)
    frame = frame.with_columns(
        published.alias("published_at"),
        pl.lit(ctx.capture_id, dtype=pl.Utf8).alias("bronze_capture_id"),
        pl.lit(ctx.capture_written_at, dtype=_UTC_DATETIME).alias("capture_written_at"),
    ).select(record_columns(record))

    duplicates = frame.select(record.entity_key).is_duplicated().sum()
    if duplicates:
        raise DuplicateEntityKeyError(
            f"neso_data_portal/{key}: capture {ctx.capture_id} has {duplicates} row(s) sharing "
            f"an entity key {list(record.entity_key)}; the key is wrong for this data"
        )
    return frame


def _with_timestamp_utc(
    frame: pl.DataFrame, record: SchemaRecord, ctx: CaptureContext
) -> pl.DataFrame:
    """Add ``timestamp_utc`` per the temporal recipe (P-5 step 2).

    Settlement and day recipes convert each DISTINCT key once and join the
    instants back, so the per-row cost is a join, not a Python call.
    """
    temporal = record.temporal
    if temporal.kind in ("utc_instant", "local_instant"):
        assert temporal.column is not None
        return frame.with_columns(
            pl.col(temporal.column).cast(_UTC_DATETIME).alias("timestamp_utc")
        )
    if temporal.kind == "none":
        return frame.with_columns(
            pl.lit(ctx.capture_written_at, dtype=_UTC_DATETIME).alias("timestamp_utc")
        )
    date_col = temporal.date_column
    assert date_col is not None
    if temporal.kind == "sp_pair":
        period_col = temporal.period_column
        assert period_col is not None
        keys = frame.select(date_col, period_col).unique()
        instants = [
            settlement_period_to_utc(row[date_col], row[period_col]) for row in keys.to_dicts()
        ]
        on = [date_col, period_col]
    else:
        keys = frame.select(date_col).unique()
        instants = [
            settlement_period_to_utc(
                value if temporal.kind == "date_sp1" else value.replace(day=1), 1
            )
            for value in keys.get_column(date_col).to_list()
        ]
        on = [date_col]
    lookup = keys.with_columns(pl.Series("timestamp_utc", instants, dtype=_UTC_DATETIME))
    return frame.join(lookup, on=on, how="left", maintain_order="left")
