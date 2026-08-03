"""Datetime parsing helpers for ENTSO-G silver transformers."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

import polars as pl

logger = logging.getLogger(__name__)

_EMPTY_DATETIME_VALUES = {"", "-", "n/a", "na", "null", "none"}
_FALLBACK_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%b %d %Y %I:%M%p",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
)


def parse_entsog_datetime(value: Any) -> datetime | None:
    """Parse ENTSO-G datetime strings, returning ``None`` for placeholders."""
    if value is None:
        return None
    parsed: datetime | None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text = str(value).strip()
        if text.lower() in _EMPTY_DATETIME_VALUES:
            return None
        parsed = _parse_datetime_text(text)
        if parsed is None:
            return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_entsog_datetime_expr(column: str) -> pl.Expr:
    """Return a Polars expression that tolerates ENTSO-G placeholder dates."""
    return pl.col(column).map_elements(
        parse_entsog_datetime,
        return_dtype=pl.Datetime("us", "UTC"),
    )


def partition_records_to_target_date(
    records: Iterable[dict[str, Any]],
    target_date: date,
    timestamp_fields: Iterable[str],
) -> tuple[list[dict[str, Any]], int]:
    """Pure core of :func:`filter_records_to_target_date` -- no logging.

    Row-wise: each record's verdict depends only on that record's own fields,
    never on the surrounding set, so callers may apply it per bronze FILE and
    aggregate the undated count into ONE warning instead of emitting one per
    file (R2-g, the lockstep read path).

    Args:
        records: Raw ENTSO-G records to partition.
        target_date: The date being read.
        timestamp_fields: Field names to probe, in priority order.

    Returns:
        ``(kept, undated_count)`` -- ``kept`` holds records dated
        ``target_date`` plus every undated record (fail-open, see the wrapper);
        ``undated_count`` counts the undated ones among them.
    """
    kept: list[dict[str, Any]] = []
    undated_count = 0
    for record in records:
        record_date = _record_date(record, timestamp_fields)
        if record_date is None:
            kept.append(record)
            undated_count += 1
        elif record_date == target_date:
            kept.append(record)
    return kept, undated_count


def log_undated_records(
    undated_count: int,
    target_date: date,
    *,
    source: str,
    dataset: str,
) -> None:
    """Emit the single bounded WARNING for undated ENTSO-G records.

    Shared by :func:`filter_records_to_target_date` and by the lockstep read
    path, which aggregates across bronze files before calling this once -- so
    both produce the identical message, and neither emits one line per record
    or one per file. No-op when ``undated_count`` is zero.

    Args:
        undated_count: Total undated records kept across the whole call.
        target_date: The date being read.
        source: Caller's ``self.source`` (message only).
        dataset: Caller's ``self.dataset`` (message only).
    """
    if not undated_count:
        return
    logger.warning(
        "%s/%s: %d record(s) had no parseable date for target_date %s; "
        "kept (fail-open hedge) rather than dropped",
        source,
        dataset,
        undated_count,
        target_date,
    )


def filter_records_to_target_date(
    records: Iterable[dict[str, Any]],
    target_date: date,
    timestamp_fields: Iterable[str],
    *,
    source: str,
    dataset: str,
) -> list[dict[str, Any]]:
    """Keep records whose first parseable timestamp falls on ``target_date``.

    Fail-open by design (R2-B / F-05): a record with no parseable timestamp in
    ``timestamp_fields`` is KEPT rather than dropped, since bronze is
    known-exact for ``entsog`` (``_EXACT_PARTITION_ONLY_SOURCES``) — an undated
    record here is a genuinely undated ENTSO-G record, not evidence of a
    wrong-day fabrication.

    **That premise became TRUE in R2-g, and was not when R2-B shipped it.** On
    master before R2-g, ``entsog`` was NOT in ``_EXACT_PARTITION_ONLY_SOURCES``,
    so ``PhysicalFlowsTransformer.read_bronze`` could still resolve a covering
    partition up to 35 days earlier — and an undated record there genuinely
    WAS potential evidence of a wrong-day read. R2-g adds ``entsog`` to that
    frozenset (F-05's open half) and removes the vintage path's dependence on
    the same fallback, which is what makes keeping an undated record safe.
    This is the mechanical reason F-05's two halves are one finding.

    What F-05 closes is the previous *silence*: undated
    records are now counted and surfaced in exactly ONE bounded WARNING per
    call, never one log line per record (mirrors the GIE precedent,
    ``silver/gie/agsi.py::_filter_news_records_to_target_date``).

    Args:
        records: Raw ENTSO-G records to filter.
        target_date: The date being read.
        timestamp_fields: Field names to probe, in priority order.
        source: Caller's ``self.source`` (keyword-only; used only in the
            warning message).
        dataset: Caller's ``self.dataset`` (keyword-only; used only in the
            warning message).

    Returns:
        Records dated ``target_date`` plus any undated records (kept, not
        dropped). Records dated to a different day are dropped.
    """
    filtered, undated_count = partition_records_to_target_date(
        records, target_date, timestamp_fields
    )
    log_undated_records(undated_count, target_date, source=source, dataset=dataset)
    return filtered


def _parse_datetime_text(text: str) -> datetime | None:
    candidates = [text.replace("Z", "+00:00")]
    if " " in text and "T" not in text:
        candidates.append(text.replace(" ", "T"))

    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass

    for fmt in _FALLBACK_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def _record_date(
    record: dict[str, Any],
    timestamp_fields: Iterable[str],
) -> date | None:
    for field in timestamp_fields:
        if field not in record:
            continue
        value = record[field]
        # Use the LOCAL date from the original string to avoid midnight-UTC boundary
        # shifts. E.g. "2026-05-01T00:00:00+02:00" → local date 2026-05-01, not
        # the UTC equivalent 2026-04-30.
        if isinstance(value, str):
            text = value.strip()
            if text:
                try:
                    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
                except ValueError:
                    pass
        # Fallback for non-string types (pre-parsed datetimes, dates).
        parsed = parse_entsog_datetime(value)
        if parsed is not None:
            return parsed.date()
    return None
