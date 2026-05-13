"""Datetime parsing helpers for ENTSO-G silver transformers."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

import polars as pl

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


def filter_records_to_target_date(
    records: Iterable[dict[str, Any]],
    target_date: date,
    timestamp_fields: Iterable[str],
) -> list[dict[str, Any]]:
    """Keep records whose first parseable timestamp falls on ``target_date``."""
    filtered: list[dict[str, Any]] = []
    for record in records:
        record_date = _record_date(record, timestamp_fields)
        if record_date is None or record_date == target_date:
            filtered.append(record)
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
