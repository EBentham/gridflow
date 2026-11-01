"""issue-19 site A: ``_resolve_dates`` must CONVERT an offset-bearing bound to
its UTC instant (not relabel it via ``.replace(tzinfo=...)``) and REJECT a naive
datetime (not silently stamp UTC), per the tz-aware-UTC contract. A bare
calendar date is unambiguous and taken as midnight UTC.

The two cutoff-relevant parsers (``_resolve_dates`` here, ``_parse_iso_utc`` in
gridflow_models) previously had no mapped tests at all (issue-19 test-efficacy
note).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
import typer

from gridflow.cli import _resolve_dates


def test_resolve_dates_converts_offset_not_relabels() -> None:
    """``--start 2026-02-01T00:00:00+05:00`` is the instant
    ``2026-01-31T19:00:00Z``, not ``2026-02-01T00:00:00Z``.

    FAILS against the pre-fix ``.replace(tzinfo=utc)``, which overwrote the
    offset and mis-anchored the ingest window by 5 hours.
    """
    start_dt, _ = _resolve_dates("2026-02-01T00:00:00+05:00", None, None, 24)
    assert start_dt == datetime(2026, 1, 31, 19, 0, 0, tzinfo=UTC)


def test_resolve_dates_rejects_naive_datetime() -> None:
    """A naive ``--start`` datetime (wall-clock, no offset) is rejected, not
    silently relabelled UTC (the BST instant-shift hazard)."""
    with pytest.raises(typer.BadParameter):
        _resolve_dates("2026-02-01T00:00:00", None, None, 24)


def test_resolve_dates_bare_date_is_midnight_utc() -> None:
    """A bare calendar date is unambiguous -> midnight UTC (ergonomic, no BST
    hazard) — preserves common ``--start 2026-02-01`` usage."""
    start_dt, _ = _resolve_dates("2026-02-01", None, None, 24)
    assert start_dt == datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)


def test_bare_end_date_is_inclusive() -> None:
    """``--end 2026-02-07`` means "through 7 Feb", so the exclusive bound is
    8 Feb 00:00Z; every connector, the transform and the build read that one
    half-open window."""
    _, end_dt = _resolve_dates("2026-02-01", "2026-02-07", None, 24)
    assert end_dt == datetime(2026, 2, 8, tzinfo=UTC)


def test_end_with_a_time_is_an_exclusive_instant() -> None:
    _, end_dt = _resolve_dates("2026-02-01", "2026-02-07T06:00Z", None, 24)
    assert end_dt == datetime(2026, 2, 7, 6, tzinfo=UTC)


def test_end_without_start_ends_the_default_lookback() -> None:
    """An ``--end`` given alone is honoured, not silently replaced by now."""
    assert _resolve_dates(None, "2020-01-02", None, 24) == (
        datetime(2020, 1, 2, tzinfo=UTC),
        datetime(2020, 1, 3, tzinfo=UTC),
    )


def test_end_with_last_ends_the_lookback() -> None:
    assert _resolve_dates(None, "2020-01-02", "48h", 24) == (
        datetime(2020, 1, 1, tzinfo=UTC),
        datetime(2020, 1, 3, tzinfo=UTC),
    )
