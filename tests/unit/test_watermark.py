"""CH2-04 / CH-COR-06: watermark round-trip, monotonic upsert, and the
``_resolve_incremental_start`` precedence helper.

These exercise the previously-dead ``update_watermark``/``get_watermark`` helpers
(``observability.py``) plus the new incremental-start resolver (``cli.py``):

- round-trip: writing then reading a watermark returns the same tz-aware UTC
  instant; an absent ``(source, dataset)`` pair returns ``None``.
- monotonic upsert (the C3-11 frontier-rewind guard): once ``last_end`` is at a
  later instant, a subsequent earlier write must NOT move it backward.
- ``_resolve_incremental_start``: ``None`` watermark falls back to the
  default-lookback start; a present watermark resolves to ``watermark - overlap``.

RED before CH2-04:
- monotonic test FAILS — the upsert was an unconditional ``SET last_end =
  excluded.last_end``, so the 2020 write clobbered the 2026 frontier.
- ``_resolve_incremental_start`` test FAILS — the helper does not yet exist
  (ImportError).
The round-trip / absent-pair assertions are green-before-and-after guards: a
single write already worked; they pin the contract the rest depends on.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from gridflow.cli import _resolve_incremental_start
from gridflow.observability import get_watermark, update_watermark
from gridflow.storage.duckdb import init_catalogue


@pytest.fixture
def con(tmp_path, monkeypatch: pytest.MonkeyPatch) -> duckdb.DuckDBPyConnection:  # type: ignore[no-untyped-def]
    """A DuckDB connection with the ``pipeline_watermarks`` table created.

    Gold SQL views reference silver tables absent from the tmpdir; stub
    ``_register_gold_views`` so ``init_catalogue`` only creates the metadata
    tables (mirrors the integration tests' isolated-env stub).
    """
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    db_path = tmp_path / "gridflow.duckdb"
    data_dir = tmp_path / "data"
    init_catalogue(db_path, data_dir)
    connection = duckdb.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()


def test_round_trip_returns_same_utc_instant(con: duckdb.DuckDBPyConnection) -> None:
    """``update_watermark`` then ``get_watermark`` returns the same UTC instant."""
    when = datetime(2026, 3, 1, 12, 30, tzinfo=UTC)
    update_watermark(con, "elexon", "fuelhh", when)

    got = get_watermark(con, "elexon", "fuelhh")

    assert got is not None
    assert got.tzinfo is not None, "watermark must come back tz-aware"
    assert got == when, "watermark must be the same instant it was written"


def test_absent_pair_returns_none(con: duckdb.DuckDBPyConnection) -> None:
    """A ``(source, dataset)`` pair that was never written returns ``None``."""
    assert get_watermark(con, "elexon", "never_written") is None


def test_upsert_is_monotonic_never_rewinds(con: duckdb.DuckDBPyConnection) -> None:
    """A later frontier is never moved backward by a subsequent earlier write.

    RED before CH2-04: ``SET last_end = excluded.last_end`` clobbered the 2026
    frontier with the 2020 write. GREEN with ``GREATEST(existing, excluded)``.
    """
    later = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    earlier = datetime(2020, 1, 1, 0, 0, tzinfo=UTC)

    update_watermark(con, "elexon", "fuelhh", later)
    update_watermark(con, "elexon", "fuelhh", earlier)

    got = get_watermark(con, "elexon", "fuelhh")
    assert got == later, "an out-of-order earlier write must not rewind the frontier"


def test_upsert_advances_on_later_write(con: duckdb.DuckDBPyConnection) -> None:
    """A genuinely-later write DOES advance the frontier (GREATEST picks it)."""
    first = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    second = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)

    update_watermark(con, "elexon", "fuelhh", first)
    update_watermark(con, "elexon", "fuelhh", second)

    assert get_watermark(con, "elexon", "fuelhh") == second


def test_resolve_incremental_start_none_falls_back_to_default(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """No watermark (first run) -> the default-lookback start, unchanged."""
    default_start = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)

    resolved = _resolve_incremental_start(
        con, "elexon", "fuelhh", default_start, timedelta(hours=0)
    )

    assert resolved == default_start


def test_resolve_incremental_start_present_subtracts_overlap(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A present watermark resolves to ``watermark - overlap``."""
    watermark = datetime(2026, 5, 20, 0, 0, tzinfo=UTC)
    update_watermark(con, "elexon", "fuelhh", watermark)
    default_start = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)

    resolved = _resolve_incremental_start(
        con, "elexon", "fuelhh", default_start, timedelta(hours=6)
    )

    assert resolved == watermark - timedelta(hours=6)


def test_resolve_incremental_start_zero_overlap_uses_watermark_exactly(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """With zero overlap (the behaviour-preserving default) start == watermark."""
    watermark = datetime(2026, 5, 20, 0, 0, tzinfo=UTC)
    update_watermark(con, "elexon", "fuelhh", watermark)
    default_start = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)

    resolved = _resolve_incremental_start(
        con, "elexon", "fuelhh", default_start, timedelta(hours=0)
    )

    assert resolved == watermark
