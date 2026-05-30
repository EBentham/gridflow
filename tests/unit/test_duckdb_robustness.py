"""Behavioural tests for DuckDB storage robustness (issue 14).

- View DDL must not break on an apostrophe in the parquet path or a space /
  hyphen in the view name (filesystem-derived identifiers + path literals).
- A malformed-DDL / binder failure must be visible (WARNING+/raise) even in
  production, while the benign "parquet not yet written" swallow is preserved.
- get_connection must fail fast on a non-transient error (missing read-only
  DB file), not retry it 8x (~255s), and surface the real DuckDB cause.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb
import polars as pl
import pytest

from gridflow.storage.duckdb import (
    _try_create_view,
    get_connection,
)


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


# --- f-string DDL: special characters in path / view name -------------------


def test_view_registers_with_apostrophe_in_path(tmp_path: Path) -> None:
    """An apostrophe in the parquet path must not corrupt the DDL.

    Pre-fix: the raw '{pattern}' literal closes early on the apostrophe ->
    ParserException. Post-fix: the path literal is escaped and the view
    registers and is queryable.
    """
    obrien = tmp_path / "O'Brien" / "data"
    _write_parquet(pl.DataFrame({"value": [7]}), obrien / "x.parquet")
    pattern = str(obrien / "**" / "*.parquet").replace("\\", "/")

    con = duckdb.connect(":memory:")
    try:
        _try_create_view(con, "silver_obrien", pattern)
        rows = con.execute("SELECT value FROM silver_obrien").fetchall()
    finally:
        con.close()
    assert rows == [(7,)]


def test_view_registers_with_space_or_hyphen_in_name(tmp_path: Path) -> None:
    """A space/hyphen in the view name must not break the unquoted identifier.

    Pre-fix: unquoted `{view_name}` -> syntax error. Post-fix: the identifier
    is quoted and the view registers (queryable via a quoted reference).
    """
    src = tmp_path / "data"
    _write_parquet(pl.DataFrame({"value": [9]}), src / "x.parquet")
    pattern = str(src / "**" / "*.parquet").replace("\\", "/")

    con = duckdb.connect(":memory:")
    try:
        _try_create_view(con, "silver_my-data set", pattern)
        rows = con.execute('SELECT value FROM "silver_my-data set"').fetchall()
    finally:
        con.close()
    assert rows == [(9,)]


def test_view_name_with_embedded_quote_is_safe(tmp_path: Path) -> None:
    """An embedded double-quote in the view name must not break the identifier."""
    src = tmp_path / "data"
    _write_parquet(pl.DataFrame({"value": [3]}), src / "x.parquet")
    pattern = str(src / "**" / "*.parquet").replace("\\", "/")

    con = duckdb.connect(":memory:")
    try:
        _try_create_view(con, 'silver_a"b', pattern)
        rows = con.execute('SELECT value FROM "silver_a""b"').fetchall()
    finally:
        con.close()
    assert rows == [(3,)]


# --- swallow discrimination: malformed DDL visible, benign absent silent ----


def test_production_swallows_benign_absent_parquet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The benign 'parquet not yet written' case stays a silent debug swallow
    in production (F15-D contract preserved).
    """
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)
    pattern = str(tmp_path / "missing" / "**" / "*.parquet").replace("\\", "/")

    con = duckdb.connect(":memory:")
    try:
        with caplog.at_level(logging.WARNING):
            _try_create_view(con, "silver_absent", pattern)  # must not raise
        # No WARNING+ for the benign absent case.
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    finally:
        con.close()


def test_production_surfaces_malformed_ddl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A malformed-DDL/binder failure must be visible in production (WARNING+).

    Pre-fix: any exception is logged at DEBUG and swallowed, so a deterministic
    DDL bug is invisible until a later SELECT fails opaquely. Post-fix: the
    binder/parser class is surfaced at WARNING or above (or raised).
    """
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    # A corrupt/torn parquet body (files present, but not valid parquet) is a
    # non-benign failure — NOT the "no files found" absent case. This is the
    # silent-corruption scenario the swallow must surface.
    corrupt = tmp_path / "year=2024" / "month=01"
    corrupt.mkdir(parents=True)
    (corrupt / "torn.parquet").write_text("not a parquet file")
    pattern = str(tmp_path / "**" / "*.parquet").replace("\\", "/")

    con = duckdb.connect(":memory:")
    raised = False
    try:
        with caplog.at_level(logging.WARNING):
            try:
                _try_create_view(con, "silver_bad", pattern)
            except Exception:
                raised = True
        surfaced = [r for r in caplog.records if r.levelno >= logging.WARNING]
    finally:
        con.close()

    # Either it raised, or it surfaced at WARNING+. What it must NOT do is be
    # silently swallowed at DEBUG.
    assert raised or surfaced, (
        "a non-benign view-registration failure must be raised or logged at WARNING+"
    )


# --- retry classification: fail fast on non-transient -----------------------


def test_get_connection_fails_fast_on_missing_readonly_db(tmp_path: Path) -> None:
    """A read_only open of a missing DB file is non-transient: fail fast.

    Pre-fix: blanket except retries 8x with exponential backoff (~255s) before
    raising a bare RuntimeError. Post-fix: surfaces in well under the backoff
    with the real DuckDB IOException as the cause.
    """
    missing = tmp_path / "does_not_exist.duckdb"

    start = time.monotonic()
    with pytest.raises(Exception) as excinfo:
        get_connection(missing, read_only=True)
    elapsed = time.monotonic() - start

    # Must not have slept through the full 8x backoff (~255s). Generous bound.
    assert elapsed < 10, f"non-transient error took {elapsed:.1f}s (expected fail-fast)"
    # The real cause must be a DuckDB error, not buried in a bare RuntimeError.
    chain = [excinfo.value, *_causes(excinfo.value)]
    assert any(isinstance(e, duckdb.Error) for e in chain), (
        f"expected a duckdb.Error in the chain, got {[type(e).__name__ for e in chain]}"
    )


def _causes(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    cur = exc.__cause__ or exc.__context__
    while cur is not None:
        out.append(cur)
        cur = cur.__cause__ or cur.__context__
    return out


def test_get_connection_opens_writable_db(tmp_path: Path) -> None:
    """A normal writable open still works (no regression)."""
    db_path = tmp_path / "ok.duckdb"
    con = get_connection(db_path)
    try:
        assert con.execute("SELECT 1").fetchone() == (1,)
    finally:
        con.close()
