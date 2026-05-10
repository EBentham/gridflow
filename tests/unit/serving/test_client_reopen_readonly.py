"""Unit tests for GridflowClient.close() and reopen_readonly() (WBH-04 / ADR-002).

These methods support the gridflow_models DuckDB-broker pattern from F11-D
(D-F11-02): the broker calls close() before a write phase and
reopen_readonly() after, so the user's bound client variable continues
to work after the broker hands control back. Both methods must be
idempotent.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pytest

from gridflow.serving.client import GridflowClient

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def tiny_duckdb(tmp_path: Path) -> Path:
    """Seed a minimal DuckDB file the client can open."""
    db_path = tmp_path / "tiny.duckdb"
    con = duckdb.connect(str(db_path), read_only=False)
    con.execute("CREATE TABLE t (x INTEGER)")
    con.execute("INSERT INTO t VALUES (1)")
    con.close()
    return db_path


def test_close_idempotent(tiny_duckdb: Path) -> None:
    """Two close() calls are safe — second is a no-op."""
    client = GridflowClient(db_path=tiny_duckdb)
    client.close()
    client.close()  # must not raise
    assert client._con is None


def test_reopen_readonly_after_close_works(tiny_duckdb: Path) -> None:
    """close() then reopen_readonly() restores a working read-only handle."""
    client = GridflowClient(db_path=tiny_duckdb)
    client.close()
    client.reopen_readonly()
    # Smoke: a real query goes through.
    df = client.query("SELECT * FROM t")
    assert df.shape == (1, 1)
    assert df.row(0) == (1,)


def test_reopen_readonly_double_call(tiny_duckdb: Path) -> None:
    """Two reopen_readonly() calls are safe — second closes-then-opens again."""
    client = GridflowClient(db_path=tiny_duckdb)
    client.reopen_readonly()
    client.reopen_readonly()
    df = client.query("SELECT * FROM t")
    assert df.row(0) == (1,)


def test_close_then_reopen_then_close(tiny_duckdb: Path) -> None:
    """Full close→reopen→close cycle leaves _con None and no exception."""
    client = GridflowClient(db_path=tiny_duckdb)
    client.close()
    client.reopen_readonly()
    client.close()
    assert client._con is None


def test_query_after_close_raises_clear_error(tiny_duckdb: Path) -> None:
    """Issuing a query after close() must surface an actionable error,
    not a bare AttributeError."""
    client = GridflowClient(db_path=tiny_duckdb)
    client.close()
    with pytest.raises(RuntimeError, match="closed"):
        client.query("SELECT 1")


def test_get_tables_after_reopen_works(tiny_duckdb: Path) -> None:
    """get_tables() must work after a close→reopen cycle (covers the
    second self._require_con() call site)."""
    client = GridflowClient(db_path=tiny_duckdb)
    client.close()
    client.reopen_readonly()
    tables = client.get_tables()
    assert "t" in tables


def test_context_manager_still_works(tiny_duckdb: Path) -> None:
    """The existing GridflowClient context manager (`with GridflowClient(...) as c`)
    relies on close(); confirm refactor didn't break it."""
    with GridflowClient(db_path=tiny_duckdb) as client:
        df = client.query("SELECT * FROM t")
        assert df.row(0) == (1,)
    # After exit, _con is None.
    assert client._con is None
