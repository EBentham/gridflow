"""CH2-04 / CH-COR-06: ``--incremental`` ingest wires the watermark frontier.

End-to-end through the CLI (CliRunner), with NO HTTP — the connector is replaced
in the registry by a minimal async-context-manager fake whose ``fetch`` returns
an empty (but successful) response list. An empty successful fetch still advances
the frontier to the requested window end (overlap covers publication lag).

Guarantees pinned here (audit C3-3 / C3-11):
- a successful ``--incremental`` ingest writes ``watermark == end_dt``;
- a FAILED ingest writes NO watermark (the frontier must never move before a
  successful write);
- a 2nd ``--incremental`` run resolves its start from the 1st run's watermark;
- a ``backfill`` over a historical range does NOT advance an existing forward
  watermark (the backfill chunk-ingests run with ``write_watermark=False``).

RED before CH2-04: ``ingest`` had no ``--incremental`` flag and never called
``update_watermark`` — no watermark row was ever written. The success/2nd-run
assertions FAIL (no row); the backfill-advance assertion FAILS once the write
exists unless backfill suppresses it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import duckdb
import pytest
from typer.testing import CliRunner

from gridflow.cli import app

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

runner = CliRunner()


def _isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    # elexon resolves its key from ELEXON_API_KEY; the fake connector ignores it.
    monkeypatch.setenv("ELEXON_API_KEY", "test-key")
    # Gold SQL views reference silver tables absent from test tmpdirs; stub out
    # (mirrors test_cli_transform_refresh.py / test_ingest_partial_fetch_warnings.py).
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return db_path


class _FakeConnector:
    """Minimal async-CM connector. Records the ``start`` of each ``fetch`` call.

    ``fetch`` returns ``responses`` (default: empty success). When ``raise_on_fetch``
    is set it raises instead, exercising the failure path. Class-level
    ``calls`` lets a test assert which start instant the 2nd run resolved.
    """

    last_skipped_units = 0
    calls: list[datetime] = []
    raise_on_fetch = False
    responses: list[RawResponse] = []

    def __init__(self, config: Any) -> None:
        self.config = config

    async def __aenter__(self) -> _FakeConnector:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def fetch(
        self, dataset: str, start: datetime, end: datetime, **params: Any
    ) -> list[RawResponse]:
        type(self).calls.append(start)
        if type(self).raise_on_fetch:
            raise RuntimeError("simulated upstream failure")
        return type(self).responses


@pytest.fixture(autouse=True)
def _reset_fake() -> None:
    """Each test starts from a clean fake-connector state."""
    _FakeConnector.calls = []
    _FakeConnector.raise_on_fetch = False
    _FakeConnector.responses = []


@pytest.fixture
def _patch_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Route ``get_connector`` (resolved at ingest call time) to the fake."""
    monkeypatch.setattr(
        "gridflow.connectors.registry.get_connector",
        lambda source_name, config: _FakeConnector(config),
    )


def _read_watermark(db_path: Path, source: str, dataset: str) -> datetime | None:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT last_end FROM pipeline_watermarks WHERE source = ? AND dataset = ?",
            [source, dataset],
        ).fetchone()
    finally:
        con.close()
    # last_end is stored as naive UTC (pytz-free TIMESTAMP); re-attach UTC so the
    # raw-storage read matches the tz-aware-UTC values the assertions compare against.
    return row[0].replace(tzinfo=UTC) if row and row[0] is not None else None


@pytest.mark.integration
def test_incremental_success_writes_watermark_at_end_dt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """A successful ``--incremental`` ingest sets ``watermark == end_dt``.

    end_dt is resolved to "now" up front; assert the written watermark is at that
    same instant (within a second of the invocation).
    """
    db_path = _isolated_env(tmp_path, monkeypatch)
    before = datetime.now(UTC)

    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    after = datetime.now(UTC)
    wm = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm is not None, "successful incremental ingest must write a watermark"
    assert before <= wm <= after, f"watermark {wm} should be end_dt (~now)"


@pytest.mark.integration
def test_failed_ingest_writes_no_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """A FAILED ingest must never advance the frontier (no watermark row).

    The frontier may only move after a successful write — this is the
    silent-bug guard for late/failed runs.
    """
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.raise_on_fetch = True

    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 1, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") is None, (
        "a failed ingest must not write a watermark"
    )


@pytest.mark.integration
def test_second_incremental_run_resolves_start_from_first_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """The 2nd ``--incremental`` run starts from the 1st run's watermark.

    With the default zero overlap, the 2nd fetch's ``start`` equals the watermark
    the 1st run wrote (== the 1st run's end_dt).
    """
    db_path = _isolated_env(tmp_path, monkeypatch)

    first = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert first.exit_code == 0, first.output
    wm_after_first = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm_after_first is not None

    second = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert second.exit_code == 0, second.output

    # calls[0] is the 1st run's start (default lookback); calls[1] is the 2nd's.
    assert len(_FakeConnector.calls) == 2
    assert _FakeConnector.calls[1] == wm_after_first, (
        "2nd incremental run must resolve its start from the 1st run's watermark"
    )


@pytest.mark.integration
def test_backfill_does_not_advance_existing_forward_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """A ``backfill`` whose range ends AFTER an existing watermark does not move it.

    Set a forward watermark, then backfill a range whose chunk-ends fall after
    that watermark. Without ``write_watermark=False`` on the chunk ingests, the
    monotonic upsert would advance the frontier to the backfill's late chunk end
    (RED). With suppression, the frontier is unchanged (GREEN).

    transform is stubbed so the backfill's silver stage is a no-op (this test
    only asserts the watermark frontier).
    """
    db_path = _isolated_env(tmp_path, monkeypatch)

    # Create the metadata tables, then establish a forward watermark deliberately
    # inside the backfill range (the table must exist before we seed it).
    from gridflow.observability import update_watermark
    from gridflow.storage.duckdb import init_catalogue

    init_catalogue(db_path, tmp_path / "data")
    existing = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
    seed_con = duckdb.connect(str(db_path))
    try:
        update_watermark(seed_con, "elexon", "fuelhh", existing)
    finally:
        seed_con.close()

    # Make transform a no-op so backfill's silver stage doesn't need bronze data.
    monkeypatch.setattr("gridflow.cli.transform", lambda **kwargs: None)

    result = runner.invoke(
        app,
        [
            "backfill",
            "elexon",
            "fuelhh",
            "--start",
            "2024-01-15",
            "--end",
            "2024-01-18",
            "--chunk-days",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output

    wm = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm == existing, (
        f"backfill must not advance the forward watermark; was {existing}, now {wm}"
    )
    # Sanity: backfill ran at least one chunk that ended after the watermark.
    assert len(_FakeConnector.calls) >= 1
