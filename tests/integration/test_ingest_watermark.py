"""CH2-04 / CH-COR-06 / R3-F04: ``--incremental`` ingest wires the watermark frontier.

End-to-end through the CLI (CliRunner), with NO HTTP — the connector is replaced
in the registry by a minimal async-context-manager fake whose ``fetch`` returns a
configurable response list.

Guarantees pinned here (audit C3-3 / C3-11; R3-F04 evidence guard):
- a ``--incremental`` ingest that OBSERVES DATA advances ``watermark == end_dt``;
- an EMPTY fetch (no data-bearing responses) does NOT advance the frontier —
  R3-F04 reverses the prior CH2-04 behaviour (an empty fetch used to advance to
  end_dt, which only self-heals with a non-zero overlap and the shipped overlap
  was 0);
- a PARTIAL fetch (``last_skipped_units`` > 0) does NOT advance to the requested
  end;
- an all-``http_status`` >= 400 fetch (ENTSO-G's "No result found" 404 shape)
  carries no evidence and does NOT advance;
- a FAILED ingest writes NO watermark (the frontier must never move before a
  successful write);
- a 2nd ``--incremental`` run resolves its start from the 1st run's watermark
  minus the configured overlap (default 72h);
- a ``backfill`` over a historical range does NOT advance an existing forward
  watermark (the backfill chunk-ingests run with ``write_watermark=False``).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import duckdb
import httpx
import pytest
import respx
from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.connectors.base import RawResponse

if TYPE_CHECKING:
    from pathlib import Path


def _data_response() -> RawResponse:
    """A minimal successful (http_status=200) data-bearing response.

    Under the R3-F04 evidence guard this is what advances the watermark; the fake
    connector defaults to an empty list (no evidence, no advance).
    """
    return RawResponse(
        body=b'{"data": [{"x": 1}]}',
        content_type="application/json",
        source="elexon",
        dataset="fuelhh",
        http_status=200,
    )


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
    _FakeConnector.last_skipped_units = 0


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
    """A ``--incremental`` ingest that observes data sets ``watermark == end_dt``.

    end_dt is resolved to "now" up front; assert the written watermark is at that
    same instant (within a second of the invocation).
    """
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]
    before = datetime.now(UTC)

    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    after = datetime.now(UTC)
    wm = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm is not None, "incremental ingest that observed data must write a watermark"
    assert before <= wm <= after, f"watermark {wm} should be end_dt (~now)"


@pytest.mark.integration
def test_incremental_empty_fetch_does_not_advance_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """R3-F04: an EMPTY fetch (no responses) leaves the watermark unchanged.

    Reverses the prior CH2-04 behaviour where an empty fetch advanced to end_dt —
    advancing past a window that had no data yet permanently strands it once the
    data lands.
    """
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = []  # empty but successful

    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") is None, (
        "an empty fetch must not advance the frontier (R3-F04)"
    )


@pytest.mark.integration
def test_incremental_partial_fetch_does_not_advance_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """R3-F04: a PARTIAL fetch (skipped units) does NOT advance the frontier.

    Data-bearing responses are present, but ``last_skipped_units`` > 0 means the
    window is incomplete; advancing to end_dt would strand the skipped units.
    """
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]
    _FakeConnector.last_skipped_units = 2

    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") is None, (
        "a partial fetch must not advance the frontier (R3-F04)"
    )


@pytest.mark.integration
def test_incremental_all_no_result_responses_do_not_advance_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """R3-F04: a fetch whose only responses are http_status >= 400 (ENTSO-G's
    "No result found" 404 short-circuit) carries no evidence — no advance."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [
        RawResponse(
            body=b'{"message": "No result found"}',
            content_type="application/json",
            source="elexon",
            dataset="fuelhh",
            http_status=404,
        )
    ]

    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") is None, (
        "an all-4xx (no-result) fetch must not advance the frontier (R3-F04)"
    )


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


@respx.mock
@pytest.mark.integration
def test_incremental_pn_http_failure_preserves_bronze_and_watermark(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed real PN connector fetch is atomic before Bronze and watermark writes."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    monkeypatch.setenv("GRIDFLOW_INCREMENTAL_OVERLAP_HOURS", "0")

    from gridflow.observability import update_watermark
    from gridflow.storage.duckdb import init_catalogue

    data_dir = tmp_path / "data"
    init_catalogue(db_path, data_dir)
    existing = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
    con = duckdb.connect(str(db_path))
    try:
        update_watermark(con, "elexon", "pn", existing)
    finally:
        con.close()

    fallback_start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    requested_end = datetime(2024, 1, 15, 23, 0, tzinfo=UTC)
    monkeypatch.setattr(
        "gridflow.pipeline.runner.resolve_dates",
        lambda *_args, **_kwargs: (fallback_start, requested_end),
    )

    async def no_sleep(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", no_sleep)
    requests: list[tuple[str, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        settlement_date = request.url.params["settlementDate"]
        period = int(request.url.params["settlementPeriod"])
        requests.append((settlement_date, period))
        if period == 1:
            return httpx.Response(
                200,
                json={
                    "data": [{"settlementPeriod": 1}],
                    "metadata": {"currentPage": 1, "totalPages": 1},
                },
            )
        return httpx.Response(429, json={"error": "rate limited"})

    respx.get(url__startswith="https://data.elexon.co.uk/bmrs/api/v1/").mock(side_effect=handler)

    result = runner.invoke(app, ["ingest", "elexon", "pn", "--incremental"])

    assert result.exit_code == 1, result.output
    assert "Ingestion failed" in result.output
    assert {settlement_date for settlement_date, _period in requests} == {"2024-01-15"}
    assert requests.count(("2024-01-15", 1)) == 1
    assert requests.count(("2024-01-15", 2)) == 5
    assert len(requests) == 6

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        status = con.execute(
            """
            SELECT status
            FROM pipeline_runs
            WHERE source = ? AND dataset = ? AND operation = 'ingest'
            ORDER BY started_at DESC
            LIMIT 1
            """,
            ["elexon", "pn"],
        ).fetchone()
    finally:
        con.close()
    assert status == ("failed",)

    bronze_dir = data_dir / "bronze" / "elexon" / "pn"
    assert not list(bronze_dir.rglob("raw_*.json"))
    assert not list(bronze_dir.rglob("raw_*.meta.json"))
    assert _read_watermark(db_path, "elexon", "pn") == existing


@pytest.mark.integration
def test_second_incremental_run_resolves_start_from_first_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """The 2nd ``--incremental`` run starts from the 1st run's watermark minus overlap.

    Run 1 observes data and advances the watermark to its end_dt; run 2 resolves
    its start to ``watermark - incremental_overlap_hours`` (the 72h default).
    """
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]

    first = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert first.exit_code == 0, first.output
    wm_after_first = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm_after_first is not None

    second = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert second.exit_code == 0, second.output

    # calls[0] is the 1st run's start (default lookback); calls[1] is the 2nd's,
    # resolved from the 1st run's watermark minus the 72h default overlap.
    assert len(_FakeConnector.calls) == 2
    assert _FakeConnector.calls[1] == wm_after_first - timedelta(hours=72), (
        "2nd incremental run must resolve its start from watermark - overlap (72h)"
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


# --------------------------------------------------------------------------- #
# R2-C Task 2: F-09 wiring -- the unified write gate, the one aggregated
# record, and completed_with_warnings status (D-4/D-17/D-22/D-23).
# --------------------------------------------------------------------------- #


def _seed_watermark(
    db_path: Path, data_dir: Path, source: str, dataset: str, when: datetime
) -> None:
    from gridflow.observability import update_watermark
    from gridflow.storage.duckdb import init_catalogue

    init_catalogue(db_path, data_dir)
    con = duckdb.connect(str(db_path))
    try:
        update_watermark(con, source, dataset, when)
    finally:
        con.close()


@pytest.mark.integration
def test_incremental_true_gap_refuses_advance_and_warns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-a/T2-i: a 30-day stall is a TRUE GAP -- refused, exactly one WARNING."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    stale = datetime.now(UTC) - timedelta(days=30)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", stale)
    _FakeConnector.responses = [_data_response()]

    with caplog.at_level(logging.WARNING, logger="gridflow.pipeline.runner"):
        result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") == stale, (
        "a TRUE GAP must refuse the advance"
    )
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"expected exactly one WARNING, got {warnings}"
    assert "unfetched_gap" in warnings[0].message
    # T2-b: the repair line is Command 1 -- gap-bounded, backfill never offered.
    assert "gridflow ingest elexon fuelhh --start" in warnings[0].message
    assert "backfill" not in warnings[0].message
    assert "will NOT self-heal" in warnings[0].message


@pytest.mark.integration
def test_incremental_clamped_no_gap_advances_with_info_not_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-j (anti-wedge): a 5-day stall clamps but has no gap -- advances, INFO
    only; a second run is not clamped at all and still advances."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    stale = datetime.now(UTC) - timedelta(hours=120)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", stale)
    _FakeConnector.responses = [_data_response()]

    with caplog.at_level(logging.INFO, logger="gridflow.pipeline.runner"):
        result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    wm = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm is not None and wm > stale, "clamped-no-gap must still advance"
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(r.levelno == logging.INFO for r in caplog.records)

    caplog.clear()
    second = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert second.exit_code == 0, second.output
    wm2 = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm2 is not None and wm2 >= wm, "the 2nd run (no longer clamped) must still advance"


@pytest.mark.integration
def test_incremental_start_equals_frontier_boundary_advances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """T2-k: with overlap=0, start == frontier exactly -- permitted (<=), advances."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    monkeypatch.setenv("GRIDFLOW_INCREMENTAL_OVERLAP_HOURS", "0")
    frontier = datetime.now(UTC) - timedelta(hours=1)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", frontier)
    _FakeConnector.responses = [_data_response()]

    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _FakeConnector.calls[-1] == frontier, "start must equal the frontier exactly (overlap=0)"
    wm = _read_watermark(db_path, "elexon", "fuelhh")
    assert wm is not None and wm > frontier


@pytest.mark.integration
def test_explicit_ingest_advances_via_unified_cas_and_raises_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """T2-l (#19): an explicit --start/--end ingest with data advances (via the
    CAS) and raises nothing -- the repair-command regression."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]

    result = runner.invoke(
        app, ["ingest", "elexon", "fuelhh", "--start", "2024-01-01", "--end", "2024-01-02"]
    )
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") == datetime(2024, 1, 2, tzinfo=UTC)


@pytest.mark.integration
def test_gap_repair_command_lands_exactly_on_gap_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """T2-m (#21): Command 1 with gap-local data advances via CAS Arm A, landing
    exactly on gap_end; an empty gap correctly does not advance."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    gap_start = datetime(2024, 1, 1, tzinfo=UTC)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", gap_start)
    gap_end = datetime(2024, 1, 5, tzinfo=UTC)
    _FakeConnector.responses = [_data_response()]

    result = runner.invoke(
        app,
        [
            "ingest",
            "elexon",
            "fuelhh",
            "--start",
            gap_start.isoformat(),
            "--end",
            gap_end.isoformat(),
        ],
    )
    assert result.exit_code == 0, result.output
    assert _read_watermark(db_path, "elexon", "fuelhh") == gap_end

    # Empty-gap variant: no data -> no advance.
    _FakeConnector.responses = []
    result2 = runner.invoke(
        app,
        [
            "ingest",
            "elexon",
            "fuelhh",
            "--start",
            gap_end.isoformat(),
            "--end",
            "2024-01-06T00:00:00+00:00",
        ],
    )
    assert result2.exit_code == 0, result2.output
    assert _read_watermark(db_path, "elexon", "fuelhh") == gap_end, "an empty gap must not advance"


@pytest.mark.integration
@pytest.mark.parametrize("delta", [timedelta(hours=1), timedelta(days=30)])
def test_explicit_future_end_denied_bronze_kept_one_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
    delta: timedelta,
) -> None:
    """T2-s (#34, the CLI-reachable case): a future --end is denied at the write
    decision (D-23); bronze is kept; exactly one WARNING (FUTURE_END)."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]
    future_end = (datetime.now(UTC) + delta).isoformat()

    with caplog.at_level(logging.WARNING, logger="gridflow.pipeline.runner"):
        result = runner.invoke(
            app, ["ingest", "elexon", "fuelhh", "--start", "2024-01-01", "--end", future_end]
        )
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") is None, "future end_dt must not advance"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "future_end" in warnings[0].message

    bronze_dir = tmp_path / "data" / "bronze" / "elexon" / "fuelhh"
    assert list(bronze_dir.rglob("raw_*.json")), "bronze bytes must be kept even when refused"


@pytest.mark.integration
def test_explicit_unreadable_frontier_denies_bronze_kept(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-t(a) (#35): the explicit path's own write-decision snapshot fails
    closed -- no write attempt, one WARNING carrying frontier_error (redacted)."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]

    from gridflow.observability import WatermarkRead

    def _broken_read(con: Any, source: str, dataset: str) -> WatermarkRead:
        return WatermarkRead(status="unreadable", value=None, error="boom securityToken=SECRET123")

    monkeypatch.setattr("gridflow.observability.read_watermark", _broken_read)

    with caplog.at_level(logging.WARNING, logger="gridflow.pipeline.runner"):
        result = runner.invoke(
            app, ["ingest", "elexon", "fuelhh", "--start", "2024-01-01", "--end", "2024-01-02"]
        )
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "frontier_unreadable" in warnings[0].message
    assert "SECRET123" not in warnings[0].message

    bronze_dir = tmp_path / "data" / "bronze" / "elexon" / "fuelhh"
    assert list(bronze_dir.rglob("raw_*.json")), "bronze bytes must be kept even when refused"


@pytest.mark.integration
def test_explicit_path_interloper_between_snapshot_and_cas_is_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-t(b) (#36): a second connection moves the row AFTER the explicit
    path's fresh snapshot and BEFORE the CAS call -- CAS_MISMATCH, frontier is
    the interloper's, WARNING names expected vs observed (timing is
    load-bearing: Sol pass-5 nit 1)."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    existing = datetime(2024, 1, 1, tzinfo=UTC)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", existing)

    from gridflow.observability import read_watermark as real_read_watermark
    from gridflow.observability import update_watermark

    interloper_value = datetime(2024, 1, 10, tzinfo=UTC)

    def _snapshot_then_interlope(con: Any, source: str, dataset: str) -> Any:
        snap = real_read_watermark(con, source, dataset)
        interloper = duckdb.connect(str(db_path))
        try:
            update_watermark(interloper, source, dataset, interloper_value)
        finally:
            interloper.close()
        return snap

    monkeypatch.setattr("gridflow.observability.read_watermark", _snapshot_then_interlope)
    _FakeConnector.responses = [_data_response()]

    with caplog.at_level(logging.WARNING, logger="gridflow.pipeline.runner"):
        result = runner.invoke(
            app, ["ingest", "elexon", "fuelhh", "--start", "2024-01-02", "--end", "2024-01-03"]
        )
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") == interloper_value
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "stale snapshot" in warnings[0].message
    assert interloper_value.isoformat() in warnings[0].message


@pytest.mark.integration
def test_refused_and_partial_produces_one_warning_with_both_conditions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-n (#4 x #21): refused AND partial -- one record at WARNING, real
    rows_skipped, no advance."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    stale = datetime.now(UTC) - timedelta(days=30)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", stale)
    _FakeConnector.responses = [_data_response()]
    _FakeConnector.last_skipped_units = 2

    with caplog.at_level(logging.WARNING, logger="gridflow.pipeline.runner"):
        result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") == stale
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "partial fetch: 2 unit(s) skipped" in warnings[0].message
    assert "unfetched_gap" in warnings[0].message
    assert "completed_with_warnings" in result.output


@pytest.mark.integration
def test_clamped_no_gap_no_evidence_is_info_not_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-o (#25): clamped-no-gap with an all-empty fetch -- INFO, "no
    evidence"; frontier really unchanged."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    stale = datetime.now(UTC) - timedelta(hours=120)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", stale)
    _FakeConnector.responses = []

    with caplog.at_level(logging.INFO, logger="gridflow.pipeline.runner"):
        result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") == stale
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("no evidence" in r.message for r in caplog.records if r.levelno == logging.INFO)


@pytest.mark.integration
def test_clamped_no_gap_partial_is_warning_and_never_claims_advance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-p (#25/P3-4): clamped-no-gap PARTIAL run -- WARNING, never claims the
    frontier moved."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    stale = datetime.now(UTC) - timedelta(hours=120)
    _seed_watermark(db_path, tmp_path / "data", "elexon", "fuelhh", stale)
    _FakeConnector.responses = [_data_response()]
    _FakeConnector.last_skipped_units = 3

    with caplog.at_level(logging.WARNING, logger="gridflow.pipeline.runner"):
        result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") == stale
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "frontier advanced" not in warnings[0].message
    assert "partial fetch: 3 unit(s) skipped" in warnings[0].message


@pytest.mark.integration
def test_write_failure_reports_write_failed_one_warning_rows_still_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_connector: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2-q (#2/#31/#38): a write exception -> WRITE_FAILED; the record says
    the attempt failed and carries the redacted detail; rows are still
    reported; caplog counts exactly ONE WARNING for the pair (D-26)."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]

    from gridflow.observability import WatermarkOutcome, WatermarkWrite

    def _boom(
        con: Any, source: str, dataset: str, last_end: Any, *, expected: Any
    ) -> WatermarkWrite:
        return WatermarkWrite(outcome=WatermarkOutcome.WRITE_FAILED, error="write boom (redacted)")

    monkeypatch.setattr("gridflow.observability.advance_watermark", _boom)

    with caplog.at_level(logging.DEBUG, logger="gridflow"):
        result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "fuelhh") is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"expected exactly one WARNING, got {warnings}"
    assert "write attempt failed" in warnings[0].message
    assert "write boom (redacted)" in warnings[0].message
    assert "1 responses ingested" in result.output


@pytest.mark.integration
def test_multi_dataset_run_mixes_refused_clamped_and_healthy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """T2-g (#13): three datasets in one run -- refused / clamped-no-gap /
    healthy -- the window is a per-iteration local, never global state.

    Calls ``runner.run_ingest`` directly (the CLI's ``ingest`` command only
    accepts one dataset name or ``--all``, and these fake dataset names are
    not registered in the source config)."""
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as runner_module

    db_path = _isolated_env(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    now = datetime.now(UTC)
    _seed_watermark(db_path, data_dir, "elexon", "refused_ds", now - timedelta(days=30))
    _seed_watermark(db_path, data_dir, "elexon", "clamped_ds", now - timedelta(hours=120))
    _seed_watermark(db_path, data_dir, "elexon", "healthy_ds", now - timedelta(hours=10))
    _FakeConnector.responses = [_data_response()]

    settings = load_settings()
    with runner_module.build_context(settings) as ctx:
        results = runner_module.run_ingest(
            ctx,
            "elexon",
            ["refused_ds", "clamped_ds", "healthy_ds"],
            now - timedelta(hours=24),
            now,
            incremental=True,
            write_watermark=True,
        )

    by_dataset = {r.dataset: r for r in results}
    assert by_dataset["refused_ds"].status == "completed_with_warnings"
    assert by_dataset["clamped_ds"].status == "success"
    assert by_dataset["healthy_ds"].status == "success"

    assert _read_watermark(db_path, "elexon", "refused_ds") == now - timedelta(days=30)
    clamped_wm = _read_watermark(db_path, "elexon", "clamped_ds")
    assert clamped_wm is not None and clamped_wm > now - timedelta(hours=120)
    healthy_wm = _read_watermark(db_path, "elexon", "healthy_ds")
    assert healthy_wm is not None and healthy_wm > now - timedelta(hours=10)


@pytest.mark.integration
def test_run_backfill_over_400_days_issues_every_chunk_no_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """T2-h (#14/R-3): a 400-day backfill, chunk_days=1 -- every chunk issued,
    no advance; the clamp never touches the backfill path."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.responses = [_data_response()]
    monkeypatch.setattr("gridflow.cli.transform", lambda **kwargs: None)

    result = runner.invoke(
        app,
        [
            "backfill",
            "elexon",
            "fuelhh",
            "--start",
            "2023-01-01",
            "--end",
            "2024-02-05",
            "--chunk-days",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output

    assert len(_FakeConnector.calls) == 400, "every 1-day chunk over 400 days must be issued"
    assert _read_watermark(db_path, "elexon", "fuelhh") is None, "backfill must never advance"


# --------------------------------------------------------------------------- #
# R2-C Task 3: C-8 -- ingest-boundary emptiness detection (parse-once).
#
# These tests drive the REAL connectors (no ``_patch_connector`` fixture) via
# respx-mocked HTTP so the boundary predicate is exercised end-to-end against
# genuinely stamped ``RawResponse.record_count`` values, not hand-built fakes.
# --------------------------------------------------------------------------- #


@respx.mock
@pytest.mark.integration
def test_elexon_settlement_date_empty_data_array_does_not_advance_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3-a (RED-first): an incremental Elexon run over a genuinely stamped
    path (``_fetch_date``) whose every response is HTTP 200 with a parsed,
    empty ``data`` array must NOT advance the watermark (C-8).

    No production ``ENDPOINTS`` entry currently uses the plain
    ``SETTLEMENT_DATE`` style (all active datasets use
    ``SETTLEMENT_DATE_PERIOD`` / ``PUBLISH_DATETIME`` / ``DATE_PATH`` /
    ``NO_PARAMS``), so a throwaway dataset entry is registered for the
    duration of this test to exercise ``_fetch_date`` specifically -- it is
    removed automatically by ``monkeypatch`` teardown.

    Before C-8 is closed, ``data_responses`` only filters on ``http_status``,
    so this 200-with-empty-array response counts as evidence and the
    watermark incorrectly advances -- RED.
    """
    from gridflow.connectors.elexon.client import ENDPOINTS
    from gridflow.connectors.elexon.endpoints import ElexonEndpoint, ParamStyle

    db_path = _isolated_env(tmp_path, monkeypatch)
    monkeypatch.setitem(
        ENDPOINTS,
        "c8_settlement_date_probe",
        ElexonEndpoint(
            path="/datasets/C8PROBE",
            description="R2-C T3-a boundary probe (SETTLEMENT_DATE, test-only)",
            param_style=ParamStyle.SETTLEMENT_DATE,
        ),
    )
    respx.get(url__startswith="https://data.elexon.co.uk/bmrs/api/v1/datasets/C8PROBE").mock(
        return_value=httpx.Response(
            200,
            json={"data": [], "metadata": {"page": 1, "totalPages": 1}},
        )
    )

    result = runner.invoke(app, ["ingest", "elexon", "c8_settlement_date_probe", "--incremental"])
    assert result.exit_code == 0, result.output

    assert _read_watermark(db_path, "elexon", "c8_settlement_date_probe") is None, (
        "a 200 response carrying a parsed, empty record array must not advance the frontier (C-8)"
    )


@respx.mock
@pytest.mark.integration
def test_elexon_mixed_empty_and_populated_responses_advances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#7: a mixed run (some responses carry records, some are genuinely
    empty) still advances -- the boundary excludes only the zero-record
    responses, not the whole run."""
    from gridflow.connectors.elexon.client import ENDPOINTS
    from gridflow.connectors.elexon.endpoints import ElexonEndpoint, ParamStyle

    db_path = _isolated_env(tmp_path, monkeypatch)
    monkeypatch.setitem(
        ENDPOINTS,
        "c8_mixed_probe",
        ElexonEndpoint(
            path="/datasets/C8MIXED",
            description="R2-C mixed-evidence boundary probe (test-only)",
            param_style=ParamStyle.SETTLEMENT_DATE,
        ),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        settlement_date = request.url.params.get("settlementDate", "")
        if settlement_date == "2024-01-15":
            body = {
                "data": [{"settlementDate": settlement_date, "x": i} for i in range(5)],
                "metadata": {"page": 1, "totalPages": 1},
            }
        else:
            body = {"data": [], "metadata": {"page": 1, "totalPages": 1}}
        return httpx.Response(200, json=body)

    respx.get(url__startswith="https://data.elexon.co.uk/bmrs/api/v1/datasets/C8MIXED").mock(
        side_effect=handler
    )

    # Explicit dates (not "--incremental") for determinism: exactly two
    # calendar dates, one populated (2024-01-15) and one genuinely empty
    # (2024-01-16), regardless of when this test happens to run.
    result = runner.invoke(
        app,
        [
            "ingest",
            "elexon",
            "c8_mixed_probe",
            "--start",
            "2024-01-15",
            "--end",
            "2024-01-16",
        ],
    )
    assert result.exit_code == 0, result.output

    wm = _read_watermark(db_path, "elexon", "c8_mixed_probe")
    assert wm is not None, "a run with at least one non-empty response must still advance"


@respx.mock
@pytest.mark.integration
def test_elexon_truncated_json_stamps_none_and_advances_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#5: a truncated/malformed JSON body stamps ``record_count is None``
    (a parse failure, not zero records, D-8) -- treated as evidence exactly
    like today, so the frontier advances unchanged, and no exception
    escapes the connector or the runner."""
    from gridflow.connectors.elexon.client import ENDPOINTS
    from gridflow.connectors.elexon.endpoints import ElexonEndpoint, ParamStyle

    db_path = _isolated_env(tmp_path, monkeypatch)
    monkeypatch.setitem(
        ENDPOINTS,
        "c8_truncated_probe",
        ElexonEndpoint(
            path="/datasets/C8TRUNC",
            description="R2-C truncated-JSON boundary probe (test-only)",
            param_style=ParamStyle.SETTLEMENT_DATE,
        ),
    )
    respx.get(url__startswith="https://data.elexon.co.uk/bmrs/api/v1/datasets/C8TRUNC").mock(
        return_value=httpx.Response(200, content=b'{"data": [{"x": 1}'),  # truncated, invalid JSON
    )

    before = datetime.now(UTC)
    result = runner.invoke(app, ["ingest", "elexon", "c8_truncated_probe", "--incremental"])
    assert result.exit_code == 0, result.output
    after = datetime.now(UTC)

    wm = _read_watermark(db_path, "elexon", "c8_truncated_probe")
    assert wm is not None, "a parse failure (None) must not be treated as zero records"
    assert before <= wm <= after
