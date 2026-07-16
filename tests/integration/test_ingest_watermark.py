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
