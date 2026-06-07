"""CH4-01 golden characterization tests — the behavior oracle for the runner refactor.

These pin the *observable* CLI behavior (stdout/stderr text + exit code) of the
five pipeline commands (``ingest``/``transform``/``build``/``pipeline``/``backfill``)
plus a ``run_pipeline.main()`` smoke test. They MUST pass on the pre-refactor code
and stay green after ``cli`` commands become thin adapters over
``gridflow.pipeline.runner`` — equivalence is the whole point.

No HTTP: the connector is replaced in the registry by a minimal async-CM fake;
transformers run against tiny on-disk bronze fixtures. Gold SQL view registration
is stubbed (the silver tables it references are absent from a tmpdir).

Deliberately NON-snapshot assertions (substring/exit-code), not full-text golden
strings: the per-dataset and summary *lines* are the contract; incidental
formatting (a trailing blank line) is not. Each assertion cites the exact source
line it locks so a future drift is traceable.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from gridflow.cli import app

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

runner = CliRunner()


def _isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point every GRIDFLOW_* path at the tmpdir and stub gold-view registration.

    Returns the DuckDB catalogue path so a test can read pipeline_runs directly.
    """
    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("ELEXON_API_KEY", "test-key")
    # Gold SQL views reference silver tables absent from a tmpdir; stub out
    # (mirrors test_cli_transform_refresh.py / test_ingest_watermark.py).
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return db_path


def _write_fuelhh_bronze(data_dir: Path, target: date = date(2024, 1, 15)) -> None:
    """Drop one real fuelhh response into the bronze partition for ``target``."""
    from pathlib import Path as _Path

    fixtures = _Path(__file__).parent.parent / "fixtures" / "elexon"
    bronze_dir = (
        data_dir
        / "bronze"
        / "elexon"
        / "fuelhh"
        / str(target.year)
        / f"{target.month:02d}"
        / f"{target.day:02d}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads((fixtures / "fuelhh_response.json").read_text())
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))


class _FakeConnector:
    """Minimal async-CM connector returning a configurable response list.

    Mirrors the fake in ``test_ingest_watermark.py``: class-level knobs let a test
    flip ``raise_on_fetch`` (failure path) or seed ``responses``/``skipped``.
    """

    last_skipped_units = 0
    calls: list[datetime] = []
    raise_on_fetch = False
    responses: list[RawResponse] = []

    def __init__(self, config: Any) -> None:
        self.config = config
        # Instance copy so a per-test ``skipped`` survives the async-with exit.
        self.last_skipped_units = type(self).last_skipped_units

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
    _FakeConnector.calls = []
    _FakeConnector.raise_on_fetch = False
    _FakeConnector.responses = []
    _FakeConnector.last_skipped_units = 0


@pytest.fixture
def _patch_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Route ``get_connector`` (resolved at command call time) to the fake."""
    monkeypatch.setattr(
        "gridflow.connectors.registry.get_connector",
        lambda source_name, config: _FakeConnector(config),
    )


# --------------------------------------------------------------------------- #
# ingest
# --------------------------------------------------------------------------- #


def test_ingest_success_empty_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """An empty-but-successful fetch exits 0 and prints the per-dataset count
    line + the final ``Ingestion complete`` (cli.ingest:201,224)."""
    _isolated_env(tmp_path, monkeypatch)
    result = runner.invoke(
        app, ["ingest", "elexon", "fuelhh", "--start", "2024-01-15", "--end", "2024-01-16"]
    )
    assert result.exit_code == 0, result.output
    assert "elexon/fuelhh: 0 responses ingested" in result.output
    assert "Ingestion complete" in result.output


def test_ingest_failure_exits_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """A fetch failure exits 1, prints the per-dataset FAILED line and the
    summary block (cli.ingest:214,220-223)."""
    _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.raise_on_fetch = True
    result = runner.invoke(
        app, ["ingest", "elexon", "fuelhh", "--start", "2024-01-15", "--end", "2024-01-16"]
    )
    assert result.exit_code == 1, result.output
    assert "elexon/fuelhh: FAILED - simulated upstream failure" in result.output
    assert "Ingestion failed for 1 dataset(s):" in result.output


def test_ingest_missing_dataset_bad_parameter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No dataset + no --all -> typer.BadParameter -> exit 2 (cli._resolve_datasets)."""
    _isolated_env(tmp_path, monkeypatch)
    result = runner.invoke(app, ["ingest", "elexon", "--start", "2024-01-15"])
    assert result.exit_code == 2, result.output
    assert "Specify a dataset name or use --all" in result.output


def test_ingest_naive_datetime_bad_parameter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A naive --start datetime is rejected as a BadParameter -> exit 2
    (cli._parse_window_bound, issue-19)."""
    _isolated_env(tmp_path, monkeypatch)
    result = runner.invoke(app, ["ingest", "elexon", "fuelhh", "--start", "2024-01-15T00:00:00"])
    assert result.exit_code == 2, result.output


# --------------------------------------------------------------------------- #
# transform
# --------------------------------------------------------------------------- #


def test_transform_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """transform of real bronze exits 0, prints the rows line + ``Transform
    complete`` (cli.transform:303,319)."""
    data_dir = tmp_path / "data"
    _isolated_env(tmp_path, monkeypatch)
    _write_fuelhh_bronze(data_dir)
    result = runner.invoke(
        app, ["transform", "elexon", "fuelhh", "--start", "2024-01-15", "--end", "2024-01-15"]
    )
    assert result.exit_code == 0, result.output
    assert "elexon/fuelhh:" in result.output
    assert "rows transformed" in result.output
    assert "Transform complete" in result.output


def test_transform_failure_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A transformer error exits 1 and prints the FAILED line + summary block
    (cli.transform:308,314-318)."""
    _isolated_env(tmp_path, monkeypatch)

    def _boom(source: str, dataset: str, data_dir: Path) -> Any:
        raise RuntimeError("simulated transform failure")

    monkeypatch.setattr("gridflow.silver.registry.get_transformer", _boom)
    result = runner.invoke(
        app, ["transform", "elexon", "fuelhh", "--start", "2024-01-15", "--end", "2024-01-15"]
    )
    assert result.exit_code == 1, result.output
    assert "elexon/fuelhh: FAILED - simulated transform failure" in result.output
    assert "Transform failed for 1 dataset(s):" in result.output


# --------------------------------------------------------------------------- #
# build
# --------------------------------------------------------------------------- #


def test_build_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """build with a fake builder exits 0, prints the rows line + ``Build
    complete`` (cli.build:372,380)."""
    data_dir = tmp_path / "data"
    _isolated_env(tmp_path, monkeypatch)

    class _FakeBuilder:
        def __init__(self, data_dir: Path) -> None:
            pass

        def run(self, start: date, end: date) -> int:
            part = data_dir / "gold" / "system_marginal_price" / "year=2026" / "month=05"
            part.mkdir(parents=True)
            pl.DataFrame({"price_gbp": [100.0]}).write_parquet(part / "part.parquet")
            return 1

    monkeypatch.setattr(
        "gridflow.gold.system_marginal_price.SystemMarginalPriceBuilder", _FakeBuilder
    )
    result = runner.invoke(
        app, ["build", "system_marginal_price", "--start", "2026-05-01", "--end", "2026-05-01"]
    )
    assert result.exit_code == 0, result.output
    assert "system_marginal_price: 1 rows built" in result.output
    assert "Build complete" in result.output


def test_build_failure_exits_0_and_prints_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LOAD-BEARING: a builder failure does NOT abort build today — it prints the
    FAILED line, logs, and STILL exits 0 with ``Build complete`` (cli.build:373-380
    catches-and-continues, never raises Exit). This pins the exit code so the
    runner refactor must NOT change it to 1."""
    _isolated_env(tmp_path, monkeypatch)

    class _BoomBuilder:
        def __init__(self, data_dir: Path) -> None:
            pass

        def run(self, start: date, end: date) -> int:
            raise RuntimeError("simulated build failure")

    monkeypatch.setattr(
        "gridflow.gold.system_marginal_price.SystemMarginalPriceBuilder", _BoomBuilder
    )
    result = runner.invoke(
        app, ["build", "system_marginal_price", "--start", "2026-05-01", "--end", "2026-05-01"]
    )
    assert result.exit_code == 0, result.output
    assert "system_marginal_price: FAILED - simulated build failure" in result.output
    assert "Build complete" in result.output


def test_build_no_target_bad_parameter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No gold dataset + no --all -> BadParameter -> exit 2 (cli.build:354)."""
    _isolated_env(tmp_path, monkeypatch)
    result = runner.invoke(app, ["build", "--start", "2026-05-01"])
    assert result.exit_code == 2, result.output
    assert "Specify a gold dataset name or use --all" in result.output


def test_build_unknown_dataset_skips(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An unknown gold dataset prints the unknown line and still exits 0
    (cli.build:363-365)."""
    _isolated_env(tmp_path, monkeypatch)
    result = runner.invoke(
        app, ["build", "no_such_gold", "--start", "2026-05-01", "--end", "2026-05-01"]
    )
    assert result.exit_code == 0, result.output
    assert "Unknown gold dataset: no_such_gold" in result.output
    assert "Build complete" in result.output


# --------------------------------------------------------------------------- #
# pipeline
# --------------------------------------------------------------------------- #


def test_pipeline_bronze_silver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """pipeline (no --gold) runs bronze then silver and prints both section
    headers, the nested per-stage completion lines, and the final marker."""
    data_dir = tmp_path / "data"
    _isolated_env(tmp_path, monkeypatch)
    _write_fuelhh_bronze(data_dir)
    result = runner.invoke(
        app, ["pipeline", "elexon", "fuelhh", "--start", "2024-01-15", "--end", "2024-01-15"]
    )
    assert result.exit_code == 0, result.output
    assert "=== Pipeline: elexon ===" in result.output
    assert "--- Bronze (ingest) ---" in result.output
    assert "Ingestion complete" in result.output
    assert "--- Silver (transform) ---" in result.output
    assert "Transform complete" in result.output
    assert "=== Pipeline complete ===" in result.output


def test_pipeline_with_gold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """pipeline --gold runs bronze, silver, and the gold build, printing the gold
    section + ``Build complete`` before the final marker."""
    data_dir = tmp_path / "data"
    _isolated_env(tmp_path, monkeypatch)
    _write_fuelhh_bronze(data_dir)

    class _FakeBuilder:
        def __init__(self, data_dir: Path) -> None:
            pass

        def run(self, start: date, end: date) -> int:
            part = data_dir / "gold" / "system_marginal_price" / "year=2024" / "month=01"
            part.mkdir(parents=True)
            pl.DataFrame({"price_gbp": [1.0]}).write_parquet(part / "part.parquet")
            return 1

    monkeypatch.setattr(
        "gridflow.gold.system_marginal_price.SystemMarginalPriceBuilder", _FakeBuilder
    )
    result = runner.invoke(
        app,
        [
            "pipeline",
            "elexon",
            "fuelhh",
            "--start",
            "2024-01-15",
            "--end",
            "2024-01-15",
            "--gold",
            "system_marginal_price",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "--- Gold (build) ---" in result.output
    assert "system_marginal_price: 1 rows built" in result.output
    assert "Build complete" in result.output
    assert "=== Pipeline complete ===" in result.output


def test_pipeline_bronze_failure_aborts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """A bronze failure raises Exit(1) inside the bronze sub-call, so the
    pipeline aborts before silver and never prints the completion marker
    (cli.pipeline delegates to ingest which raises Exit(1))."""
    _isolated_env(tmp_path, monkeypatch)
    _FakeConnector.raise_on_fetch = True
    result = runner.invoke(
        app, ["pipeline", "elexon", "fuelhh", "--start", "2024-01-15", "--end", "2024-01-15"]
    )
    assert result.exit_code == 1, result.output
    assert "=== Pipeline complete ===" not in result.output


# --------------------------------------------------------------------------- #
# backfill
# --------------------------------------------------------------------------- #


def test_backfill_multi_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """backfill over 3 days at chunk-days=1 runs 3 chunks per dataset, prints the
    per-chunk lines + the per-dataset chunk count + the final marker
    (cli.backfill:409,415,448,450). transform is stubbed (no real bronze)."""
    _isolated_env(tmp_path, monkeypatch)
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
    assert "--- Backfilling elexon/fuelhh ---" in result.output
    assert "Chunk 1: 2024-01-15 to 2024-01-16" in result.output
    assert "Chunk 3: 2024-01-17 to 2024-01-18" in result.output
    assert "elexon/fuelhh: 3 chunks processed" in result.output
    assert "Backfill complete" in result.output


def test_backfill_chunk_ingest_does_not_write_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """Backfill chunk-ingests run with write_watermark=False, so a backfill over a
    range whose chunks end after an existing forward watermark never advances it
    (C3-11). transform stubbed."""
    db_path = _isolated_env(tmp_path, monkeypatch)
    from gridflow.observability import update_watermark
    from gridflow.storage.duckdb import init_catalogue

    init_catalogue(db_path, tmp_path / "data")
    existing = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
    seed_con = duckdb.connect(str(db_path))
    try:
        update_watermark(seed_con, "elexon", "fuelhh", existing)
    finally:
        seed_con.close()

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

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT last_end FROM pipeline_watermarks WHERE source='elexon' AND dataset='fuelhh'"
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    assert row[0].replace(tzinfo=UTC) == existing


# --------------------------------------------------------------------------- #
# run_pipeline.py smoke (script entrypoint had NO tests)
# --------------------------------------------------------------------------- #


def test_run_pipeline_main_smoke(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """scripts/run_pipeline.py main() runs bronze+silver end-to-end without error
    on real bronze and a stubbed connector. Smoke only — run_pipeline behavior is
    intentionally allowed to CHANGE in the refactor (exits, redaction, refresh),
    so this asserts a clean exit, not exact stdout."""
    import importlib.util
    from pathlib import Path as _Path

    data_dir = tmp_path / "data"
    _isolated_env(tmp_path, monkeypatch)
    _write_fuelhh_bronze(data_dir)

    script_path = _Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_pipeline_smoke", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monkeypatch.setattr(
        "sys.argv",
        [
            "run_pipeline.py",
            "--step",
            "silver",
            "--source",
            "elexon",
            "--dataset",
            "fuelhh",
            "--start",
            "2024-01-15",
            "--end",
            "2024-01-15",
        ],
    )
    # Should complete without raising (silver step over real bronze).
    module.main()


# --------------------------------------------------------------------------- #
# runner.run_backfill — multi-chunk connection reuse on a real temp DuckDB
# --------------------------------------------------------------------------- #


def test_run_backfill_multi_chunk_real_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """run_backfill walks multiple chunks on ONE shared connection writing to a
    real temp DuckDB (Windows connection-reuse-across-chunks risk).

    Three 1-day chunks; the stubbed connector records 3 fetch starts; transform
    runs on (empty) bronze without error; the run reports ok and the catalogue is
    queryable afterwards (the shared connection closed cleanly)."""
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner

    db_path = _isolated_env(tmp_path, monkeypatch)
    settings = load_settings()

    start_dt = datetime(2024, 1, 15, tzinfo=UTC)
    end_dt = datetime(2024, 1, 18, tzinfo=UTC)
    report = runner.run_backfill(settings, "elexon", ["fuelhh"], start_dt, end_dt, chunk_days=1)

    assert report.ok, [r for r in report.results if r.status == "failed"]
    # 3 chunks x (1 ingest + 1 transform) = 6 results.
    assert len(report.results) == 6
    assert len(_FakeConnector.calls) == 3

    # The catalogue file is closed and queryable after the run (no lingering lock).
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()
    finally:
        con.close()


def test_run_backfill_chunk_ingest_does_not_advance_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """run_backfill's chunk ingests use write_watermark=False, so a forward
    watermark inside the range is never advanced (C3-11)."""
    from gridflow.config.settings import load_settings
    from gridflow.observability import update_watermark
    from gridflow.pipeline import runner
    from gridflow.storage.duckdb import init_catalogue

    db_path = _isolated_env(tmp_path, monkeypatch)
    init_catalogue(db_path, tmp_path / "data")
    existing = datetime(2024, 1, 16, tzinfo=UTC)
    seed = duckdb.connect(str(db_path))
    try:
        update_watermark(seed, "elexon", "fuelhh", existing)
    finally:
        seed.close()

    settings = load_settings()
    runner.run_backfill(
        settings,
        "elexon",
        ["fuelhh"],
        datetime(2024, 1, 15, tzinfo=UTC),
        datetime(2024, 1, 18, tzinfo=UTC),
        chunk_days=1,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT last_end FROM pipeline_watermarks WHERE source='elexon' AND dataset='fuelhh'"
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    assert row[0].replace(tzinfo=UTC) == existing


def test_backfill_script_in_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _patch_connector: None
) -> None:
    """scripts/backfill.py runs in-process (no subprocess) over multiple chunks
    and exits cleanly on a stubbed connector + empty bronze."""
    import importlib.util
    from datetime import date as _date
    from pathlib import Path as _Path

    _isolated_env(tmp_path, monkeypatch)

    script_path = _Path(__file__).parent.parent.parent / "scripts" / "backfill.py"
    spec = importlib.util.spec_from_file_location("backfill_smoke", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Should complete without raising SystemExit (all chunks succeed: empty fetch
    # + empty-bronze transform are both non-failing).
    module.backfill(
        source="elexon",
        dataset="fuelhh",
        start=_date(2024, 1, 15),
        end=_date(2024, 1, 18),
        chunk_days=1,
    )
    assert len(_FakeConnector.calls) == 3
