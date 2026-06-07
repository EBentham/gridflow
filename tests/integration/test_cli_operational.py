"""CH4-07 / CH-TEST-01 (C3-12): CliRunner coverage for operational commands.

Closes the operational-CLI test gap left by the CH4-01 runner golden suite. The
golden suite (``test_cli_runner_golden.py``) already covers the PIPELINE commands
(``ingest``/``transform``/``build``/``pipeline``/``backfill``); the destructive
``reset`` guard lives in ``test_cli_reset_guard.py`` and ``prune`` in
``test_cli_prune.py``. This file covers the remaining OPERATIONAL commands those
files do not touch:

  - ``status``  — the no-catalogue guard and the populated-runs path;
  - ``quality`` — the no-silver guard and a real run over a tiny silver tree;
  - ``export-csv`` — the happy path (silver Parquet -> CSV) and the skip paths.

Coverage split (pinned):
  - ingest/transform/build/pipeline/backfill -> tests/integration/test_cli_runner_golden.py
  - reset (containment + dry-run)            -> tests/integration/test_cli_reset_guard.py
  - prune (retention + containment)          -> tests/integration/test_cli_prune.py
  - status / quality / export-csv            -> THIS FILE

These characterize EXISTING behavior (GREEN-on-write): real CLI stdout + exit
codes against a real tmp workspace, no mocks of the command internals.

Note on ``status``: its happy path renders ``pipeline_runs`` via DuckDB
``.fetchdf()`` (pandas). pandas is not a runtime dependency of this project, so
when it is absent the render raises and ``status`` swallows it into a
"Could not query pipeline runs" line (still exit 0). The populated-runs test
asserts whichever branch the environment takes, so it is correct with or without
pandas installed.
"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from gridflow.cli import app

if TYPE_CHECKING:
    from pathlib import Path

runner = CliRunner()

_HAS_PANDAS = importlib.util.find_spec("pandas") is not None


def _isolated_env(data_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point gridflow at a tmp data_dir / duckdb / log dir; return the duckdb path."""
    db_path = data_dir / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("ELEXON_API_KEY", "test-key")
    # Gold SQL views reference silver tables absent from a tmpdir; stub out so
    # init_catalogue does not loud-fail under the F15-D strict-mode gate.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return db_path


def _stage_silver_parquet(data_dir: Path, source: str, dataset: str, df: pl.DataFrame) -> Path:
    """Write ``df`` to ``silver/<source>/<dataset>/year=YYYY/month=MM`` and return the file."""
    part = data_dir / "silver" / source / dataset / "year=2024" / "month=01"
    part.mkdir(parents=True, exist_ok=True)
    out = part / f"{dataset}.parquet"
    df.write_parquet(out)
    return out


# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_status_no_catalogue_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """status with no DuckDB catalogue prints the init hint and exits 1.

    This is the pandas-free branch (it returns before opening a connection), so
    it pins unconditionally regardless of whether pandas is installed.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    result = runner.invoke(app, ["status"])

    assert result.exit_code == 1, result.output
    assert "No DuckDB catalogue found" in result.output
    assert "gridflow init" in result.output


@pytest.mark.integration
def test_status_with_runs_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """status over a catalogue holding a recent pipeline_run reports it (or, with
    pandas absent, the graceful fallback line) — never crashes."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = _isolated_env(data_dir, tmp_path, monkeypatch)

    from gridflow.storage.duckdb import init_catalogue

    init_catalogue(db_path, data_dir)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            "INSERT INTO pipeline_runs "
            "(run_id, source, dataset, operation, started_at, completed_at, status, "
            " rows_in, rows_out, rows_skipped, duration_seconds) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "run-1",
                "elexon",
                "fuelhh",
                "ingest",
                datetime.now(UTC),
                datetime.now(UTC),
                "success",
                10,
                10,
                0,
                1.5,
            ],
        )
    finally:
        con.close()

    result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    if _HAS_PANDAS:
        # The render path (.fetchdf()) succeeds: the seeded run is shown.
        assert "Last 24h Pipeline Runs:" in result.output
        assert "elexon" in result.output
        assert "fuelhh" in result.output
    else:
        # pandas absent -> .fetchdf() raises -> swallowed into the fallback line.
        assert "Could not query pipeline runs" in result.output


# --------------------------------------------------------------------------- #
# quality
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_quality_no_silver_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """quality with no silver directory prints a clear message and exits 1."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    result = runner.invoke(app, ["quality"])

    assert result.exit_code == 1, result.output
    assert "No silver data found" in result.output


@pytest.mark.integration
def test_quality_runs_over_small_silver_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """quality scans a tiny silver dataset, writes a report, and prints a summary.

    Exercises check_row_count + the numeric-null-rate loop over a real Polars
    frame, and the QualityReporter DuckDB write path (which creates its own
    quality_reports table), then asserts the summary line.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = _isolated_env(data_dir, tmp_path, monkeypatch)

    _stage_silver_parquet(
        data_dir,
        "elexon",
        "system_prices",
        pl.DataFrame(
            {
                "settlement_date": ["2024-01-15", "2024-01-15"],
                "settlement_period": [1, 2],
                "system_sell_price": [45.5, 46.0],
            }
        ),
    )

    result = runner.invoke(app, ["quality"])

    assert result.exit_code == 0, result.output
    assert "Quality Report:" in result.output
    assert "checks passed" in result.output

    # The report was persisted to the quality_reports table.
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM quality_reports").fetchone()
    finally:
        con.close()
    assert count is not None
    assert count[0] >= 1


@pytest.mark.integration
def test_quality_source_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """--source narrows the scan to one source's datasets."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    _stage_silver_parquet(
        data_dir,
        "elexon",
        "system_prices",
        pl.DataFrame({"settlement_date": ["2024-01-15"], "system_sell_price": [45.5]}),
    )

    result = runner.invoke(app, ["quality", "--source", "elexon"])

    assert result.exit_code == 0, result.output
    assert "Quality Report:" in result.output


# --------------------------------------------------------------------------- #
# export-csv
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_export_csv_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """export-csv writes a CSV for a silver dataset and prints the row count."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    _stage_silver_parquet(
        data_dir,
        "elexon",
        "system_prices",
        pl.DataFrame(
            {
                "settlement_date": ["2024-01-15", "2024-01-15"],
                "system_sell_price": [45.5, 46.0],
            }
        ),
    )

    result = runner.invoke(app, ["export-csv", "elexon", "system_prices"])

    assert result.exit_code == 0, result.output
    assert "Export complete" in result.output
    assert "2 rows" in result.output

    out_csv = data_dir / "exports" / "elexon" / "system_prices.csv"
    assert out_csv.exists(), "export-csv must write the dataset CSV"
    exported = pl.read_csv(out_csv)
    assert exported.height == 2
    assert "system_sell_price" in exported.columns


@pytest.mark.integration
def test_export_csv_custom_output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """--output redirects the CSV into a caller-chosen directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    _stage_silver_parquet(
        data_dir,
        "elexon",
        "system_prices",
        pl.DataFrame({"settlement_date": ["2024-01-15"], "system_sell_price": [45.5]}),
    )
    out_root = tmp_path / "custom_exports"

    result = runner.invoke(
        app, ["export-csv", "elexon", "system_prices", "--output", str(out_root)]
    )

    assert result.exit_code == 0, result.output
    assert (out_root / "elexon" / "system_prices.csv").exists()


@pytest.mark.integration
def test_export_csv_missing_silver_skips(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A dataset with no silver data is skipped (not an error) and exits 0."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    result = runner.invoke(app, ["export-csv", "elexon", "system_prices"])

    assert result.exit_code == 0, result.output
    assert "no silver data found, skipping" in result.output
    assert "Export complete" in result.output
