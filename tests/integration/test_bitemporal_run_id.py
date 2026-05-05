"""Integration tests for bitemporal run-id propagation."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.storage.duckdb import get_connection, init_catalogue
from gridflow.storage.parquet import read_parquet

FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"
TARGET_DATE = date(2024, 1, 15)
runner = CliRunner()


@dataclass(frozen=True)
class IsolatedPaths:
    data_dir: Path
    duckdb_path: Path
    log_dir: Path


def _isolated_env(tmp_path: Path, monkeypatch) -> IsolatedPaths:
    paths = IsolatedPaths(
        data_dir=tmp_path / "data",
        duckdb_path=tmp_path / "catalogue" / "gridflow.duckdb",
        log_dir=tmp_path / "logs",
    )
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(paths.data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(paths.duckdb_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(paths.log_dir))
    return paths


def _write_fuelhh_bronze(
    data_dir: Path,
    fetched_at: datetime | None = None,
) -> None:
    bronze_dir = (
        data_dir
        / "bronze"
        / "elexon"
        / "fuelhh"
        / str(TARGET_DATE.year)
        / f"{TARGET_DATE.month:02d}"
        / f"{TARGET_DATE.day:02d}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads((FIXTURES / "fuelhh_response.json").read_text())
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    if fetched_at is not None:
        (bronze_dir / "raw_test.meta.json").write_text(
            json.dumps(
                {
                    "source": "elexon",
                    "dataset": "fuelhh",
                    "fetched_at": fetched_at.isoformat(),
                    "data_date": TARGET_DATE.isoformat(),
                }
            )
        )


def _load_run_pipeline_script():
    script_path = Path(__file__).parents[2] / "scripts" / "run_pipeline.py"
    spec = importlib.util.spec_from_file_location("gridflow_run_pipeline_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_transform_stamps_source_run_id_matching_pipeline_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = _isolated_env(tmp_path, monkeypatch)
    _write_fuelhh_bronze(paths.data_dir)

    result = runner.invoke(
        app,
        [
            "transform",
            "elexon",
            "fuelhh",
            "--start",
            TARGET_DATE.isoformat(),
            "--end",
            TARGET_DATE.isoformat(),
        ],
    )

    assert result.exit_code == 0, result.output

    silver = read_parquet(paths.data_dir / "silver" / "elexon" / "fuelhh" / "**" / "*.parquet")
    run_ids = set(silver["source_run_id"].to_list())
    assert len(run_ids) == 1

    con = get_connection(paths.duckdb_path, read_only=True)
    try:
        runs = con.execute(
            """
            SELECT run_id, source, dataset, operation, status
            FROM pipeline_runs
            WHERE source = 'elexon' AND dataset = 'fuelhh' AND operation = 'transform'
            """
        ).fetchall()
    finally:
        con.close()

    assert runs == [(next(iter(run_ids)), "elexon", "fuelhh", "transform", "success")]

    init_catalogue(paths.duckdb_path, paths.data_dir)
    con = get_connection(paths.duckdb_path, read_only=True)
    try:
        duckdb_rows = con.execute(
            """
            SELECT event_time IS NOT NULL, available_at IS NOT NULL, source_run_id, dataset_version
            FROM silver_fuelhh
            LIMIT 5
            """
        ).fetchall()
    finally:
        con.close()

    assert duckdb_rows
    assert all(row[0] for row in duckdb_rows)
    assert all(row[1] for row in duckdb_rows)
    assert {row[2] for row in duckdb_rows} == run_ids
    assert {row[3] for row in duckdb_rows} == {"1.0.0"}


def test_cli_transform_reingest_uses_bronze_sidecar_available_at(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = _isolated_env(tmp_path, monkeypatch)
    sidecar_time = datetime(2024, 1, 16, 9, 30, tzinfo=UTC)
    _write_fuelhh_bronze(paths.data_dir, fetched_at=sidecar_time)

    result = runner.invoke(
        app,
        [
            "transform",
            "elexon",
            "fuelhh",
            "--start",
            TARGET_DATE.isoformat(),
            "--end",
            TARGET_DATE.isoformat(),
            "--reingest",
        ],
    )

    assert result.exit_code == 0, result.output

    silver = read_parquet(paths.data_dir / "silver" / "elexon" / "fuelhh" / "**" / "*.parquet")
    assert set(silver["available_at"].to_list()) == {sidecar_time}


def test_script_silver_step_threads_run_id_and_reingest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gridflow.config.settings import load_settings

    paths = _isolated_env(tmp_path, monkeypatch)
    sidecar_time = datetime(2024, 1, 16, 10, 15, tzinfo=UTC)
    _write_fuelhh_bronze(paths.data_dir, fetched_at=sidecar_time)

    from gridflow.storage.duckdb import get_connection, init_catalogue

    settings = load_settings()
    init_catalogue(paths.duckdb_path, paths.data_dir)
    con = get_connection(paths.duckdb_path)
    try:
        start_dt = datetime(2024, 1, 15, tzinfo=UTC)
        run_pipeline = _load_run_pipeline_script()
        run_pipeline.run_silver(
            "elexon",
            ["fuelhh"],
            start_dt,
            start_dt,
            settings,
            con,
            reingest=True,
        )
    finally:
        con.close()

    silver = read_parquet(paths.data_dir / "silver" / "elexon" / "fuelhh" / "**" / "*.parquet")
    run_ids = set(silver["source_run_id"].to_list())
    assert len(run_ids) == 1
    assert set(silver["available_at"].to_list()) == {sidecar_time}

    con = get_connection(paths.duckdb_path, read_only=True)
    try:
        runs = con.execute(
            """
            SELECT run_id, source, dataset, operation, status
            FROM pipeline_runs
            WHERE source = 'elexon' AND dataset = 'fuelhh' AND operation = 'transform'
            """
        ).fetchall()
    finally:
        con.close()

    assert runs == [(next(iter(run_ids)), "elexon", "fuelhh", "transform", "success")]
