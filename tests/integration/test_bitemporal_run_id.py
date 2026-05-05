"""Integration tests for bitemporal run-id propagation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.storage.duckdb import get_connection
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


def _write_fuelhh_bronze(data_dir: Path) -> None:
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
