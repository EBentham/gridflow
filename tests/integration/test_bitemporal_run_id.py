"""Integration tests for bitemporal run-id propagation."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.storage.duckdb import get_connection, init_catalogue
from gridflow.storage.parquet import scan_parquet_dir

if TYPE_CHECKING:
    from conftest import IsolatedPaths

FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"
TARGET_DATE = date(2024, 1, 15)
runner = CliRunner()


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


def test_cli_transform_stamps_source_run_id_matching_pipeline_run(
    isolated_env: IsolatedPaths,
) -> None:
    _write_fuelhh_bronze(isolated_env.data_dir)

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

    silver = scan_parquet_dir(isolated_env.data_dir / "silver" / "elexon" / "fuelhh").collect()
    run_ids = set(silver["source_run_id"].to_list())
    assert len(run_ids) == 1

    con = get_connection(isolated_env.duckdb_path, read_only=True)
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

    assert runs == [
        (
            next(iter(run_ids)),
            "elexon",
            "fuelhh",
            "transform",
            "completed_with_warnings",
        )
    ]

    init_catalogue(isolated_env.duckdb_path, isolated_env.data_dir)
    con = get_connection(isolated_env.duckdb_path, read_only=True)
    try:
        duckdb_rows = con.execute(
            """
            SELECT event_time IS NOT NULL, available_at IS NOT NULL, source_run_id, dataset_version
            FROM silver_elexon_fuelhh
            LIMIT 5
            """
        ).fetchall()
    finally:
        con.close()

    assert duckdb_rows
    assert all(row[0] for row in duckdb_rows)
    assert all(row[1] for row in duckdb_rows)
    assert {row[2] for row in duckdb_rows} == run_ids
    assert {row[3] for row in duckdb_rows} == {"2.0.0"}


def test_cli_transform_reingest_uses_bronze_sidecar_available_at(
    isolated_env: IsolatedPaths,
) -> None:
    """--reingest takes available_at from the bronze sidecar and still stamps
    the transform run's id on every silver row."""
    sidecar_time = datetime(2024, 1, 16, 9, 30, tzinfo=UTC)
    _write_fuelhh_bronze(isolated_env.data_dir, fetched_at=sidecar_time)

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

    silver = scan_parquet_dir(isolated_env.data_dir / "silver" / "elexon" / "fuelhh").collect()
    assert set(silver["available_at"].to_list()) == {sidecar_time}

    con = get_connection(isolated_env.duckdb_path, read_only=True)
    try:
        runs = con.execute(
            """
            SELECT run_id FROM pipeline_runs
            WHERE source = 'elexon' AND dataset = 'fuelhh' AND operation = 'transform'
            """
        ).fetchall()
    finally:
        con.close()
    assert len(runs) == 1
    assert set(silver["source_run_id"].to_list()) == {runs[0][0]}
