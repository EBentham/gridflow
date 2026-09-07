"""Regression coverage for ``gridflow status`` without pandas or NumPy."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import duckdb
import pytest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("seed_run", [True, False])
def test_status_without_pandas_or_numpy(tmp_path: Path, seed_run: bool) -> None:
    """Render recent runs, and retain the empty message, without pandas/NumPy."""
    data_root = tmp_path / "status-root"
    data_root.mkdir()
    db_path = data_root / "catalogue.duckdb"

    with duckdb.connect(str(db_path)) as con:
        con.execute("""
            CREATE TABLE pipeline_runs (
                source VARCHAR,
                dataset VARCHAR,
                operation VARCHAR,
                status VARCHAR,
                rows_out INTEGER,
                duration_seconds FLOAT,
                started_at TIMESTAMP WITH TIME ZONE
            )
        """)
        if seed_run:
            con.execute(
                """
                INSERT INTO pipeline_runs
                    (source, dataset, operation, status, rows_out,
                     duration_seconds, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "elexon",
                    "fuelhh",
                    "ingest",
                    "success",
                    17,
                    1.5,
                    datetime.now(UTC),
                ],
            )

    script = """
import sys

sys.modules["numpy"] = None
sys.modules["pandas"] = None

from typer.testing import CliRunner
from gridflow.cli import app

result = CliRunner().invoke(app, ["status"])
sys.stdout.write(result.output)
raise SystemExit(result.exit_code)
"""
    env = os.environ.copy()
    env.update(
        {
            "GRIDFLOW_DATA_DIR": str(data_root),
            "GRIDFLOW_DUCKDB_PATH": str(db_path),
            "GRIDFLOW_LOG_DIR": str(tmp_path / "status-logs"),
            "PYTHONIOENCODING": "cp1252",
        }
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        env=env,
        check=False,
    )
    stdout = completed.stdout.decode("cp1252")
    stderr = completed.stderr.decode("cp1252")

    assert completed.returncode == 0, stderr
    assert "Could not query pipeline runs" not in stdout
    if seed_run:
        for header in (
            "source",
            "dataset",
            "operation",
            "status",
            "rows_out",
            "duration_s",
        ):
            assert header in stdout
        for value in ("elexon", "fuelhh", "ingest", "success", "17", "1.5"):
            assert value in stdout
    else:
        assert "No pipeline runs in the last 24 hours." in stdout
