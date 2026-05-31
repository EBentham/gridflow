"""Live CLI smoke tests for NESO command paths."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from gridflow.cli import app

if TYPE_CHECKING:
    from pathlib import Path

START = "2026-02-01"
END = "2026-02-02"
CURATED_DATASET = "carbon_intensity"

runner = CliRunner()


@dataclass(frozen=True)
class CliSmokePaths:
    data_dir: Path
    duckdb_path: Path
    log_dir: Path


def _isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliSmokePaths:
    paths = CliSmokePaths(
        data_dir=tmp_path / "data",
        duckdb_path=tmp_path / "catalogue" / "gridflow.duckdb",
        log_dir=tmp_path / "logs",
    )
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(paths.data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(paths.duckdb_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(paths.log_dir))
    return paths


def _invoke_cli(args: list[str]):
    result = runner.invoke(app, args)
    assert result.exit_code == 0, (
        f"command failed: gridflow {' '.join(args)}\n"
        f"exit_code={result.exit_code}\n"
        f"output={result.output}\n"
        f"exception={result.exception!r}"
    )
    return result


def _assert_outputs(paths: CliSmokePaths, dataset: str) -> None:
    bronze_root = paths.data_dir / "bronze" / "neso" / dataset
    silver_root = paths.data_dir / "silver" / "neso" / dataset

    assert list(bronze_root.rglob("raw_*.json")), f"missing bronze files: {bronze_root}"
    assert list(silver_root.rglob("*.parquet")), f"missing silver files: {silver_root}"


@pytest.mark.live
def test_live_pipeline_neso_carbon_intensity_creates_bronze_and_silver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _isolated_env(tmp_path, monkeypatch)

    result = _invoke_cli(
        [
            "pipeline",
            "neso",
            CURATED_DATASET,
            "--start",
            START,
            "--end",
            END,
        ]
    )

    assert "Pipeline: neso" in result.output
    assert "Bronze (ingest)" in result.output
    assert "Silver (transform)" in result.output
    assert "Pipeline complete" in result.output
    _assert_outputs(paths, CURATED_DATASET)
