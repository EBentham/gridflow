"""Live CLI smoke tests for GIE AGSI command paths."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from typer.testing import CliRunner

from gridflow.cli import app

START = "2026-05-01"
END = "2026-05-01"
BACKFILL_END = "2026-05-02"
CURATED_DATASET = "storage_reports"
PHASE_DOC = (
    Path(__file__).resolve().parents[2]
    / ".planning"
    / "phases"
    / "L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification"
    / "L4-LIVE-COMMANDS.md"
)

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


def _require_gie_key() -> None:
    if not os.environ.get("GIE_API_KEY"):
        pytest.skip("source=gie_agsi stage=setup outcome=missing GIE_API_KEY")


def _assert_under_tmp(path: Path, tmp_path: Path) -> None:
    resolved = path.resolve()
    tmp_resolved = tmp_path.resolve()
    assert resolved == tmp_resolved or tmp_resolved in resolved.parents


def _assert_outputs(paths: CliSmokePaths, dataset: str, tmp_path: Path) -> None:
    bronze_root = paths.data_dir / "bronze" / "gie_agsi" / dataset
    silver_root = paths.data_dir / "silver" / "gie_agsi" / dataset

    bronze_files = list(bronze_root.rglob("raw_*.json"))
    silver_files = list(silver_root.rglob("*.parquet"))
    assert bronze_files, f"missing bronze files: {bronze_root}"
    assert silver_files, f"missing silver files: {silver_root}"
    for output in bronze_files + silver_files:
        _assert_under_tmp(output, tmp_path)


def _invoke_cli(args: list[str]):
    result = runner.invoke(app, args)
    assert result.exit_code == 0, (
        f"command failed: gridflow {' '.join(args)}\n"
        f"exit_code={result.exit_code}\n"
        f"output={result.output}\n"
        f"exception={result.exception!r}"
    )
    return result


@pytest.mark.live
def test_live_pipeline_gie_agsi_storage_reports_creates_bronze_and_silver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_gie_key()
    paths = _isolated_env(tmp_path, monkeypatch)

    result = _invoke_cli([
        "pipeline",
        "gie_agsi",
        CURATED_DATASET,
        "--start",
        START,
        "--end",
        END,
    ])

    assert "Pipeline: gie_agsi" in result.output
    assert "Bronze (ingest)" in result.output
    assert "Silver (transform)" in result.output
    assert "Pipeline complete" in result.output
    _assert_outputs(paths, CURATED_DATASET, tmp_path)


@pytest.mark.live
def test_live_ingest_then_transform_gie_agsi_storage_reports_creates_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_gie_key()
    paths = _isolated_env(tmp_path, monkeypatch)

    ingest_result = _invoke_cli([
        "ingest",
        "gie_agsi",
        CURATED_DATASET,
        "--start",
        START,
        "--end",
        END,
    ])
    assert f"gie_agsi/{CURATED_DATASET}" in ingest_result.output

    transform_result = _invoke_cli([
        "transform",
        "gie_agsi",
        CURATED_DATASET,
        "--start",
        START,
        "--end",
        END,
    ])
    assert f"gie_agsi/{CURATED_DATASET}" in transform_result.output
    _assert_outputs(paths, CURATED_DATASET, tmp_path)


@pytest.mark.live
def test_live_backfill_gie_agsi_storage_reports_creates_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_gie_key()
    paths = _isolated_env(tmp_path, monkeypatch)

    result = _invoke_cli([
        "backfill",
        "gie_agsi",
        CURATED_DATASET,
        "--start",
        START,
        "--end",
        BACKFILL_END,
        "--chunk-days",
        "1",
    ])

    assert f"Backfilling gie_agsi/{CURATED_DATASET}" in result.output
    assert "Backfill complete" in result.output
    _assert_outputs(paths, CURATED_DATASET, tmp_path)


def test_l4_live_command_documentation_covers_closeout_requirements() -> None:
    content = PHASE_DOC.read_text(encoding="utf-8")
    required_terms = {
        "pipeline",
        "ingest",
        "transform",
        "backfill",
        "GIE_API_KEY",
        "GRIDFLOW_DATA_DIR",
        "GRIDFLOW_DUCKDB_PATH",
        "GRIDFLOW_LOG_DIR",
        "AGSI-11",
        "AGSI-12",
        "unavailability",
        "ALSI",
    }
    missing = sorted(term for term in required_terms if term not in content)
    assert not missing, f"missing L4 command documentation terms: {missing}"
