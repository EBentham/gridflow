"""Live CLI smoke tests for Elexon command paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from typer.testing import CliRunner

from gridflow.cli import app

START = "2026-02-01"
END = "2026-02-02"
PUBLISH_DATASET = "freq"
PHASE_DOC = (
    Path(__file__).resolve().parents[2]
    / ".planning"
    / "phases"
    / "I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs"
    / "I4-LIVE-COMMANDS.md"
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


def _assert_under_tmp(path: Path, tmp_path: Path) -> None:
    assert tmp_path.resolve() in path.resolve().parents or path.resolve() == tmp_path.resolve()


def _assert_bronze_created(paths: CliSmokePaths, dataset: str, tmp_path: Path) -> None:
    bronze_root = paths.data_dir / "bronze" / "elexon" / dataset
    assert bronze_root.exists(), f"missing bronze root for {dataset}: {bronze_root}"
    bronze_files = list(bronze_root.rglob("raw_*.json"))
    assert bronze_files, f"missing bronze files for {dataset}: {bronze_root}"
    for bronze_file in bronze_files:
        _assert_under_tmp(bronze_file, tmp_path)


def _assert_silver_created(paths: CliSmokePaths, dataset: str, tmp_path: Path) -> None:
    silver_root = paths.data_dir / "silver" / "elexon" / dataset
    assert silver_root.exists(), f"missing silver root for {dataset}: {silver_root}"
    silver_files = list(silver_root.rglob("*.parquet"))
    assert silver_files, f"missing silver parquet files for {dataset}: {silver_root}"
    for silver_file in silver_files:
        _assert_under_tmp(silver_file, tmp_path)


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
def test_live_pipeline_elexon_system_prices_creates_bronze_and_silver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _isolated_env(tmp_path, monkeypatch)

    result = _invoke_cli(["pipeline", "elexon", "system_prices", "--start", START, "--end", END])

    assert "Pipeline: elexon" in result.output
    assert "Bronze (ingest)" in result.output
    assert "Silver (transform)" in result.output
    assert "Pipeline complete" in result.output
    _assert_bronze_created(paths, "system_prices", tmp_path)
    _assert_silver_created(paths, "system_prices", tmp_path)


@pytest.mark.live
def test_live_ingest_then_transform_elexon_freq_creates_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _isolated_env(tmp_path, monkeypatch)

    ingest_result = _invoke_cli(
        [
            "ingest",
            "elexon",
            PUBLISH_DATASET,
            "--start",
            START,
            "--end",
            END,
        ]
    )
    assert f"elexon/{PUBLISH_DATASET}" in ingest_result.output
    _assert_bronze_created(paths, PUBLISH_DATASET, tmp_path)

    transform_result = _invoke_cli(
        [
            "transform",
            "elexon",
            PUBLISH_DATASET,
            "--start",
            START,
            "--end",
            END,
        ]
    )
    assert f"elexon/{PUBLISH_DATASET}" in transform_result.output
    _assert_silver_created(paths, PUBLISH_DATASET, tmp_path)


@pytest.mark.live
@pytest.mark.parametrize(
    ("dataset", "purpose"),
    [
        ("system_prices", "path-date backfill"),
        (PUBLISH_DATASET, "publish/from-to datetime backfill"),
        ("bmunits_reference", "no-param/reference backfill"),
    ],
)
def test_live_backfill_elexon_curated_dataset_creates_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dataset: str,
    purpose: str,
) -> None:
    paths = _isolated_env(tmp_path, monkeypatch)

    result = _invoke_cli(
        [
            "backfill",
            "elexon",
            dataset,
            "--start",
            START,
            "--end",
            END,
            "--chunk-days",
            "1",
        ]
    )

    assert purpose
    assert f"Backfilling elexon/{dataset}" in result.output
    assert "Backfill complete" in result.output
    _assert_bronze_created(paths, dataset, tmp_path)
    _assert_silver_created(paths, dataset, tmp_path)


def test_i4_live_command_documentation_covers_cli_closeout_requirements() -> None:
    content = PHASE_DOC.read_text(encoding="utf-8")
    required_terms = {
        "pipeline",
        "ingest",
        "transform",
        "backfill",
        "system_prices",
        "bmunits_reference",
        "ELEXON-CLI-01",
        "ELEXON-CLI-02",
        "ELEXON-CLI-03",
        "ELEXON-DOC-01",
        "ELEXON-DOC-02",
    }
    missing = sorted(term for term in required_terms if term not in content)
    assert not missing, f"missing I4 command documentation terms: {missing}"
