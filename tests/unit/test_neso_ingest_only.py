"""Ingest-only NESO families are skipped by transform, never failed (ADR-034 P-13).

T-B1-12 and T-B1-13. On master ``d7cf513`` every registry key without a
transformer made ``run_transform`` fail with ``No transformer registered``
(ADR-033 C-7), so ``pipeline neso_data_portal --all`` exited 1 on a clean run.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from _neso_registry_support import ingest_context

from gridflow.cli import _echo_transform_results
from gridflow.config.settings import load_settings
from gridflow.connectors.neso_data_portal.registry import load_registry
from gridflow.pipeline import runner as pipeline_runner

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

SOURCE = "neso_data_portal"
TABULAR = "aggregated_bsad"
FILES = "demand_forecast_1d_files"
DAY = datetime(2026, 10, 7, tzinfo=UTC)


def test_t_b1_12_tabular_skips_with_warnings_and_files_skip_quietly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Detects an ingest-only key failing, or passing silently as a transform."""
    pipeline_runner.import_transformers()
    with ingest_context(tmp_path / "data", monkeypatch) as ctx, caplog.at_level(logging.WARNING):
        results = pipeline_runner.run_transform(ctx, SOURCE, [TABULAR, FILES], DAY, DAY)
    tabular, files = results
    assert tabular.status == "completed_with_warnings"
    assert tabular.skip_reason == "ingest-only: no frozen schema record"
    assert files.status == "success"
    assert files.skip_reason == "non-tabular family: catalogue only"
    assert f"Transform skipped for {SOURCE}/{TABULAR}" in caplog.text
    _echo_transform_results(SOURCE, results)
    out = capsys.readouterr().out
    assert f"  {SOURCE}/{TABULAR}: skipped (ingest-only: no frozen schema record)" in out
    assert f"  {SOURCE}/{FILES}: skipped (non-tabular family: catalogue only)" in out


def test_t_b1_13_every_configured_key_transforms_without_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects ``--all`` failing on ingest-only keys (A's C-7), with an empty data dir."""
    pipeline_runner.import_transformers()
    keys = list(load_settings().get_source_config(SOURCE).datasets)
    assert len(keys) == 316
    with ingest_context(tmp_path / "data", monkeypatch) as ctx:
        results = pipeline_runner.run_transform(ctx, SOURCE, keys, DAY, DAY)
    failed = [(r.dataset, r.error) for r in results if r.status == "failed"]
    assert failed == []
    families = load_registry().families
    recorded = {key for key, (_package, family) in families.items() if family.record is not None}
    assert sum(r.skip_reason is not None for r in results) == 316 - 3 - len(recorded)
    assert not [r.dataset for r in results if r.dataset in recorded and r.skip_reason is not None]
