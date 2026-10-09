"""Adjudicated reconcile gaps (v0.22-GEN-2H, ADR-040).

Every test builds a synthetic resource-partitioned family on a short tmp data root through
the multi-resource helpers (``install_multi`` / ``capture_multi``), so nothing reads or writes
``C:/gridflow-data``. The CLI is driven in process through ``main`` with ``GRIDFLOW_DATA_DIR``
pointed at the tmp root.

**I-1 (byte-unchanged).** :func:`w0_outputs` renders world W0 (two overlap gaps, a
duplicate-key failure, a missing later capture and a ghost completion) through
``reconcile``, the CLI and ``drain``. ``tests/fixtures/neso_data_portal/gen2h/
reconcile_base_pin.json`` was written from it on the untouched base (master ``0f77c85``),
before any ``src/`` edit of the unit; with no adjudication entry the output must stay equal.
"""

from __future__ import annotations

import contextlib
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from test_neso_multi_resource import DAY, HEADER, KEY, capture_multi, install_multi

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.reconcile import main
from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    completion_row,
    record_completion,
)
from gridflow.silver.neso_data_portal.reconcile import drain, reconcile

PIN_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "neso_data_portal"
    / "gen2h"
    / "reconcile_base_pin.json"
)
CUTOFF = DAY.isoformat()
GHOST = f"bronze/neso_data_portal/{KEY}/2026/10/07/raw_ghost.csv"


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 7, hour, minute, tzinfo=UTC)


def point_settings(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point ``load_settings`` (and so the CLI) at ``data``; no gold views."""
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(data / "cat.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(data / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    pipeline_runner.import_transformers()


@pytest.fixture
def data(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A short data root the settings (and so the CLI) point at."""
    root = tmp_path_factory.mktemp("j")
    point_settings(root, monkeypatch)
    return root


def run_cli(*args: str) -> tuple[int, list[str]]:
    """``main(args)`` with stdout captured; returns ``(exit code, lines)``."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = main(list(args))
    return code, buffer.getvalue().splitlines()


def build_w0(data: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """World W0 on the base: every gap category a synthetic family can show.

    - A = (P1, P2) and B = (P2), transformed: two ``overlap`` gaps;
    - C repeats one pair: ``failed`` (``DuplicateEntityKeyError``), drainable;
    - a later capture of A written after the run: ``missing``, drainable;
    - one ghost completion: ``orphaned`` (a).

    Every name and timestamp is fixed, so every line is deterministic.

    Returns:
        The capture ids by role.
    """
    generated = install_multi(monkeypatch, data)
    a = capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n2026-10-07,2,2.0\n", _t(8))
    b = capture_multi(data, "B", HEADER + b"2026-10-07,2,5.0\n", _t(9))
    c = capture_multi(data, "C", HEADER + b"2026-10-07,3,1.0\n2026-10-07,3,2.0\n", _t(9, 30))
    with contextlib.suppress(NesoCaptureFailedError):
        generated.transformers[KEY](data).run(DAY, run_id="r")
    later = capture_multi(data, "A", HEADER + b"2026-10-07,1,7.0\n", _t(10))
    record_completion(
        data,
        completion_row(
            family=KEY,
            capture_id=GHOST,
            source_key=KEY,
            partition_date=DAY,
            resource_id="eeeeeeee-0000-4000-8000-00000000000a",
            body_sha256="0" * 64,
            capture_written_at=_t(7),
            published_at=_t(7),
            outcome="valid_empty",
            row_count=0,
            rows_excluded=0,
            output_path=None,
            children=[],
            versions=generated.transformers[KEY].versions(),
        ),
    )
    return {"a": a, "b": b, "c": c, "later": later}


def w0_outputs(data: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Build W0, then render ``reconcile`` (API and CLI) and ``drain``; the pin's shape."""
    build_w0(data, monkeypatch)
    loaded = registry_module.load_registry()
    api = reconcile(data, loaded, [KEY], DAY)
    code, lines = run_cli(KEY, "--cutoff", CUTOFF)
    assert lines == api.lines()
    drained = drain(data, loaded, [KEY], DAY, lambda: None)
    return {
        "reconcile": {"exit": code, "lines": lines},
        "drain": {"lines": drained.lines()},
    }


def test_i1_reconcile_output_is_byte_unchanged_without_entries(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects any change to reconcile's or drain's lines, or to the CLI exit code, for a
    family no adjudication entry names (I-1 (a)): W0 against the pin written on the base."""
    pin = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    assert w0_outputs(data, monkeypatch) == pin
