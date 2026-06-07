"""CH2-03 — failures must surface, not be swallowed (CH-COR-03, CH-COR-05).

Three classes of silent-failure bug this guards against:

- A broken CORE connector/transformer module import is currently swallowed by
  ``contextlib.suppress(ImportError)`` in ``cli._import_connectors`` /
  ``_import_transformers``. A genuine bug then masquerades as a missing
  registration ("unknown source"). It must be LOGGED with the module name.
- ``PipelineRunTracker``'s status writes swallow to a bare ``logger.warning``.
  A failed status write can leave a run stuck at ``'running'``; that must be
  loud (ERROR with context) but NON-fatal to the ingest/transform.
- ``QualityReporter.write_report`` returns ``0`` on a DB-write failure, which is
  indistinguishable from a clean "no results" empty write. The failure must
  surface to the caller (re-raise) rather than look like success.
"""

from __future__ import annotations

import builtins
import logging
from typing import TYPE_CHECKING

import pytest

from gridflow.observability import PipelineRunTracker
from gridflow.quality.checks import QualityResult
from gridflow.quality.reporter import QualityReporter

if TYPE_CHECKING:
    from pathlib import Path


# --- (A) CH-COR-03: broken import is logged with its module name ---------------


def test_broken_connector_import_is_logged_with_module_name(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A core connector module that fails to import must WARN with its name.

    RED before fix: ``contextlib.suppress(ImportError)`` swallows it silently.
    """
    from gridflow import cli

    broken = "gridflow.connectors.gie"
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == broken:
            raise ImportError("boom: simulated broken connector module")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with caplog.at_level(logging.WARNING, logger="gridflow.cli"):
        cli._import_connectors()

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(broken in r.getMessage() for r in warnings), (
        f"expected a WARNING naming the broken module; got: {[r.getMessage() for r in warnings]}"
    )


def test_broken_transformer_import_is_logged_with_module_name(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A core transformer module that fails to import must WARN with its name."""
    from gridflow import cli

    broken = "gridflow.silver.neso"
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == broken:
            raise ImportError("boom: simulated broken transformer module")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with caplog.at_level(logging.WARNING, logger="gridflow.cli"):
        cli._import_transformers()

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(broken in r.getMessage() for r in warnings), (
        f"expected a WARNING naming the broken module; got: {[r.getMessage() for r in warnings]}"
    )


# --- (B) CH-COR-05: failed status write is loud but non-fatal ------------------


class _RaisingConnection:
    """A DuckDB-connection stub whose ``execute`` always raises."""

    def execute(self, *args: object, **kwargs: object) -> object:
        raise RuntimeError("simulated tracking-DB failure")


def test_failed_status_write_logs_error_and_does_not_crash(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed ``pipeline_runs`` write must log ERROR with context, not crash.

    RED before fix: it swallows to ``logger.warning`` (no ERROR record), so a
    lost status transition is invisible. The run must NOT crash on a tracking
    hiccup, and the loss of the transition must be explicit at ERROR level.
    """
    con = _RaisingConnection()

    with caplog.at_level(logging.ERROR, logger="gridflow.observability"):
        # __init__ runs _record_start() which hits the raising execute.
        tracker = PipelineRunTracker(
            con,  # type: ignore[arg-type]
            source="gie",
            dataset="storage",
            operation="ingest",
        )
        # complete() must also be non-fatal.
        tracker.complete(rows_in=1, rows_out=1)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors, "expected at least one ERROR record for the failed status write"
    # Context must be present so the operator can locate the stuck run.
    joined = " ".join(r.getMessage() for r in errors)
    assert "gie" in joined and "storage" in joined and "ingest" in joined, (
        f"expected operation/source/dataset context in the ERROR; got: {joined}"
    )
    assert tracker.run_id  # tracker still usable; no exception escaped


def test_failed_completion_write_marks_lost_transition(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed completion write must state the status transition was lost."""
    con = _RaisingConnection()

    with caplog.at_level(logging.ERROR, logger="gridflow.observability"):
        tracker = PipelineRunTracker(
            con,  # type: ignore[arg-type]
            source="elexon",
            dataset="system_prices",
            operation="ingest",
        )
        tracker.complete(rows_out=5)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    completion_errors = [r for r in errors if "running" in r.getMessage().lower()]
    assert completion_errors, (
        "expected the completion failure to note the run remains 'running'; "
        f"got: {[r.getMessage() for r in errors]}"
    )


def test_failed_fail_write_marks_lost_transition(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed ``fail()`` write is the highest-stakes lost transition.

    If the ``'failed'`` write itself fails, the row stays ``'running'`` even
    though the operation actually failed — doubly misleading. It must log ERROR
    with context and stay non-fatal.
    """
    con = _RaisingConnection()

    with caplog.at_level(logging.ERROR, logger="gridflow.observability"):
        tracker = PipelineRunTracker(
            con,  # type: ignore[arg-type]
            source="entsoe",
            dataset="day_ahead_prices",
            operation="ingest",
        )
        tracker.fail("upstream 500")

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    fail_errors = [r for r in errors if "running" in r.getMessage().lower()]
    assert fail_errors, (
        "expected the fail() write failure to note the run remains 'running'; "
        f"got: {[r.getMessage() for r in errors]}"
    )
    joined = " ".join(r.getMessage() for r in fail_errors)
    assert "entsoe" in joined and "day_ahead_prices" in joined, (
        f"expected source/dataset context in the fail() ERROR; got: {joined}"
    )


# --- (B) CH-COR-05: reporter DB-write failure surfaces -------------------------


def test_write_report_db_failure_surfaces(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A DB-write failure must surface (raise + ERROR), not return a clean 0.

    RED before fix: ``write_report`` returns 0 on failure, identical to the
    legitimate empty-results path, so a failed write looks like success.
    """
    import gridflow.quality.reporter as reporter_mod

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    duckdb_path = tmp_path / "q.duckdb"

    reporter = QualityReporter(data_dir, duckdb_path)
    reporter.add_result(QualityResult("c", "ds", "src", True, 0.0, "ok"))

    def boom(*args: object, **kwargs: object) -> object:
        raise RuntimeError("simulated DuckDB connect failure")

    monkeypatch.setattr(reporter_mod.duckdb, "connect", boom)

    with (
        caplog.at_level(logging.ERROR, logger="gridflow.quality.reporter"),
        pytest.raises(RuntimeError),
    ):
        reporter.write_report()

    assert any(r.levelno == logging.ERROR for r in caplog.records), (
        "expected an ERROR record for the failed quality-report write"
    )


def test_write_report_empty_still_returns_zero(tmp_path: Path) -> None:
    """The legitimate empty-results path must still return 0 (not raise)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    reporter = QualityReporter(data_dir, tmp_path / "q.duckdb")
    assert reporter.write_report() == 0
