"""Quality report writer — writes QualityResult objects to Parquet and DuckDB."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import duckdb
import polars as pl

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.quality.checks import QualityResult

logger = logging.getLogger(__name__)


class QualityReporter:
    """Collects quality check results and writes them to storage."""

    def __init__(self, data_dir: Path, duckdb_path: Path):
        self.data_dir = data_dir
        self.duckdb_path = duckdb_path
        self._results: list[QualityResult] = []

    def add_result(self, result: QualityResult) -> None:
        """Add a quality check result."""
        self._results.append(result)

    def add_results(self, results: list[QualityResult]) -> None:
        """Add multiple quality check results."""
        self._results.extend(results)

    @property
    def results(self) -> list[QualityResult]:
        return self._results

    @property
    def failed_checks(self) -> list[QualityResult]:
        return [r for r in self._results if not r.passed]

    def write_report(self) -> int:
        """Write all collected results to DuckDB quality_reports table.

        Returns the number of results written.
        """
        if not self._results:
            logger.info("No quality results to write")
            return 0

        now = datetime.now(UTC)
        # One id per write_report() call; (run_id, id) is unique across runs.
        # `id` alone is only a within-run ordinal — the bare enumerate index
        # restarted at 0 every run and collided across the appended table.
        run_id = str(uuid.uuid4())
        rows = []
        for i, r in enumerate(self._results):
            rows.append(
                {
                    "run_id": run_id,
                    "id": i,
                    "run_date": now,
                    "check_name": r.check_name,
                    "dataset": r.dataset,
                    "source": r.source,
                    "passed": r.passed,
                    "metric": r.metric,
                    "detail": r.detail,
                }
            )

        # `df` is consumed by DuckDB's replacement scan (the "FROM df" query
        # below) — ruff's F841 can't see the string reference, so the binding
        # only *looks* unused. Removing it breaks the INSERT (Catalog Error:
        # Table 'df' does not exist).
        df = pl.DataFrame(rows)  # noqa: F841

        # Write to DuckDB
        try:
            con = duckdb.connect(str(self.duckdb_path))
            con.execute("""
                CREATE TABLE IF NOT EXISTS quality_reports (
                    run_id          VARCHAR,
                    id              INTEGER,
                    run_date        TIMESTAMP WITH TIME ZONE,
                    check_name      VARCHAR,
                    dataset         VARCHAR,
                    source          VARCHAR,
                    passed          BOOLEAN,
                    metric          DOUBLE,
                    detail          VARCHAR
                )
            """)
            con.execute(
                "INSERT INTO quality_reports "
                "(run_id, id, run_date, check_name, dataset, source, passed, metric, detail) "
                "SELECT run_id, id, run_date, check_name, dataset, source, passed, metric, detail "
                "FROM df"
            )
            con.close()
        except Exception as e:
            logger.error(f"Failed to write quality report to DuckDB: {e}")
            return 0

        logger.info(f"Wrote {len(self._results)} quality results to DuckDB")
        return len(self._results)

    def summary(self) -> str:
        """Return a human-readable summary of the quality results."""
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        failed = total - passed
        lines = [f"Quality Report: {passed}/{total} checks passed"]
        if failed > 0:
            lines.append("Failed checks:")
            for r in self.failed_checks:
                lines.append(f"  - [{r.source}/{r.dataset}] {r.check_name}: {r.detail}")
        return "\n".join(lines)
