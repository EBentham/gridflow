"""Backfill helper script for date ranges.

A thin adapter over :func:`gridflow.pipeline.runner.run_backfill` (CH-ARCH-01 /
C3-8). The old implementation shelled out to ``python -m gridflow ingest`` /
``transform`` per chunk via ``subprocess`` — a fresh interpreter, fresh DuckDB
connection, and a fresh registry import for every chunk. It now runs in-process
on a single shared connection, which is faster and lets the runner reuse the
catalogue across chunks.

Two behaviour reconciliations vs the old subprocess form (documented, intended):
  * ``chunk_days`` default is now **1** (was 7), matching the CLI's ``backfill``
    default — the two entry points no longer disagree on chunk size.
  * Each chunk's transform end is ``chunk_end - 1 day`` (the runner preserves the
    CLI's deliberate off-by-one: ``chunk_end`` is the exclusive API boundary but
    transform date iteration is inclusive). The old subprocess form transformed
    through ``chunk_end`` inclusive, which spuriously warned "no bronze data" for
    the boundary date.
"""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime
from pathlib import Path

# Ensure `gridflow` is importable regardless of how this script is launched.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT / "src"))


def backfill(
    source: str,
    dataset: str,
    start: date,
    end: date,
    chunk_days: int = 1,
) -> None:
    """Run an in-process chunked backfill for one source/dataset.

    Args:
        source: Data source name (e.g. ``"elexon"``).
        dataset: Dataset name (e.g. ``"fuelhh"``).
        start: Inclusive range start date.
        end: Exclusive range end date.
        chunk_days: Days per chunk (default 1, matching the CLI).

    Raises:
        SystemExit: With code 1 if any chunk's ingest or transform failed.
    """
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    runner.import_connectors()
    runner.import_transformers()

    start_dt = datetime(start.year, start.month, start.day, tzinfo=UTC)
    end_dt = datetime(end.year, end.month, end.day, tzinfo=UTC)

    report = runner.run_backfill(
        settings,
        source,
        [dataset],
        start_dt,
        end_dt,
        chunk_days=chunk_days,
    )

    for r in report.results:
        if r.status == "failed":
            print(f"  {r.operation} {source}/{r.dataset}: FAILED - {r.error}", file=sys.stderr)

    if not report.ok:
        print(f"Backfill FAILED: {len(report.failed)} step(s) failed.", file=sys.stderr)
        raise SystemExit(1)

    print(f"Backfill complete: {len(report.results)} step(s) processed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("dataset")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--chunk-days", type=int, default=1)
    args = parser.parse_args()

    backfill(
        source=args.source,
        dataset=args.dataset,
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        chunk_days=args.chunk_days,
    )
