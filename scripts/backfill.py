"""Backfill helper script for date ranges."""

from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta


def backfill(
    source: str,
    dataset: str,
    start: date,
    end: date,
    chunk_days: int = 7,
) -> None:
    """Run backfill in chunks."""
    current = start
    chunk_num = 0

    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        chunk_num += 1
        print(f"Chunk {chunk_num}: {current} to {chunk_end}")

        # Ingest
        subprocess.run(
            [
                sys.executable, "-m", "gridflow", "ingest",
                source, dataset,
                "--start", current.isoformat(),
                "--end", chunk_end.isoformat(),
            ],
            check=True,
        )

        # Transform
        subprocess.run(
            [
                sys.executable, "-m", "gridflow", "transform",
                source, dataset,
                "--start", current.isoformat(),
                "--end", chunk_end.isoformat(),
            ],
            check=True,
        )

        current = chunk_end

    print(f"Backfill complete: {chunk_num} chunks")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("dataset")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--chunk-days", type=int, default=7)
    args = parser.parse_args()

    backfill(
        source=args.source,
        dataset=args.dataset,
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        chunk_days=args.chunk_days,
    )
