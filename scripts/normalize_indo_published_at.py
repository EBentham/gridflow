"""One-time migration: normalize the elexon/indo silver schema.

Back-fills a typed-null ``published_at`` column into any ``elexon/indo`` partition
file that omits it. The INDO silver contract (:class:`gridflow.schemas.elexon.ElexonINDO`)
declares ``published_at`` as an always-present nullable column, but the transformer
historically dropped it entirely when a bronze record lacked ``publishTime``. The
resulting schema drift breaks ``SELECT *`` partition-glob reads that span files which
do carry the column (DuckDB unifies the glob schema before applying the event-time
filter).

Idempotent and silver-only: files already carrying ``published_at`` are skipped, bronze
is never read or modified, and every rewrite preserves the existing bitemporal lineage
columns (``event_time`` / ``available_at`` / ``source_run_id`` / ``dataset_version``)
exactly — only the one nullable column is added. Writes go through the atomic
temp-then-``os.replace`` path in :func:`gridflow.storage.parquet.write_parquet`.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from gridflow.storage.parquet import write_parquet

logger = logging.getLogger(__name__)

# The single nullable column the drift omits, and where it belongs in the canonical
# INDO column order (immediately after the measure column, matching the transformer's
# output_cols). Reordering keeps the on-disk store uniform even though union_by_name
# readers are position-insensitive.
_PUBLISHED_AT = "published_at"
_PUBLISHED_AT_DTYPE = pl.Datetime("us", "UTC")
_INSERT_AFTER = "initial_demand_outturn_mw"


def _indo_partition_files(data_dir: Path) -> list[Path]:
    indo_dir = data_dir / "silver" / "elexon" / "indo"
    return sorted(indo_dir.glob("year=*/**/*.parquet"))


def _canonical_order(columns: list[str]) -> list[str]:
    """Return ``columns`` with ``published_at`` placed after the measure column."""
    rest = [c for c in columns if c != _PUBLISHED_AT]
    if _INSERT_AFTER in rest:
        idx = rest.index(_INSERT_AFTER) + 1
        return rest[:idx] + [_PUBLISHED_AT] + rest[idx:]
    # Defensive: measure column missing (unexpected) — append rather than fail.
    return rest + [_PUBLISHED_AT]


def normalize_indo_published_at(data_dir: Path, *, dry_run: bool = False) -> int:
    """Back-fill typed-null ``published_at`` into drifted indo partition files.

    Args:
        data_dir: Gridflow data root (the directory containing ``silver/``).
        dry_run: When True, only report which files would change; write nothing.

    Returns:
        The number of files that were (or would be) rewritten.
    """
    files = _indo_partition_files(data_dir)
    if not files:
        logger.warning("No indo partition files found under %s", data_dir)
        return 0

    rewritten = 0
    for file in files:
        schema = pl.read_parquet_schema(file)
        if _PUBLISHED_AT in schema:
            continue

        rewritten += 1
        if dry_run:
            logger.info("[dry-run] would back-fill %s: %s", _PUBLISHED_AT, file)
            continue

        df = pl.read_parquet(file)
        original_height = df.height
        original_cols = set(df.columns)

        df = df.with_columns(
            pl.lit(None).cast(_PUBLISHED_AT_DTYPE).alias(_PUBLISHED_AT)
        ).select(_canonical_order(df.columns))

        # Fail loud before replacing the original: the rewrite must add exactly the
        # one nullable column and touch no rows.
        if df.height != original_height:
            raise RuntimeError(
                f"row count changed for {file}: {original_height} -> {df.height}"
            )
        if set(df.columns) != original_cols | {_PUBLISHED_AT}:
            raise RuntimeError(f"unexpected column set after back-fill for {file}: {df.columns}")
        if df.schema[_PUBLISHED_AT] != _PUBLISHED_AT_DTYPE:
            raise RuntimeError(
                f"published_at dtype mismatch for {file}: {df.schema[_PUBLISHED_AT]}"
            )

        write_parquet(df, file)
        logger.info("back-filled %s into %s (%d rows)", _PUBLISHED_AT, file, original_height)

    verb = "would rewrite" if dry_run else "rewrote"
    logger.info(
        "indo published_at normalization complete: %s %d/%d files",
        verb,
        rewritten,
        len(files),
    )
    return rewritten


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Gridflow data root containing silver/ (default: ./data)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report which files would change without writing.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    normalize_indo_published_at(args.data_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
