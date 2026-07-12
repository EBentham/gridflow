"""One-time migration: normalize published_at across the Elexon silver store.

Back-fills a typed-null ``published_at`` column into any partition file that omits
it, for every Elexon dataset whose silver schema declares ``published_at`` as an
always-present nullable column and whose transformer emits it. Historically these
transformers dropped the column entirely when a bronze record lacked the publish
field (publishTime / publishDateTime), and many partitions were written before the
column was emitted at all — so the on-disk stores drifted. The drift breaks
``SELECT *`` partition-glob reads that span files which do carry the column (DuckDB
matches columns by name and fails the read when one is missing).

Pairs with the transformer determinism fix (each transformer now emits a typed-null
``published_at`` when bronze lacks the publish field). Without this back-fill the
forward-only fix would itself introduce drift: old files (no column) vs new files
(typed-null column).

Idempotent and silver-only: files already carrying ``published_at`` are skipped,
bronze is never touched, and every rewrite preserves the existing bitemporal lineage
columns exactly. DuckDB reads by column name, so the inserted column's position does
not affect read correctness; it is placed just before ``data_provider`` to match the
transformer's own column order. Writes go through the atomic temp-then-``os.replace``
path in :func:`gridflow.storage.parquet.write_parquet`.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from gridflow.storage.parquet import is_reserved_temp_path, write_parquet

logger = logging.getLogger(__name__)

_PUBLISHED_AT = "published_at"
_PUBLISHED_AT_DTYPE = pl.Datetime("us", "UTC")

# Elexon datasets whose transformer emits published_at as a nullable contract column.
# (Excludes fuelinst, which routes the publish time into timestamp_utc. wind_forecast
# and demand_forecast now emit published_at too, but their historical partitions carry
# a legacy issue_time column needing an issue_time->published_at rename migration —
# tracked separately, not handled by this typed-null back-fill pass.)
_DATASETS = (
    "indo",
    "imbalngc",
    "melngc",
    "inddem",
    "itsdo",
    "indgen",
    "agpt",
    "agws",
    "atl",
    "nonbm",
    "fou2t14d",
    "lolpdrm",
    "fuelhh",
    "uou2t14d",
    "tsdfd",
)


def _partition_files(data_dir: Path, dataset: str) -> list[Path]:
    return sorted(
        file
        for file in (data_dir / "silver" / "elexon" / dataset).glob("year=*/**/*.parquet")
        if not is_reserved_temp_path(file)
    )


def _canonical_order(columns: list[str]) -> list[str]:
    """Place published_at just before data_provider (else before the bitemporal block)."""
    rest = [c for c in columns if c != _PUBLISHED_AT]
    for anchor in ("data_provider", "event_time"):
        if anchor in rest:
            idx = rest.index(anchor)
            return rest[:idx] + [_PUBLISHED_AT] + rest[idx:]
    return rest + [_PUBLISHED_AT]


def normalize_published_at(data_dir: Path, *, dry_run: bool = False) -> int:
    """Back-fill typed-null published_at into drifted Elexon partition files.

    Args:
        data_dir: Gridflow data root (the directory containing ``silver/``).
        dry_run: When True, only report which files would change; write nothing.

    Returns:
        The number of files that were (or would be) rewritten.
    """
    rewritten = 0
    for dataset in _DATASETS:
        files = _partition_files(data_dir, dataset)
        drifted = [f for f in files if _PUBLISHED_AT not in pl.read_parquet_schema(f)]
        if not drifted:
            logger.info("%-10s ok (%d files, none missing %s)", dataset, len(files), _PUBLISHED_AT)
            continue
        logger.info(
            "%-10s back-filling %s into %d/%d files",
            dataset,
            _PUBLISHED_AT,
            len(drifted),
            len(files),
        )
        for file in drifted:
            rewritten += 1
            if dry_run:
                logger.info("  [dry-run] would back-fill %s", file)
                continue

            df = pl.read_parquet(file)
            original_height = df.height
            original_cols = set(df.columns)

            df = df.with_columns(
                pl.lit(None).cast(_PUBLISHED_AT_DTYPE).alias(_PUBLISHED_AT)
            ).select(_canonical_order(df.columns))

            # Fail loud before replacing the original: add exactly the one nullable
            # column and touch no rows.
            if df.height != original_height:
                raise RuntimeError(
                    f"row count changed for {file}: {original_height} -> {df.height}"
                )
            if set(df.columns) != original_cols | {_PUBLISHED_AT}:
                raise RuntimeError(
                    f"unexpected column set after back-fill for {file}: {df.columns}"
                )
            if df.schema[_PUBLISHED_AT] != _PUBLISHED_AT_DTYPE:
                raise RuntimeError(
                    f"published_at dtype mismatch for {file}: {df.schema[_PUBLISHED_AT]}"
                )

            write_parquet(df, file)

    verb = "would rewrite" if dry_run else "rewrote"
    logger.info("published_at normalization complete: %s %d files", verb, rewritten)
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
    normalize_published_at(args.data_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
