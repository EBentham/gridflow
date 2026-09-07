"""Bounded, network-free partition-trim rebuild and conservation harness.

The harness reads retained bronze from ``--input-root`` and writes silver only
below ``--output-root``. Baseline and candidate checkouts are intentionally run
as separate processes; their JSON manifests can then be compared byte-for-byte.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from gridflow.pipeline.runner import import_transformers
from gridflow.silver import base as silver_base
from gridflow.silver.registry import get_transformer
from gridflow.storage.paths import PathBuilder
from gridflow.utils.time import utc_to_settlement_period


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _files(root: Path, pattern: str) -> dict[str, dict[str, int | str]]:
    return {
        path.relative_to(root).as_posix(): {
            "sha256": _hash(path),
            "size": path.stat().st_size,
        }
        for path in sorted(root.rglob(pattern))
        if path.is_file()
    }


def _raw_records(input_root: Path, dataset: str, source_date: date) -> list[dict[str, Any]]:
    partition = PathBuilder(input_root).bronze_date_dir("elexon", dataset, source_date)
    records: list[dict[str, Any]] = []
    if not partition.exists():
        return records
    for body in sorted(partition.glob("raw_*.json")):
        if body.name.endswith(".meta.json"):
            continue
        payload = json.loads(body.read_text())
        rows = payload.get("data", []) if isinstance(payload, dict) else payload
        if isinstance(rows, list):
            records.extend(row for row in rows if isinstance(row, dict))
    return records


def raw_expected_keys(
    input_root: Path,
    dataset: str,
    start: date,
    end: date,
) -> set[tuple[object, ...]]:
    """Derive destination identities directly from the D-1..end bronze universe."""
    expected: set[tuple[object, ...]] = set()
    source_date = start - timedelta(days=1)
    while source_date <= end:
        for row in _raw_records(input_root, dataset, source_date):
            if dataset == "mid":
                owner = date.fromisoformat(
                    str(row.get("settlementDate", row.get("settlement_date")))
                )
                period = int(row.get("settlementPeriod", row.get("settlement_period")))
                provider = row.get(
                    "dataProvider",
                    row.get("dataProviderId", row.get("data_provider_id")),
                )
                key: tuple[object, ...] = (owner, period, provider)
            elif dataset == "fuelhh":
                raw_start = next(
                    (
                        row.get(name)
                        for name in ("startTime", "startTimeOfHalfHrPeriod", "start_time")
                        if row.get(name)
                    ),
                    None,
                )
                if raw_start is not None:
                    timestamp = datetime.fromisoformat(str(raw_start).replace("Z", "+00:00"))
                    if timestamp.tzinfo is None:
                        raise ValueError("raw FUELHH start time must be timezone-aware")
                    owner, period = utc_to_settlement_period(timestamp.astimezone(UTC))
                else:
                    owner = date.fromisoformat(
                        str(row.get("settlementDate", row.get("settlement_date")))
                    )
                    period = int(row.get("settlementPeriod", row.get("settlement_period")))
                key = (owner, period, row.get("fuelType", row.get("fuel_type")))
            else:
                raise ValueError("raw key oracle supports only mid and fuelhh")
            if start <= owner <= end:
                expected.add(key)
        source_date += timedelta(days=1)
    return expected


def actual_keys(
    output_root: Path,
    dataset: str,
    start: date,
    end: date,
) -> tuple[
    set[tuple[object, ...]],
    list[tuple[object, ...]],
    dict[tuple[object, ...], int],
]:
    """Read rebuilt keys and report rows stored under a non-owner destination."""
    keys: set[tuple[object, ...]] = set()
    multiplicity: Counter[tuple[object, ...]] = Counter()
    misplaced: list[tuple[object, ...]] = []
    paths = PathBuilder(output_root)
    destination = start
    while destination <= end:
        path = paths.silver_file("elexon", dataset, destination)
        if path.exists():
            frame = pl.read_parquet(path)
            optional = ["data_provider_id"] if dataset == "mid" else ["fuel_type"]
            columns = ["settlement_date", "settlement_period", *optional]
            for key in frame.select(columns).iter_rows():
                keys.add(key)
                multiplicity[key] += 1
                if key[0] != destination:
                    misplaced.append((destination, *key))
        destination += timedelta(days=1)
    duplicates = {key: count for key, count in multiplicity.items() if count != 1}
    return keys, misplaced, duplicates


def rebuild(
    input_root: Path,
    output_root: Path,
    dataset: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    """Run a bounded candidate rebuild and return its evidence manifest."""
    input_root = input_root.resolve()
    output_root = output_root.resolve()
    if (
        input_root == output_root
        or input_root in output_root.parents
        or output_root in input_root.parents
    ):
        raise ValueError("output root must be separate from the read-only bronze input root")
    output_root.mkdir(parents=True, exist_ok=True)
    import_transformers()
    transformer = get_transformer("elexon", dataset, output_root)
    transformer.bronze_dir = PathBuilder(input_root).bronze_dir("elexon", dataset)
    transformer.silver_dir = PathBuilder(output_root).silver_dir("elexon", dataset)

    original_write_parquet = silver_base.write_parquet

    def guarded_write_parquet(
        frame: pl.DataFrame,
        path: Path,
        compression: str = "zstd",
    ) -> Path:
        resolved = path.resolve()
        if resolved != output_root and output_root not in resolved.parents:
            raise ValueError(f"refusing harness write outside output root: {resolved}")
        return original_write_parquet(frame, path, compression)

    accounting: list[dict[str, object]] = []
    silver_base.write_parquet = guarded_write_parquet
    try:
        destination = start
        while destination <= end:
            rows = transformer.run(
                destination,
                run_id=f"partition-trim-{dataset}-{destination}",
                reingest=True,
            )
            accounting.append(
                {
                    "destination": destination.isoformat(),
                    "rows": rows,
                    "fallback": transformer.last_start_time_fallback_count,
                    "trimmed": transformer.last_partition_trimmed_count,
                    "unsafe": transformer.last_partition_trim_unrecoverable_count,
                    "unresolved": transformer.last_partition_filter_unresolved_count,
                    "ownership": transformer.last_partition_trim_details,
                    "exclusions": transformer.last_source_exclusion_details,
                }
            )
            destination += timedelta(days=1)
    finally:
        silver_base.write_parquet = original_write_parquet

    expected = raw_expected_keys(input_root, dataset, start, end)
    actual, misplaced, duplicates = actual_keys(output_root, dataset, start, end)
    source_root = Path(__file__).resolve().parents[1]
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=source_root,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    manifest: dict[str, Any] = {
        "source_root": str(source_root),
        "commit": commit,
        "python": sys.version,
        "polars": pl.__version__,
        "dataset": dataset,
        "range": [start.isoformat(), end.isoformat()],
        "bronze_files": _files(PathBuilder(input_root).bronze_dir("elexon", dataset), "raw_*"),
        "silver_files": _files(PathBuilder(output_root).silver_dir("elexon", dataset), "*.parquet"),
        "empty_files": [
            path
            for path in _files(PathBuilder(output_root).silver_dir("elexon", dataset), "*.parquet")
            if pl.read_parquet(
                PathBuilder(output_root).silver_dir("elexon", dataset) / path
            ).is_empty()
        ],
        "accounting": accounting,
        "conservation": {
            "expected": len(expected),
            "actual": len(actual),
            "missing": sorted(map(str, expected - actual)),
            "extra": sorted(map(str, actual - expected)),
            "misplaced": list(map(str, misplaced)),
            "duplicates": {str(key): count for key, count in duplicates.items()},
        },
    }
    if expected != actual or misplaced or duplicates:
        raise RuntimeError(json.dumps(manifest["conservation"], indent=2))
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--dataset", choices=("mid", "fuelhh"), required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    manifest = rebuild(args.input_root, args.output_root, args.dataset, args.start, args.end)
    rendered = json.dumps(manifest, indent=2, default=str)
    if args.evidence is not None:
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.write_text(rendered)
    print(rendered)


if __name__ == "__main__":
    main()
