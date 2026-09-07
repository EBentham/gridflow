"""Independent, bounded partition-trim rebuild and conservation control.

Bronze is read-only. Every write is contained below ``--output-root``. The raw
oracle deliberately scans a ±2-day halo and does not import the transformer's
covering-set declaration, so candidate and oracle cannot repeat the same bug.

Full-history FUELHH handoff note: finalized days must be complete except the
unfinalized trailing day and 2021-09-01's mirror leading-edge gap. The latter is
one period short because its BST SP1 starts at 2021-08-31T23:00Z and retained
bronze partition 2021-08-31 does not exist.
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
from gridflow.silver.elexon import fuelhh as fuelhh_module
from gridflow.silver.elexon import mid as mid_module
from gridflow.silver.elexon import system_prices as system_prices_module
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.registry import get_transformer
from gridflow.storage.paths import PathBuilder
from gridflow.utils.time import (
    settlement_period_to_utc,
    settlement_periods_in_day,
    utc_to_settlement_period,
)

Identity = tuple[object, ...]
_ORACLE_HALO_DAYS = 2


class _FixedClock(datetime):
    @classmethod
    def now(cls, tz: object = None) -> datetime:
        return datetime(2026, 9, 7, 12, tzinfo=UTC)


class _OldFuelHHCoveringSet(FuelHHTransformer):
    """N-3 control equivalent to b2dbf11 for FUELHH."""

    PARTITION_SOURCE_OFFSETS = (-1, 0)

    def _is_partition_owner_recoverable(self, source_date: date, owner_date: date) -> bool:
        return source_date in (owner_date - timedelta(days=1), owner_date)


class _OldFuelHHRecoverability(FuelHHTransformer):
    """N-3 control restoring only the pre-fix accounting predicate."""

    def _is_partition_owner_recoverable(self, source_date: date, owner_date: date) -> bool:
        return source_date in (owner_date - timedelta(days=1), owner_date)


class _OldSystemPriceCoveringSet(SystemPriceTransformer):
    """Pre-narrowing system_prices control; D-1 bodies must change no output."""

    PARTITION_SOURCE_OFFSETS = (-1, 0)


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _files(root: Path, pattern: str) -> dict[str, dict[str, int | str]]:
    if not root.exists():
        return {}
    return {
        path.relative_to(root).as_posix(): {"sha256": _hash(path), "size": path.stat().st_size}
        for path in sorted(root.rglob(pattern))
        if path.is_file()
    }


def _partition_files(input_root: Path, dataset: str, source_date: date) -> list[Path]:
    partition = PathBuilder(input_root).bronze_date_dir("elexon", dataset, source_date)
    if not partition.exists():
        return []
    return [
        path
        for path in sorted(partition.glob("raw_*.json"))
        if not path.name.endswith(".meta.json")
    ]


def _raw_records(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text())
    rows = payload.get("data", []) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _first(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return None


def _owner_period(dataset: str, row: dict[str, Any]) -> tuple[date, int]:
    if dataset == "fuelhh":
        raw_start = _first(row, "startTime", "startTimeOfHalfHrPeriod", "start_time")
        if raw_start is not None:
            timestamp = datetime.fromisoformat(str(raw_start).replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                raise ValueError("raw FUELHH start time must be timezone-aware")
            return utc_to_settlement_period(timestamp.astimezone(UTC))
    owner = date.fromisoformat(str(_first(row, "settlementDate", "settlement_date")))
    period = int(_first(row, "settlementPeriod", "settlement_period"))
    return owner, period


def _identity(dataset: str, row: dict[str, Any]) -> Identity:
    owner, period = _owner_period(dataset, row)
    if dataset == "fuelhh":
        return owner, period, _first(row, "fuelType", "fuel_type")
    if dataset == "mid":
        return (
            owner,
            period,
            _first(row, "dataProvider", "dataProviderId", "data_provider_id"),
            _first(row, "settlementRunType", "runType", "run_type"),
        )
    if dataset == "system_prices":
        return owner, period, _first(row, "settlementRunType", "run_type")
    raise ValueError(f"unsupported dataset: {dataset}")


def _scan_dates(start: date, end: date) -> list[date]:
    first = start - timedelta(days=_ORACLE_HALO_DAYS)
    last = end + timedelta(days=_ORACLE_HALO_DAYS)
    return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]


def raw_expected_keys(input_root: Path, dataset: str, start: date, end: date) -> set[Identity]:
    """Derive expected identities independently from a specification-owned ±2 halo."""
    expected: set[Identity] = set()
    for source_date in _scan_dates(start, end):
        for body in _partition_files(input_root, dataset, source_date):
            for row in _raw_records(body):
                key = _identity(dataset, row)
                if start <= key[0] <= end:
                    expected.add(key)
    return expected


def _actual_rows(output_root: Path, dataset: str) -> list[tuple[date, Identity, dict[str, Any]]]:
    rows: list[tuple[date, Identity, dict[str, Any]]] = []
    root = PathBuilder(output_root).silver_dir("elexon", dataset)
    for path in sorted(root.rglob("*.parquet")) if root.exists() else []:
        stem_date = path.name[len(dataset) + 1 : len(dataset) + 9]
        destination = date.fromisoformat(f"{stem_date[:4]}-{stem_date[4:6]}-{stem_date[6:8]}")
        for row in pl.read_parquet(path).iter_rows(named=True):
            rows.append((destination, _identity(dataset, row), row))
    return rows


def actual_keys(
    output_root: Path, dataset: str, start: date, end: date
) -> tuple[set[Identity], list[Identity], dict[Identity, int]]:
    keys: set[Identity] = set()
    multiplicity: Counter[Identity] = Counter()
    misplaced: list[Identity] = []
    for destination, key, _row in _actual_rows(output_root, dataset):
        if not start <= key[0] <= end:
            continue
        keys.add(key)
        multiplicity[key] += 1
        if key[0] != destination:
            misplaced.append((destination, *key))
    duplicates = (
        {}
        if dataset == "system_prices"
        else {key: count for key, count in multiplicity.items() if count != 1}
    )
    return keys, misplaced, duplicates


def _input_evidence(input_root: Path, dataset: str, start: date, end: date) -> dict[str, Any]:
    files: dict[str, dict[str, int | str]] = {}
    missing: list[str] = []
    proof_dependencies: list[str] = []
    for source_date in _scan_dates(start, end):
        bodies = _partition_files(input_root, dataset, source_date)
        if not bodies:
            missing.append(source_date.isoformat())
        for body in bodies:
            for candidate in (body, body.with_suffix(".meta.json")):
                if candidate.exists():
                    files[candidate.relative_to(input_root).as_posix()] = {
                        "sha256": _hash(candidate),
                        "size": candidate.stat().st_size,
                    }
                    if candidate.name.endswith(".meta.json") and dataset == "fuelhh":
                        proof_dependencies.append(candidate.relative_to(input_root).as_posix())
    return {
        "oracle_halo_days": _ORACLE_HALO_DAYS,
        "files": files,
        "missing_partitions": sorted(set(missing)),
        "proof_sidecars": sorted(proof_dependencies),
    }


def _sidecar_stamp(body: Path) -> datetime | None:
    sidecar = body.with_suffix(".meta.json")
    if not sidecar.exists():
        return None
    payload = json.loads(sidecar.read_text())
    if not isinstance(payload, dict):
        return None
    for name in ("available_at", "written_at", "response_received_at", "fetched_at"):
        raw = payload.get(name)
        if raw:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError(f"sidecar stamp must be timezone-aware: {sidecar}")
            return parsed.astimezone(UTC)
    return None


def _system_capture_multisets(
    input_root: Path, output_root: Path, start: date, end: date
) -> tuple[
    Counter[tuple[Identity, datetime, str]],
    Counter[tuple[Identity, datetime, str]],
]:
    expected: Counter[tuple[Identity, datetime, str]] = Counter()
    policy_cutover = datetime(2026, 7, 31, tzinfo=UTC)
    policy_name = "elexon-system_prices/vp-2026-09"
    destination = start
    while destination <= end:
        for body in _partition_files(input_root, "system_prices", destination):
            stamp = _sidecar_stamp(body)
            if stamp is None:
                continue
            for row in _raw_records(body):
                key = _identity("system_prices", row)
                if key[0] == destination:
                    event_time = settlement_period_to_utc(key[0], int(key[1]))
                    reconstructed = event_time + timedelta(minutes=90)
                    use_policy = event_time < policy_cutover and reconstructed < stamp
                    effective_stamp = reconstructed if use_policy else stamp
                    label = policy_name if use_policy else "ingest-clock"
                    expected[(key, effective_stamp, label)] += 1
        destination += timedelta(days=1)
    actual: Counter[tuple[Identity, datetime, str]] = Counter()
    for _destination, key, row in _actual_rows(output_root, "system_prices"):
        stamp = row.get("available_at")
        label = row.get("vintage_policy")
        if isinstance(stamp, datetime) and isinstance(label, str) and start <= key[0] <= end:
            actual[(key, stamp.astimezone(UTC), label)] += 1
    return expected, actual


def _period_evidence(
    expected: set[Identity], actual: set[Identity], start: date, end: date
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    destination = start
    while destination <= end:
        required = set(range(1, settlement_periods_in_day(destination) + 1))
        raw_periods = {int(key[1]) for key in expected if key[0] == destination}
        actual_periods = {int(key[1]) for key in actual if key[0] == destination}
        evidence[destination.isoformat()] = {
            "required": sorted(required),
            "raw": sorted(raw_periods),
            "actual": sorted(actual_periods),
            "complete": raw_periods == required and actual_periods == required,
        }
        destination += timedelta(days=1)
    return evidence


def _run_transformer(
    input_root: Path,
    output_root: Path,
    dataset: str,
    start: date,
    end: date,
    transformer_type: type[silver_base.BaseSilverTransformer] | None = None,
) -> tuple[list[dict[str, object]], silver_base.BaseSilverTransformer]:
    import_transformers()
    transformer = (
        transformer_type(output_root)
        if transformer_type is not None
        else get_transformer("elexon", dataset, output_root)
    )
    transformer.bronze_dir = PathBuilder(input_root).bronze_dir("elexon", dataset)
    transformer.silver_dir = PathBuilder(output_root).silver_dir("elexon", dataset)
    accounting: list[dict[str, object]] = []
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
                "routine_covering_set_trim": transformer.last_partition_trimmed_count,
                "unsafe": transformer.last_partition_trim_unrecoverable_count,
                "unresolved": transformer.last_partition_filter_unresolved_count,
                "ownership": transformer.last_partition_trim_details,
                "exclusions": transformer.last_source_exclusion_details,
            }
        )
        destination += timedelta(days=1)
    return accounting, transformer


def _assert_containment(input_root: Path, output_root: Path) -> None:
    input_root = input_root.resolve()
    output_root = output_root.resolve()
    if (
        input_root == output_root
        or input_root in output_root.parents
        or output_root in input_root.parents
    ):
        raise ValueError("output root must be separate from the read-only bronze input root")
    if output_root.name.lower() == "data" or "gridflow-data" in str(output_root).lower():
        raise ValueError("output root must not be named data or contain gridflow-data")


def _availability_map(output_root: Path, dataset: str) -> dict[Identity, tuple[object, object]]:
    return {
        key: (row.get("published_at"), row.get("available_at"))
        for _destination, key, row in _actual_rows(output_root, dataset)
    }


def rebuild(
    input_root: Path,
    output_root: Path,
    dataset: str,
    start: date,
    end: date,
    *,
    run_controls: bool = False,
) -> dict[str, Any]:
    """Run persisted-output conservation checks and return complete evidence."""
    input_root = input_root.resolve()
    output_root = output_root.resolve()
    _assert_containment(input_root, output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    silver_base.datetime = _FixedClock
    fuelhh_module.datetime = _FixedClock
    mid_module.datetime = _FixedClock
    system_prices_module.datetime = _FixedClock

    original_write_parquet = silver_base.write_parquet

    def guarded_write_parquet(frame: pl.DataFrame, path: Path, compression: str = "zstd") -> Path:
        resolved = path.resolve()
        if resolved != output_root and output_root not in resolved.parents:
            raise ValueError(f"refusing harness write outside output root: {resolved}")
        return original_write_parquet(frame, path, compression)

    silver_base.write_parquet = guarded_write_parquet
    try:
        accounting, _transformer = _run_transformer(input_root, output_root, dataset, start, end)
    finally:
        silver_base.write_parquet = original_write_parquet

    expected = raw_expected_keys(input_root, dataset, start, end)
    actual, misplaced, duplicates = actual_keys(output_root, dataset, start, end)
    conservation = {
        "expected": len(expected),
        "actual": len(actual),
        "missing": sorted(map(str, expected - actual)),
        "extra": sorted(map(str, actual - expected)),
        "misplaced": list(map(str, misplaced)),
        "duplicates": {str(key): count for key, count in duplicates.items()},
    }
    periods = _period_evidence(expected, actual, start, end)
    source_root = Path(__file__).resolve().parents[1]
    manifest: dict[str, Any] = {
        "source_root": str(source_root),
        "commit": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=source_root,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip(),
        "python": sys.version,
        "polars": pl.__version__,
        "dataset": dataset,
        "requested_destinations": [
            (start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)
        ],
        "input": _input_evidence(input_root, dataset, start, end),
        "silver_files": _files(PathBuilder(output_root).silver_dir("elexon", dataset), "*.parquet"),
        "accounting": accounting,
        "conservation": conservation,
        "periods": periods,
        "outside_range_owners": sorted(
            {
                str(_identity(dataset, row)[0])
                for source_date in _scan_dates(start, end)
                for body in _partition_files(input_root, dataset, source_date)
                for row in _raw_records(body)
                if not start <= _identity(dataset, row)[0] <= end
            }
        ),
    }

    if dataset == "fuelhh":
        availability = _availability_map(output_root, dataset)
        candidate_availability = availability.copy()
        leakage = {
            "rows": len(availability),
            "published_nulls": sum(published is None for published, _ in availability.values()),
            "available_nulls": sum(available is None for _, available in availability.values()),
            "available_equals_published": sum(a == p for p, a in availability.values()),
            "vintage_policy_absent": all(
                "vintage_policy" not in row
                for _destination, _key, row in _actual_rows(output_root, dataset)
            ),
        }
        manifest["availability"] = leakage
        final_periods = {
            destination: settlement_periods_in_day(destination)
            for destination in (
                start + timedelta(days=offset) for offset in range((end - start).days + 1)
            )
        }
        successor_supplied = {
            key
            for destination, final_period in final_periods.items()
            for body in _partition_files(input_root, dataset, destination + timedelta(days=1))
            for row in _raw_records(body)
            if (key := _identity(dataset, row))[0] == destination and key[1] == final_period
        }
        manifest["boundary_composition"] = {
            "successor_supplied_final_keys": len(successor_supplied),
            "all_persisted": successor_supplied <= actual,
        }
        if (
            leakage["published_nulls"]
            or leakage["available_nulls"]
            or leakage["available_equals_published"] != leakage["rows"]
            or not leakage["vintage_policy_absent"]
        ):
            raise RuntimeError(json.dumps({"availability_hard_fail": leakage}, indent=2))

    if run_controls and dataset == "fuelhh":
        controls: dict[str, Any] = {}
        for label, transformer_type in (
            ("old_covering_set", _OldFuelHHCoveringSet),
            ("old_recoverability_only", _OldFuelHHRecoverability),
        ):
            control_root = output_root / "controls" / label
            original_write_parquet = silver_base.write_parquet

            def control_write(
                frame: pl.DataFrame,
                path: Path,
                compression: str = "zstd",
                *,
                _control_root: Path = control_root,
                _writer: Any = original_write_parquet,
            ) -> Path:
                resolved = path.resolve()
                if _control_root.resolve() not in resolved.parents:
                    raise ValueError(f"refusing control write outside control root: {resolved}")
                return _writer(frame, path, compression)

            silver_base.write_parquet = control_write
            try:
                control_accounting, _ = _run_transformer(
                    input_root, control_root, dataset, start, end, transformer_type
                )
            finally:
                silver_base.write_parquet = original_write_parquet
            control_actual, control_misplaced, control_duplicates = actual_keys(
                control_root, dataset, start, end
            )
            controls[label] = {
                "missing": sorted(map(str, expected - control_actual)),
                "extra": sorted(map(str, control_actual - expected)),
                "misplaced": list(map(str, control_misplaced)),
                "duplicates": {str(k): v for k, v in control_duplicates.items()},
                "periods": _period_evidence(expected, control_actual, start, end),
                "accounting": control_accounting,
            }
            if label == "old_recoverability_only":
                probe = transformer_type(control_root)
                probe._record_partition_ownership(
                    pl.DataFrame({"settlement_date": [start]}, schema={"settlement_date": pl.Date}),
                    start + timedelta(days=1),
                    start + timedelta(days=1),
                    trim=False,
                )
                controls[label]["predicate_probe_unsafe"] = (
                    probe.last_partition_trim_unrecoverable_count
                )
            if label == "old_covering_set":
                baseline = _availability_map(control_root, dataset)
                changed = {
                    str(key): {
                        "baseline": baseline[key],
                        "candidate": candidate_availability[key],
                    }
                    for key in baseline.keys() & candidate_availability.keys()
                    if baseline[key] != candidate_availability[key]
                }
                controls[label]["common_stamp_changes"] = changed
                if changed:
                    raise RuntimeError(
                        json.dumps({"availability_hard_fail": changed}, default=str, indent=2)
                    )
        manifest["discriminating_controls"] = controls

    if dataset == "system_prices":
        expected_captures, actual_captures = _system_capture_multisets(
            input_root, output_root, start, end
        )
        capture_evidence = {
            "expected_rows": sum(expected_captures.values()),
            "actual_rows": sum(actual_captures.values()),
            "missing": sorted(map(str, (expected_captures - actual_captures).elements())),
            "extra": sorted(map(str, (actual_captures - expected_captures).elements())),
        }
        manifest["capture_multiset"] = capture_evidence
        if run_controls:
            baseline_root = output_root / "controls" / "old_system_prices_covering_set"
            original_write_parquet = silver_base.write_parquet

            def baseline_write(frame: pl.DataFrame, path: Path, compression: str = "zstd") -> Path:
                resolved = path.resolve()
                if baseline_root.resolve() not in resolved.parents:
                    raise ValueError(f"refusing control write outside control root: {resolved}")
                return original_write_parquet(frame, path, compression)

            silver_base.write_parquet = baseline_write
            try:
                _run_transformer(
                    input_root,
                    baseline_root,
                    dataset,
                    start,
                    end,
                    _OldSystemPriceCoveringSet,
                )
            finally:
                silver_base.write_parquet = original_write_parquet
            candidate_files = manifest["silver_files"]
            baseline_files = _files(
                PathBuilder(baseline_root).silver_dir("elexon", dataset), "*.parquet"
            )
            manifest["old_covering_set_byte_parity"] = candidate_files == baseline_files
            if candidate_files != baseline_files:
                raise RuntimeError("system_prices exact-D narrowing changed filenames or bytes")
        if expected_captures != actual_captures:
            raise RuntimeError(json.dumps({"capture_multiset": capture_evidence}, indent=2))

    incomplete = [day for day, detail in periods.items() if not detail["complete"]]
    if expected != actual or misplaced or duplicates or incomplete:
        raise RuntimeError(
            json.dumps({"conservation": conservation, "incomplete_periods": incomplete}, indent=2)
        )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--dataset", choices=("mid", "fuelhh", "system_prices"), required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--skip-controls", action="store_true")
    args = parser.parse_args()
    manifest = rebuild(
        args.input_root,
        args.output_root,
        args.dataset,
        args.start,
        args.end,
        run_controls=not args.skip_controls,
    )
    rendered = json.dumps(manifest, indent=2, default=str)
    if args.evidence is not None:
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.write_text(rendered)
    print(rendered)


if __name__ == "__main__":
    main()
