"""T-B8-1: the per-file branch releases each body's frame (ADR-034 P-9).

The ``VINTAGE_PER_BRONZE_FILE`` branch of ``BaseSilverTransformer.run`` kept
every written frame in a local list until the run ended, so one date's memory
grew with its body count. P-9 replaces the list with a running row and frame
count. Two things are pinned here:

1. **Byte-unchanged behaviour.** :func:`run_harness` drives the per-file branch
   over the ``elexon/system_prices`` JSON shape and the NESO
   ``daily_wind_availability`` CSV fixture, including unvouched and
   unusable-provenance bodies and the opt-in silver CSV sidecar, and records the
   silver filenames and column order, every written row (minus the per-run
   ``source_run_id`` and system_prices' wall-clock ``ingested_at``), the CSV
   rows, ``run()``'s return value and every ``last_*`` counter.
   The golden ``tests/fixtures/per_file_branch_golden.json`` was produced by
   this exact harness on a checkout of master ``d7cf513`` (before P-9), via
   ``json.dumps(run_harness(root), indent=2, sort_keys=True)``.
2. **Release.** A weak reference to each body's written frame is dead once the
   next body is being written. On ``d7cf513`` the list kept every frame alive,
   so that assertion fails there.
"""

from __future__ import annotations

import gc
import json
import weakref
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.neso_data_portal.daily_wind_availability import (
    DailyWindAvailabilityTransformer,
)

if TYPE_CHECKING:
    import pytest

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "per_file_branch_golden.json"
DWA_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "neso_data_portal"
    / "daily_wind_availability.csv"
)
SP_DATE = date(2024, 1, 15)
_CLOCK_COLUMNS = frozenset({"source_run_id", "ingested_at"})
"""Per-run values: the run id and system_prices' own wall-clock ``ingested_at``."""
DWA_DATE = date(2026, 8, 16)


def _sp_payload(day: date, sell_price: float) -> str:
    return json.dumps(
        {
            "data": [
                {
                    "settlementDate": day.isoformat(),
                    "settlementPeriod": period,
                    "systemSellPrice": sell_price + period,
                    "systemBuyPrice": 55.0,
                    "netImbalanceVolume": -120.5,
                }
                for period in (1, 2)
            ]
        }
    )


def _sp_partition(data_dir: Path) -> Path:
    partition = data_dir / "bronze" / "elexon" / "system_prices" / "2024" / "01" / "15"
    partition.mkdir(parents=True, exist_ok=True)
    return partition


def _seed_sp(data_dir: Path, *, vintages: bool = True, unvouched: bool = True) -> None:
    partition = _sp_partition(data_dir)
    if vintages:
        for name, hour, price in (("first", 8, 44.0), ("second", 12, 45.5), ("third", 16, 47.0)):
            (partition / f"raw_{name}.json").write_text(_sp_payload(SP_DATE, price))
            stamp = datetime(2024, 1, 15, hour, tzinfo=UTC).isoformat()
            (partition / f"raw_{name}.meta.json").write_text(json.dumps({"written_at": stamp}))
    if unvouched:
        (partition / "raw_orphan.json").write_text(_sp_payload(SP_DATE, 1.0))
        (partition / "raw_nostamp.json").write_text(_sp_payload(SP_DATE, 2.0))
        (partition / "raw_nostamp.meta.json").write_text(json.dumps({"other": "x"}))


def _dwa_sidecar(written_at: datetime, ckan_last_modified: str) -> dict[str, Any]:
    return {
        "source": "neso_data_portal",
        "dataset": "daily_wind_availability",
        "written_at": written_at.isoformat(),
        "request_params": {
            "package": "daily-wind-availability",
            "package_id": "3758a0ed-6c96-4e36-88d0-107f5020ddf3",
            "resource_id": "7aa508eb-36f5-4298-820f-2fa6745ae2e7",
            "resource_name": "Daily Wind Availability",
            "resource_filename": "windavailability.csv",
            "ckan_last_modified": ckan_last_modified,
            "ckan_format": "CSV",
            "body_sha256": "0" * 64,
        },
    }


def _seed_dwa(data_dir: Path, *, all_valid: bool = False) -> None:
    partition = data_dir / "bronze" / "neso_data_portal" / "daily_wind_availability"
    partition = partition / "2026" / "08" / "16"
    partition.mkdir(parents=True, exist_ok=True)
    fixture = DWA_FIXTURE.read_bytes()
    revised = fixture.replace(b"120.5", b"121.5")
    bodies = (
        ("20260816T090000Z_aaaa", fixture, datetime(2026, 8, 16, 9, tzinfo=UTC), "08:55"),
        ("20260816T183000Z_bbbb", revised, datetime(2026, 8, 16, 18, 30, tzinfo=UTC), "18:20"),
        ("20260816T200000Z_cccc", fixture, datetime(2026, 8, 16, 20, tzinfo=UTC), ""),
    )
    for name, body, written_at, modified in bodies:
        (partition / f"raw_{name}.csv").write_bytes(body)
        modified = modified or ("20:00" if all_valid else "")
        lm = f"2026-08-16T{modified}:00.000001" if modified else ""
        (partition / f"raw_{name}.meta.json").write_text(
            json.dumps(_dwa_sidecar(written_at, lm), indent=2)
        )


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _counters(transformer: BaseSilverTransformer, root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in sorted(dir(transformer)):
        if not name.startswith("last_"):
            continue
        value = getattr(transformer, name)
        if callable(value):
            continue
        if isinstance(value, frozenset):
            value = sorted(
                [Path(path).relative_to(root).as_posix(), str(reason)] for path, reason in value
            )
        elif isinstance(value, tuple):
            value = [str(item).replace(str(root), "<ROOT>") for item in value]
        out[name] = value
    return out


def _rows(frame: pl.DataFrame) -> dict[str, Any]:
    kept = [c for c in frame.columns if c not in _CLOCK_COLUMNS]
    return {
        "columns": frame.columns,
        "rows": [
            {key: _jsonable(val) for key, val in row.items()} for row in frame[kept].to_dicts()
        ],
    }


def _silver(root: Path) -> dict[str, Any]:
    silver = root / "silver"
    files: dict[str, Any] = {}
    for path in sorted(silver.rglob("*.parquet")):
        files[path.relative_to(root).as_posix()] = _rows(pl.read_parquet(path))
    for path in sorted(silver.rglob("*.csv")):
        files[path.relative_to(root).as_posix()] = _rows(pl.read_csv(path, infer_schema_length=0))
    return files


def _run_case(
    root: Path,
    cls: type[BaseSilverTransformer],
    target_date: date,
    *,
    write_csv: bool = False,
) -> dict[str, Any]:
    transformer = cls(root)
    transformer.write_silver_csv = write_csv
    rows = transformer.run(target_date, run_id="golden")
    return {
        "returned": rows,
        "counters": _counters(transformer, root),
        "silver": _silver(root),
    }


def run_harness(base: Path) -> dict[str, Any]:
    """Run every per-file case under ``base`` and return the comparable record.

    Args:
        base: An empty scratch directory; one data root is created per case.

    Returns:
        Case name -> ``{returned, counters, silver}``.
    """
    cases: dict[str, Any] = {}

    root = base / "sp_mixed"
    _seed_sp(root)
    cases["sp_mixed"] = _run_case(root, SystemPriceTransformer, SP_DATE)

    root = base / "sp_csv"
    _seed_sp(root, unvouched=False)
    cases["sp_csv"] = _run_case(root, SystemPriceTransformer, SP_DATE, write_csv=True)

    root = base / "sp_all_unvouched"
    _seed_sp(root, vintages=False)
    cases["sp_all_unvouched"] = _run_case(root, SystemPriceTransformer, SP_DATE)

    root = base / "sp_empty"
    cases["sp_empty"] = _run_case(root, SystemPriceTransformer, SP_DATE)

    root = base / "dwa"
    _seed_dwa(root)
    cases["dwa"] = _run_case(root, DailyWindAvailabilityTransformer, DWA_DATE)
    return cases


def test_per_file_branch_matches_the_d7cf513_golden(tmp_path: Path) -> None:
    """Detects any change P-9 makes to per-file filenames, rows, CSV or counters.

    The golden was captured from this harness on master ``d7cf513``.
    """
    actual = json.loads(json.dumps(run_harness(tmp_path), sort_keys=True))
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert actual == expected


def test_each_bodys_frame_is_released_before_the_next_is_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects the per-file branch retaining written frames across bodies.

    Driven through ``daily_wind_availability`` (``VINTAGE_PER_BRONZE_FILE``, no
    ``PARTITION_DATE_COLUMN``); ``system_prices`` declares a partition column, so
    it runs the partition-owned path P-9 leaves untouched. On ``d7cf513``
    ``frames.append(clean_df)`` kept every body's frame alive until ``run()``
    returned, so these weak references stayed live (``[0, 1, 2]``).
    """
    _seed_dwa(tmp_path, all_valid=True)
    refs: list[weakref.ref[pl.DataFrame]] = []
    alive_at_next_write: list[int] = []
    original = BaseSilverTransformer._write_silver

    def _spy(
        self: BaseSilverTransformer, df: pl.DataFrame, target_date: date, available_at: datetime
    ) -> None:
        gc.collect()
        alive_at_next_write.append(sum(ref() is not None for ref in refs))
        refs.append(weakref.ref(df))
        original(self, df, target_date, available_at)

    monkeypatch.setattr(BaseSilverTransformer, "_write_silver", _spy)
    transformer = DailyWindAvailabilityTransformer(tmp_path)
    assert transformer.PARTITION_DATE_COLUMN is None
    assert transformer.run(DWA_DATE, run_id="release") == 18
    assert alive_at_next_write == [0, 0, 0]
