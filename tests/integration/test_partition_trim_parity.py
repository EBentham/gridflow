"""Bounded fixture parity and raw-derived conservation checks."""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from gridflow.storage.paths import PathBuilder
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from collections.abc import Callable


_SCRIPT = Path(__file__).parents[2] / "scripts" / "verify_partition_trim.py"
_SPEC = importlib.util.spec_from_file_location("verify_partition_trim", _SCRIPT)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
rebuild: Callable[..., dict[str, object]] = _MODULE.rebuild


DAY = date(2024, 1, 15)
STAMP = "2024-01-16T12:00:00+00:00"


class _Clock(datetime):
    @classmethod
    def now(cls, tz: object = None) -> datetime:
        return datetime(2026, 9, 7, 12, tzinfo=UTC)


def _write_mid_partition(root: Path, source_date: date, rows: list[dict[str, object]]) -> None:
    partition = PathBuilder(root).bronze_date_dir("elexon", "mid", source_date)
    partition.mkdir(parents=True, exist_ok=True)
    (partition / "raw_fixture.json").write_text(json.dumps({"data": rows}))
    (partition / "raw_fixture.meta.json").write_text(json.dumps({"written_at": STAMP}))


def _write_fuelhh_partition(
    root: Path,
    source_date: date,
    window_start: datetime,
    window_end: datetime,
    rows: list[dict[str, object]],
) -> None:
    partition = PathBuilder(root).bronze_date_dir("elexon", "fuelhh", source_date)
    partition.mkdir(parents=True, exist_ok=True)
    body = partition / "raw_fixture.json"
    body.write_text(json.dumps({"data": rows}))
    body.with_suffix(".meta.json").write_text(
        json.dumps(
            {
                "source": "elexon",
                "dataset": "fuelhh",
                "data_date": source_date.isoformat(),
                "request_params": {
                    "publishDateTimeFrom": window_start.isoformat().replace("+00:00", "Z"),
                    "publishDateTimeTo": window_end.isoformat().replace("+00:00", "Z"),
                },
                "page": 1,
                "total_pages": 1,
                "written_at": (window_end + timedelta(hours=1)).isoformat(),
            }
        )
    )


def _fuel_row(day: date, period: int, fuel_type: str, generation: float) -> dict[str, object]:
    start = settlement_period_to_utc(day, period)
    published = start + timedelta(minutes=30)
    return {
        "settlementDate": day.isoformat(),
        "settlementPeriod": period,
        "startTime": start.isoformat().replace("+00:00", "Z"),
        "publishTime": published.isoformat().replace("+00:00", "Z"),
        "fuelType": fuel_type,
        "generation": generation,
    }


def test_p_t01_p_t07_fixture_rebuild_is_byte_stable_and_conserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("gridflow.silver.base.datetime", _Clock)
    monkeypatch.setattr("gridflow.silver.elexon.mid.datetime", _Clock)
    clock_modules = (
        _MODULE.silver_base,
        _MODULE.fuelhh_module,
        _MODULE.mid_module,
        _MODULE.system_prices_module,
    )
    incoming_clocks = tuple(module.datetime for module in clock_modules)
    input_root = tmp_path / "input"
    predecessor = DAY - timedelta(days=1)
    _write_mid_partition(
        input_root,
        predecessor,
        [
            {
                "settlementDate": DAY.isoformat(),
                "settlementPeriod": 1,
                "dataProvider": "A",
                "price": 10.0,
                "volume": 1.0,
            }
        ],
    )
    _write_mid_partition(
        input_root,
        DAY,
        [
            {
                "settlementDate": DAY.isoformat(),
                "settlementPeriod": period,
                "dataProvider": "A",
                "price": float(period),
                "volume": 1.0,
            }
            for period in range(1, 49)
        ],
    )

    first = rebuild(input_root, tmp_path / "first", "mid", DAY, DAY)
    second = rebuild(input_root, tmp_path / "second", "mid", DAY, DAY)
    assert tuple(module.datetime for module in clock_modules) == incoming_clocks
    first_hashes = {path: details["sha256"] for path, details in first["silver_files"].items()}
    second_hashes = {path: details["sha256"] for path, details in second["silver_files"].items()}
    assert first_hashes == second_hashes
    assert first["conservation"] == {
        "expected": 48,
        "actual": 48,
        "missing": [],
        "extra": [],
        "misplaced": [],
        "duplicates": {},
    }


def test_p_t19_harness_rejects_output_inside_input(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="separate"):
        rebuild(tmp_path, tmp_path / "output", "mid", DAY, DAY)


def test_l_t06_rebuild_restores_exact_clock_bindings_on_early_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    modules = (
        _MODULE.silver_base,
        _MODULE.fuelhh_module,
        _MODULE.mid_module,
        _MODULE.system_prices_module,
    )
    incoming = tuple(type(f"CallerClock{index}", (datetime,), {}) for index in range(4))
    for module, clock in zip(modules, incoming, strict=True):
        monkeypatch.setattr(module, "datetime", clock)
    monkeypatch.setattr(
        _MODULE,
        "_run_transformer",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("early clock probe")),
    )

    with pytest.raises(RuntimeError, match="early clock probe"):
        rebuild(tmp_path / "input", tmp_path / "output", "mid", DAY, DAY)
    assert tuple(module.datetime for module in modules) == incoming


def test_l_t06_rebuild_restores_clocks_after_later_evidence_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root = tmp_path / "input"
    _write_mid_partition(
        input_root,
        DAY,
        [
            {
                "settlementDate": DAY.isoformat(),
                "settlementPeriod": 1,
                "dataProvider": "A",
                "price": 10.0,
                "volume": 1.0,
            }
        ],
    )
    modules = (
        _MODULE.silver_base,
        _MODULE.fuelhh_module,
        _MODULE.mid_module,
        _MODULE.system_prices_module,
    )
    incoming = tuple(module.datetime for module in modules)
    monkeypatch.setattr(
        _MODULE,
        "_period_evidence",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("late clock probe")),
    )

    with pytest.raises(RuntimeError, match="late clock probe"):
        rebuild(input_root, tmp_path / "output", "mid", DAY, DAY)
    assert tuple(module.datetime for module in modules) == incoming


def test_p_t17b_portable_real_window_successor_conservation_and_controls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real-shaped GMT boundary survives only through FUELHH's successor input."""
    monkeypatch.setattr("gridflow.silver.base.datetime", _Clock)
    monkeypatch.setattr("gridflow.silver.elexon.fuelhh.datetime", _Clock)
    day = date(2025, 11, 15)
    successor = day + timedelta(days=1)
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    successor_start = datetime.combine(successor, datetime.min.time(), tzinfo=UTC)
    input_root = tmp_path / "fuel-input"
    own_rows = [_fuel_row(day, period, "CCGT", float(period)) for period in range(1, 49)]
    successor_rows = [
        _fuel_row(day, 48, "CCGT", 48.0),
        _fuel_row(day, 48, "WIND", 99.0),
    ]
    _write_fuelhh_partition(input_root, day, day_start, successor_start, own_rows)
    _write_fuelhh_partition(
        input_root,
        successor,
        successor_start,
        successor_start + timedelta(days=1),
        successor_rows,
    )

    evidence = rebuild(
        input_root,
        tmp_path / "fuel-output",
        "fuelhh",
        day,
        day,
        run_controls=True,
    )

    assert evidence["conservation"]["missing"] == []
    assert evidence["conservation"]["extra"] == []
    assert evidence["conservation"]["misplaced"] == []
    assert evidence["periods"][day.isoformat()]["complete"] is True
    assert evidence["availability"]["available_equals_published"] == 49
    controls = evidence["discriminating_controls"]
    assert controls["old_covering_set"]["missing"]
    assert controls["old_covering_set"]["periods"][day.isoformat()]["complete"] is False
    assert controls["old_covering_set"]["common_stamp_changes"] == {}
    assert controls["old_recoverability_only"]["predicate_probe_unsafe"] == 1

    repeated = rebuild(input_root, tmp_path / "fuel-repeat", "fuelhh", day, day)
    assert repeated["silver_files"] == evidence["silver_files"]

    order_maps: list[dict[str, bytes]] = []
    for label, order in (
        ("forward", (day, successor)),
        ("reverse", (successor, day)),
    ):
        order_root = tmp_path / f"fuel-{label}"
        transformer = _MODULE.FuelHHTransformer(order_root)
        transformer.bronze_dir = PathBuilder(input_root).bronze_dir("elexon", "fuelhh")
        transformer.silver_dir = PathBuilder(order_root).silver_dir("elexon", "fuelhh")
        for destination in order:
            transformer.run(destination, run_id=f"fixed-{destination}", reingest=True)
        order_maps.append(
            {
                path.relative_to(order_root).as_posix(): path.read_bytes()
                for path in order_root.rglob("*.parquet")
            }
        )
    assert order_maps[0] == order_maps[1]

    bst_day = date(2025, 6, 10)
    bst_start = datetime.combine(bst_day, datetime.min.time(), tzinfo=UTC)
    bst_input = tmp_path / "fuel-bst-input"
    _write_fuelhh_partition(
        bst_input,
        bst_day - timedelta(days=1),
        bst_start - timedelta(days=1),
        bst_start,
        [_fuel_row(bst_day, 1, "CCGT", 1.0)],
    )
    _write_fuelhh_partition(
        bst_input,
        bst_day,
        bst_start,
        bst_start + timedelta(days=1),
        [_fuel_row(bst_day, period, "CCGT", float(period)) for period in range(2, 49)],
    )
    bst = rebuild(bst_input, tmp_path / "fuel-bst-output", "fuelhh", bst_day, bst_day)
    assert bst["periods"][bst_day.isoformat()]["complete"] is True
    assert bst["conservation"]["missing"] == []
