"""Bounded fixture parity and raw-derived conservation checks."""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from gridflow.storage.paths import PathBuilder

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


def test_p_t01_p_t07_p_t17_fixture_rebuild_is_byte_stable_and_conserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("gridflow.silver.base.datetime", _Clock)
    monkeypatch.setattr("gridflow.silver.elexon.mid.datetime", _Clock)
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
                "settlementPeriod": 1,
                "dataProvider": "A",
                "price": 20.0,
                "volume": 1.0,
            },
            {
                "settlementDate": DAY.isoformat(),
                "settlementPeriod": 2,
                "dataProvider": "B",
                "price": 30.0,
                "volume": 1.0,
            },
        ],
    )

    first = rebuild(input_root, tmp_path / "first", "mid", DAY, DAY)
    second = rebuild(input_root, tmp_path / "second", "mid", DAY, DAY)
    first_hashes = {path: details["sha256"] for path, details in first["silver_files"].items()}
    second_hashes = {path: details["sha256"] for path, details in second["silver_files"].items()}
    assert first_hashes == second_hashes
    assert first["conservation"] == {
        "expected": 2,
        "actual": 2,
        "missing": [],
        "extra": [],
        "misplaced": [],
        "duplicates": {},
    }


def test_p_t19_harness_rejects_output_inside_input(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="separate"):
        rebuild(tmp_path, tmp_path / "output", "mid", DAY, DAY)
