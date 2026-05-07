"""Integration tests for append-only silver writes.

Two consecutive runs of an `APPEND_ONLY` transformer must produce two distinct
Parquet files in the same partition directory, each carrying a run-suffixed
filename derived from `available_at`. The union of rows across the files must
contain revisions that the pre-F7 `keep="last"` deduplication would have
discarded.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from gridflow.silver.elexon.remit import REMITTransformer
from gridflow.storage.parquet import read_parquet

TARGET_DATE = date(2024, 2, 1)


def _write_remit_bronze(data_dir: Path) -> None:
    bronze = (
        data_dir
        / "bronze"
        / "elexon"
        / "remit"
        / str(TARGET_DATE.year)
        / f"{TARGET_DATE.month:02d}"
        / f"{TARGET_DATE.day:02d}"
    )
    bronze.mkdir(parents=True, exist_ok=True)
    payload = {
        "data": [
            {
                "mrid": "MSG-A",
                "revisionNumber": 1,
                "publishTime": "2024-02-01T08:00:00Z",
                "messageType": "Production unavailability",
                "messageHeading": "Initial outage notice",
                "eventType": "Planned",
                "unavailabilityType": "Planned",
                "participantId": "PART-A",
                "registrationCode": "REG-A",
                "assetId": "ASSET-1",
                "assetType": "Generation",
                "affectedUnit": "UNIT-1",
                "affectedUnitEIC": "EIC-1",
                "biddingZone": "GB",
                "fuelType": "Gas",
                "normalCapacity": 500.0,
                "availableCapacity": 0.0,
                "unavailableCapacity": 500.0,
                "eventStatus": "Active",
                "eventStartTime": "2024-02-02T00:00:00Z",
                "eventEndTime": "2024-02-05T00:00:00Z",
                "cause": "Maintenance",
                "relatedInformation": "",
            },
            {
                "mrid": "MSG-A",
                "revisionNumber": 2,
                "publishTime": "2024-02-01T12:00:00Z",
                "messageType": "Production unavailability",
                "messageHeading": "Revised outage notice",
                "eventType": "Planned",
                "unavailabilityType": "Planned",
                "participantId": "PART-A",
                "registrationCode": "REG-A",
                "assetId": "ASSET-1",
                "assetType": "Generation",
                "affectedUnit": "UNIT-1",
                "affectedUnitEIC": "EIC-1",
                "biddingZone": "GB",
                "fuelType": "Gas",
                "normalCapacity": 500.0,
                "availableCapacity": 0.0,
                "unavailableCapacity": 500.0,
                "eventStatus": "Active",
                "eventStartTime": "2024-02-02T00:00:00Z",
                "eventEndTime": "2024-02-06T00:00:00Z",
                "cause": "Maintenance extended",
                "relatedInformation": "",
            },
        ]
    }
    (bronze / "raw_remit.json").write_text(json.dumps(payload))


def _list_parquet_files(silver_dir: Path) -> list[Path]:
    return sorted(silver_dir.rglob("*.parquet"))


def test_remit_append_only_produces_two_files_for_two_runs(
    tmp_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two live runs with distinct available_at values produce two files."""
    _write_remit_bronze(tmp_data_dir)
    silver_dir = tmp_data_dir / "silver" / "elexon" / "remit"

    fixed_times = iter(
        [
            datetime(2024, 2, 1, 8, 30, tzinfo=UTC),
            datetime(2024, 2, 1, 14, 30, tzinfo=UTC),
        ]
    )

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return next(fixed_times)

    monkeypatch.setattr("gridflow.silver.base.datetime", _FakeDateTime)

    REMITTransformer(tmp_data_dir).run(TARGET_DATE, run_id="run-1")
    REMITTransformer(tmp_data_dir).run(TARGET_DATE, run_id="run-2")

    parquet_files = _list_parquet_files(silver_dir)
    assert len(parquet_files) == 2, (
        f"expected two run-suffixed parquet files, got {parquet_files!r}"
    )
    for path in parquet_files:
        assert "run" in path.stem, f"expected run-suffixed filename, got {path.name}"


def test_remit_append_only_preserves_revisions_across_runs(
    tmp_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two runs across the same revision pool keep both revisions in silver."""
    _write_remit_bronze(tmp_data_dir)
    silver_dir = tmp_data_dir / "silver" / "elexon" / "remit"

    fixed_times = iter(
        [
            datetime(2024, 2, 1, 8, 30, tzinfo=UTC),
            datetime(2024, 2, 1, 14, 30, tzinfo=UTC),
        ]
    )

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return next(fixed_times)

    monkeypatch.setattr("gridflow.silver.base.datetime", _FakeDateTime)

    REMITTransformer(tmp_data_dir).run(TARGET_DATE, run_id="run-1")
    REMITTransformer(tmp_data_dir).run(TARGET_DATE, run_id="run-2")

    parquet_files = _list_parquet_files(silver_dir)
    frames = [read_parquet(p) for p in parquet_files]
    union = pl.concat(frames, how="diagonal_relaxed")

    revisions = union.filter(pl.col("mrid") == "MSG-A")["revision_number"].to_list()
    assert set(revisions) >= {1, 2}, (
        f"expected both revisions across runs, got {revisions}"
    )


def test_default_atomic_replace_overwrites_single_file(tmp_data_dir: Path) -> None:
    """Default APPEND_ONLY=False: two runs produce one file (atomic replace)."""
    from gridflow.silver.elexon.fuelhh import FuelHHTransformer

    fixtures = Path(__file__).parent.parent / "fixtures" / "elexon"
    payload = json.loads((fixtures / "fuelhh_response.json").read_text())
    bronze = (
        tmp_data_dir
        / "bronze"
        / "elexon"
        / "fuelhh"
        / str(TARGET_DATE.year)
        / f"{TARGET_DATE.month:02d}"
        / f"{TARGET_DATE.day:02d}"
    )
    bronze.mkdir(parents=True, exist_ok=True)
    (bronze / "raw_fuelhh.json").write_text(json.dumps(payload))

    FuelHHTransformer(tmp_data_dir).run(TARGET_DATE, run_id="run-1")
    FuelHHTransformer(tmp_data_dir).run(TARGET_DATE, run_id="run-2")

    silver_dir = tmp_data_dir / "silver" / "elexon" / "fuelhh"
    parquet_files = _list_parquet_files(silver_dir)
    assert len(parquet_files) == 1, (
        f"default APPEND_ONLY=False should overwrite, got {parquet_files!r}"
    )
