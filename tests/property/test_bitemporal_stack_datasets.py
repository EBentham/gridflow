"""Property tests for bitemporal columns on F7 stack datasets.

Verifies that REMIT, BM Unit reference, FOU2T14D, and ENTSO-E installed-capacity
silver outputs carry the bitemporal lineage columns (`event_time`,
`available_at`, `source_run_id`, `dataset_version`) and that each transformer
declares an explicit `DATASET_VERSION` class attribute.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from gridflow.silver.elexon.bmunits import BMUnitsTransformer
from gridflow.silver.elexon.fou2t14d import FOU2T14DTransformer
from gridflow.silver.elexon.remit import REMITTransformer
from gridflow.silver.entsoe.installed_capacity_units import (
    InstalledCapacityUnitsTransformer,
)
from gridflow.storage.parquet import read_parquet

TARGET_DATE = date(2024, 1, 15)
RUN_ID = "f7-test-run"


def _date_dir(root: Path, source: str, dataset: str, target_date: date) -> Path:
    return (
        root
        / "bronze"
        / source
        / dataset
        / str(target_date.year)
        / f"{target_date.month:02d}"
        / f"{target_date.day:02d}"
    )


def _write_remit_bronze(data_dir: Path) -> None:
    bronze = _date_dir(data_dir, "elexon", "remit", TARGET_DATE)
    bronze.mkdir(parents=True, exist_ok=True)
    payload = {
        "data": [
            {
                "mrid": "MSG-001",
                "revisionNumber": 1,
                "publishTime": "2024-01-15T08:00:00Z",
                "messageType": "Production unavailability",
                "messageHeading": "Outage notice",
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
                "eventStartTime": "2024-01-16T00:00:00Z",
                "eventEndTime": "2024-01-20T00:00:00Z",
                "cause": "Maintenance",
                "relatedInformation": "",
            },
        ]
    }
    (bronze / "raw_remit.json").write_text(json.dumps(payload))


def _write_fou2t14d_bronze(data_dir: Path) -> None:
    bronze = _date_dir(data_dir, "elexon", "fou2t14d", TARGET_DATE)
    bronze.mkdir(parents=True, exist_ok=True)
    payload = {
        "data": [
            {
                "settlementDate": "2024-01-17",
                "settlementPeriod": 1,
                "publishTime": "2024-01-15T07:00:00Z",
                "fuelType": "Gas",
                "outputUsable": 12000.0,
            },
            {
                "settlementDate": "2024-01-17",
                "settlementPeriod": 1,
                "publishTime": "2024-01-15T07:00:00Z",
                "fuelType": "Wind",
                "outputUsable": 4500.0,
            },
        ]
    }
    (bronze / "raw_fou2t14d.json").write_text(json.dumps(payload))


def _write_bmunits_bronze(data_dir: Path) -> None:
    bronze = _date_dir(data_dir, "elexon", "bmunits_reference", TARGET_DATE)
    bronze.mkdir(parents=True, exist_ok=True)
    payload = {
        "data": [
            {
                "elexonBmUnit": "T_BMU01",
                "name": "Sample Unit 1",
                "fuelType": "Gas",
                "registeredCapacity": 800.0,
                "leadPartyName": "Operator Ltd",
                "gspGroupId": "_A",
                "nationalGridBmUnit": "NG_T_BMU01",
            },
            {
                "elexonBmUnit": "T_BMU02",
                "name": "Sample Unit 2",
                "fuelType": "Wind",
                "registeredCapacity": 200.0,
                "leadPartyName": "Operator Ltd",
                "gspGroupId": "_B",
                "nationalGridBmUnit": "NG_T_BMU02",
            },
        ]
    }
    (bronze / "raw_bmunits.json").write_text(json.dumps(payload))


def _write_icu_bronze(data_dir: Path) -> None:
    bronze = _date_dir(data_dir, "entsoe", "installed_capacity_units", TARGET_DATE)
    bronze.mkdir(parents=True, exist_ok=True)
    fixture = (
        Path(__file__).parent.parent
        / "fixtures"
        / "entsoe"
        / "installed_capacity_units_gb.xml"
    )
    (bronze / "raw_icu.xml").write_bytes(fixture.read_bytes())


def _read_silver(data_dir: Path, source: str, dataset: str) -> pl.DataFrame:
    pattern = data_dir / "silver" / source / dataset / "**" / "*.parquet"
    return read_parquet(pattern)


def _assert_bitemporal_columns(
    df: pl.DataFrame,
    expected_version: str,
) -> None:
    for column in ("event_time", "available_at", "source_run_id", "dataset_version"):
        assert column in df.columns, f"missing bitemporal column {column}"
        assert df[column].null_count() == 0, f"null values in {column}"
    assert df["event_time"].dtype == pl.Datetime("us", "UTC")
    assert df["available_at"].dtype == pl.Datetime("us", "UTC")
    assert df["source_run_id"].to_list() == [RUN_ID] * len(df)
    assert set(df["dataset_version"].to_list()) == {expected_version}


def test_remit_silver_has_bitemporal_columns_and_version_2(
    tmp_data_dir: Path,
) -> None:
    """REMIT silver carries bitemporal columns and DATASET_VERSION == 2.0.0.

    The version bump from 1.0.0 to 2.0.0 reflects the F7 schema change:
    silver now contains one row per (mrid, revision_number, available_at)
    instead of one row per mrid.
    """
    _write_remit_bronze(tmp_data_dir)

    rows = REMITTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    assert rows > 0

    df = _read_silver(tmp_data_dir, "elexon", "remit")
    assert REMITTransformer.DATASET_VERSION == "2.0.0"
    _assert_bitemporal_columns(df, expected_version="2.0.0")


def test_fou2t14d_silver_has_bitemporal_columns(tmp_data_dir: Path) -> None:
    _write_fou2t14d_bronze(tmp_data_dir)

    rows = FOU2T14DTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    assert rows > 0

    df = _read_silver(tmp_data_dir, "elexon", "fou2t14d")
    assert FOU2T14DTransformer.DATASET_VERSION == "1.0.0"
    _assert_bitemporal_columns(df, expected_version="1.0.0")


def test_bmunits_silver_has_bitemporal_columns(tmp_data_dir: Path) -> None:
    _write_bmunits_bronze(tmp_data_dir)

    rows = BMUnitsTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    assert rows > 0

    df = _read_silver(tmp_data_dir, "elexon", "bmunits_reference")
    assert BMUnitsTransformer.DATASET_VERSION == "1.0.0"
    _assert_bitemporal_columns(df, expected_version="1.0.0")


def test_installed_capacity_units_silver_has_bitemporal_columns(
    tmp_data_dir: Path,
) -> None:
    _write_icu_bronze(tmp_data_dir)

    rows = InstalledCapacityUnitsTransformer(tmp_data_dir).run(
        TARGET_DATE, run_id=RUN_ID
    )
    assert rows > 0

    df = _read_silver(tmp_data_dir, "entsoe", "installed_capacity_units")
    assert InstalledCapacityUnitsTransformer.DATASET_VERSION == "1.0.0"
    _assert_bitemporal_columns(df, expected_version="1.0.0")


def test_remit_event_time_within_available_at(tmp_data_dir: Path) -> None:
    """For each row, event_time precedes available_at (causality invariant)."""
    _write_remit_bronze(tmp_data_dir)

    REMITTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    df = _read_silver(tmp_data_dir, "elexon", "remit")

    assert (df["event_time"] <= df["available_at"]).all()
