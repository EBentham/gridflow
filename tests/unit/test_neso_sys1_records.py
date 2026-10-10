"""The system batch's frozen records (v0.22-K-SYS-1): eight records in six packages.

``voltage_units_utilisation``, ``stability_midterm_y1``, ``system_inertia``,
``system_inertia_cost``, ``outturn_voltage_costs_historical`` and
``outturn_voltage_costs_main`` are eligible (the 2026 inertia cost resource is a resource-level
HOLD); the two Pathfinder reports are held (E-SEM) and six of their captures are adjudicated failed
(ADR-040, ruling 642). Every test writes recorded fixture
captures (cuts of the 2026-10-08 swept bronze, and of the 2026-10-10 voltage utilisation capture,
under ``tests/fixtures/neso_data_portal/sys1/``, provenance in ``PROVENANCE.md``) into a short data
root and runs the transformer the **real package registry** generates, so a record that does not
fit its vendor body fails here, not at activation. On master none of the eight families has a
record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-SYS-1-FACTS, K-SYS-1-SPEC, RULINGS 642):

- Identifiers are stored as the vendor spelled them (``THURB-1,2 & 3``, ``NLSCC-1``,
  ``Unvailable``); zeros and negatives are values; blanks are null; no undocumented token is read
  as null.
- Units live in the FACTS g4 citations (the record model has no unit field): the voltage utilisation
  quantities are MVAr, the midterm ``Inertia (in MVA.s)`` is MVA.s, the system inertia quantities
  are GVA.s, the inertia cost is GBP per GVA.s (both header epochs), the voltage costs are GBP
  million; the Pathfinder ``Inertia`` has no stated unit (TODO).
- Voltage utilisation: the ``month`` recipe, ``Aug-26`` is the whole utilisation month. Open TODOs
  (the record model has no notes field, so they live here): SYS0-KEY (no vendor primary-key
  guarantee for BMU and month), SYS0-REPLACEMENT (whether a later upload replaces or appends the
  month) and SYS0-AGGREGATION (the sign and aggregation convention behind the signed MVAr totals).
- Midterm: the documented GMT interval boundaries type as UTC; ``Month & Year`` is a reporting
  label never forced to equal the start month; ``Hours Utilised`` stays text (TODO
  MIDTERM-DURATION).
- Pathfinder: the four time columns are raw strings because their literal ``+00:00`` conflicts with
  NESO's documented local clock; ``Inertia`` has no unit (TODO); the key is every vendor column, so
  captures repeating an identical row fail ``DuplicateEntityKeyError`` and are adjudicated.
- System inertia: ``sp_pair`` over (date, period 1..50); sparse days are preserved, never filled
  (TODO INERTIA-SPARSITY).
- System inertia cost: ``Cost`` and ``Cost_per_GVAs`` are one silver column (the legacy ``Cost`` is
  the same quantity and unit, FACTS 7 T01); the date spelling is per exact filename; the 2026
  resource shares ``inertia_costs.csv`` with ISO resources but has a slash date, so it is a HOLD
  (TODO INERTIA-COST-ZERO: the meaning of a zero is undocumented).
- Voltage costs: the date spelling is per exact filename; a zero is a value (TODO
  VOLTAGE-COST-ZERO).

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the bronze
originals' CRLF convention; a BOM stays where the original had one."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from _neso_generic_support import install_generated, write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings, run_cli

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import (
    RECONCILE_ADJUDICATIONS_FILE,
    DocDisposition,
    Eligible,
    Held,
    HoldDisposition,
    SilverDisposition,
    load_registry,
)
from gridflow.connectors.neso_data_portal.registry.record import has_issue_time
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.casting import DuplicateEntityKeyError
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    read_failure,
)
from gridflow.silver.neso_data_portal.reconcile import reconcile
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "sys1"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
VOLTAGE_DAY = date(2026, 10, 10)
POUND = "£"

HOLD_UTILISATION = (
    "TODO: are the Pathfinder timestamps actual UTC instants or GB local wall times with a "
    "mechanically attached +00:00 offset (incl. clock-change periods), what unit/scale is "
    "`Inertia`, what does a blank `Inertia` mean, and what vendor row identity distinguishes "
    "identical rows?"
)
HOLD_AVAILABILITY = (
    "TODO: the timestamp clock (as §3), and what vendor row identity / grain makes the report "
    "unique given identical repeated rows?"
)
HELD = {
    "stability_pathfinder_utilisation_report": HOLD_UTILISATION,
    "stability_pathfinder_availability_report": HOLD_AVAILABILITY,
}
ELIGIBLE = (
    "voltage_units_utilisation",
    "stability_midterm_y1",
    "system_inertia",
    "system_inertia_cost",
    "outturn_voltage_costs_historical",
    "outturn_voltage_costs_main",
)
PARTITIONED = (
    "stability_pathfinder_utilisation_report",
    "stability_pathfinder_availability_report",
    "system_inertia",
    "system_inertia_cost",
    "outturn_voltage_costs_historical",
)

V_HEADER = [
    "BMU ID",
    "Month and Year",
    "Location",
    "Total Injection MVAr",
    "Total Absorption MVAr",
    "Total Injection and Absorption MVAr",
]
M_HEADER = [
    "BMU ID",
    "Inertia (in MVA.s)",
    "Month & Year",
    "Utilisation Start Datetime",
    "Utilisation End Datetime",
    "Hours Utilised",
]
U_HEADER = [
    "UNIT",
    "Settlement Period Start Date Time",
    "Settlement Period End Date Time",
    "Instruction Code",
    "Instruction Issue Time",
    "Actual Service Start or Cease Time",
    "Inertia",
]
A_HEADER = [
    "UNIT",
    "Settlement Period Start Date Time",
    "Settlement Period End Date Time",
    "Availability Flag",
    "REMARK",
]
I_HEADER = ["Settlement Date", "Settlement Period", "Outturn Inertia", "Market Provided Inertia"]
C_OLD_HEADER = ["Settlement Date", "Cost"]
C_NEW_HEADER = ["Settlement Date", "Cost_per_GVAs"]
H_HEADER = [
    "Settlement Month",
    "Voltage Constraint Group",
    f"Sync Costs ({POUND}m)",
    f"Utilisation Costs ({POUND}m)",
    "Coordinates",
]
HISTORICAL_FORMATS = {
    "voltagecsv-2014_15.csv": "%d/%m/%Y",
    "voltagecsv-2015_16.csv": "%Y-%m-%d",
    "voltagecsv-2016_17.csv": "%Y-%m-%d",
    "voltagecsv-2017_18.csv": "%Y-%m-%d",
    "voltagecsv-2018_19.csv": "%Y-%m-%d",
    "voltagecsv-2019_20.csv": "%Y-%m-%d",
    "voltagecsv-2020_21.csv": "%Y-%m-%d",
    "voltagecsv-2021_22.csv": "%Y-%m-%d",
    "voltagecsv-2022_23.csv": "%Y-%m-%d",
    "voltagecsv-2023_24.csv": "%Y-%m-%d",
    "voltagecsv-2024_25.csv": "%d/%m/%Y",
}
COST_FORMATS = {
    "inertia_costs17.csv": "%d/%m/%Y",
    "inertia_costs18.csv": "%d/%m/%Y",
    "inertia_costs19.csv": "%d/%m/%Y",
    "inertia_costs20.csv": "%d/%m/%Y",
    "inertia_costs22.csv": "%Y-%m-%d",
    "inertia_costs.csv": "%Y-%m-%d",
}
C26_ID = "6295f4ed-b43d-4a80-8ca9-c27c9fa16517"
PATHFINDER_UTILISATION_FAILED = {
    "f43f5d61-b51f-45f9-a7e7-66f2959d38b5",
    "fc08cdba-1a86-460b-85c4-3d8c0d3d4e42",
}
PATHFINDER_AVAILABILITY_FAILED = {
    "a4fd8208-3e36-4f9c-bdc7-435c3734a2da",
    "bacc45e5-11ac-4bac-88ef-6872f499db97",
    "1264f36f-27ce-4ed9-aab1-393c7612d6bc",
    "690a9e42-20e2-4b0e-b74f-f3e37cac4bac",
}


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    fixture: str
    family: str
    package: str
    package_id: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str
    day: date = DAY


def _capture(
    fixture: str,
    family: str,
    package: str,
    package_id: str,
    resource_id: str,
    name: str,
    filename: str,
    modified: str,
    written: str,
    day: date = DAY,
) -> Capture:
    return Capture(
        fixture, family, package, package_id, resource_id, name, filename, modified, written, day
    )


PF = "stability-pathfinder-service-information"
PF_ID = "ae59b12b-4cfe-40a0-844f-1d139c63d893"
SI = "system-inertia"
SI_ID = "8f3cd0ce-6636-469e-b582-55eadfeaa1d9"
SC = "system-inertia-cost"
SC_ID = "59f20804-9c9e-4f70-a809-c4c48a4f4c6e"
VC = "outturn-voltage-costs"
VC_ID = "8df6dd77-315f-465e-a8d9-ce0492853050"
U, A, INERTIA_KEY, C, H = (
    "stability_pathfinder_utilisation_report",
    "stability_pathfinder_availability_report",
    "system_inertia",
    "system_inertia_cost",
    "outturn_voltage_costs_historical",
)

CAPTURES: dict[str, Capture] = {
    "v": _capture(
        "v.csv",
        "voltage_units_utilisation",
        "monthly-utilisation-data-of-voltage-contracted-units",
        "f8386ee1-4e9c-474d-afbf-6339852c898b",
        "e13539d0-eed0-4561-82a4-4517c14253c1",
        "Monthly Utilisation Data of Voltage 2026 Contracted Units",
        "utilisation-report.csv",
        "2026-10-09T12:57:26.168291",
        "2026-10-10T15:50:55.896317+00:00",
        VOLTAGE_DAY,
    ),
    "m": _capture(
        "m.csv",
        "stability_midterm_y1",
        "stability-midterm-y-1-utilisation-report",
        "84abeae2-be21-4755-bbfc-3792f5315a36",
        "e0c86d21-8ad1-4ba3-a112-b2bbcf5277ce",
        "Stability midterm (Y-1) utilisation report 25/26",
        "stability.csv",
        "2026-09-28T16:54:21.377483",
        "2026-10-08T11:27:05.641643+00:00",
    ),
    "u23": _capture(
        "u23.csv",
        U,
        PF,
        PF_ID,
        "52af0f68-e1c3-45dd-ae7f-54f68e32e0c6",
        "Stability Pathfinder Utilisation Report 2023-2024",
        "stability-pathfinder-utilisation-report-2023.csv",
        "2024-04-05T15:45:09.278460",
        "2026-10-08T11:27:20.815424+00:00",
    ),
    "u25": _capture(
        "u25.csv",
        U,
        PF,
        PF_ID,
        "fc08cdba-1a86-460b-85c4-3d8c0d3d4e42",
        "Stability Pathfinder Utilisation Report 2025-2026",
        "stability-pathfinder-utilisation-report-2025.csv",
        "2026-04-10T15:11:46.723351",
        "2026-10-08T11:27:25.857163+00:00",
    ),
    "u26": _capture(
        "u26.csv",
        U,
        PF,
        PF_ID,
        "75996c0a-70fb-4ddb-a345-bb1e8d39de35",
        "Stability Pathfinder Utilisation Report 2026-2027",
        "stability-pathfinder-utilisation-report-2026.csv",
        "2026-10-02T14:49:24.733259",
        "2026-10-08T11:27:28.771297+00:00",
    ),
    "a23": _capture(
        "a23.csv",
        A,
        PF,
        PF_ID,
        "a4fd8208-3e36-4f9c-bdc7-435c3734a2da",
        "Stability Pathfinder Availability Report 2023-2024",
        "stability-pathfinder-availability-report-2023.csv",
        "2024-04-05T15:44:41.388092",
        "2026-10-08T11:27:09.606261+00:00",
    ),
    "a26": _capture(
        "a26.csv",
        A,
        PF,
        PF_ID,
        "690a9e42-20e2-4b0e-b74f-f3e37cac4bac",
        "Stability Pathfinder Availability Report 2026-2027",
        "stability-pathfinder-availability-report-2026.csv",
        "2026-10-02T14:49:34.759439",
        "2026-10-08T11:27:17.484052+00:00",
    ),
    "i21": _capture(
        "i21.csv",
        INERTIA_KEY,
        SI,
        SI_ID,
        "55161fb4-1396-46e2-9250-2e2b9df904bf",
        "GB System Inertia - 2021-2022",
        "inertia.csv",
        "2022-04-04T10:31:22.375254",
        "2026-10-08T11:37:33.979353+00:00",
    ),
    "i26": _capture(
        "i26.csv",
        INERTIA_KEY,
        SI,
        SI_ID,
        "3ff8b466-5c16-4713-abfe-ad332298f15f",
        "GB System Inertia - 2026-2027",
        "inertia.csv",
        "2026-07-27T14:01:47.463582",
        "2026-10-08T11:37:47.650701+00:00",
    ),
    "c17": _capture(
        "c17.csv",
        C,
        SC,
        SC_ID,
        "28dc603e-3472-45cd-8e7b-998efc674084",
        "GB System Inertia Costs 2017",
        "inertia_costs17.csv",
        "2021-12-07T12:12:09.974451",
        "2026-10-08T11:37:51.909336+00:00",
    ),
    "c22": _capture(
        "c22.csv",
        C,
        SC,
        SC_ID,
        "8a3a4233-e9aa-45c2-b0c4-28b8d614165b",
        "GB System Inertia Costs 2022",
        "inertia_costs22.csv",
        "2024-04-02T16:17:02.960104",
        "2026-10-08T11:38:05.921910+00:00",
    ),
    "c24": _capture(
        "c24.csv",
        C,
        SC,
        SC_ID,
        "91947c0c-bbbf-4d55-a29d-dc610d91d075",
        "GB System Inertia Costs 2024",
        "inertia_costs.csv",
        "2025-09-04T13:30:48.701007",
        "2026-10-08T11:38:11.406081+00:00",
    ),
    "c26": _capture(
        "c26.csv",
        C,
        SC,
        SC_ID,
        C26_ID,
        "GB System Inertia Costs 2026",
        "inertia_costs.csv",
        "2026-04-14T14:54:26.059319",
        "2026-10-08T11:38:16.330604+00:00",
    ),
    "h14": _capture(
        "h14.csv",
        H,
        VC,
        VC_ID,
        "fae5a592-deb4-4c66-8d51-d6ef450ccd95",
        "Historical Outturn Voltage Costs 2014-2015",
        "voltagecsv-2014_15.csv",
        "2024-05-24T13:55:17.240734",
        "2026-10-08T11:16:16.340937+00:00",
    ),
    "h15": _capture(
        "h15.csv",
        H,
        VC,
        VC_ID,
        "035ab58a-7b96-4e10-bf51-cf3a7e64f6a0",
        "Historical Outturn Voltage Costs 2015-2016",
        "voltagecsv-2015_16.csv",
        "2024-05-24T13:56:19.823004",
        "2026-10-08T11:16:18.592535+00:00",
    ),
    "h24": _capture(
        "h24.csv",
        H,
        VC,
        VC_ID,
        "3eced73f-b7e6-4974-8d3c-3ebd48eba74c",
        "Historical Outturn Voltage Costs 2024-2025",
        "voltagecsv-2024_25.csv",
        "2025-04-29T15:42:04.442764",
        "2026-10-08T11:16:41.786398+00:00",
    ),
    "vm": _capture(
        "vm.csv",
        "outturn_voltage_costs_main",
        VC,
        VC_ID,
        "073f9ffa-05d5-47e5-8835-e1ac31b7656d",
        "Outturn Voltage Costs 2025-2026",
        "voltagecsv-2025_26.csv",
        "2025-06-23T11:40:39.592575",
        "2026-10-08T11:16:45.428605+00:00",
    ),
}
PACKAGE_FILES = {
    "voltage_units_utilisation": "monthly-utilisation-data-of-voltage-contracted-units.json",
    "stability_midterm_y1": "stability-midterm-y-1-utilisation-report.json",
    U: "stability-pathfinder-service-information.json",
    A: "stability-pathfinder-service-information.json",
    INERTIA_KEY: "system-inertia.json",
    C: "system-inertia-cost.json",
    H: "outturn-voltage-costs.json",
    "outturn_voltage_costs_main": "outturn-voltage-costs.json",
}
FAMILIES = tuple(PACKAGE_FILES)


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings (and so the CLI) point at."""
    with tempfile.TemporaryDirectory(
        prefix="sy1", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's CRLF line endings (a BOM stays)."""
    raw = (FIXTURES / CAPTURES[alias].fixture).read_bytes()
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    text = raw[len(bom) :].replace(b"\r\n", b"\n")
    return bom + text.replace(b"\n", b"\r\n")


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed."""
    data_bytes = raw if raw is not None else body(alias)
    reader = csv.DictReader(io.StringIO(data_bytes.decode("utf-8-sig"), newline=""))
    return [r for r in reader if any(v for v in r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8-sig").split("\r\n", 1)[0]
    return next(csv.reader([first]))


def capture(
    data: Path,
    alias: str,
    *,
    raw: bytes | None = None,
    filename: str | None = None,
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=meta.day,
        package_slug=meta.package,
        package_id=meta.package_id,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=filename or meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
        ckan_format="CSV",
        extension="csv",
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _no_silver(data: Path, key: str) -> bool:
    return not list((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))


def _run(data: Path, alias: str, **kwargs: Any) -> tuple[str, int]:
    """Capture ``alias``, run its family's transformer; the capture id and rows written."""
    capture_id = capture(data, alias, **kwargs)
    meta = CAPTURES[alias]
    written = get_transformer(SOURCE, meta.family, data).run(meta.day, run_id="r")
    return capture_id, written


def _package_doc(filename: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / filename).read_text(encoding="utf-8"))
    return document


def _columns(key: str) -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record(key)))
        if c not in ("year", "month")
    ]


def _assert_clean_load(data: Path, alias: str, expected_rows: int) -> pl.DataFrame:
    """Capture + run ``alias``: every row written, none excluded, the generic output columns."""
    meta = CAPTURES[alias]
    capture_id, written = _run(data, alias)
    assert written == expected_rows
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
    frame = _silver(data, meta.family)
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(meta.family)
    return frame


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header (incl.
    the ``£`` of the voltage cost columns and the quoted inertia headers), CRLF endings, the
    Pathfinder BOMs, the composite BMU id and negative MVAr of the voltage utilisation row, a
    midterm label month that differs from the start month and both ``Hours Utilised`` shapes, the
    identical Pathfinder row pairs (U25, A23), the one real ``Unvailable`` row, the 50- and
    46-period clock-change days, both inertia cost headers and date spellings, the lone slash-dated
    2026 cost row, and both voltage cost date spellings."""
    assert header("v") == V_HEADER and header("m") == M_HEADER
    for alias in ("u23", "u25", "u26"):
        assert header(alias) == U_HEADER, alias
    for alias in ("a23", "a26"):
        assert header(alias) == A_HEADER, alias
    for alias in ("i21", "i26"):
        assert header(alias) == I_HEADER, alias
    assert header("c17") == C_OLD_HEADER
    assert header("c22") == header("c24") == header("c26") == C_NEW_HEADER
    for alias in ("h14", "h15", "h24", "vm"):
        assert header(alias) == H_HEADER, alias
    assert POUND in H_HEADER[2] and POUND in H_HEADER[3]
    for alias in CAPTURES:
        assert body(alias).count(b"\r\n") == body(alias).count(b"\n"), alias
    for alias in ("u25", "u26", "h14", "h15", "h24", "vm"):
        assert body(alias).startswith(b"\xef\xbb\xbf"), alias
    for alias in ("v", "m", "u23", "a23", "a26", "i21", "c17", "c26"):
        assert not body(alias).startswith(b"\xef\xbb\xbf"), alias

    (voltage,) = rows("v")
    assert voltage["BMU ID"] == "THURB-1,2 & 3" and voltage["Month and Year"] == "Aug-26"
    assert (
        float(voltage["Total Absorption MVAr"]) < 0 and float(voltage["Total Injection MVAr"]) > 0
    )

    midterm = rows("m")
    assert len(midterm) == 61
    crossing = [
        r
        for r in midterm
        if datetime.strptime(r["Month & Year"], "%b-%y").month
        != datetime.strptime(r["Utilisation Start Datetime"], "%d/%m/%Y %H:%M").month
    ]
    assert crossing, "a label month that is not the start month"
    assert {len(r["Hours Utilised"].split(":")) for r in midterm} == {2, 3}

    for alias in ("u25", "a23"):
        identical = [tuple(r.values()) for r in rows(alias)]
        assert len(identical) - len(set(identical)) == 1, alias
    assert [r for r in rows("u25") if r["UNIT"] == "THRSC-1"][0] == [
        r for r in rows("u25") if r["UNIT"] == "THRSC-1"
    ][1]
    assert [r["Availability Flag"] for r in rows("a26")].count("Unvailable") == 1
    assert "Unavailable" in {r["Availability Flag"] for r in rows("a26")}
    assert all(
        r[c].endswith("+00:00")
        for r in rows("u23") + rows("u25") + rows("u26") + rows("a23") + rows("a26")
        for c in ("Settlement Period Start Date Time", "Settlement Period End Date Time")
    )

    per_day: dict[str, int] = {}
    for r in rows("i21"):
        per_day[r["Settlement Date"]] = per_day.get(r["Settlement Date"], 0) + 1
    assert per_day["2021-10-31"] == 50 and per_day["2022-03-27"] == 46

    assert rows("c17")[0]["Settlement Date"] == "01/04/2017"
    assert rows("c22")[0]["Settlement Date"] == "2022-04-01"
    assert rows("c24")[0]["Settlement Date"] == "2024-04-01"
    assert [r["Settlement Date"] for r in rows("c26")] == ["01/04/2026"]
    assert rows("h14")[0]["Settlement Month"] == "01/04/2014"
    assert rows("h15")[0]["Settlement Month"] == "2015-04-01"
    assert rows("h24")[0]["Settlement Month"] == "01/04/2024"
    assert len(rows("vm")) == 38 and rows("vm")[0]["Settlement Month"] == "01/04/2025"


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", FAMILIES)
def test_every_record_is_csv_utf8_version_1_with_ckan_last_modified_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time or a fallback vintage: every family's record is
    version 1, utf-8 CSV, ``ckan_last_modified`` vintage (all captures are uploads), whole-capture
    selection, per-resource selection exactly for the five multi-resource families, and no epoch
    declares an issue recipe (``issue_time`` is never emitted)."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert (record.vintage, record.vintage_evidence) == ("ckan_last_modified", None)
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key in PARTITIONED else None)
    assert ("resource_id" in record.entity_key) == (key in PARTITIONED)
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)
    assert record.siblings == () and record.xlsx is None


def test_voltage_utilisation_record() -> None:
    """Detects the month parsed with a wrong format, a composite BMU split, a signed MVAr quantity
    typed as an integer or forced non-negative, or a month key invented: ``Aug-26`` reads
    ``%b-%y`` into a non-nullable date that is the ``month`` recipe's input, the three MVAr
    columns are nullable float64, BMU and month are the key."""
    record = _record("voltage_units_utilisation")
    (epoch,) = record.epochs
    assert list(epoch.header) == V_HEADER
    assert [(c.name, c.dtype, c.format, c.nullable) for c in epoch.columns] == [
        ("bmu_id", "string", None, False),
        ("month_and_year", "date", "%b-%y", False),
        ("location", "string", None, True),
        ("total_injection_mvar", "float64", None, True),
        ("total_absorption_mvar", "float64", None, True),
        ("total_injection_and_absorption_mvar", "float64", None, True),
    ]
    assert not any(c.min is not None or c.max is not None or c.null_tokens for c in epoch.columns)
    assert (record.temporal.kind, record.temporal.date_column) == ("month", "month_and_year")
    assert record.entity_key == ("bmu_id", "month_and_year")


def test_midterm_record_types_the_gmt_boundaries_and_keeps_the_duration_text() -> None:
    """Detects the GMT boundaries read in London time, the duration converted, or the reporting
    month forced to the start month: start and end are non-nullable UTC datetimes in
    ``%d/%m/%Y %H:%M``, ``Month & Year`` a ``%b-%y`` date column of its own, ``Hours Utilised`` a
    string, the temporal recipe the start instant, the key BMU + start + end."""
    record = _record("stability_midterm_y1")
    (epoch,) = record.epochs
    assert list(epoch.header) == M_HEADER
    by_name = {c.name: c for c in epoch.columns}
    assert [c.name for c in epoch.columns] == [
        "bmu_id",
        "inertia_mva_s",
        "month_and_year",
        "utilisation_start_datetime",
        "utilisation_end_datetime",
        "hours_utilised",
    ]
    for name in ("utilisation_start_datetime", "utilisation_end_datetime"):
        column = by_name[name]
        assert (column.dtype, column.format, column.zone, column.nullable) == (
            "datetime",
            "%d/%m/%Y %H:%M",
            "UTC",
            False,
        )
    assert (by_name["inertia_mva_s"].dtype, by_name["inertia_mva_s"].nullable) == ("float64", True)
    assert (by_name["month_and_year"].dtype, by_name["month_and_year"].format) == ("date", "%b-%y")
    assert (by_name["hours_utilised"].dtype, by_name["hours_utilised"].format) == ("string", None)
    assert (record.temporal.kind, record.temporal.column) == (
        "utc_instant",
        "utilisation_start_datetime",
    )
    assert record.entity_key == (
        "bmu_id",
        "utilisation_start_datetime",
        "utilisation_end_datetime",
    )


def test_pathfinder_records_keep_the_time_columns_raw_and_key_on_every_column() -> None:
    """Detects a time column parsed, stripped or relabelled (the literal ``+00:00`` conflicts with
    the documented local clock) or a derived key: the four utilisation time columns and the two
    availability ones are strings, the utilisation ``Inertia`` is a nullable int64, temporal is
    ``none``, and each key is ``resource_id`` plus every vendor column, in header order."""
    util, avail = _record(U), _record(A)
    (util_epoch,) = util.epochs
    (avail_epoch,) = avail.epochs
    assert list(util_epoch.header) == U_HEADER and list(avail_epoch.header) == A_HEADER
    assert [(c.name, c.dtype) for c in util_epoch.columns] == [
        ("unit", "string"),
        ("settlement_period_start_date_time", "string"),
        ("settlement_period_end_date_time", "string"),
        ("instruction_code", "string"),
        ("instruction_issue_time", "string"),
        ("actual_service_start_or_cease_time", "string"),
        ("inertia", "int64"),
    ]
    assert [(c.name, c.dtype) for c in avail_epoch.columns] == [
        ("unit", "string"),
        ("settlement_period_start_date_time", "string"),
        ("settlement_period_end_date_time", "string"),
        ("availability_flag", "string"),
        ("remark", "string"),
    ]
    for epoch in (util_epoch, avail_epoch):
        assert all(c.format is None and c.zone is None and not c.null_tokens for c in epoch.columns)
        assert all(c.min is None and c.max is None for c in epoch.columns)
    assert next(c for c in util_epoch.columns if c.name == "inertia").nullable is True
    for record, epoch in ((util, util_epoch), (avail, avail_epoch)):
        assert record.temporal.kind == "none"
        assert record.entity_key == ("resource_id", *[c.name for c in epoch.columns])


def test_inertia_record_bounds_the_period_and_keys_on_the_settlement_pair() -> None:
    """Detects the settlement period left unbounded or capped at 48 (clock-change days have 46 and
    50), the pair recipe lost, or a quantity typed as a float: ``Settlement Period`` is a
    non-nullable int64 in 1..50, the date ``%Y-%m-%d``, both inertia quantities nullable int64,
    temporal ``sp_pair`` and the key (resource, date, period)."""
    record = _record(INERTIA_KEY)
    (epoch,) = record.epochs
    assert list(epoch.header) == I_HEADER
    assert [(c.name, c.dtype, c.format, c.nullable, c.min, c.max) for c in epoch.columns] == [
        ("settlement_date", "date", "%Y-%m-%d", False, None, None),
        ("settlement_period", "int64", None, False, 1, 50),
        ("outturn_inertia", "int64", None, True, None, None),
        ("market_provided_inertia", "int64", None, True, None, None),
    ]
    assert (record.temporal.kind, record.temporal.date_column, record.temporal.period_column) == (
        "sp_pair",
        "settlement_date",
        "settlement_period",
    )
    assert record.entity_key == ("resource_id", "settlement_date", "settlement_period")


def test_inertia_cost_record_has_two_epochs_and_one_cost_column() -> None:
    """Detects the legacy ``Cost`` kept as a second silver column (it is the same quantity and
    unit) or a date spelling that is not the exact per-filename map: two epochs (``Cost`` and
    ``Cost_per_GVAs``) both yield the int64 ``cost_gbp_per_gvas`` and a non-nullable
    ``settlement_date`` whose formats are exactly the six filenames (the DMY ones are the 2017-2020
    resources), ``date_sp1`` anchor, key (resource, date)."""
    record = _record(C)
    old, new = record.epochs
    assert list(old.header) == C_OLD_HEADER and list(new.header) == C_NEW_HEADER
    for epoch, value_source in ((old, "Cost"), (new, "Cost_per_GVAs")):
        date_column, cost = epoch.columns
        assert (date_column.source, date_column.name, date_column.dtype) == (
            "Settlement Date",
            "settlement_date",
            "date",
        )
        assert date_column.format is None and date_column.nullable is False
        assert date_column.formats_by_filename is not None
        assert dict(date_column.formats_by_filename) == COST_FORMATS
        assert len(date_column.formats_by_filename) == len(COST_FORMATS)
        assert (cost.source, cost.name, cost.dtype, cost.nullable) == (
            value_source,
            "cost_gbp_per_gvas",
            "int64",
            True,
        )
    assert [c.name for c in old.columns] == [c.name for c in new.columns]
    assert (record.temporal.kind, record.temporal.date_column) == ("date_sp1", "settlement_date")
    assert record.entity_key == ("resource_id", "settlement_date")


def test_voltage_cost_records_share_one_header_and_differ_in_the_date_spelling() -> None:
    """Detects a cost typed as a string, the group renamed or a date spelling guessed: both records
    read the same header (with ``£``), the group and coordinates as strings, the two costs as
    nullable float64; the historical month uses exactly the eleven-filename map, the main month the
    scalar ``%d/%m/%Y``; ``month`` recipe over ``settlement_month``; keys with and without the
    resource."""
    hist, main = _record(H), _record("outturn_voltage_costs_main")
    for record in (hist, main):
        (epoch,) = record.epochs
        assert list(epoch.header) == H_HEADER
        assert [(c.name, c.dtype, c.nullable) for c in epoch.columns] == [
            ("settlement_month", "date", False),
            ("voltage_constraint_group", "string", False),
            ("sync_costs_gbp_m", "float64", True),
            ("utilisation_costs_gbp_m", "float64", True),
            ("coordinates", "string", True),
        ]
        assert (record.temporal.kind, record.temporal.date_column) == (
            "month",
            "settlement_month",
        )
    month = hist.epochs[0].columns[0]
    assert month.format is None and month.formats_by_filename is not None
    assert dict(month.formats_by_filename) == HISTORICAL_FORMATS
    assert len(month.formats_by_filename) == 11
    main_month = main.epochs[0].columns[0]
    assert (main_month.format, main_month.formats_by_filename) == ("%d/%m/%Y", None)
    assert hist.entity_key == ("resource_id", "settlement_month", "voltage_constraint_group")
    assert main.entity_key == ("settlement_month", "voltage_constraint_group")


@pytest.mark.parametrize("key", list(HELD))
def test_pathfinder_reports_are_held_exactly_as_ruled_and_the_package_stays_eligible(
    key: str,
) -> None:
    """Detects a hold lost, reworded or moved to the package, or an unlisted question: both
    Pathfinder reports carry the spec's verbatim E-SEM question, are effectively held, and the
    package itself stays eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == ("E-SEM", HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_six_other_families_are_eligible(key: str) -> None:
    """Detects a hold wrongly put on an eligible family: none carries a record-level eligibility, so
    each is effectively eligible (the 2026 inertia cost HOLD is resource-level)."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_each_family_is_in_the_package_file_the_spec_names() -> None:
    """Detects a record in the wrong package file, a family split, the PDF routed to silver, the
    2026 inertia cost resource left SILVER, or a resource disposition drifting: each file carries
    exactly its record(s); the network diagram PDF stays DOC; every CSV resource of a record family
    is SILVER of that family, except the 2026 inertia cost resource (HOLD, unit SYS-1) whose reason
    names the shared ``inertia_costs.csv`` filename and the slash date."""
    registry = load_registry()
    by_file: dict[str, set[str]] = {}
    for key, filename in PACKAGE_FILES.items():
        by_file.setdefault(filename, set()).add(key)
    for filename, keys in by_file.items():
        document = _package_doc(filename)
        assert {f["key"] for f in document["families"] if "record" in f} == keys, filename
        for resource in document["resources"]:
            family = resource["family"]
            if family == "outturn_voltage_costs_files":
                assert resource["disposition"] == {"kind": "DOC"}
            elif resource["id"] == C26_ID:
                assert resource["disposition"]["kind"] == "HOLD"
            else:
                assert resource["disposition"] == {"kind": "SILVER", "key": family}
    for alias, meta in CAPTURES.items():
        assert registry.resources[meta.resource_id][1].family == meta.family, alias
    counts = {
        key: len([r for _p, r in registry.resources.values() if r.family == key])
        for key in FAMILIES
    }
    assert counts == {
        "voltage_units_utilisation": 1,
        "stability_midterm_y1": 1,
        U: 4,
        A: 4,
        INERTIA_KEY: 10,
        C: 10,
        H: 11,
        "outturn_voltage_costs_main": 1,
    }
    pdf = [r for _p, r in registry.resources.values() if r.family == "outturn_voltage_costs_files"]
    assert len(pdf) == 1 and isinstance(pdf[0].disposition, DocDisposition)


def test_the_2026_inertia_cost_resource_is_a_resource_level_hold() -> None:
    """Detects the held resource silently dropped, a made-up ADR-040 failure instead of a HOLD, or a
    reason that lost the evidence: exactly C26 is HOLD (unit SYS-1) and the other nine cost
    resources are SILVER; the reason carries the shared filename, both header and date spellings,
    ADR-039 and what re-dispositions it."""
    registry = load_registry()
    holds = [
        r
        for _p, r in registry.resources.values()
        if r.family == C and not isinstance(r.disposition, SilverDisposition)
    ]
    assert [r.id for r in holds] == [C26_ID]
    disposition = holds[0].disposition
    assert isinstance(disposition, HoldDisposition) and disposition.unit == "SYS-1"
    for needle in (
        "Settlement Date,Cost_per_GVAs",
        "inertia_costs.csv",
        "01/04/2026",
        "ISO",
        "ADR-039",
        "exact filename",
        "corrected vendor upload",
    ):
        assert needle in disposition.reason, needle
    assert len([r for _p, r in registry.resources.values() if r.family == C]) == 10


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


def test_voltage_utilisation_types_the_month_and_keeps_the_signed_quantities(data: Path) -> None:
    """Detects the month mis-parsed, the composite BMU split or normalised, a negative MVAr dropped
    or made positive, or the ``month`` anchor off: the one row loads with zero exclusions,
    ``month_and_year`` is 2026-08-01, the BMU is ``THURB-1,2 & 3`` verbatim, the three quantities
    are float64 equal to the cells (absorption and the combined total negative), and
    ``timestamp_utc`` is the month's first London midnight, 2026-07-31T23:00Z."""
    (cells,) = rows("v")
    frame = _assert_clean_load(data, "v", 1)
    row = frame.row(0, named=True)
    assert row["bmu_id"] == "THURB-1,2 & 3" and row["location"] == cells["Location"]
    assert row["month_and_year"] == date(2026, 8, 1)
    assert frame.schema["month_and_year"] == pl.Date
    for column, source in (
        ("total_injection_mvar", "Total Injection MVAr"),
        ("total_absorption_mvar", "Total Absorption MVAr"),
        ("total_injection_and_absorption_mvar", "Total Injection and Absorption MVAr"),
    ):
        assert frame.schema[column] == pl.Float64
        assert row[column] == float(cells[source]), column
    assert row["total_absorption_mvar"] < 0 and row["total_injection_and_absorption_mvar"] < 0
    assert row["timestamp_utc"] == datetime(2026, 7, 31, 23, 0, tzinfo=UTC)


def test_midterm_types_the_gmt_boundaries_as_utc_and_keeps_text_and_labels(data: Path) -> None:
    """Detects a GMT boundary shifted by London time, the duration text converted, the reporting
    month forced to the start month, or a row lost: all 61 rows load with zero exclusions, start and
    end are the UTC instants of the CSV text, ``timestamp_utc`` is the start, ``Hours Utilised`` is
    the CSV string (two- and three-component), ``Month & Year`` is the first of its label month and
    still differs from the start month on the crossing rows, inertia is a float64, and the key is
    unique."""
    source = rows("m")
    frame = _assert_clean_load(data, "m", len(source))

    def parse(text: str) -> datetime:
        return datetime.strptime(text, "%d/%m/%Y %H:%M").replace(tzinfo=UTC)

    assert frame["utilisation_start_datetime"].to_list() == [
        parse(r["Utilisation Start Datetime"]) for r in source
    ]
    assert frame["utilisation_end_datetime"].to_list() == [
        parse(r["Utilisation End Datetime"]) for r in source
    ]
    assert frame["timestamp_utc"].to_list() == frame["utilisation_start_datetime"].to_list()
    assert frame.schema["hours_utilised"] == pl.Utf8
    assert frame["hours_utilised"].to_list() == [r["Hours Utilised"] for r in source]
    assert {len(v.split(":")) for v in frame["hours_utilised"].to_list()} == {2, 3}
    assert frame["month_and_year"].to_list() == [
        datetime.strptime(r["Month & Year"], "%b-%y").date() for r in source
    ]
    crossing = frame.filter(
        pl.col("month_and_year").dt.month() != pl.col("utilisation_start_datetime").dt.month()
    )
    assert crossing.height >= 1
    assert frame.schema["inertia_mva_s"] == pl.Float64
    assert frame["inertia_mva_s"].to_list() == [float(r["Inertia (in MVA.s)"]) for r in source]
    key = ["bmu_id", "utilisation_start_datetime", "utilisation_end_datetime"]
    assert frame.select(key).is_duplicated().sum() == 0


@pytest.mark.parametrize("alias", ["u23", "u26"])
def test_utilisation_fixtures_keep_every_time_string_raw_and_blanks_null(
    data: Path, alias: str
) -> None:
    """Detects a Pathfinder timestamp parsed, stripped of its ``+00:00``, relabelled or shifted, a
    blank issue time or inertia read as zero or text, or a zero inertia dropped: every row loads
    with zero exclusions; the four time columns equal the CSV text byte for byte (blank is null),
    the instruction code and unit are untouched, ``Inertia`` is int64 (blank null, zero a value) and
    ``timestamp_utc`` is the capture time because no recipe is evidenced."""
    source = rows(alias)
    frame = _assert_clean_load(data, alias, len(source))
    meta = CAPTURES[alias]
    for column, vendor in (
        ("settlement_period_start_date_time", "Settlement Period Start Date Time"),
        ("settlement_period_end_date_time", "Settlement Period End Date Time"),
        ("instruction_issue_time", "Instruction Issue Time"),
        ("actual_service_start_or_cease_time", "Actual Service Start or Cease Time"),
    ):
        assert frame.schema[column] == pl.Utf8, column
        assert frame[column].to_list() == [r[vendor] or None for r in source], column
    assert frame["unit"].to_list() == [r["UNIT"] for r in source]
    assert frame["instruction_code"].to_list() == [r["Instruction Code"] for r in source]
    assert frame.schema["inertia"] == pl.Int64
    assert frame["inertia"].to_list() == [
        int(r["Inertia"]) if r["Inertia"] else None for r in source
    ]
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    assert set(frame["resource_id"].to_list()) == {meta.resource_id}
    assert frame.select(["resource_id", *_columns(U)[:7]]).is_duplicated().sum() == 0
    if alias == "u23":
        assert frame["inertia"].null_count() > 0
        assert frame["instruction_issue_time"].null_count() > 0
        assert {"Service Start", "Service Cease", "No"} <= set(frame["instruction_code"].to_list())
    else:
        assert 0 in frame["inertia"].to_list()


@pytest.mark.parametrize(("alias", "family"), [("u25", U), ("a23", A)])
def test_identical_pathfinder_rows_fail_the_capture_and_are_never_deduplicated(
    data: Path, alias: str, family: str
) -> None:
    """Detects an identical row deduplicated, absorbed into the key or given an invented occurrence
    index: a body repeating one complete row (THRSC-1 in the 2025-26 utilisation report, RASSP-1 in
    the 2023-24 availability report) fails with ``DuplicateEntityKeyError``, leaves a failure
    record, no completion and no silver."""
    capture_id = capture(data, alias)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    failure = read_failure(data, family, capture_id)
    assert failure is not None and failure["error_class"] == DuplicateEntityKeyError.__name__
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_availability_fixture_keeps_every_string_including_the_unvailable_flag(data: Path) -> None:
    """Detects the vendor misspelling ``Unvailable`` normalised or nulled, a remark altered, or a
    blank remark read as text: the clean slice loads every row with zero exclusions, the flags
    ``Available`` / ``Unavailable`` / ``Unvailable`` equal the CSV text, the ``Unvailable at ...``
    remark is verbatim and a blank remark is null, the time columns are raw strings."""
    source = rows("a26")
    frame = _assert_clean_load(data, "a26", len(source))
    assert frame["availability_flag"].to_list() == [r["Availability Flag"] for r in source]
    assert frame["availability_flag"].to_list().count("Unvailable") == 1
    assert frame["remark"].to_list() == [r["REMARK"] or None for r in source]
    assert "Unvailable at 2026-09-24T00:59:00+00:00" in frame["remark"].to_list()
    assert frame["remark"].null_count() == sum(1 for r in source if not r["REMARK"])
    assert frame["settlement_period_start_date_time"].to_list() == [
        r["Settlement Period Start Date Time"] for r in source
    ]
    assert set(frame["timestamp_utc"].to_list()) == {
        datetime.fromisoformat(CAPTURES["a26"].written)
    }


def test_inertia_fixture_keeps_the_clock_change_days_and_never_fills_a_gap(data: Path) -> None:
    """Detects a clock-change day squeezed into 48 periods, a sparse day filled, a quantity changed
    or the pair recipe off: the slice loads every row with zero exclusions, the 50-period autumn day
    and the 46-period spring day are intact, the rows are exactly the CSV rows (nothing filled),
    both quantities are int64 equal to the cells, ``timestamp_utc`` is the start of the settlement
    period (period 50 of 2021-10-31 is 2021-10-31T23:30Z), and the key is unique."""
    source = rows("i21")
    frame = _assert_clean_load(data, "i21", len(source))
    per_day = frame.group_by("settlement_date").len()
    counts = dict(zip(per_day["settlement_date"].to_list(), per_day["len"].to_list(), strict=True))
    assert counts[date(2021, 10, 31)] == 50 and counts[date(2022, 3, 27)] == 46
    assert counts[date(2021, 4, 1)] == 3
    assert frame["settlement_date"].to_list() == [
        date.fromisoformat(r["Settlement Date"]) for r in source
    ]
    assert frame["settlement_period"].to_list() == [int(r["Settlement Period"]) for r in source]
    assert frame.schema["outturn_inertia"] == pl.Int64
    assert frame["outturn_inertia"].to_list() == [int(r["Outturn Inertia"]) for r in source]
    assert frame["market_provided_inertia"].to_list() == [
        int(r["Market Provided Inertia"]) for r in source
    ]
    expected = [
        settlement_period_to_utc(d, p)
        for d, p in zip(frame["settlement_date"], frame["settlement_period"], strict=True)
    ]
    assert frame["timestamp_utc"].to_list() == expected
    assert settlement_period_to_utc(date(2021, 10, 31), 50) == datetime(
        2021, 10, 31, 23, 30, tzinfo=UTC
    )
    assert (
        frame.select(["resource_id", "settlement_date", "settlement_period"]).is_duplicated().sum()
        == 0
    )


@pytest.mark.parametrize(
    ("cell", "rule"),
    [(b"2021-04-01,51,175,121", "range"), (b"2021-04-01,49,175,121", "settlement_period")],
)
def test_an_impossible_settlement_period_is_excluded_and_surfaced_not_loaded(
    data: Path, cell: bytes, rule: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects an out-of-range or impossible settlement period reaching silver: period 51 breaches
    the 1..50 bound and period 49 does not exist on an ordinary day, so the row is excluded, the
    completion tallies it, and the warning names the rule."""
    raw = body("i21").replace(b"2021-04-01,1,175,121", cell, 1)
    assert raw != body("i21")
    with caplog.at_level("WARNING"):
        capture_id = capture(data, "i21", raw=raw)
        written = get_transformer(SOURCE, INERTIA_KEY, data).run(DAY, run_id="r")
    assert written == len(rows("i21")) - 1
    completion = read_completion(data, INERTIA_KEY, capture_id)
    assert completion is not None and completion["rows_excluded"] == 1
    assert any(rule in r.getMessage() for r in caplog.records)
    assert 51 not in _silver(data, INERTIA_KEY)["settlement_period"].to_list()


def test_both_inertia_cost_header_epochs_land_in_one_cost_column(data: Path) -> None:
    """Detects the legacy ``Cost`` kept in a second column or its slash dates mis-read as ISO (or
    the reverse): a 2017 resource (``Cost``, ``inertia_costs17.csv``, DMY), a 2022 one
    (``Cost_per_GVAs``, ``inertia_costs22.csv``, ISO) and a 2024 one (``Cost_per_GVAs``,
    ``inertia_costs.csv``, ISO) all load with zero exclusions into the int64 ``cost_gbp_per_gvas``
    equal to their cells, dates equal to each file's own spelling, zeros kept, and the
    ``date_sp1`` anchor of each date is its London midnight."""
    for alias in ("c17", "c22", "c24"):
        _run(data, alias)
    frame = _silver(data, C)
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(C)
    assert "cost" not in frame.columns and "cost_per_gvas" not in frame.columns
    assert frame.schema["cost_gbp_per_gvas"] == pl.Int64
    for alias, fmt in (("c17", "%d/%m/%Y"), ("c22", "%Y-%m-%d"), ("c24", "%Y-%m-%d")):
        meta = CAPTURES[alias]
        source = rows(alias)
        part = frame.filter(pl.col("resource_id") == meta.resource_id)
        assert part["settlement_date"].to_list() == [
            datetime.strptime(r["Settlement Date"], fmt).date() for r in source
        ]
        value_column = "Cost" if alias == "c17" else "Cost_per_GVAs"
        assert part["cost_gbp_per_gvas"].to_list() == [int(r[value_column]) for r in source]
    assert 0 in frame["cost_gbp_per_gvas"].to_list()
    assert frame.select(["resource_id", "settlement_date"]).is_duplicated().sum() == 0
    london = ZoneInfo("Europe/London")
    assert frame["timestamp_utc"].to_list() == [
        datetime.combine(d, time(0), tzinfo=london).astimezone(UTC)
        for d in frame["settlement_date"].to_list()
    ]


def test_an_unlisted_cost_filename_fails_the_capture_with_no_fallback(data: Path) -> None:
    """Detects a guessed date format for a filename the record does not list: the 2017 body under
    ``inertia_costs21.csv`` fails the capture (no completion, no silver), because the per-filename
    map is exact."""
    capture_id = capture(data, "c17", filename="inertia_costs21.csv")
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, C, data).run(DAY, run_id="r")
    assert read_completion(data, C, capture_id) is None
    assert _no_silver(data, C)


def test_the_2026_inertia_cost_resource_is_never_transformed_or_cast(data: Path) -> None:
    """Detects the slash-dated 2026 file reaching the engine (a failed capture and a reconcile gap)
    or being cast under the shared ISO spelling: its header equals the record's second epoch (so
    only the resource-level HOLD keeps it out), its date fails the ISO cast, and with a clean
    resource captured beside it the clean one alone completes, C26 has no completion or failure,
    nothing of it is in silver and reconcile reports no gap."""
    registry = load_registry()
    assert header("c26") == list(_record(C).epochs[1].header)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pl.Series(["01/04/2026"]).str.strptime(pl.Date, "%Y-%m-%d", strict=True)

    c26_id = capture(data, "c26")
    c24_id = capture(data, "c24")
    written = get_transformer(SOURCE, C, data).run(DAY, run_id="r")
    assert written == len(rows("c24"))
    assert read_completion(data, C, c26_id) is None and read_failure(data, C, c26_id) is None
    completion = read_completion(data, C, c24_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, C)
    assert set(frame["resource_id"].to_list()) == {CAPTURES["c24"].resource_id}
    report = reconcile(data, registry, [C], DAY)
    assert report.gaps == (), report.lines()


@pytest.mark.parametrize("alias", ["h14", "h15", "h24"])
def test_historical_voltage_costs_type_each_files_own_month_spelling(
    data: Path, alias: str
) -> None:
    """Detects a month spelling guessed for the wrong file (2014-15 and 2024-25 are ``%d/%m/%Y``,
    the others ``%Y-%m-%d``), a cost changed, a group relabelled or a zero dropped: the slice loads
    every row with zero exclusions, the month equals the file's own spelling, the group labels
    (``Dumfries & Galloway``, ``E CORRIDOR``) and coordinates are the CSV text, both costs are
    float64 equal to the cells with zeros kept, the ``month`` anchor is the month's first London
    midnight, and the resource-partitioned key is unique."""
    meta = CAPTURES[alias]
    fmt = HISTORICAL_FORMATS[meta.filename]
    source = rows(alias)
    frame = _assert_clean_load(data, alias, len(source))
    assert frame["settlement_month"].to_list() == [
        datetime.strptime(r["Settlement Month"], fmt).date() for r in source
    ]
    assert frame["voltage_constraint_group"].to_list() == [
        r["Voltage Constraint Group"] for r in source
    ]
    assert "Dumfries & Galloway" in frame["voltage_constraint_group"].to_list()
    assert frame["coordinates"].to_list() == [r["Coordinates"] for r in source]
    assert frame.schema["sync_costs_gbp_m"] == pl.Float64
    assert frame["sync_costs_gbp_m"].to_list() == [float(r[H_HEADER[2]]) for r in source]
    assert frame["utilisation_costs_gbp_m"].to_list() == [float(r[H_HEADER[3]]) for r in source]
    assert 0.0 in frame["sync_costs_gbp_m"].to_list()
    assert set(frame["resource_id"].to_list()) == {meta.resource_id}
    london = ZoneInfo("Europe/London")
    assert frame["timestamp_utc"].to_list() == [
        datetime.combine(d, time(0), tzinfo=london).astimezone(UTC)
        for d in frame["settlement_month"].to_list()
    ]
    key = ["resource_id", "settlement_month", "voltage_constraint_group"]
    assert frame.select(key).is_duplicated().sum() == 0


def test_a_historical_body_under_the_other_filename_fails_loud(data: Path) -> None:
    """Detects the per-filename map being bypassed: the ISO-dated 2015-16 body published under the
    DMY 2014-15 filename cannot pass the strict ``%d/%m/%Y`` cast, so the capture fails (no
    completion, no silver) instead of guessing the order."""
    capture_id = capture(data, "h15", filename="voltagecsv-2014_15.csv")
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, H, data).run(DAY, run_id="r")
    assert read_completion(data, H, capture_id) is None
    assert _no_silver(data, H)


def test_main_voltage_costs_load_whole_and_do_not_overlap_the_historical_months(
    data: Path,
) -> None:
    """Detects the main resource sharing the historical key space, a partition invented for it, or
    a month mis-read: all 38 rows (two months over 19 groups) load with zero exclusions, the months
    are 2025-04-01 and 2025-05-01, there is no ``resource_id`` column (single resource), the key is
    unique, and no month overlaps the 2014-2025 historical ones."""
    source = rows("vm")
    frame = _assert_clean_load(data, "vm", len(source))
    assert len(source) == 38
    assert sorted(set(frame["settlement_month"].to_list())) == [date(2025, 4, 1), date(2025, 5, 1)]
    assert frame["voltage_constraint_group"].n_unique() == 19
    assert "resource_id" not in frame.columns
    assert frame.select(["settlement_month", "voltage_constraint_group"]).is_duplicated().sum() == 0
    assert frame["sync_costs_gbp_m"].to_list() == [float(r[H_HEADER[2]]) for r in source]
    for alias in ("h14", "h15", "h24"):
        months = {
            datetime.strptime(r["Settlement Month"], HISTORICAL_FORMATS[CAPTURES[alias].filename])
            for r in rows(alias)
        }
        assert not {m.date() for m in months} & set(frame["settlement_month"].to_list())


@pytest.mark.parametrize(
    ("alias", "needle", "wrong"),
    [
        ("v", b"Aug-26", b"August 2026"),
        ("m", b"04/10/2025 21:12", b"2025-10-04 21:12"),
        ("u23", b",No,,,\r\n", b",No,,,N/A\r\n"),
        ("i21", b"2021-04-01,1,175,121", b"2021-04-01,1,175.5,121"),
        ("c24", b"2024-04-06,2284", b"2024-04-06,2,284"),
        ("h15", b"0.076994607", b"N/A"),
    ],
)
def test_an_undocumented_token_is_never_read_as_null_or_repaired(
    data: Path, alias: str, needle: bytes, wrong: bytes
) -> None:
    """Detects a silent repair (a month name guessed, a date re-ordered, a decimal truncated, a
    thousands separator removed, ``N/A`` nulled): a cell the strict cast cannot read fails the
    capture loudly, with no completion and no silver."""
    raw = body(alias).replace(needle, wrong, 1)
    assert raw != body(alias)
    capture_id = capture(data, alias, raw=raw)
    meta = CAPTURES[alias]
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, meta.family, data).run(meta.day, run_id="r")
    assert read_completion(data, meta.family, capture_id) is None
    assert _no_silver(data, meta.family)


# --------------------------------------------------------------------------- #
# Reconcile: six Pathfinder captures are adjudicated
# --------------------------------------------------------------------------- #

LEDGER_FIELDS = {"category": "failed", "cause": "DuplicateEntityKeyError", "ruling": "642"}


def _entries(family: str) -> list[dict[str, Any]]:
    entries = json.loads((REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8"))
    return [e for e in entries if e["family"] == family]


def test_the_committed_ledger_adjudicates_exactly_the_six_pathfinder_captures() -> None:
    """Detects a Pathfinder capture left an open gap (reconcile red forever), an entry for a
    capture that does not repeat a row (a clean U23, U26 or A-capture hidden), a wider scope, a
    lost question or an entry the registry does not back: one ``failed`` /
    ``DuplicateEntityKeyError`` / ruling 642 entry per capture, the utilisation ones on the
    2024-25 and 2025-26 resources, the availability ones on all four, each a one-line reason and
    question naming the unit; they are the six entries before K-BAL-1's overlap entry; the
    registry backs them."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    util, avail = _entries(U), _entries(A)
    assert len(util) == 2 and len(avail) == 4
    registry = load_registry()
    for family, group, expected in (
        (U, util, PATHFINDER_UTILISATION_FAILED),
        (A, avail, PATHFINDER_AVAILABILITY_FAILED),
    ):
        resources = set()
        for entry in group:
            for field, value in LEDGER_FIELDS.items():
                assert entry[field] == value, field
            assert len(entry["captures"]) == 1
            (capture_path,) = entry["captures"]
            assert capture_path.startswith(f"bronze/neso_data_portal/{family}/2026/10/08/raw_")
            match = registry_module.CAPTURE_ID_PATTERN.fullmatch(capture_path)
            assert match is not None
            resources.add(match["rid"])
            assert registry.resources[match["rid"]][1].family == family
            for text in ("reason", "question", "evidence"):
                assert "\n" not in entry[text] and entry[text].strip()
            assert "never deduplicated" in entry["reason"]
            assert "v0.22-K-SYS-1" in entry["evidence"] and "ADR-040" in entry["evidence"]
        assert resources == expected, family
    assert [e.family for e in entries[-7:-1]] == [U, U, A, A, A, A]
    reasons = " ".join(e["reason"] for e in util + avail)
    for excess in ("3,964", "306", "1,003", "46,921", "7,679"):
        assert excess in reasons, excess


def test_pathfinder_failures_are_adjudicated_not_open_and_not_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a vendor-caused failure left as an open gap, an entry that no longer matches the
    failure record (stale), or an adjudication that alters data: without the ledger reconcile
    reports each failed capture; with the committed entries (their capture ids swapped for the
    fixture captures') every gap is adjudicated, none is open or stale, each ADJUDICATED line names
    ``DuplicateEntityKeyError`` and ruling 642, and the silver and state bytes are equal before and
    after."""
    install_generated(
        monkeypatch,
        data / "_registry",
        [_package_doc("stability-pathfinder-service-information.json")],
    )
    ids = {family: capture(data, alias) for alias, family in (("u25", U), ("a23", A))}
    for family in (U, A):
        with contextlib.suppress(NesoCaptureFailedError):
            get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    before = (_tree_bytes(data, "silver"), _tree_bytes(data, "state"))
    for family in (U, A):
        code, lines = run_cli(family, "--cutoff", DAY.isoformat())
        assert code == 1, lines
        assert any(line.startswith("GAP failed") and ids[family] in line for line in lines), lines
    entries = [
        {**_entries(family)[0], "captures": [ids[family]], "family": family} for family in (U, A)
    ]
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json(entries), encoding="utf-8"
    )
    for family in (U, A):
        code, lines = run_cli(family, "--cutoff", DAY.isoformat())
        assert code == 0, lines
        assert [line for line in lines if line.startswith("GAP")] == []
        assert "SUMMARY adjudicated 1" in lines and "SUMMARY stale_adjudication 0" in lines
        (adjudicated,) = [line for line in lines if ids[family] in line]
        assert adjudicated.startswith(f"ADJUDICATED failed {family}")
        assert "DuplicateEntityKeyError" in adjudicated and "ruling 642" in adjudicated
    assert (_tree_bytes(data, "silver"), _tree_bytes(data, "state")) == before


def _tree_bytes(data: Path, top: str) -> dict[str, bytes]:
    return {
        p.relative_to(data / top).as_posix(): p.read_bytes()
        for p in sorted((data / top).rglob("*"))
        if p.is_file()
    }


def test_no_other_system_family_has_a_ledger_entry() -> None:
    """Detects a stray entry for a family whose captures load (the voltage, midterm, inertia and
    cost families have no repeated key) or the cost HOLD dressed as an ADR-040 failure."""
    families = {e.family for e in registry_module.load_reconcile_adjudications()}
    assert not families & (set(FAMILIES) - {U, A})


# --------------------------------------------------------------------------- #
# Vintage, the catalogue and the generated pages
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ["v", "m", "u26", "a26", "i21", "c24", "h15", "vm"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy (RULINGS 529/597) or a catalogue view that cannot carry the new
    columns: ``available_at`` is the CKAN ``last_modified``, ``timestamp_utc`` follows the record's
    recipe (the capture time for the raw Pathfinder reports, the start instant for the midterm, the
    settlement period start for the inertia pair, the London midnight of the month or day
    otherwise), an as-of read before the vintage serves nothing even though the capture is later and
    one after serves it, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    capture_id, _ = _run(data, alias)
    frame = _silver(data, meta.family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    recipe = _record(meta.family).temporal
    london = ZoneInfo("Europe/London")
    if recipe.kind == "none":
        assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    elif recipe.kind == "utc_instant":
        assert recipe.column is not None
        assert frame["timestamp_utc"].to_list() == frame[recipe.column].to_list()
    elif recipe.kind == "sp_pair":
        assert recipe.date_column is not None and recipe.period_column is not None
        assert frame["timestamp_utc"].to_list() == [
            settlement_period_to_utc(d, p)
            for d, p in zip(frame[recipe.date_column], frame[recipe.period_column], strict=True)
        ]
    else:
        assert recipe.date_column is not None
        assert set(frame["timestamp_utc"].to_list()) == {
            datetime.combine(d, time(0), tzinfo=london).astimezone(UTC)
            for d in frame[recipe.date_column].to_list()
        }
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(days=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, meta.family, before) == []
    assert set(both_as_of(db, data, meta.family, after)) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}


def test_the_skeleton_pages_render_the_new_records() -> None:
    """Detects a record the docs generator cannot render (the held Pathfinder questions, the
    resource-level HOLD, the two cost epochs, the per-filename date maps)."""
    registry = load_registry()
    seen = set()
    for key, filename in PACKAGE_FILES.items():
        slug = filename.removesuffix(".json")
        page = skeleton.render_package(
            registry,
            {
                "name": slug,
                "title": slug,
                "organization": {"title": "NESO"},
                "license_title": "NESO Open Data Licence",
                "extras": [],
            },
            None,
        )
        assert f"`{key}`" in page, key
        seen.add(key)
    assert seen == set(FAMILIES)
