"""The constraint-management frozen records (v0.22-K-CON): seven records in six packages.

``constraint_limits_24m``, ``cmis_intertrip``, ``da_constraint_flows_limits``,
``otf_network_congestion`` and ``voltage_requirement`` are held (E-SEM);
``thermal_constraint_costs`` and its workbook child ``thermal_constraint_costs_xlsx`` are eligible.
Every test writes recorded fixture captures (cuts of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/constraint_mgmt/``, provenance in ``PROVENANCE.md``) into a short
data root and runs the transformer the **real package registry** generates, so a record that does
not fit its vendor body fails here, not at activation. On master none of the seven families has a
record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-CON-FACTS, K-CON-SPEC, RULINGS 632):

- Identifiers are stored as vendor spelled them; zeros, negatives, ``-1`` and ``99999`` are values;
  blanks are null; no undocumented token is read as null.
- Units live in the FACTS g4 citations (the record model has no unit field): every
  ``constraint_limits_24m`` boundary column and ``Limit_MW`` is MW; the CMIS utilisation cost, the
  thermal ``Daily Cost (GBP)`` and the older CMIS fee are GBP (the older fee per settlement period);
  ``Flow_MW`` has no declared unit (TODO); ``Units`` of the voltage requirement is a machine count,
  not MVAr.
- CMIS: two header epochs whose fee columns are two separate silver columns (never merged or
  converted); the arming and disarming times stay raw strings because their literal ``+00:00``
  offset conflicts with the documented GB local clock.
- Day-ahead flows and limits: the target timestamp is a raw string (two spellings, period endpoint
  and DST fold undocumented); the archive repeats keys, so the capture fails
  ``DuplicateEntityKeyError`` and ADR-040 adjudicates it as ``failed``; nothing is deduplicated.
- Thermal costs: the 2021-22 CSV carries comma-formatted costs, so that resource is a resource-level
  HOLD (not an ADR-040 failure); the workbooks' ``Data`` sheet is read by a sibling-fed ``xlsx``
  record whose date format is the spelling the installed calamine reader emits.
- Voltage requirement: ``Last Updated`` is an ordinary UTC column, never ``issue_time``; the 17
  reversed ``End Date`` ranges are kept as captured.
- ``year`` is a reserved silver name (a Hive partition), so the limits table's ``YEAR`` is
  ``year_vendor``. ``Last Updated`` is typed with the literal ``Z`` of every measured spelling
  (``%Y-%m-%dT%H:%MZ``, zone UTC): a numeric offset would fail the capture rather than be
  relabelled.

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the bronze
originals' CRLF convention; a BOM stays where the original had one."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import logging
import os
import tempfile
import zipfile
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
from gridflow.silver.neso_data_portal.readers import read_csv_body, read_xlsx_body
from gridflow.silver.neso_data_portal.reconcile import reconcile
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "constraint_mgmt"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)

HOLD_L = (
    "TODO: what week-numbering convention, week start/end, year-boundary handling and timezone "
    "do YEAR/Week No denote, and is each monthly upload a forecast issued before every target "
    "week? NESO's dictionary states neither."
)
HOLD_M = (
    "TODO: do the arming/disarming timestamps carry GB local wall time with a mechanically added "
    "+00:00 offset, or actual UTC instants? And is the newer arming fee GBP/MWh (header) or "
    "GBP/SP (dictionary)?"
)
HOLD_D = (
    "TODO: what period start/end and DST-fold treatment does the target timestamp denote, what are "
    "Flow_MW's unit and the meaning of blanks and 99999, and what row grain makes the archive "
    "unique? The archive has no issue column (RULINGS 529 fails: 732,332 targets precede the "
    "vintage)."
)
HOLD_O = (
    "TODO: is Date a week start, week end or reporting label (38 Saturdays, 15 Sundays, one 8-day "
    "step), how are actuals calculated, and when was each forecast issued? 26 targets are on or "
    "before the vintage, so the forward-target test fails."
)
HOLD_V = (
    "TODO: what is the overnight operating window and endpoint inclusivity of Start/End Date, what "
    "do the reversed ranges mean, which zone/members does each V_ group code cover, and is Last "
    "Updated an original issue time with the row available and immutable from then?"
)
HELD = {
    "constraint_limits_24m": HOLD_L,
    "cmis_intertrip": HOLD_M,
    "da_constraint_flows_limits": HOLD_D,
    "otf_network_congestion": HOLD_O,
    "voltage_requirement": HOLD_V,
}
ELIGIBLE = ("thermal_constraint_costs", "thermal_constraint_costs_xlsx")

BOUNDARIES = (
    "DRESHEX1",
    "ESTEX",
    "FLOWSTH",
    "GM_SNOW5A",
    "HARSPNBLY",
    "NKILGRMO",
    "SCOTEX",
    "SEIMPPR2",
    "SSE_GRM",
    "SSEN_S",
    "SSE_SP2",
    "SSHARN3",
)
CMIS_OLD = [
    "BMU ID",
    "Arming Date Time",
    "Disarming Date Time",
    "Current Arming Fee (£ / SP)",
    "Cost for this utilisation (£)",
]
CMIS_NEW = [
    "BMU ID",
    "Arming Date Time",
    "Disarming Date Time",
    "Current Arming Fee (£ / MWH)",
    "Cost for this utilisation (£)",
    "B6/EC5",
]
OTF_LABELS = (
    "Min of B4 B5",
    "B6",
    "B6a",
    "B7",
    "GMSNOW",
    "LE1",
    "B9",
    "DRESHEX",
    "EC5",
    "B15",
    "SC",
)
OTF_HEADER = ["Date"] + [f"{lab} - {role}" for role in ("Actual", "Forecast") for lab in OTF_LABELS]
THERMAL_HEADER = ["Settlement Date", "Constraint Group", "Daily Cost (GBP)"]
VOLTAGE_HEADER = ["Start Date", "End Date", "Group", "Units", "Notes", "Last Updated"]
D_HEADER = ["Constraint Group", "Date_ Time GMT_BST", "Limit_MW", "Flow_MW"]
L_HEADER = ["YEAR", "Week No", *BOUNDARIES]

CALAMINE_DATE = "%Y-%m-%d %H:%M:%S"
"""The spelling the installed calamine reader emits for the workbooks' Excel date cells."""


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    fixture: str
    family: str
    bronze: str
    package: str
    package_id: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str
    extension: str = "csv"
    ckan_format: str = "CSV"


CAPTURES: dict[str, Capture] = {
    "l": Capture(
        "l.csv",
        "constraint_limits_24m",
        "constraint_limits_24m",
        "24-months-ahead-constraint-limits",
        "d515b4a9-60a1-489c-a126-004efc04f121",
        "3c359e33-3dac-4bdd-87d1-efbf4cbc2f07",
        "24 Months ahead constraint limits ",
        "24-months-ahead-constraint-limit_sept26.csv",
        "2026-09-10T09:48:47.500911",
        "2026-10-08T09:15:45.347537+00:00",
    ),
    "m1": Capture(
        "m1.csv",
        "cmis_intertrip",
        "cmis_intertrip",
        "constraint-management-intertrip-service-information-cmis",
        "7c20761d-3aab-4e9f-926e-6117fa8c4524",
        "60b4055c-d87e-4ebe-8d21-e023f506e461",
        "Constraint Management Intertrip Arming 2022-2023",
        "cmp-management-intertrip-arming-2022-2023.csv",
        "2024-03-12T14:16:03.129176",
        "2026-10-08T09:14:57.004056+00:00",
    ),
    "m3": Capture(
        "m3.csv",
        "cmis_intertrip",
        "cmis_intertrip",
        "constraint-management-intertrip-service-information-cmis",
        "7c20761d-3aab-4e9f-926e-6117fa8c4524",
        "2f0777a3-719c-4ffe-96c2-61117a5ec468",
        "Constraint Management Intertrip Arming 2024-2025",
        "cmp-intertrip-arming-2024-2025.csv",
        "2025-05-06T10:00:06.772385",
        "2026-10-08T09:15:02.989951+00:00",
    ),
    "m4": Capture(
        "m4.csv",
        "cmis_intertrip",
        "cmis_intertrip",
        "constraint-management-intertrip-service-information-cmis",
        "7c20761d-3aab-4e9f-926e-6117fa8c4524",
        "c6f0c279-87c9-4123-a4d1-b2d6d98b43a7",
        "Constraint Management Intertrip Arming 2025-2026",
        "cmp-intertrip-arming-2025-2026.csv",
        "2026-04-28T16:25:43.722522",
        "2026-10-08T09:15:05.121807+00:00",
    ),
    "d": Capture(
        "d_clean.csv",
        "da_constraint_flows_limits",
        "da_constraint_flows_limits",
        "day-ahead-constraint-flows-and-limits",
        "cf3cbc92-2d5d-4c2b-bd29-e11a21070b26",
        "38a18ec1-9e40-465d-93fb-301e80fd1352",
        "Day Ahead Constraint Flows and Limits",
        "day-ahead-constraints-limits-and-flow-output-v1.5.csv",
        "2026-10-07T17:28:41.804546",
        "2026-10-08T09:16:59.251115+00:00",
    ),
    "dc": Capture(
        "d_collide.csv",
        "da_constraint_flows_limits",
        "da_constraint_flows_limits",
        "day-ahead-constraint-flows-and-limits",
        "cf3cbc92-2d5d-4c2b-bd29-e11a21070b26",
        "38a18ec1-9e40-465d-93fb-301e80fd1352",
        "Day Ahead Constraint Flows and Limits",
        "day-ahead-constraints-limits-and-flow-output-v1.5.csv",
        "2026-10-07T17:28:41.804546",
        "2026-10-08T09:16:59.251115+00:00",
    ),
    "o": Capture(
        "o.csv",
        "otf_network_congestion",
        "otf_network_congestion",
        "operational-transparency-forum-network-congestion-data",
        "a30dacc7-af6e-465b-ad96-eb2383376ac9",
        "aa9d4303-b7ec-4881-be07-16bad8824ab6",
        "Network congestion Forecast and Actual data",
        "otf_constraint_data_07-10-2026.csv",
        "2026-10-07T10:43:02.910511",
        "2026-10-08T11:16:09.107077+00:00",
    ),
    "t1": Capture(
        "t1.csv",
        "thermal_constraint_costs",
        "thermal_constraint_costs",
        "thermal-constraint-costs",
        "f0055054-c55c-4068-a01c-61da4334e58f",
        "4357dd3b-5c7a-4caa-8d1a-8cf848521143",
        "Thermal Constraint Costs Data 21-22",
        "outturn-system-costs-2021-2022.csv",
        "2022-04-08T15:22:06.203234",
        "2026-10-08T11:38:31.219010+00:00",
    ),
    "t2": Capture(
        "t2.csv",
        "thermal_constraint_costs",
        "thermal_constraint_costs",
        "thermal-constraint-costs",
        "f0055054-c55c-4068-a01c-61da4334e58f",
        "476b8d39-5eda-425c-9756-73ddfd36dc4d",
        "Thermal Constraint Costs Data 22-23",
        "outturn-system-costs-2022-2023.csv",
        "2023-04-17T16:03:55.101783",
        "2026-10-08T11:38:34.163169+00:00",
    ),
    "x1": Capture(
        "x1.xlsx",
        "thermal_constraint_costs_xlsx",
        "thermal_constraint_costs_files",
        "thermal-constraint-costs",
        "f0055054-c55c-4068-a01c-61da4334e58f",
        "d195f1d8-7d9e-46f1-96a6-4251e75e9bd0",
        "Thermal Constraint Costs 19-20",
        "map-of-outturn-system-costs-19-20.xlsx",
        "2020-06-12T14:41:30.987252",
        "2026-10-08T11:38:49.949763+00:00",
        "xlsx",
        "XLSX",
    ),
    "v": Capture(
        "v.csv",
        "voltage_requirement",
        "voltage_requirement",
        "voltage-requirement",
        "9f4acccb-bf79-452b-aa77-a680ae728722",
        "00881643-7a5e-4eed-b144-06423a88202b",
        "Week Ahead Overnight Voltage Requirement 20-26",
        "overnightvoltagerequirement20-26_1.csv",
        "2026-10-02T13:22:22.101474",
        "2026-10-08T11:41:05.238883+00:00",
    ),
}
PACKAGE_FILES = {
    "constraint_limits_24m": "24-months-ahead-constraint-limits.json",
    "cmis_intertrip": "constraint-management-intertrip-service-information-cmis.json",
    "da_constraint_flows_limits": "day-ahead-constraint-flows-and-limits.json",
    "otf_network_congestion": "operational-transparency-forum-network-congestion-data.json",
    "thermal_constraint_costs": "thermal-constraint-costs.json",
    "thermal_constraint_costs_xlsx": "thermal-constraint-costs.json",
    "voltage_requirement": "voltage-requirement.json",
}
FAMILIES = tuple(PACKAGE_FILES)


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings (and so the CLI) point at."""
    with tempfile.TemporaryDirectory(
        prefix="con", dir=_short_base(), ignore_cleanup_errors=True
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
    """The fixture's records as text, header-keyed (a wholly blank record is skipped)."""
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
    written: datetime | None = None,
    filename: str | None = None,
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    payload = (
        raw
        if raw is not None
        else ((FIXTURES / meta.fixture).read_bytes() if meta.extension == "xlsx" else body(alias))
    )
    path, _sidecar = write_capture(
        data,
        meta.bronze,
        body=payload,
        written_at=written or datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=meta.package,
        package_id=meta.package_id,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=filename or meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
        ckan_format=meta.ckan_format,
        extension=meta.extension,
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
    written = get_transformer(SOURCE, CAPTURES[alias].family, data).run(DAY, run_id="r")
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
    completion = read_completion(
        data, meta.bronze if meta.extension == "csv" else meta.family, capture_id
    )
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
    frame = _silver(data, meta.family)
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(meta.family)
    return frame


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header
    (incl. the ``£`` of the CMIS fee and cost columns, the older ``£ / SP`` and newer ``£ / MWH``
    epochs, the ``Date_ Time GMT_BST`` spelling and the 22 OTF value labels), CRLF endings, the
    limits file's BOM, the year boundaries, CMIS zero cost and its wholly blank record, both
    day-ahead timestamp spellings, the blank / ``99999`` / ``-1`` / negative cells, the whole-row
    collision (ERROEX 2025-08-12T00:00:00) and a differing-value collision (the fold hour), the OTF
    blank actuals, the quoted comma cost of the 2021-22 thermal file, a negative and zero cost, and
    the voltage reversed range, repeated (start, group) pair, note text and ``Z`` update times."""
    assert header("l") == L_HEADER and body("l").startswith(b"\xef\xbb\xbf")
    assert header("m1") == CMIS_OLD and header("m3") == header("m4") == CMIS_NEW
    assert header("d") == header("dc") == D_HEADER and body("d").startswith(b"\xef\xbb\xbf")
    assert header("o") == OTF_HEADER and len(OTF_HEADER) == 23
    assert header("t1") == header("t2") == THERMAL_HEADER
    assert header("v") == VOLTAGE_HEADER
    for alias in ("l", "m1", "m3", "m4", "d", "dc", "o", "t1", "t2", "v"):
        assert body(alias).count(b"\r\n") == body(alias).count(b"\n"), alias
    assert {(r["YEAR"], r["Week No"]) for r in rows("l")} >= {
        ("2026", "52"),
        ("2027", "1"),
        ("2027", "2"),
        ("2028", "1"),
        ("2028", "40"),
    }
    assert body("m3").endswith(b"\r\n\r\n")
    assert any(float(r["Cost for this utilisation (£)"]) == 0 for r in rows("m3") + rows("m4"))
    assert {r["B6/EC5"] for r in rows("m3") + rows("m4")} == {"B6", "EC5"}
    assert all(r["Arming Date Time"].endswith("+00:00") for r in rows("m1") + rows("m4"))
    stamps = [r["Date_ Time GMT_BST"] for r in rows("d")]
    assert {len(s) for s in stamps} == {16, 19}
    limit = [r["Limit_MW"] for r in rows("d")]
    flow = [r["Flow_MW"] for r in rows("d")]
    assert "" in limit and "" in flow and "99999" in limit and "-1" in flow
    assert any(f.startswith("-") and f != "-1" for f in flow)
    collide = rows("dc")
    erroex = [r for r in collide if r["Date_ Time GMT_BST"] == "2025-08-12T00:00:00"]
    assert len(erroex) == 2 and erroex[0] == erroex[1]
    fold = [r for r in collide if r["Date_ Time GMT_BST"] == "2024-10-27T01:00:00"]
    assert len(fold) == 2 and fold[0]["Flow_MW"] != fold[1]["Flow_MW"]
    actuals = [c for c in OTF_HEADER if c.endswith("- Actual")]
    blank = [r for r in rows("o") if all(r[c] == "" for c in actuals)]
    assert blank and any(all(r[c] != "" for c in actuals) for r in rows("o"))
    assert all(r[c] != "" for r in rows("o") for c in OTF_HEADER if c.endswith("- Forecast"))
    assert b'"2,150,551"' in body("t1")
    costs = [int(r["Daily Cost (GBP)"]) for r in rows("t2")]
    assert 0 in costs and any(c < 0 for c in costs)
    voltage = rows("v")
    assert [r for r in voltage if r["End Date"] < r["Start Date"]]
    pair = [r for r in voltage if (r["Start Date"], r["Group"]) == ("2025-08-19", "V_North")]
    assert len(pair) == 2 and pair[0]["Last Updated"] != pair[1]["Last Updated"]
    assert any(r["Notes"] for r in voltage)
    assert all(r["Last Updated"].endswith("Z") for r in voltage)


def test_the_workbook_fixture_keeps_the_five_sheets_and_the_real_data_header() -> None:
    """Detects a workbook cut that the reader cannot inventory (P-5 demands the registry's five
    sheets) or whose Data sheet lost its vendor header: the zip is a sound workbook with the five
    sheet names, and its Data sheet is cut to the header and 21 data rows (22 populated rows)."""
    with zipfile.ZipFile(FIXTURES / "x1.xlsx") as archive:
        assert archive.testzip() is None
        workbook = archive.read("xl/workbook.xml").decode("utf-8")
        sheet = archive.read("xl/worksheets/sheet1.xml").decode("utf-8")
    for name in ("Data", "Map", "Network Diagram E&amp;W", "Network Diagram Scot", "Dates"):
        assert f'<sheet name="{name}"' in workbook, name
    assert '<dimension ref="A1:C22"/>' in sheet and sheet.count("<row ") == 22


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", FAMILIES)
def test_every_record_is_csv_or_xlsx_utf8_version_1_with_ckan_last_modified_and_no_issue(
    key: str,
) -> None:
    """Detects a record that invents an issue time or a fallback vintage: every family's record is
    version 1, utf-8, ``ckan_last_modified`` vintage (all captures are uploads), whole-capture
    selection, and no epoch declares an issue recipe (``issue_time`` is never emitted)."""
    record = _record(key)
    assert (record.version, record.encoding) == ("1", "utf-8")
    assert record.reader == ("xlsx" if key.endswith("_xlsx") else "csv")
    assert (record.vintage, record.vintage_evidence) == ("ckan_last_modified", None)
    assert record.latest == "whole_capture"
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key
    assert "issue_time" not in _columns(key)


def test_constraint_limits_record() -> None:
    """Detects ``YEAR`` typed into a reserved or date name, a date invented from year and week (no
    week calendar is evidenced), or a boundary code renamed: ``year_vendor`` and ``week_no`` are
    non-nullable int64 kept raw, the 12 boundary columns are nullable int64 named by their
    lower-cased vendor code (MW), temporal is ``none``, the key is the pair, selection is
    family-scope."""
    record = _record("constraint_limits_24m")
    (epoch,) = record.epochs
    assert list(epoch.header) == L_HEADER
    assert [(c.source, c.name, c.dtype, c.nullable) for c in epoch.columns[:2]] == [
        ("YEAR", "year_vendor", "int64", False),
        ("Week No", "week_no", "int64", False),
    ]
    assert [(c.source, c.name, c.dtype, c.nullable) for c in epoch.columns[2:]] == [
        (b, b.lower(), "int64", True) for b in BOUNDARIES
    ]
    assert all(c.format is None and not c.null_tokens for c in epoch.columns)
    assert all(c.min is None and c.max is None for c in epoch.columns)
    assert record.temporal.kind == "none"
    assert record.entity_key == ("year_vendor", "week_no")
    assert record.latest_partition is None and record.siblings == ()
    assert "year" not in {c.name for c in epoch.columns}


def test_cmis_record_keeps_two_epochs_and_two_fee_columns() -> None:
    """Detects the two fee columns merged, converted or one epoch lost, or the event times parsed:
    two epochs (``£ / SP`` for M1/M2, ``£ / MWH`` plus ``B6/EC5`` for M3-M5), the fees two separate
    nullable float64 silver names (GBP/SP and GBP/MWh, never in one epoch together), the arming and
    disarming times raw strings, the cost GBP, the key resource-partitioned."""
    record = _record("cmis_intertrip")
    old, new = record.epochs
    assert list(old.header) == CMIS_OLD and list(new.header) == CMIS_NEW
    old_names = {c.source: c for c in old.columns}
    new_names = {c.source: c for c in new.columns}
    assert old_names["Current Arming Fee (£ / SP)"].name == "current_arming_fee_gbp_per_sp"
    assert new_names["Current Arming Fee (£ / MWH)"].name == "current_arming_fee_gbp_per_mwh"
    for column in (
        old_names["Current Arming Fee (£ / SP)"],
        new_names["Current Arming Fee (£ / MWH)"],
    ):
        assert (column.dtype, column.nullable, column.format) == ("float64", True, None)
    assert "current_arming_fee_gbp_per_mwh" not in {c.name for c in old.columns}
    assert "current_arming_fee_gbp_per_sp" not in {c.name for c in new.columns}
    for epoch_names in (old_names, new_names):
        for source, name, nullable in (
            ("BMU ID", "bmu_id", False),
            ("Arming Date Time", "arming_date_time", False),
            ("Disarming Date Time", "disarming_date_time", True),
        ):
            column = epoch_names[source]
            assert (column.name, column.dtype, column.nullable, column.format) == (
                name,
                "string",
                nullable,
                None,
            )
        cost = epoch_names["Cost for this utilisation (£)"]
        assert (cost.name, cost.dtype, cost.nullable) == ("utilisation_cost_gbp", "float64", True)
    assert (new_names["B6/EC5"].name, new_names["B6/EC5"].dtype) == ("b6_ec5", "string")
    assert "B6/EC5" not in old_names
    assert record.temporal.kind == "none"
    assert record.entity_key == ("resource_id", "bmu_id", "arming_date_time")
    assert (record.latest, record.latest_partition) == ("whole_capture", "resource_id")


def test_day_ahead_record_keeps_the_target_timestamp_raw() -> None:
    """Detects a timestamp recipe invented for the target (period endpoint and DST fold are
    undocumented) or a value column in the key: the timestamp is a raw non-nullable string, limit
    and flow nullable int64, temporal ``none``, key ``(constraint_group, raw timestamp)``, no
    resource partition and no ordinal."""
    record = _record("da_constraint_flows_limits")
    (epoch,) = record.epochs
    assert list(epoch.header) == D_HEADER
    assert [(c.name, c.dtype, c.nullable, c.format) for c in epoch.columns] == [
        ("constraint_group", "string", False, None),
        ("date_time_gmt_bst_raw", "string", False, None),
        ("limit_mw", "int64", True, None),
        ("flow_mw", "int64", True, None),
    ]
    assert not any(c.null_tokens or c.min is not None or c.max is not None for c in epoch.columns)
    assert record.temporal.kind == "none"
    assert record.entity_key == ("constraint_group", "date_time_gmt_bst_raw")
    assert record.latest_partition is None


def test_otf_record() -> None:
    """Detects a label renamed, an actual made non-nullable (26 rows would be excluded) or a week
    meaning invented: ``Date`` is a non-nullable ``%Y-%m-%d`` date and the engine's ``date_sp1``
    anchor only, the 22 value columns are nullable float64 named ``<label>_<actual|forecast>`` in
    vendor order, the key is ``(date,)``."""
    record = _record("otf_network_congestion")
    (epoch,) = record.epochs
    assert list(epoch.header) == OTF_HEADER
    date_column, *values = epoch.columns
    assert (date_column.name, date_column.dtype, date_column.format, date_column.nullable) == (
        "date",
        "date",
        "%Y-%m-%d",
        False,
    )
    assert [(c.source, c.dtype, c.nullable) for c in values] == [
        (source, "float64", True) for source in OTF_HEADER[1:]
    ]
    assert [c.name for c in values][:2] == ["min_of_b4_b5_actual", "b6_actual"]
    assert values[-1].name == "sc_forecast" and values[11].name == "min_of_b4_b5_forecast"
    assert len({c.name for c in values}) == 22
    assert (record.temporal.kind, record.temporal.date_column) == ("date_sp1", "date")
    assert record.entity_key == ("date",)
    assert record.latest_partition is None


def test_thermal_csv_and_workbook_records_share_columns_but_not_the_date_spelling() -> None:
    """Detects a cost typed as float (the vendor rounds to integers), a lost resource partition, or
    a date format the reader does not emit: the CSV record reads ``%Y-%m-%d``, the workbook record
    ``%Y-%m-%d %H:%M:%S`` (calamine's spelling), both ``date_sp1(settlement_date)``, int64 GBP cost,
    key ``(resource_id, settlement_date, constraint_group)`` per resource; the workbook record is a
    sibling-fed ``xlsx`` record on header row 1, columns ``A:C``, no ``last_row`` (X1 and X2
    differ)."""
    csv_record, xlsx_record = (
        _record("thermal_constraint_costs"),
        _record("thermal_constraint_costs_xlsx"),
    )
    for record, fmt in ((csv_record, "%Y-%m-%d"), (xlsx_record, CALAMINE_DATE)):
        (epoch,) = record.epochs
        assert list(epoch.header) == THERMAL_HEADER
        assert [(c.name, c.dtype, c.format, c.nullable) for c in epoch.columns] == [
            ("settlement_date", "date", fmt, False),
            ("constraint_group", "string", None, False),
            ("daily_cost_gbp", "int64", None, True),
        ]
        assert (record.temporal.kind, record.temporal.date_column) == (
            "date_sp1",
            "settlement_date",
        )
        assert record.entity_key == ("resource_id", "settlement_date", "constraint_group")
        assert (record.latest, record.latest_partition) == ("whole_capture", "resource_id")
    assert csv_record.xlsx is None and csv_record.siblings == ()
    assert xlsx_record.xlsx is not None
    assert (xlsx_record.xlsx.header_row, xlsx_record.xlsx.columns, xlsx_record.xlsx.last_row) == (
        1,
        "A:C",
        None,
    )
    assert xlsx_record.siblings == ("thermal_constraint_costs_files",)


def test_voltage_record() -> None:
    """Detects ``Last Updated`` promoted to an issue time, parsed with a numeric offset it never
    has, or the machine count read as MVAr: ``Last Updated`` is an ordinary non-nullable UTC
    datetime (literal ``Z``), ``Units`` a nullable int64 count, ``Notes`` a nullable string,
    ``Group`` a string kept as-is, both dates ``%Y-%m-%d`` (``End Date`` not the temporal input),
    temporal ``date_sp1(start_date)``, key ``(start_date, group, last_updated)``, no ``issue_time``
    anywhere."""
    record = _record("voltage_requirement")
    (epoch,) = record.epochs
    assert list(epoch.header) == VOLTAGE_HEADER
    by_name = {c.name: c for c in epoch.columns}
    assert [c.name for c in epoch.columns] == [
        "start_date",
        "end_date",
        "group",
        "units",
        "notes",
        "last_updated",
    ]
    assert (by_name["start_date"].dtype, by_name["start_date"].nullable) == ("date", False)
    assert (by_name["end_date"].dtype, by_name["end_date"].format) == ("date", "%Y-%m-%d")
    assert (by_name["group"].dtype, by_name["units"].dtype) == ("string", "int64")
    assert (by_name["notes"].dtype, by_name["notes"].nullable) == ("string", True)
    updated = by_name["last_updated"]
    assert (updated.dtype, updated.format, updated.zone, updated.nullable) == (
        "datetime",
        "%Y-%m-%dT%H:%MZ",
        "UTC",
        False,
    )
    assert epoch.issue.kind == "none" and record.vintage == "ckan_last_modified"
    assert (record.temporal.kind, record.temporal.date_column) == ("date_sp1", "start_date")
    assert record.entity_key == ("start_date", "group", "last_updated")


@pytest.mark.parametrize("key", list(HELD))
def test_held_exactly_as_ruled_and_the_package_stays_eligible(key: str) -> None:
    """Detects a hold lost, reworded or moved to the package, or an unlisted question: the five
    held families carry the spec's verbatim E-SEM question, are effectively held, and the package
    itself stays eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == ("E-SEM", HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", ELIGIBLE)
def test_thermal_families_are_eligible(key: str) -> None:
    """Detects a hold wrongly put on the thermal costs: neither family carries a record-level
    eligibility, so each is effectively eligible (T1 is a resource-level HOLD, not a family one)."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_each_family_is_in_the_package_file_the_spec_names() -> None:
    """Detects a record in the wrong package file, a family split, a held PDF routed to silver or
    the tabular resources' dispositions drifting: each file carries exactly its record(s); the PDFs
    and PNGs stay DOC; every CSV resource of a record family is SILVER of that family, except the
    2021-22 thermal file (HOLD, E-SEM)."""
    registry = load_registry()
    by_file: dict[str, set[str]] = {}
    for key, filename in PACKAGE_FILES.items():
        by_file.setdefault(filename, set()).add(key)
    for filename, keys in by_file.items():
        document = _package_doc(filename)
        assert {f["key"] for f in document["families"] if "record" in f} == keys, filename
        for resource in document["resources"]:
            family = resource["family"]
            if family.endswith("_files"):
                kind = resource["disposition"]["kind"]
                if resource["format"] == "XLSX":
                    assert kind == "SILVER"
                else:
                    assert resource["disposition"] == {"kind": "DOC"}, resource["name"]
            elif resource["id"] == CAPTURES["t1"].resource_id:
                assert resource["disposition"]["kind"] == "HOLD"
            else:
                assert resource["disposition"] == {"kind": "SILVER", "key": family}
    for alias, meta in CAPTURES.items():
        resource = registry.resources[meta.resource_id][1]
        expected = "thermal_constraint_costs_files" if alias == "x1" else meta.family
        assert resource.family == expected, alias
    cmis = [r for _p, r in registry.resources.values() if r.family == "cmis_intertrip"]
    assert len(cmis) == 5
    thermal = [r for _p, r in registry.resources.values() if r.family == "thermal_constraint_costs"]
    assert len(thermal) == 6


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


def test_limits_fixture_types_with_no_exclusion_and_keeps_every_cell(data: Path) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the BOM), a
    cast the body does not satisfy, a row excluded and a value changed: the capture completes with
    every row, zero exclusions, ``year_vendor`` / ``week_no`` / the 12 boundaries int64 and equal to
    their CSV cells in order, no date column and the engine's ``timestamp_utc`` the capture time."""
    meta = CAPTURES["l"]
    source = rows("l")
    frame = _assert_clean_load(data, "l", len(source))
    assert frame.schema["year_vendor"] == pl.Int64 and frame.schema["week_no"] == pl.Int64
    assert "year" not in frame.columns or frame.schema["year"] != pl.Int64
    assert frame["year_vendor"].to_list() == [int(r["YEAR"]) for r in source]
    assert frame["week_no"].to_list() == [int(r["Week No"]) for r in source]
    for code in BOUNDARIES:
        assert frame.schema[code.lower()] == pl.Int64, code
        assert frame[code.lower()].to_list() == [int(r[code]) for r in source], code
    assert frame.select(["year_vendor", "week_no"]).is_duplicated().sum() == 0
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}


@pytest.mark.parametrize("alias", ["m1", "m3", "m4"])
def test_cmis_fixture_types_and_keeps_the_raw_strings_and_zeros(
    data: Path, alias: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects a header matching no epoch (the older ``£ / SP`` or newer ``£ / MWH`` + ``B6/EC5``),
    a timestamp parsed, stripped or relabelled (the literal ``+00:00`` stays), a zero cost dropped
    or read as null, the blank record kept as data or dropped without a log: every populated row
    loads with zero exclusions, BMU ids and both times are the CSV text, the cost is the float and a
    zero stays ``0.0``, and the M3 trailing wholly blank record is dropped by the reader's logged
    path."""
    source = rows(alias)
    with caplog.at_level(logging.INFO, logger="gridflow.silver.csv_bronze"):
        frame = _assert_clean_load(data, alias, len(source))
    logged = [r.getMessage() for r in caplog.records if "blank row" in r.getMessage()]
    assert len(logged) == (1 if alias == "m3" else 0)
    assert all("dropped 1 blank row(s)" in message for message in logged)
    assert frame["bmu_id"].to_list() == [r["BMU ID"] for r in source]
    assert frame["arming_date_time"].to_list() == [r["Arming Date Time"] for r in source]
    assert frame["disarming_date_time"].to_list() == [r["Disarming Date Time"] for r in source]
    assert all(v.endswith("+00:00") for v in frame["arming_date_time"].to_list())
    assert frame.schema["arming_date_time"] == pl.Utf8
    assert frame["utilisation_cost_gbp"].to_list() == [
        float(r["Cost for this utilisation (£)"]) for r in source
    ]
    if alias != "m1":
        assert 0.0 in frame["utilisation_cost_gbp"].to_list()
    assert frame.select(["resource_id", "bmu_id", "arming_date_time"]).is_duplicated().sum() == 0


def test_cmis_epochs_land_in_separate_fee_columns_never_merged(data: Path) -> None:
    """Detects the two fee columns merged into one (a GBP/SP and a GBP/MWh figure would sit
    together) or converted: loading an older and a newer resource, the SP column is filled only for
    the older rows and null for the newer, the MWh column the reverse, each equal to its CSV cell;
    ``b6_ec5`` is null for the older epoch and the vendor code for the newer."""
    for alias in ("m1", "m4"):
        _run(data, alias)
    frame = _silver(data, "cmis_intertrip")
    old = frame.filter(pl.col("resource_id") == CAPTURES["m1"].resource_id)
    new = frame.filter(pl.col("resource_id") == CAPTURES["m4"].resource_id)
    assert old.height == len(rows("m1")) and new.height == len(rows("m4"))
    assert old["current_arming_fee_gbp_per_sp"].to_list() == [
        float(r["Current Arming Fee (£ / SP)"]) for r in rows("m1")
    ]
    assert old["current_arming_fee_gbp_per_mwh"].null_count() == old.height
    assert new["current_arming_fee_gbp_per_mwh"].to_list() == [
        float(r["Current Arming Fee (£ / MWH)"]) for r in rows("m4")
    ]
    assert new["current_arming_fee_gbp_per_sp"].null_count() == new.height
    assert old["b6_ec5"].null_count() == old.height
    assert new["b6_ec5"].to_list() == [r["B6/EC5"] for r in rows("m4")]
    assert {"current_arming_fee_gbp_per_sp", "current_arming_fee_gbp_per_mwh"} <= set(frame.columns)


def test_cmis_resources_are_separate_partitions(data: Path) -> None:
    """Detects family-wide newest-capture selection (one annual resource displacing the others):
    two resources of the family are both served by ``_latest`` (catalogue = Polars), and a later
    re-capture of one replaces only that resource."""
    first, second = (capture(data, a) for a in ("m1", "m4"))
    get_transformer(SOURCE, "cmis_intertrip", data).run(DAY, run_id="r")
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, "cmis_intertrip", None)) == {first, second}
    later = capture(data, "m1", written=datetime(2026, 10, 8, 18, 0, tzinfo=UTC))
    get_transformer(SOURCE, "cmis_intertrip", data).run(DAY, run_id="r2")
    assert set(both_as_of(db, data, "cmis_intertrip", None)) == {later, second}


def test_day_ahead_clean_slice_types_and_keeps_raw_timestamps_and_special_values(
    data: Path,
) -> None:
    """Detects a timestamp parsed, re-spelled or fold-resolved, or a special value nulled: the clean
    slice (no repeated key) loads every row with zero exclusions, both spellings (``...T00:00`` and
    ``...T00:00:00``) are the CSV text, a blank limit and flow are null, ``99999``, ``-1`` and a
    negative flow are values."""
    source = rows("d")
    frame = _assert_clean_load(data, "d", len(source))
    assert frame["constraint_group"].to_list() == [r["Constraint Group"] for r in source]
    assert frame["date_time_gmt_bst_raw"].to_list() == [r["Date_ Time GMT_BST"] for r in source]
    assert {len(s) for s in frame["date_time_gmt_bst_raw"].to_list()} == {16, 19}
    assert frame.schema["limit_mw"] == pl.Int64 and frame.schema["flow_mw"] == pl.Int64
    assert frame["limit_mw"].to_list() == [
        int(r["Limit_MW"]) if r["Limit_MW"] else None for r in source
    ]
    assert frame["flow_mw"].to_list() == [
        int(r["Flow_MW"]) if r["Flow_MW"] else None for r in source
    ]
    assert 99999 in frame["limit_mw"].to_list() and -1 in frame["flow_mw"].to_list()
    assert any(v < -1 for v in frame["flow_mw"].drop_nulls().to_list())
    assert frame["limit_mw"].null_count() == 1 and frame["flow_mw"].null_count() == 1
    assert frame.select(["constraint_group", "date_time_gmt_bst_raw"]).is_duplicated().sum() == 0


def test_day_ahead_repeated_keys_fail_the_capture_and_are_never_deduplicated(data: Path) -> None:
    """Detects a repeated key deduplicated, absorbed into the key by its value, or an ordinal key
    invented: the body with the whole-row repeat (ERROEX 2025-08-12T00:00:00) and the
    differing-value repeat (the fold hour) fails with ``DuplicateEntityKeyError``, leaves no
    completion and no silver, and the same body without the repeats loads."""
    capture_id = capture(data, "dc")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, "da_constraint_flows_limits", data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    failure = read_failure(data, "da_constraint_flows_limits", capture_id)
    assert failure is not None and failure["error_class"] == DuplicateEntityKeyError.__name__
    assert read_completion(data, "da_constraint_flows_limits", capture_id) is None
    assert _no_silver(data, "da_constraint_flows_limits")


def test_otf_fixture_types_and_keeps_blank_actuals_null(data: Path) -> None:
    """Detects a blank actual dropped, read as zero or excluding its row, or a date mis-parsed:
    every row loads with zero exclusions, ``date`` is the ``%Y-%m-%d`` date, the 22 values are
    float64 equal to their cells, the blank actuals are null while every forecast is populated, and
    the engine's date anchor (``timestamp_utc``) is the capture time."""
    source = rows("o")
    frame = _assert_clean_load(data, "o", len(source))
    assert frame.schema["date"] == pl.Date
    assert frame["date"].to_list() == [date.fromisoformat(r["Date"]) for r in source]
    record = _record("otf_network_congestion")
    for spec in record.epochs[0].columns[1:]:
        assert frame.schema[spec.name] == pl.Float64, spec.name
        assert frame[spec.name].to_list() == [
            float(r[spec.source]) if r[spec.source] else None for r in source
        ], spec.name
    blank = sum(1 for r in source if r["B6 - Actual"] == "")
    assert blank >= 4
    assert frame["b6_actual"].null_count() == blank
    assert frame["b6_forecast"].null_count() == 0
    assert frame.select("date").is_duplicated().sum() == 0


def test_thermal_csv_fixture_types_and_keeps_zeros_and_negatives(data: Path) -> None:
    """Detects a zero or negative cost dropped, a cost cast as float or a group renamed: the 2022-23
    slice loads every row with zero exclusions, ``settlement_date`` is the date, ``daily_cost_gbp``
    int64 equal to the cell (a zero and a negative stay), the group spelling (``SSE-SP`` with a
    hyphen) is untouched, and the key is unique per resource."""
    source = rows("t2")
    frame = _assert_clean_load(data, "t2", len(source))
    assert frame["settlement_date"].to_list() == [
        date.fromisoformat(r["Settlement Date"]) for r in source
    ]
    assert frame.schema["daily_cost_gbp"] == pl.Int64
    assert frame["daily_cost_gbp"].to_list() == [int(r["Daily Cost (GBP)"]) for r in source]
    assert 0 in frame["daily_cost_gbp"].to_list() and min(frame["daily_cost_gbp"].to_list()) < 0
    assert "SSE-SP" in frame["constraint_group"].to_list()
    assert frame["timestamp_utc"].null_count() == 0
    key = ["resource_id", "settlement_date", "constraint_group"]
    assert frame.select(key).is_duplicated().sum() == 0


def test_the_2021_22_thermal_resource_is_a_hold_that_is_never_transformed_or_cast(
    data: Path,
) -> None:
    """Detects the comma-formatted file reaching the engine (a failed capture and a reconcile gap),
    cast with commas removed, or the hold being only an accident of the header: the T1 body's header
    equals the record's epoch (so only the resource-level HOLD keeps it out), its comma costs fail
    the strict int64 cast, the HOLD names the lexical numbers, and with a clean resource captured
    beside it the clean one alone completes, T1 has no completion or failure, nothing of it is in
    silver and reconcile reports no gap."""
    registry = load_registry()
    held = registry.resources[CAPTURES["t1"].resource_id][1]
    assert isinstance(held.disposition, HoldDisposition)
    assert held.disposition.unit == "E-SEM"
    for needle in ("360", "2,150,551", "int64", "lexical-number", "new last_modified"):
        assert needle in held.disposition.reason, needle
    others = [
        r
        for _p, r in registry.resources.values()
        if r.family == "thermal_constraint_costs" and r.id != held.id
    ]
    assert len(others) == 5 and all(isinstance(r.disposition, SilverDisposition) for r in others)

    t1_id = capture(data, "t1")
    path = next((data / "bronze" / SOURCE / "thermal_constraint_costs").rglob(f"*{held.id}*.csv"))
    parsed = next(read_csv_body(path, _record("thermal_constraint_costs"), ()))
    assert list(parsed.header) == list(_record("thermal_constraint_costs").epochs[0].header)
    comma = [r["Daily Cost (GBP)"] for r in rows("t1") if "," in r["Daily Cost (GBP)"]]
    assert comma
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pl.Series(comma).cast(pl.Int64, strict=True)

    t2_id = capture(data, "t2")
    written = get_transformer(SOURCE, "thermal_constraint_costs", data).run(DAY, run_id="r")
    assert written == len(rows("t2"))
    assert read_completion(data, "thermal_constraint_costs", t1_id) is None
    assert read_failure(data, "thermal_constraint_costs", t1_id) is None
    completion = read_completion(data, "thermal_constraint_costs", t2_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, "thermal_constraint_costs")
    assert set(frame["resource_id"].to_list()) == {CAPTURES["t2"].resource_id}
    report = reconcile(data, registry, ["thermal_constraint_costs"], DAY)
    assert report.gaps == (), report.lines()


def test_workbook_date_spelling_is_the_calamine_spelling_and_is_frozen(data: Path) -> None:
    """Detects a record date format the reader does not emit (``%Y-%m-%d`` would fail the whole
    sheet) or a spelling that drifted: the installed calamine reader emits the Excel date cells as
    ``YYYY-MM-DD 00:00:00``, the record's frozen format parses exactly that spelling, and the plain
    ``%Y-%m-%d`` of the CSV record does not."""
    capture(data, "x1")
    path = next((data / "bronze" / SOURCE / "thermal_constraint_costs_files").rglob("*.xlsx"))
    (table,) = read_xlsx_body(path, _record("thermal_constraint_costs_xlsx"), ("Data",))
    dates = table.frame["Settlement Date"].to_list()
    assert dates[0] == "2019-08-01 00:00:00"
    assert all(len(d) == 19 and d.endswith(" 00:00:00") for d in dates)
    spec = _record("thermal_constraint_costs_xlsx").epochs[0].columns[0]
    assert spec.format == CALAMINE_DATE
    assert datetime.strptime(dates[0], CALAMINE_DATE).date() == date(2019, 8, 1)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        table.frame["Settlement Date"].str.strptime(pl.Date, "%Y-%m-%d", strict=True)


def test_workbook_data_sheet_types_through_the_sibling_fed_record(data: Path) -> None:
    """Detects the Data sheet routed to the wrong family, a workbook child lost, a recipe off by a
    row or column, or the auxiliary sheets read: only the ``Data`` child feeds the XLSX record,
    which yields the cut's 21 rows with zero exclusions, ``child_id`` ``Data``, the resource
    stamped, dates and int64 costs equal to the sheet, a unique key, a zero kept; the CSV family
    never sees the workbook; the capture is a ``thermal_constraint_costs_files`` body."""
    capture_id = capture(data, "x1")
    written = get_transformer(SOURCE, "thermal_constraint_costs_xlsx", data).run(DAY, run_id="r")
    assert written == 21
    completion = read_completion(data, "thermal_constraint_costs_xlsx", capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
    frame = _silver(data, "thermal_constraint_costs_xlsx")
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(
        "thermal_constraint_costs_xlsx"
    )
    assert set(frame["child_id"].to_list()) == {"Data"}
    assert set(frame["resource_id"].to_list()) == {CAPTURES["x1"].resource_id}
    assert frame.schema["settlement_date"] == pl.Date and frame.schema["daily_cost_gbp"] == pl.Int64
    assert frame["settlement_date"].min() == date(2019, 8, 1)
    assert set(frame["constraint_group"].to_list()) <= {
        "ESTEX",
        "SCOTEX",
        "SEIMP",
        "SSE-SP",
        "SSHARN",
        "SWALEX",
    }
    assert 0 in frame["daily_cost_gbp"].to_list()
    key = ["resource_id", "settlement_date", "constraint_group"]
    assert frame.select(key).is_duplicated().sum() == 0
    assert _no_silver(data, "thermal_constraint_costs")


def test_workbook_resource_dispositions_feed_only_the_data_sheet() -> None:
    """Detects an auxiliary sheet fed to silver or the Data sheet left held: each of the two
    workbooks is SILVER for the XLSX family with exactly the ``Data`` child SILVER and the other
    four sheets (``Map``, both network diagrams, ``Dates``) DOC; the three PNGs stay DOC."""
    registry = load_registry()
    workbooks = [
        r for _p, r in registry.resources.values() if r.family == "thermal_constraint_costs_files"
    ]
    xlsx = [r for r in workbooks if r.format == "XLSX"]
    assert len(xlsx) == 2 and len(workbooks) == 5
    for resource in xlsx:
        assert resource.disposition == SilverDisposition(
            kind="SILVER", key="thermal_constraint_costs_xlsx"
        )
        children = {c.child: c.disposition for c in resource.children}
        assert set(children) == {
            "Data",
            "Map",
            "Network Diagram E&W",
            "Network Diagram Scot",
            "Dates",
        }
        assert children["Data"] == SilverDisposition(
            kind="SILVER", key="thermal_constraint_costs_xlsx"
        )
        assert all(isinstance(d, DocDisposition) for name, d in children.items() if name != "Data")
    assert {r.format for r in workbooks if r not in xlsx} == {"PNG"}
    assert all(isinstance(r.disposition, DocDisposition) for r in workbooks if r not in xlsx)


def test_voltage_fixture_types_keeps_reversed_ranges_and_never_sets_an_issue_time(
    data: Path,
) -> None:
    """Detects a reversed range swapped or dropped, ``Last Updated`` landing as ``issue_time`` or
    shifted off UTC, a repeated (start, group) pair collapsed, or a group renamed: every row loads
    with zero exclusions; start and end dates equal the CSV (reversed ones keep end < start);
    ``last_updated`` is the UTC instant of the ``Z`` text; the two ``V_North`` 2025-08-19 rows with
    different update times both survive; ``issue_time`` is not an output column; units are int64
    counts (a zero stays), a blank note is null and the group codes are untouched."""
    source = rows("v")
    frame = _assert_clean_load(data, "v", len(source))
    assert "issue_time" not in frame.columns
    assert frame["start_date"].to_list() == [date.fromisoformat(r["Start Date"]) for r in source]
    assert frame["end_date"].to_list() == [date.fromisoformat(r["End Date"]) for r in source]
    reversed_rows = frame.filter(pl.col("end_date") < pl.col("start_date"))
    assert reversed_rows.height == 2
    assert reversed_rows["start_date"].to_list() == [date(2025, 8, 23)] * 2
    assert reversed_rows["end_date"].to_list() == [date(2025, 8, 18)] * 2
    assert frame.schema["last_updated"] == pl.Datetime("us", "UTC")
    assert frame["last_updated"].to_list() == [
        datetime.strptime(r["Last Updated"], "%Y-%m-%dT%H:%MZ").replace(tzinfo=UTC) for r in source
    ]
    north = frame.filter(
        (pl.col("group") == "V_North") & (pl.col("start_date") == date(2025, 8, 19))
    )
    assert north.height == 2 and north["last_updated"].n_unique() == 2
    assert frame["group"].to_list() == [r["Group"] for r in source]
    assert frame.schema["units"] == pl.Int64
    assert frame["units"].to_list() == [int(r["Units"]) for r in source]
    assert 0 in frame["units"].to_list()
    assert frame["notes"].to_list() == [r["Notes"] or None for r in source]
    assert frame["notes"].null_count() == sum(1 for r in source if not r["Notes"])


@pytest.mark.parametrize(
    "stamp", ["2025-08-22T14:05+01:00", "2025-08-22T14:05:00Z", "22/08/2025 14:05"]
)
def test_a_last_updated_other_than_the_measured_z_spelling_fails_loud(
    data: Path, stamp: str
) -> None:
    """Detects an offset relabelled as UTC or a new spelling guessed: a ``Last Updated`` with a
    numeric offset, with seconds or in another layout fails the capture (no completion, no silver),
    because the record accepts only the 294 measured ``YYYY-MM-DDTHH:MMZ`` spellings."""
    raw = body("v").replace(b"2025-08-22T14:05Z", stamp.encode(), 1)
    assert raw != body("v")
    capture_id = capture(data, "v", raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "voltage_requirement", data).run(DAY, run_id="r")
    assert read_completion(data, "voltage_requirement", capture_id) is None
    assert _no_silver(data, "voltage_requirement")


@pytest.mark.parametrize(
    ("alias", "needle", "wrong"),
    [
        ("l", b"2026,40,6400", b"2026,40,6400.5"),
        ("d", b"ESTEX,2023-01-01T00:00:00,3950,894", b"ESTEX,2023-01-01T00:00:00,3950,89.4"),
        ("t2", b"2022-04-03,SCOTEX,798617", b"2022-04-03,SCOTEX,798,617"),
        ("o", b"2026-04-11,1850", b"2026-04-11,N/A"),
    ],
)
def test_an_undocumented_token_is_never_read_as_null_or_repaired(
    data: Path, alias: str, needle: bytes, wrong: bytes
) -> None:
    """Detects a silent repair (a decimal truncated, a thousands separator removed, ``N/A`` nulled):
    a numeric cell the strict cast cannot read fails the capture loudly, with no completion and no
    silver."""
    raw = body(alias).replace(needle, wrong, 1)
    assert raw != body(alias)
    capture_id = capture(data, alias, raw=raw)
    meta = CAPTURES[alias]
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert read_completion(data, meta.family, capture_id) is None
    assert _no_silver(data, meta.family)


# --------------------------------------------------------------------------- #
# Reconcile: the day-ahead archive is adjudicated
# --------------------------------------------------------------------------- #

LEDGER_ENTRY: dict[str, Any] = {
    "family": "da_constraint_flows_limits",
    "category": "failed",
    "cause": "DuplicateEntityKeyError",
    "captures": [
        "bronze/neso_data_portal/da_constraint_flows_limits/2026/10/08/"
        "raw_20261008T091659Z_38a18ec1-9e40-465d-93fb-301e80fd1352_437e9f61.csv"
    ],
    "evidence": (
        "K-CON-FACTS §4 (Reader and key evidence), K-CON-SPEC §3 (unit v0.22-K-CON); ADR-040"
    ),
    "ruling": "632",
}


def _entry() -> dict[str, Any]:
    entries = json.loads((REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8"))
    ours = [e for e in entries if e["family"] == "da_constraint_flows_limits"]
    assert len(ours) == 1 and isinstance(ours[0], dict)
    entry: dict[str, Any] = ours[0]
    return entry


def _tree_bytes(data: Path, top: str) -> dict[str, bytes]:
    return {
        p.relative_to(data / top).as_posix(): p.read_bytes()
        for p in sorted((data / top).rglob("*"))
        if p.is_file()
    }


def test_the_committed_ledger_entry_is_the_ruled_text_and_backed_by_the_registry() -> None:
    """Detects the committed entry drifting from the ruling (another capture, another cause, a wider
    scope, a lost question) or naming a resource that is not the day-ahead CSV: the entry is the
    last one in the ledger, cause ``DuplicateEntityKeyError``, ruling 632, the capture of the
    day-ahead resource only, and a one-line reason and question; the registry backs it (the
    referential check)."""
    entries = registry_module.load_reconcile_adjudications()
    assert reconcile_problems(entries) == []
    entry = _entry()
    for field, value in LEDGER_ENTRY.items():
        assert entry[field] == value, field
    for text in ("reason", "question"):
        assert "\n" not in entry[text] and entry[text].strip()
    assert "1,436" in entry["reason"] and "never deduplicated" in entry["reason"]
    assert entries[-1].family == "da_constraint_flows_limits"


def reconcile_problems(entries: Any) -> list[str]:
    """The registry's referential check of the ledger."""
    return registry_module.reconcile_adjudication_problems(load_registry(), entries)


def test_the_day_ahead_failure_is_adjudicated_not_open_and_not_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects the vendor-caused failure left as an open gap (reconcile red forever), an entry that
    no longer matches the failure record (stale), or an adjudication that alters data: without the
    ledger reconcile reports the failed capture; with the committed entry (its capture id swapped
    for the fixture capture's) every gap is adjudicated, none is open or stale, the ADJUDICATED line
    names ``DuplicateEntityKeyError`` and ruling 632, and the silver and state bytes are equal
    before and after."""
    install_generated(
        monkeypatch,
        data / "_registry",
        [_package_doc("day-ahead-constraint-flows-and-limits.json")],
    )
    capture_id = capture(data, "dc")
    with contextlib.suppress(NesoCaptureFailedError):
        get_transformer(SOURCE, "da_constraint_flows_limits", data).run(DAY, run_id="r")
    before = (_tree_bytes(data, "silver"), _tree_bytes(data, "state"))
    code, lines = run_cli("da_constraint_flows_limits", "--cutoff", DAY.isoformat())
    assert code == 1, lines
    assert any(line.startswith("GAP failed") and capture_id in line for line in lines), lines
    entries = [{**_entry(), "captures": [capture_id]}]
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json(entries), encoding="utf-8"
    )
    code, lines = run_cli("da_constraint_flows_limits", "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert [line for line in lines if line.startswith("GAP")] == []
    assert "SUMMARY adjudicated 1" in lines
    assert "SUMMARY stale_adjudication 0" in lines
    adjudicated = [line for line in lines if capture_id in line]
    assert len(adjudicated) == 1
    assert adjudicated[0].startswith("ADJUDICATED failed da_constraint_flows_limits")
    assert "DuplicateEntityKeyError" in adjudicated[0] and "ruling 632" in adjudicated[0]
    assert (_tree_bytes(data, "silver"), _tree_bytes(data, "state")) == before


def test_the_ledger_has_exactly_one_new_entry_after_the_building_block_ones() -> None:
    """Detects a second or stray entry for another constraint family: the ledger's families end with
    the day-ahead archive once, and no other CON family appears."""
    families = [e.family for e in registry_module.load_reconcile_adjudications()]
    assert families.count("da_constraint_flows_limits") == 1
    assert not set(families) & (set(FAMILIES) - {"da_constraint_flows_limits"})


# --------------------------------------------------------------------------- #
# Vintage, the catalogue and the generated pages
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ["l", "m4", "o", "t2", "v"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy (RULINGS 529/597) or a catalogue view that cannot carry the new
    columns: ``available_at`` is the CKAN ``last_modified``, ``timestamp_utc`` stays the capture
    time (the date anchor's London midnight for the ``date_sp1`` records), an as-of read before the
    vintage serves nothing even though the capture is later and one after serves it, in the DuckDB
    view and in Polars; for the voltage record the in-row ``Last Updated`` plays no part."""
    meta = CAPTURES[alias]
    capture_id, _ = _run(data, alias)
    frame = _silver(data, meta.family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    recipe = _record(meta.family).temporal
    if recipe.kind == "none":
        assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    else:
        assert recipe.date_column is not None
        london = ZoneInfo("Europe/London")
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
    """Detects a record the docs generator cannot render (the held questions, the resource-level
    HOLD, the workbook child family, the CMIS epochs)."""
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
