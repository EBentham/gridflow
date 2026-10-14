"""The FES ES2 / FLX1 / ED5 / ED6 frozen records (v0.22-K-SCN-2a).

Four families of four NESO packages get record version 1:

- ``fes_es2_european_supply`` (ES2), ``fes_flx1_flexibility`` (FLX1) and ``fes_ed5_road_transport``
  (ED5): wide, one unpivot header epoch per distinct year range, held E-SEM (the period a year
  label denotes and the meaning of a blank projection cell are undocumented).
- ``fes_ed6_road_transport_notes`` (ED6): the ED5 codebook, a reference table with no years,
  eligible.
- Two resource-level HOLD dispositions: the ES2 2026 body is a notes table (no years, no values),
  the FLX1 2023 body has numbers with trailing spaces the unpivot value cannot trim.

Every test writes recorded fixture captures into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here, not
at activation. On master the four families have no record, so ``get_transformer`` raises.

Fixtures (``tests/fixtures/neso_data_portal/scn2a/``, provenance in ``PROVENANCE.md``): each file
is a cut of the 2026-10-08 swept bronze (a scratch script, not committed): whole vendor records,
byte for byte, chosen for the oddities named in the tests. ``.gitattributes`` keeps their CRLF
terminators and BOMs.
"""

from __future__ import annotations

import csv
import io
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import write_capture
from polars.exceptions import InvalidOperationError
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import (
    HoldDisposition,
    SilverDisposition,
    load_registry,
)
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.casting import DuplicateEntityKeyError
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    read_failure,
)
from gridflow.silver.neso_data_portal.readers import read_csv_body
from gridflow.silver.neso_data_portal.reconcile import reconcile
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "scn2a"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)

ES2 = "fes_es2_european_supply"
FLX1 = "fes_flx1_flexibility"
ED5 = "fes_ed5_road_transport"
ED6 = "fes_ed6_road_transport_notes"
FAMILIES = (ES2, FLX1, ED5, ED6)
UNPIVOT_FAMILIES = (ES2, FLX1, ED5)

PACKAGES = {
    ES2: ("fes-european-electricity-supply-data-table-es2", "bd83ce0b-7b1e-4ff2-89e8-12d524c34d99"),
    FLX1: (
        "fes-flexibility-data-table-data-table-flx1",
        "2e3275e2-dd6a-4c2e-8cfb-eeb9b4320dcb",
    ),
    ED5: ("fes-road-transport-summary-data-table-ed5", "28303fe9-62d4-46c5-ad15-a1d0e59f98cb"),
    ED6: ("fes-road-transport-notes-data-table-ed6", "5a3b9d32-5da8-4237-8468-dfb7bb4ebe78"),
}

HELD_QUESTIONS = {
    ES2: (
        "TODO: For each quantitative ES2 edition, what interval does `Demand (TWh)` cover, and "
        "what observation date does `Capacity (MW)` represent? Does a year label denote the "
        "starting or ending year? What does a blank projection cell mean? Is "
        "`10yo2026_es2_v001.csv` intentionally a notes resource, or was the European electricity "
        "supply table replaced by an incorrect upload? Please identify or republish the "
        "quantitative European supply resource for this edition."
    ),
    FLX1: (
        "TODO: For each FLX1 edition and measure, what interval or observation date does its "
        "year label denote, including annual interconnector energy, storage energy potential, "
        "installed capacity and flexibility at peak? Does a winter-related label identify the "
        "starting or ending year? What does a blank projection cell mean? Can NESO republish "
        "`fes2023_flx1_v001.csv` without numeric edge whitespace, or will a separately "
        "authorized engine change permit explicitly declared ASCII trimming of unpivot values?"
    ),
    ED5: (
        "TODO: For each ED5 annual-energy, vehicle-count and capacity item, what interval or "
        "observation date does the year label denote? For winter-peak items, does the integer "
        "label identify the starting or ending winter year? What does a blank projection cell "
        "mean?"
    ),
}

KEYS = {
    ES2: (
        "resource_id",
        "edition",
        "country",
        "eu_scenario",
        "fes_scenario_alignment",
        "variable",
        "category",
        "type",
        "subtype",
        "subsubtype",
        "fes_pathway_alignment",
        "projection_year",
    ),
    FLX1: (
        "resource_id",
        "edition",
        "flexibility_type",
        "flexibility_sub_type",
        "data_item",
        "unit",
        "scenario",
        "fuel",
        "detail",
        "pathway",
        "projection_year",
    ),
    ED5: (
        "resource_id",
        "edition",
        "data_item",
        "unit",
        "scenario",
        "fuel",
        "peak_annual_minimum",
        "pathway",
        "projection_year",
    ),
    ED6: ("resource_id", "edition", "column_name", "category"),
}

EDITIONS = {
    ES2: (
        ("fes2023_es2_v001.csv", 2023),
        ("fes2024_es2_v001.csv", 2024),
        ("fes2025_es2_v006.csv", 2025),
    ),
    FLX1: (
        ("fes2024_flx1_v003.csv", 2024),
        ("fes2025_flx1_v006.csv", 2025),
        ("10yo2026_flx1_v001.csv", 2026),
    ),
    ED5: (
        ("fes2023_ed5_v002.csv", 2023),
        ("fes2024_ed5_v003.csv", 2024),
        ("fes2025_ed5_v006.csv", 2025),
        ("10yo2026_ed5_v001.csv", 2026),
    ),
    ED6: (
        ("fes2023_ed6_v001.csv", 2023),
        ("fes2024_ed6_v001.csv", 2024),
        ("fes2025_ed6_v006.csv", 2025),
        ("10yo2026_ed6_v001.csv", 2026),
    ),
}

# Vendor identifier header -> silver name, in header order, with the spec's nullability.
ES2_COLUMNS = {
    "Country": ("country", False),
    "EU Scenario": ("eu_scenario", False),
    "FES Scenario Alignment": ("fes_scenario_alignment", True),
    "FES Pathway Alignment": ("fes_pathway_alignment", True),
    "Variable": ("variable", False),
    "Category": ("category", False),
    "Type": ("type", False),
    "SubType": ("subtype", True),
    "SubSubType": ("subsubtype", True),
}
FLX1_COLUMNS = {
    "Flexibility type": ("flexibility_type", False),
    "Flexibility sub-type": ("flexibility_sub_type", False),
    "Data item": ("data_item", False),
    "Unit": ("unit", False),
    "Scenario": ("scenario", True),
    "Pathway": ("pathway", True),
    "Fuel": ("fuel", False),
    "Detail": ("detail", False),
}
ED5_COLUMNS = {
    "Data item": ("data_item", False),
    "Unit": ("unit", False),
    "Scenario": ("scenario", True),
    "Pathway": ("pathway", True),
    "Fuel": ("fuel", False),
    "Peak/Annual/Minimum": ("peak_annual_minimum", False),
}
ED6_COLUMNS = {
    "Column": ("column_name", False),
    "Category": ("category", False),
    "Description": ("description", False),
}
COLUMNS = {ES2: ES2_COLUMNS, FLX1: FLX1_COLUMNS, ED5: ED5_COLUMNS, ED6: ED6_COLUMNS}


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture and its sidecar provenance."""

    family: str
    resource_id: str
    name: str
    filename: str
    edition: int
    modified: str
    written: str


CAPTURES: dict[str, Capture] = {
    "es2_23": Capture(
        ES2,
        "6563801b-6da4-46e7-b147-3d81c0237779",
        "European Electricity Supply Data Table (ES2) 2023",
        "fes2023_es2_v001.csv",
        2023,
        "2023-07-10T03:32:05.482290",
        "2026-10-08T10:55:14.398706+00:00",
    ),
    "es2_24": Capture(
        ES2,
        "7902b197-ada0-47ae-bd2e-db566a8d0fab",
        "European Electricity Supply Data Table (ES2) 2024",
        "fes2024_es2_v001.csv",
        2024,
        "2024-07-15T06:30:08.953371",
        "2026-10-08T10:55:17.318725+00:00",
    ),
    "es2_25": Capture(
        ES2,
        "fbfdc6c6-52b6-4e3a-96b9-f4e7941f61ec",
        " European Electricity Supply Data Table (ES2) 2025",
        "fes2025_es2_v006.csv",
        2025,
        "2025-12-10T16:55:04.054020",
        "2026-10-08T10:55:20.326635+00:00",
    ),
    "es2_26": Capture(
        ES2,
        "e8ed65c6-6785-4e2b-84a5-696057f103c9",
        "European Electricity Supply Data Table (ES2) 2026",
        "10yo2026_es2_v001.csv",
        2026,
        "2026-09-16T14:33:44.536460",
        "2026-10-08T10:55:23.249005+00:00",
    ),
    "flx1_23": Capture(
        FLX1,
        "e86745fd-1ef2-4112-a14c-84682a387b68",
        "Flexibility data table (FLX1) 2023",
        "fes2023_flx1_v001.csv",
        2023,
        "2023-07-10T03:27:12.556782",
        "2026-10-08T10:55:27.105892+00:00",
    ),
    "flx1_24": Capture(
        FLX1,
        "c7d7a19d-255f-4585-b53b-487891aeda54",
        "Flexibility data table (FLX1) 2024",
        "fes2024_flx1_v003.csv",
        2024,
        "2025-06-27T09:22:49.054408",
        "2026-10-08T10:55:29.882933+00:00",
    ),
    "flx1_25": Capture(
        FLX1,
        "299bc6c8-7608-4946-ab0a-5bc22129c897",
        "Flexibility data table (FLX1) 2025",
        "fes2025_flx1_v006.csv",
        2025,
        "2025-12-10T17:00:25.208610",
        "2026-10-08T10:55:33.057523+00:00",
    ),
    "flx1_26": Capture(
        FLX1,
        "0ff1a94f-224d-4863-8137-7d9a0d2cd7f8",
        "Flexibility data table (FLX1) 2026",
        "10yo2026_flx1_v001.csv",
        2026,
        "2026-09-16T14:35:58.106567",
        "2026-10-08T10:55:35.461289+00:00",
    ),
    "ed5_23": Capture(
        ED5,
        "076c5c72-ecea-47c9-81b6-e93fe824d5d4",
        "Road Transport Summary (ED5) 2023",
        "fes2023_ed5_v002.csv",
        2023,
        "2025-07-01T17:03:39.781640",
        "2026-10-08T10:54:30.701995+00:00",
    ),
    "ed5_24": Capture(
        ED5,
        "7189f5d3-aed3-41b7-962f-fdb9fe11cbb7",
        "Road Transport Summary (ED5) 2024",
        "fes2024_ed5_v003.csv",
        2024,
        "2025-07-01T17:04:57.258965",
        "2026-10-08T10:54:34.085427+00:00",
    ),
    "ed5_25": Capture(
        ED5,
        "ce76da34-6eb0-43a1-8e93-3cc833d6d1b4",
        "Road Transport Summary (ED5) 2025",
        "fes2025_ed5_v006.csv",
        2025,
        "2025-12-10T17:01:17.874457",
        "2026-10-08T10:54:36.640837+00:00",
    ),
    "ed5_26": Capture(
        ED5,
        "1b59c48f-f51a-4e32-a9e7-baca0b78a042",
        "Road Transport Summary (ED5) 2026",
        "10yo2026_ed5_v001.csv",
        2026,
        "2026-09-16T14:30:16.337617",
        "2026-10-08T10:54:39.292172+00:00",
    ),
    "ed6_23": Capture(
        ED6,
        "30012023-6c0b-4da5-bd77-4f2b244a950d",
        "Road Transport Notes (ED6) 2023",
        "fes2023_ed6_v001.csv",
        2023,
        "2023-07-10T03:26:39.716174",
        "2026-10-08T10:54:43.214013+00:00",
    ),
    "ed6_24": Capture(
        ED6,
        "7fe93ddc-7a88-478b-8f60-4531c15bd538",
        "Road Transport Notes (ED6) 2024",
        "fes2024_ed6_v001.csv",
        2024,
        "2024-07-15T06:41:16.480938",
        "2026-10-08T10:54:46.048424+00:00",
    ),
    "ed6_25": Capture(
        ED6,
        "43add83b-bf94-493b-8f5d-de7fdbefc5bb",
        "Road Transport Notes (ED6) 2025",
        "fes2025_ed6_v006.csv",
        2025,
        "2025-12-10T17:02:44.975794",
        "2026-10-08T10:54:48.733314+00:00",
    ),
    "ed6_26": Capture(
        ED6,
        "06601add-c47b-427c-bdde-845b270e72e1",
        "Road Transport Notes (ED6) 2026",
        "10yo2026_ed6_v001.csv",
        2026,
        "2026-09-16T15:01:14.222912",
        "2026-10-08T10:54:51.332378+00:00",
    ),
}
HELD_ALIASES = ("es2_26", "flx1_23")
TYPED = tuple(a for a in CAPTURES if a not in HELD_ALIASES)
TYPED_UNPIVOT = tuple(a for a in TYPED if CAPTURES[a].family != ED6)
TYPED_ED6 = tuple(a for a in TYPED if CAPTURES[a].family == ED6)
HOLD_REASONS = {
    "es2_26": (
        "11-row `Notes`, `Description` notes table",
        "no projection-year columns and no values",
        "V-18",
        "NESO confirms the resource's intent",
        "a new last_modified",
    ),
    "flx1_23": (
        "strict float64 cast on 1152 projection cells",
        "trailing ASCII spaces",
        "`6.06 `",
        "`ValueSpec` has no `trim`",
        "a new last_modified",
    ),
}
FIRST_YEAR = {
    "es2_23": 2022,
    "es2_24": 2023,
    "es2_25": 2024,
    "flx1_24": 2021,
    "flx1_25": 2024,
    "flx1_26": 2024,
    "ed5_23": 2017,
    "ed5_24": 2020,
    "ed5_25": 2023,
    "ed5_26": 2023,
}
BOMS = ("es2_24", "flx1_24", "ed6_24")


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings point at."""
    with tempfile.TemporaryDirectory(
        prefix="sc", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's CRLF line endings.

    ``.gitattributes`` keeps the committed bytes, so this only guards a checkout that rewrote
    them: a bare ``\\n`` outside a quoted cell is a record terminator, an embedded one (ED6
    descriptions) stays."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def lines(alias: str, raw: bytes | None = None) -> list[list[str]]:
    """Every record of the fixture as text."""
    data_bytes = raw if raw is not None else body(alias)
    return list(csv.reader(io.StringIO(data_bytes.decode("utf-8-sig"), newline="")))


def table(alias: str, raw: bytes | None = None) -> tuple[list[str], list[list[str]]]:
    """The fixture's table header and populated (not all-blank) records as text."""
    records = lines(alias, raw)
    return records[0], [line for line in records[1:] if any(line)]


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's populated records keyed by header."""
    header, records = table(alias, raw)
    return [dict(zip(header, record, strict=True)) for record in records]


def label_columns(header: list[str]) -> list[str]:
    """The header's year labels, in header order."""
    return [h for h in header if h.isdigit()]


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
    slug, package_id = PACKAGES[meta.family]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=written or datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=slug,
        package_id=package_id,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=filename or meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
    )
    return capture_id_for(path, data)


def _record(family: str) -> SchemaRecord:
    record = registry_module.load_registry().families[family][1].record
    assert record is not None
    return record


def _silver(data: Path, family: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / family).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _no_silver(data: Path, family: str) -> bool:
    return not list((data / "silver" / SOURCE / family).rglob("[!.]*.parquet"))


def _run(data: Path, alias: str, **kwargs: Any) -> tuple[str, int]:
    """Capture ``alias``, run its family's transformer; the capture id and rows written."""
    capture_id = capture(data, alias, **kwargs)
    written = get_transformer(SOURCE, CAPTURES[alias].family, data).run(DAY, run_id="r")
    return capture_id, written


def _replace_cell(raw: bytes, row: int, column: int, value: str) -> bytes:
    """``raw`` with table record ``row`` (0 = the header) cell ``column`` replaced.

    Only for bodies whose records never span a physical line."""
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    parts = raw[len(bom) :].split(b"\r\n")
    fields = next(csv.reader([parts[row].decode("utf-8")]))
    fields[column] = value
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    parts[row] = out.getvalue().encode("utf-8")
    return bom + b"\r\n".join(parts)


def expected_long(alias: str) -> list[tuple[Any, ...]]:
    """The long rows the vendor body must unpivot to, sorted: every silver identifier column of
    the family (``None`` for a blank or an absent one), projection year and value."""
    family = CAPTURES[alias].family
    names = sorted({name for name, _nullable in COLUMNS[family].values()})
    header, records = table(alias)
    pos = {name: header.index(name) for name in header}
    out: list[tuple[Any, ...]] = []
    for record in records:
        cells = {
            COLUMNS[family][h][0]: (record[pos[h]] or None) for h in header if h in COLUMNS[family]
        }
        for year in label_columns(header):
            cell = record[pos[year]]
            out.append(
                (
                    *(cells.get(n) for n in names),
                    int(year),
                    float(cell) if cell != "" else None,
                )
            )
    return sorted(out, key=repr)


def actual_long(frame: pl.DataFrame, family: str) -> list[tuple[Any, ...]]:
    """The silver rows in :func:`expected_long`'s shape."""
    names = sorted({name for name, _nullable in COLUMNS[family].values()})
    return sorted(
        (
            (*(row[n] for n in names), row["projection_year"], row["value"])
            for row in frame.to_dicts()
        ),
        key=repr,
    )


# --------------------------------------------------------------------------- #
# Fixtures and record shape
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: CRLF terminators, each
    original's BOM (the 2024 ES2, FLX1 and ED6 bodies have one), the
    exact vendor headers and each edition's first year, blank / zero / negative projection cells,
    the ES2 ``Ireland (IAI)`` / ``SEM`` countries, a comma-bearing alignment cell, a blank
    ``SubType``, the FLX1 and ED5 units, the padded FLX1 2023 numbers, the notes body and the ED6
    separator rows and embedded newline."""
    bom = b"\xef\xbb\xbf"
    for alias in CAPTURES:
        raw = body(alias)
        assert raw.startswith(bom) == (alias in BOMS), alias
        assert raw.endswith(b"\r\n"), alias
        assert b"\r\n" in raw, alias
    for alias in TYPED_UNPIVOT:
        header, _ = table(alias)
        assert label_columns(header)[0] == str(FIRST_YEAR[alias]), alias
        assert label_columns(header)[-1] == "2050", alias
        cells = [v for r in rows(alias) for k, v in r.items() if k.isdigit()]
        assert any(c != "" and float(c) == 0 for c in cells) or alias in {"es2_23"}, alias
    assert table("es2_23")[0][:8] == [
        "Country",
        "EU Scenario",
        "FES Scenario Alignment",
        "Variable",
        "Category",
        "Type",
        "SubType",
        "SubSubType",
    ]
    for alias in ("es2_24", "es2_25"):
        assert table(alias)[0][:7] == [
            "Country",
            "EU Scenario",
            "FES Pathway Alignment",
            "Variable",
            "Category",
            "Type",
            "SubType",
        ]
    r23 = rows("es2_23")
    assert any(r["Country"] == "Ireland (IAI)" for r in r23)
    assert any(r["SubType"] == "" and r["SubSubType"] == "" for r in r23)
    assert any(r["Variable"] == "Demand (TWh)" and r["SubType"] == "" for r in r23)
    assert any("," in r["FES Scenario Alignment"] for r in r23)
    assert any(r["Country"] == "SEM" for r in rows("es2_24"))
    assert any(r["2023"] == "" for r in rows("es2_24"))
    assert any(r["2036"] == "" and r["2050"] == "" for r in rows("es2_25"))
    assert any("," in r["FES Pathway Alignment"] for r in rows("es2_25"))
    assert table("es2_26")[0] == ["Notes", "Description"]
    assert {r["Unit"] for a in ("flx1_24", "flx1_25", "flx1_26") for r in rows(a)} == {
        "GW",
        "GWh",
        "TWh",
    }
    assert any(r["2021"] == "" for r in rows("flx1_24"))
    assert any(r["2024"] == "" for r in rows("flx1_26"))
    assert {r["Scenario"] for r in rows("flx1_26")} == {"Ten Year Outlook"}
    padded = [v for r in rows("flx1_23") for k, v in r.items() if k.isdigit()]
    assert "6.06 " in padded and all(v != v.strip() for v in padded)
    for alias in ("ed5_23", "ed5_24", "ed5_25", "ed5_26"):
        units = {r["Unit"] for r in rows(alias)}
        assert {"# Cars", "GW", "TWh"} <= units, alias
        assert any(r["Unit"] == "GW" and "eak" in r["Peak/Annual/Minimum"] for r in rows(alias))
    for alias in ("ed5_23", "ed5_24", "ed5_25", "ed5_26"):
        assert any(r["2050"] == "" for r in rows(alias)), alias
    assert table("ed5_23")[0][2] == "Scenario" and table("ed5_24")[0][2] == "Pathway"
    for alias in TYPED_ED6:
        assert table(alias)[0] == ["Column", "Category", "Description"], alias
    assert sum(not any(line) for line in lines("ed6_23")[1:]) == 3
    assert any("\n" in r["Description"] for r in rows("ed6_24"))


@pytest.mark.parametrize("family", FAMILIES)
def test_record_shape_matches_the_unit_spec(family: str) -> None:
    """Detects a record that drifts from the spec: one csv/utf-8 record, no issue time, temporal
    ``none``, whole-capture selection per ``resource_id``, ``ckan_last_modified`` vintage, the
    exact ``edition_by_filename`` pairs of the SILVER resources only and the spec's entity key."""
    record = _record(family)
    assert (record.version, record.reader, record.encoding) == ("1", "csv", "utf-8")
    assert record.temporal.kind == "none"
    assert record.latest == "whole_capture"
    assert record.latest_partition == "resource_id"
    assert record.vintage == "ckan_last_modified"
    assert record.xlsx is None and record.siblings == ()
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    assert record.edition_by_filename == EDITIONS[family]
    assert record.entity_key == KEYS[family]


@pytest.mark.parametrize(
    ("family", "aliases"),
    [
        (ES2, ("es2_23", "es2_24", "es2_25")),
        (FLX1, ("flx1_24", "flx1_25", "flx1_26")),
        (ED5, ("ed5_23", "ed5_24", "ed5_25", "ed5_26")),
    ],
)
def test_unpivot_epochs_carry_the_exact_year_labels(family: str, aliases: tuple[str, ...]) -> None:
    """Detects a dropped, invented or mis-mapped year, an epoch merged away, or a blank column
    removed: one epoch per distinct header (the editions differ in label column or first year),
    each frozen exactly as the real body's reader-returned header, the year labels exactly the
    header's year columns in order, each mapped to its integer and ending in 2050, the value a
    nullable float64 with no token or bound."""
    record = _record(family)
    assert len(record.epochs) == len(aliases)
    for epoch, alias in zip(record.epochs, aliases, strict=True):
        header, _ = table(alias)
        assert list(epoch.header) == header, alias
        assert epoch.unpivot is not None
        labels = [label for label, _ in epoch.unpivot.years]
        assert labels == label_columns(list(epoch.header))
        years = [year for _, year in epoch.unpivot.years]
        assert years == [int(label) for label in labels]
        assert years == list(range(FIRST_YEAR[alias], 2051))
        assert epoch.unpivot.value.dtype == "float64" and epoch.unpivot.value.nullable is True
        assert epoch.unpivot.value.null_tokens == ()
        assert (epoch.unpivot.value.min, epoch.unpivot.value.max) == (None, None)


def test_the_flx1_2026_all_year_tail_is_declared_in_full() -> None:
    """Detects a trailing blank-only year column left out of the unpivot (FACTS: 2026's
    ``2037``-``2050`` and ED5 2026's ``2037``-``2050`` are blank in every row, the long-row counts
    include them)."""
    flx1 = _record(FLX1).epochs[2].unpivot
    ed5 = _record(ED5).epochs[3].unpivot
    assert flx1 is not None and ed5 is not None
    assert {"2024", "2037", "2050"} <= {label for label, _ in flx1.years}
    assert {"2037", "2050"} <= {label for label, _ in ed5.years}
    assert (len(flx1.years), len(ed5.years)) == (27, 28)


def test_the_reference_record_has_one_epoch_and_no_unpivot() -> None:
    """Detects ED6 forced into the unpivot shape or split per edition: one ``Column`` /
    ``Category`` / ``Description`` epoch for all four editions, no ``projection_year`` and no
    ``value``, ``Column`` landing as ``column_name`` (``column`` is an SQL reserved word)."""
    record = _record(ED6)
    assert len(record.epochs) == 1
    epoch = record.epochs[0]
    assert epoch.unpivot is None
    assert list(epoch.header) == ["Column", "Category", "Description"]
    assert [(c.source, c.name) for c in epoch.columns] == [
        ("Column", "column_name"),
        ("Category", "category"),
        ("Description", "description"),
    ]
    names = [name for name, _type in generic.output_columns(record)]
    assert "projection_year" not in names and "value" not in names
    assert "column" not in names and "column_name" in names


@pytest.mark.parametrize("family", FAMILIES)
def test_dimension_columns_are_strings_with_the_spec_nullability(family: str) -> None:
    """Detects an identifier cast to a number, a null token declared, a unit column invented or a
    column nullable that FACTS g5 shows populated: every column is a string without null tokens,
    ``nullable`` exactly as the spec (columns absent from an epoch or blank in a body are
    nullable, the rest are not), the vendor names lower-cased with non-alphanumeric runs as ``_``
    and the silver columns are only the dimensions plus the unpivot's ``projection_year`` and
    ``value``."""
    record = _record(family)
    seen: set[str] = set()
    for epoch in record.epochs:
        for column in epoch.columns:
            name, nullable = COLUMNS[family][column.source]
            assert (column.name, column.dtype, column.nullable) == (name, "string", nullable), (
                column.source
            )
            assert column.null_tokens == ()
            seen.add(column.source)
    assert seen == set(COLUMNS[family])
    names = [name for name, _type in generic.output_columns(record)]
    if family == ED6:
        assert not [n for n in names if n in {"unit", "value"}]
    else:
        assert "projection_year" in names and "value" in names


def test_scenario_and_pathway_are_two_nullable_columns_never_merged() -> None:
    """Detects ``Scenario`` and ``Pathway`` (FLX1, ED5) or the ES2 ``FES Scenario Alignment`` and
    ``FES Pathway Alignment`` renamed into one label (the vendor does not equate them): each
    epoch declares only the one its header has, both are nullable strings and in the key."""
    for family, pairs in (
        (FLX1, ("scenario", "pathway")),
        (ED5, ("scenario", "pathway")),
        (ES2, ("fes_scenario_alignment", "fes_pathway_alignment")),
    ):
        record = _record(family)
        for epoch in record.epochs:
            declared = {c.name for c in epoch.columns} & set(pairs)
            assert len(declared) == 1, (family, declared)
        assert set(pairs) <= set(record.entity_key)
        for epoch in record.epochs:
            for column in epoch.columns:
                if column.name in pairs:
                    assert column.nullable is True and column.dtype == "string"
    assert [e.header[2] for e in _record(ED5).epochs] == [
        "Scenario",
        "Pathway",
        "Pathway",
        "Scenario",
    ]
    assert [e.header[4] for e in _record(FLX1).epochs] == ["Pathway", "Pathway", "Scenario"]


def test_held_and_eligible_exactly_as_ruled() -> None:
    """Detects a hold lost or reworded, or a package-level hold: ES2, FLX1 and ED5 are held
    E-SEM with the spec's verbatim question, ED6 is eligible (a reference codebook with no
    temporal semantic), and all four packages stay eligible."""
    registry = load_registry()
    for family, question in HELD_QUESTIONS.items():
        package, entry = registry.families[family]
        effective = effective_eligibility(package, entry)
        assert effective.status == "held", family
        assert effective.unit == "E-SEM"  # type: ignore[union-attr]
        assert effective.question == question  # type: ignore[union-attr]
        assert package.eligibility.status == "eligible"
    package, entry = registry.families[ED6]
    assert effective_eligibility(package, entry).status == "eligible"
    assert package.eligibility.status == "eligible"
    assert _record(ED6).eligibility is None


@pytest.mark.parametrize("family", FAMILIES)
def test_the_family_lives_in_its_package_file(family: str) -> None:
    """Detects a record put in another package file: each record is in its own package file, and
    that file's only recorded family."""
    slug, _id = PACKAGES[family]
    document = json.loads((REGISTRY_DIR / f"{slug}.json").read_text(encoding="utf-8"))
    assert {f["key"] for f in document["families"] if f.get("record")} == {family}


def test_no_adjudication_entry_for_the_four_families() -> None:
    """Detects an ADR-040 recorded-failure entry added (the two failing bodies are resource
    HOLDs, not adjudications): no reconcile adjudication names any of the four families."""
    entries = json.loads((REGISTRY_DIR / "_reconcile_adjudications.json").read_text("utf-8"))
    assert not [e for e in entries if e["family"] in FAMILIES]


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", TYPED_UNPIVOT)
def test_fixture_unpivots_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the BOM
    bodies, the Scenario / Pathway swaps), a cast the vendor body does not satisfy, a row
    excluded or lost, a blank projection cell dropped, a blank ``SubType`` excluded and a stamp
    wrong: the capture completes with one long row per source row and header year label, zero
    exclusions, ``edition`` from the filename, the resource id stamped, a unique key, and every
    cell equal to the vendor's."""
    meta = CAPTURES[alias]
    family = meta.family
    capture_id, written = _run(data, alias)
    header, records = table(alias)
    assert written == len(records) * len(label_columns(header))
    completion = read_completion(data, family, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == written
    assert completion["rows_excluded"] == 0

    frame = _silver(data, family)
    expected_columns = [name for name, _type in generic.output_columns(_record(family))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected_columns if c not in ("year", "month")
    ]
    assert frame.schema["projection_year"] == pl.Int64
    assert frame.schema["value"] == pl.Float64
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame.select(list(KEYS[family])).is_duplicated().sum() == 0
    assert actual_long(frame, family) == expected_long(alias)


@pytest.mark.parametrize("alias", TYPED_ED6)
def test_reference_fixture_types_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects ED6 rows lost, merged or altered: one silver row per populated source record, zero
    exclusions, ``edition`` from the filename, a unique ``(column_name, category)`` key and every
    cell (commas, quotes and embedded newlines included) equal to the vendor's."""
    meta = CAPTURES[alias]
    capture_id, written = _run(data, alias)
    _header, records = table(alias)
    assert written == len(records)
    completion = read_completion(data, ED6, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, ED6)
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame.select(list(KEYS[ED6])).is_duplicated().sum() == 0
    assert sorted(
        (r["column_name"], r["category"], r["description"]) for r in frame.to_dicts()
    ) == sorted((a, b, c) for a, b, c in records)


def test_scenario_and_pathway_land_in_separate_columns(data: Path) -> None:
    """Detects the two vendor labels merged in the silver: ED5 2023 and 2026 rows have a
    ``scenario`` and a null ``pathway``, 2024 and 2025 the reverse; FLX1 2026 is ``Ten Year
    Outlook`` in ``scenario``; ES2 2023 rows carry ``fes_scenario_alignment`` only, 2024 and 2025
    ``fes_pathway_alignment`` only; the key keeps every edition's rows distinct."""
    for alias in TYPED_UNPIVOT:
        _run(data, alias)
    ed5 = _silver(data, ED5)
    for edition in (2023, 2026):
        part = ed5.filter(pl.col("edition") == edition)
        assert part["scenario"].null_count() == 0 and part["pathway"].null_count() == part.height
    for edition in (2024, 2025):
        part = ed5.filter(pl.col("edition") == edition)
        assert part["pathway"].null_count() == 0 and part["scenario"].null_count() == part.height
    flx1 = _silver(data, FLX1)
    part = flx1.filter(pl.col("edition") == 2026)
    assert set(part["scenario"].to_list()) == {"Ten Year Outlook"}
    assert part["pathway"].null_count() == part.height
    assert (
        flx1.filter(pl.col("edition") != 2026)["scenario"].null_count()
        == flx1.filter(pl.col("edition") != 2026).height
    )
    es2 = _silver(data, ES2)
    for edition, present, absent in (
        (2023, "fes_scenario_alignment", "fes_pathway_alignment"),
        (2024, "fes_pathway_alignment", "fes_scenario_alignment"),
        (2025, "fes_pathway_alignment", "fes_scenario_alignment"),
    ):
        part = es2.filter(pl.col("edition") == edition)
        assert part[present].null_count() == 0 and part[absent].null_count() == part.height
    for family in UNPIVOT_FAMILIES:
        assert _silver(data, family).select(list(KEYS[family])).is_duplicated().sum() == 0


def test_comma_bearing_alignment_cells_are_single_values(data: Path) -> None:
    """Detects an alignment cell split on its commas into several scenarios (FACTS: 397 / 448 such
    cells in ES2 2023 / 2025): a cell such as ``A, B`` lands whole in one row's
    ``fes_scenario_alignment`` / ``fes_pathway_alignment``."""
    for alias in ("es2_23", "es2_25"):
        _run(data, alias)
    frame = _silver(data, ES2)
    expected = {
        r["FES Scenario Alignment"] for r in rows("es2_23") if "," in r["FES Scenario Alignment"]
    } | {r["FES Pathway Alignment"] for r in rows("es2_25") if "," in r["FES Pathway Alignment"]}
    assert expected
    present = set(frame["fes_scenario_alignment"].drop_nulls().to_list()) | set(
        frame["fes_pathway_alignment"].drop_nulls().to_list()
    )
    assert expected <= present


def test_blank_projection_cells_are_null_values_never_zero(data: Path) -> None:
    """Detects a blank cell read as zero or dropping its row, and a genuine zero turned into null
    (FACTS g5: no zero run is a sentinel): in every family every blank projection cell of the
    fixtures is a null-valued long row, every ``0`` / ``0.0`` stays ``0.0``."""
    for family in UNPIVOT_FAMILIES:
        aliases = [a for a in TYPED_UNPIVOT if CAPTURES[a].family == family]
        for alias in aliases:
            _run(data, alias)
        frame = _silver(data, family)
        cells = [v for a in aliases for r in rows(a) for k, v in r.items() if k.isdigit()]
        blanks = sum(v == "" for v in cells)
        zeros = sum(v != "" and float(v) == 0 for v in cells)
        assert blanks > 0 and zeros > 0, family
        assert frame["value"].null_count() == blanks, family
        assert (frame["value"] == 0.0).sum() == zeros, family


def test_blank_tails_stay_as_null_valued_rows(data: Path) -> None:
    """Detects ED5 2026's all-blank ``2037``-``2050`` tail (FACTS: 63 blank cells in each, the
    1764 long rows count them) or ES2 2025's ``2036``-``2050`` tail dropped or filled: every
    fixture row has a row for each blank year, all null, and the row count is source rows x
    labels."""
    _, written = _run(data, "ed5_26")
    n = len(rows("ed5_26"))
    assert written == n * 28
    frame = _silver(data, ED5)
    tail = frame.filter(pl.col("projection_year") >= 2037)
    assert tail.height == n * 14
    assert tail["value"].null_count() == tail.height
    _, written = _run(data, "es2_25")
    assert written == len(rows("es2_25")) * 27
    tail = _silver(data, ES2).filter(pl.col("projection_year") >= 2036)
    assert tail["value"].null_count() == sum(r["2036"] == "" for r in rows("es2_25")) * 15


def test_blank_subtype_is_null_and_keeps_its_rows(data: Path) -> None:
    """Detects a row with a blank ``SubType`` / ``SubSubType`` excluded or the blank kept as text
    (FACTS g5: 36 blanks in ES2 2023, ``Demand (TWh)`` rows): the rows are written, the dimension
    is null and nothing else is."""
    _run(data, "es2_23")
    frame = _silver(data, ES2)
    n_blank = sum(r["SubType"] == "" for r in rows("es2_23"))
    assert n_blank > 0
    assert frame["subtype"].null_count() == n_blank * 29
    assert frame["subsubtype"].null_count() >= n_blank * 29
    assert "" not in frame["subtype"].drop_nulls().to_list()
    for name in ("country", "eu_scenario", "variable", "category", "type"):
        assert frame[name].null_count() == 0, name


def test_countries_are_kept_as_neso_names_without_a_code(data: Path) -> None:
    """Detects a country normalised to an ISO code or the 2023 ``Ireland (IAI)`` and the 2024
    ``SEM`` merged (TODO: GEOGRAPHY-MAPPING is non-holding): every country equals its cell byte
    for byte."""
    for alias in ("es2_23", "es2_24", "es2_25"):
        _run(data, alias)
    countries = set(_silver(data, ES2)["country"].to_list())
    assert countries == {r["Country"] for a in ("es2_23", "es2_24", "es2_25") for r in rows(a)}
    assert {"Ireland (IAI)", "SEM"} <= countries


def test_units_stay_in_variable_and_unit_columns_verbatim(data: Path) -> None:
    """Detects a unit parsed out of ``Variable``, a label normalised, a unit column invented for
    ES2 or a global unit applied (RULINGS 707): ES2 keeps ``Capacity (MW)`` / ``Demand (TWh)``
    whole in ``variable``; FLX1 and ED5 keep GW / GWh / TWh and ``# Cars`` / ``# Vans`` per row."""
    for alias in TYPED_UNPIVOT:
        _run(data, alias)
    es2 = _silver(data, ES2)
    assert set(es2["variable"].to_list()) == {"Capacity (MW)", "Demand (TWh)"}
    assert not [c for c in es2.columns if "unit" in c.lower()]
    flx1 = _silver(data, FLX1)
    assert set(flx1["unit"].to_list()) == {"GW", "GWh", "TWh"}
    ed5 = _silver(data, ED5)
    assert {"GW", "TWh", "# Cars", "# Vans"} <= set(ed5["unit"].to_list())
    flx1_cells = {r["Unit"] for a in ("flx1_24", "flx1_25", "flx1_26") for r in rows(a)}
    assert set(flx1["unit"].to_list()) == flx1_cells


def test_negative_values_survive(data: Path) -> None:
    """Detects a sign lost: a negative projection cell (FLX1 / ED5 flows and V2G) is a negative
    value equal to the vendor's, in every family that has one."""
    for alias in TYPED_UNPIVOT:
        _run(data, alias)
    for family in (FLX1, ED5):
        aliases = [a for a in TYPED_UNPIVOT if CAPTURES[a].family == family]
        negatives = sorted(
            float(v)
            for a in aliases
            for r in rows(a)
            for k, v in r.items()
            if k.isdigit() and v.startswith("-")
        )
        assert negatives, family
        frame = _silver(data, family)
        assert sorted(frame.filter(pl.col("value") < 0)["value"].to_list()) == negatives


@pytest.mark.parametrize(
    ("alias", "label", "value"),
    [
        ("es2_24", "2030", "3,118.00"),
        ("es2_25", "2040", "0.03 "),
        ("flx1_24", "2030", "6.06 "),
        ("flx1_26", "2030", "N/A"),
        ("ed5_25", "2040", "-163.99 "),
        ("ed5_23", "2030", "1,000"),
    ],
)
def test_an_uncastable_cell_fails_the_capture(
    data: Path, alias: str, label: str, value: str
) -> None:
    """Detects a record that swallows a bad cell (a null-token list, a lenient cast, a thousands
    separator, padding): the lexical forms that break other bodies (``3,118.00``, ``N/A``,
    ``6.06 ``) fail the whole capture before any row is written and leave no completion."""
    family = CAPTURES[alias].family
    raw = _replace_cell(body(alias), 2, table(alias)[0].index(label), value)
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize(
    ("alias", "column"),
    [("es2_24", "Country"), ("flx1_25", "Unit"), ("ed5_24", "Data item"), ("es2_23", "Variable")],
)
def test_a_blank_identifier_is_excluded_and_counted(data: Path, alias: str, column: str) -> None:
    """Detects an identifier made nullable (a blank ``Country`` / ``Unit`` / ``Data item`` /
    ``Variable`` becoming a null-keyed row): the source row's every long row is excluded, counted
    on the transformer and in the completion, and no null reaches the output."""
    family = CAPTURES[alias].family
    header, records = table(alias)
    per_row = len(label_columns(header))
    raw = _replace_cell(body(alias), 2, header.index(column), "")
    capture_id = capture(data, alias, raw=raw)
    transformer = get_transformer(SOURCE, family, data)
    written = transformer.run(DAY, run_id="r")
    assert written == (len(records) - 1) * per_row
    assert transformer.last_excluded_row_count == per_row
    completion = read_completion(data, family, capture_id)
    assert completion is not None and completion["rows_excluded"] == per_row
    assert _silver(data, family)[COLUMNS[family][column][0]].null_count() == 0


@pytest.mark.parametrize(
    ("alias", "cell"),
    [("es2_23", "2030"), ("flx1_25", "2030"), ("ed5_26", "2030")],
)
def test_a_repeated_dimension_tuple_fails_the_capture(data: Path, alias: str, cell: str) -> None:
    """Detects a key too coarse to guard the body: a repeated dimension tuple with another value
    fails with ``DuplicateEntityKeyError`` naming the shared rows and leaves no completion."""
    family = CAPTURES[alias].family
    raw = body(alias)
    text = raw.decode("utf-8-sig").split("\r\n")
    header = next(csv.reader([text[0]]))
    first = text[1]
    fields = next(csv.reader([first]))
    index = header.index(cell)
    fields[index] = "123.456"
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    duplicate = out.getvalue()
    assert duplicate != first
    capture_id = capture(data, alias, raw=raw + duplicate.encode() + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert (
        f"{2 * len(label_columns(header))} row(s) sharing an entity key"
        in (info.value.failures[0][2])
    )
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("family", UNPIVOT_FAMILIES)
def test_a_header_outside_the_epochs_fails_loud(data: Path, family: str) -> None:
    """Detects a lenient header match: a body whose first column is renamed (a vendor schema
    change) matches no frozen epoch and fails the capture, leaving no completion and no silver."""
    alias = next(a for a in TYPED_UNPIVOT if CAPTURES[a].family == family)
    capture_id = capture(data, alias, raw=_replace_cell(body(alias), 0, 0, "Renamed"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_a_reference_header_outside_the_epoch_fails_loud(data: Path) -> None:
    """Detects an ED6 body with a changed header read anyway: the single epoch is exact."""
    capture_id = capture(data, "ed6_25", raw=_replace_cell(body("ed6_25"), 0, 1, "Kind"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, ED6, data).run(DAY, run_id="r")
    assert read_completion(data, ED6, capture_id) is None
    assert _no_silver(data, ED6)


# --------------------------------------------------------------------------- #
# ED6: separator rows, newlines, key
# --------------------------------------------------------------------------- #


def test_ed6_separator_rows_are_dropped_by_the_reader_not_deduplicated(data: Path) -> None:
    """Detects the all-blank separator rows (FACTS: nine in ED6 2023) reaching the casting or key
    guard, or a dedup / occurrence index standing in for the reader's blank-row rule: the
    fixture's trailing separators give exactly the populated rows, zero exclusions (the
    between-rows case is the next test)."""
    records = lines("ed6_23")
    assert sum(not any(r) for r in records) == 3
    capture_id, written = _run(data, "ed6_23")
    assert written == len(table("ed6_23")[1]) == 4
    completion = read_completion(data, ED6, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0


def test_ed6_separators_between_populated_rows_give_the_same_rows(data: Path) -> None:
    """Detects blank rows between populated records breaking the reader or the key: the same
    four silver rows with separators between them as without."""
    raw = body("ed6_23").split(b"\r\n")
    between = b"\r\n".join(
        [raw[0], raw[1], b",,", b",,", raw[2], b",,", raw[3], raw[4], b",,", b""]
    )
    _, written = _run(data, "ed6_23", raw=between)
    assert written == 4
    frame = _silver(data, ED6)
    assert sorted(frame["category"].to_list()) == sorted(r["Category"] for r in rows("ed6_23"))


def test_ed6_embedded_newlines_and_commas_are_kept_byte_equal(data: Path) -> None:
    """Detects an embedded newline flattened or a comma-bearing description split (FACTS: 19 cells
    with a newline in each of 2024-2026): every description equals its cell exactly and at least
    one contains a newline."""
    for alias in ("ed6_24", "ed6_25", "ed6_26"):
        _run(data, alias)
    frame = _silver(data, ED6)
    expected = {r["Description"] for a in ("ed6_24", "ed6_25", "ed6_26") for r in rows(a)}
    assert set(frame["description"].to_list()) == expected
    assert any("\n" in d for d in frame["description"].to_list())
    assert any("," in d for d in frame["description"].to_list())


def test_ed6_is_keyed_by_column_name_and_category_per_edition(data: Path) -> None:
    """Detects a key that merges editions or ignores the category: the same ``(column_name,
    category)`` appears in several editions as separate rows, and the key has no duplicate."""
    for alias in TYPED_ED6:
        _run(data, alias)
    frame = _silver(data, ED6)
    assert set(frame["edition"].to_list()) == {2023, 2024, 2025, 2026}
    keys = frame.select("edition", "column_name", "category")
    assert keys.is_duplicated().sum() == 0
    assert (
        frame.group_by("column_name", "category")
        .agg(pl.col("edition").n_unique())
        .get_column("edition")
        .max()
        or 0
    ) > 1


def test_ed6_repeated_populated_key_fails_the_capture(data: Path) -> None:
    """Detects a duplicate populated ``(Column, Category)`` silently dropped (no dedup): a
    repeated populated record fails with ``DuplicateEntityKeyError``, no completion."""
    raw = body("ed6_23")
    first = raw.split(b"\r\n")[1]
    capture_id = capture(data, "ed6_23", raw=raw + first + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, ED6, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert read_completion(data, ED6, capture_id) is None
    assert _no_silver(data, ED6)


# --------------------------------------------------------------------------- #
# Editions
# --------------------------------------------------------------------------- #


def test_editions_are_stamped_from_the_filename_and_all_are_served(data: Path) -> None:
    """Detects an edition inferred from the vintage or the year labels, or one edition displacing
    another: FLX1 2024 (vintage 2025), 2025 and 2026 stamp their filename's year and ``_latest``
    serves each."""
    ids = {a: _run(data, a)[0] for a in ("flx1_24", "flx1_25", "flx1_26")}
    frame = _silver(data, FLX1)
    by_capture = {
        cid: set(editions)
        for cid, editions in frame.group_by("bronze_capture_id")
        .agg(pl.col("edition").unique())
        .iter_rows()
    }
    assert by_capture == {ids["flx1_24"]: {2024}, ids["flx1_25"]: {2025}, ids["flx1_26"]: {2026}}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, FLX1, None)) == set(ids.values())


@pytest.mark.parametrize(
    ("alias", "wrong"),
    [
        ("es2_24", "fes2026_es2_v001.csv"),
        ("flx1_24", "fes2024_flx1_v003.csv.bak"),
        ("flx1_25", "fes2023_flx1_v001.csv"),
        ("ed5_25", "fes_ed5.csv"),
        ("ed6_24", "fes2022_ed6_v001.csv"),
        ("es2_25", "10yo2026_es2_v001.csv"),
    ],
)
def test_an_unmapped_filename_fails_loud(data: Path, alias: str, wrong: str) -> None:
    """Detects a fallback edition: a filename the map does not list (a new year, a suffixed copy,
    a held resource's name) fails the capture, leaves no completion and writes nothing."""
    family = CAPTURES[alias].family
    capture_id = capture(data, alias, filename=wrong)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


# --------------------------------------------------------------------------- #
# Dispositions: the two held resources
# --------------------------------------------------------------------------- #


def test_the_two_resources_are_held_with_their_reasons() -> None:
    """Detects a resource routed to silver, dispositioned DOC/GIS, given an edition entry or left
    without its reason: ES2 2026 and FLX1 2023 alone are resource-level HOLD (E-SEM) quoting the
    FACTS evidence, every other resource of the four packages is SILVER to its own family, the
    four packages stay eligible and the held filenames have no edition pair."""
    registry = load_registry()
    for alias, needles in HOLD_REASONS.items():
        meta = CAPTURES[alias]
        held = registry.resources[meta.resource_id][1]
        assert held.family == meta.family
        assert isinstance(held.disposition, HoldDisposition)
        assert held.disposition.unit == "E-SEM"
        for needle in needles:
            assert needle in held.disposition.reason, (alias, needle)
        mapped = {fn for fn, _edition in _record(meta.family).edition_by_filename or ()}
        assert meta.filename not in mapped
    for family in FAMILIES:
        slug, _id = PACKAGES[family]
        package = next(p for p in registry.packages if p.package == slug)
        assert package.eligibility.status == "eligible"
        for resource in package.resources:
            assert resource.family == family
            if resource.id in {CAPTURES[a].resource_id for a in HELD_ALIASES}:
                assert isinstance(resource.disposition, HoldDisposition)
            else:
                assert isinstance(resource.disposition, SilverDisposition)
                assert resource.disposition.key == family
        silver = {r.id for r in package.resources if isinstance(r.disposition, SilverDisposition)}
        assert silver == {CAPTURES[a].resource_id for a in TYPED if CAPTURES[a].family == family}


def test_the_held_bodies_are_not_readable_by_the_record(data: Path) -> None:
    """Detects a body the record could read (the claim behind each HOLD): the ES2 2026 notes
    header (``Notes``, ``Description``) and the FLX1 2023 header (``Scenario``, ``2021``-``2050``)
    match no epoch, and the FLX1 2023 projection cells are all padded numbers whose strict float64
    cast fails."""
    for alias in HELD_ALIASES:
        meta = CAPTURES[alias]
        record = _record(meta.family)
        capture(data, alias)
        path = next((data / "bronze" / SOURCE / meta.family).rglob(f"*{meta.resource_id}*.csv"))
        parsed = next(read_csv_body(path, record, ()))
        assert not any(list(epoch.header) == list(parsed.header) for epoch in record.epochs)
    padded = [v for r in rows("flx1_23") for k, v in r.items() if k.isdigit() and v != ""]
    assert padded and all(v != v.strip() for v in padded)
    with pytest.raises(InvalidOperationError):
        pl.Series(padded, dtype=pl.String).cast(pl.Float64, strict=True)
    assert pl.Series(padded, dtype=pl.String).str.strip_chars().cast(pl.Float64, strict=True).len()


def test_held_captures_are_never_transformed_and_not_a_gap(data: Path) -> None:
    """Detects a held capture reaching an owner (a failed capture and a reconcile gap) or expected
    by reconcile despite its HOLD: with every fixture captured the SILVER editions complete alone,
    the two held captures have no completion and no failure, and reconcile reports no gap."""
    held_ids = {a: capture(data, a) for a in HELD_ALIASES}
    for alias in TYPED:
        capture(data, alias)
    for family in FAMILIES:
        aliases = [a for a in TYPED if CAPTURES[a].family == family]
        expected = sum(
            len(rows(a)) * len(label_columns(table(a)[0])) if family != ED6 else len(rows(a))
            for a in aliases
        )
        assert get_transformer(SOURCE, family, data).run(DAY, run_id="r") == expected, family
    for alias, capture_id in held_ids.items():
        family = CAPTURES[alias].family
        assert read_completion(data, family, capture_id) is None, alias
        assert read_failure(data, family, capture_id) is None, alias
    report = reconcile(data, load_registry(), list(FAMILIES), DAY)
    assert report.gaps == (), report.lines()
    for family in FAMILIES:
        frame = _silver(data, family)
        assert set(frame["resource_id"].to_list()) == {
            CAPTURES[a].resource_id for a in TYPED if CAPTURES[a].family == family
        }


# --------------------------------------------------------------------------- #
# Vintage and generated artefacts
# --------------------------------------------------------------------------- #


def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(data: Path) -> None:
    """Detects an issue-time proxy or a projection-year clock (RULINGS 529/597), or a catalogue
    view that cannot carry the unpivot columns: ``available_at`` is the CKAN ``last_modified``
    (FLX1 2024: 2025-06-27, not the 2024 edition year), ``timestamp_utc`` stays the capture time,
    an as-of read before the vintage serves nothing and one after it serves the capture."""
    meta = CAPTURES["flx1_24"]
    capture_id, _ = _run(data, "flx1_24")
    frame = _silver(data, FLX1)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, FLX1, datetime(2025, 6, 26, tzinfo=UTC)) == []
    assert set(both_as_of(db, data, FLX1, datetime(2025, 6, 28, tzinfo=UTC))) == {capture_id}
    assert set(both_as_of(db, data, FLX1, None)) == {capture_id}


@pytest.mark.parametrize(
    ("family", "title", "needles"),
    [
        (ES2, "European Electricity Supply Data Table (ES2)", ("`projection_year`", "`variable`")),
        (FLX1, "Flexibility data table (FLX1)", ("`projection_year`", "`pathway`", "`unit`")),
        (ED5, "Road Transport Summary (ED5)", ("`projection_year`", "`peak_annual_minimum`")),
        (ED6, "Road Transport Notes (ED6)", ("`column_name`", "`description`")),
    ],
)
def test_the_skeleton_page_renders_the_new_record(
    family: str, title: str, needles: tuple[str, ...]
) -> None:
    """Detects a record the docs generator cannot render (the unpivot columns, the held question,
    the held resources)."""
    slug, _id = PACKAGES[family]
    page = skeleton.render_package(
        load_registry(),
        {
            "name": slug,
            "title": title,
            "organization": {"title": "FES: Pathways to Net Zero"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    for needle in (*needles, family):
        assert needle in page, needle


def test_every_fixture_is_named_in_the_provenance() -> None:
    """Detects a fixture left out of the provenance table: every committed fixture is named in
    ``PROVENANCE.md`` with its resource id."""
    provenance = (FIXTURES / "PROVENANCE.md").read_text(encoding="utf-8")
    for alias, meta in CAPTURES.items():
        assert f"`{alias}.csv`" in provenance, alias
        assert meta.resource_id in provenance, alias
    committed = {p.stem for p in FIXTURES.glob("*.csv")}
    assert committed == set(CAPTURES)
    assert re.search(r"^## Held questions", provenance, re.MULTILINE)
