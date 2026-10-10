"""The tRESP demand and generation pathway frozen records (v0.22-K-SCN-1a).

Ten already-long CSV families (five demand, five generation; one resource and one header epoch
each) and two reference families read from the ``tresp_demand_pathways_files`` workbook.
Every test writes recorded fixture captures into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here,
not at activation. On master none of the twelve families has a record, so ``get_transformer``
raises for each of them.

Fixtures (``tests/fixtures/neso_data_portal/scn1a/``, provenance in ``PROVENANCE.md``): each CSV
is a cut of the 2026-10-08 swept bronze (a scratch script, not committed): two geographies (the
first, and one whose code carries ``@`` / ``|`` for the GSP files), the building blocks that show
every unit (demand ``GWh`` / ``m2`` / ``Number``; generation ``MW`` only), the years 2025, 2034,
2035, 2036 and 2050 so that the shared 2035 boundary is present in all three pathways, the first
zero value, and (where the real body has one) a scientific-notation ``Value`` cell. The workbook
is a byte copy of the real body. ``git`` normalises a committed CSV fixture's line endings, so
:func:`body` rebuilds the bronze original's CRLF convention.

Record decisions under test (K-SCN-1 FACTS g1-g6):

- ``Year`` -> ``projection_year`` int64 and ``Value`` -> ``value`` float64, both ordinary
  non-nullable columns (no unpivot, no edition map): the measured bodies hold zero blank or
  tokenised cells, so a blank cell excludes its row (counted, never written as null) and a
  non-numeric cell fails the capture.
- ``unit`` is kept per row as the vendor states it (demand mixes ``GWh``, ``m2`` and ``Number``).
- The vendor identifiers (pathway labels, block ids, GSP area ids, LAD codes and names, RESP
  region labels) are stored as written.
- Demand is held (E-SEM) on the annual-energy interval; generation is eligible. The record model
  has no free-text eligible field, so "``Year`` is the observation date of installed capacity at
  31 March" is recorded here and in the unit notes, not in the registry.
"""

from __future__ import annotations

import csv
import hashlib
import io
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
from _neso_generic_support import write_capture
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
from gridflow.connectors.neso_data_portal.registry.record import XlsxSpec
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.casting import DuplicateEntityKeyError
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
)
from gridflow.silver.neso_data_portal.containers import open_container
from gridflow.silver.neso_data_portal.readers import XlsxBlockError, read_sheet
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "scn1a"
DAY = date(2026, 10, 8)
DEMAND_PACKAGE_ID = "e5e8eb8e-9fbd-4355-b4ad-9bbf00569d15"
GENERATION_PACKAGE_ID = "7f30dbc0-5d71-412a-b4f3-7e852ff7dfa7"
WORKBOOK_RESOURCE = "11c36b60-eee5-45e9-b0da-6240e6e60d1c"
WORKBOOK_SHA256 = "fe3dde0f91c11acd6a3daf7f8eb0255dabb32c0203108ccbe7e2d7acb96d902e"
WORKBOOK_VINTAGE = "2026-01-30T08:15:29.726307"
WORKBOOK_WRITTEN = "2026-10-08T11:39:46.157281+00:00"

HEADER_GSP = [
    "Building_block_id",
    "tRESP_GSP_area",
    "Pathway",
    "Year",
    "Value",
    "Unit",
    "DNO_licence_area",
]
HEADER_LA = ["Building_block_id", "LAD24CD", "LAD24NM", "Pathway", "Year", "Unit", "Value"]
HEADER_RR = ["Building_block_id", "RESP_region", "Pathway", "Year", "Value", "Unit"]

HELD_QUESTION = (
    "TODO: for the annual-energy building blocks (`Unit` = GWh), what interval does the `Year` "
    "label cover — the financial year ending 31 March of that year, the calendar year, or "
    "another interval? The tRESP dictionary dates volumes at 31 March but states no interval "
    "for annual energy. Also: which FES definition applies to the suffixed ids "
    "`Dem_BB005_1/_2`, `Lct_BB015_1/_2` (no exact FES building-block id)."
)

PATHWAYS = ("tRESP - HT", "tRESP - EE", "tRESP - HE")


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture and its sidecar provenance."""

    fixture: str
    family: str
    package: str
    package_id: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str
    header: list[str]
    geo: str
    """The geography code column the record's key uses."""
    demand: bool


CAPTURES: dict[str, Capture] = {
    "d_gsp": Capture(
        "d_gsp",
        "tresp_demand_pathways_gsp",
        "tresp-demand-pathways",
        DEMAND_PACKAGE_ID,
        "d616d330-4829-4f8a-b74b-00a57b41bc20",
        "tRESP Demand Pathways per Grid Supply Point Area",
        "tresp_pathways_demand_published.csv",
        "2026-01-30T08:24:47.341529",
        "2026-10-08T11:39:50.139049+00:00",
        HEADER_GSP,
        "tRESP_GSP_area",
        True,
    ),
    "d_lae": Capture(
        "d_lae",
        "tresp_demand_pathways_la_england",
        "tresp-demand-pathways",
        DEMAND_PACKAGE_ID,
        "c3a261a4-94e9-4d75-b0de-d3805b0b5ae8",
        "tRESP Indicative Demand Pathways per Local Authority for England",
        "la_england_pathways_demand_published.csv",
        "2026-01-30T08:21:33.560888",
        "2026-10-08T11:39:54.204836+00:00",
        HEADER_LA,
        "LAD24CD",
        True,
    ),
    "d_las": Capture(
        "d_las",
        "tresp_demand_pathways_la_scotland",
        "tresp-demand-pathways",
        DEMAND_PACKAGE_ID,
        "67d699eb-c5e9-42cd-a57a-74fcc52bcea9",
        "tRESP Indicative Demand Pathways per Local Authority for Scotland",
        "la_scotland_pathways_demand_published.csv",
        "2026-01-30T08:19:49.400933",
        "2026-10-08T11:39:57.658066+00:00",
        HEADER_LA,
        "LAD24CD",
        True,
    ),
    "d_law": Capture(
        "d_law",
        "tresp_demand_pathways_la_wales",
        "tresp-demand-pathways",
        DEMAND_PACKAGE_ID,
        "ccb3dfc8-ab4a-4f32-b944-8eced2e87bff",
        "tRESP Indicative Demand Pathways per Local Authority for Wales",
        "la_wales_pathways_demand_published.csv",
        "2026-01-30T08:18:41.157016",
        "2026-10-08T11:40:02.562200+00:00",
        HEADER_LA,
        "LAD24CD",
        True,
    ),
    "d_rr": Capture(
        "d_rr",
        "tresp_demand_pathways_resp_region",
        "tresp-demand-pathways",
        DEMAND_PACKAGE_ID,
        "cb1f1b31-1fde-40ab-86c9-34a3e1bdb665",
        "tRESP Demand Pathways per RESP Nation and Region",
        "tresp_pathways_demand_by_resp_region_published.csv",
        "2026-01-30T08:23:31.655328",
        "2026-10-08T11:40:06.710260+00:00",
        HEADER_RR,
        "RESP_region",
        True,
    ),
    "g_gsp": Capture(
        "g_gsp",
        "tresp_generation_pathways_gsp",
        "tresp-generation-pathways",
        GENERATION_PACKAGE_ID,
        "970b7a25-6086-4b07-b109-b01ae43ae78c",
        "tRESP Generation Pathways per Grid Supply Point Area",
        "tresp_pathways_generation_storage_published.csv",
        "2026-01-30T08:36:50.063067",
        "2026-10-08T11:40:10.436470+00:00",
        HEADER_GSP,
        "tRESP_GSP_area",
        False,
    ),
    "g_lae": Capture(
        "g_lae",
        "tresp_generation_pathways_la_england",
        "tresp-generation-pathways",
        GENERATION_PACKAGE_ID,
        "d20aec73-01df-486d-a1aa-390c9cab2976",
        "tRESP Indicative Generation Pathways per Local Authority for England",
        "la_england_pathways_generation_storage_published.csv",
        "2026-01-30T08:31:42.342845",
        "2026-10-08T11:40:14.933216+00:00",
        HEADER_LA,
        "LAD24CD",
        False,
    ),
    "g_las": Capture(
        "g_las",
        "tresp_generation_pathways_la_scotland",
        "tresp-generation-pathways",
        GENERATION_PACKAGE_ID,
        "38379002-1fcf-461a-beb1-2b15b555f994",
        "tRESP Indicative Generation Pathways per Local Authority for Scotland",
        "la_scotland_pathways_generation_storage_published.csv",
        "2026-01-30T08:28:56.090445",
        "2026-10-08T11:40:18.223960+00:00",
        HEADER_LA,
        "LAD24CD",
        False,
    ),
    "g_law": Capture(
        "g_law",
        "tresp_generation_pathways_la_wales",
        "tresp-generation-pathways",
        GENERATION_PACKAGE_ID,
        "62275682-ed2c-44e8-aede-8404b19f3d67",
        "tRESP Indicative Generation Pathways per Local Authority for Wales",
        "la_wales_pathways_generation_storage_published.csv",
        "2026-01-30T08:27:52.691429",
        "2026-10-08T11:40:21.606872+00:00",
        HEADER_LA,
        "LAD24CD",
        False,
    ),
    "g_rr": Capture(
        "g_rr",
        "tresp_generation_pathways_resp_region",
        "tresp-generation-pathways",
        GENERATION_PACKAGE_ID,
        "f295b8b5-8022-433d-b07f-b75b729fb199",
        "tRESP Generation Pathways per RESP Nation and Region",
        "tresp_pathways_generation_storage_by_resp_region_published.csv",
        "2026-01-30T08:34:24.152349",
        "2026-10-08T11:40:26.441105+00:00",
        HEADER_RR,
        "RESP_region",
        False,
    ),
}
DEMAND = tuple(alias for alias, meta in CAPTURES.items() if meta.demand)
GENERATION = tuple(alias for alias, meta in CAPTURES.items() if not meta.demand)

SILVER_NAMES = {
    "Building_block_id": "building_block_id",
    "tRESP_GSP_area": "tresp_gsp_area",
    "LAD24CD": "lad24cd",
    "LAD24NM": "lad24nm",
    "RESP_region": "resp_region",
    "Pathway": "pathway",
    "Year": "projection_year",
    "Value": "value",
    "Unit": "unit",
    "DNO_licence_area": "dno_licence_area",
}
GEO_KEY = {"tRESP_GSP_area": "tresp_gsp_area", "LAD24CD": "lad24cd", "RESP_region": "resp_region"}

BB_FAMILY = "tresp_building_block_definitions"
GSP_FAMILY = "tresp_gsp_area_names"
BB_HEADER = [
    "Demand/ Generation/ Storage",
    "Technology",
    "FES BB ID Number",
    "Technology Detail",
    "Units",
]
GSP_HEADER = ["tresp_gsp_area", "tresp_gsp_area_name"]


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
    """Fixture ``alias`` with the bronze original's CRLF line endings."""
    raw = (FIXTURES / f"{CAPTURES[alias].fixture}.csv").read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n")


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed."""
    text = (raw if raw is not None else body(alias)).decode("utf-8")
    return list(csv.DictReader(io.StringIO(text, newline="")))


def capture(
    data: Path, alias: str, *, raw: bytes | None = None, written: datetime | None = None
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=written or datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=meta.package,
        package_id=meta.package_id,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
    )
    return capture_id_for(path, data)


def capture_workbook(data: Path, raw: bytes | None = None) -> str:
    """Write the real workbook (or ``raw``) into the files family's bronze."""
    path, _sidecar = write_capture(
        data,
        "tresp_demand_pathways_files",
        body=raw if raw is not None else (FIXTURES / "tresp_lists.xlsx").read_bytes(),
        written_at=datetime.fromisoformat(WORKBOOK_WRITTEN).astimezone(UTC),
        partition=DAY,
        package_slug="tresp-demand-pathways",
        package_id=DEMAND_PACKAGE_ID,
        resource_id=WORKBOOK_RESOURCE,
        resource_name="Lists of tRESP Pathways Building Blocks and of tRESP GSP areas with names",
        resource_filename="lists-of-tresp-pathways-building-blocks-and-of-tresp-gsp-areas-with-names.xlsx",
        ckan_last_modified=WORKBOOK_VINTAGE,
        url_type="upload",
        ckan_format="XLSX",
        extension="xlsx",
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _key(alias: str) -> list[str]:
    meta = CAPTURES[alias]
    return ["resource_id", "building_block_id", GEO_KEY[meta.geo], "pathway", "projection_year"]


# --------------------------------------------------------------------------- #
# Fixtures and record shapes
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact header, the
    three demand units and the single generation unit, all three pathways, the 2035 boundary
    year in each pathway (and 2025 / 2034 only in the short-term HT pathway), a zero value,
    the scientific-notation cell in generation, the CRLF convention, and a GSP area id that
    carries ``@`` and ``|``."""
    for alias, meta in CAPTURES.items():
        assert list(rows(alias)[0]) == meta.header, alias
        assert body(alias).count(b"\r\n") == len(rows(alias)) + 1, alias
        assert body(alias).count(b"\n") == body(alias).count(b"\r\n"), alias
        by_pathway = {p: {r["Year"] for r in rows(alias) if r["Pathway"] == p} for p in PATHWAYS}
        assert set(by_pathway) == set(PATHWAYS), alias
        assert all("2035" in years for years in by_pathway.values()), alias
        assert {"2025", "2034"} <= by_pathway["tRESP - HT"], alias
        assert not {"2025", "2034"} & (by_pathway["tRESP - EE"] | by_pathway["tRESP - HE"]), alias
        assert any(float(r["Value"]) == 0.0 for r in rows(alias)), alias
    for alias in DEMAND:
        assert {r["Unit"] for r in rows(alias)} == {"GWh", "m2", "Number"}, alias
    for alias in GENERATION:
        assert {r["Unit"] for r in rows(alias)} == {"MW"}, alias
        assert any("e-" in r["Value"] for r in rows(alias)), alias
    assert any("@" in r["tRESP_GSP_area"] and "|" in r["tRESP_GSP_area"] for r in rows("d_gsp"))
    assert any("@" in r["tRESP_GSP_area"] for r in rows("g_gsp"))


@pytest.mark.parametrize("alias", list(CAPTURES))
def test_record_shape_matches_the_unit_spec(alias: str) -> None:
    """Detects a record that drifts from the spec: one csv epoch with the exact vendor header,
    ``Year`` an int64 ``projection_year`` and ``Value`` a float64 ``value`` (both ordinary
    non-nullable columns, no unpivot), every other column a non-nullable string under its
    vendor-derived name, no null tokens, no edition map, ``temporal none``, no issue time,
    whole-capture selection per ``resource_id``, ``ckan_last_modified`` vintage, and the
    entity key ``resource_id`` + block + geography code + pathway + year."""
    meta = CAPTURES[alias]
    record = _record(meta.family)
    assert (record.version, record.reader, record.encoding) == ("1", "csv", "utf-8")
    assert len(record.epochs) == 1
    epoch = record.epochs[0]
    assert list(epoch.header) == meta.header
    assert epoch.unpivot is None
    assert epoch.issue.kind == "none"
    assert [(c.source, c.name) for c in epoch.columns] == [
        (h, SILVER_NAMES[h]) for h in meta.header
    ]
    for column in epoch.columns:
        expected = {"projection_year": "int64", "value": "float64"}.get(column.name, "string")
        assert (column.dtype, column.nullable, column.null_tokens) == (expected, False, ()), (
            alias,
            column.name,
        )
    assert record.temporal.kind == "none"
    assert record.edition_by_filename is None
    assert record.latest == "whole_capture"
    assert record.latest_partition == "resource_id"
    assert record.vintage == "ckan_last_modified"
    assert list(record.entity_key) == _key(alias)
    assert record.xlsx is None
    assert record.siblings == ()


def test_demand_is_held_on_the_annual_interval_and_generation_is_eligible() -> None:
    """Detects the demand hold lost (or reworded), a generation family held for a demand
    question, or a package-level hold swallowing the reference families: the five demand
    families carry the verbatim E-SEM question; the five generation families and both workbook
    reference families publish."""
    registry = load_registry()
    for alias in DEMAND:
        package, family = registry.families[CAPTURES[alias].family]
        held = effective_eligibility(package, family)
        assert held.status == "held", alias
        assert held.question == HELD_QUESTION, alias  # type: ignore[union-attr]
        assert held.unit == "E-SEM", alias  # type: ignore[union-attr]
    for key in (*(CAPTURES[a].family for a in GENERATION), BB_FAMILY, GSP_FAMILY):
        package, family = registry.families[key]
        assert effective_eligibility(package, family).status == "eligible", key
    # the record model has no free text on an eligible family: nothing hides a demand question
    for alias in GENERATION:
        assert _record(CAPTURES[alias].family).eligibility is None


def test_the_workbook_children_are_two_references_and_the_old_sheet_stays_held() -> None:
    """Detects a child routed to the wrong family, the old sheet read or reclassified as
    documentation, a GPKG leaving GIS, a reference family without its sibling, or a recipe
    that no longer matches the sheet: ``tRESP Building Blocks`` (A:E to row 36, key
    ``fes_bb_id_number``) and ``tRESP GSP Areas with Names`` (A:B to row 232, key
    ``tresp_gsp_area``) are sibling-fed, resource-partitioned and temporal none;
    ``tRESP BB list - old`` keeps a HOLD whose reason names the live row 48 after blank row 47
    and the missing contiguous recipe; the two GPKGs stay GIS."""
    registry = load_registry()
    workbook = registry.resources[WORKBOOK_RESOURCE][1]
    assert workbook.family == "tresp_demand_pathways_files"
    assert [(c.child, c.disposition.kind) for c in workbook.children] == [
        ("tRESP Building Blocks", "SILVER"),
        ("tRESP GSP Areas with Names", "SILVER"),
        ("tRESP BB list - old", "HOLD"),
    ]
    targets = {
        c.child: c.disposition.key
        for c in workbook.children
        if isinstance(c.disposition, SilverDisposition)
    }
    assert targets == {
        "tRESP Building Blocks": BB_FAMILY,
        "tRESP GSP Areas with Names": GSP_FAMILY,
    }
    old = workbook.children[2].disposition
    assert isinstance(old, HoldDisposition)
    assert "row 47" in old.reason and "row 48" in old.reason
    assert "Dem_BB001a" in old.reason and "contiguous" in old.reason
    assert isinstance(workbook.disposition, HoldDisposition)  # V-15b: two targets, so not SILVER

    gis = [
        r
        for _p, r in registry.resources.values()
        if r.family == "tresp_demand_pathways_files" and r.format == "GPKG"
    ]
    assert len(gis) == 2
    assert {r.disposition.kind for r in gis} == {"GIS"}

    for key, xlsx, entity in (
        (BB_FAMILY, XlsxSpec(header_row=1, columns="A:E", last_row=36), "fes_bb_id_number"),
        (GSP_FAMILY, XlsxSpec(header_row=1, columns="A:B", last_row=232), "tresp_gsp_area"),
    ):
        record = _record(key)
        assert record.reader == "xlsx"
        assert record.xlsx == xlsx
        assert record.siblings == ("tresp_demand_pathways_files",)
        assert list(record.entity_key) == ["resource_id", entity]
        assert (record.temporal.kind, record.latest, record.latest_partition) == (
            "none",
            "whole_capture",
            "resource_id",
        )
        assert record.vintage == "ckan_last_modified"
        assert record.edition_by_filename is None
        assert all(c.dtype == "string" for c in record.epochs[0].columns)
    bb = {c.name: c.nullable for c in _record(BB_FAMILY).epochs[0].columns}
    assert bb == {
        "demand_generation_storage": False,
        "technology": False,
        "fes_bb_id_number": False,
        "technology_detail": True,
        "units": False,
    }
    assert [c.source for c in _record(BB_FAMILY).epochs[0].columns] == BB_HEADER
    assert [c.source for c in _record(GSP_FAMILY).epochs[0].columns] == GSP_HEADER


# --------------------------------------------------------------------------- #
# The ten CSV families through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", list(CAPTURES))
def test_fixture_types_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch, a cast
    the vendor body does not satisfy (a scientific-notation value, a zero), any row excluded
    and a repeated entity key: the capture completes with every populated row, zero
    exclusions, an int64 ``projection_year``, a float64 ``value`` and a unique key."""
    meta = CAPTURES[alias]
    transformer = get_transformer(SOURCE, meta.family, data)
    capture_id = capture(data, alias)
    written = transformer.run(DAY, run_id="r")
    assert written == len(rows(alias))
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == len(rows(alias))
    assert completion["rows_excluded"] == 0

    frame = _silver(data, meta.family)
    expected = [name for name, _type in generic.output_columns(_record(meta.family))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected if c not in ("year", "month")
    ]
    assert frame.schema["projection_year"] == pl.Int64
    assert frame.schema["value"] == pl.Float64
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame.select(_key(alias)).is_duplicated().sum() == 0


@pytest.mark.parametrize("alias", list(CAPTURES))
def test_identifiers_units_zeros_and_values_survive_byte_identical(data: Path, alias: str) -> None:
    """Detects a normalised identifier (case-folded or suffix-stripped block id, a rewritten
    ``@`` / ``|`` GSP area, a re-spelled pathway label, a trimmed LAD name), a dropped or
    converted unit, a zero turned into null, or a value re-rounded: every string column equals
    its CSV cell and every year and value equals its parsed cell, in order."""
    meta = CAPTURES[alias]
    capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    source = rows(alias)
    assert frame.height == len(source)
    for header in meta.header:
        name = SILVER_NAMES[header]
        if name == "projection_year":
            assert frame[name].to_list() == [int(r[header]) for r in source]
        elif name == "value":
            assert frame[name].to_list() == [float(r[header]) for r in source]
            assert frame[name].null_count() == 0
        else:
            assert frame[name].to_list() == [r[header] for r in source], (alias, name)
            assert frame.schema[name] == pl.Utf8
    assert frame["value"].to_list().count(0.0) == [float(r["Value"]) for r in source].count(0.0)


@pytest.mark.parametrize("alias", list(CAPTURES))
def test_the_shared_boundary_year_is_kept_once_per_pathway(data: Path, alias: str) -> None:
    """Detects a key that drops ``pathway`` (the 2035 row of HT, EE and HE collapsing into one
    or failing as a duplicate): each block and geography of the cut has one 2035 row in each of
    the three pathways (at least four such cells), all kept, and the pathway labels are stored
    as written."""
    meta = CAPTURES[alias]
    capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    boundary = frame.filter(pl.col("projection_year") == 2035)
    geo = GEO_KEY[meta.geo]
    per_cell = boundary.group_by("building_block_id", geo).agg(pl.col("pathway").sort())
    cells = per_cell["pathway"].to_list()
    # a cell the cut added for its zero or scientific-notation value may hold fewer pathways
    assert all(len(set(c)) == len(c) for c in cells)
    assert sum(c == sorted(PATHWAYS) for c in cells) >= 4
    assert set(frame["pathway"].to_list()) == set(PATHWAYS)


def test_demand_keeps_a_unit_per_row_and_each_block_has_one_unit(data: Path) -> None:
    """Detects one scalar unit assigned to demand ``value`` (or a unit lost): the GWh, m2 and
    Number rows keep their own unit on every row, and a block never changes unit (K-SCN-1
    FACTS g4)."""
    capture(data, "d_lae")
    get_transformer(SOURCE, CAPTURES["d_lae"].family, data).run(DAY, run_id="r")
    frame = _silver(data, CAPTURES["d_lae"].family)
    per_block = frame.group_by("building_block_id").agg(pl.col("unit").unique())
    units = {r["building_block_id"]: r["unit"] for r in per_block.to_dicts()}
    assert all(len(v) == 1 for v in units.values())
    assert {b: units[b] for b in ("Dem_BB005_1", "Dem_BB005_2", "Lct_BB001")} == {
        "Dem_BB005_1": ["GWh"],
        "Dem_BB005_2": ["m2"],
        "Lct_BB001": ["Number"],
    }


# --------------------------------------------------------------------------- #
# A body that breaks the record fails loud
# --------------------------------------------------------------------------- #


def _replace_cell(raw: bytes, row: int, column: int, value: str) -> bytes:
    lines = raw.split(b"\r\n")
    fields = next(csv.reader([lines[row].decode("utf-8")]))
    fields[column] = value
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    lines[row] = out.getvalue().encode("utf-8")
    return b"\r\n".join(lines)


@pytest.mark.parametrize(
    ("alias", "header", "value"),
    [("g_lae", "Value", "n/a"), ("d_rr", "Year", "2035.5"), ("d_gsp", "Value", "1,5")],
)
def test_an_uncastable_cell_fails_the_capture(
    data: Path, alias: str, header: str, value: str
) -> None:
    """Detects a record that swallows a bad cell (a token list, a lenient year cast, a decimal
    comma): a non-numeric ``Value`` or a fractional ``Year`` fails the whole capture before any
    row is written and leaves no completion."""
    meta = CAPTURES[alias]
    raw = _replace_cell(body(alias), 3, meta.header.index(header), value)
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert read_completion(data, meta.family, capture_id) is None
    assert not list((data / "silver" / SOURCE / meta.family).rglob("[!.]*.parquet"))


@pytest.mark.parametrize(
    ("alias", "header"),
    [
        ("d_gsp", "Value"),
        ("g_law", "Year"),
        ("d_las", "Building_block_id"),
        ("g_rr", "Pathway"),
    ],
)
def test_a_blank_cell_is_excluded_and_counted_never_written_as_null(
    data: Path, alias: str, header: str
) -> None:
    """Detects a nullable column (a blank ``Value`` / ``Year`` / key becoming a null row) or a
    silent drop: the measured bodies hold no blanks, so a blank cell excludes its row only,
    the exclusion is counted on the transformer and in the completion, and the other rows are
    written with no null in the column."""
    meta = CAPTURES[alias]
    raw = _replace_cell(body(alias), 3, meta.header.index(header), "")
    capture_id = capture(data, alias, raw=raw)
    transformer = get_transformer(SOURCE, meta.family, data)
    written = transformer.run(DAY, run_id="r")
    assert written == len(rows(alias)) - 1
    assert transformer.last_excluded_row_count == 1
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None
    assert (completion["row_count"], completion["rows_excluded"]) == (written, 1)
    frame = _silver(data, meta.family)
    assert frame[SILVER_NAMES[header]].null_count() == 0


@pytest.mark.parametrize("alias", ["d_gsp", "g_rr"])
def test_a_repeated_entity_key_fails_the_capture(data: Path, alias: str) -> None:
    """Detects a key too coarse to guard the body: a second row for the same block,
    geography, pathway and year with another value fails with ``DuplicateEntityKeyError``
    and leaves no completion."""
    meta = CAPTURES[alias]
    raw = body(alias)
    first = raw.split(b"\r\n")[1]
    changed = _replace_cell(first + b"\r\n", 0, meta.header.index("Value"), "123456.789")
    capture_id = capture(data, alias, raw=raw + changed.rstrip(b"\r\n") + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert read_completion(data, meta.family, capture_id) is None


# --------------------------------------------------------------------------- #
# Vintage: the publication date, never the projection year
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ["d_gsp", "g_lae"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy or a projection-year clock (RULINGS 529/597): ``available_at``
    is the CKAN ``last_modified`` (2026-01-30), ``timestamp_utc`` stays the capture time, an as-of
    read before the vintage serves nothing even though the capture is later, and a read after
    the vintage serves the capture while ``projection_year`` (2025-2050) plays no part."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, meta.family, datetime(2026, 1, 29, tzinfo=UTC)) == []
    assert set(both_as_of(db, data, meta.family, datetime(2026, 2, 1, tzinfo=UTC))) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}


def test_a_second_resource_is_a_second_partition_not_a_replacement(data: Path) -> None:
    """Detects family-wide newest-capture selection (the one resource of a family displacing
    a later re-capture of another): two captures of the same resource serve the newer one only."""
    meta = CAPTURES["g_rr"]
    first = capture(data, "g_rr")
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    later = capture(data, "g_rr", written=datetime(2026, 10, 8, 18, 0, tzinfo=UTC))
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r2")
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert first != later
    assert set(both_as_of(db, data, meta.family, None)) == {later}


# --------------------------------------------------------------------------- #
# The workbook: two reference sheets, one held
# --------------------------------------------------------------------------- #


def test_the_workbook_fixture_is_the_real_body() -> None:
    """Detects a fixture edited since it was copied: the byte copy re-hashes to the sidecar
    ``body_sha256`` recorded in PROVENANCE.md."""
    raw = (FIXTURES / "tresp_lists.xlsx").read_bytes()
    assert len(raw) == 41878
    assert hashlib.sha256(raw).hexdigest() == WORKBOOK_SHA256
    provenance = (FIXTURES / "PROVENANCE.md").read_text(encoding="utf-8")
    assert WORKBOOK_SHA256 in provenance
    assert WORKBOOK_RESOURCE in provenance


def test_the_workbook_children_read_with_their_recipes(data: Path) -> None:
    """Detects a recipe off by a row or a column and a child routed to the wrong family: the
    Building Blocks sheet yields its 35 rows and the GSP sheet its 231, each family sees only
    its own sheet (the held old sheet is never read), keys are unique, the seven blank
    ``Technology Detail`` cells are null, the vendor text (including a non-breaking space) is
    kept, and every row carries its sheet as ``child_id``."""
    capture_id = capture_workbook(data)
    get_transformer(SOURCE, BB_FAMILY, data).run(DAY, run_id="r")
    get_transformer(SOURCE, GSP_FAMILY, data).run(DAY, run_id="r")
    for key in (BB_FAMILY, GSP_FAMILY):
        completion = read_completion(data, "tresp_demand_pathways_files", capture_id)
        assert completion is None or completion["outcome"] == "populated"
        completion = read_completion(data, key, capture_id)
        assert completion is not None and completion["rows_excluded"] == 0, key

    bb = _silver(data, BB_FAMILY)
    assert bb.height == 35
    assert set(bb["child_id"].to_list()) == {"tRESP Building Blocks"}
    assert bb["fes_bb_id_number"].n_unique() == 35
    assert bb["technology_detail"].null_count() == 7
    assert bb.filter(pl.col("technology_detail").is_null())["fes_bb_id_number"].to_list() == [
        "Gen_BB005",
        "Gen_BB006",
        "Gen_BB007",
        "Gen_BB008",
        "Gen_BB009",
        "Gen_BB011",
        "Gen_BB019",
    ]
    assert set(bb["demand_generation_storage"].to_list()) == {"Demand", "Generation", "Storage"}
    assert set(bb["units"].to_list()) == {
        "Number",
        "Surface Area",
        "Annual Demand (GWh)",
        "Capacity Installed (MW)",
    }
    assert "Pure Electric\xa0(Vans, Cars & Motorbikes)" in bb["technology_detail"].to_list()
    assert bb.filter(pl.col("fes_bb_id_number") == "Gen_BB023")["technology"].to_list() == [
        "Hydrogen"
    ]
    assert bb.select("resource_id").unique().to_series().to_list() == [WORKBOOK_RESOURCE]

    gsp = _silver(data, GSP_FAMILY)
    assert gsp.height == 231
    assert set(gsp["child_id"].to_list()) == {"tRESP GSP Areas with Names"}
    assert gsp["tresp_gsp_area"].n_unique() == 231
    assert gsp["tresp_gsp_area_name"].null_count() == 0
    assert gsp.filter(pl.col("tresp_gsp_area") == "_D@WYLF_1")["tresp_gsp_area_name"].to_list() == [
        "tRESP Wylfa SPEN"
    ]


def test_gsp_area_ids_of_both_gsp_csvs_resolve_in_the_workbook(data: Path) -> None:
    """Detects an identifier normalised on one side of the join (FACTS C11: both GSP CSVs
    carry exactly the workbook's 231 codes): every GSP area id in the demand and generation
    fixtures is a workbook ``tresp_gsp_area`` key, byte for byte."""
    capture_workbook(data)
    get_transformer(SOURCE, GSP_FAMILY, data).run(DAY, run_id="r")
    known = set(_silver(data, GSP_FAMILY)["tresp_gsp_area"].to_list())
    for alias in ("d_gsp", "g_gsp"):
        ids = {r["tRESP_GSP_area"] for r in rows(alias)}
        assert len(ids) >= 2
        assert ids <= known, (alias, ids - known)


def test_the_sheet_recipes_are_tight_to_the_tables() -> None:
    """Detects a recipe that silently loses or invents rows (ADR-037 P-6): the real sheets read
    with exactly their recipe; one row short fails growth rule (f) (the next row is populated);
    one row long fails the empty-data-row rule (d) (a ``last_row`` past the table)."""
    raw = (FIXTURES / "tresp_lists.xlsx").read_bytes()
    for sheet, columns, last, child in (
        ("tRESP Building Blocks", "A:E", 36, 35),
        ("tRESP GSP Areas with Names", "A:B", 232, 231),
    ):
        exact = read_sheet(
            open_container(raw, "w"),
            sheet,
            XlsxSpec(header_row=1, columns=columns, last_row=last),
            sheet,
        )
        assert exact.frame.height == child
        with pytest.raises(XlsxBlockError, match=r"\(f\)"):
            read_sheet(
                open_container(raw, "w"),
                sheet,
                XlsxSpec(header_row=1, columns=columns, last_row=last - 1),
                sheet,
            )
        with pytest.raises(XlsxBlockError, match=r"\(d\)"):
            read_sheet(
                open_container(raw, "w"),
                sheet,
                XlsxSpec(header_row=1, columns=columns, last_row=last + 1),
                sheet,
            )


# --------------------------------------------------------------------------- #
# Generated artefacts
# --------------------------------------------------------------------------- #


def test_the_skeleton_pages_render_the_new_records() -> None:
    """Detects a record the docs generator cannot render (the ordinary projection_year / value
    columns, the held demand question, the sibling-fed reference families)."""
    registry = load_registry()
    for slug, name in (
        ("tresp-demand-pathways", "tRESP Demand Pathways"),
        ("tresp-generation-pathways", "tRESP Generation Pathways"),
    ):
        snapshot = {
            "name": slug,
            "title": name,
            "organization": {"title": "NESO"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        }
        page = skeleton.render_package(registry, snapshot, None)
        assert "`projection_year`" in page
        assert "`value`" in page
    demand = skeleton.render_package(
        registry,
        {
            "name": "tresp-demand-pathways",
            "title": "t",
            "organization": {"title": "NESO"},
            "license_title": "x",
            "extras": [],
        },
        None,
    )
    assert BB_FAMILY in demand and GSP_FAMILY in demand
