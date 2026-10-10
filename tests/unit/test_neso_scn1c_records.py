"""The FES building block frozen records (v0.22-K-SCN-1c).

Three families of the ``future-energy-scenario-fes-building-block-data`` package get record
version 1. Every test writes recorded fixture captures into a short data root and runs the
transformer the **real package registry** generates, so a record that does not fit its vendor body
fails here, not at activation. On master none of the three families has a record, so
``get_transformer`` raises for each of them.

Fixtures (``tests/fixtures/neso_data_portal/scn1c/``, provenance in ``PROVENANCE.md``): each file
is a cut of the 2026-10-08 swept bronze (a scratch script, not committed): a handful of whole
vendor rows per resource, chosen for the oddities named in the tests. ``git`` normalises a committed
CSV fixture's line endings, so :func:`body` rebuilds the bronze originals' CRLF convention; a BOM
stays where the original had one.

Record decisions under test (K-SCN-1 FACTS SCN-1c, RULINGS 608):

- ``fes_building_blocks_main``: six header epochs, wide -> ``unpivot`` (ADR-042). The index columns
  are kept as captured; ``FES Scenario`` (2020-2023) and ``FES Pathway`` (2024-2025) are two
  separate nullable silver columns, never merged. The vendor's ``Baseline (2019)`` /
  ``Baseline (2020)`` columns unpivot to projection years 2019 / 2020 (vendor dictionaries call them
  the baseline values for that year). The value is a nullable float64: a blank projection cell stays
  a null-valued long row, a zero is a value. Held (E-SEM): the period a year label denotes and the
  row grain the 2020-2022 rows omit. The 2020, 2021 and 2022 bodies repeat the key and fail the
  duplicate guard (``DuplicateEntityKeyError``); each capture is an ADR-040 adjudicated gap.
- ``fes_building_blocks_block_definitions``: five layouts over six editions, reference, eligible.
  The 2022 body's two unnamed columns (positions 8 and 10) are kept as ``unnamed_8`` /
  ``unnamed_10``; ``Units`` is the vendor descriptor verbatim.
- ``fes_building_blocks_block_licence_area``: one 2024 resource, reference, eligible; ``N/A`` stays
  text.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "scn1c"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
PACKAGE = "future-energy-scenario-fes-building-block-data"
PACKAGE_ID = "30df2649-99cf-4f84-9128-6c58fc1ea72a"

MAIN = "fes_building_blocks_main"
DEFS = "fes_building_blocks_block_definitions"
LICENCE = "fes_building_blocks_block_licence_area"
FAMILIES = (MAIN, DEFS, LICENCE)
HELD = {MAIN}
HELD_QUESTION = (
    "TODO: for each edition, what period does a building-block year label (and the 2019/2020 "
    "baseline) denote — calendar year, financial year or winter? And what omitted row-grain "
    "dimension makes the 2020–2022 rows unique (repeated building block × GSP × DNO "
    "× unit rows with different values)? NESO's dictionaries state neither."
)
BRONZE_DIR = "bronze/neso_data_portal/fes_building_blocks_main/2026/10/08/"
COLLISION_CAPTURES = {
    "bff7061d-fbd3-4d8a-a95b-876affc2033d": BRONZE_DIR
    + "raw_20261008T105318Z_bff7061d-fbd3-4d8a-a95b-876affc2033d_4902afc9.csv",
    "5f93098e-1d52-44bf-a375-d3edfb89f8a5": BRONZE_DIR
    + "raw_20261008T105321Z_5f93098e-1d52-44bf-a375-d3edfb89f8a5_847ddfb0.csv",
    "36fd3aa9-6e42-418f-b1bb-a31bbfcf2008": BRONZE_DIR
    + "raw_20261008T105324Z_36fd3aa9-6e42-418f-b1bb-a31bbfcf2008_e7b4ed31.csv",
}


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


MAIN_RESOURCES: dict[int, tuple[str, str, str, str, str]] = {
    2020: (
        "bff7061d-fbd3-4d8a-a95b-876affc2033d",
        "FES 2020 Building Blocks - Version 1.3",
        "fes2020_building_blocks.csv",
        "2020-09-07T13:38:22.491166",
        "2026-10-08T10:53:18.158336+00:00",
    ),
    2021: (
        "5f93098e-1d52-44bf-a375-d3edfb89f8a5",
        "FES 2021 Building Blocks - Version 8.0",
        "fes-2021-building-blocks-version-008.csv",
        "2022-02-17T10:22:57.199905",
        "2026-10-08T10:53:21.060308+00:00",
    ),
    2022: (
        "36fd3aa9-6e42-418f-b1bb-a31bbfcf2008",
        "FES 2022 Building Blocks - Version 4.0",
        "fes-2022-building-blocks-version-4.0.csv",
        "2022-09-23T16:06:07.625574",
        "2026-10-08T10:53:24.135967+00:00",
    ),
    2023: (
        "8d57568c-2534-4682-ab1d-72fcf2d14998",
        "FES 2023 Building Blocks - Version 1.1",
        "fes-2023-building-blocks-version-1.1.csv",
        "2023-07-21T15:52:22.011143",
        "2026-10-08T10:53:27.019988+00:00",
    ),
    2024: (
        "be1f002b-bd8b-4f8d-9e1d-72d680336e26",
        "FES 2024 Building Blocks - Version 1.1",
        "fes-2024-building-blocks-version-1.1.csv",
        "2024-08-02T10:37:06.008284",
        "2026-10-08T10:53:30.058267+00:00",
    ),
    2025: (
        "73f69d8f-e9cb-4a2d-8538-baf03b5eadef",
        "FES 2025 Building Blocks",
        "fes2025_bb1_v006.csv",
        "2025-12-10T16:53:30.336039",
        "2026-10-08T10:53:34.208849+00:00",
    ),
}
CAPTURES: dict[str, Capture] = {}
for _edition, (_rid, _name, _filename, _modified, _written) in MAIN_RESOURCES.items():
    CAPTURES[f"m{_edition % 100}"] = Capture(
        MAIN, _rid, _name, _filename, _edition, _modified, _written
    )
    if _edition <= 2022:
        CAPTURES[f"m{_edition % 100}c"] = CAPTURES[f"m{_edition % 100}"]
CAPTURES |= {
    "d20": Capture(
        DEFS,
        "9fb5211f-9689-4d6f-a73e-cf28f03c5885",
        "Building Block Definitions 2020",
        "building-block-definitions.csv",
        2020,
        "2020-08-13T12:40:08.469499",
        "2026-10-08T10:52:56.827482+00:00",
    ),
    "d22": Capture(
        DEFS,
        "e5ab7ecb-0ab1-4fe7-833c-1fe905b086f8",
        "Building Block Definitions 2022",
        "building-block-definitions-2022.csv",
        2022,
        "2026-09-03T13:45:29.162340",
        "2026-10-08T10:53:02.747471+00:00",
    ),
    "d24": Capture(
        DEFS,
        "778a505c-1972-463b-b4bf-53e33c4d3470",
        "Building Block Definitions 2024",
        "building-block-definitions-2024.csv",
        2024,
        "2024-07-15T06:35:12.667550",
        "2026-10-08T10:53:07.837772+00:00",
    ),
    "l24": Capture(
        LICENCE,
        "0ec29244-277e-41d1-bca7-17f12b71eb60",
        "Building Block Licence Area Name Mapping 2024",
        "building-block-licence-area-name-mapping-2024.csv",
        2024,
        "2024-07-15T06:35:22.821993",
        "2026-10-08T10:53:14.274565+00:00",
    ),
}
COLLISIONS = ("m20c", "m21c", "m22c")
MAIN_TYPED = ("m20", "m21", "m22", "m23", "m24", "m25")
RUNNABLE = (*MAIN_TYPED, "d20", "d22", "d24", "l24")
SCENARIO_COLUMN = {2020: "FES Scenario", 2021: "FES Scenario", 2022: "FES Scenario"}
INDEX = ("Building Block ID Number", "Unit", "DNO License Area", "GSP")

MAIN_EDITIONS = (
    ("fes2020_building_blocks.csv", 2020),
    ("fes-2021-building-blocks-version-008.csv", 2021),
    ("fes-2022-building-blocks-version-4.0.csv", 2022),
    ("fes-2023-building-blocks-version-1.1.csv", 2023),
    ("fes-2024-building-blocks-version-1.1.csv", 2024),
    ("fes2025_bb1_v006.csv", 2025),
)
DEFS_EDITIONS = (
    ("building-block-definitions.csv", 2020),
    ("building-block-definitions-2021.csv", 2021),
    ("building-block-definitions-2022.csv", 2022),
    ("building-block-definitions-2023.csv", 2023),
    ("building-block-definitions-2024.csv", 2024),
    ("fes2025_bb2_v001.csv", 2025),
)
LICENCE_EDITIONS = (("building-block-licence-area-name-mapping-2024.csv", 2024),)

DEF_COMMON = (
    "Template",
    "Technology",
    "Building Block ID Number",
    "Technology Detail",
    "Units",
    "Detail",
    "Comments",
)
ALIGN = "Alignment with Ofgem Core Scenario Key Drivers"
DEF_LAYOUTS = (
    (*DEF_COMMON, "ESO Additional Notes"),
    (*DEF_COMMON, ALIGN, "ESO Comments"),
    (*DEF_COMMON, "", ALIGN, "_duplicated_0", "Included in ESO Data", "ESO Comments"),
    (*DEF_COMMON, ALIGN, "Included in ESO Data", "ESO Comments"),
    (*DEF_COMMON, ALIGN),
)
LICENCE_HEADER = (
    "Area Type",
    "FES23 BB Area Name",
    "Full Licence Name",
    "Elexon GSP Group",
    "Elexon GSP Group Name",
    "FES2024 Area Name",
    "Comments",
)
MAIN_KEY = (
    "resource_id",
    "edition",
    "fes_scenario",
    "fes_pathway",
    "building_block_id_number",
    "dno_license_area",
    "gsp",
    "unit",
    "projection_year",
)
YEARS_PER_EPOCH = {
    # edition -> (first header year label, years in the epoch incl. the baseline column)
    2020: 1 + 31,
    2021: 1 + 30,
    2022: 30,
    2023: 29,
    2024: 28,
    2025: 27,
}


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
    raw = (FIXTURES / f"{alias}.csv").read_bytes()
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    text = raw[len(bom) :].replace(b"\r\n", b"\n")
    return bom + text.replace(b"\n", b"\r\n")


def table(alias: str, raw: bytes | None = None) -> tuple[list[str], list[list[str]]]:
    """The fixture's header and populated records as text (all-blank records are dropped)."""
    data_bytes = raw if raw is not None else body(alias)
    lines = list(csv.reader(io.StringIO(data_bytes.decode("utf-8-sig"), newline="")))
    return lines[0], [line for line in lines[1:] if any(line)]


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's populated records keyed by position-qualified header (blank names unique)."""
    header, records = table(alias, raw)
    names = [name or f"_blank{i}" for i, name in enumerate(header)]
    return [dict(zip(names, record, strict=True)) for record in records]


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
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=written or datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=PACKAGE,
        package_id=PACKAGE_ID,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=filename or meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
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
    meta = CAPTURES[alias]
    capture_id = capture(data, alias, **kwargs)
    written = get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    return capture_id, written


def _replace_cell(raw: bytes, row: int, column: int, value: str) -> bytes:
    """``raw`` with table record ``row`` (0 = the header) cell ``column`` replaced.

    Only for bodies whose records never span a physical line."""
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    lines = raw[len(bom) :].split(b"\r\n")
    fields = next(csv.reader([lines[row].decode("utf-8")]))
    fields[column] = value
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    lines[row] = out.getvalue().encode("utf-8")
    return bom + b"\r\n".join(lines)


def parsed_header(header: list[str]) -> list[str]:
    """The header as the reader names it: blank names become ``""``, ``_duplicated_0``, ..."""
    out: list[str] = []
    blanks = 0
    for name in header:
        if name:
            out.append(name)
        else:
            out.append("" if blanks == 0 else f"_duplicated_{blanks - 1}")
            blanks += 1
    return out


def year_of(label: str) -> int | None:
    """The projection year a header label unpivots to, or ``None`` for an index column."""
    if label.isdigit():
        return int(label)
    baseline = re.fullmatch(r"Baseline \((\d{4})\)", label)
    return int(baseline.group(1)) if baseline else None


def label_columns(header: list[str]) -> list[str]:
    """The header's year / baseline labels, in header order."""
    return [h for h in header if year_of(h) is not None]


def scenario_column(header: list[str]) -> str:
    """The header's ``FES Scenario`` / ``FES Pathway`` column."""
    return "FES Scenario" if "FES Scenario" in header else "FES Pathway"


def expected_long(alias: str) -> list[tuple[Any, ...]]:
    """The long rows the vendor body must unpivot to, sorted: the scenario / pathway cell, the
    index cells, comment, share, projection year and value (blank = ``None``)."""
    header, records = table(alias)
    labels = label_columns(header)
    pos = {name: header.index(name) for name in header}
    scenario = scenario_column(header)
    out: list[tuple[Any, ...]] = []
    for record in records:
        share = record[pos["Share of GSP"]] if "Share of GSP" in pos else ""
        comment = record[pos["Comment"]]
        for label in labels:
            cell = record[pos[label]]
            out.append(
                (
                    scenario,
                    record[pos[scenario]],
                    *(record[pos[c]] for c in INDEX),
                    comment or None,
                    float(share) if share != "" else None,
                    year_of(label),
                    float(cell) if cell != "" else None,
                )
            )
    return sorted(out, key=repr)


def actual_long(frame: pl.DataFrame, scenario: str) -> list[tuple[Any, ...]]:
    """The silver rows in :func:`expected_long`'s shape."""
    column = {"FES Scenario": "fes_scenario", "FES Pathway": "fes_pathway"}[scenario]
    return sorted(
        (
            (
                scenario,
                row[column],
                row["building_block_id_number"],
                row["unit"],
                row["dno_license_area"],
                row["gsp"],
                row["comment"],
                row["share_of_gsp"],
                row["projection_year"],
                row["value"],
            )
            for row in frame.to_dicts()
        ),
        key=repr,
    )


# --------------------------------------------------------------------------- #
# Fixtures and record shapes
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the exact vendor headers (six
    main epochs, the 2022 definitions' two unnamed columns, the 2024 definitions layout, the
    licence-area header), CRLF line endings and each original's BOM, the 2020 blank baseline and
    blank projection cells, a ``0`` value, the blank rows of the 2020 definitions and of the 2023
    main body, the named collision rows (Camblesforth, Ratcliffe, the Direct(NGET) triple), a
    ``Share of GSP`` value in 2024 and 2025, a non-blank ``Comment``, the ``" MW"`` spelling and a
    blank ``Units`` and the ``N/A`` licence-area cells."""
    for alias in CAPTURES:
        assert body(alias).count(b"\r\n") >= body(alias).count(b"\n") - 4, alias
    bom = b"\xef\xbb\xbf"
    for with_bom in ("m20", "m21", "m22", "m23", "d20", "d22", "d24", "l24"):
        assert body(with_bom).startswith(bom), with_bom
    for without in ("m24", "m25"):
        assert not body(without).startswith(bom), without
    for alias, edition in (("m20", 2020), ("m21", 2021), ("m22", 2022), ("m23", 2023)):
        assert scenario_column(table(alias)[0]) == "FES Scenario", alias
        assert table(alias)[0][0] in ("Building Block ID Number", "FES Scenario"), alias
        assert CAPTURES[alias].edition == edition
    assert scenario_column(table("m24")[0]) == scenario_column(table("m25")[0]) == "FES Pathway"
    assert "Baseline (2019)" in table("m20")[0] and "Share of GSP" not in table("m20")[0]
    assert "Baseline (2020)" in table("m21")[0] and "Share of GSP" in table("m21")[0]
    for alias in ("m22", "m23", "m24", "m25"):
        assert not [h for h in table(alias)[0] if h.startswith("Baseline")], alias
    assert any(r["Baseline (2019)"] == "" for r in rows("m20"))
    assert any(v == "" for r in rows("m20") for k, v in r.items() if k.isdigit())
    assert any(v == "" for r in rows("m22") for k, v in r.items() if k.isdigit())
    for alias in MAIN_TYPED:
        cells = [v for r in rows(alias) for k, v in r.items() if year_of(k) is not None]
        assert "0" in cells or "0.0" in cells, alias
        assert "" in cells or alias in ("m21", "m23", "m24", "m25"), alias
    assert any(r["Share of GSP"] != "" for r in rows("m24"))
    assert any(r["Share of GSP"] != "" for r in rows("m25"))
    assert all(r["Share of GSP"] == "" for r in rows("m22"))
    assert any(r["Comment"] != "" for r in rows("m21"))
    raw23 = body("m23").decode("utf-8-sig").split("\r\n")
    assert sum(1 for line in raw23[1:] if line and set(line) == {","}) == 3
    assert body("d20").decode("utf-8-sig").count(",,,,,,,\r\n") >= 3
    assert any(r["Units"] == " MW" for r in rows("d20"))
    assert tuple(parsed_header(table("d22")[0])) == DEF_LAYOUTS[2]
    assert {r["_blank7"] for r in rows("d22")} == {""} == {r["_blank9"] for r in rows("d22")}
    assert tuple(parsed_header(table("d24")[0])) == DEF_LAYOUTS[4]
    assert any(r["Units"] == "" for r in rows("d24"))
    assert {r["Units"] for r in rows("d22")} >= {" Metres squared ", "% customers ", "Number of "}
    assert tuple(table("l24")[0]) == LICENCE_HEADER
    assert sum(r["Elexon GSP Group"] == "N/A" for r in rows("l24")) == 5
    camblesforth = [
        r
        for r in rows("m20c")
        if (r["FES Scenario"], r["GSP"], r["DNO License Area"])
        == ("CT", "Camblesforth", "NPg Yorkshire")
        and r["Building Block ID Number"] == "Gen_BB015"
    ]
    ratcliffe = [
        r
        for r in rows("m21c")
        if (r["FES Scenario"], r["GSP"], r["Building Block ID Number"])
        == ("Central Forecast", "Ratcliffe", "Gen_BB001")
    ]
    triple = [
        r
        for r in rows("m22c")
        if (r["FES Scenario"], r["GSP"], r["Building Block ID Number"], r["Unit"])
        == ("Leading the Way", "Direct(NGET)", "Gen_BB001", "MW")
    ]
    assert (len(camblesforth), len(ratcliffe), len(triple)) == (2, 2, 3)
    assert camblesforth[0] != camblesforth[1] and ratcliffe[0] != ratcliffe[1]


@pytest.mark.parametrize("family", FAMILIES)
def test_record_shape_matches_the_unit_spec(family: str) -> None:
    """Detects a record that drifts from the spec: one csv/utf-8 record, no issue time, temporal
    ``none``, whole-capture selection per ``resource_id``, ``ckan_last_modified`` vintage, the
    exact ``edition_by_filename`` pairs and a key of ``resource_id`` + ``edition`` + the
    spec's identifiers (and ``projection_year`` for the unpivoted main family)."""
    record = _record(family)
    assert (record.version, record.reader, record.encoding) == ("1", "csv", "utf-8")
    assert record.temporal.kind == "none"
    assert record.latest == "whole_capture"
    assert record.latest_partition == "resource_id"
    assert record.vintage == "ckan_last_modified"
    assert record.xlsx is None and record.siblings == ()
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    expected = {
        MAIN: (MAIN_EDITIONS, MAIN_KEY),
        DEFS: (DEFS_EDITIONS, ("resource_id", "edition", "building_block_id_number")),
        LICENCE: (LICENCE_EDITIONS, ("resource_id", "edition", "fes23_bb_area_name")),
    }[family]
    assert record.edition_by_filename == expected[0]
    assert record.entity_key == expected[1]


def test_main_has_six_unpivot_epochs_with_the_baselines_mapped() -> None:
    """Detects a baseline column left as an index column, mapped to its projection label or to the
    wrong year, a dropped or invented year, or an epoch merged away: six epochs whose year labels
    are exactly their header's year / baseline columns in header order, every label mapped to
    its integer (``Baseline (2019)`` -> 2019 in the 2020 edition, ``Baseline (2020)`` -> 2020 in
    the 2021 edition, no baseline later), the value a nullable float64 with no token or bound, and
    no label repeating a year."""
    record = _record(MAIN)
    assert len(record.epochs) == 6
    first_years = []
    for epoch, edition in zip(record.epochs, range(2020, 2026), strict=True):
        assert epoch.unpivot is not None
        labels = [label for label, _ in epoch.unpivot.years]
        assert labels == label_columns(list(epoch.header))
        assert [year for _, year in epoch.unpivot.years] == [year_of(label) for label in labels]
        assert len(labels) == YEARS_PER_EPOCH[edition]
        years = [year for _, year in epoch.unpivot.years]
        assert years == sorted(years) and len(set(years)) == len(years)
        first_years.append(years[0])
        assert epoch.unpivot.value.dtype == "float64" and epoch.unpivot.value.nullable is True
        assert epoch.unpivot.value.null_tokens == ()
        assert (epoch.unpivot.value.min, epoch.unpivot.value.max) == (None, None)
        assert years[-1] == 2050
        assert "Baseline (2019)" in labels if edition == 2020 else True
        assert ("Baseline (2020)" in labels) == (edition == 2021)
    assert first_years == [2019, 2020, 2021, 2022, 2023, 2024]
    baselines = {
        label: year
        for epoch in record.epochs
        if epoch.unpivot
        for label, year in epoch.unpivot.years
        if label.startswith("Baseline")
    }
    assert baselines == {"Baseline (2019)": 2019, "Baseline (2020)": 2020}


def test_scenario_and_pathway_are_two_nullable_columns_never_merged() -> None:
    """Detects ``FES Scenario`` and ``FES Pathway`` renamed into one label (the vendor does not
    equate them): editions 2020-2023 declare only ``fes_scenario``, 2024-2025 only
    ``fes_pathway``, both are nullable strings (null in the other editions' rows), and both
    are in the entity key."""
    record = _record(MAIN)
    for epoch, edition in zip(record.epochs, range(2020, 2026), strict=True):
        by_source = {c.source: c for c in epoch.columns}
        if edition <= 2023:
            assert "FES Pathway" not in by_source
            assert (
                by_source["FES Scenario"].name,
                by_source["FES Scenario"].dtype,
                by_source["FES Scenario"].nullable,
            ) == ("fes_scenario", "string", True)
        else:
            assert "FES Scenario" not in by_source
            assert (
                by_source["FES Pathway"].name,
                by_source["FES Pathway"].dtype,
                by_source["FES Pathway"].nullable,
            ) == ("fes_pathway", "string", True)
    assert {"fes_scenario", "fes_pathway"} <= set(record.entity_key)


def test_main_index_columns_are_strings_and_share_of_gsp_is_a_float() -> None:
    """Detects an identifier cast to a number or allowed to be null, a ``Share of GSP`` kept as
    text, an invented null token, or an index column lost: every index column is a string, the
    block id / unit / DNO / GSP / scenario non-nullable, ``Comment`` nullable, ``Share of GSP`` a
    nullable float64 present from the 2021 edition and absent from 2020, no token anywhere."""
    for epoch in _record(MAIN).epochs:
        names = {c.name: c for c in epoch.columns}
        for name in ("building_block_id_number", "unit", "dno_license_area", "gsp"):
            assert (names[name].dtype, names[name].nullable) == ("string", False), name
        assert (names["comment"].dtype, names["comment"].nullable) == ("string", True)
        for column in epoch.columns:
            assert column.null_tokens == (), column.name
            assert column.dtype in ("string", "float64")
            if column.dtype == "float64":
                assert column.name == "share_of_gsp" and column.nullable is True
    shares = [
        any(c.name == "share_of_gsp" for c in epoch.columns) for epoch in _record(MAIN).epochs
    ]
    assert shares == [False, True, True, True, True, True]


def test_definitions_have_five_layouts_and_name_the_unnamed_columns() -> None:
    """Detects a layout collapsed or lost, the 2022 unnamed columns dropped or mis-numbered
    (positions 8 and 10, parsed ``""`` and ``_duplicated_0``), a typed ``Units`` or an
    identifier made nullable: five epochs in the vendor's column order, every column a string,
    only the block id non-nullable, no token."""
    record = _record(DEFS)
    assert [tuple(e.header) for e in record.epochs] == list(DEF_LAYOUTS)
    for epoch in record.epochs:
        assert all(c.dtype == "string" for c in epoch.columns)
        assert all(c.null_tokens == () for c in epoch.columns)
        assert {c.name for c in epoch.columns if not c.nullable} == {"building_block_id_number"}
        assert epoch.unpivot is None
    unnamed = {
        c.source: c.name for c in record.epochs[2].columns if c.source in ("", "_duplicated_0")
    }
    assert unnamed == {"": "unnamed_8", "_duplicated_0": "unnamed_10"}
    assert [c.name for c in record.epochs[2].columns][7] == "unnamed_8"
    assert [c.name for c in record.epochs[2].columns][9] == "unnamed_10"


def test_licence_area_is_seven_strings_with_no_null_token() -> None:
    """Detects ``N/A`` registered as a null token, a typed column or a lost column: seven vendor
    columns in order, all strings, ``FES23 BB Area Name`` the only non-nullable one."""
    (epoch,) = _record(LICENCE).epochs
    assert tuple(epoch.header) == LICENCE_HEADER
    assert all(c.dtype == "string" and c.null_tokens == () for c in epoch.columns)
    assert {c.name for c in epoch.columns if not c.nullable} == {"fes23_bb_area_name"}
    assert [c.name for c in epoch.columns][1] == "fes23_bb_area_name"


def test_held_and_eligible_families_are_exactly_as_ruled() -> None:
    """Detects a hold lost or reworded, a held question that is not the spec's verbatim ``TODO:``,
    an eligible family held for another family's question, or a package-level hold swallowing the
    others: the main family alone is held E-SEM with the spec's question; the definitions and
    licence-area families publish."""
    registry = load_registry()
    for family in FAMILIES:
        package, entry = registry.families[family]
        effective = effective_eligibility(package, entry)
        if family in HELD:
            assert effective.status == "held", family
            assert effective.unit == "E-SEM"  # type: ignore[union-attr]
            assert effective.question == HELD_QUESTION  # type: ignore[union-attr]
        else:
            assert effective.status == "eligible", family
            assert _record(family).eligibility is None
    assert package_status() == "eligible"


def package_status() -> str:
    """The package-level eligibility (it must stay eligible so only the main family is held)."""
    return load_registry().families[MAIN][0].eligibility.status


def test_each_family_is_in_the_package_file_the_spec_names() -> None:
    """Detects a record put in another package file or a family left without one: all three
    records live in the building block package file."""
    document = json.loads((REGISTRY_DIR / f"{PACKAGE}.json").read_text(encoding="utf-8"))
    recorded = {f["key"] for f in document["families"] if f.get("record")}
    assert recorded == set(FAMILIES)


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", MAIN_TYPED)
def test_main_fixture_unpivots_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the BOM
    bodies, the epoch-specific column order), a cast the vendor body does not satisfy, a row
    excluded or lost, a blank projection cell dropped, and a stamp wrong: the capture completes
    with one long row per populated source row and header year label (blank cells stay
    null-valued rows), zero exclusions, ``edition`` from the filename, the resource id stamped,
    a unique key, and every cell byte-equal to the vendor's."""
    meta = CAPTURES[alias]
    capture_id, written = _run(data, alias)
    header, records = table(alias)
    per_row = len(label_columns(header))
    assert per_row == YEARS_PER_EPOCH[meta.edition]
    assert written == len(records) * per_row
    completion = read_completion(data, MAIN, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == written
    assert completion["rows_excluded"] == 0

    frame = _silver(data, MAIN)
    expected_columns = [name for name, _type in generic.output_columns(_record(MAIN))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected_columns if c not in ("year", "month")
    ]
    assert frame.schema["projection_year"] == pl.Int64
    assert frame.schema["value"] == pl.Float64
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame.select(list(MAIN_KEY)).is_duplicated().sum() == 0
    assert actual_long(frame, scenario_column(header)) == expected_long(alias)


def test_the_baseline_labels_land_as_2019_and_2020(data: Path) -> None:
    """Detects a baseline value filed under a projection year, dropped, or given the edition's own
    first year: the 2020 edition's ``Baseline (2019)`` cells are the projection-year-2019 rows and
    the 2021 edition's ``Baseline (2020)`` cells the projection-year-2020 rows, value for value;
    the 2020 edition also has its own 2020 rows and no 2021 edition row is year 2019."""
    _run(data, "m20")
    _run(data, "m21")
    frame = _silver(data, MAIN)
    first = frame.filter(pl.col("edition") == 2020)
    second = frame.filter(pl.col("edition") == 2021)
    assert first["projection_year"].min() == 2019 and second["projection_year"].min() == 2020
    assert second.filter(pl.col("projection_year") < 2020).height == 0
    baseline_2019 = sorted(
        (r["Building Block ID Number"], r["GSP"], r["FES Scenario"], r["Baseline (2019)"])
        for r in rows("m20")
    )
    got = sorted(
        (
            r["building_block_id_number"],
            r["gsp"],
            r["fes_scenario"],
            "" if r["value"] is None else r["value"],
        )
        for r in first.filter(pl.col("projection_year") == 2019).to_dicts()
    )
    assert [(a, b, c, "" if d == "" else float(d)) for a, b, c, d in baseline_2019] == got
    baseline_2020 = sorted(
        (r["Building Block ID Number"], r["GSP"], r["FES Scenario"], float(r["Baseline (2020)"]))
        for r in rows("m21")
    )
    got_2020 = sorted(
        (r["building_block_id_number"], r["gsp"], r["fes_scenario"], r["value"])
        for r in second.filter(pl.col("projection_year") == 2020).to_dicts()
    )
    assert baseline_2020 == got_2020
    own = first.filter(pl.col("projection_year") == 2020)
    assert own.height == len(rows("m20"))
    assert sorted(own["value"].to_list(), key=repr) == sorted(
        [float(r["2020"]) if r["2020"] != "" else None for r in rows("m20")], key=repr
    )


def test_a_blank_baseline_or_projection_cell_is_a_null_value_never_zero(data: Path) -> None:
    """Detects a blank cell read as zero or dropping its row (FACTS: 3316 blank 2019 baselines,
    thousands of blank projections, kept as null-valued long rows), and a genuine zero turned
    into null: the 2020 blank baseline rows stay with a null value, zeros stay ``0.0``."""
    _run(data, "m20")
    frame = _silver(data, MAIN)
    blank_baselines = sum(r["Baseline (2019)"] == "" for r in rows("m20"))
    assert blank_baselines > 0
    assert frame.filter(pl.col("projection_year") == 2019)["value"].null_count() == blank_baselines
    expected_nulls = sum(v == "" for r in rows("m20") for k, v in r.items() if year_of(k))
    assert frame["value"].null_count() == expected_nulls
    zeros = sum(v in ("0", "0.0") for r in rows("m20") for k, v in r.items() if year_of(k))
    assert zeros > 0 and (frame["value"] == 0.0).sum() == zeros


def test_main_blank_projection_cells_are_kept_as_null_valued_rows(data: Path) -> None:
    """Detects a blank projection cell excluding its long row: the 2022 fixture's blank cells
    (FACTS: 1745.. blank per year) are null values, the row count still source rows x year
    labels, and the exclusion count 0."""
    capture_id, written = _run(data, "m22")
    blanks = sum(v == "" for r in rows("m22") for k, v in r.items() if year_of(k))
    assert blanks > 0
    assert _silver(data, MAIN)["value"].null_count() == blanks
    assert written == len(rows("m22")) * 30
    completion = read_completion(data, MAIN, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0


def test_share_of_gsp_comment_and_units_survive_exactly(data: Path) -> None:
    """Detects a share or comment dropped, filled backwards or rewritten, or a row unit
    normalised: the 2024 and 2025 ``Share of GSP`` values are floats on their rows and null on the
    rest, ``Comment`` is the cell (blank = null), and ``Unit`` is the row's own spelling
    (``MW``, ``Number``, ``GWh``) on every long row."""
    _run(data, "m24")
    _run(data, "m25")
    frame = _silver(data, MAIN)
    for edition, alias in ((2024, "m24"), (2025, "m25")):
        part = frame.filter(pl.col("edition") == edition)
        shares = sum(r["Share of GSP"] != "" for r in rows(alias))
        years = YEARS_PER_EPOCH[edition]
        assert shares > 0
        assert part["share_of_gsp"].drop_nulls().len() == shares * years
        assert set(part["unit"].to_list()) == {r["Unit"] for r in rows(alias)}
        comments = sum(r["Comment"] != "" for r in rows(alias))
        assert part["comment"].drop_nulls().len() == comments * years
    assert {"MW", "Number", "GWh"} <= set(frame["unit"].to_list())


def test_scenario_and_pathway_land_in_separate_columns(data: Path) -> None:
    """Detects the two vendor labels merged in the silver: 2020-2023 rows have a ``fes_scenario``
    and a null ``fes_pathway``; 2024-2025 rows the reverse; the key keeps all four distinct
    capture families apart."""
    for alias in ("m22", "m23", "m24", "m25"):
        _run(data, alias)
    frame = _silver(data, MAIN)
    old = frame.filter(pl.col("edition") <= 2023)
    new = frame.filter(pl.col("edition") >= 2024)
    assert old["fes_scenario"].null_count() == 0 and old["fes_pathway"].null_count() == old.height
    assert new["fes_pathway"].null_count() == 0 and new["fes_scenario"].null_count() == new.height
    assert set(new["fes_pathway"].to_list()) == {r["FES Pathway"] for r in rows("m24")} | {
        r["FES Pathway"] for r in rows("m25")
    }
    assert frame.select(list(MAIN_KEY)).is_duplicated().sum() == 0


def test_the_2023_blank_tail_goes_through_the_logged_blank_row_path(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects the 2023 body's all-blank tail rows (4551 in the real body) excluded as failures,
    dropped silently or reaching the output: the reader's blank-row path drops them and logs
    one INFO record, ``rows_excluded`` stays 0, and the rows written equal the populated rows x
    29 year labels."""
    raw = body("m23")
    assert raw.count(b",,,,,,,,") >= 3
    with caplog.at_level(logging.INFO, logger="gridflow.silver.csv_bronze"):
        capture_id, written = _run(data, "m23")
    assert written == len(rows("m23")) * 29
    completion = read_completion(data, MAIN, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    messages = [r.getMessage() for r in caplog.records if "blank row" in r.getMessage()]
    assert len(messages) == 1, messages
    assert messages[0].startswith("dropped 3 blank row(s)")
    assert _silver(data, MAIN)["building_block_id_number"].null_count() == 0


@pytest.mark.parametrize(
    ("alias", "label", "value"),
    [
        ("m24", "2030", "n/a"),
        ("m24", "Share of GSP", "5%"),
        ("m21", "Baseline (2020)", "-"),
        ("m25", "2050", "1,5"),
    ],
)
def test_an_uncastable_cell_fails_the_capture(
    data: Path, alias: str, label: str, value: str
) -> None:
    """Detects a record that swallows a bad cell (a null-token list, a lenient cast, a decimal
    comma): a non-numeric projection, baseline or share cell fails the whole capture before any
    row is written and leaves no completion."""
    raw = _replace_cell(body(alias), 2, table(alias)[0].index(label), value)
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, MAIN, data).run(DAY, run_id="r")
    assert read_completion(data, MAIN, capture_id) is None
    assert _no_silver(data, MAIN)


def test_a_blank_identifier_is_excluded_and_counted(data: Path) -> None:
    """Detects an identifier made nullable (a blank GSP becoming a null-keyed row): the source
    row's every long row is excluded, counted on the transformer and in the completion, and no
    null reaches the output."""
    raw = _replace_cell(body("m24"), 2, table("m24")[0].index("GSP"), "")
    capture_id = capture(data, "m24", raw=raw)
    transformer = get_transformer(SOURCE, MAIN, data)
    written = transformer.run(DAY, run_id="r")
    assert written == (len(rows("m24")) - 1) * 28
    assert transformer.last_excluded_row_count == 28
    completion = read_completion(data, MAIN, capture_id)
    assert completion is not None and completion["rows_excluded"] == 28
    assert _silver(data, MAIN)["gsp"].null_count() == 0


# --------------------------------------------------------------------------- #
# The colliding 2020-2022 captures
# --------------------------------------------------------------------------- #


def _key_of(header: list[str], record: list[str]) -> tuple[str, ...]:
    pos = {name: header.index(name) for name in header}
    scenario = scenario_column(header)
    return (record[pos[scenario]], *(record[pos[c]] for c in INDEX))


def _first_of_each_key(alias: str) -> bytes:
    """The collision fixture with every later row of a repeated key removed."""
    raw = body(alias)
    header, _ = table(alias)
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    lines = raw[len(bom) :].split(b"\r\n")
    seen: set[tuple[str, ...]] = set()
    kept = [lines[0]]
    for line in lines[1:]:
        if not line:
            continue
        key = _key_of(header, next(csv.reader([line.decode("utf-8")])))
        if key in seen:
            continue
        seen.add(key)
        kept.append(line)
    return bom + b"\r\n".join(kept) + b"\r\n"


@pytest.mark.parametrize(
    ("alias", "shared"), [("m20c", 2 * 32), ("m21c", 2 * 31), ("m22c", 3 * 30)]
)
def test_a_colliding_capture_fails_with_duplicate_entity_key_error(
    data: Path, alias: str, shared: int
) -> None:
    """Detects a duplicate guard weakened or a value added to the key (the collisions are real
    vendor repeats, never deduplicated): the Camblesforth pair (2020), the Ratcliffe pair (2021)
    and the Direct(NGET) triple (2022) fail the whole capture with
    ``DuplicateEntityKeyError`` naming the shared rows x year labels, leave no completion and
    write nothing."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, MAIN, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert f"{shared} row(s) sharing an entity key" in info.value.failures[0][2]
    failure = read_failure(data, MAIN, capture_id)
    assert failure is not None and failure["error_class"] == "DuplicateEntityKeyError"
    assert read_completion(data, MAIN, capture_id) is None
    assert _no_silver(data, MAIN)
    assert meta.edition == int(alias[1:3]) + 2000


@pytest.mark.parametrize("alias", COLLISIONS)
def test_removing_the_repeated_rows_makes_the_capture_type(data: Path, alias: str) -> None:
    """Detects a collision caused by anything but the repeated rows (a key column that is
    wrong, a baseline label colliding with a projection year): the same fixture with only the
    first row of each key completes with zero exclusions, so the failure above is the vendor's
    repeated key and nothing else."""
    raw = _first_of_each_key(alias)
    assert len(rows(alias, raw)) < len(rows(alias))
    capture_id, written = _run(data, alias, raw=raw)
    completion = read_completion(data, MAIN, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    assert written == len(rows(alias, raw)) * YEARS_PER_EPOCH[CAPTURES[alias].edition]


def test_the_committed_ledger_entries_are_backed_by_the_registry() -> None:
    """Detects an adjudication the registry no longer backs (a renamed family, a capture under the
    wrong directory or outside the package, a ruling that is not a line number), one that names
    another failure class, or an entry covering more than its one capture: exactly three
    building block entries, each a ``failed`` ``DuplicateEntityKeyError`` of ruling 608 on one of
    the 2020 / 2021 / 2022 main captures."""
    entries = registry_module.load_reconcile_adjudications(None)
    mine = [e for e in entries if e.family == MAIN]
    assert len(mine) == 3
    assert [(e.category, e.cause, e.ruling) for e in mine] == [
        ("failed", "DuplicateEntityKeyError", "608")
    ] * 3
    assert [e.captures for e in mine] == [(c,) for c in COLLISION_CAPTURES.values()]
    for rid, capture_path in COLLISION_CAPTURES.items():
        assert rid in capture_path
        assert rid in {CAPTURES[a].resource_id for a in COLLISIONS}
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    held = [e for e in entries if e.family in (DEFS, LICENCE)]
    assert held == []


def _package_registry(monkeypatch: pytest.MonkeyPatch, data: Path) -> None:
    document = json.loads((REGISTRY_DIR / f"{PACKAGE}.json").read_text(encoding="utf-8"))
    install_generated(monkeypatch, data / "_registry", [document])


def test_the_three_failures_are_adjudicated_not_open_and_not_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a collision capture left as an open gap (reconcile red forever), an entry that no
    longer matches its failure record (stale), or an adjudication that alters data: without the
    ledger reconcile reports three ``failed`` gaps; with the committed entries (their capture
    ids swapped for the fixture captures') every one is adjudicated, none is open or stale,
    the ruling is named, and the silver and state bytes are equal before and after."""
    _package_registry(monkeypatch, data)
    captured = {CAPTURES[a].resource_id: capture(data, a) for a in COLLISIONS}
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, MAIN, data).run(DAY, run_id="r")
    assert sorted(cls for _c, cls, _m in info.value.failures) == ["DuplicateEntityKeyError"] * 3

    def snapshot() -> dict[str, bytes]:
        return {
            p.relative_to(data).as_posix(): p.read_bytes()
            for top in ("silver", "state")
            for p in sorted((data / top).rglob("*"))
            if p.is_file()
        }

    before = snapshot()
    code, lines = run_cli(MAIN, "--cutoff", DAY.isoformat())
    assert code == 1, lines
    gaps = [line for line in lines if line.startswith("GAP failed")]
    assert len(gaps) == 3 and all(any(c in g for c in captured.values()) for g in gaps)
    committed = json.loads(
        (REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8")
    )
    entries = [e for e in committed if e["family"] == MAIN]
    swapped = []
    for entry in entries:
        (path,) = entry["captures"]
        rid = next(r for r in captured if r in path)
        swapped.append({**entry, "captures": [captured[rid]]})
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json(swapped), encoding="utf-8"
    )
    code, lines = run_cli(MAIN, "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert "SUMMARY adjudicated 3" in lines
    assert "SUMMARY stale_adjudication 0" in lines
    assert [line for line in lines if line.startswith("GAP")] == []
    for cid in captured.values():
        adjudicated = [line for line in lines if cid in line]
        assert len(adjudicated) == 1
        assert "DuplicateEntityKeyError" in adjudicated[0] and "ruling 608" in adjudicated[0]
    assert snapshot() == before


# --------------------------------------------------------------------------- #
# Editions
# --------------------------------------------------------------------------- #


def test_editions_are_stamped_from_the_filename_and_both_are_served(data: Path) -> None:
    """Detects an edition inferred from the vintage or the year labels, or one edition
    displacing another: the 2024 and 2025 resources stamp 2024 and 2025 from their filenames
    (their labels start in 2023 and 2024), and ``_latest`` serves each."""
    first, _ = _run(data, "m24")
    second, _ = _run(data, "m25")
    frame = _silver(data, MAIN)
    by_capture = {
        cid: set(editions)
        for cid, editions in frame.group_by("bronze_capture_id")
        .agg(pl.col("edition").unique())
        .iter_rows()
    }
    assert by_capture == {first: {2024}, second: {2025}}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, MAIN, None)) == {first, second}


@pytest.mark.parametrize(
    ("alias", "wrong"),
    [
        ("m24", "fes-2026-building-blocks-version-1.0.csv"),
        ("m23", "fes-2022-building-blocks-version-4.0.csv.bak"),
        ("d22", "building-block-definitions-2026.csv"),
        ("l24", "building-block-licence-area-name-mapping-2025.csv"),
    ],
)
def test_an_unmapped_filename_fails_loud(data: Path, alias: str, wrong: str) -> None:
    """Detects a fallback edition: a filename the family's map does not list (a new year, a
    suffixed copy of a listed name) fails the capture, leaves no completion and writes nothing."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias, filename=wrong)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert read_completion(data, meta.family, capture_id) is None
    assert _no_silver(data, meta.family)


# --------------------------------------------------------------------------- #
# Block definitions
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ("d20", "d22", "d24"))
def test_definitions_type_with_every_cell_exact(data: Path, alias: str) -> None:
    """Detects a header matching no layout (the BOM, the 2022 unnamed columns), a stripped or
    folded descriptor, an excluded row, a blank turned into text and a stamp wrong: every
    populated row is written, ``Units`` and every other string equals the cell byte for byte
    (leading / trailing spaces kept, a blank is null), the edition and resource are stamped."""
    meta = CAPTURES[alias]
    capture_id, written = _run(data, alias)
    header, records = table(alias)
    assert written == len(records)
    completion = read_completion(data, DEFS, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, DEFS)
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert (
        frame.select(["resource_id", "edition", "building_block_id_number"]).is_duplicated().sum()
        == 0
    )
    record = _record(DEFS)
    parsed = parsed_header(header)
    epoch = next(e for e in record.epochs if list(e.header) == parsed)
    for column in epoch.columns:
        cells = [r[parsed.index(column.source)] for r in records]
        assert frame[column.name].to_list() == [c if c != "" else None for c in cells], column.name


def test_units_spellings_are_kept_as_the_vendor_descriptor(data: Path) -> None:
    """Detects ``Units`` normalised into a guessed unit (FACTS g4): the leading-space ``" MW"`` of
    2020, ``" Metres squared "``, ``"% customers "`` and ``"Number of "`` of 2022 survive
    exactly, and the 2024 blank units are null."""
    for alias in ("d20", "d22", "d24"):
        _run(data, alias)
    units = _silver(data, DEFS)["units"].to_list()
    for spelling in (" MW", " Metres squared ", "% customers ", "Number of ", "GWh", "MW"):
        assert spelling in units, spelling
    assert None in units
    blanks = sum(r["Units"] == "" for r in rows("d24"))
    assert blanks > 0
    assert _silver(data, DEFS).filter(pl.col("edition") == 2024)["units"].null_count() == blanks


def test_the_2022_unnamed_columns_are_kept_blank(data: Path) -> None:
    """Detects the two unnamed columns of the 2022 body dropped or mis-named (FACTS: all blank in
    every row): ``unnamed_8`` and ``unnamed_10`` exist and are null in every 2022 row, and the
    other layouts leave them null."""
    for alias in ("d20", "d22"):
        _run(data, alias)
    frame = _silver(data, DEFS)
    for name in ("unnamed_8", "unnamed_10"):
        assert frame[name].null_count() == frame.height, name
    assert frame.filter(pl.col("edition") == 2022).height == len(rows("d22"))
    assert "eso_additional_notes" in frame.columns and "included_in_eso_data" in frame.columns


def test_the_2020_definitions_blank_rows_go_through_the_logged_path(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects the 2020 body's four all-blank rows (49 retained of 53) reaching the output as
    null-keyed rows or being dropped without a record: the reader drops them with one INFO
    record and ``rows_excluded`` stays 0."""
    blank_rows = body("d20").decode("utf-8-sig").count(",,,,,,,\r\n")
    assert blank_rows >= 3
    with caplog.at_level(logging.INFO, logger="gridflow.silver.csv_bronze"):
        capture_id, written = _run(data, "d20")
    assert written == len(rows("d20"))
    completion = read_completion(data, DEFS, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    messages = [r.getMessage() for r in caplog.records if "blank row" in r.getMessage()]
    assert len(messages) == 1 and messages[0].startswith(f"dropped {blank_rows} blank row(s)")
    assert _silver(data, DEFS)["building_block_id_number"].null_count() == 0


def test_a_repeated_definition_id_fails_the_capture(data: Path) -> None:
    """Detects a block id key too coarse to guard the body: a repeated ``Building Block ID
    Number`` with another detail fails with ``DuplicateEntityKeyError`` and leaves no completion."""
    raw = body("d22")
    first = raw.decode("utf-8-sig").split("\r\n")[1]
    changed = first.replace("Installed capacity", "Another detail")
    capture_id = capture(data, "d22", raw=raw + changed.encode() + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, DEFS, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert read_completion(data, DEFS, capture_id) is None


# --------------------------------------------------------------------------- #
# Licence area
# --------------------------------------------------------------------------- #


def test_licence_area_keeps_na_as_text(data: Path) -> None:
    """Detects ``N/A`` turned into null (a null token) or a blank comment kept as text: the five
    ``N/A`` cells of each Elexon column stay ``"N/A"``, blank comments are null, all 19 rows
    are written and the area-name key is unique."""
    capture_id, written = _run(data, "l24")
    assert written == 19 == len(rows("l24"))
    completion = read_completion(data, LICENCE, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, LICENCE)
    for column in ("elexon_gsp_group", "elexon_gsp_group_name"):
        assert frame[column].to_list().count("N/A") == 5, column
        assert frame[column].null_count() == 0
    assert frame["comments"].null_count() == sum(r["Comments"] == "" for r in rows("l24"))
    assert frame["edition"].to_list() == [2024] * 19
    assert frame["fes23_bb_area_name"].to_list() == [r["FES23 BB Area Name"] for r in rows("l24")]
    assert frame.select(["resource_id", "edition", "fes23_bb_area_name"]).is_duplicated().sum() == 0


# --------------------------------------------------------------------------- #
# Vintage, the catalogue and generated artefacts
# --------------------------------------------------------------------------- #


def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(data: Path) -> None:
    """Detects an issue-time proxy or a projection-year clock (RULINGS 529/597), or a catalogue
    view that cannot carry the unpivot columns: ``available_at`` is the CKAN ``last_modified``
    (2024-08-02), ``timestamp_utc`` stays the capture time, an as-of read before the vintage serves
    nothing and one after it serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES["m24"]
    capture_id, _ = _run(data, "m24")
    frame = _silver(data, MAIN)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, MAIN, datetime(2024, 8, 1, tzinfo=UTC)) == []
    assert set(both_as_of(db, data, MAIN, datetime(2024, 8, 3, tzinfo=UTC))) == {capture_id}
    assert set(both_as_of(db, data, MAIN, None)) == {capture_id}


def test_the_skeleton_page_renders_the_new_records() -> None:
    """Detects a record the docs generator cannot render (the unpivot columns, the unnamed
    definitions columns, the held question)."""
    page = skeleton.render_package(
        load_registry(),
        {
            "name": PACKAGE,
            "title": "FES: Pathways to Net Zero Building Block Data",
            "organization": {"title": "FES: Pathways to Net Zero"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    for needle in ("`projection_year`", "`fes_scenario`", "`fes_pathway`", "`unnamed_8`"):
        assert needle in page, needle
    for family in FAMILIES:
        assert family in page
