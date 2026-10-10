"""The FES ES1 electricity supply frozen record (v0.22-K-SCN-1d).

The ``fes_es1_electricity_supply`` family of the
``future-energy-scenario-electricity-supply-data-table-es1`` package gets record version 1. Every
test writes recorded fixture captures into a short data root and runs the transformer the **real
package registry** generates, so a record that does not fit its vendor body fails here, not at
activation. On master the family has no record, so ``get_transformer`` raises for it.

Fixtures (``tests/fixtures/neso_data_portal/scn1d/``, provenance in ``PROVENANCE.md``): each file
is a cut of the 2026-10-08 swept bronze (a scratch script, not committed): whole vendor rows
chosen for the oddities named in the tests. ``git`` normalises a committed CSV fixture's line
endings, so :func:`body` rebuilds the bronze originals' CRLF convention; a BOM stays where the
original had one.

Record decisions under test (K-SCN-1 FACTS SCN-1d, RULINGS 603):

- Four SILVER editions (2023-2026), four header epochs (the engine matches the exact header, and
  each edition's year columns start in a different year), wide -> ``unpivot`` (ADR-042). The
  dimension columns are kept as captured; ``Scenario`` (2023, 2026) and ``Pathway`` (2024, 2025)
  are two separate nullable silver columns, never merged. The value is a nullable float64: a blank
  projection cell (incl. 2026's all-blank 2024 and 2037-2050 columns) stays a null-valued long
  row, a zero is a value. The unit lives in ``Variable`` and is kept verbatim.
- Held (E-SEM): the period a year label denotes, for every edition.
- The 2020, 2021 and 2022 resources are resource-level HOLD dispositions: 2020's cells fail the
  strict float64 cast (thousands separators, literal ``N/A``), 2021 and 2022 carry a preamble the
  reader cannot skip and whitespace-padded numbers.
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "scn1d"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
PACKAGE = "future-energy-scenario-electricity-supply-data-table-es1"
PACKAGE_ID = "549b0667-b533-4748-95bd-f6e13933a47d"
ES1 = "fes_es1_electricity_supply"
HELD_QUESTION = (
    "TODO: for each ES1 edition, what period does a year label denote (calendar, financial or "
    "winter year; start or end label)? What does N/A mean in the 2020 edition, and will NESO "
    "republish the 2020–2022 tables without preambles, padding and thousands separators?"
)
KEY = (
    "resource_id",
    "edition",
    "connection",
    "scenario",
    "pathway",
    "variable",
    "category",
    "type",
    "sub_type",
    "projection_year",
)
DIMENSIONS = ("Connection", "Variable", "Category", "Type", "SubType")


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture and its sidecar provenance."""

    resource_id: str
    name: str
    filename: str
    edition: int
    modified: str
    written: str


CAPTURES: dict[str, Capture] = {
    "e20": Capture(
        "40f40b39-5eba-4479-94b1-328ea9b8eefe",
        "Electricity Supply Data table (ES1) 2020",
        "fes_es1.csv",
        2020,
        "2020-12-03T11:58:42.191637",
        "2026-10-08T10:54:55.231873+00:00",
    ),
    "e21": Capture(
        "bca9679e-9860-4efd-9145-220d7dc4b912",
        "Electricity Supply Data table (ES1) 2021",
        "fes2021_es1.csv",
        2021,
        "2021-07-12T10:24:36.047429",
        "2026-10-08T10:54:57.796823+00:00",
    ),
    "e22": Capture(
        "90c4a0e8-22fd-4bda-b5bd-14a540893a98",
        "Electricity Supply Data table (ES1) 2022",
        "fes2022_es1_v001.csv",
        2022,
        "2022-07-17T23:22:23.498433",
        "2026-10-08T10:55:00.561891+00:00",
    ),
    "e23": Capture(
        "86812136-3f52-43e5-8f7c-7e4f6d5f95fc",
        "Electricity Supply Data table (ES1) 2023",
        "fes2023_es1_v002.csv",
        2023,
        "2023-08-24T10:35:22.143299",
        "2026-10-08T10:55:02.875326+00:00",
    ),
    "e24": Capture(
        "8c8a436d-408a-441b-8c7a-84249805772c",
        "Electricity Supply Data table (ES1) 2024",
        "fes2024_es1_v002.csv",
        2024,
        "2024-08-02T11:01:29.547730",
        "2026-10-08T10:55:05.578005+00:00",
    ),
    "e25": Capture(
        "6c78a777-b885-4bb6-bc35-8100f9e137a2",
        "Electricity Supply Data table (ES1) 2025",
        "fes2025_es1_v006.csv",
        2025,
        "2025-12-10T16:48:54.482473",
        "2026-10-08T10:55:08.139618+00:00",
    ),
    "e26": Capture(
        "b3bf8ac0-d27a-447c-975c-208ae92c0fa5",
        "Electricity Supply Data table (ES1) 2026",
        "10yo2026_es1_v001.csv",
        2026,
        "2026-09-16T14:27:45.949534",
        "2026-10-08T10:55:10.553294+00:00",
    ),
}
TYPED = ("e23", "e24", "e25", "e26")
HELD_ALIASES = ("e20", "e21", "e22")
EDITIONS = (
    ("fes2023_es1_v002.csv", 2023),
    ("fes2024_es1_v002.csv", 2024),
    ("fes2025_es1_v006.csv", 2025),
    ("10yo2026_es1_v001.csv", 2026),
)
LABEL_COLUMN = {2023: "Scenario", 2024: "Pathway", 2025: "Pathway", 2026: "Scenario"}
FIRST_YEAR = {2023: 2022, 2024: 2023, 2025: 2024, 2026: 2024}
LABELS_PER_ROW = {2023: 29, 2024: 28, 2025: 27, 2026: 27}
HOLD_REASONS = {
    "e20": ("2,645", "2,811", "N/A", "float64"),
    "e21": ("header on physical line 10", "preamble", "whitespace"),
    "e22": ("header on physical line 10", "preamble", "whitespace"),
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


def lines(alias: str, raw: bytes | None = None) -> list[list[str]]:
    """Every record of the fixture as text."""
    data_bytes = raw if raw is not None else body(alias)
    return list(csv.reader(io.StringIO(data_bytes.decode("utf-8-sig"), newline="")))


def header_index(alias: str) -> int:
    """The row index of the table header (after the preamble of the 2021 / 2022 bodies)."""
    return next(
        i
        for i, line in enumerate(lines(alias))
        if line[:1] == ["Connection"] and line[1:2] in (["Scenario"], ["Pathway"])
    )


def table(alias: str, raw: bytes | None = None) -> tuple[list[str], list[list[str]]]:
    """The fixture's table header and populated records as text."""
    records = lines(alias, raw)
    start = header_index(alias) if raw is None else 0
    return records[start], [line for line in records[start + 1 :] if any(line)]


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's populated records keyed by header (blank names made unique)."""
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
        ES1,
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


def _record() -> SchemaRecord:
    record = registry_module.load_registry().families[ES1][1].record
    assert record is not None
    return record


def _silver(data: Path) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / ES1).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _no_silver(data: Path) -> bool:
    return not list((data / "silver" / SOURCE / ES1).rglob("[!.]*.parquet"))


def _run(data: Path, alias: str, **kwargs: Any) -> tuple[str, int]:
    """Capture ``alias``, run the family's transformer; the capture id and rows written."""
    capture_id = capture(data, alias, **kwargs)
    written = get_transformer(SOURCE, ES1, data).run(DAY, run_id="r")
    return capture_id, written


def _replace_cell(raw: bytes, row: int, column: int, value: str) -> bytes:
    """``raw`` with table record ``row`` (0 = the header) cell ``column`` replaced.

    Only for bodies whose records never span a physical line and have no preamble."""
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    parts = raw[len(bom) :].split(b"\r\n")
    fields = next(csv.reader([parts[row].decode("utf-8")]))
    fields[column] = value
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    parts[row] = out.getvalue().encode("utf-8")
    return bom + b"\r\n".join(parts)


def label_columns(header: list[str]) -> list[str]:
    """The header's year labels, in header order."""
    return [h for h in header if h.isdigit()]


def expected_long(alias: str) -> list[tuple[Any, ...]]:
    """The long rows the vendor body must unpivot to, sorted: the scenario / pathway cell, the
    five dimension cells, projection year and value (blank = ``None``)."""
    header, records = table(alias)
    pos = {name: header.index(name) for name in header}
    label = "Scenario" if "Scenario" in header else "Pathway"
    out: list[tuple[Any, ...]] = []
    for record in records:
        for year in label_columns(header):
            cell = record[pos[year]]
            out.append(
                (
                    label,
                    record[pos[label]],
                    *(record[pos[c]] or None for c in DIMENSIONS),
                    int(year),
                    float(cell) if cell != "" else None,
                )
            )
    return sorted(out, key=repr)


def actual_long(frame: pl.DataFrame, label: str) -> list[tuple[Any, ...]]:
    """The silver rows in :func:`expected_long`'s shape."""
    column = label.lower()
    return sorted(
        (
            (
                label,
                row[column],
                row["connection"],
                row["variable"],
                row["category"],
                row["type"],
                row["sub_type"],
                row["projection_year"],
                row["value"],
            )
            for row in frame.to_dicts()
        ),
        key=repr,
    )


# --------------------------------------------------------------------------- #
# Fixtures and record shape
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the exact vendor headers (the
    ``Scenario`` / ``Pathway`` epochs and each edition's first year), CRLF line endings and each
    original's BOM (2023, 2024 and the held 2020-2022 bodies have one, 2025 and 2026 do not), blank
    ``Type`` and ``SubType`` cells, a negative value, a zero, blank projection cells (2026's whole
    2024 column and its 2037-2050 columns), a ``(TWh)``, ``(MW)`` and ``(GWh)`` variable, the
    2021 / 2022 preamble with its header on physical line 10 and their whitespace-padded numbers,
    and the 2020 thousands separators and ``N/A`` cells."""
    for alias in CAPTURES:
        assert body(alias).count(b"\r\n") >= body(alias).count(b"\n") - 1, alias
    bom = b"\xef\xbb\xbf"
    for with_bom in ("e20", "e21", "e22", "e23", "e24"):
        assert body(with_bom).startswith(bom), with_bom
    for without in ("e25", "e26"):
        assert not body(without).startswith(bom), without
    for alias in TYPED:
        meta = CAPTURES[alias]
        header, _ = table(alias)
        label = LABEL_COLUMN[meta.edition]
        assert header[:6] == ["Connection", label, "Variable", "Category", "Type", "SubType"]
        assert label_columns(header)[0] == str(FIRST_YEAR[meta.edition])
        assert label_columns(header)[-1] == "2050"
        assert len(label_columns(header)) == LABELS_PER_ROW[meta.edition]
        cells = [v for r in rows(alias) for k, v in r.items() if k.isdigit()]
        assert any(c != "" and float(c) == 0 for c in cells), alias
        assert any(c.startswith("-") for c in cells), alias
        assert "" in cells or alias == "e26", alias
        variables = {r["Variable"] for r in rows(alias)}
        assert any(v.endswith("(TWh)") for v in variables), alias
        assert any(v.endswith("(MW)") for v in variables), alias
        assert any(r["Type"] == "" for r in rows(alias)), alias
        assert any(r["SubType"] == "" for r in rows(alias)), alias
    assert any(r["Type"] == "" and r["SubType"] == "" for r in rows("e23"))
    assert any(r["Variable"].endswith("(GWh)") for r in rows("e23"))
    assert {r["2024"] for r in rows("e26")} == {""}
    assert any(r["2036"] != "" for r in rows("e26"))
    assert any(r["2037"] == "" for r in rows("e26"))
    assert all(r["2050"] == "" for r in rows("e26"))
    assert header_index("e20") == 0
    assert header_index("e21") == header_index("e22") == 9
    for alias in ("e21", "e22"):
        padded = [v for r in rows(alias) for k, v in r.items() if k.isdigit() and v != ""]
        assert padded and all(v != v.strip() for v in padded), alias
        assert lines(alias)[0][0] == "ES1: Electricity supply data table"
    e20_cells = [v for r in rows("e20") for k, v in r.items() if k.isdigit()]
    assert any("," in v for v in e20_cells) and "N/A" in e20_cells


def test_record_shape_matches_the_unit_spec() -> None:
    """Detects a record that drifts from the spec: one csv/utf-8 record, no issue time, temporal
    ``none``, whole-capture selection per ``resource_id``, ``ckan_last_modified`` vintage, the four
    exact ``edition_by_filename`` pairs (none for the held 2020-2022 filenames) and the spec's
    entity key."""
    record = _record()
    assert (record.version, record.reader, record.encoding) == ("1", "csv", "utf-8")
    assert record.temporal.kind == "none"
    assert record.latest == "whole_capture"
    assert record.latest_partition == "resource_id"
    assert record.vintage == "ckan_last_modified"
    assert record.xlsx is None and record.siblings == ()
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    assert record.edition_by_filename == EDITIONS
    mapped = {filename for filename, _edition in record.edition_by_filename or ()}
    assert mapped.isdisjoint({CAPTURES[a].filename for a in HELD_ALIASES})
    assert record.entity_key == KEY


def test_four_unpivot_epochs_with_the_exact_year_labels() -> None:
    """Detects a dropped, invented or mis-mapped year, an epoch merged away, or a blank column
    removed: four epochs (the 2023, 2024, 2025 and 2026 headers differ in label column or first
    year) whose year labels are exactly their header's year columns in header order, each mapped to
    its integer, ending in 2050, the value a nullable float64 with no token or bound. 2026's
    all-blank 2024 and 2037-2050 columns stay in the list (the 3375 long rows count them)."""
    record = _record()
    assert len(record.epochs) == 4
    for epoch, (_filename, edition) in zip(record.epochs, EDITIONS, strict=True):
        assert epoch.unpivot is not None
        labels = [label for label, _ in epoch.unpivot.years]
        assert labels == label_columns(list(epoch.header))
        years = [year for _, year in epoch.unpivot.years]
        assert years == [int(label) for label in labels]
        assert years == list(range(FIRST_YEAR[edition], 2051))
        assert len(years) == LABELS_PER_ROW[edition]
        assert epoch.unpivot.value.dtype == "float64" and epoch.unpivot.value.nullable is True
        assert epoch.unpivot.value.null_tokens == ()
        assert (epoch.unpivot.value.min, epoch.unpivot.value.max) == (None, None)
    last = record.epochs[3].unpivot
    assert last is not None
    assert {"2024", "2037", "2050"} <= {label for label, _ in last.years}


def test_scenario_and_pathway_are_two_nullable_columns_never_merged() -> None:
    """Detects ``Scenario`` and ``Pathway`` renamed into one label (the vendor does not equate
    them): 2023 and 2026 declare only ``scenario``, 2024 and 2025 only ``pathway``, both are
    nullable strings (null in the other editions' rows) and both are in the entity key."""
    record = _record()
    for epoch, (_filename, edition) in zip(record.epochs, EDITIONS, strict=True):
        by_source = {c.source: c for c in epoch.columns}
        used, other = (
            ("Scenario", "Pathway")
            if LABEL_COLUMN[edition] == "Scenario"
            else ("Pathway", "Scenario")
        )
        assert other not in by_source
        assert (by_source[used].name, by_source[used].dtype, by_source[used].nullable) == (
            used.lower(),
            "string",
            True,
        )
    assert {"scenario", "pathway"} <= set(record.entity_key)


def test_dimension_columns_are_strings_and_the_unit_stays_in_variable() -> None:
    """Detects an identifier cast to a number or a unit column invented: ``Connection`` and
    ``Variable`` are non-nullable strings, ``Category``, ``Type`` and ``SubType`` nullable strings
    (the vendor leaves lower-level dimensions blank), no null token anywhere, and the only
    silver columns are the six dimensions plus the unpivot's ``projection_year`` and ``value``."""
    for epoch in _record().epochs:
        by_name = {c.name: c for c in epoch.columns}
        for name in ("connection", "variable"):
            assert (by_name[name].dtype, by_name[name].nullable) == ("string", False), name
        for name in ("category", "type", "sub_type"):
            assert (by_name[name].dtype, by_name[name].nullable) == ("string", True), name
        assert all(c.dtype == "string" and c.null_tokens == () for c in epoch.columns)
        assert len(epoch.columns) == 6
    names = [name for name, _type in generic.output_columns(_record())]
    assert not [n for n in names if "unit" in n.lower()]
    assert "projection_year" in names and "value" in names


def test_held_and_eligible_exactly_as_ruled() -> None:
    """Detects a hold lost or reworded, or a package-level hold: the family is held E-SEM with the
    spec's verbatim question while the package stays eligible."""
    registry = load_registry()
    package, entry = registry.families[ES1]
    effective = effective_eligibility(package, entry)
    assert effective.status == "held"
    assert effective.unit == "E-SEM"  # type: ignore[union-attr]
    assert effective.question == HELD_QUESTION  # type: ignore[union-attr]
    assert package.eligibility.status == "eligible"


def test_the_family_lives_in_its_package_file() -> None:
    """Detects a record put in another package file: the ES1 record is in the ES1 package file."""
    document = json.loads((REGISTRY_DIR / f"{PACKAGE}.json").read_text(encoding="utf-8"))
    assert {f["key"] for f in document["families"] if f.get("record")} == {ES1}


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", TYPED)
def test_fixture_unpivots_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the BOM
    bodies, the Scenario / Pathway swap), a cast the vendor body does not satisfy, a row excluded
    or lost, a blank projection cell dropped, a blank ``Type`` / ``SubType`` excluded and a stamp
    wrong: the capture completes with one long row per source row and header year label, zero
    exclusions, ``edition`` from the filename, the resource id stamped, a unique key, and every
    cell equal to the vendor's."""
    meta = CAPTURES[alias]
    capture_id, written = _run(data, alias)
    header, records = table(alias)
    per_row = len(label_columns(header))
    assert per_row == LABELS_PER_ROW[meta.edition]
    assert written == len(records) * per_row
    completion = read_completion(data, ES1, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == written
    assert completion["rows_excluded"] == 0

    frame = _silver(data)
    expected_columns = [name for name, _type in generic.output_columns(_record())]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected_columns if c not in ("year", "month")
    ]
    assert frame.schema["projection_year"] == pl.Int64
    assert frame.schema["value"] == pl.Float64
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame.select(list(KEY)).is_duplicated().sum() == 0
    assert actual_long(frame, LABEL_COLUMN[meta.edition]) == expected_long(alias)


def test_scenario_and_pathway_land_in_separate_columns(data: Path) -> None:
    """Detects the two vendor labels merged in the silver: 2023 and 2026 rows have a ``scenario``
    and a null ``pathway``; 2024 and 2025 rows the reverse; the key keeps all four captures'
    rows distinct."""
    for alias in TYPED:
        _run(data, alias)
    frame = _silver(data)
    for edition in (2023, 2026):
        part = frame.filter(pl.col("edition") == edition)
        assert part["scenario"].null_count() == 0 and part["pathway"].null_count() == part.height
    for edition in (2024, 2025):
        part = frame.filter(pl.col("edition") == edition)
        assert part["pathway"].null_count() == 0 and part["scenario"].null_count() == part.height
    assert set(frame.filter(pl.col("edition") == 2026)["scenario"].to_list()) == {
        "Ten Year Outlook"
    }
    assert frame.select(list(KEY)).is_duplicated().sum() == 0


def test_blank_projection_cells_are_null_values_never_zero(data: Path) -> None:
    """Detects a blank cell read as zero or dropping its row, and a genuine zero turned into null
    (FACTS: 2023 has 89 blank cells in each year from 2029): every blank projection cell of the
    four fixtures is a null-valued long row, every ``0`` / ``0.0`` stays ``0.0``."""
    for alias in TYPED:
        _run(data, alias)
    frame = _silver(data)
    blanks = sum(v == "" for a in TYPED for r in rows(a) for k, v in r.items() if k.isdigit())
    zeros = sum(
        v != "" and float(v) == 0
        for a in TYPED
        for r in rows(a)
        for k, v in r.items()
        if k.isdigit()
    )
    assert blanks > 0 and zeros > 0
    assert frame["value"].null_count() == blanks
    assert (frame["value"] == 0.0).sum() == zeros


def test_the_2026_blank_columns_are_kept_as_null_valued_rows(data: Path) -> None:
    """Detects 2026's all-blank 2024 and 2037-2050 columns dropped (FACTS: 125 blank cells in each,
    the 3375 long rows count them) or filled: every fixture row has a projection-year-2024 row and
    a 2037-2050 row, all null, and the row count is source rows x 27."""
    capture_id, written = _run(data, "e26")
    n = len(rows("e26"))
    assert written == n * 27
    frame = _silver(data)
    blank_years = frame.filter(
        (pl.col("projection_year") == 2024) | (pl.col("projection_year") >= 2037)
    )
    assert blank_years.height == n * (1 + 14)
    assert blank_years["value"].null_count() == blank_years.height
    nonblank = frame.filter(
        (pl.col("projection_year") >= 2025) & (pl.col("projection_year") <= 2036)
    )
    assert nonblank["value"].null_count() < nonblank.height
    completion = read_completion(data, ES1, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0


def test_blank_type_and_sub_type_are_null_and_keep_their_rows(data: Path) -> None:
    """Detects a row with a blank ``Type`` or ``SubType`` excluded, or the blank kept as text
    (the 2023 CO2 rows, FACTS: Type 20 and SubType 41 blank): the rows are written, the dimension
    is null and nothing else is."""
    _run(data, "e23")
    frame = _silver(data)
    n_type = sum(r["Type"] == "" for r in rows("e23"))
    n_sub = sum(r["SubType"] == "" for r in rows("e23"))
    assert n_type > 0 and n_sub > n_type
    assert frame["type"].null_count() == n_type * 29
    assert frame["sub_type"].null_count() == n_sub * 29
    assert "" not in frame["type"].drop_nulls().to_list()
    assert frame["category"].null_count() == 0
    assert frame["connection"].null_count() == frame["variable"].null_count() == 0


def test_the_unit_stays_in_the_variable_verbatim(data: Path) -> None:
    """Detects a unit parsed out of ``Variable`` or a label normalised (FACTS: the exact label is
    the unit carrier; ``gCO2/KWh`` and ``gCO2/kWh`` are different spellings): every variable
    equals its cell byte for byte, and the MW, GWh, TWh and CO2 labels all survive."""
    for alias in TYPED:
        _run(data, alias)
    variables = set(_silver(data)["variable"].to_list())
    assert variables == {r["Variable"] for a in TYPED for r in rows(a)}
    for needle in ("(MW)", "(GWh)", "(TWh)", "(gCO2/kWh)"):
        assert any(needle in v for v in variables), needle


def test_negative_values_survive(data: Path) -> None:
    """Detects a sign lost (exports, net flows and CO2 are negative): the negative cells of the
    four fixtures are negative values, equal to the vendor's."""
    for alias in TYPED:
        _run(data, alias)
    frame = _silver(data)
    negatives = sorted(
        float(v)
        for a in TYPED
        for r in rows(a)
        for k, v in r.items()
        if k.isdigit() and v.startswith("-")
    )
    assert negatives
    assert sorted(frame.filter(pl.col("value") < 0)["value"].to_list()) == negatives


@pytest.mark.parametrize(
    ("alias", "label", "value"),
    [
        ("e24", "2030", "3,118.00"),
        ("e23", "2030", "N/A"),
        ("e25", "2040", "0.03 "),
        ("e26", "2030", "-163.99 "),
    ],
)
def test_an_uncastable_cell_fails_the_capture(
    data: Path, alias: str, label: str, value: str
) -> None:
    """Detects a record that swallows a bad cell (a null-token list, a lenient cast, a thousands
    separator, padding): the exact lexical forms that fail the 2020-2022 bodies (``3,118.00``,
    ``N/A``, ``0.03 ``) fail the whole capture before any row is written and leave no
    completion."""
    raw = _replace_cell(body(alias), 2, table(alias)[0].index(label), value)
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, ES1, data).run(DAY, run_id="r")
    assert read_completion(data, ES1, capture_id) is None
    assert _no_silver(data)


def test_a_blank_connection_is_excluded_and_counted(data: Path) -> None:
    """Detects an identifier made nullable (a blank ``Connection`` becoming a null-keyed row): the
    source row's every long row is excluded, counted on the transformer and in the completion,
    and no null reaches the output."""
    raw = _replace_cell(body("e24"), 2, table("e24")[0].index("Connection"), "")
    capture_id = capture(data, "e24", raw=raw)
    transformer = get_transformer(SOURCE, ES1, data)
    written = transformer.run(DAY, run_id="r")
    assert written == (len(rows("e24")) - 1) * 28
    assert transformer.last_excluded_row_count == 28
    completion = read_completion(data, ES1, capture_id)
    assert completion is not None and completion["rows_excluded"] == 28
    assert _silver(data)["connection"].null_count() == 0


def test_a_repeated_dimension_tuple_fails_the_capture(data: Path) -> None:
    """Detects a key too coarse to guard the body: a repeated dimension tuple (same connection,
    scenario, variable, category, type and subtype) with other values fails with
    ``DuplicateEntityKeyError`` naming the shared rows and leaves no completion."""
    raw = body("e23")
    first = raw.decode("utf-8-sig").split("\r\n")[1]
    changed = first.replace("53.74129", "54.00001", 1)
    assert changed != first
    capture_id = capture(data, "e23", raw=raw + changed.encode() + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, ES1, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert f"{2 * 29} row(s) sharing an entity key" in info.value.failures[0][2]
    assert read_completion(data, ES1, capture_id) is None
    assert _no_silver(data)


# --------------------------------------------------------------------------- #
# Editions
# --------------------------------------------------------------------------- #


def test_editions_are_stamped_from_the_filename_and_all_are_served(data: Path) -> None:
    """Detects an edition inferred from the vintage or the year labels, or one edition displacing
    another: 2024, 2025 and 2026 stamp their filename's year (their labels start in 2023 and
    2024) and ``_latest`` serves each."""
    ids = {a: _run(data, a)[0] for a in ("e24", "e25", "e26")}
    frame = _silver(data)
    by_capture = {
        cid: set(editions)
        for cid, editions in frame.group_by("bronze_capture_id")
        .agg(pl.col("edition").unique())
        .iter_rows()
    }
    assert by_capture == {ids["e24"]: {2024}, ids["e25"]: {2025}, ids["e26"]: {2026}}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, ES1, None)) == set(ids.values())


@pytest.mark.parametrize(
    ("alias", "wrong"),
    [
        ("e24", "fes2026_es1_v001.csv"),
        ("e23", "fes2023_es1_v002.csv.bak"),
        ("e23", "fes2022_es1_v001.csv"),
        ("e25", "fes_es1.csv"),
    ],
)
def test_an_unmapped_filename_fails_loud(data: Path, alias: str, wrong: str) -> None:
    """Detects a fallback edition: a filename the map does not list (a new year, a suffixed copy,
    or a held edition's name) fails the capture, leaves no completion and writes nothing."""
    capture_id = capture(data, alias, filename=wrong)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, ES1, data).run(DAY, run_id="r")
    assert read_completion(data, ES1, capture_id) is None
    assert _no_silver(data)


# --------------------------------------------------------------------------- #
# The 2020, 2021 and 2022 resources: held
# --------------------------------------------------------------------------- #


def test_the_three_early_resources_are_held_with_their_reasons() -> None:
    """Detects an early resource routed to silver, dispositioned DOC/GIS, given an edition
    entry or left without its reason: 2020, 2021 and 2022 alone are resource-level HOLD (E-SEM)
    quoting FACTS (the 2020 comma and ``N/A`` cell counts, the 2021 / 2022 header on physical
    line 10 and padding), the four others are SILVER, and the held filenames have no edition
    pair."""
    registry = load_registry()
    for alias, needles in HOLD_REASONS.items():
        held = registry.resources[CAPTURES[alias].resource_id][1]
        assert held.family == ES1
        assert isinstance(held.disposition, HoldDisposition)
        assert held.disposition.unit == "E-SEM"
        for needle in needles:
            assert needle in held.disposition.reason, (alias, needle)
        assert "new last_modified" in held.disposition.reason
    others = [
        r
        for _p, r in registry.resources.values()
        if r.family == ES1 and r.id not in {CAPTURES[a].resource_id for a in HELD_ALIASES}
    ]
    assert sorted(r.id for r in others) == sorted(CAPTURES[a].resource_id for a in TYPED)
    assert all(isinstance(r.disposition, SilverDisposition) for r in others)
    held_resources = sorted(
        r.id
        for p in registry.packages
        if p.package == PACKAGE
        for r in p.resources
        if isinstance(r.disposition, HoldDisposition)
    )
    assert held_resources == sorted(CAPTURES[a].resource_id for a in HELD_ALIASES)


def test_the_held_bodies_are_not_readable_by_the_record(data: Path) -> None:
    """Detects an early body that the record could read (the claim behind each HOLD): the 2020
    header (years from 2019) and the 2021 / 2022 preamble line match no epoch, the 2020 body has
    cells that fail the strict float64 cast, and the 2021 / 2022 numbers are all padded."""
    record = _record()
    for alias in HELD_ALIASES:
        capture(data, alias)
        meta = CAPTURES[alias]
        path = next((data / "bronze" / SOURCE / ES1).rglob(f"*{meta.resource_id}*.csv"))
        parsed = next(read_csv_body(path, record, ()))
        assert not any(list(epoch.header) == list(parsed.header) for epoch in record.epochs)
    uncastable = [
        v
        for r in rows("e20")
        for k, v in r.items()
        if k.isdigit() and v != "" and not re.fullmatch(r"-?\d+(\.\d+)?", v)
    ]
    assert any("," in v for v in uncastable) and "N/A" in uncastable
    for alias in ("e21", "e22"):
        padded = [v for r in rows(alias) for k, v in r.items() if k.isdigit() and v != ""]
        assert all(not re.fullmatch(r"-?\d+(\.\d+)?", v) for v in padded)


def test_held_captures_are_never_transformed_and_not_a_gap(data: Path) -> None:
    """Detects an early capture reaching an owner (a failed capture and a reconcile gap) or
    expected by reconcile despite its HOLD: with all seven fixtures captured the four SILVER
    editions complete alone, the three held captures have no completion and no failure, and
    reconcile reports no gap."""
    held_ids = {a: capture(data, a) for a in HELD_ALIASES}
    expected = sum(len(rows(a)) * LABELS_PER_ROW[CAPTURES[a].edition] for a in TYPED)
    for alias in TYPED:
        capture(data, alias)
    assert get_transformer(SOURCE, ES1, data).run(DAY, run_id="r") == expected
    for alias, capture_id in held_ids.items():
        assert read_completion(data, ES1, capture_id) is None, alias
        assert read_failure(data, ES1, capture_id) is None, alias
    report = reconcile(data, load_registry(), [ES1], DAY)
    assert report.gaps == (), report.lines()
    frame = _silver(data)
    assert set(frame["edition"].to_list()) == {2023, 2024, 2025, 2026}
    assert set(frame["resource_id"].to_list()) == {CAPTURES[a].resource_id for a in TYPED}


# --------------------------------------------------------------------------- #
# Vintage, the catalogue and generated artefacts
# --------------------------------------------------------------------------- #


def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(data: Path) -> None:
    """Detects an issue-time proxy or a projection-year clock (RULINGS 529/597), or a catalogue
    view that cannot carry the unpivot columns: ``available_at`` is the CKAN ``last_modified``
    (2024-08-02), ``timestamp_utc`` stays the capture time, an as-of read before the vintage serves
    nothing and one after it serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES["e24"]
    capture_id, _ = _run(data, "e24")
    frame = _silver(data)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, ES1, datetime(2024, 8, 1, tzinfo=UTC)) == []
    assert set(both_as_of(db, data, ES1, datetime(2024, 8, 3, tzinfo=UTC))) == {capture_id}
    assert set(both_as_of(db, data, ES1, None)) == {capture_id}


def test_the_skeleton_page_renders_the_new_record() -> None:
    """Detects a record the docs generator cannot render (the unpivot columns, the scenario and
    pathway columns, the held question, the held resources)."""
    page = skeleton.render_package(
        load_registry(),
        {
            "name": PACKAGE,
            "title": "Future Energy Scenario Electricity Supply Data table (ES1)",
            "organization": {"title": "FES: Pathways to Net Zero"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    for needle in ("`projection_year`", "`scenario`", "`pathway`", "`sub_type`", ES1):
        assert needle in page, needle
