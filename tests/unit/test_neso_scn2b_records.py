"""The FES ED3 / ED7 heat-and-gas package and the ED4 gas-demand definitions (v0.22-K-SCN-2b).

Four families of two NESO packages get record version 1:

- ``fes_ed3_heat_demand_natural_gas`` (the 2023 / 2024 combined ED3 table),
  ``fes_ed3_heat_demand_natural_gas_sum_ed`` (the 2025 / 2026 natural-gas summary ED3) and
  ``fes_ed3_heat_demand_sum_ed`` (the 2025 / 2026 heat summary ED7): wide, one unpivot header
  epoch per edition, held E-SEM (the period a year label denotes, the scale of ``uptake`` /
  ``stock`` / ``number`` and the meaning of a blank projection cell are undocumented). The three
  split one vendor table content-wise but not row for row, so they stay three silver datasets
  and are never added, unioned or summed.
- ``fes_ed4_gas_demand_defs`` (ED4): the codebook of the ED3 items, a reference table with no
  years, eligible.

Every test writes recorded fixture captures into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here, not
at activation. On master the four families have no record, so ``get_transformer`` raises.

Fixtures (``tests/fixtures/neso_data_portal/scn2b/``, provenance in ``PROVENANCE.md``): each file
is a cut of the 2026-10-08 swept bronze (a scratch script, not committed): whole vendor records,
byte for byte, chosen for the oddities named in the tests (the ED4 bodies are whole).
``.gitattributes`` keeps their CRLF terminators and BOMs.
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
from gridflow.connectors.neso_data_portal.registry import SilverDisposition, load_registry
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

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "scn2b"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)

ED3C = "fes_ed3_heat_demand_natural_gas"
ED3G = "fes_ed3_heat_demand_natural_gas_sum_ed"
ED7 = "fes_ed3_heat_demand_sum_ed"
ED4 = "fes_ed4_gas_demand_defs"
FAMILIES = (ED3C, ED3G, ED7, ED4)
UNPIVOT_FAMILIES = (ED3C, ED3G, ED7)
HELD_FAMILIES = UNPIVOT_FAMILIES

ED3_PACKAGE = (
    "fes-natural-gas-residential-and-non-domestic-i-c-heat-demand-summary-data-table-ed3",
    "2fd7b713-9a85-45d8-87d8-cb3f01fd0e89",
)
ED4_PACKAGE = ("fes-natural-gas-demand-definitions-ed4", "af30ab4c-eb46-487c-80bd-84a57d7d5f18")
PACKAGES = {ED3C: ED3_PACKAGE, ED3G: ED3_PACKAGE, ED7: ED3_PACKAGE, ED4: ED4_PACKAGE}

HELD_QUESTIONS = {
    ED3C: (
        "TODO: For each heat and gas measure, define its year interval or stock/snapshot date "
        "and whether the integer labels a starting or ending year. Define the scale and "
        "denominator of `uptake`, the basis of `number`, and the timeframe labels including "
        "cumulative and peak outputs. Confirm the reported unit for the peak daily gas-demand "
        "item in the earlier table. What does each blank projection cell mean?"
    ),
    ED3G: (
        "TODO: What interval does each `Annuals` GWh value cover, and how does its integer year "
        "label identify that interval? What does a blank projection cell mean? Please confirm "
        "the edition-specific coverage of electricity-generation gas demand and the "
        "network-constraint treatment described in ED4."
    ),
    ED7: (
        "TODO: For every ED7 unit and data type, what interval or observation date does a year "
        "header denote? What are the scale and denominator of `uptake`, the counting basis and "
        "snapshot date of `stock`, and the start/end convention for peak-year labels? How are "
        "savings related to the reported technology demand? What does a blank projection cell "
        "mean?"
    ),
}

KEYS = {
    ED3C: (
        "resource_id",
        "edition",
        "scenario",
        "sector",
        "fuel",
        "technology",
        "time",
        "units",
        "pathway",
        "sector_aggregation_level",
        "data_type",
        "projection_year",
    ),
    ED3G: (
        "resource_id",
        "edition",
        "pathway",
        "description",
        "fuel",
        "component",
        "data_type",
        "units",
        "scenario",
        "projection_year",
    ),
    ED7: (
        "resource_id",
        "edition",
        "pathway",
        "sector_aggregation_level",
        "fuel",
        "technology",
        "data_type",
        "units",
        "scenario",
        "projection_year",
    ),
    ED4: ("resource_id", "edition", "item_no"),
}

EDITIONS = {
    ED3C: (("fes2023_ed3_v001.csv", 2023), ("fes2024_ed3_v005.csv", 2024)),
    ED3G: (("fes2025_ed3_v006.csv", 2025), ("10yo2026_ed3_v001.csv", 2026)),
    ED7: (("fes2025_ed7_v006.csv", 2025), ("10yo2026_ed7_v001.csv", 2026)),
    ED4: (
        ("fes2023_ed4_v001.csv", 2023),
        ("fes2024_ed4_v001.csv", 2024),
        ("fes2025_ed4_v006.csv", 2025),
        ("10yo2026_ed4_v001.csv", 2026),
    ),
}

# Vendor identifier header -> silver name, with the spec's nullability.
ED3C_COLUMNS = {
    "Scenario": ("scenario", True),
    "Sector": ("sector", True),
    "Fuel": ("fuel", True),
    "Technology": ("technology", False),
    "Time": ("time", True),
    "Units": ("units", False),
    "Pathway": ("pathway", True),
    "Sector/Aggregation Level": ("sector_aggregation_level", True),
    "Data Type": ("data_type", True),
}
ED3G_COLUMNS = {
    "Pathway": ("pathway", True),
    "Scenario": ("scenario", True),
    "Description": ("description", False),
    "Fuel": ("fuel", False),
    "Component": ("component", False),
    "Data Type": ("data_type", False),
    "Units": ("units", False),
}
ED7_COLUMNS = {
    "Pathway": ("pathway", True),
    "Scenario": ("scenario", True),
    "Sector/Aggregation Level": ("sector_aggregation_level", False),
    "Fuel": ("fuel", True),
    "Technology": ("technology", False),
    "Data Type": ("data_type", False),
    "Units": ("units", False),
}
ED4_COLUMNS = {
    "Item no.": ("item_no", False),
    "Description": ("description", False),
    "Definition": ("definition", False),
}
COLUMNS = {ED3C: ED3C_COLUMNS, ED3G: ED3G_COLUMNS, ED7: ED7_COLUMNS, ED4: ED4_COLUMNS}


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
    "ed3c_23": Capture(
        ED3C,
        "1f1de28d-eff7-4c5b-83fa-33cd7ad1bd39",
        "Natural Gas and Residential Heat Demand Summary (ED3) 2023",
        "fes2023_ed3_v001.csv",
        2023,
        "2023-07-10T03:31:06.331229",
        "2026-10-08T10:53:59.217853+00:00",
    ),
    "ed3c_24": Capture(
        ED3C,
        "fb452e53-881c-4d4e-a236-a5b6b50f2840",
        "Natural Gas and Residential Heat Demand Summary (ED3) 2024",
        "fes2024_ed3_v005.csv",
        2024,
        "2025-02-17T17:12:48.492536",
        "2026-10-08T10:54:01.922700+00:00",
    ),
    "ed3g_25": Capture(
        ED3G,
        "f33e8ded-d302-40c4-be10-c7ca3a04b401",
        " Natural Gas Demand Summary (ED3) 2025",
        "fes2025_ed3_v006.csv",
        2025,
        "2025-12-10T16:50:20.589918",
        "2026-10-08T10:54:06.022320+00:00",
    ),
    "ed3g_26": Capture(
        ED3G,
        "c3d4acaf-5cd5-47f8-81a9-a4c10e16d55a",
        "Natural Gas Demand Summary (ED3) 2026",
        "10yo2026_ed3_v001.csv",
        2026,
        "2026-09-16T14:54:22.558692",
        "2026-10-08T10:54:08.488330+00:00",
    ),
    "ed7_25": Capture(
        ED7,
        "2019e900-11eb-40ce-ae54-a49a2abb5d4b",
        "Heat Demand Summary (ED7) 2025",
        "fes2025_ed7_v006.csv",
        2025,
        "2025-12-10T16:51:16.997421",
        "2026-10-08T10:54:12.302570+00:00",
    ),
    "ed7_26": Capture(
        ED7,
        "9394f025-8454-4db0-b73c-083e301a1e74",
        "Heat Demand Summary (ED7) 2026",
        "10yo2026_ed7_v001.csv",
        2026,
        "2026-09-16T14:55:40.190751",
        "2026-10-08T10:54:15.301755+00:00",
    ),
    "ed4_23": Capture(
        ED4,
        "60a8299f-9ba1-4663-9ece-8005a8a42fa2",
        "Natural Gas demand definitions (ED4) 2023",
        "fes2023_ed4_v001.csv",
        2023,
        "2023-07-10T03:31:27.976626",
        "2026-10-08T10:54:18.774686+00:00",
    ),
    "ed4_24": Capture(
        ED4,
        "a014dfba-0728-448a-a2cc-d34fec0703af",
        "Natural Gas demand definitions (ED4) 2024",
        "fes2024_ed4_v001.csv",
        2024,
        "2024-07-15T06:40:17.398169",
        "2026-10-08T10:54:21.628757+00:00",
    ),
    "ed4_25": Capture(
        ED4,
        "74485527-6c09-4c1a-bf8c-8569810eb918",
        "Natural Gas demand definitions (ED4) 2025",
        "fes2025_ed4_v006.csv",
        2025,
        "2025-12-10T16:56:34.330431",
        "2026-10-08T10:54:24.609913+00:00",
    ),
    "ed4_26": Capture(
        ED4,
        "b5ae9932-2ce9-42f0-a820-15a13f0eedd0",
        "Natural Gas demand definitions (ED4) 2026",
        "10yo2026_ed4_v001.csv",
        2026,
        "2026-09-16T14:59:52.804239",
        "2026-10-08T10:54:27.264484+00:00",
    ),
}
UNPIVOT_ALIASES = tuple(a for a, c in CAPTURES.items() if c.family != ED4)
ED4_ALIASES = tuple(a for a, c in CAPTURES.items() if c.family == ED4)
FIRST_YEAR = {"ed3c_23": 2020, "ed3c_24": 2020}
FIRST_YEAR.update({a: 2023 for a in ("ed3g_25", "ed3g_26", "ed7_25", "ed7_26")})
BOMS = ("ed3c_24", "ed4_24")
# Aliases whose body has no embedded newline inside a record (cells can be patched by line).
LINE_BODIES = tuple(a for a in CAPTURES if a != "ed4_25")


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
    them: a bare ``\\n`` outside a quoted cell is a record terminator, an embedded one (the ED4
    2025 definition) stays."""
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
    original's BOM (the 2024 ED3 and ED4 bodies have one), the exact vendor headers and each
    edition's first year, blank / zero projection cells, a blank ``Fuel``, the ``number``,
    ``uptake`` and ``stock`` units, the peak row labelled ``GWh`` (2023) and ``GWh/day`` (later),
    the all-blank ED7 2026 ``2023`` / ``2024`` columns, the ED4 embedded newline, and ED4 whole
    (10 / 10 / 11 / 11 items)."""
    bom = b"\xef\xbb\xbf"
    for alias in CAPTURES:
        raw = body(alias)
        assert raw.startswith(bom) == (alias in BOMS), alias
        assert raw.endswith(b"\r\n"), alias
        assert b"\r\n" in raw, alias
    for alias in UNPIVOT_ALIASES:
        header, _ = table(alias)
        assert label_columns(header)[0] == str(FIRST_YEAR[alias]), alias
        assert label_columns(header)[-1] == "2050", alias
    assert table("ed3c_23")[0][:6] == ["Scenario", "Sector", "Fuel", "Technology", "Time", "Units"]
    assert table("ed3c_24")[0][:6] == [
        "Pathway",
        "Sector/Aggregation Level",
        "Fuel",
        "Technology",
        "Data Type",
        "Units",
    ]
    for alias in ("ed3g_25", "ed3g_26"):
        assert table(alias)[0][1:6] == ["Description", "Fuel", "Component", "Data Type", "Units"]
    assert [table(a)[0][0] for a in ("ed3g_25", "ed3g_26")] == ["Pathway", "Scenario"]
    assert [table(a)[0][0] for a in ("ed7_25", "ed7_26")] == ["Pathway", "Scenario"]
    r23 = rows("ed3c_23")
    assert any(r["Fuel"] == "" for r in r23)
    assert {"number", "uptake", "GWh", "TWh"} <= {r["Units"] for r in r23}
    assert any(r["Time"] == "Peak 1-in-20" and r["Units"] == "GWh" for r in r23)
    assert any(r["2028"] == "" for r in r23)
    r24 = rows("ed3c_24")
    assert any(r["Data Type"] == "Peak 1in20" and r["Units"] == "GWh/day" for r in r24)
    assert any(r["Fuel"] == "" for r in r24)
    assert any(r["2050"] == "" for r in r24)
    for alias in ("ed3g_25", "ed3g_26"):
        units = {(r["Data Type"], r["Units"]) for r in rows(alias)}
        assert ("Annuals", "GWh") in units and ("Peak 1in20", "GWh/day") in units, alias
        assert {r["Fuel"] for r in rows(alias)} == {"gas"}, alias
    assert any(r["2036"] == "" and r["2050"] == "" for r in rows("ed3g_25"))
    assert {r["Scenario"] for r in rows("ed3g_26")} == {"Ten Year Outlook"}
    assert {"uptake", "stock"} <= {r["Units"] for r in rows("ed7_25")}
    assert any(r["Fuel"] == "" for r in rows("ed7_25"))
    assert any(r["2050"] == "" for r in rows("ed7_25"))
    r26 = rows("ed7_26")
    assert all(r["2023"] == "" and r["2024"] == "" and r["2050"] == "" for r in r26)
    assert any(r["Fuel"] == "" for r in r26)
    for alias in ED4_ALIASES:
        assert table(alias)[0] == ["Item no.", "Description", "Definition"], alias
    assert [len(table(a)[1]) for a in ED4_ALIASES] == [10, 10, 11, 11]
    assert any("\n" in r["Definition"] for r in rows("ed4_25"))
    for family in UNPIVOT_FAMILIES:
        cells = [
            v
            for a in UNPIVOT_ALIASES
            if CAPTURES[a].family == family
            for r in rows(a)
            for k, v in r.items()
            if k.isdigit()
        ]
        assert any(c != "" and float(c) == 0 for c in cells), family


@pytest.mark.parametrize("family", FAMILIES)
def test_record_shape_matches_the_unit_spec(family: str) -> None:
    """Detects a record that drifts from the spec: one csv/utf-8 record, no issue time, temporal
    ``none``, whole-capture selection per ``resource_id``, ``ckan_last_modified`` vintage, the
    exact ``edition_by_filename`` pairs and the spec's entity key."""
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
        (ED3C, ("ed3c_23", "ed3c_24")),
        (ED3G, ("ed3g_25", "ed3g_26")),
        (ED7, ("ed7_25", "ed7_26")),
    ],
)
def test_unpivot_epochs_carry_the_exact_year_labels(family: str, aliases: tuple[str, ...]) -> None:
    """Detects a dropped, invented or mis-mapped year, an epoch merged away, or a blank column
    removed: one epoch per edition (the label column differs), each frozen exactly as the real
    body's reader-returned header, the year labels exactly the header's year columns in order,
    each mapped to its integer and ending in 2050, the value a nullable float64 with no token or
    bound."""
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


def test_the_all_blank_ed7_2026_columns_are_declared_in_full() -> None:
    """Detects a blank-only year column left out of the unpivot (FACTS: ED7 2026's ``2023``,
    ``2024`` and ``2037``-``2050`` are blank in all 249 rows and the 6,972 long rows count them):
    all 28 labels are declared, the gas summary 2026's blank tail too."""
    ed7 = _record(ED7).epochs[1].unpivot
    ed3g = _record(ED3G).epochs[1].unpivot
    assert ed7 is not None and ed3g is not None
    assert {"2023", "2024", "2037", "2050"} <= {label for label, _ in ed7.years}
    assert {"2037", "2050"} <= {label for label, _ in ed3g.years}
    assert (len(ed7.years), len(ed3g.years)) == (28, 28)


def test_the_reference_record_has_one_epoch_and_no_unpivot() -> None:
    """Detects ED4 forced into the unpivot shape or split per edition: one ``Item no.`` /
    ``Description`` / ``Definition`` epoch for all four editions, no ``projection_year`` and no
    ``value``, ``item_no`` an ``int64`` identifier (the vendor's ``"Unique Key"``, not a
    float64 measure)."""
    record = _record(ED4)
    assert len(record.epochs) == 1
    epoch = record.epochs[0]
    assert epoch.unpivot is None
    assert list(epoch.header) == ["Item no.", "Description", "Definition"]
    assert [(c.source, c.name, c.dtype, c.nullable) for c in epoch.columns] == [
        ("Item no.", "item_no", "int64", False),
        ("Description", "description", "string", False),
        ("Definition", "definition", "string", False),
    ]
    assert all(c.null_tokens == () for c in epoch.columns)
    names = [name for name, _type in generic.output_columns(record)]
    assert "projection_year" not in names and "value" not in names


@pytest.mark.parametrize("family", UNPIVOT_FAMILIES)
def test_dimension_columns_are_strings_with_the_spec_nullability(family: str) -> None:
    """Detects an identifier cast to a number, a null token declared, a unit column invented or a
    column nullable that FACTS g5 shows populated: every column is a string without null tokens,
    ``nullable`` exactly as the spec (a column absent from an epoch, or blank in a body - ``Fuel``
    of the combined table and ED7 - is nullable, the rest are not), the vendor names lower-cased
    with non-alphanumeric runs as ``_`` and the silver columns only the dimensions plus
    ``projection_year`` and ``value``."""
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
    dimensions = {name for name, _nullable in COLUMNS[family].values()}
    assert dimensions | {"resource_id", "edition", "projection_year", "value"} <= set(names)
    assert not {"unit", "sector_unit"} & set(names)


def test_differently_named_dimensions_stay_separate_columns() -> None:
    """Detects ``Scenario`` / ``Pathway``, ``Sector`` / ``Sector/Aggregation Level`` or ``Time`` /
    ``Data Type`` merged into one label (the vendor does not equate them, FACTS l.36): each epoch
    declares only the ones its header has, all are nullable strings and in the key."""
    combined = _record(ED3C)
    first, second = combined.epochs
    assert {c.name for c in first.columns} == {
        "scenario",
        "sector",
        "fuel",
        "technology",
        "time",
        "units",
    }
    assert {c.name for c in second.columns} == {
        "pathway",
        "sector_aggregation_level",
        "fuel",
        "technology",
        "data_type",
        "units",
    }
    assert {
        "scenario",
        "sector",
        "time",
        "pathway",
        "sector_aggregation_level",
        "data_type",
    } <= set(combined.entity_key)
    for family in (ED3G, ED7):
        record = _record(family)
        assert [e.header[0] for e in record.epochs] == ["Pathway", "Scenario"]
        for epoch in record.epochs:
            assert len({c.name for c in epoch.columns} & {"pathway", "scenario"}) == 1
        assert {"pathway", "scenario"} <= set(record.entity_key)
        for epoch in record.epochs:
            for column in epoch.columns:
                if column.name in {"pathway", "scenario"}:
                    assert column.nullable is True and column.dtype == "string"


def test_held_and_eligible_exactly_as_ruled() -> None:
    """Detects a hold lost or reworded, or a package-level hold: the combined table, the gas
    summary and ED7 are held E-SEM with the spec's verbatim question, ED4 is eligible (a reference
    codebook with no temporal semantic), and both packages stay eligible."""
    registry = load_registry()
    for family, question in HELD_QUESTIONS.items():
        package, entry = registry.families[family]
        effective = effective_eligibility(package, entry)
        assert effective.status == "held", family
        assert effective.unit == "E-SEM"  # type: ignore[union-attr]
        assert effective.question == question  # type: ignore[union-attr]
        assert package.eligibility.status == "eligible"
    package, entry = registry.families[ED4]
    assert effective_eligibility(package, entry).status == "eligible"
    assert package.eligibility.status == "eligible"
    assert _record(ED4).eligibility is None


def test_the_packages_hold_exactly_the_recorded_families() -> None:
    """Detects a record put in another package file: the ED3 / ED7 package file holds the three
    held families, the ED4 file its one, and nothing else is recorded in either."""
    for slug, expected in (
        (ED3_PACKAGE[0], {ED3C, ED3G, ED7}),
        (ED4_PACKAGE[0], {ED4}),
    ):
        document = json.loads((REGISTRY_DIR / f"{slug}.json").read_text(encoding="utf-8"))
        assert {f["key"] for f in document["families"] if f.get("record")} == expected
        assert {f["key"] for f in document["families"]} == expected


def test_every_resource_is_silver_to_its_own_family() -> None:
    """Detects a resource routed to a HOLD, DOC or the wrong family: all ten resources of the two
    packages stay SILVER to their own family (no resource HOLD in this unit) and every captured
    id is a registry resource."""
    registry = load_registry()
    for slug, families in (
        (ED3_PACKAGE[0], {ED3C, ED3G, ED7}),
        (ED4_PACKAGE[0], {ED4}),
    ):
        package = next(p for p in registry.packages if p.package == slug)
        assert package.eligibility.status == "eligible"
        assert {r.family for r in package.resources} == families
        for resource in package.resources:
            assert isinstance(resource.disposition, SilverDisposition), resource.id
            assert resource.disposition.key == resource.family
        assert {r.id for r in package.resources} == {
            m.resource_id for m in CAPTURES.values() if m.family in families
        }
    for alias, meta in CAPTURES.items():
        assert registry.resources[meta.resource_id][1].family == meta.family, alias


def test_no_adjudication_entry_for_the_four_families() -> None:
    """Detects an ADR-040 recorded-failure entry added (every body casts and passes its key): no
    reconcile adjudication names any of the four families."""
    entries = json.loads((REGISTRY_DIR / "_reconcile_adjudications.json").read_text("utf-8"))
    assert not [e for e in entries if e["family"] in FAMILIES]


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", UNPIVOT_ALIASES)
def test_fixture_unpivots_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the BOM
    body, the Scenario / Pathway swaps), a cast the vendor body does not satisfy, a row excluded
    or lost, a blank projection cell dropped, a blank ``Fuel`` excluded and a stamp wrong: the
    capture completes with one long row per source row and header year label, zero exclusions,
    ``edition`` from the filename, the resource id stamped, a unique key, and every cell equal to
    the vendor's."""
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


@pytest.mark.parametrize("alias", ED4_ALIASES)
def test_reference_fixture_types_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects ED4 rows lost, merged or altered or ``item_no`` left a string / float: one silver
    row per source record, zero exclusions, ``edition`` from the filename, an ``Int64`` unique
    ``item_no`` and every cell (commas and the 2025 embedded newline included) equal to the
    vendor's."""
    meta = CAPTURES[alias]
    capture_id, written = _run(data, alias)
    _header, records = table(alias)
    assert written == len(records)
    completion = read_completion(data, ED4, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, ED4)
    assert frame.schema["item_no"] == pl.Int64
    assert "projection_year" not in frame.columns and "value" not in frame.columns
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame.select(list(KEYS[ED4])).is_duplicated().sum() == 0
    assert frame["item_no"].is_unique().all()
    assert sorted((r["item_no"], r["description"], r["definition"]) for r in frame.to_dicts()) == (
        sorted((int(a), b, c) for a, b, c in records)
    )


def test_the_three_ed3_ed7_families_are_three_silver_datasets(data: Path) -> None:
    """Detects the combined table, the gas summary and ED7 merged, unioned or summed (FACTS
    l.338-360: a content split, not a row partition, never additive): each family writes its own
    silver dataset from its own resources only, with its own columns, and no silver row of one
    family appears in another."""
    for alias in UNPIVOT_ALIASES:
        _run(data, alias)
    frames = {family: _silver(data, family) for family in UNPIVOT_FAMILIES}
    dirs = {family: data / "silver" / SOURCE / family for family in UNPIVOT_FAMILIES}
    assert len({str(d) for d in dirs.values()}) == 3
    assert all(d.is_dir() for d in dirs.values())
    for family, frame in frames.items():
        assert set(frame["resource_id"].to_list()) == {
            m.resource_id for m in CAPTURES.values() if m.family == family
        }
    assert "description" in frames[ED3G].columns and "description" not in frames[ED3C].columns
    assert "component" in frames[ED3G].columns and "component" not in frames[ED7].columns
    assert "time" in frames[ED3C].columns and "time" not in frames[ED7].columns
    resources = [set(f["resource_id"].unique().to_list()) for f in frames.values()]
    assert not (resources[0] & resources[1] or resources[0] & resources[2])
    assert not resources[1] & resources[2]
    assert sum(f.height for f in frames.values()) == sum(
        len(rows(a)) * len(label_columns(table(a)[0])) for a in UNPIVOT_ALIASES
    )


def test_scenario_and_pathway_land_in_separate_columns(data: Path) -> None:
    """Detects the vendor labels merged in the silver: the combined table's 2023 rows carry
    ``scenario`` / ``sector`` / ``time`` only and 2024's ``pathway`` / ``sector_aggregation_level``
    / ``data_type`` only; the gas summary and ED7 have ``pathway`` in 2025 and ``scenario``
    (``Ten Year Outlook``) in 2026; the key keeps every edition's rows distinct."""
    for alias in UNPIVOT_ALIASES:
        _run(data, alias)
    combined = _silver(data, ED3C)
    part23 = combined.filter(pl.col("edition") == 2023)
    part24 = combined.filter(pl.col("edition") == 2024)
    for name in ("scenario", "sector", "time"):
        assert part23[name].null_count() == 0, name
        assert part24[name].null_count() == part24.height, name
    for name in ("pathway", "sector_aggregation_level", "data_type"):
        assert part23[name].null_count() == part23.height, name
        assert part24[name].null_count() == 0, name
    for family in (ED3G, ED7):
        frame = _silver(data, family)
        part25 = frame.filter(pl.col("edition") == 2025)
        part26 = frame.filter(pl.col("edition") == 2026)
        assert part25["scenario"].null_count() == part25.height
        assert part25["pathway"].null_count() == 0
        assert part26["pathway"].null_count() == part26.height
        assert set(part26["scenario"].to_list()) == {"Ten Year Outlook"}
    for family in UNPIVOT_FAMILIES:
        assert _silver(data, family).select(list(KEYS[family])).is_duplicated().sum() == 0


def test_blank_projection_cells_are_null_values_never_zero(data: Path) -> None:
    """Detects a blank cell read as zero or dropping its row, and a genuine zero turned into null
    (FACTS g5: no zero run is a sentinel): in every family every blank projection cell of the
    fixtures is a null-valued long row, every ``0`` / ``0.0`` stays ``0.0``."""
    for family in UNPIVOT_FAMILIES:
        aliases = [a for a in UNPIVOT_ALIASES if CAPTURES[a].family == family]
        for alias in aliases:
            _run(data, alias)
        frame = _silver(data, family)
        cells = [v for a in aliases for r in rows(a) for k, v in r.items() if k.isdigit()]
        blanks = sum(v == "" for v in cells)
        zeros = sum(v != "" and float(v) == 0 for v in cells)
        assert blanks > 0 and zeros > 0, family
        assert frame["value"].null_count() == blanks, family
        assert (frame["value"] == 0.0).sum() == zeros, family


def test_all_blank_columns_stay_as_null_valued_rows(data: Path) -> None:
    """Detects ED7 2026's all-blank ``2023`` / ``2024`` / ``2037``-``2050`` columns (FACTS: 249
    blank each, the 6,972 long rows count them) or the gas summary's tail dropped or filled: every
    source row has a row for each of the 28 labels, all of the blank columns null, and the row
    count is source rows x labels."""
    _, written = _run(data, "ed7_26")
    n = len(rows("ed7_26"))
    assert written == n * 28
    frame = _silver(data, ED7)
    blank_years = [2023, 2024, *range(2037, 2051)]
    blank = frame.filter(pl.col("projection_year").is_in(blank_years))
    assert blank.height == n * len(blank_years)
    assert blank["value"].null_count() == blank.height
    _, written = _run(data, "ed3g_25")
    assert written == len(rows("ed3g_25")) * 28
    tail = _silver(data, ED3G).filter(pl.col("projection_year") >= 2036)
    assert tail["value"].null_count() == sum(r["2036"] == "" for r in rows("ed3g_25")) * 15


def test_blank_fuel_is_a_null_dimension_that_keeps_its_rows(data: Path) -> None:
    """Detects a row with a blank ``Fuel`` excluded or the blank kept as text (FACTS g5: 335 / 486
    blanks in the combined table, 402 / 78 in ED7): the rows are written, ``fuel`` is null and
    nothing else is; the gas summary's ``fuel`` (all ``gas``) is never null."""
    for alias in ("ed3c_23", "ed3c_24", "ed7_25", "ed7_26", "ed3g_25"):
        _run(data, alias)
    for family, aliases in ((ED3C, ("ed3c_23", "ed3c_24")), (ED7, ("ed7_25", "ed7_26"))):
        frame = _silver(data, family)
        n_blank_cells = sum(
            (r["Fuel"] == "") * len(label_columns(table(a)[0])) for a in aliases for r in rows(a)
        )
        assert n_blank_cells > 0, family
        assert frame["fuel"].null_count() == n_blank_cells
        assert "" not in frame["fuel"].drop_nulls().to_list()
        for name in ("technology", "units"):
            assert frame[name].null_count() == 0, (family, name)
    gas = _silver(data, ED3G)
    assert set(gas["fuel"].to_list()) == {"gas"}


def test_units_stay_in_the_units_column_verbatim(data: Path) -> None:
    """Detects a unit rewritten, converted or invented (RULINGS 707): the combined table's 2023
    peak row stays ``GWh`` while the 2024 one is ``GWh/day``, the non-physical ``number`` /
    ``uptake`` stay, ED7 keeps ``uptake`` / ``stock``, the gas summary keeps ``GWh`` /
    ``GWh/day``, no ``unit`` column exists and each silver unit set equals the fixtures'."""
    for alias in UNPIVOT_ALIASES:
        _run(data, alias)
    combined = _silver(data, ED3C)
    peak23 = combined.filter((pl.col("edition") == 2023) & (pl.col("time") == "Peak 1-in-20"))
    peak24 = combined.filter((pl.col("edition") == 2024) & (pl.col("data_type") == "Peak 1in20"))
    assert peak23.height > 0 and peak24.height > 0
    assert set(peak23["units"].to_list()) == {"GWh"}
    assert set(peak24["units"].to_list()) == {"GWh/day"}
    assert {"number", "uptake", "TWh"} <= set(combined["units"].to_list())
    ed7 = _silver(data, ED7)
    assert {"uptake", "stock"} <= set(ed7["units"].to_list())
    assert "stock" not in set(combined["units"].to_list())
    gas = _silver(data, ED3G)
    assert set(gas["units"].to_list()) == {"GWh", "GWh/day"}
    for family, aliases in (
        (ED3C, ("ed3c_23", "ed3c_24")),
        (ED3G, ("ed3g_25", "ed3g_26")),
        (ED7, ("ed7_25", "ed7_26")),
    ):
        assert set(_silver(data, family)["units"].to_list()) == {
            r["Units"] for a in aliases for r in rows(a)
        }
        assert not [c for c in _silver(data, family).columns if c == "unit"]


def test_descriptions_keep_their_trailing_space(data: Path) -> None:
    """Detects an identifier trimmed (no trim in this unit, ADR-042): the gas summary's
    ``Total (NTS+LDZ) gas shrinkage `` and ``Hydrogen Production: Natural Gas Demand `` keep their
    trailing space in 2025 and 2026."""
    for alias in ("ed3g_25", "ed3g_26"):
        _run(data, alias)
    descriptions = set(_silver(data, ED3G)["description"].to_list())
    assert "Hydrogen Production: Natural Gas Demand " in descriptions
    assert "Total (NTS+LDZ) gas shrinkage " in descriptions
    assert descriptions == {r["Description"] for a in ("ed3g_25", "ed3g_26") for r in rows(a)}


@pytest.mark.parametrize(
    ("alias", "label", "value"),
    [
        ("ed3c_23", "2030", "1,000"),
        ("ed3c_24", "2030", "N/A"),
        ("ed3g_25", "2040", "5,082.0"),
        ("ed3g_26", "2030", "0.03 "),
        ("ed7_25", "2030", "-"),
        ("ed7_26", "2030", "NaN%"),
    ],
)
def test_an_uncastable_cell_fails_the_capture(
    data: Path, alias: str, label: str, value: str
) -> None:
    """Detects a record that swallows a bad cell (a null-token list, a lenient cast, a thousands
    separator, padding): the lexical forms that break other bodies fail the whole capture before
    any row is written and leave no completion."""
    family = CAPTURES[alias].family
    raw = _replace_cell(body(alias), 2, table(alias)[0].index(label), value)
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize(
    ("alias", "column"),
    [
        ("ed3c_24", "Technology"),
        ("ed3c_23", "Units"),
        ("ed3g_25", "Description"),
        ("ed3g_26", "Units"),
        ("ed7_25", "Technology"),
        ("ed7_26", "Sector/Aggregation Level"),
    ],
)
def test_a_blank_identifier_is_excluded_and_counted(data: Path, alias: str, column: str) -> None:
    """Detects an identifier made nullable (a blank ``Technology`` / ``Units`` / ``Description`` /
    ``Sector/Aggregation Level`` becoming a null-keyed row): the source row's every long row is
    excluded, counted on the transformer and in the completion, and no null reaches the output."""
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
    ("alias", "cell"), [("ed3c_23", "2030"), ("ed3g_25", "2030"), ("ed7_26", "2030")]
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
    fields[header.index(cell)] = "123.456"
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
    alias = next(a for a in UNPIVOT_ALIASES if CAPTURES[a].family == family)
    capture_id = capture(data, alias, raw=_replace_cell(body(alias), 0, 0, "Renamed"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_a_header_with_swapped_label_columns_fails_loud(data: Path) -> None:
    """Detects an epoch matched by set rather than exactly: the 2024 combined header with
    ``Pathway`` renamed to ``Scenario`` (the 2023 label on 2024's other columns) is no epoch."""
    capture_id = capture(data, "ed3c_24", raw=_replace_cell(body("ed3c_24"), 0, 0, "Scenario"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, ED3C, data).run(DAY, run_id="r")
    assert read_completion(data, ED3C, capture_id) is None
    assert _no_silver(data, ED3C)


def test_a_reference_header_outside_the_epoch_fails_loud(data: Path) -> None:
    """Detects an ED4 body with a changed header read anyway: the single epoch is exact."""
    capture_id = capture(data, "ed4_23", raw=_replace_cell(body("ed4_23"), 0, 1, "Item"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, ED4, data).run(DAY, run_id="r")
    assert read_completion(data, ED4, capture_id) is None
    assert _no_silver(data, ED4)


# --------------------------------------------------------------------------- #
# ED4: item_no, newline, definitions
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bad", ["1.0", "one", "", "01x"])
def test_an_uncastable_or_blank_item_number_never_reaches_the_output(data: Path, bad: str) -> None:
    """Detects ``item_no`` read leniently (a float string, text, a blank) into the output: a
    value that is not a strict integer either fails the capture or, when blank, is excluded and
    counted, and no row with that item reaches the silver."""
    raw = _replace_cell(body("ed4_23"), 2, 0, bad)
    capture_id = capture(data, "ed4_23", raw=raw)
    transformer = get_transformer(SOURCE, ED4, data)
    if bad == "":
        written = transformer.run(DAY, run_id="r")
        assert written == 9 and transformer.last_excluded_row_count == 1
        assert _silver(data, ED4)["item_no"].null_count() == 0
    else:
        with pytest.raises(NesoCaptureFailedError):
            transformer.run(DAY, run_id="r")
        assert read_completion(data, ED4, capture_id) is None
        assert _no_silver(data, ED4)


def test_a_repeated_item_number_fails_the_capture(data: Path) -> None:
    """Detects a duplicate ``Item no.`` (the vendor's ``"Unique Key"``) silently dropped: a
    repeated item fails with ``DuplicateEntityKeyError``, no completion."""
    raw = body("ed4_23")
    first = raw.split(b"\r\n")[1]
    capture_id = capture(data, "ed4_23", raw=raw + first + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, ED4, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert read_completion(data, ED4, capture_id) is None
    assert _no_silver(data, ED4)


def test_ed4_is_keyed_by_item_number_per_edition(data: Path) -> None:
    """Detects a key that merges editions or a count off: item numbers 1-10 in 2023 and 2024 and
    1-11 in 2025 and 2026 appear as separate rows per edition, 42 in all, with no duplicate key."""
    for alias in ED4_ALIASES:
        _run(data, alias)
    frame = _silver(data, ED4)
    assert frame.height == 42
    assert frame.select("edition", "item_no").is_duplicated().sum() == 0
    for edition, top in ((2023, 10), (2024, 10), (2025, 11), (2026, 11)):
        part = frame.filter(pl.col("edition") == edition)
        assert sorted(part["item_no"].to_list()) == list(range(1, top + 1))
    assert (
        frame.group_by("item_no").agg(pl.col("edition").n_unique()).get_column("edition").max() or 0
    ) > 1


def test_ed4_newline_and_commas_are_kept_byte_equal(data: Path) -> None:
    """Detects an embedded newline flattened or a comma-bearing definition split (FACTS: one
    newline cell in 2025, comma cells in every edition): every description and definition equals
    its cell exactly and the 2025 peak definition still contains its newline."""
    for alias in ED4_ALIASES:
        _run(data, alias)
    frame = _silver(data, ED4)
    expected = {(r["Description"], r["Definition"]) for a in ED4_ALIASES for r in rows(a)}
    assert {(r["description"], r["definition"]) for r in frame.to_dicts()} == expected
    assert any("\n" in d for d in frame["definition"].to_list())
    assert any("," in d for d in frame["definition"].to_list())
    newline = frame.filter(pl.col("definition").str.contains("\n"))
    assert newline["edition"].to_list() == [2025]
    assert "Total GB Peak 1-in-20 Demand" in newline["description"].to_list()


def test_ed4_edition_specific_definitions_are_kept_as_published(data: Path) -> None:
    """Detects an edition's definition replaced by another's or normalised (FACTS l.321-322): the
    peak 1-in-20 definition states a gas year in every edition and its first example year differs
    (2022 in 2023 / 2024, 2023 in 2025 / 2026); only 2026's electricity-generation definition
    carries the network-constraint non-comparability warning."""
    for alias in ED4_ALIASES:
        _run(data, alias)
    frame = _silver(data, ED4)
    peak = frame.filter(pl.col("description") == "Total GB Peak 1-in-20 Demand")
    assert sorted(peak["edition"].to_list()) == [2023, 2024, 2025, 2026]
    for row in peak.to_dicts():
        assert "gas years" in row["definition"] or "gas years" in row["definition"].lower()
        assert "01/10/" in row["definition"] and "30/09/" in row["definition"]
    by_edition = {r["edition"]: r["definition"] for r in peak.to_dicts()}
    assert "2022 is GY 2022/23" in by_edition[2023] and "2022 is GY 2022/23" in by_edition[2024]
    assert "2023 is GY 2023/24" in by_edition[2025] and "2023 is GY 2023/24" in by_edition[2026]
    generation = frame.filter(
        pl.col("description").str.starts_with("Total gas demand (gas NTS connected only)")
    )
    flagged = generation.filter(pl.col("definition").str.contains("not directly comparable"))
    assert flagged["edition"].to_list() == [2026]
    assert "electricity network constraints" in flagged["definition"].to_list()[0]
    for edition in (2023, 2024, 2025):
        text = generation.filter(pl.col("edition") == edition)["definition"].to_list()
        assert text and "constraint" not in text[0]


# --------------------------------------------------------------------------- #
# Editions
# --------------------------------------------------------------------------- #


def test_editions_are_stamped_from_the_filename_and_all_are_served(data: Path) -> None:
    """Detects an edition inferred from the vintage or the year labels, or one edition displacing
    another: the ED7 2025 and 2026 bodies stamp their filename's year and ``_latest`` serves
    each."""
    ids = {a: _run(data, a)[0] for a in ("ed7_25", "ed7_26")}
    frame = _silver(data, ED7)
    by_capture = {
        cid: set(editions)
        for cid, editions in frame.group_by("bronze_capture_id")
        .agg(pl.col("edition").unique())
        .iter_rows()
    }
    assert by_capture == {ids["ed7_25"]: {2025}, ids["ed7_26"]: {2026}}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, ED7, None)) == set(ids.values())


@pytest.mark.parametrize(
    ("alias", "wrong"),
    [
        ("ed3c_24", "fes2026_ed3_v001.csv"),
        ("ed3c_23", "fes2023_ed3_v001.csv.bak"),
        ("ed3g_25", "fes2023_ed3_v001.csv"),
        ("ed3g_26", "fes_ed3.csv"),
        ("ed7_25", "fes2023_ed7_v001.csv"),
        ("ed7_26", "10yo2027_ed7_v001.csv"),
        ("ed4_24", "fes2022_ed4_v001.csv"),
        ("ed4_26", "10yo2026_ed4_v002.csv"),
    ],
)
def test_an_unmapped_filename_fails_loud(data: Path, alias: str, wrong: str) -> None:
    """Detects a fallback edition: a filename the map does not list (a new year, a suffixed copy,
    another family's name) fails the capture, leaves no completion and writes nothing."""
    family = CAPTURES[alias].family
    capture_id = capture(data, alias, filename=wrong)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_all_captures_complete_and_reconcile_with_no_gap(data: Path) -> None:
    """Detects a capture reaching no owner or a reconcile gap (there is no resource HOLD here):
    with every fixture captured each family completes every capture with no failure, the row
    counts equal source rows x labels, and reconcile reports no gap."""
    ids = {a: capture(data, a) for a in CAPTURES}
    for family in FAMILIES:
        aliases = [a for a in CAPTURES if CAPTURES[a].family == family]
        expected = sum(
            len(rows(a)) * len(label_columns(table(a)[0])) if family != ED4 else len(rows(a))
            for a in aliases
        )
        assert get_transformer(SOURCE, family, data).run(DAY, run_id="r") == expected, family
        for alias in aliases:
            assert read_completion(data, family, ids[alias]) is not None, alias
            assert read_failure(data, family, ids[alias]) is None, alias
    report = reconcile(data, load_registry(), list(FAMILIES), DAY)
    assert report.gaps == (), report.lines()


# --------------------------------------------------------------------------- #
# Vintage and generated artefacts
# --------------------------------------------------------------------------- #


def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(data: Path) -> None:
    """Detects an issue-time proxy or a projection-year clock (RULINGS 529/597), or a catalogue
    view that cannot carry the unpivot columns: ``available_at`` is the CKAN ``last_modified``
    (ED3 combined 2024: 2025-02-17, not the 2024 edition year), ``timestamp_utc`` stays the capture
    time, an as-of read before the vintage serves nothing and one after it serves the capture."""
    meta = CAPTURES["ed3c_24"]
    capture_id, _ = _run(data, "ed3c_24")
    frame = _silver(data, ED3C)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, ED3C, datetime(2025, 2, 16, tzinfo=UTC)) == []
    assert set(both_as_of(db, data, ED3C, datetime(2025, 2, 18, tzinfo=UTC))) == {capture_id}
    assert set(both_as_of(db, data, ED3C, None)) == {capture_id}


@pytest.mark.parametrize(
    ("slug", "title", "needles"),
    [
        (
            ED3_PACKAGE[0],
            "Natural Gas and Residential Heat Demand Summary (ED3)",
            (
                "`projection_year`",
                "`sector_aggregation_level`",
                "`data_type`",
                "`component`",
                ED3C,
                ED3G,
                ED7,
            ),
        ),
        (
            ED4_PACKAGE[0],
            "Natural Gas demand definitions (ED4)",
            ("`item_no`", "`definition`", ED4),
        ),
    ],
)
def test_the_skeleton_page_renders_the_new_records(
    slug: str, title: str, needles: tuple[str, ...]
) -> None:
    """Detects a record the docs generator cannot render (the unpivot columns, the held
    questions, the three families of the ED3 / ED7 package)."""
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
    for needle in needles:
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
