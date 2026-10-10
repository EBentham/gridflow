"""The regional breakdown of FES (electricity) frozen records (v0.22-K-SCN-1b).

Nine families of the ``regional-breakdown-of-fes-data-electricity`` package get record version 1.
Every test writes recorded fixture captures into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here, not
at activation. On master none of the nine families has a record, so ``get_transformer`` raises for
each of them.

Fixtures (``tests/fixtures/neso_data_portal/scn1b/``, provenance in ``PROVENANCE.md``): each file
is a cut of the 2026-10-08 swept bronze (a scratch script, not committed): the first one or two
locations, the years 20 / 21 / 22 / 23 / 35 / 50 (a year label of the capture's own first
projection year, the middle and the last), every scenario and technology of those, plus the
vendor oddities named in the tests. ``git`` normalises a committed CSV fixture's line endings, so
:func:`body` rebuilds the bronze originals' CRLF convention; a BOM stays where the original had one.

Record decisions under test (K-SCN-1 FACTS SCN-1b, RULINGS 607):

- The vendor ``year`` column is a two-digit label (``20``..``50``). The engine has no
  ordinary-column label mapping and adding a century would be an invented conversion, so every
  record types it ``year_label`` int64 (min 20, max 50) and never ``projection_year``. Vendor
  meaning, which the record model has no free-text eligible field to carry: demand and DSR, the
  financial year starting in April (``25`` = 2025/26); distributed generation above 1 MW and both
  storage families, the winter starting in the labelled year.
- The several measures stay ordinary nullable float64 columns (no unpivot): a blank cell is null,
  never zero, and a zero is a value.
- Identifiers (``B_EXTRA_1``, ``TONG1``, ``TOWH``, GSP ids) are stored as written; no join to the
  GSP lookup is enforced.
- Eligible: demand, DSR, storage above 1 MW (current and the 2022 body), storage below 1 MW
  (pre-2023). Held (E-SEM): distributed generation above and below 1 MW, the GSP lookup and
  storage below 1 MW. The 2021 above-1 MW storage resource carries a resource-level HOLD; the 2022
  GSP lookup capture fails ``UnicodeDecodeError`` and is adjudicated (ADR-040).
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
from _neso_generic_support import install_generated, write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings, run_cli

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import (
    RECONCILE_ADJUDICATIONS_FILE,
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "scn1b"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
PACKAGE = "regional-breakdown-of-fes-data-electricity"
PACKAGE_ID = "963525d6-5d83-4448-a99c-663f1c76330a"

DEMAND = "fes_regional_demand_active_power"
DG_GT = "fes_regional_dg_gt_1mw"
DG_LT = "fes_regional_dg_lt_1mw"
DSR = "fes_regional_dsr"
GSP = "fes_regional_gsp_info"
STG = "fes_regional_storage_gt_1mw"
STG_PRE = "fes_regional_storage_gt_1mw_pre2023"
STL = "fes_regional_storage_lt_1mw"
STL_PRE = "fes_regional_storage_lt_1mw_pre2023"
FAMILIES = (DEMAND, DG_GT, DG_LT, DSR, GSP, STG, STG_PRE, STL, STL_PRE)
HELD = {DG_GT, DG_LT, GSP, STL}
HOLD_RESOURCE = "b954c63f-c108-4e71-9b43-b249d0d92a1b"
GSP_2022_CAPTURE = (
    "bronze/neso_data_portal/fes_regional_gsp_info/2026/10/08/"
    "raw_20261008T105727Z_000d08b9-12d9-4396-95f8-6b3677664836_6a93974d.csv"
)


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture and its sidecar provenance."""

    family: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str

    @property
    def edition(self) -> int:
        """The edition the filename map must stamp."""
        match = re.match(r"fes(\d{4})_", self.filename)
        assert match is not None, self.filename
        return int(match.group(1))


CAPTURES: dict[str, Capture] = {
    "dem21": Capture(
        "fes_regional_demand_active_power",
        "3360c832-acc3-4656-8a38-f0bdf57bde88",
        "Regional Breakdown of 2021 FES: Demand (Active Power)",
        "fes2021_regional_breakdown_active_power.csv",
        "2021-07-06T11:06:43.990644",
        "2026-10-08T10:56:39.787331+00:00",
    ),
    "dem24": Capture(
        "fes_regional_demand_active_power",
        "7bd643c0-6aad-41ac-aa59-92b68992d9a4",
        "Regional Breakdown of 2024 FES: Demand (Active Power)",
        "fes2024_regional_breakdown_active_power.csv",
        "2024-07-15T06:43:07.255666",
        "2026-10-08T10:56:48.413285+00:00",
    ),
    "dgg21": Capture(
        "fes_regional_dg_gt_1mw",
        "fc6e9d6e-0995-447a-8819-2099c845ad7e",
        "Regional Breakdown of 2021 FES: Distributed generation greater than 1 MW",
        "fes2021_regional_breakdown_distributed_generation.csv",
        "2021-07-06T10:59:37.680231",
        "2026-10-08T10:56:52.270524+00:00",
    ),
    "dgg22": Capture(
        "fes_regional_dg_gt_1mw",
        "162bdb05-1d05-46f8-b3cd-55a86ef65b8e",
        "Regional Breakdown of 2022 FES: Distributed generation greater than 1 MW",
        "fes2022_regional_breakdown_distributed_generation.csv",
        "2022-07-15T13:35:02.326000",
        "2026-10-08T10:56:54.720227+00:00",
    ),
    "dgl21": Capture(
        "fes_regional_dg_lt_1mw",
        "e05c34ec-1a0b-496c-b098-e35d4f998dc9",
        "Regional Breakdown of 2021 FES: Distributed generation less than 1 MW",
        "fes2021_regional_breakdown_sub1mw_generation.csv",
        "2021-07-06T10:54:05.003828",
        "2026-10-08T10:57:04.504161+00:00",
    ),
    "dgl22": Capture(
        "fes_regional_dg_lt_1mw",
        "8cd8a80a-9c36-436c-9014-8666c77e95d7",
        "Regional Breakdown of 2022 FES: Distributed generation less than 1 MW",
        "fes2022_regional_breakdown_sub1mw_generation.csv",
        "2022-07-15T13:35:59.358715",
        "2026-10-08T10:57:07.008367+00:00",
    ),
    "dsr21": Capture(
        "fes_regional_dsr",
        "180d9908-7c4a-4db7-b6f1-92067209854c",
        "Regional Breakdown of 2021 FES: Demand Side Response (DSR)",
        "fes2021_regional_breakdown_demand_side_response.csv",
        "2021-07-06T11:01:06.905207",
        "2026-10-08T10:57:15.972865+00:00",
    ),
    "gsp21": Capture(
        "fes_regional_gsp_info",
        "41fb4ca1-7b59-4fce-b480-b46682f346c9",
        "FES 2021 Grid Supply Point Info",
        "fes2021_regional_breakdown_gsp_info.csv",
        "2021-07-06T10:53:01.441148",
        "2026-10-08T10:57:24.776324+00:00",
    ),
    "gsp22": Capture(
        "fes_regional_gsp_info",
        "000d08b9-12d9-4396-95f8-6b3677664836",
        "FES 2022 Grid Supply Point Info",
        "fes2022_regional_breakdown_gsp_info.csv",
        "2022-07-15T13:31:25.009443",
        "2026-10-08T10:57:27.213496+00:00",
    ),
    "stg23": Capture(
        "fes_regional_storage_gt_1mw",
        "0465e5a3-ff3a-4e95-ac66-f59afd54cfe3",
        "Regional Breakdown of 2023 FES: Demand from distributed storage sites greater than 1 MW",
        "fes2023_regional_breakdown_dxstorage_gt1mw.csv",
        "2023-07-07T13:36:43.988472",
        "2026-10-08T10:57:36.424151+00:00",
    ),
    "stg24": Capture(
        "fes_regional_storage_gt_1mw",
        "eba5fd0a-ea22-4c00-84c2-35260e328736",
        "Regional Breakdown of 2024 FES: Demand from distributed storage sites greater than 1 MW",
        "fes2024_regional_breakdown_dxstorage_gt1mw.csv",
        "2024-07-15T06:45:04.345465",
        "2026-10-08T10:57:39.027979+00:00",
    ),
    "stgpre21": Capture(
        "fes_regional_storage_gt_1mw_pre2023",
        "b954c63f-c108-4e71-9b43-b249d0d92a1b",
        "Regional Breakdown of 2021 FES: Demands from distributed storage sites greater than 1 MW",
        "fes2021_regional_breakdown_dxstorage_gt1mw.csv",
        "2021-07-06T10:50:48.532360",
        "2026-10-08T10:57:42.611783+00:00",
    ),
    "stgpre22": Capture(
        "fes_regional_storage_gt_1mw_pre2023",
        "ae5e3faf-b264-478e-82a5-fe0eb3101bba",
        "Regional Breakdown of 2022 FES: Demands from distributed storage sites greater than 1 MW",
        "fes2022_regional_breakdown_dxstorage_gt1mw.csv",
        "2022-07-15T13:33:26.428024",
        "2026-10-08T10:57:45.166372+00:00",
    ),
    "stl23": Capture(
        "fes_regional_storage_lt_1mw",
        "86f90e3f-2a48-4bfc-80a9-9ff65c353d6e",
        "Regional Breakdown of 2023 FES: Demand from distributed storage sites less than 1 MW",
        "fes2023_regional_breakdown_dxstorage_sub1mw.csv",
        "2023-07-07T13:37:11.264211",
        "2026-10-08T10:57:49.488080+00:00",
    ),
    "stl24": Capture(
        "fes_regional_storage_lt_1mw",
        "0a1fce9e-8711-4017-bdef-867c0ab040a1",
        "Regional Breakdown of 2024 FES: Demand from distributed storage sites less than 1 MW",
        "fes2024_regional_breakdown_dxstorage_sub1mw.csv",
        "2024-07-15T06:45:51.492269",
        "2026-10-08T10:57:52.168193+00:00",
    ),
    "stlpre21": Capture(
        "fes_regional_storage_lt_1mw_pre2023",
        "f0db3dc8-4e8f-40e0-9def-58e0bca11e47",
        "Regional Breakdown of 2021 FES: Demands from distributed storage sites less than 1 MW",
        "fes2021_regional_breakdown_dxstorage_sub1mw.csv",
        "2021-07-06T10:48:22.393456",
        "2026-10-08T10:57:55.801307+00:00",
    ),
    "stlpre22": Capture(
        "fes_regional_storage_lt_1mw_pre2023",
        "08f1f7f3-0448-4e80-aeb6-0ef082d86e8f",
        "Regional Breakdown of 2022 FES: Demands from distributed storage sites less than 1 MW",
        "fes2022_regional_breakdown_dxstorage_sub1mw.csv",
        "2022-07-15T13:36:47.954918",
        "2026-10-08T10:57:58.726143+00:00",
    ),
}
NOT_RUN = {"stgpre21", "gsp22"}
"""The 2021 above-1 MW storage resource is HOLD and the 2022 GSP lookup fails; neither types."""
RUNNABLE = tuple(alias for alias in CAPTURES if alias not in NOT_RUN)
CAPTURE_OF_FAMILY: dict[str, tuple[str, ...]] = {
    family: tuple(a for a in CAPTURES if CAPTURES[a].family == family) for family in FAMILIES
}

HDR_MEASURES = ("capacity", "wintpk", "summam", "summpm")
HDR_DG = ("scenario", "tech", "year", "etys_location", *HDR_MEASURES)
HDR_STG = ("scenario", "tech", "year", "location", "Capacity", "wintpk", "summam", "summpm")
HDR_STL0 = ("scenario", "year", "etys_location", *HDR_MEASURES)
HDR_STL1 = ("scenario", "tech", "year", "etys_location", *HDR_MEASURES)
UNNAMED = ("", "_duplicated_0", "_duplicated_1", "_duplicated_2")


@dataclass(frozen=True)
class Expect:
    """What the unit spec says one family's record is."""

    headers: tuple[tuple[str, ...], ...]
    key: tuple[str, ...]
    editions: tuple[tuple[str, int], ...]


def _pairs(stem: str, editions: tuple[int, ...]) -> tuple[tuple[str, int], ...]:
    return tuple((f"fes{e}_regional_breakdown_{stem}.csv", e) for e in editions)


EXPECT: dict[str, Expect] = {
    DEMAND: Expect(
        (("scenario", "GSP", "DemandPk", "DemandAM", "DemandPM", "type", "year"),),
        ("resource_id", "edition", "scenario", "gsp", "type", "year_label"),
        _pairs("active_power", (2021, 2022, 2023, 2024)),
    ),
    DG_GT: Expect(
        (HDR_DG, (*HDR_DG, *UNNAMED)),
        ("resource_id", "edition", "scenario", "tech", "etys_location", "year_label"),
        _pairs("distributed_generation", (2021, 2022, 2023, 2024)),
    ),
    DG_LT: Expect(
        (HDR_DG,),
        ("resource_id", "edition", "scenario", "tech", "etys_location", "year_label"),
        _pairs("sub1mw_generation", (2021, 2022, 2023, 2024)),
    ),
    DSR: Expect(
        (("scenario", "GSP", "DSR", "year"),),
        ("resource_id", "edition", "scenario", "gsp", "year_label"),
        _pairs("demand_side_response", (2021, 2022, 2023)),
    ),
    GSP: Expect(
        (("GSP ID", "GSP Group", "Minor FLOP", "Name", "Latitude", "Longitude", "Comments"),),
        ("resource_id", "edition", "gsp_id"),
        _pairs("gsp_info", (2021, 2022, 2023, 2024)),
    ),
    STG: Expect(
        (HDR_STG,),
        ("resource_id", "edition", "scenario", "tech", "location", "year_label"),
        _pairs("dxstorage_gt1mw", (2023, 2024)),
    ),
    STG_PRE: Expect(
        (HDR_STG,),
        ("resource_id", "edition", "scenario", "tech", "location", "year_label"),
        _pairs("dxstorage_gt1mw", (2022,)),
    ),
    STL: Expect(
        (HDR_STL0, HDR_STL1),
        ("resource_id", "edition", "scenario", "tech", "etys_location", "year_label"),
        _pairs("dxstorage_sub1mw", (2023, 2024)),
    ),
    STL_PRE: Expect(
        (HDR_STL0,),
        ("resource_id", "edition", "scenario", "etys_location", "year_label"),
        _pairs("dxstorage_sub1mw", (2021, 2022)),
    ),
}
HELD_PHRASES = {
    DG_GT: "vendor correction confirming the capacity population/threshold",
    DG_LT: "calendar/financial/winter period meaning for this family",
    GSP: "coordinate unit/CRS",
    STL: "matching-edition vendor definitions resolving population, capacity meaning and sign",
}
UNPARSED_NAMES = {"": "unnamed_9", "_duplicated_0": "unnamed_10", "_duplicated_1": "unnamed_11"}


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
        prefix="sb", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's CRLF line endings."""
    raw = (FIXTURES / f"{alias}.csv").read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n")


def table(alias: str, raw: bytes | None = None) -> tuple[list[str], list[list[str]]]:
    """The fixture's header and records as text (the table starts at the ``scenario`` header
    of a preamble body; the 2022 GSP lookup is Windows-1252)."""
    data_bytes = raw if raw is not None else body(alias)
    text = data_bytes.decode("cp1252" if alias == "gsp22" else "utf-8-sig")
    lines = list(csv.reader(io.StringIO(text, newline="")))
    start = next(i for i, line in enumerate(lines) if line and line[0] in ("scenario", "GSP ID"))
    return lines[start], [line for line in lines[start + 1 :] if any(line)]


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, keyed by position-qualified header (blank names unique)."""
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


def _run(data: Path, alias: str, **kwargs: Any) -> tuple[str, int]:
    """Capture ``alias``, run its family's transformer; the capture id and rows written."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias, **kwargs)
    written = get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    return capture_id, written


def _replace_cell(raw: bytes, row: int, column: int, value: str) -> bytes:
    lines = raw.split(b"\r\n")
    fields = next(csv.reader([lines[row].decode("utf-8")]))
    fields[column] = value
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    lines[row] = out.getvalue().encode("utf-8")
    return b"\r\n".join(lines)


def parsed_header(header: list[str]) -> list[str]:
    """The header as the reader names it: blank names become ``""``, ``_duplicated_0``, ..."""
    blanks = iter(UNNAMED)
    return [name if name else next(blanks) for name in header]


def _matching_epoch(record: SchemaRecord, header: list[str]) -> Any:
    wanted = parsed_header(header)
    return next(e for e in record.epochs if list(e.header) == wanted)


# --------------------------------------------------------------------------- #
# Fixtures and record shapes
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the exact vendor headers
    (the 2022 distributed-generation file with its four unnamed trailing columns, the 2024 sub-1
    MW storage with ``tech`` and the 2023 one without), the CRLF convention and each original's
    BOM, a blank ``DemandPk`` (2021), the unmatched codes ``B_EXTRA_1`` / ``TOWH`` / ``TONG1``,
    the ``add GSPs`` note, negative storage values, an all-zero storage column, the ``#N/A``
    coordinates, the 2022 GSP lookup's invalid UTF-8 byte, and the 2021 storage preamble with its
    three repeated rows."""
    for alias in CAPTURES:
        assert body(alias).count(b"\r\n") == body(alias).count(b"\n"), alias
    bom = b"\xef\xbb\xbf"
    for with_bom in ("dem21", "dem24", "gsp21"):
        assert body(with_bom).startswith(bom), with_bom
    for without in ("dgg22", "stg23", "gsp22"):
        assert not body(without).startswith(bom), without
    for alias, family in (("stl24", STL), ("stl23", STL), ("gsp21", GSP), ("dgg21", DG_GT)):
        assert tuple(table(alias)[0]) in EXPECT[family].headers, alias
    assert tuple(table("dgg22")[0]) == (*HDR_DG, "", "", "", "")
    assert [r["_blank11"] for r in rows("dgg22") if r["_blank11"]] == ["add GSPs"]
    assert {r["_blank8"] for r in rows("dgg22")} == {""}
    assert any(r["DemandPk"] == "" for r in rows("dem21"))
    assert all(r["DemandAM"] != "" and r["DemandPM"] != "" for r in rows("dem21"))
    assert any(r["GSP"] == "B_EXTRA_1" for r in rows("dem21"))
    assert any(r["etys_location"] == "TOWH" for r in rows("dgl21"))
    assert any(r["etys_location"] == "TONG1" for r in rows("dgl22"))
    assert any(r["capacity"] == "" for r in rows("dgl21"))
    for alias in ("stg24", "stl23", "stl24", "stlpre21", "stlpre22"):
        assert any(float(r["summpm"]) < 0 for r in rows(alias)), alias
    assert {r["wintpk"] for r in rows("stg23")} == {"0"}
    assert all(r["year"] in {"20", "21", "22", "23", "35", "50"} for r in rows("dem21"))
    assert any(r["Latitude"] == "#N/A" for r in rows("gsp21"))
    assert any(r["Latitude"] == "#N/A" for r in rows("gsp22"))
    assert body("gsp22").count(b"\x92") == 1
    with pytest.raises(UnicodeDecodeError):
        body("gsp22").decode("utf-8")
    body("gsp21").decode("utf-8")
    preamble = body("stgpre21").split(b"\r\n")
    assert preamble[0].lstrip(b"\xef\xbb\xbf").startswith(b"Created by CalcDxStorage.sas")
    assert set(preamble[1]) <= {ord(","), ord("\r")}
    assert preamble[2].startswith(b"scenario,tech,year,location,Capacity")
    repeated = [r for r in rows("stgpre21") if r["location"] == "NORT_1"]
    assert len(repeated) == 3 and all(r == repeated[0] for r in repeated)
    assert (repeated[0]["scenario"], repeated[0]["tech"], repeated[0]["year"]) == (
        "CF",
        "CAES",
        "25",
    )


@pytest.mark.parametrize("family", FAMILIES)
def test_record_shape_matches_the_unit_spec(family: str) -> None:
    """Detects a record that drifts from the spec: one csv/utf-8 record of the exact vendor
    header epochs, the vendor ``year`` typed ``year_label`` int64 (min 20, max 50, non-nullable)
    and never ``projection_year``, no unpivot, no issue time, temporal ``none``, whole-capture
    selection per ``resource_id``, ``ckan_last_modified`` vintage, the exact
    ``edition_by_filename`` pairs and the per-family key of ``resource_id`` + ``edition`` + the
    spec's identifiers + ``year_label``."""
    expect = EXPECT[family]
    record = _record(family)
    assert (record.version, record.reader, record.encoding) == ("1", "csv", "utf-8")
    assert [tuple(e.header) for e in record.epochs] == list(expect.headers)
    for epoch in record.epochs:
        assert epoch.unpivot is None
        assert epoch.issue.kind == "none"
        names = [c.name for c in epoch.columns]
        assert len(set(names)) == len(names)
        assert "projection_year" not in names
        years = [c for c in epoch.columns if c.source == "year"]
        if family == GSP:
            assert years == []
            continue
        (year,) = years
        assert (year.name, year.dtype, year.nullable, year.min, year.max) == (
            "year_label",
            "int64",
            False,
            20,
            50,
        )
    assert record.temporal.kind == "none"
    assert record.latest == "whole_capture"
    assert record.latest_partition == "resource_id"
    assert record.vintage == "ckan_last_modified"
    assert record.edition_by_filename == expect.editions
    assert record.xlsx is None and record.siblings == ()
    assert record.entity_key == expect.key


def test_measures_are_nullable_floats_and_identifiers_are_strings() -> None:
    """Detects a measure typed so a blank cell drops the row or becomes zero, an identifier cast
    to a number, or an identifier column allowed to be null: every measure is a nullable
    float64 with no null token and no bound, every identifier a string, the key identifiers
    non-nullable, and the GSP lookup's coordinates raw nullable strings."""
    measures = {
        "demand_pk",
        "demand_am",
        "demand_pm",
        "dsr",
        "capacity",
        "wintpk",
        "summam",
        "summpm",
    }
    for family in FAMILIES:
        record = _record(family)
        for epoch in record.epochs:
            for column in epoch.columns:
                assert column.null_tokens == (), (family, column.name)
                if column.name in measures:
                    assert (column.dtype, column.nullable) == ("float64", True), column.name
                    assert (column.min, column.max) == (None, None)
                elif column.name != "year_label":
                    assert column.dtype == "string", (family, column.name)
                if column.name in {
                    "scenario",
                    "gsp",
                    "type",
                    "location",
                    "etys_location",
                    "gsp_id",
                }:
                    assert column.nullable is False, (family, column.name)
    gsp = {c.name: c for c in _record(GSP).epochs[0].columns}
    assert gsp["latitude"].dtype == gsp["longitude"].dtype == "string"
    assert gsp["latitude"].nullable and gsp["longitude"].nullable
    names = [c.name for c in _record(DG_GT).epochs[1].columns]
    assert names[-4:] == ["unnamed_9", "unnamed_10", "unnamed_11", "note"]
    assert [c.source for c in _record(DG_GT).epochs[1].columns][-4:] == list(UNNAMED)
    assert all(c.dtype == "string" and c.nullable for c in _record(DG_GT).epochs[1].columns[-4:])
    tech = {c.name: c for c in _record(STL).epochs[1].columns}["tech"]
    assert tech.nullable is True
    assert "tech" not in [c.name for c in _record(STL).epochs[0].columns]


def test_held_and_eligible_families_are_exactly_as_ruled() -> None:
    """Detects a hold lost or reworded, a held question that is not a ``TODO:``, an eligible
    family held for another family's question, or a package-level hold swallowing the
    others: distributed generation (both), the GSP lookup and storage below 1 MW (current) are
    held E-SEM with their FACTS question; demand, DSR, both above-1 MW storage families and
    storage below 1 MW (pre-2023) publish."""
    registry = load_registry()
    for family in FAMILIES:
        package, entry = registry.families[family]
        effective = effective_eligibility(package, entry)
        if family in HELD:
            assert effective.status == "held", family
            assert effective.unit == "E-SEM", family  # type: ignore[union-attr]
            question = effective.question  # type: ignore[union-attr]
            assert question.startswith("TODO"), family
            assert HELD_PHRASES[family] in question, family
        else:
            assert effective.status == "eligible", family
            assert _record(family).eligibility is None
    assert len(HELD) == 4 and len(FAMILIES) - len(HELD) == 5


def test_each_family_is_in_the_package_file_the_spec_names() -> None:
    """Detects a record put in another package file or a family left without one: all nine
    records live in the regional-breakdown package file, and no other package file changes
    which families have a record."""
    document = json.loads((REGISTRY_DIR / f"{PACKAGE}.json").read_text(encoding="utf-8"))
    recorded = {f["key"] for f in document["families"] if f.get("record")}
    assert recorded == set(FAMILIES)


# --------------------------------------------------------------------------- #
# Every family through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", RUNNABLE)
def test_fixture_types_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the 2022
    unnamed columns, the BOM bodies), a cast the vendor body does not satisfy, any row excluded
    and a repeated entity key: the capture completes with every populated row, zero
    exclusions, an int64 ``year_label`` (never ``projection_year``), the edition stamped from
    the filename, the resource id stamped and a unique key."""
    meta = CAPTURES[alias]
    capture_id, written = _run(data, alias)
    assert written == len(rows(alias))
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
    assert "projection_year" not in frame.columns
    if meta.family != GSP:
        assert frame.schema["year_label"] == pl.Int64
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame.select(list(EXPECT[meta.family].key)).is_duplicated().sum() == 0


@pytest.mark.parametrize("alias", RUNNABLE)
def test_identifiers_measures_zeros_and_blanks_survive_exactly(data: Path, alias: str) -> None:
    """Detects a normalised identifier (case-folded, trimmed, joined away), a measure re-rounded,
    a zero turned into null, a blank turned into zero or a vendor two-digit label rewritten:
    every string column equals its cell, every measure equals its parsed cell (a blank is
    null), ``year_label`` equals the label as written, in order."""
    meta = CAPTURES[alias]
    _run(data, alias)
    frame = _silver(data, meta.family)
    header, records = table(alias)
    record = _record(meta.family)
    epoch = _matching_epoch(record, header)
    assert frame.height == len(records)
    for column in epoch.columns:
        cells = [r[parsed_header(header).index(column.source)] for r in records]
        got = frame[column.name].to_list()
        if column.dtype == "string":
            assert got == [c if c != "" or not column.nullable else None for c in cells], (
                column.name
            )
        elif column.dtype == "int64":
            assert got == [int(c) for c in cells], column.name
        else:
            assert got == [float(c) if c != "" else None for c in cells], column.name
            assert got.count(0.0) == [c for c in cells if c != ""].count("0") + sum(
                1 for c in cells if c not in ("", "0") and float(c) == 0.0
            )


def test_a_blank_measure_is_null_never_zero(data: Path) -> None:
    """Detects a blank cell read as zero or dropping its row: the 2021 demand blank ``DemandPk``
    cells (and the 2021 sub-1 MW generation blanks in all four measures) are null, their rows
    kept with the other measures intact, and the genuine zeros stay zero."""
    _run(data, "dem21")
    demand = _silver(data, DEMAND)
    blank = sum(r["DemandPk"] == "" for r in rows("dem21"))
    assert blank > 0 and demand["demand_pk"].null_count() == blank
    assert demand["demand_am"].null_count() == demand["demand_pm"].null_count() == 0
    assert (demand["demand_am"] == 0.0).sum() > 0
    _run(data, "dgl21")
    dg = _silver(data, DG_LT)
    blanks = sum(r["capacity"] == "" for r in rows("dgl21"))
    assert blanks > 0
    for measure in HDR_MEASURES:
        assert dg[measure].null_count() == blanks, measure


def test_negative_and_all_zero_storage_values_are_kept(data: Path) -> None:
    """Detects a negative filling rate clamped or dropped, or an all-zero column treated as
    absent: the above-1 MW 2023 ``wintpk`` is all zero and all kept, and the 2024 / pre-2023
    ``summpm`` negatives are kept with their sign."""
    _run(data, "stg23")
    _run(data, "stg24")
    _run(data, "stlpre21")
    stg = _silver(data, STG)
    zero = stg.filter(pl.col("edition") == 2023)
    assert zero.height > 0 and zero["wintpk"].null_count() == 0 and (zero["wintpk"] == 0.0).all()
    assert stg.filter(pl.col("edition") == 2024)["summpm"].min() < 0  # type: ignore[operator]
    negatives = [float(r["summpm"]) for r in rows("stlpre21") if float(r["summpm"]) < 0]
    assert sorted(
        _silver(data, STL_PRE).filter(pl.col("summpm") < 0)["summpm"].to_list()
    ) == sorted(negatives)


def test_unmatched_location_codes_are_kept_as_written(data: Path) -> None:
    """Detects an inner join or normalisation against the GSP lookup (FACTS: demand keeps
    ``B_EXTRA_1``, sub-1 MW generation keeps ``TOWH`` / ``TONG1``, none of which is in the same
    edition's lookup): the codes survive byte-for-byte although the fixture lookup does not
    list them."""
    _run(data, "dem21")
    _run(data, "dgl21")
    _run(data, "dgl22")
    _run(data, "gsp21")
    known = set(_silver(data, GSP)["gsp_id"].to_list())
    demand_codes = set(_silver(data, DEMAND)["gsp"].to_list())
    towh = _silver(data, DG_LT)
    assert "B_EXTRA_1" in demand_codes and "B_EXTRA_1" not in known
    assert {"TOWH", "TONG1"} <= set(towh["etys_location"].to_list())
    assert not {"TOWH", "TONG1"} & known


def test_the_vendor_year_label_is_kept_and_a_full_year_is_not_accepted(data: Path) -> None:
    """Detects an invented century conversion or a loosened bound (RULINGS 607): ``25`` is stored
    as 25, a four-digit label (what a converted ``projection_year`` would hold) is excluded and
    counted rather than accepted, and so is a label below 20."""
    meta = CAPTURES["dem24"]
    raw = _replace_cell(body("dem24"), 2, table("dem24")[0].index("year"), "2035")
    raw = _replace_cell(raw, 3, table("dem24")[0].index("year"), "19")
    _capture_id, written = _run(data, "dem24", raw=raw)
    transformer = get_transformer(SOURCE, meta.family, data)
    assert written == len(rows("dem24")) - 2
    frame = _silver(data, DEMAND)
    assert set(frame["year_label"].to_list()) <= set(range(20, 51))
    assert {23, 35, 50} == set(frame["year_label"].to_list())
    del transformer


@pytest.mark.parametrize(
    ("alias", "header", "value"),
    [
        ("dem24", "year", "2x"),
        ("dem24", "DemandAM", "n/a"),
        ("dsr21", "DSR", "-"),
        ("stg24", "summpm", "1,5"),
    ],
)
def test_an_uncastable_cell_fails_the_capture(
    data: Path, alias: str, header: str, value: str
) -> None:
    """Detects a record that swallows a bad cell (a null-token list, a lenient year cast, a
    decimal comma): a non-numeric year or measure fails the whole capture before any row is
    written and leaves no completion."""
    meta = CAPTURES[alias]
    raw = _replace_cell(body(alias), 3, table(alias)[0].index(header), value)
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert read_completion(data, meta.family, capture_id) is None
    assert not list((data / "silver" / SOURCE / meta.family).rglob("[!.]*.parquet"))


def test_a_blank_identifier_is_excluded_and_counted(data: Path) -> None:
    """Detects an identifier made nullable (a blank scenario becoming a null-keyed row): the
    row is excluded, counted on the transformer and in the completion, and no null reaches
    the output."""
    raw = _replace_cell(body("dsr21"), 3, 0, "")
    capture_id = capture(data, "dsr21", raw=raw)
    transformer = get_transformer(SOURCE, DSR, data)
    written = transformer.run(DAY, run_id="r")
    assert written == len(rows("dsr21")) - 1
    assert transformer.last_excluded_row_count == 1
    completion = read_completion(data, DSR, capture_id)
    assert completion is not None and completion["rows_excluded"] == 1
    assert _silver(data, DSR)["scenario"].null_count() == 0


@pytest.mark.parametrize("alias", ["dem24", "stl24"])
def test_a_repeated_entity_key_fails_the_capture(data: Path, alias: str) -> None:
    """Detects a key too coarse to guard the body: a second row for the same key with another
    measure fails with ``DuplicateEntityKeyError`` and leaves no completion."""
    meta = CAPTURES[alias]
    raw = body(alias)
    measure = {"dem24": "DemandPk", "stl24": "capacity"}[alias]
    first = raw.split(b"\r\n")[1]
    repeated = _replace_cell(first + b"\r\n", 0, table(alias)[0].index(measure), "123456.789")
    capture_id = capture(data, alias, raw=raw + repeated)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert read_completion(data, meta.family, capture_id) is None


# --------------------------------------------------------------------------- #
# Editions
# --------------------------------------------------------------------------- #


def test_editions_are_stamped_from_the_filename_and_both_are_served(data: Path) -> None:
    """Detects an edition inferred from the vintage or the year labels, or one edition
    displacing another: the 2021 and 2024 demand resources stamp 2021 and 2024 from their
    filenames (their labels both span 2020-2050), and ``_latest`` serves each."""
    first, _ = _run(data, "dem21")
    second, _ = _run(data, "dem24")
    frame = _silver(data, DEMAND)
    by_capture = {
        cid: set(editions)
        for cid, editions in frame.group_by("bronze_capture_id")
        .agg(pl.col("edition").unique())
        .iter_rows()
    }
    assert by_capture == {first: {2021}, second: {2024}}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, DEMAND, None)) == {first, second}


@pytest.mark.parametrize(
    ("alias", "wrong"),
    [
        ("dem24", "fes2025_regional_breakdown_active_power.csv"),
        ("stgpre22", "fes2021_regional_breakdown_dxstorage_gt1mw.csv"),
        ("dsr21", "fes2024_regional_breakdown_demand_side_response.csv"),
    ],
)
def test_an_unmapped_filename_fails_loud(data: Path, alias: str, wrong: str) -> None:
    """Detects a fallback edition: a filename the family's map does not list (a new year, the
    held 2021 storage name on the 2022 family, a year past a family's last edition) fails the
    capture, leaves no completion and writes nothing."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias, filename=wrong)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert read_completion(data, meta.family, capture_id) is None
    assert not list((data / "silver" / SOURCE / meta.family).rglob("[!.]*.parquet"))


# --------------------------------------------------------------------------- #
# Header epochs
# --------------------------------------------------------------------------- #


def test_the_2022_unnamed_trailing_columns_are_kept_with_the_note(data: Path) -> None:
    """Detects the four unnamed trailing columns dropped, mis-named or the ``add GSPs`` note lost
    (FACTS: columns 9-11 blank, column 12 holds ``add GSPs`` in one row): both layouts load into
    one family, the three blank columns are null everywhere, ``note`` holds ``add GSPs`` in
    exactly the one 2022 row and null in the standard 2021 rows."""
    _run(data, "dgg21")
    _run(data, "dgg22")
    frame = _silver(data, DG_GT)
    for name in ("unnamed_9", "unnamed_10", "unnamed_11"):
        assert frame[name].null_count() == frame.height, name
    noted = frame.filter(pl.col("note").is_not_null())
    assert noted["note"].to_list() == ["add GSPs"]
    assert noted["edition"].to_list() == [2022]
    assert frame.filter(pl.col("edition") == 2021)["note"].null_count() == len(rows("dgg21"))


def test_storage_below_1mw_gains_tech_in_2024_and_stays_null_before(data: Path) -> None:
    """Detects the 2024 ``tech`` column dropped or the 2023 body failing for lacking it: both
    epochs load into one family, 2023 ``tech`` is null for every row and 2024 carries the
    vendor's value, and the key (which holds ``tech``) stays unique across them."""
    _run(data, "stl23")
    _run(data, "stl24")
    frame = _silver(data, STL)
    assert frame.filter(pl.col("edition") == 2023)["tech"].null_count() == len(rows("stl23"))
    assert frame.filter(pl.col("edition") == 2024)["tech"].to_list() == [
        r["tech"] for r in rows("stl24")
    ]
    assert frame.select(list(EXPECT[STL].key)).is_duplicated().sum() == 0


# --------------------------------------------------------------------------- #
# The GSP lookup: raw coordinates, one failing encoding
# --------------------------------------------------------------------------- #


def test_the_gsp_lookup_keeps_na_coordinates_as_text(data: Path) -> None:
    """Detects a numeric coordinate cast (it would fail on the six ``#N/A`` cells) or a
    sentinel turned into null: the UTF-8 BOM body loads, ``#N/A`` is kept verbatim in both
    coordinates, and blank comments are null."""
    _run(data, "gsp21")
    frame = _silver(data, GSP)
    na = [r for r in rows("gsp21") if r["Latitude"] == "#N/A"]
    assert len(na) >= 1
    assert frame["latitude"].to_list().count("#N/A") == len(na)
    assert frame["longitude"].to_list().count("#N/A") == len(na)
    assert frame.schema["latitude"] == pl.Utf8
    assert frame["comments"].null_count() == sum(r["Comments"] == "" for r in rows("gsp21"))


def test_the_2022_gsp_lookup_fails_alone_with_unicode_decode_error(data: Path) -> None:
    """Detects a body decoded with replacement or a lossy codec, a generic failure class, or one
    bad capture taking the family down: the 2022 capture (byte ``0x92``) fails by itself with
    ``UnicodeDecodeError`` and ``bytes.decode``'s message, leaves no completion and no output,
    while the 2021 capture completes with no replacement character anywhere in the silver."""
    bad = capture(data, "gsp22")
    good = capture(data, "gsp21")
    with pytest.raises(UnicodeDecodeError) as decoded:
        body("gsp22").decode("utf-8")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, GSP, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [(bad, "UnicodeDecodeError")]
    failure = read_failure(data, GSP, bad)
    assert failure is not None and failure["error_class"] == "UnicodeDecodeError"
    assert failure["message"] == str(decoded.value)
    assert read_completion(data, GSP, bad) is None
    assert read_completion(data, GSP, good) is not None
    silver = _silver(data, GSP)
    assert set(silver["bronze_capture_id"].to_list()) == {good}
    text = "".join(str(v) for column in silver.columns for v in silver[column].to_list())
    assert "�" not in text and "\x92" not in text


def _gsp_package_registry(monkeypatch: pytest.MonkeyPatch, data: Path) -> None:
    document = json.loads((REGISTRY_DIR / f"{PACKAGE}.json").read_text(encoding="utf-8"))
    install_generated(monkeypatch, data / "_registry", [document])


def test_the_committed_entry_is_backed_by_the_registry() -> None:
    """Detects a ledger entry the registry no longer backs (a renamed family, a capture under
    the wrong directory or outside the package, a ruling that is not a line number) and one
    that names another failure class: exactly one SCN-1b entry, a ``failed``
    ``UnicodeDecodeError`` on the 2022 GSP lookup capture of resource 000d08b9."""
    entries = registry_module.load_reconcile_adjudications(None)
    mine = [e for e in entries if e.family == GSP]
    assert len(mine) == 1
    entry = mine[0]
    assert (entry.category, entry.cause, entry.ruling) == ("failed", "UnicodeDecodeError", "607")
    assert entry.captures == (GSP_2022_CAPTURE,)
    assert CAPTURES["gsp22"].resource_id in GSP_2022_CAPTURE
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []


def test_the_2022_gsp_lookup_failure_is_adjudicated_not_open_and_not_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects the invalid-encoding capture left as an open gap (reconcile red forever), an
    entry that no longer matches its failure record (stale), or an adjudication that alters
    data: without an entry reconcile reports the ``failed`` gap; with the committed entry (its
    capture id swapped for the fixture capture's) it is adjudicated, none is open or stale,
    and the silver and state bytes are equal before and after."""
    _gsp_package_registry(monkeypatch, data)
    bad, _good = capture(data, "gsp22"), capture(data, "gsp21")
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, GSP, data).run(DAY, run_id="r")

    def snapshot() -> dict[str, bytes]:
        return {
            p.relative_to(data).as_posix(): p.read_bytes()
            for top in ("silver", "state")
            for p in sorted((data / top).rglob("*"))
            if p.is_file()
        }

    before = snapshot()
    code, lines = run_cli(GSP, "--cutoff", DAY.isoformat())
    assert code == 1, lines
    assert any(line.startswith("GAP failed") and bad in line for line in lines)
    committed = json.loads(
        (REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8")
    )
    (entry,) = [e for e in committed if e["family"] == GSP]
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json([{**entry, "captures": [bad]}]), encoding="utf-8"
    )
    code, lines = run_cli(GSP, "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert "SUMMARY adjudicated 1" in lines
    assert "SUMMARY stale_adjudication 0" in lines
    assert [line for line in lines if line.startswith("GAP")] == []
    adjudicated = [line for line in lines if bad in line]
    assert len(adjudicated) == 1
    assert "UnicodeDecodeError" in adjudicated[0] and "ruling 607" in adjudicated[0]
    assert snapshot() == before


# --------------------------------------------------------------------------- #
# The 2021 above-1 MW storage resource: held
# --------------------------------------------------------------------------- #


def test_the_2021_storage_resource_is_held_with_its_reason() -> None:
    """Detects the 2021 resource routed to silver, dispositioned DOC/GIS, given an edition
    entry or left without its reason: it alone is a resource-level HOLD (E-SEM) naming the
    preamble and the repeated NORT_1 tuple, the family's other resource is SILVER, and the
    family's edition map has no 2021 pair."""
    registry = load_registry()
    held = registry.resources[HOLD_RESOURCE][1]
    assert held.family == STG_PRE
    assert isinstance(held.disposition, HoldDisposition)
    assert held.disposition.unit == "E-SEM"
    assert "preamble" in held.disposition.reason and "NORT_1" in held.disposition.reason
    assert "header on physical line 3" in held.disposition.reason
    others = [
        r for _p, r in registry.resources.values() if r.family == STG_PRE and r.id != HOLD_RESOURCE
    ]
    assert [r.id for r in others] == [CAPTURES["stgpre22"].resource_id]
    assert isinstance(others[0].disposition, SilverDisposition)
    assert all(isinstance(r.disposition, SilverDisposition) for r in others)
    held_resources = [
        r.id
        for p in registry.packages
        if p.package == PACKAGE
        for r in p.resources
        if isinstance(r.disposition, HoldDisposition)
    ]
    assert held_resources == [HOLD_RESOURCE]
    assert all(
        "2021" not in filename for filename, _edition in _record(STG_PRE).edition_by_filename or ()
    )


def test_the_held_resource_cannot_be_read_by_the_record_and_is_never_transformed(
    data: Path,
) -> None:
    """Detects the 2021 capture reaching an owner (a failed capture and a reconcile gap) or
    expected by reconcile despite its HOLD: its first parsed header matches no epoch of the
    record (the preamble the reader cannot skip), the 2022 sibling completes alone, the
    held capture has no completion and no failure, and reconcile reports no gap."""
    held = capture(data, "stgpre21")
    capture(data, "stgpre22")
    path = next((data / "bronze" / SOURCE / STG_PRE).rglob(f"*{HOLD_RESOURCE}*.csv"))
    parsed = next(read_csv_body(path, _record(STG_PRE), ()))
    assert not any(list(epoch.header) == list(parsed.header) for epoch in _record(STG_PRE).epochs)
    assert parsed.header[0] == "Created by CalcDxStorage.sas"
    assert get_transformer(SOURCE, STG_PRE, data).run(DAY, run_id="r") == len(rows("stgpre22"))
    assert read_completion(data, STG_PRE, held) is None
    assert read_failure(data, STG_PRE, held) is None
    report = reconcile(data, load_registry(), [STG_PRE], DAY)
    assert report.gaps == (), report.lines()
    frame = _silver(data, STG_PRE)
    assert set(frame["edition"].to_list()) == {2022}
    assert frame["resource_id"].unique().to_list() == [CAPTURES["stgpre22"].resource_id]


def test_blank_summer_measures_of_the_2022_storage_body_are_null(data: Path) -> None:
    """Detects blank ``summam`` / ``summpm`` cells (76 each in the real body) dropped or zeroed:
    the 2022 above-1 MW storage rows with blanks are kept with null in those measures only."""
    _run(data, "stgpre22")
    frame = _silver(data, STG_PRE)
    blanks = sum(r["summam"] == "" for r in rows("stgpre22"))
    assert blanks > 0
    assert frame["summam"].null_count() == frame["summpm"].null_count() == blanks
    assert frame["wintpk"].null_count() == 0


# --------------------------------------------------------------------------- #
# Vintage, the catalogue and generated artefacts
# --------------------------------------------------------------------------- #


def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(data: Path) -> None:
    """Detects an issue-time proxy or a projection-year clock (RULINGS 529/597), or a catalogue
    view that cannot carry the ``type`` column: ``available_at`` is the CKAN ``last_modified``
    (2021-07-06), ``timestamp_utc`` stays the capture time, an as-of read before the vintage
    serves nothing and one after it serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES["dem21"]
    capture_id, _ = _run(data, "dem21")
    frame = _silver(data, DEMAND)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, DEMAND, datetime(2021, 7, 5, tzinfo=UTC)) == []
    assert set(both_as_of(db, data, DEMAND, datetime(2021, 7, 7, tzinfo=UTC))) == {capture_id}
    assert set(both_as_of(db, data, DEMAND, None)) == {capture_id}


def test_the_skeleton_page_renders_the_new_records() -> None:
    """Detects a record the docs generator cannot render (the ``year_label`` columns, the unnamed
    trailing columns, the held questions, the held 2021 resource)."""
    page = skeleton.render_package(
        load_registry(),
        {
            "name": PACKAGE,
            "title": "Regional breakdown of FES data (Electricity)",
            "organization": {"title": "FES: Pathways to Net Zero"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    assert "`year_label`" in page and "`unnamed_9`" in page and "`note`" in page
    assert "projection_year" not in page
    for family in FAMILIES:
        assert family in page
