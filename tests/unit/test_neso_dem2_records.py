"""The four demand-reference frozen records (v0.22-K-DEM-2).

``national_demand_bmus``, ``transmission_losses_main``,
``transmission_losses_financial_year`` (eligible) and ``school_holiday_percentages``
(held, per-resource selection, ADR-039). Every test writes recorded fixture captures
(slices of the 2026-10-08 swept bronze, ``tests/fixtures/neso_data_portal/dem2/``) into
a short data root and runs the transformer the **real package registry** generates, so a
record that does not fit its vendor body fails here, not at activation. On master none of
the four families has a record, so ``get_transformer`` raises for every key below.

Fixture cuts: the two transmission-loss bodies are whole (161 and 13 rows); the BMU body
holds its header, its first three names and CSV lines 988-992 (which include the
trailing-space identifier ``"EEND01 "`` of line 990); each school body holds its header,
its first sixteen ``Aberdeen`` rows (a holiday starts inside them) and the first three
``Total`` rows. ``git`` and ``core.autocrlf`` normalise a committed fixture's line
endings, so :func:`body` re-terminates every record with the bronze original's CRLF and
no test asserts on a fixture's raw bytes.

``demand_profile_dates`` has no record (RULINGS 538), pinned below.
"""

from __future__ import annotations

import csv
import io
import os
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import Eligible, Held
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
from gridflow.silver.neso_data_portal.completion import (
    capture_id_for,
    read_completion,
    scan_completions,
)
from gridflow.silver.neso_data_portal.reconcile import reconcile
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "dem2"
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 11, 0, tzinfo=UTC)

BMU = "national_demand_bmus"
SCHOOL = "school_holiday_percentages"
LOSS_MAIN = "transmission_losses_main"
LOSS_FY = "transmission_losses_financial_year"

# family -> (package slug, package id)
PACKAGES: dict[str, tuple[str, str]] = {
    BMU: ("national-demand-balancing-mechanism-units", "1999ccfb-5f8c-4921-b408-d92ab5c61c2b"),
    SCHOOL: ("school-holiday-percentages", "c0e9769c-0a6b-460d-a6e6-c66515317a64"),
    LOSS_MAIN: ("transmission-losses", "ec5a1356-f6dd-40a1-b714-dbdad3ed00af"),
    LOSS_FY: ("transmission-losses", "ec5a1356-f6dd-40a1-b714-dbdad3ed00af"),
}

# fixture filename (= the vendor resource_filename) -> (family, resource id, resource name,
# ckan_last_modified): the real sidecar identities of the 2026-10-08 captures.
SIDECARS: dict[str, tuple[str, str, str, str]] = {
    "bmunits_29_09_26.csv": (
        BMU,
        "7e1e84cf-89f4-4c69-a72a-d778087c22a4",
        "National Demand Balancing Mechanism Units",
        "2026-09-29T13:45:22.344455",
    ),
    "monthly-losses.csv": (
        LOSS_MAIN,
        "fddc307d-fc5a-458d-809f-2ad9a697b142",
        "Transmission Losses",
        "2026-09-25T13:27:36.411183",
    ),
    "financial-year-losses.csv": (
        LOSS_FY,
        "c0cd9512-4f8a-468e-8823-7c8238dcb9b9",
        "Financial Year Losses",
        "2026-06-04T09:14:02.612393",
    ),
    "school_holiday_2021_22.csv": (
        SCHOOL,
        "48f922ef-b18f-4193-868c-e95c1f271b5c",
        "School Holiday Percentages 2021/22",
        "2024-10-17T16:13:23.814979",
    ),
    "school_holiday_2022_23.csv": (
        SCHOOL,
        "8a9a21e2-91d9-40e0-ac99-68f5e3f1a732",
        "School Holiday Percentages 2022/23",
        "2024-10-17T16:09:06.364747",
    ),
    "school_holiday_2023_24.csv": (
        SCHOOL,
        "4b640366-af9a-4785-b12f-ff2274e369c0",
        "School Holiday Percentages 2023/24",
        "2024-10-17T16:07:36.806001",
    ),
    "school_holiday_2024_25.csv": (
        SCHOOL,
        "f2c25078-5721-4abd-b9e3-0fd6d839bb04",
        "School Holiday Percentages 2024/25",
        "2024-10-17T16:05:05.134322",
    ),
    "school_holiday_2025_26.csv": (
        SCHOOL,
        "73c68bc9-d08b-4173-bfef-6ba1ac0684a9",
        "School Holiday Percentages 2025/26",
        "2025-07-15T11:27:03.752628",
    ),
    "school_holiday_2026_27.csv": (
        SCHOOL,
        "5e4bb837-a43b-4709-91e6-ddf8ca9a660a",
        "School Holiday Percentages 2026/27",
        "2026-02-12T16:12:22.167008",
    ),
}
SCHOOL_FILES = tuple(name for name, (family, *_rest) in SIDECARS.items() if family == SCHOOL)

SCHOOL_QUESTION = (
    "TODO: the numeric scale of the Total rows: the vendor calls the aggregate a percentage, "
    "but the values measure as a fraction equal to the sum of (multiplier x local indicator) "
    "within 0.0005; NESO has not stated the encoding"
)

# (key, filenames, entity key)
SHAPES: dict[str, tuple[str, ...]] = {
    BMU: ("bm_name",),
    LOSS_MAIN: ("financial_year", "month_vendor", "nget", "spt", "shetl", "gb_totals"),
    LOSS_FY: ("financial_year", "sum_of_nget", "sum_of_spt", "sum_of_shetl", "sum_of_gb_totals"),
    SCHOOL: ("resource_id", "local_authority", "multiplier", "date", "school_holiday"),
}


def _short_base() -> str:
    """The drive root on Windows (ADR-036: engine names pass MAX_PATH there); tmp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root; gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="d2", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def body(filename: str) -> bytes:
    """The fixture ``filename`` terminated by the bronze original's CRLF."""
    raw = (FIXTURES / filename).read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n")


def _table(raw: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline="")))


def _rows(filename: str) -> int:
    return len(_table(body(filename)))


def _filenames(family: str) -> list[str]:
    return [name for name, (fam, *_rest) in SIDECARS.items() if fam == family]


def _capture(data: Path, filename: str, *, as_filename: str | None = None) -> str:
    family, resource_id, name, modified = SIDECARS[filename]
    slug, package_id = PACKAGES[family]
    # distinct write instants so six captures of one family never share a body name
    written = WRITTEN + timedelta(seconds=list(SIDECARS).index(filename))
    path, _sidecar = write_capture(
        data,
        family,
        body=body(filename),
        written_at=written,
        partition=DAY,
        package_slug=slug,
        package_id=package_id,
        resource_id=resource_id,
        resource_name=name,
        resource_filename=as_filename or filename,
        ckan_last_modified=modified,
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _run(data: Path, key: str) -> dict[str, str]:
    """Capture every fixture of ``key`` and transform them; filename -> capture id."""
    ids = {filename: _capture(data, filename) for filename in _filenames(key)}
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    return ids


@pytest.mark.parametrize("key", [BMU, LOSS_MAIN, LOSS_FY, SCHOOL])
def test_every_fixture_types_with_no_exclusion(data: Path, key: str) -> None:
    """Detects a header that matches no epoch, a cast the vendor body does not satisfy and
    any row excluded: every capture completes with all its rows (so a column that fails
    its dtype, or a non-nullable column, is caught here and not at activation)."""
    transformer = get_transformer(SOURCE, key, data)
    ids = {filename: _capture(data, filename) for filename in _filenames(key)}
    written = transformer.run(DAY, run_id="r")
    assert written == sum(_rows(filename) for filename in ids)
    assert transformer.last_excluded_row_count == 0
    for filename, capture_id in ids.items():
        completion = read_completion(data, key, capture_id)
        assert completion is not None, filename
        assert completion["outcome"] == "populated", filename
        assert completion["row_count"] == _rows(filename), filename
        assert completion["rows_excluded"] == 0, filename


@pytest.mark.parametrize("key", [BMU, LOSS_MAIN, LOSS_FY, SCHOOL])
def test_entity_key_is_unique_in_the_output(data: Path, key: str) -> None:
    """Detects a guard key that does not identify the output grain: the entity key must
    be unique across everything the family wrote."""
    _run(data, key)
    silver = _silver(data, key)
    assert silver.height > 0
    assert silver.select(SHAPES[key]).is_duplicated().sum() == 0


def test_bm_names_survive_byte_identical(data: Path) -> None:
    """Detects any normalisation of a BM unit ID (RULINGS: stored as-is): the trailing
    space of ``"EEND01 "`` (CSV line 990 of the real body), case and order are all
    preserved exactly."""
    source = [row["BM_Name"] for row in _table(body("bmunits_29_09_26.csv"))]
    assert "EEND01 " in source  # the fixture keeps the identifier that tests trimming
    _run(data, BMU)
    silver = _silver(data, BMU)
    assert silver["bm_name"].to_list() == source


def test_transmission_losses_keep_the_published_values_and_labels(data: Path) -> None:
    """Detects the month label cast to a date (the profiler's ``%b-%y``, which invents a
    day), a financial year parsed, and a published ``GB totals`` recomputed from the owner
    columns (63 of the real 161 months differ from the sum by up to 0.001 TWh)."""
    _run(data, LOSS_MAIN)
    silver = _silver(data, LOSS_MAIN)
    source = _table(body("monthly-losses.csv"))
    assert silver.schema["month_vendor"] == pl.Utf8
    assert silver.schema["financial_year"] == pl.Utf8
    assert silver["month_vendor"].to_list() == [row["Month"] for row in source]
    assert silver["financial_year"].to_list() == [row["Financial Year"] for row in source]
    assert silver["gb_totals"].to_list() == [float(row["GB totals"]) for row in source]
    assert silver["nget"].to_list() == [float(row["NGET"]) for row in source]
    differs = [
        row
        for row in source
        if abs(
            float(row["NGET"]) + float(row["SPT"]) + float(row["SHETL"]) - float(row["GB totals"])
        )
        > 1e-9
    ]
    assert differs, "the fixture must keep rows where the published total is not the sum"

    _run_fy = {filename: _capture(data, filename) for filename in _filenames(LOSS_FY)}
    assert _run_fy
    get_transformer(SOURCE, LOSS_FY, data).run(DAY, run_id="r")
    annual = _silver(data, LOSS_FY)
    fy_source = _table(body("financial-year-losses.csv"))
    assert annual.schema["financial_year"] == pl.Utf8
    assert annual["financial_year"].to_list() == [row["Financial Year"] for row in fy_source]
    assert annual["sum_of_gb_totals"].to_list() == [
        float(row["Sum of GB totals"]) for row in fy_source
    ]


def test_school_holiday_resources_with_different_date_formats_both_type(data: Path) -> None:
    """Detects one date format applied to every resource: ``school_holiday_2024_25.csv``
    is ``%d/%m/%Y`` and the other five are ``%Y-%m-%d``, so a scalar format fails one
    side, and a day/month swap would put 2024-07-06 on 2024-06-07. Every date falls in its
    resource's holiday year."""
    ids = _run(data, SCHOOL)
    silver = _silver(data, SCHOOL)
    expected_start = {
        "school_holiday_2021_22.csv": date(2021, 6, 19),
        "school_holiday_2022_23.csv": date(2022, 6, 24),
        "school_holiday_2023_24.csv": date(2023, 6, 24),
        "school_holiday_2024_25.csv": date(2024, 6, 27),
        "school_holiday_2025_26.csv": date(2025, 6, 25),
        "school_holiday_2026_27.csv": date(2026, 6, 24),
    }
    for filename, start in expected_start.items():
        rows = silver.filter(pl.col("bronze_capture_id") == ids[filename])
        assert rows["date"].min() == start, filename
        assert rows["date"].max() < start + timedelta(days=366), filename
        assert set(rows["resource_id"].to_list()) == {SIDECARS[filename][1]}, filename
    dmy = silver.filter(pl.col("bronze_capture_id") == ids["school_holiday_2024_25.csv"])
    first_holiday = dmy.filter(
        (pl.col("local_authority") == "Aberdeen") & (pl.col("school_holiday") == 1)
    )
    assert first_holiday["date"].min() == date(2024, 7, 6)


def test_school_holiday_latest_serves_every_resource_and_no_overlap(data: Path) -> None:
    """Detects the per-family ``LIMIT 1`` (one resource surviving) in the catalogue or in
    Polars, and a false overlap among the disjoint annual resources."""
    ids = _run(data, SCHOOL)
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    con = duckdb.connect(str(db), read_only=True)
    try:
        latest = con.execute(f'SELECT * FROM "silver_{SOURCE}_{SCHOOL}_latest"').pl()
    finally:
        con.close()
    assert set(latest["bronze_capture_id"].to_list()) == set(ids.values())
    assert set(latest["resource_id"].to_list()) == {SIDECARS[f][1] for f in SCHOOL_FILES}
    files = sorted((data / "silver" / SOURCE / SCHOOL).rglob("[!.]*.parquet"))
    polars = select_latest_vintage(
        pl.scan_parquet(files, hive_partitioning=False),
        LATEST_VIEW_SPECS[(SOURCE, SCHOOL)],
        completions=scan_completions(data),
    ).collect()
    assert sorted(polars["bronze_capture_id"].to_list()) == sorted(
        latest["bronze_capture_id"].to_list()
    )
    assert latest.height == sum(_rows(f) for f in SCHOOL_FILES)
    report = reconcile(data, registry_module.load_registry(), [SCHOOL], DAY)
    assert report.clean, report.lines()
    assert "SUMMARY overlap 0" in report.lines()


def test_school_holiday_total_rows_are_kept_unscaled(data: Path) -> None:
    """Detects the ``Total`` aggregate dropped, renamed or rescaled (x100 as a
    'percentage'): NESO has not stated its encoding, so the value is published as the vendor
    typed it and the family is held on it."""
    ids = _run(data, SCHOOL)
    silver = _silver(data, SCHOOL).filter(
        pl.col("bronze_capture_id") == ids["school_holiday_2024_25.csv"]
    )
    totals = silver.filter(pl.col("local_authority") == "Total").sort("date")
    assert totals["school_holiday"].to_list() == [0.028, 0.04, 0.079]
    assert totals["multiplier"].to_list() == [1.0, 1.0, 1.0]
    assert silver.schema["school_holiday"] == pl.Float64
    assert silver.schema["multiplier"] == pl.Float64


@pytest.mark.parametrize("key", [BMU, LOSS_MAIN, LOSS_FY, SCHOOL])
def test_record_shapes_match_the_unit_table(key: str) -> None:
    """Detects a record drifting from the unit spec: csv reader, one header epoch, no issue
    recipe, CKAN vintage, whole-capture selection, ``temporal: none`` (no invented instant),
    the guard key (every column, never a value key), every column nullable with no invented
    null token or bound, and eligibility."""
    record = _record(key)
    assert record.reader == "csv"
    assert record.encoding == "utf-8"
    assert len(record.epochs) == 1
    assert record.epochs[0].issue.kind == "none"
    assert record.vintage == "ckan_last_modified"
    assert record.latest == "whole_capture"
    assert record.temporal.kind == "none"
    assert record.entity_key == SHAPES[key]
    for column in record.epochs[0].columns:
        assert column.nullable, (key, column.name)
        assert not column.null_tokens, (key, column.name)
        assert column.min is None and column.max is None, (key, column.name)
    if key == SCHOOL:
        assert record.latest_partition == "resource_id"
        assert isinstance(record.eligibility, Held)
        assert record.eligibility.unit == "E-SEM"
        assert record.eligibility.question == SCHOOL_QUESTION
        by_name = {c.name: c for c in record.epochs[0].columns}
        assert by_name["date"].format is None
        assert by_name["date"].formats_by_filename == (
            ("school_holiday_2021_22.csv", "%Y-%m-%d"),
            ("school_holiday_2022_23.csv", "%Y-%m-%d"),
            ("school_holiday_2023_24.csv", "%Y-%m-%d"),
            ("school_holiday_2024_25.csv", "%d/%m/%Y"),
            ("school_holiday_2025_26.csv", "%Y-%m-%d"),
            ("school_holiday_2026_27.csv", "%Y-%m-%d"),
        )
        assert by_name["local_authority"].dtype == "string"
    else:
        assert record.latest_partition is None
        # eligible records carry no per-output override and inherit the package's status
        assert record.eligibility is None
        package = registry_module.load_registry().families[key][0]
        assert isinstance(package.eligibility, Eligible)


def test_transmission_loss_columns_are_the_vendor_headers() -> None:
    """Detects a renamed or re-typed loss column: the owner/GB columns are float64 (TWh,
    vendor-defined) and the month and financial-year labels are strings."""
    main = {c.source: (c.name, c.dtype) for c in _record(LOSS_MAIN).epochs[0].columns}
    assert main == {
        "Financial Year": ("financial_year", "string"),
        "Month": ("month_vendor", "string"),
        "NGET": ("nget", "float64"),
        "SPT": ("spt", "float64"),
        "SHETL": ("shetl", "float64"),
        "GB totals": ("gb_totals", "float64"),
    }
    annual = {c.source: (c.name, c.dtype) for c in _record(LOSS_FY).epochs[0].columns}
    assert annual == {
        "Financial Year": ("financial_year", "string"),
        "Sum of NGET": ("sum_of_nget", "float64"),
        "Sum of SPT": ("sum_of_spt", "float64"),
        "Sum of SHETL": ("sum_of_shetl", "float64"),
        "Sum of GB totals": ("sum_of_gb_totals", "float64"),
    }


def test_demand_profile_dates_has_no_record() -> None:
    """Detects ``demand_profile_dates`` gaining a record (RULINGS 538). The vendor body
    repeats identical rows (3,224 rows, 3,216 distinct; 8 duplicates, one date pair three
    times) and the engine's duplicate guard rejects them under any available key, so a
    record would fail every capture. Silent deduplication is forbidden, so the family
    stays ingest-only until NESO corrects the body or a separate engine policy is approved.
    """
    entry = registry_module.load_registry().families["demand_profile_dates"][1]
    assert entry.record is None
