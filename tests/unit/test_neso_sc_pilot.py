"""The FES ED1 pilot record through unpivot and edition (v0.22-SC, ADR-042).

T-SC5..T-SC8 of the unit plan, over the committed registry and the four ED1 fixtures in
``tests/fixtures/neso_data_portal/sc/``, each named by the real ``resource_filename``.

**Fixture rule.** Each fixture is its bronze body's header line plus data rows 1-5 plus
the first row of every ``Peak/ Annual/ Minimum`` value not yet included, copied as
physical lines (the 2024 UTF-8 BOM kept). The picked data rows, long rows and blank year
cells are (E16):

- 2023 rows 1-5, 151, 276, 406: 8 / 328 / 22;
- 2024 rows 1-5, 146, 271, 401: 8 / 248 / 21;
- 2025 rows 1-5, 126: 6 / 168 / 15;
- 2026 rows 1-5, 21, 33: 7 / 196 / 110.

The bronze bodies use CRLF; git normalises fixtures to LF in the index, so
:func:`body` re-terminates every line with CRLF, the bronze convention, on every
platform. Captures carry E2's real sidecar resource ids, names, filenames,
``ckan_last_modified`` and ``written_at``.
"""

from __future__ import annotations

import csv
import io
import json
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest
from _neso_dem1h_pin import dump, generated_pin
from _neso_generic_support import write_capture
from _neso_sc_pin import PIN_PATH
from test_neso_dem1_records import _short_base
from test_neso_multi_resource import SCN1A_ADDED, SCN1B_ADDED, SCN1C_ADDED, SCN1D_ADDED

from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.registry import Held, load_registry
from gridflow.connectors.neso_data_portal.registry.record import EDITION, PROJECTION_YEAR, VALUE
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, latest_select_sql, select_latest_vintage
from gridflow.silver.neso_data_portal.casting import (
    CAPTURE_STAMP_COLUMNS,
    UnmappedResourceEditionError,
)
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    scan_completions,
)
from gridflow.silver.neso_data_portal.generic import output_columns
from gridflow.silver.neso_data_portal.reconcile import reconcile
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry import SchemaRecord

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "neso_data_portal" / "sc"
SOURCE = "neso_data_portal"
PILOT = "fes_ed1_electricity_demand"
SLUG = "fes-electricity-demand-summary-data-table-ed1"
PACKAGE_ID = "2c15c755-d8fe-4229-9169-3b6dd7c88fec"
DAY = date(2026, 10, 8)
FIRST_CAPTURE = datetime(2026, 10, 8, 10, 53, 37, tzinfo=UTC)
DIMENSIONS = (
    "aggregation_level",
    "level",
    "data_item",
    "unit",
    "scenario",
    "pathway",
    "fuel",
    "peak_annual_minimum",
)
VENDOR = {
    "Aggregation Level": "aggregation_level",
    "Level": "level",
    "Data item": "data_item",
    "Unit": "unit",
    "Scenario": "scenario",
    "Pathway": "pathway",
    "Fuel": "fuel",
    "Peak/ Annual/ Minimum": "peak_annual_minimum",
}
QUESTION = (
    "TODO: For every ED1 variable and edition 2023–2026, what period does a year header "
    "denote, and does its integer label denote the starting or ending year? Which same-edition "
    "ED2 definition applies to each ED1 Data item, aggregation level and measure, including the "
    "2026 Ten Year Outlook? What does a blank projection cell mean (not modelled, not applicable "
    "or zero)? NESO states none of these in the CSVs; projection_year is the vendor's header label "
    "and blank cells are kept as null."
)


@dataclass(frozen=True)
class Edition:
    """One ED1 capture as E2 records it, and its E16 fixture counts."""

    filename: str
    resource_id: str
    edition: int
    ckan_last_modified: str
    written_at: datetime
    long_rows: int
    blanks: int

    @property
    def resource_name(self) -> str:
        return f"Electricity Demand Summary (ED1) {self.edition}"

    @property
    def vintage(self) -> datetime:
        """``ckan_last_modified`` read as UTC, as the engine reads it."""
        return datetime.fromisoformat(self.ckan_last_modified).replace(tzinfo=UTC)


EDITIONS = (
    Edition(
        "fes2023_ed1_v001.csv",
        "de41fa7f-5bd6-4da4-aff1-aede85d7b651",
        2023,
        "2023-07-10T03:30:52.627453",
        datetime(2026, 10, 8, 10, 53, 37, 320445, tzinfo=UTC),
        328,
        22,
    ),
    Edition(
        "fes2024_ed1_v002.csv",
        "9514d486-5fa8-4bbb-80d3-59668e1eeb5f",
        2024,
        "2024-07-16T11:10:23.666541",
        datetime(2026, 10, 8, 10, 53, 39, 931489, tzinfo=UTC),
        248,
        21,
    ),
    Edition(
        "fes2025_ed1_v006.csv",
        "300c07b9-baeb-4411-bc40-987cbb4aec0b",
        2025,
        "2025-12-10T17:03:48.740109",
        datetime(2026, 10, 8, 10, 53, 43, 56847, tzinfo=UTC),
        168,
        15,
    ),
    Edition(
        "10yo2026_ed1_v001.csv",
        "2769bcac-e2ae-45b9-a9d6-0e2c3e6312c7",
        2026,
        "2026-09-16T14:24:51.109781",
        datetime(2026, 10, 8, 10, 53, 45, 551066, tzinfo=UTC),
        196,
        110,
    ),
)
BY_FILENAME = {item.filename: item for item in EDITIONS}
BY_RESOURCE = {item.resource_id: item for item in EDITIONS}


def body(filename: str) -> bytes:
    """The fixture's bytes, every line CRLF-terminated (the bronze convention)."""
    raw = (FIXTURES / filename).read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n")


def oracle_rows(filename: str) -> tuple[list[str], list[list[str]]]:
    """The stdlib ``csv`` reading of a fixture (header stripped, BOM removed)."""
    rows = list(csv.reader(io.StringIO(body(filename).decode("utf-8-sig"), newline="")))
    return [name.strip() for name in rows[0]], rows[1:]


def capture(data: Path, item: Edition, *, raw: bytes | None = None, **overrides: Any) -> str:
    """Write one ED1 capture with E2's sidecar values; returns its capture id."""
    values: dict[str, Any] = {
        "resource_id": item.resource_id,
        "resource_name": item.resource_name,
        "resource_filename": item.filename,
        "ckan_last_modified": item.ckan_last_modified,
        "written_at": item.written_at,
    }
    values.update(overrides)
    path, _sidecar = write_capture(
        data,
        PILOT,
        package_slug=SLUG,
        package_id=PACKAGE_ID,
        body=raw if raw is not None else body(item.filename),
        partition=DAY,
        **values,
    )
    return capture_id_for(path, data)


def pilot_record() -> SchemaRecord:
    record = load_registry().families[PILOT][1].record
    assert record is not None
    return record


def _query(db: Path, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, params).pl() if params else con.execute(sql).pl()
    finally:
        con.close()


def both_latest(db: Path, data: Path, as_of: datetime | None) -> pl.DataFrame:
    """The catalogue's ``_latest`` (or the parameterised select); asserts SQL = Polars."""
    view = f"silver_{SOURCE}_{PILOT}"
    spec = LATEST_VIEW_SPECS[(SOURCE, PILOT)]
    if as_of is None:
        sql = _query(db, f'SELECT * FROM "{view}_latest"')
    else:
        columns = set(_query(db, f'SELECT * FROM "{view}" LIMIT 0').columns)
        select = latest_select_sql(view, spec, columns, as_of_param=True)
        assert select is not None
        sql = _query(db, select, {"as_of": as_of.isoformat()})
    files = sorted((data / "silver" / SOURCE / PILOT).rglob("[!.]*.parquet"))
    lf = pl.scan_parquet(files, hive_partitioning=False)
    polars = select_latest_vintage(lf, spec, as_of, completions=scan_completions(data)).collect()
    assert sorted(sql["bronze_capture_id"].to_list()) == sorted(
        polars["bronze_capture_id"].to_list()
    ), as_of
    return sql


@dataclass(frozen=True)
class Pilot:
    """The four ED1 captures transformed once, plus their catalogue."""

    data: Path
    db: Path
    ids: dict[int, str]
    excluded: int


@pytest.fixture(scope="module")
def pilot() -> Iterator[Pilot]:
    """Every ED1 fixture captured with E2's sidecar values, run once, catalogued."""
    with (
        pytest.MonkeyPatch.context() as patch,
        tempfile.TemporaryDirectory(
            prefix="sc", dir=_short_base(), ignore_cleanup_errors=True
        ) as root,
    ):
        patch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
        data = Path(root)
        ids = {item.edition: capture(data, item) for item in EDITIONS}
        transformer = get_transformer(SOURCE, PILOT, data)
        transformer.run(DAY, run_id="pilot")
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        yield Pilot(data, db, ids, transformer.last_excluded_row_count)


def _silver(data: Path) -> pl.DataFrame:
    return pl.concat(
        pl.read_parquet(path, hive_partitioning=False)
        for path in sorted((data / "silver" / SOURCE / PILOT).rglob("[!.]*.parquet"))
    )


# --------------------------------------------------------------------------- #
# T-SC5: the pilot (SC4, SC6)
# --------------------------------------------------------------------------- #


class TestPilot:
    def test_t_sc5_a_each_fixture_types_without_exclusions(self, pilot: Pilot) -> None:
        """Detects a pilot row excluded (a blank cell judged non-nullable, a dimension
        refused), or a fixture matching another edition's epoch."""
        assert pilot.excluded == 0
        record = pilot_record()
        for item in EDITIONS:
            completion = read_completion(pilot.data, PILOT, pilot.ids[item.edition])
            assert completion is not None, item.filename
            assert completion["outcome"] == "populated"
            assert completion["rows_excluded"] == 0
            header, _rows = oracle_rows(item.filename)
            matched = [epoch for epoch in record.epochs if list(epoch.header) == header]
            assert len(matched) == 1, item.filename
            assert record.epochs.index(matched[0]) == EDITIONS.index(item)

    def test_t_sc5_b_long_rows_and_nulls_equal_the_oracle(self, pilot: Pilot) -> None:
        """Detects a lost or invented long row, or a blank cell not kept as null: per
        capture, rows = picked rows x year columns and null ``value`` = the blank year cells
        the stdlib ``csv`` module counts, both equal to E16's figures."""
        silver = _silver(pilot.data)
        for item in EDITIONS:
            header, rows = oracle_rows(item.filename)
            years = len(header) - 6
            blanks = sum(1 for row in rows for cell in row[6:] if cell == "")
            assert (len(rows) * years, blanks) == (item.long_rows, item.blanks), item.filename
            mine = silver.filter(pl.col("bronze_capture_id") == pilot.ids[item.edition])
            assert mine.height == item.long_rows, item.filename
            assert mine[VALUE].null_count() == item.blanks, item.filename

    def test_t_sc5_c_editions_and_years_are_the_declared_ones(self, pilot: Pilot) -> None:
        """Detects a wrong edition stamp or a year set differing from the header's."""
        silver = _silver(pilot.data)
        for item in EDITIONS:
            mine = silver.filter(pl.col("bronze_capture_id") == pilot.ids[item.edition])
            assert mine[EDITION].unique().to_list() == [item.edition]
            header, _rows = oracle_rows(item.filename)
            assert set(mine[PROJECTION_YEAR].to_list()) == {int(label) for label in header[6:]}

    def test_t_sc5_d_every_cell_is_the_vendor_value(self, pilot: Pilot) -> None:
        """Detects a converted, shifted or mis-joined value: every long row equals its
        vendor cell (``float(cell)``, blank as null) and ``unit`` is the vendor cell."""
        silver = _silver(pilot.data)
        for item in EDITIONS:
            header, rows = oracle_rows(item.filename)
            expected: dict[tuple[Any, ...], float | None] = {}
            for row in rows:
                dims = dict.fromkeys(DIMENSIONS)
                dims.update({VENDOR[name]: row[i] for i, name in enumerate(header[:6])})
                for offset, label in enumerate(header[6:], start=6):
                    cell = row[offset]
                    key = (*(dims[name] for name in DIMENSIONS), int(label))
                    expected[key] = float(cell) if cell != "" else None
            mine = silver.filter(pl.col("bronze_capture_id") == pilot.ids[item.edition])
            actual = {
                tuple(values[:-1]): values[-1]
                for values in mine.select(*DIMENSIONS, PROJECTION_YEAR, VALUE).iter_rows()
            }
            assert actual == expected, item.filename
            assert set(mine["unit"].to_list()) <= {"GW", "GWh"}

    def test_t_sc5_e_output_columns_are_the_planned_list(self, pilot: Pilot) -> None:
        """Detects a year label, a temporary name or a misplaced stamp in the output."""
        planned = [
            "aggregation_level",
            "data_item",
            "unit",
            "scenario",
            "fuel",
            "peak_annual_minimum",
            PROJECTION_YEAR,
            VALUE,
            "pathway",
            "level",
            EDITION,
            "resource_id",
            *CAPTURE_STAMP_COLUMNS,
        ]
        bitemporal = ["event_time", "available_at", "source_run_id", "dataset_version"]
        names = [name for name, _kind in output_columns(pilot_record())]
        assert names == [*planned, *bitemporal, "month", "year"]
        assert _silver(pilot.data).columns == [*planned, *bitemporal]

    def test_t_sc5_f_latest_serves_every_edition(self, pilot: Pilot) -> None:
        """Detects editions collapsing in ``_latest`` or a false overlap between them."""
        served = both_latest(pilot.db, pilot.data, None)
        assert set(served["bronze_capture_id"].to_list()) == set(pilot.ids.values())
        assert sorted(served[EDITION].unique().to_list()) == [2023, 2024, 2025, 2026]
        assert served.height == sum(item.long_rows for item in EDITIONS)
        report = reconcile(pilot.data, load_registry(), [PILOT], DAY)
        assert report.gaps == ()
        assert "SUMMARY overlap 0" in report.lines()

    def test_t_sc5_g_an_unmapped_filename_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Detects a fifth (or re-versioned) file given a guessed edition (FM-3)."""
        monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
        item = BY_FILENAME["fes2025_ed1_v006.csv"]
        with tempfile.TemporaryDirectory(
            prefix="sc", dir=_short_base(), ignore_cleanup_errors=True
        ) as root:
            data = Path(root)
            capture_id = capture(
                data, item, raw=body(item.filename), resource_filename="fes2027_ed1_v001.csv"
            )
            with pytest.raises(NesoCaptureFailedError) as info:
                get_transformer(SOURCE, PILOT, data).run(DAY, run_id="r")
            failures = [cls for _capture, cls, _message in info.value.failures]
            assert failures == [UnmappedResourceEditionError.__name__]
            assert "fes2027_ed1_v001.csv" in info.value.failures[0][2]
            assert read_completion(data, PILOT, capture_id) is None

    def test_t_sc5_h_the_output_is_held_on_the_semantics_question(self) -> None:
        """Detects the pilot published before its year, definition and blank-cell
        semantics are answered (decision 16)."""
        eligibility = pilot_record().eligibility
        assert isinstance(eligibility, Held)
        assert eligibility.unit == "E-SEM"
        assert eligibility.question == QUESTION

    def test_t_sc5_i_only_the_pilot_opts_in(self) -> None:
        """Detects any other committed record setting ``unpivot`` or ``edition_by_filename``
        (only the pilot, K-SCN-1b's nine regional FES records which map editions but never
        unpivot, and K-SCN-1c's three building block records, the main one unpivoted, and K-SCN-1d's
        ES1 record, unpivoted too; in a fresh interpreter, so nothing collection imported can mask
        it)."""
        code = textwrap.dedent(
            """
            from gridflow.connectors.neso_data_portal.registry import load_registry
            families = load_registry().families
            opted = sorted(
                k for k, (_p, f) in families.items()
                if f.record is not None and (
                    f.record.edition_by_filename is not None
                    or any(e.unpivot is not None for e in f.record.epochs)
                )
            )
            assert opted == [
                "fes_building_blocks_block_definitions",
                "fes_building_blocks_block_licence_area",
                "fes_building_blocks_main",
                "fes_ed1_electricity_demand",
                "fes_es1_electricity_supply",
                "fes_regional_demand_active_power",
                "fes_regional_dg_gt_1mw",
                "fes_regional_dg_lt_1mw",
                "fes_regional_dsr",
                "fes_regional_gsp_info",
                "fes_regional_storage_gt_1mw",
                "fes_regional_storage_gt_1mw_pre2023",
                "fes_regional_storage_lt_1mw",
                "fes_regional_storage_lt_1mw_pre2023",
            ], opted
            print("OK", opted)
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        assert "OK" in result.stdout, result.stdout


# --------------------------------------------------------------------------- #
# T-SC6: the leakage matrix (SC2, SC3, B3, C-8)
# --------------------------------------------------------------------------- #


def _z(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


GRID: list[tuple[datetime | None, set[int]]] = [
    (_z("2023-07-10T03:30:52.627452"), set()),
    (_z("2023-07-10T03:30:52.627453"), {2023}),
    (_z("2024-07-16T11:10:23.666540"), {2023}),
    (_z("2024-07-16T11:10:23.666541"), {2023, 2024}),
    (_z("2025-12-10T17:03:48.740109"), {2023, 2024, 2025}),
    (_z("2026-09-16T14:24:51.109780"), {2023, 2024, 2025}),
    (_z("2026-09-16T14:24:51.109781"), {2023, 2024, 2025, 2026}),
    (None, {2023, 2024, 2025, 2026}),
]


class TestLeakageMatrix:
    """T-SC6. The bound is the capture's ``available_at`` (the CKAN ``last_modified``
    publication vintage), not its capture time (SC-SPEC leakage line; RULINGS 597)."""

    @pytest.mark.parametrize(
        ("as_of", "expected"),
        GRID,
        ids=[as_of.isoformat() if as_of else "latest" for as_of, _e in GRID],
    )
    def test_t_sc6_an_edition_is_served_from_its_vintage(
        self, pilot: Pilot, as_of: datetime | None, expected: set[int]
    ) -> None:
        """Detects an edition served before its publication vintage (leakage), withheld
        after it, a renderer disagreement, or capture time used as the bound."""
        completions = scan_completions(pilot.data, PILOT).collect()
        oracle = {
            BY_RESOURCE[row["resource_id"]].edition
            for row in completions.iter_rows(named=True)
            if as_of is None or row["available_at"] <= as_of
        }
        served = both_latest(pilot.db, pilot.data, as_of)
        editions = set(served[EDITION].to_list())
        assert editions == expected == oracle
        if expected:
            assert oracle
        if as_of is not None:
            # DuckDB hands TIMESTAMPTZ back in the session zone; compare instants in UTC.
            clocks = served.select(
                pl.col("available_at", "capture_written_at").dt.convert_time_zone("UTC")
            )
            assert clocks.filter(pl.col("available_at") > as_of).height == 0
            if as_of < FIRST_CAPTURE and clocks.height:
                assert clocks.filter(pl.col("capture_written_at") <= as_of).height == 0

    def test_t_sc6_a_correction_is_served_only_from_its_vintage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a correction of one edition leaking into an as-of before its vintage, the
        original surviving after it, or another edition disturbed by it."""
        monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
        item = BY_FILENAME["fes2025_ed1_v006.csv"]
        header, rows = oracle_rows(item.filename)
        changed = next(i for i, cell in enumerate(rows[0]) if i >= 6 and cell != "")
        original_value = float(rows[0][changed])
        rows[0][changed] = repr(original_value + 1000.0)
        buffer = io.StringIO()
        csv.writer(buffer, lineterminator="\r\n").writerows([header, *rows])
        with tempfile.TemporaryDirectory(
            prefix="sc", dir=_short_base(), ignore_cleanup_errors=True
        ) as root:
            data = Path(root)
            ids = {entry.edition: capture(data, entry) for entry in EDITIONS}
            correction = capture(
                data,
                item,
                raw=buffer.getvalue().encode("utf-8"),
                ckan_last_modified="2026-02-01T00:00:00",
                written_at=datetime(2026, 10, 8, 12, 0, tzinfo=UTC),
            )
            get_transformer(SOURCE, PILOT, data).run(DAY, run_id="r")
            db = data / "cat.duckdb"
            init_catalogue(db, data)

            def by_edition(as_of: datetime | None) -> dict[int, set[str]]:
                served = both_latest(db, data, as_of)
                out: dict[int, set[str]] = {}
                for edition, capture_id in (
                    served.select(EDITION, "bronze_capture_id").unique().iter_rows()
                ):
                    out.setdefault(edition, set()).add(capture_id)
                return out

            before = by_edition(_z("2026-01-01T00:00:00"))
            assert before == {2023: {ids[2023]}, 2024: {ids[2024]}, 2025: {ids[2025]}}
            latest = by_edition(None)
            assert latest == {
                2023: {ids[2023]},
                2024: {ids[2024]},
                2025: {correction},
                2026: {ids[2026]},
            }
            year = int(header[changed])
            dims = {VENDOR[name]: rows[0][i] for i, name in enumerate(header[:6])}
            served = both_latest(db, data, None).filter(
                (pl.col(EDITION) == 2025) & (pl.col(PROJECTION_YEAR) == year)
            )
            for name, cell in dims.items():
                served = served.filter(pl.col(name) == cell)
            assert served[VALUE].to_list() == [original_value + 1000.0]


# --------------------------------------------------------------------------- #
# T-SC7: skeleton (P-5); T-SC8: the only generated addition (I-1)
# --------------------------------------------------------------------------- #


class TestSkeletonAndPin:
    def test_t_sc7_the_page_shows_the_unpivot_and_the_edition_map(self) -> None:
        """Detects the reshape or the edition map missing from the generated docs page."""
        snapshot = {
            "name": SLUG,
            "title": "FES Electricity Demand Summary Data Table (ED1)",
            "organization": {"title": "NESO"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        }
        page = skeleton.render_package(load_registry(), snapshot, None)
        record = pilot_record()
        for index, epoch in enumerate(record.epochs, start=1):
            assert epoch.unpivot is not None
            mapping = ", ".join(f"`{label}` → {year}" for label, year in epoch.unpivot.years)
            assert f"Unpivot, epoch {index}: {mapping}" in page
        assert page.count("| (unpivot) | `projection_year` | int64 | — | no | — | — |") == 4
        assert page.count("| (unpivot) | `value` | float64 | — | yes | — | — |") == 4
        assert (
            "- Edition: `fes2023_ed1_v001.csv` → 2023; `fes2024_ed1_v002.csv` → 2024; "
            "`fes2025_ed1_v006.csv` → 2025; `10yo2026_ed1_v001.csv` → 2026"
        ) in page

    def test_t_sc8_the_pilot_is_the_only_generated_addition(self) -> None:
        """Detects any generated family other than the FES ED1 pilot, the twelve tRESP records
        of K-SCN-1a, the nine regional FES records of K-SCN-1b and the three building block records
        of K-SCN-1c and the ES1 record of K-SCN-1d appearing since the base golden
        (master ``34992b6``), and any DEM-1 engine digest added or lost."""
        golden = json.loads(PIN_PATH.read_text(encoding="utf-8"))
        current = json.loads(dump(generated_pin()))
        for section in ("sql", "records", "columns"):
            assert set(current[section]) - set(golden[section]) == {
                PILOT,
                *SCN1A_ADDED,
                *SCN1B_ADDED,
                *SCN1C_ADDED,
                *SCN1D_ADDED,
            }, section
        assert set(current["engine"]) == set(golden["engine"])
