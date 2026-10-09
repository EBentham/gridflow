"""The ``historic_demand`` frozen record (v0.22-DEM-1H, H5).

Every test writes recorded fixture captures into a short data root and runs the
transformer the **real package registry** generates, so a record that does not
fit its vendor bodies fails here, not at activation. On master the family has no
record, so ``get_transformer`` raises for it.

Fixtures (``tests/fixtures/neso_data_portal/dem1h/``) are slices of the
2026-10-08 swept bronze, one per real resource filename, cut by a scratch script
(not committed) with these rules: each holds its body's header line and first four
data lines; ``demanddata_2024.csv`` also holds every line of 31-Mar-2024 (46
periods) and 27-Oct-2024 (50 periods); ``demanddata_2009.csv`` also holds its
01-Jul-2009 period-1 line (a BST row). Epochs (E7): 2009 is epoch 0, 2019 epoch 1,
2001/2023/2024/2025 epoch 2 (three date formats), 2026 epoch 3.

``git`` and ``core.autocrlf`` normalise a committed fixture's line endings, so
:func:`body` re-terminates every record with the bronze original's convention
(CRLF, except ``demanddataupdate_2026.csv``, which is LF) and no test asserts on a
fixture's raw bytes.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
from gridflow.silver.neso_data_portal.casting import UnmappedResourceFormatError, epoch_for
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
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
KEY = "historic_demand"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "dem1h"
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 11, 8, tzinfo=UTC)
PACKAGE = ("historic-demand-data", "8f2fe0af-871c-488d-8bad-960426f24601")
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# filename -> (resource id, resource name, ckan_last_modified, header epoch): the real
# sidecar identities of the 2026-10-08 captures.
SIDECARS: dict[str, tuple[str, str, str, int]] = {
    "demanddata_2009.csv": (
        "ed8a37cb-65ac-4581-8dbc-a3130780da3a",
        "Historic Demand Data 2009",
        "2023-07-24T12:16:55.273828",
        0,
    ),
    "demanddata_2019.csv": (
        "dd9de980-d724-415a-b344-d8ae11321432",
        "Historic Demand Data 2019",
        "2025-04-08T19:45:40.926204",
        1,
    ),
    "demanddata_2001.csv": (
        "e8608e9a-f56c-457f-b9e7-bfffcfd19731",
        "Historic Demand Data 2001",
        "2025-06-20T08:35:31.018121",
        2,
    ),
    "demanddata_2023.csv": (
        "bf5ab335-9b40-4ea4-b93a-ab4af7bce003",
        "Historic Demand Data 2023",
        "2025-04-08T19:54:08.935442",
        2,
    ),
    "demanddata_2024.csv": (
        "f6d02c0f-957b-48cb-82ee-09003f2ba759",
        "Historic Demand Data 2024",
        "2025-04-08T19:55:41.954513",
        2,
    ),
    "demanddata_2025.csv": (
        "b2bde559-3455-4021-b179-dfe60c0337b0",
        "Historic Demand Data 2025",
        "2026-06-09T13:25:07.456371",
        2,
    ),
    "demanddataupdate_2026.csv": (
        "8a4a771c-3929-4e56-93ad-cdf13219dea5",
        "Historic Demand Data 2026",
        "2026-10-08T08:20:27.652049",
        3,
    ),
}
LF_FILES = frozenset({"demanddataupdate_2026.csv"})

QUESTION = (
    "TODO: literal NA in the epoch-2 bodies of 2001-2008 in TSD, the embedded "
    "wind/solar columns, SCOTTISH_TRANSFER and every interconnector flow but IFA: missing, not "
    "applicable or zero? NESO defines no meaning; those columns stay text across every epoch "
    "until it does."
)


def _short_base() -> str:
    """The drive root on Windows (ADR-036: engine names pass MAX_PATH there); tmp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root; gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="dh", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def _records(raw: bytes) -> int:
    return len(list(csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""))))


def body(filename: str) -> bytes:
    """The fixture ``filename`` terminated by the bronze original's line convention."""
    raw = (FIXTURES / filename).read_bytes().replace(b"\r\n", b"\n")
    if filename not in LF_FILES and _records(raw) == raw.count(b"\n"):
        return raw.replace(b"\n", b"\r\n")
    return raw


def _rows(raw: bytes) -> int:
    return _records(raw) - 1


def _capture(data: Path, filename: str, raw: bytes, *, as_filename: str | None = None) -> str:
    resource_id, name, modified, _epoch = SIDECARS[filename]
    path, _sidecar = write_capture(
        data,
        KEY,
        body=raw,
        written_at=WRITTEN,
        partition=DAY,
        package_slug=PACKAGE[0],
        package_id=PACKAGE[1],
        resource_id=resource_id,
        resource_name=name,
        resource_filename=as_filename or filename,
        ckan_last_modified=modified,
    )
    return capture_id_for(path, data)


def _record() -> SchemaRecord:
    record = registry_module.load_registry().families[KEY][1].record
    assert record is not None
    return record


def _silver(data: Path) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / KEY).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _year(filename: str) -> int:
    match = re.search(r"_(\d{4})\.csv$", filename)
    assert match is not None, filename
    return int(match.group(1))


def _run_all(data: Path) -> dict[str, str]:
    """Capture every fixture into ``data`` and transform them; filename -> capture id."""
    ids = {filename: _capture(data, filename, body(filename)) for filename in SIDECARS}
    get_transformer(SOURCE, KEY, data).run(DAY, run_id="r")
    return ids


def test_t_h10_a_every_fixture_types_under_its_epoch_with_no_exclusion(data: Path) -> None:
    """Detects a header that matches no epoch (or the wrong one), a cast the vendor body
    does not satisfy, and any row excluded: every capture completes with all its rows."""
    record = _record()
    for filename, (_rid, _name, _modified, index) in SIDECARS.items():
        header = tuple(next(csv.reader(io.StringIO(body(filename).decode("utf-8-sig")))))
        assert epoch_for(record, header) is record.epochs[index], filename
    transformer = get_transformer(SOURCE, KEY, data)
    ids = {filename: _capture(data, filename, body(filename)) for filename in SIDECARS}
    written = transformer.run(DAY, run_id="r")
    assert written == sum(_rows(body(filename)) for filename in SIDECARS)
    assert transformer.last_excluded_row_count == 0
    for filename, capture_id in ids.items():
        completion = read_completion(data, KEY, capture_id)
        assert completion is not None, filename
        assert completion["outcome"] == "populated", filename
        assert completion["row_count"] == _rows(body(filename)), filename
        assert completion["rows_excluded"] == 0, filename
        assert completion["resource_id"] == SIDECARS[filename][0], filename


def test_t_h10_b_every_date_falls_in_its_filename_year(data: Path) -> None:
    """Detects a format that parses silently into the wrong century (E11: ``%d-%b-%Y``
    reads ``01-Jan-23`` as year 0023), i.e. a 2-digit-year file mapped to the 4-digit
    format, or a year read from anywhere but the body."""
    ids = _run_all(data)
    silver = _silver(data)
    for filename, capture_id in ids.items():
        years = (
            silver.filter(pl.col("bronze_capture_id") == capture_id)["settlement_date"]
            .dt.year()
            .unique()
            .to_list()
        )
        assert years == [_year(filename)], (filename, years)


def test_t_h10_c_an_unlisted_filename_fails_loudly(data: Path) -> None:
    """Detects a fallback format for a file the epoch-2 map does not name: the 2023 body
    under ``demanddata_2099.csv`` fails with ``UnmappedResourceFormatError`` and writes
    no output and no completion."""
    capture_id = _capture(
        data, "demanddata_2023.csv", body("demanddata_2023.csv"), as_filename="demanddata_2099.csv"
    )
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, KEY, data).run(DAY, run_id="r")
    assert [cls for _cid, cls, _msg in info.value.failures] == [
        UnmappedResourceFormatError.__name__
    ]
    assert read_completion(data, KEY, capture_id) is None
    assert not list(data.rglob("silver/**/*.parquet"))


def test_t_h10_d_dst_days_and_the_uk_period_start(data: Path) -> None:
    """Detects a 1..48 bound or a dedup collapsing the clock-change days (46 spring,
    50 autumn), and ``sp_pair`` read as UTC wall clock: period 1 of a BST date starts
    at 23:00Z the day before, of a GMT date at 00:00Z."""
    ids = _run_all(data)
    silver = _silver(data)
    year_2024 = silver.filter(pl.col("bronze_capture_id") == ids["demanddata_2024.csv"])
    per_day = dict(year_2024.group_by("settlement_date").len().iter_rows())
    assert per_day[date(2024, 3, 31)] == 46
    assert per_day[date(2024, 10, 27)] == 50
    assert (
        year_2024.filter(pl.col("settlement_date") == date(2024, 10, 27))["settlement_period"].max()
        == 50
    )
    year_2009 = silver.filter(pl.col("bronze_capture_id") == ids["demanddata_2009.csv"])
    by_pair = {
        (d, p): t
        for d, p, t in year_2009.select(
            "settlement_date", "settlement_period", "timestamp_utc"
        ).iter_rows()
    }
    assert by_pair[(date(2009, 7, 1), 1)] == datetime(2009, 6, 30, 23, 0, tzinfo=UTC)
    assert by_pair[(date(2009, 1, 1), 1)] == datetime(2009, 1, 1, 0, 0, tzinfo=UTC)


def test_t_h10_e_na_stays_text_and_demand_is_numeric(data: Path) -> None:
    """Detects ``NA`` rewritten by an invented null token or cast (no NESO definition;
    the family is held on it), and ``ND`` losing its numeric type."""
    ids = _run_all(data)
    silver = _silver(data)
    assert silver.schema["tsd"] == pl.Utf8
    assert silver.schema["nd"] == pl.Float64
    assert silver.schema["england_wales_demand"] == pl.Float64
    early = silver.filter(pl.col("bronze_capture_id") == ids["demanddata_2001.csv"])
    assert "NA" in early["tsd"].to_list()
    assert early["nd"].null_count() == 0


def test_t_h10_f_latest_serves_every_resource_and_no_overlap(data: Path) -> None:
    """Detects the per-family ``LIMIT 1`` (one year surviving H1) in the catalogue or in
    Polars, and a false overlap among the disjoint yearly resources."""
    ids = _run_all(data)
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    con = duckdb.connect(str(db), read_only=True)
    try:
        latest = con.execute(f'SELECT * FROM "silver_{SOURCE}_{KEY}_latest"').pl()
    finally:
        con.close()
    assert set(latest["bronze_capture_id"].to_list()) == set(ids.values())
    files = sorted((data / "silver" / SOURCE / KEY).rglob("[!.]*.parquet"))
    polars = select_latest_vintage(
        pl.scan_parquet(files, hive_partitioning=False),
        LATEST_VIEW_SPECS[(SOURCE, KEY)],
        completions=scan_completions(data),
    ).collect()
    assert sorted(polars["bronze_capture_id"].to_list()) == sorted(
        latest["bronze_capture_id"].to_list()
    )
    assert latest.height == sum(_rows(body(filename)) for filename in SIDECARS)
    report = reconcile(data, registry_module.load_registry(), [KEY], DAY)
    assert report.clean, report.lines()
    assert "SUMMARY overlap 0" in report.lines()


def test_t_h10_g_the_record_is_held_on_na_and_partitioned_per_resource() -> None:
    """Detects the family published before NESO defines ``NA``, a reworded hold
    question, and a record shape drifting from P-6 (key, selection, vintage, epochs).

    The committed-record contract is read in a fresh interpreter (repo rule: registry
    tests are subprocess-driven), so nothing collection imported can mask a load failure.
    """
    code = textwrap.dedent(
        f"""
        import json
        from gridflow.connectors.neso_data_portal.registry import load_registry
        record = load_registry().families[{KEY!r}][1].record
        assert record is not None
        print(json.dumps({{
            "status": record.eligibility.status,
            "unit": record.eligibility.unit,
            "question": record.eligibility.question,
            "entity_key": list(record.entity_key),
            "latest": record.latest,
            "latest_partition": record.latest_partition,
            "vintage": record.vintage,
            "temporal": record.temporal.kind,
            "epochs": len(record.epochs),
            "issues": sorted({{e.issue.kind for e in record.epochs}}),
        }}))
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
    assert result.returncode == 0, "stdout:\n" + result.stdout + "\nstderr:\n" + result.stderr
    record = json.loads(result.stdout)
    assert record["status"] == "held"
    assert record["unit"] == "E-SEM"
    assert record["question"] == QUESTION
    assert record["entity_key"] == ["resource_id", "settlement_date", "settlement_period"]
    assert record["latest"] == "whole_capture"
    assert record["latest_partition"] == "resource_id"
    assert record["vintage"] == "ckan_last_modified"
    assert record["temporal"] == "sp_pair"
    assert record["epochs"] == 4
    assert record["issues"] == ["none"]


MEASURED_NA_YEARS = frozenset(range(2001, 2009))
"""The ``historic_demand`` resources whose body holds a literal ``NA`` cell, measured by
streaming all 26 bronze bodies of the 2026-10-08 sweep (v0.22-K-GEN-1, RULINGS 539): each of
2001-2008 holds literal ``NA`` cells (in the hold's columns, some only in part of those years)
and none of 2009-2026 does. DEM-1H's docs check had measured 2001-2008; the hold had also named
2023-2025."""


def test_hold_question_names_exactly_the_measured_literal_na_years() -> None:
    """Detects the hold naming a year range with no literal ``NA``: the question's years
    (every ``yyyy-yyyy`` range it states) are exactly the measured set, and the committed
    fixtures agree where they overlap (2001 holds literal ``NA`` cells; the 2009, 2019, 2023,
    2024, 2025 and 2026 slices hold none)."""
    stated: set[int] = set()
    for first, last in re.findall(r"(\d{4})-(\d{4})", QUESTION):
        stated |= set(range(int(first), int(last) + 1))
    assert stated == MEASURED_NA_YEARS
    for filename in SIDECARS:
        has_na = any(
            cell == "NA"
            for row in csv.reader(io.StringIO(body(filename).decode("utf-8-sig"), newline=""))
            for cell in row
        )
        assert has_na == (_year(filename) in MEASURED_NA_YEARS), filename
