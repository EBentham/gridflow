"""The weekly wind availability frozen record (v0.22-K-GEN-2): one held family.

Every test writes a recorded fixture capture (a slice of the 2026-10-08 swept bronze,
``tests/fixtures/neso_data_portal/gen2/weeklywindavailability.csv``) into a short data root and
runs the transformer the **real package registry** generates, so a record that does not fit its
vendor body fails here, not at activation. On master the family has no record, so
``get_transformer`` raises for it.

Units (K-GEN-2-FACTS g4; a ``ColumnSpec`` has no unit field, so they are recorded here): ``MW``
is generator capacity in MW; ``BMU_ID`` is an identifier (the vendor dictionary's MW unit on it is
a vendor error) and ``Week Number`` is a raw label with no unit.

Fixture cut (a scratch script, not committed): the header, every week of four BMUs (the first,
one that is zero on every week, one that is zero on some weeks, and the last one's final 40
weeks: 505 rows) and the vendor's trailing lone ``\\r`` line. ``git`` and ``core.autocrlf``
normalise a committed fixture's line endings, so :func:`body` rebuilds the record with the bronze
original's convention (LF data lines, then one ``\\r\\n`` blank line) and no test asserts on the
fixture's raw bytes.

``metered_wind_output_monthly`` and ``wind_bmu_boa_volumes`` have no record here: RULINGS 547
moved them to unit GEN-2H, which records them (``test_neso_gen2h_records.py``); pinned below.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Held
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.completion import capture_id_for, read_completion
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
KEY = "weekly_wind_availability"
FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "neso_data_portal"
    / "gen2"
    / "weeklywindavailability.csv"
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
CKAN_MODIFIED = "2026-10-08T11:20:20.163158"
HEADER = ["BMU_ID", "Week Number", "MW"]
QUESTION = (
    "TODO: NESO does not define the `Week Number` calendar (week system, year, start day, "
    "zone), so no target week can be dated; the body also carries 155 week labels against a "
    "documented 2-52-week horizon."
)


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root; gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="g2", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def body() -> bytes:
    """The fixture terminated by the bronze original's line convention: LF data lines, then
    the vendor's one ``\\r\\n`` blank line."""
    raw = FIXTURE.read_bytes().replace(b"\r\n", b"\n")
    return raw.rstrip(b"\n") + b"\n\r\n"


def _table(raw: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8"), newline="")))


def _populated(raw: bytes) -> list[dict[str, str]]:
    """The body's rows with at least one non-blank cell (the reader drops the others)."""
    return [row for row in _table(raw) if any(value.strip() for value in row.values())]


def _record() -> SchemaRecord:
    record = registry_module.load_registry().families[KEY][1].record
    assert record is not None, KEY
    return record


def _capture(data: Path, *, raw: bytes | None = None) -> str:
    path, _sidecar = write_capture(
        data,
        KEY,
        body=raw if raw is not None else body(),
        written_at=WRITTEN,
        partition=DAY,
        package_slug="weekly-wind-availability",
        package_id="542b7b69-7fbc-412c-affc-074ef58495c8",
        resource_id="bb375594-dd0b-462b-9063-51e93c607e41",
        resource_name="Weekly Wind Availability",
        resource_filename="weeklywindavailability.csv",
        ckan_last_modified=CKAN_MODIFIED,
        url_type="upload",
    )
    return capture_id_for(path, data)


def _silver(data: Path) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / KEY).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _run(data: Path) -> str:
    """Capture the fixture and transform it; the capture id."""
    capture_id = _capture(data)
    get_transformer(SOURCE, KEY, data).run(DAY, run_id="r")
    return capture_id


def test_fixture_keeps_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the exact header, a
    non-trivial row count, zero MW cells (an all-zero BMU and a part-zero one), the vendor's
    trailing blank record and non-numeric week labels."""
    assert list(_table(body())[0]) == HEADER
    rows = _populated(body())
    assert len(rows) == 505
    assert body().endswith(b"\n\r\n")
    assert body().count(b"\n") == len(rows) + 2  # header, data lines, the blank line
    zero_bmus = {r["BMU_ID"] for r in rows if r["MW"] == "0"}
    assert {"BENBW-2", "BLLA-1"} <= zero_bmus
    assert {r["Week Number"] for r in rows if r["BMU_ID"] == "ABRBO-1"} >= {"43W26", "52W26"}


def test_fixture_types_with_no_exclusion(data: Path) -> None:
    """Detects a family without a generated transformer, a header that matches no epoch, a
    cast the vendor body does not satisfy, a clock taken from anywhere but the declared
    recipe, and any row excluded: the capture completes with all its populated rows."""
    transformer = get_transformer(SOURCE, KEY, data)
    capture_id = _capture(data)
    written = transformer.run(DAY, run_id="r")
    rows = _populated(body())
    assert written == len(rows)
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, KEY, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == len(rows)
    assert completion["rows_excluded"] == 0

    record = _record()
    frame = _silver(data)
    expected = [name for name, _type in generic.output_columns(record)]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected if c not in ("year", "month")
    ]
    assert frame.schema["bmu_id"] == pl.Utf8
    assert frame.schema["week_number"] == pl.Utf8
    assert frame.schema["mw"] == pl.Float64
    assert frame["timestamp_utc"].null_count() == 0


def test_entity_key_is_unique_and_excludes_the_value(data: Path) -> None:
    """Detects an entity key that does not identify the grain, or one that carries ``mw``
    (the proposal's draft key): the (BMU, week) pair is unique across the output, and
    ``mw`` is a value, never identity."""
    _run(data)
    silver = _silver(data)
    assert _record().entity_key == ("bmu_id", "week_number")
    assert silver.height == 505
    assert silver.select("bmu_id", "week_number").is_duplicated().sum() == 0


def test_record_shape_matches_the_unit_table() -> None:
    """Detects a record drifting from the spec: csv reader, utf-8, one header epoch exactly
    ``BMU_ID,Week Number,MW``, the three typed columns, whole-capture selection with no
    partition, temporal ``none``, issue ``none``, ``ckan_last_modified`` vintage, and no
    invented null token or bound."""
    record = _record()
    assert record.reader == "csv"
    assert record.encoding == "utf-8"
    assert len(record.epochs) == 1
    epoch = record.epochs[0]
    assert list(epoch.header) == HEADER
    assert [(c.source, c.name, c.dtype) for c in epoch.columns] == [
        ("BMU_ID", "bmu_id", "string"),
        ("Week Number", "week_number", "string"),
        ("MW", "mw", "float64"),
    ]
    for column in epoch.columns:
        assert not column.null_tokens, column.name
        assert column.min is None and column.max is None, column.name
        assert column.nullable, column.name
        assert column.format is None, column.name
    assert epoch.issue.kind == "none"
    assert record.temporal.kind == "none"
    assert record.latest == "whole_capture"
    assert record.latest_partition is None
    assert record.vintage == "ckan_last_modified"
    assert record.entity_key == ("bmu_id", "week_number")


def test_family_is_held_on_the_undefined_week_calendar() -> None:
    """Detects the family published without its hold: NESO defines no calendar for the
    ``Week Number`` label (and the body's 155 labels exceed the documented horizon), so the
    forward-target property cannot be certified (RULINGS 529); the hold is on the record, is
    unit E-SEM, and is the family's effective eligibility."""
    record = _record()
    assert isinstance(record.eligibility, Held)
    assert record.eligibility.unit == "E-SEM"
    assert record.eligibility.question == QUESTION
    package, family = registry_module.load_registry().families[KEY]
    assert effective_eligibility(package, family) == record.eligibility


def test_bmu_id_and_week_number_survive_byte_identical(data: Path) -> None:
    """Detects a normalised BMU id or a week label parsed as a number or date: ``43W26``
    stays the vendor's text (no int parse), and both columns are byte-identical and in order."""
    _run(data)
    silver = _silver(data)
    rows = _populated(body())
    assert silver["bmu_id"].to_list() == [r["BMU_ID"] for r in rows]
    assert silver["week_number"].to_list() == [r["Week Number"] for r in rows]
    assert "43W26" in set(silver["week_number"].to_list())
    # the 2027-2029 labels also stay text
    assert {"01W27", "40W29"} <= set(silver["week_number"].to_list())


def test_zero_mw_is_preserved_not_nulled(data: Path) -> None:
    """Detects a zero capacity turned into null or dropped (K-GEN-2-FACTS g5: 2,012 zero-MW
    rows, 12 BMUs zero on every week; no source separates unavailable from placeholder): every
    MW cell survives as the vendor's number, an all-zero BMU keeps all its weeks."""
    _run(data)
    silver = _silver(data)
    rows = _populated(body())
    assert silver["mw"].to_list() == [float(r["MW"]) for r in rows]
    assert silver["mw"].null_count() == 0
    zeros = sum(1 for r in rows if r["MW"] == "0")
    assert zeros == 162
    assert (silver["mw"] == 0.0).sum() == zeros
    all_zero = silver.filter(pl.col("bmu_id") == "BENBW-2")
    assert all_zero.height == 155
    assert (all_zero["mw"] == 0.0).all()


def test_no_target_instant_is_invented_from_the_week_label(data: Path) -> None:
    """Detects a target week derived from the undefined label: with temporal ``none`` the
    row's ``timestamp_utc`` is the capture's own instant for every row, and no issue column
    is emitted."""
    _run(data)
    silver = _silver(data)
    assert silver["timestamp_utc"].n_unique() == 1
    assert silver["timestamp_utc"].to_list() == silver["capture_written_at"].to_list()
    assert "issue_time" not in silver.columns


def test_blank_record_is_dropped_by_the_logged_reader_path(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects the vendor's trailing all-blank record reaching the typed output, or being
    dropped silently: the reader's blank-row path removes it and logs one INFO record naming
    it, so ``rows_excluded`` stays 0 (a blank line is not an excluded data row) and the rows
    written equal the populated rows."""
    raw = body()
    assert raw.endswith(b"\n\r\n"), "the fixture must keep the vendor's trailing blank line"
    populated = _populated(raw)
    with caplog.at_level(logging.INFO, logger="gridflow.silver.csv_bronze"):
        capture_id = _capture(data)
        transformer = get_transformer(SOURCE, KEY, data)
        written = transformer.run(DAY, run_id="r")
    assert written == len(populated)
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, KEY, capture_id)
    assert completion is not None
    assert completion["rows_excluded"] == 0
    messages = [r.getMessage() for r in caplog.records if "blank row" in r.getMessage()]
    assert len(messages) == 1, messages
    assert messages[0].startswith("dropped 1 blank row(s)")


def test_the_gen2h_families_are_recorded_by_gen2h() -> None:
    """Detects a GEN-2 family losing its record: in a fresh interpreter the two families
    RULINGS 547 moved to unit GEN-2H (``metered_wind_output_monthly`` and
    ``wind_bmu_boa_volumes``) and this unit's family all have one."""
    code = textwrap.dedent(
        """
        from gridflow.connectors.neso_data_portal.registry import load_registry
        families = load_registry().families
        for key in (
            "metered_wind_output_monthly", "wind_bmu_boa_volumes", "weekly_wind_availability"
        ):
            assert families[key][1].record is not None, key
        print("OK")
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
