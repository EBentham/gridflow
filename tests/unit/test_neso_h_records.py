"""The system frequency rendering families through their committed records (v0.22-H, ADR-043).

Package ``system-frequency-data`` keeps its record-less bronze home ``system_frequency`` and
gains five sibling-fed families, one per rendering and body format (P-1):

- ``system_frequency_dmy_csv`` (2014, day-first, no offset): ``dtm`` stays a raw string and the
  output is held (H4); October 2014's repeated hour fails its key and is adjudicated (P-6);
- ``system_frequency_offset_csv`` / ``_offset_zip`` (2015-2019): ``%z`` offsets in every row;
- ``system_frequency_gmt_zip`` / ``_gmt_csv`` (2020 on): NESO's GMT statement, with August
  2026's date-only midnights typed through ``alt_formats``.

The four typed families form one overlap group, so reconcile checks the month edges across the
family boundaries (ratification note 2). Every transform test writes captures into
``bronze/neso_data_portal/system_frequency/2026/10/08/`` of a short data root with the real
sidecar identity, and runs the transformer the **committed registry** generates; fixture bodies
are cut byte-exact from the 2026-10-08 bronze (``tests/fixtures/neso_data_portal/h/``,
``PROVENANCE.md``). On master the five families do not exist, so ``get_transformer`` raises.
"""

from __future__ import annotations

import io
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Held, RegistryError, load_registry
from gridflow.silver.latest_views import (
    LATEST_VIEW_SPECS,
    latest_select_sql,
    select_latest_vintage,
)
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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "h"
DAY = date(2026, 10, 8)
PACKAGE = "system-frequency-data"
PACKAGE_ID = "cb1cc925-ecd8-4406-b021-3a3f368196e1"
HOME = "system_frequency"
DMY = "system_frequency_dmy_csv"
OFFSET_CSV = "system_frequency_offset_csv"
OFFSET_ZIP = "system_frequency_offset_zip"
GMT_ZIP = "system_frequency_gmt_zip"
GMT_CSV = "system_frequency_gmt_csv"
TYPED = (OFFSET_CSV, OFFSET_ZIP, GMT_ZIP, GMT_CSV)
FIVE = (DMY, *TYPED)
Q = (
    "TODO: dtm: the zone, period start/end and clock of these datetimes are undocumented "
    "(local_instant is forbidden until evidenced). The 2014 day-first labels carry no offset "
    "and NESO's GMT statement starts January 2020; the labels 26/10/2014 01:00:00-01:59:59 "
    "repeat with different readings and are told apart only by dtm_occurrence (body order), and "
    "the December 2014 to January 2015 edge cannot be overlap-checked until the zone is "
    "documented."
)
OCTOBER_2014 = (
    "bronze/neso_data_portal/system_frequency/2026/10/08/"
    "raw_20261008T112815Z_32f82531-81ad-43b9-99a4-ca42b3d7aa59_96c5213b.csv"
)


@dataclass(frozen=True)
class Member:
    """One real resource of the package: its sidecar identity and, for a ZIP, its member."""

    resource_id: str
    filename: str
    ckan_last_modified: str
    member: str | None = None


MEMBERS: dict[str, Member] = {
    "2014-01": Member(
        "0cd6ff2e-62ca-4fbf-8417-18b3539530e6", "f-2014-1.csv", "2023-03-24T15:08:11.628448"
    ),
    "2014-10": Member(
        "32f82531-81ad-43b9-99a4-ca42b3d7aa59", "f-2014-10.csv", "2023-03-24T16:52:41.911424"
    ),
    "2015-03": Member(
        "0f008751-3f0b-4353-9d4a-72f208221532", "f-2015-3.csv", "2023-03-24T13:07:55.362409"
    ),
    "2018-12": Member(
        "67f7a127-e352-4c09-b403-d44f2740e510", "f-2018-12.csv", "2023-03-24T11:14:03.249349"
    ),
    "2019-01": Member(
        "2a653f90-7948-4203-a49e-8763733debb2",
        "f-2019-1.zip",
        "2020-02-14T18:16:02.192605",
        "f 2019 1.csv",
    ),
    "2019-12": Member(
        "f0933bdd-1b0e-4dd3-aa7f-5498df1ba5b9",
        "f-2019-12.zip",
        "2020-02-14T18:28:17.366499",
        "f 2019 12.csv",
    ),
    "2020-01": Member(
        "e1eb079f-abab-47c8-b59d-d558b3399796",
        "fnew-2020-1.zip",
        "2020-06-11T10:56:50.039159",
        "fNew 2020 1.csv",
    ),
    "2022-03": Member(
        "182081de-9773-4b5b-9c87-17adbd1576e6",
        "fnew-2022-3.zip",
        "2022-04-06T13:39:38.778650",
        "fNew 2022 3.csv",
    ),
    "2022-04": Member(
        "b3bbd2e7-50b1-49cb-8326-bc8f09003cde", "fnew-2022-4.csv", "2022-05-11T11:15:42.897749"
    ),
    "2023-01": Member(
        "5da35567-ca7f-4127-861c-cc07d488fd2d", "fnew-2023-1.csv", "2023-02-03T15:34:50.636559"
    ),
    "2023-02": Member(
        "7a9b6d47-b2fd-4efe-bb41-f6e227b0786a", "fnew-2023-2.csv", "2023-03-02T10:08:01.963372"
    ),
    "2023-10": Member(
        "530d18c9-01bc-4620-9911-0d03c7152081", "fnew-2023-10.csv", "2023-11-02T15:29:22.324236"
    ),
    "2026-08": Member(
        "758fa76d-b645-4532-a532-ab49335ee4c3", "fnew-2026-8.csv", "2026-09-09T13:16:21.702788"
    ),
}


def _short_base() -> str:
    """The drive root on Windows (ADR-036: engine names pass MAX_PATH there); tmp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root; gold views are out of scope for this catalogue."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="h1", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def fixture(name: str) -> bytes:
    """A fixture body re-terminated with the bronze original's CRLF (git may normalise it)."""
    raw = (FIXTURES / name).read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n")


def zipped(member: str, body: bytes) -> bytes:
    """A deflated ZIP holding ``body`` as its one member, the real archives' shape."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, body)
    return buffer.getvalue()


def rows(*lines: str) -> bytes:
    """A ``dtm,f`` body from ``"<dtm>,<f>"`` lines, CRLF-terminated."""
    return b"dtm,f\r\n" + b"".join(f"{line}\r\n".encode() for line in lines)


_STAMPS: dict[Path, int] = {}


def capture(
    data: Path,
    month: str,
    body: bytes,
    *,
    resource_id: str | None = None,
    ckan_last_modified: str | None = None,
    as_zip: bool | None = None,
) -> str:
    """Write one capture of ``month``'s real resource (``body`` is the CSV text; a ZIP
    resource's body is zipped under its real member name); return its capture id."""
    member = MEMBERS[month]
    count = _STAMPS[data] = _STAMPS.get(data, 0) + 1
    zip_body = member.member is not None if as_zip is None else as_zip
    raw = zipped(member.member or "m.csv", body) if zip_body else body
    name = load_registry().resources[member.resource_id][1].name
    path, _sidecar = write_capture(
        data,
        HOME,
        package_slug=PACKAGE,
        package_id=PACKAGE_ID,
        resource_id=resource_id or member.resource_id,
        resource_name=name,
        resource_filename=member.filename,
        body=raw,
        written_at=datetime(2026, 10, 8, 11, 0, count, tzinfo=UTC),
        ckan_last_modified=ckan_last_modified or member.ckan_last_modified,
        partition=DAY,
        extension="zip" if member.filename.endswith(".zip") else "csv",
    )
    return capture_id_for(path, data)


def run(data: Path, key: str) -> int:
    return get_transformer(SOURCE, key, data).run(DAY, run_id="r")


def silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    assert files, key
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def no_silver(data: Path, key: str) -> bool:
    return not list((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))


def record_of(key: str) -> SchemaRecord:
    rec = load_registry().families[key][1].record
    assert rec is not None, key
    return rec


def body_cells(raw: bytes) -> list[tuple[str, str]]:
    lines = raw.decode("utf-8").split("\r\n")[1:]
    return [(line.split(",")[0], line.split(",")[1]) for line in lines if line]


def utc(*parts: int) -> datetime:
    return datetime(*parts, tzinfo=UTC)  # type: ignore[misc]


def failure_names(info: pytest.ExceptionInfo[NesoCaptureFailedError]) -> list[str]:
    return [cls for _capture, cls, _message in info.value.failures]


def overlap_gaps(report: Any) -> list[Any]:
    return [gap for gap in report.gaps if gap.category == "overlap"]


def _query(db: Path, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, params).pl() if params else con.execute(sql).pl()
    finally:
        con.close()


def both_as_of(db: Path, data: Path, key: str, as_of: datetime | None) -> pl.DataFrame:
    """``_latest`` (or the parameterised select) = Polars ``select_latest_vintage``; rows."""
    view = f"silver_{SOURCE}_{key}"
    spec = LATEST_VIEW_SPECS[(SOURCE, key)]
    if as_of is None:
        sql = _query(db, f'SELECT * FROM "{view}_latest"')
    else:
        columns = set(_query(db, f'SELECT * FROM "{view}" LIMIT 0').columns)
        select = latest_select_sql(view, spec, columns, as_of_param=True)
        assert select is not None
        sql = _query(db, select, {"as_of": as_of.isoformat()})
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    lf = pl.scan_parquet(files, hive_partitioning=False)
    polars = select_latest_vintage(lf, spec, as_of, completions=scan_completions(data)).collect()
    shape = ["dtm", "f", "bronze_capture_id"]
    left = sql.select(shape).with_columns(pl.col("dtm").dt.convert_time_zone("UTC")).sort(shape)
    right = polars.select(shape).sort(shape)
    assert left.rows() == right.rows(), (key, as_of)
    return right


# --------------------------------------------------------------------------- #
# T-F7: a revised historical month (H1-H3, ruling 7; FM-3)
# --------------------------------------------------------------------------- #


class TestRevisedMonth:
    def test_t_f7_a_revision_replaces_its_month_whole_and_as_of_sees_the_original(
        self, data: Path
    ) -> None:
        """Detects ``_latest = base`` or an instant-keyed selection (the revision's dropped
        second 1 must not reappear), a revision leaking into another month, an as-of read
        between the uploads seeing the revision, and the SQL view and the Polars renderer
        disagreeing. January 2023 (original at 2026-01-01, revision at 2026-02-01 dropping
        second 1 and changing second 0) and February 2023 (2026-01-01), committed
        ``system_frequency_gmt_csv`` record."""
        original = capture(
            data,
            "2023-01",
            rows(
                "2023-01-01 00:00:00,50.0", "2023-01-01 00:00:01,50.1", "2023-01-01 00:00:02,50.2"
            ),
            ckan_last_modified="2026-01-01T00:00:00",
        )
        revision = capture(
            data,
            "2023-01",
            rows("2023-01-01 00:00:00,51.0", "2023-01-01 00:00:02,50.2"),
            ckan_last_modified="2026-02-01T00:00:00",
        )
        february = capture(
            data,
            "2023-02",
            rows("2023-02-01 00:00:00,49.9", "2023-02-01 00:00:01,50.0"),
            ckan_last_modified="2026-01-01T00:00:00",
        )
        assert run(data, GMT_CSV) == 7
        base = silver(data, GMT_CSV)
        assert base.height == 7
        assert sorted(set(base["bronze_capture_id"])) == sorted({original, revision, february})

        db = data / "cat.duckdb"
        init_catalogue(db, data)
        latest = both_as_of(db, data, GMT_CSV, None)
        assert sorted(latest.rows()) == sorted(
            [
                (utc(2023, 1, 1), 51.0, revision),
                (utc(2023, 1, 1, 0, 0, 2), 50.2, revision),
                (utc(2023, 2, 1), 49.9, february),
                (utc(2023, 2, 1, 0, 0, 1), 50.0, february),
            ]
        )
        assert utc(2023, 1, 1, 0, 0, 1) not in latest["dtm"].to_list()
        before = both_as_of(db, data, GMT_CSV, utc(2026, 1, 11))
        assert sorted(before["bronze_capture_id"].to_list()) == sorted(
            [original] * 3 + [february] * 2
        )
        views = _query(
            db,
            "SELECT sql FROM duckdb_views() WHERE view_name = $name",
            {"name": f"silver_{SOURCE}_{GMT_CSV}_latest"},
        )
        assert views.height == 1
        assert "$as_of" not in views["sql"][0]


# --------------------------------------------------------------------------- #
# T-F8: month-edge duplicates within one family (FM-8, FM-14)
# --------------------------------------------------------------------------- #


class TestMonthEdgeWithinFamily:
    def test_t_f8_a_a_shared_edge_instant_is_kept_twice_and_reported(self, data: Path) -> None:
        """Detects a month-edge duplicate deduplicated (one month's row lost) or passing
        silently: February's first row carries January's last instant; both rows survive in
        base and ``_latest`` and reconcile reports ``overlap`` on both captures (not passed,
        so the CLI exits non-zero)."""
        january = capture(data, "2023-01", rows("2023-01-31 23:59:59,50.0"))
        february = capture(
            data, "2023-02", rows("2023-01-31 23:59:59,50.1", "2023-02-01 00:00:00,50.2")
        )
        run(data, GMT_CSV)
        assert silver(data, GMT_CSV).height == 3
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        latest = both_as_of(db, data, GMT_CSV, None)
        assert latest.filter(pl.col("dtm") == utc(2023, 1, 31, 23, 59, 59)).height == 2
        report = reconcile(data, load_registry(), list(TYPED), DAY)
        assert sorted(gap.capture_id for gap in overlap_gaps(report)) == sorted([january, february])
        assert {gap.family for gap in overlap_gaps(report)} == {GMT_CSV}
        assert not report.passed

    def test_t_f8_b_a_month_reuploaded_under_a_new_uuid_overlaps(self, data: Path) -> None:
        """Detects a re-upload under a new resource UUID (resolved by the registered name)
        shadowing or being shadowed by the old resource without a report (FM-14)."""
        old = capture(data, "2023-01", rows("2023-01-01 00:00:00,50.0"))
        new = capture(
            data,
            "2023-01",
            rows("2023-01-01 00:00:00,50.1"),
            resource_id="5da35567-ca7f-4127-861c-cc07d48800ff",
        )
        run(data, GMT_CSV)
        completion = read_completion(data, GMT_CSV, new)
        assert completion is not None
        assert completion["resource_id"] == "5da35567-ca7f-4127-861c-cc07d48800ff"
        report = reconcile(data, load_registry(), list(TYPED), DAY)
        assert sorted(gap.capture_id for gap in overlap_gaps(report)) == sorted([old, new])
        assert not report.passed


# --------------------------------------------------------------------------- #
# T-F9: renderings through the committed records (H4, H5; FM-4, FM-6, FM-7)
# --------------------------------------------------------------------------- #


def _copied_registry(directory: Path) -> Path:
    """A copy of the committed registry directory (package files and the three ledgers)."""
    source = Path(registry_module.__file__).parent
    directory.mkdir(parents=True)
    for path in source.glob("*.json"):
        shutil.copyfile(path, directory / path.name)
    return directory


class TestRenderings:
    def test_t_f9_a_day_first_labels_stay_raw_and_held(self, data: Path) -> None:
        """Detects an instant derived from an unevidenced zone (H4): the 2014 day-first
        ``dtm`` is byte-equal to the body, ``timestamp_utc`` is the capture time, and the
        output is held with unit ``H-R1`` and the verbatim question."""
        raw = fixture("f-2014-1.csv")
        capture(data, "2014-01", raw)
        run(data, DMY)
        frame = silver(data, DMY).sort("dtm")
        assert frame.schema["dtm"] == pl.Utf8
        assert list(zip(frame["dtm"], frame["f"].cast(pl.Utf8), strict=True)) == [
            (cell, str(float(value))) for cell, value in body_cells(raw)
        ]
        assert frame["timestamp_utc"].to_list() == frame["capture_written_at"].to_list()
        package, family = load_registry().families[DMY]
        eligibility = effective_eligibility(package, family)
        assert isinstance(eligibility, Held)
        assert eligibility.unit == "H-R1"
        assert eligibility.question == Q

    def test_t_f9_b_october_2014s_repeated_hour_loads_keyed_by_its_occurrence(
        self, data: Path
    ) -> None:
        """Detects the repeated 26/10/2014 01:00:00 label failing the capture (20ao), merged,
        deduplicated or resolved to an instant: both rows load with their own readings, the raw
        label kept, ``dtm_occurrence`` 1 then 2 in body order, every other label 1, and the key
        (resource_id, dtm, dtm_occurrence) unique; no adjudication entry remains."""
        raw = fixture("f-2014-10.csv")
        capture_id = capture(data, "2014-10", raw)
        assert run(data, DMY) == len(body_cells(raw))
        assert read_completion(data, DMY, capture_id) is not None
        frame = silver(data, DMY)
        assert frame.schema["dtm"] == pl.Utf8
        assert frame.schema["dtm_occurrence"] == pl.Int64
        repeated = frame.filter(pl.col("dtm") == "26/10/2014 01:00:00").sort("dtm_occurrence")
        assert repeated.select("dtm_occurrence", "f").rows() == [(1, 50.029), (2, 49.94)]
        others = frame.filter(pl.col("dtm") != "26/10/2014 01:00:00")
        assert others["dtm_occurrence"].to_list() == [1, 1, 1]
        assert frame.select("resource_id", "dtm", "dtm_occurrence").is_duplicated().sum() == 0
        assert not [e for e in registry_module.load_reconcile_adjudications() if e.family == DMY]

    def test_t_f9_c_offsets_convert_across_the_dst_edge_and_a_missing_one_fails(
        self, data: Path
    ) -> None:
        """Detects an offset relabelled as UTC or dropped: the March 2015 rows at the spring
        change (``+0000`` then ``+0100``) type to 00:59:58Z, 00:59:59Z, 01:00:00Z; the same
        cut with one ``+0100`` removed fails the capture (FM-6)."""
        raw = fixture("f-2015-3.csv")
        assert b" +0000," in raw and b" +0100," in raw
        capture(data, "2015-03", raw)
        run(data, OFFSET_CSV)
        instants = silver(data, OFFSET_CSV).sort("dtm")["dtm"].to_list()
        assert instants[-3:] == [
            utc(2015, 3, 29, 0, 59, 58),
            utc(2015, 3, 29, 0, 59, 59),
            utc(2015, 3, 29, 1, 0, 0),
        ]
        assert instants[0] == utc(2015, 3, 1)

    def test_t_f9_c_a_row_without_its_offset_fails_the_capture(self, data: Path) -> None:
        """FM-6: an R2 cell without an offset never parses as naive."""
        stripped = fixture("f-2015-3.csv").replace(b" +0100,", b",", 1)
        capture_id = capture(data, "2015-03", stripped)
        with pytest.raises(NesoCaptureFailedError) as info:
            run(data, OFFSET_CSV)
        assert len(info.value.failures) == 1
        assert read_completion(data, OFFSET_CSV, capture_id) is None
        assert no_silver(data, OFFSET_CSV)

    def test_t_f9_d_a_zip_member_reads_equal_to_the_same_bytes_as_a_csv_body(
        self, data: Path
    ) -> None:
        """Detects the ZIP path typing differently from the CSV path: December 2019's member
        cut under its ZIP resource equals the same bytes as a CSV body under a 2015-2018
        resource, on ``(dtm, f)``."""
        raw = fixture("f 2019 12.csv")
        capture(data, "2019-12", raw)
        capture(data, "2018-12", raw)
        run(data, OFFSET_ZIP)
        run(data, OFFSET_CSV)
        via_zip = silver(data, OFFSET_ZIP).select("dtm", "f").sort("dtm")
        via_csv = silver(data, OFFSET_CSV).select("dtm", "f").sort("dtm")
        assert via_zip.height == 5
        assert via_zip.equals(via_csv)
        assert silver(data, OFFSET_ZIP)["child_id"].unique().to_list() == ["f 2019 12.csv"]

    def test_t_f9_e_gmt_zip_members_type_as_utc(self, data: Path) -> None:
        """Detects a local-zone reading of the GMT-stated January 2020 member."""
        capture(data, "2020-01", fixture("fNew 2020 1.csv"))
        run(data, GMT_ZIP)
        assert silver(data, GMT_ZIP).sort("dtm")["dtm"][0] == utc(2020, 1, 1)

    def test_t_f9_f_gmt_csv_types_without_a_bst_shift_and_keeps_date_only_midnights(
        self, data: Path
    ) -> None:
        """Detects a BST shift of October 2023 (still summer time on the 1st) and August
        2026's date-only first row excluded or failed (ruling 6): it types to midnight UTC
        with zero exclusions."""
        october = capture(data, "2023-10", fixture("fnew-2023-10.csv"))
        august = capture(data, "2026-08", fixture("fnew-2026-8.csv"))
        run(data, GMT_CSV)
        frame = silver(data, GMT_CSV)
        by_capture = {
            key: frame.filter(pl.col("bronze_capture_id") == key).sort("dtm")["dtm"].to_list()
            for key in (october, august)
        }
        assert by_capture[october][0] == utc(2023, 10, 1)
        assert by_capture[august] == [
            utc(2026, 8, 1),
            utc(2026, 8, 1, 0, 0, 1),
            utc(2026, 8, 1, 0, 0, 2),
        ]
        for capture_id in (october, august):
            completion = read_completion(data, GMT_CSV, capture_id)
            assert completion is not None
            assert completion["rows_excluded"] == 0
            assert completion["row_count"] == 3

    @pytest.mark.parametrize(
        ("key", "month", "name"),
        [
            (DMY, "2014-01", "f-2014-1.csv"),
            (OFFSET_CSV, "2015-03", "f-2015-3.csv"),
            (OFFSET_ZIP, "2019-12", "f 2019 12.csv"),
            (GMT_ZIP, "2020-01", "fNew 2020 1.csv"),
            (GMT_CSV, "2026-08", "fnew-2026-8.csv"),
        ],
        ids=["dmy", "offset-csv", "offset-zip", "gmt-zip", "gmt-csv"],
    )
    def test_t_f9_g_every_reading_is_the_bodys_float(
        self, data: Path, key: str, month: str, name: str
    ) -> None:
        """Detects ``f`` typed other than ``Float64`` or a value altered on the way."""
        raw = fixture(name)
        capture(data, month, raw)
        run(data, key)
        frame = silver(data, key)
        assert frame.schema["f"] == pl.Float64
        assert sorted(frame["f"].to_list()) == sorted(float(v) for _c, v in body_cells(raw))


# --------------------------------------------------------------------------- #
# T-F10: capture failure accounting (FM-16)
# --------------------------------------------------------------------------- #


class TestFailureAccounting:
    def test_t_f10_a_a_zip_body_under_a_csv_resource_is_a_recorded_failure(
        self, data: Path
    ) -> None:
        """Detects a ZIP body read as CSV yielding zero rows instead of a failure (today's
        state for the 39 ZIP bodies): under a childless ``csv`` resource it fails, writes no
        output, and reconcile reports ``failed`` with the pinned cause."""
        body = zipped("fNew 2022 4.csv", fixture("fnew-2023-10.csv"))
        capture_id = capture(data, "2022-04", body, as_zip=False)
        with pytest.raises(NesoCaptureFailedError) as info:
            run(data, GMT_CSV)
        assert failure_names(info) == ["UnicodeDecodeError"]
        assert read_completion(data, GMT_CSV, capture_id) is None
        assert no_silver(data, GMT_CSV)
        report = reconcile(data, load_registry(), [GMT_CSV], DAY)
        assert [(g.category, g.capture_id, g.cause) for g in report.gaps] == [
            ("failed", capture_id, "UnicodeDecodeError")
        ]

    def test_t_f10_b_a_csv_body_under_a_zip_resource_is_a_recorded_failure(
        self, data: Path
    ) -> None:
        """Detects a plain CSV body under a child-bearing ZIP resource read as anything."""
        capture_id = capture(data, "2020-01", fixture("fNew 2020 1.csv"), as_zip=False)
        with pytest.raises(NesoCaptureFailedError) as info:
            run(data, GMT_ZIP)
        assert len(info.value.failures) == 1
        assert failure_names(info) == ["ContainerReadError"]
        assert read_completion(data, GMT_ZIP, capture_id) is None
        assert no_silver(data, GMT_ZIP)

    def test_t_f10_c_a_zip_resource_moved_to_a_csv_family_is_refused(self, tmp_path: Path) -> None:
        """Detects the registry accepting ZIP children on a ``csv`` family (V-15)."""
        directory = _copied_registry(tmp_path / "registry")
        path = directory / f"{PACKAGE}.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        moved = next(r for r in document["resources"] if r["id"] == MEMBERS["2022-03"].resource_id)
        moved["disposition"] = {"kind": "SILVER", "key": GMT_CSV}
        for child in moved["children"]:
            child["disposition"] = {"kind": "SILVER", "key": GMT_CSV}
        path.write_text(registry_module.dump_json(document), encoding="utf-8")
        with pytest.raises(RegistryError, match="V-15"):
            registry_module.load_registry(directory)


# --------------------------------------------------------------------------- #
# T-F11: registry and eligibility pins (H6)
# --------------------------------------------------------------------------- #


def _assert_ok(code: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "OK" in result.stdout, result.stdout


class TestRegistryPins:
    def test_t_f11_the_package_layout_records_and_ledger(self) -> None:
        """Detects any drift from P-1/P-2/P-6: the bronze home record-less, the five
        sibling-fed records exactly as specified, each resource on its rendering family by
        name month (12/48/12/27/53, all SILVER), the ZIP families holding exactly the 39
        child-bearing resources with each child on the same key, and P-6's entry verbatim
        and backed (in a fresh interpreter)."""
        _assert_ok(
            f"""
            import re
            from gridflow.connectors.neso_data_portal.registry import (
                load_registry, load_reconcile_adjudications, reconcile_adjudication_problems,
            )
            registry = load_registry()
            package, home = registry.families['system_frequency']
            assert home.record is None
            months = ('January February March April May June July August September October '
                      'November December').split()
            ranges = {{
                '{DMY}': ((2014, 1), (2014, 12), 12),
                '{OFFSET_CSV}': ((2015, 1), (2018, 12), 48),
                '{OFFSET_ZIP}': ((2019, 1), (2019, 12), 12),
                '{GMT_ZIP}': ((2020, 1), (2022, 3), 27),
                '{GMT_CSV}': ((2022, 4), (2026, 8), 53),
            }}
            keys = [f.key for f in package.families]
            assert keys == ['system_frequency', *ranges], keys
            seen = {{key: 0 for key in ranges}}
            for resource in package.resources:
                assert resource.family == 'system_frequency'
                match = re.match(r'({{}}) (\\d{{{{4}}}}) '.format('|'.join(months)), resource.name)
                month = (int(match.group(2)), months.index(match.group(1)) + 1)
                (key,) = [k for k, (lo, hi, _n) in ranges.items() if lo <= month <= hi]
                assert resource.disposition.kind == 'SILVER', resource
                assert resource.disposition.key == key, (resource.name, key)
                if key.endswith('_zip'):
                    assert len(resource.children) == 1, resource.name
                    assert resource.children[0].disposition.kind == 'SILVER'
                    assert resource.children[0].disposition.key == key
                else:
                    assert not resource.children, resource.name
                seen[key] += 1
            assert seen == {{key: n for key, (_lo, _hi, n) in ranges.items()}}, seen
            for key in ranges:
                rec = registry.families[key][1].record
                assert rec.siblings == ('system_frequency',)
                # 2: the 2014 record gained dtm_occurrence (20ao), so its outputs re-transform.
                assert rec.version == ('2' if key == '{DMY}' else '1') and rec.encoding == 'utf-8'
                assert rec.entity_key == (
                    ('resource_id', 'dtm', 'dtm_occurrence') if key == '{DMY}'
                    else ('resource_id', 'dtm'))
                assert rec.occurrence_column == ('dtm_occurrence' if key == '{DMY}' else None)
                assert rec.latest == 'whole_capture' and rec.latest_partition == 'resource_id'
                assert rec.vintage == 'ckan_last_modified'
                (epoch,) = rec.epochs
                assert epoch.header == ('dtm', 'f') and epoch.issue.kind == 'none'
                dtm, f = epoch.columns
                assert (f.source, f.name, f.dtype, f.nullable) == ('f', 'f', 'float64', True)
                assert f.min is None and f.max is None and not f.null_tokens
                assert not dtm.nullable and not dtm.null_tokens
            dmy = registry.families['{DMY}'][1].record
            assert dmy.reader == 'csv' and dmy.temporal.kind == 'none'
            assert dmy.epochs[0].columns[0].dtype == 'string' and dmy.overlap_group is None
            assert dmy.eligibility.status == 'held' and dmy.eligibility.unit == 'H-R1'
            assert dmy.eligibility.question == {Q!r}
            for key in {list(TYPED)!r}:
                rec = registry.families[key][1].record
                dtm = rec.epochs[0].columns[0]
                assert rec.temporal.kind == 'utc_instant' and rec.temporal.column == 'dtm'
                assert rec.overlap_group == 'system_frequency' and rec.eligibility is None
                if 'offset' in key:
                    assert (dtm.format, dtm.zone, dtm.alt_formats) == (
                        '%Y-%m-%d %H:%M:%S %z', None, None)
                else:
                    assert (dtm.format, dtm.zone, dtm.alt_formats) == (
                        '%Y-%m-%d %H:%M:%S', 'UTC', ('%Y-%m-%d',))
                if key.endswith('_zip'):
                    assert rec.reader == 'zip_member' and rec.zip_member.inner == 'csv'
                    prefix = 'f' if 'offset' in key else 'fNew'
                    assert rec.zip_member.member_pattern == (
                        prefix + r' \\d{{4}} (?:[1-9]|1[0-2])\\.csv')
                else:
                    assert rec.reader == 'csv' and rec.zip_member is None
            entries = load_reconcile_adjudications()
            assert not [e for e in entries if e.family == '{DMY}']
            assert reconcile_adjudication_problems(registry, entries) == []
            print('OK')
            """
        )

    def test_t_f11_generated_artefacts_are_current_and_show_the_hold(self) -> None:
        """Detects a registry commit without regenerated ``sources.yaml`` or eligibility
        report, and the report not showing the 2014 family held with its question."""
        for module in (
            ["gridflow.connectors.neso_data_portal.eligibility", "--check"],
            ["gridflow.connectors.neso_data_portal.registry", "yaml", "--check"],
        ):
            result = subprocess.run(
                [sys.executable, "-m", *module],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=300,
                check=False,
            )
            assert result.returncode == 0, (module, result.stdout, result.stderr)
        report = (PROJECT_ROOT / "docs" / "neso_data_portal" / "eligibility.md").read_text(
            encoding="utf-8"
        )
        (line,) = [row for row in report.splitlines() if f"`{DMY}`" in row]
        assert "held" in line and "H-R1" in line
        assert " ".join(Q.split()) in line


# --------------------------------------------------------------------------- #
# T-F12: format-boundary overlap checks (ratification note 2; FM-9, FM-10)
# --------------------------------------------------------------------------- #

EDGES = {
    # earlier (month, key, last real row)   later (month, key, first real row, dup row)
    "dec2018-jan2019": (
        ("2018-12", OFFSET_CSV, "2018-12-31 23:59:59 +0000,50.11"),
        ("2019-01", OFFSET_ZIP, "2019-01-01 00:00:00 +0000,50.107", "2018-12-31 23:59:59 +0000"),
    ),
    "dec2019-jan2020": (
        ("2019-12", OFFSET_ZIP, "2019-12-31 23:59:59 +0000,50.117"),
        ("2020-01", GMT_ZIP, "2020-01-01 00:00:00,50.115", "2019-12-31 23:59:59"),
    ),
    "mar2022-apr2022": (
        ("2022-03", GMT_ZIP, "2022-03-31 23:59:59,50.09"),
        ("2022-04", GMT_CSV, "2022-04-01 00:00:00,50.087", "2022-03-31 23:59:59"),
    ),
}
"""The real last row of the earlier month and first row of the later one, read from the
2026-10-08 bronze (``tail -1`` of the CSV body or inside the ZIP member): December 2018
``f-2018-12.csv`` ``2018-12-31 23:59:59 +0000,50.11``; January 2019 ``f 2019 1.csv``
``2019-01-01 00:00:00 +0000,50.107``; December 2019 ``f 2019 12.csv``
``2019-12-31 23:59:59 +0000,50.117``; January 2020 ``fNew 2020 1.csv``
``2020-01-01 00:00:00,50.115``; March 2022 ``fNew 2022 3.csv`` ``2022-03-31 23:59:59,50.09``;
April 2022 ``fnew-2022-4.csv`` ``2022-04-01 00:00:00,50.087``."""


class TestFormatBoundaries:
    @pytest.mark.parametrize("edge", list(EDGES))
    @pytest.mark.parametrize("variant", ["clean", "dup"])
    def test_t_f12_each_format_boundary_is_overlap_checked(
        self, data: Path, edge: str, variant: str
    ) -> None:
        """Detects a family boundary the per-family check cannot see: the real edge rows are
        contiguous (``SUMMARY overlap 0``), and the later month's first row set to the earlier
        month's last instant is reported ``overlap`` in both families, both rows served."""
        (early_month, early_key, early_row), (late_month, late_key, late_row, dup) = EDGES[edge]
        if variant == "dup":
            late_row = f"{dup},{late_row.split(',')[1]}"
        early = capture(data, early_month, rows(early_row))
        late = capture(data, late_month, rows(late_row))
        run(data, early_key)
        run(data, late_key)
        report = reconcile(data, load_registry(), list(TYPED), DAY)
        if variant == "clean":
            assert report.clean, report.lines()
            assert "SUMMARY overlap 0" in report.lines()
            return
        gaps = overlap_gaps(report)
        assert {(gap.family, gap.capture_id) for gap in gaps} == {
            (early_key, early),
            (late_key, late),
        }
        by_capture = {gap.capture_id: gap.peers for gap in gaps}
        assert by_capture[early] == (late,) and by_capture[late] == (early,)
        assert silver(data, early_key)["dtm"].to_list() == silver(data, late_key)["dtm"].to_list()


# --------------------------------------------------------------------------- #
# 20ao: the occurrence column's load-time rule (V-22)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("update", "fragment"),
    [
        ({"occurrence_column": "Dtm Occurrence"}, "not a silver name"),
        ({"occurrence_column": "f"}, "collides"),
        ({"occurrence_column": "resource_id"}, "collides"),
        ({"entity_key": ("resource_id", "dtm")}, "must be in entity_key"),
    ],
    ids=["bad-name", "typed-output", "reserved", "not-in-key"],
)
def test_v22_an_occurrence_column_outside_its_rule_is_refused(
    update: dict[str, object], fragment: str
) -> None:
    """Detects an occurrence column that could shadow a vendor or engine column, or that the
    key does not carry (so repeats would still collide)."""
    from gridflow.connectors.neso_data_portal.registry.record import RecordError, validate_record

    package, family = load_registry().families[DMY]
    record = family.record
    assert record is not None
    broken = record.model_copy(update=update)
    with pytest.raises(RecordError, match=fragment):
        validate_record(
            broken,
            key=DMY,
            kind="tabular",
            legacy=False,
            package_families={f.key: f.record is not None for f in package.families},
            family_url_types=frozenset({"upload"}),
        )
