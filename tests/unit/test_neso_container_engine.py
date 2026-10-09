"""The two activation families end to end through the generic engine (ADR-037 P-12).

Every X2 engine row of the unit X test matrix plus T-X2-10. Each test installs
the committed package files of the two activation packages through P-15's seam
and rebuilds the captures from the P-15 fixtures with ``write_capture`` under a
short tmp data root; nothing touches ``C:/gridflow-data``.
"""

from __future__ import annotations

import io
import json
import shutil
import tempfile
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest
from _container_support import (  # noqa: F401 - forbid_zipfile_reads is a fixture
    FIXTURES,
    Cell,
    forbid_zipfile_reads,
    workbook,
    zip_bytes,
)
from _neso_generic_support import assert_same_output, install_generated, snapshot, write_capture
from _neso_registry_support import family, package, record, resource

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    read_completion,
    read_failure,
    scan_completions,
)
from gridflow.silver.neso_data_portal.reconcile import drain, reconcile
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry import Registry

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = ROOT / FIXTURES
REGISTRY_DIR = Path(registry_module.__file__).parent
SOURCE = "neso_data_portal"
DAY = date(2026, 10, 8)
CUTOFF = date(2026, 10, 31)
WRITTEN = datetime(2026, 10, 8, 9, 16, 2, tzinfo=UTC)

CMP_KEY = "current_bsuos_cap_adjustments"
CMP_DIR = "current_bsuos_files"
CMP_PKG = "current-balancing-services-use-of-system-bsuos-data"
CMP_PKG_ID = "d6a4bf54-c63f-4014-a716-49fd3878ca52"
CMP_ID = "88dcf101-1358-4d78-934f-527484d8789d"
CMP_ENTITY = ("settlement_day", "settlement_period", "run_type")

RS_KEY = "ffr_phase2_result_summary_archive"
RS_DIR = "ffr_phase2_auction_files"
RS_PKG = "phase-2-ffr-auction-results-summary"
RS_PKG_ID = "2d649d03-fb37-46a2-ae82-9e651438b559"
RS_ID = "3928f192-97a5-4b4f-9234-0dcc9ad071a0"
RS_ENTITY = ("service", "date", "efa")
RS_FOLDER = "ResultSummary 2019-11-22 to 2019-11-29/"
RS_MEMBERS = (
    f"{RS_FOLDER}NGESO_FRA_2019-11-22_ResultSummary.csv",
    f"{RS_FOLDER}NGESO_FRA_2019-11-29_ResultSummary.csv",
)


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A data root with a short path; transformer modules imported under the real registry.

    The activation keys run to 33 characters and appear twice in an output path,
    so pytest's long ``tmp_path`` would pass Windows' 260-character MAX_PATH;
    the production root (``C:/gridflow-data``) is short (as the pilot's tests do).
    """
    pipeline_runner.import_transformers()
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(prefix="x", ignore_cleanup_errors=True) as root:
        yield Path(root)


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _package_doc(slug: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(
        (REGISTRY_DIR / f"{slug}.json").read_text(encoding="utf-8")
    )
    return document


def _install(
    monkeypatch: pytest.MonkeyPatch, data: Path, *docs: dict[str, Any], where: str = "_reg"
) -> tuple[Registry, Any]:
    return install_generated(monkeypatch, data / where, list(docs))  # type: ignore[no-any-return]


def _cmp_capture(data: Path, body: bytes, *, resource_id: str = CMP_ID) -> str:
    path, _sidecar = write_capture(
        data,
        CMP_DIR,
        package_slug=CMP_PKG,
        package_id=CMP_PKG_ID,
        resource_id=resource_id,
        resource_name="CMP381 II BSUoS Data",
        body=body,
        written_at=WRITTEN,
        ckan_last_modified="2022-04-11T16:08:00.098987",
        resource_filename="cmp381-current_ii_bsuos_110422.xlsx",
        ckan_format="XLSX",
        extension="xlsx",
        partition=DAY,
    )
    return path.relative_to(data).as_posix()


def _rs_capture(data: Path, body: bytes) -> str:
    path, _sidecar = write_capture(
        data,
        RS_DIR,
        package_slug=RS_PKG,
        package_id=RS_PKG_ID,
        resource_id=RS_ID,
        resource_name="ResultSummary 2019-11-22 to 2019-11-29",
        body=body,
        written_at=WRITTEN,
        ckan_last_modified="2021-03-31T09:22:26.071477",
        resource_filename="resultsummary-2019-11-22-to-2019-11-29.zip",
        ckan_format="ZIP",
        extension="zip",
        partition=DAY,
    )
    return path.relative_to(data).as_posix()


def _outputs(data: Path, key: str) -> list[Path]:
    return sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))


def _rs_entries(**replace: bytes) -> list[tuple[str, bytes]]:
    with zipfile.ZipFile(io.BytesIO(_fixture("result_summary.zip"))) as archive:
        entries = [
            (info.filename, archive.read(info)) for info in archive.infolist() if not info.is_dir()
        ]
    return [(name, replace.get(name, raw)) for name, raw in entries]


def _rs_body(entries: list[tuple[str, bytes]]) -> bytes:
    return zip_bytes(entries, dirs=(RS_FOLDER,))


class TestResultSummary:
    """T-X2-2: one output, both members' rows, one completion naming both."""

    def test_two_members_one_output(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _registry, generated = _install(monkeypatch, data, _package_doc(RS_PKG))
        capture_id = _rs_capture(data, _fixture("result_summary.zip"))
        assert generated.transformers[RS_KEY](data).run(DAY, run_id="r") == 168
        (output,) = _outputs(data, RS_KEY)
        frame = pl.read_parquet(output, hive_partitioning=False)
        assert frame.height == 168
        assert frame.group_by("child_id").len().sort("child_id").rows() == [
            (RS_MEMBERS[0], 84),
            (RS_MEMBERS[1], 84),
        ]
        crcs = dict(frame.select("child_id", "child_crc32").unique().rows())
        assert crcs == {RS_MEMBERS[0]: 2362721365, RS_MEMBERS[1]: 4128616702}
        assert frame.schema["date"] == pl.Date and frame.schema["efa"] == pl.Int64
        ledger = read_completion(data, RS_KEY, capture_id)
        assert ledger is not None
        assert ledger["children"] == list(RS_MEMBERS) and ledger["row_count"] == 168
        assert scan_completions(data, RS_KEY).collect().height == 1


class TestWorkbookSheets:
    """T-X2-2b: SILVER sheets are read into one output; a DOC sheet is never read."""

    def test_cmp_fixture_reads_ii_output_only(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _registry, generated = _install(monkeypatch, data, _package_doc(CMP_PKG))
        capture_id = _cmp_capture(data, _fixture("cmp381_ii.xlsx"))
        assert generated.transformers[CMP_KEY](data).run(DAY, run_id="r") == 3550
        (output,) = _outputs(data, CMP_KEY)
        frame = pl.read_parquet(output, hive_partitioning=False)
        assert frame["child_id"].unique().to_list() == ["II Output"]
        ledger = read_completion(data, CMP_KEY, capture_id)
        assert ledger is not None and ledger["children"] == ["II Output"]

    def test_synthetic_three_sheet_workbook(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def sheet(unit: str) -> dict[str, Cell]:
            return {
                "A1": "SettlementDate",
                "B1": "SettlementPeriod",
                "C1": "Unit",
                "D1": "Value",
                "A2": "2026-10-07",
                "B2": 1,
                "C2": unit,
                "D2": 1.5,
                "A3": "2026-10-07",
                "B3": 2,
                "C3": unit,
                "D3": 2.5,
            }

        body = workbook([("One", sheet("A")), ("Notes", {"A1": "read me"}), ("Two", sheet("B"))])
        rec = record(reader="xlsx", xlsx={"header_row": 1, "columns": "A:D"}, siblings=("box",))
        children = [
            {"child": "One", "disposition": {"kind": "SILVER", "key": "book"}},
            {"child": "Notes", "disposition": {"kind": "DOC"}},
            {"child": "Two", "disposition": {"kind": "SILVER", "key": "book"}},
        ]
        rid = "eeeeeeee-0000-4000-8000-000000000001"
        doc = package(
            "pkg-book",
            "eeeeeeee-0000-4000-8000-000000000000",
            [family("book", record=rec), family("box", kind="files")],
            [
                resource(
                    rid,
                    "Book",
                    "box",
                    fmt="XLSX",
                    disposition={"kind": "SILVER", "key": "book"},
                    children=children,
                )
            ],
        )
        _registry, generated = _install(monkeypatch, data, doc)
        write_capture(
            data,
            "box",
            package_slug="pkg-book",
            package_id="eeeeeeee-0000-4000-8000-000000000000",
            resource_id=rid,
            resource_name="Book",
            body=body,
            written_at=WRITTEN,
            ckan_format="XLSX",
            extension="xlsx",
            partition=DAY,
        )
        assert generated.transformers["book"](data).run(DAY, run_id="r") == 4
        (output,) = _outputs(data, "book")
        frame = pl.read_parquet(output, hive_partitioning=False)
        assert sorted(frame["child_id"].unique().to_list()) == ["One", "Two"]
        assert sorted(frame["unit"].to_list()) == ["A", "A", "B", "B"]


def _assert_failed(data: Path, key: str, capture_id: str, error_class: str) -> None:
    assert _outputs(data, key) == []
    assert read_completion(data, key, capture_id) is None
    failure = read_failure(data, key, capture_id)
    assert failure is not None and failure["error_class"] == error_class, failure


class TestLoudFailures:
    """T-X2-3, 3b, 4, 5: an inventory or member defect fails the capture loud."""

    def test_x2_3_an_unlisted_member(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _registry, generated = _install(monkeypatch, data, _package_doc(RS_PKG))
        extra = (f"{RS_FOLDER}NGESO_FRA_2019-12-06_ResultSummary.csv", b"x\n")
        capture_id = _rs_capture(data, _rs_body([*_rs_entries(), extra]))
        with pytest.raises(NesoCaptureFailedError, match="ContainerInventoryError"):
            generated.transformers[RS_KEY](data).run(DAY, run_id="r")
        _assert_failed(data, RS_KEY, capture_id, "ContainerInventoryError")

    def test_x2_3b_a_shadowed_member(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _registry, generated = _install(monkeypatch, data, _package_doc(RS_PKG))
        entries = _rs_entries()
        shadow = (entries[0][0], entries[0][1].replace(b"DLH", b"DLX"))
        capture_id = _rs_capture(data, _rs_body([*entries, shadow]))
        with pytest.raises(NesoCaptureFailedError, match="ContainerReadError"):
            generated.transformers[RS_KEY](data).run(DAY, run_id="r")
        _assert_failed(data, RS_KEY, capture_id, "ContainerReadError")

    def test_x2_4_a_missing_declared_sheet(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        doc = _package_doc(CMP_PKG)
        for res in doc["resources"]:
            if res["id"] == CMP_ID:
                res["children"].append({"child": "Ghost", "disposition": {"kind": "DOC"}})
        _registry, generated = _install(monkeypatch, data, doc)
        capture_id = _cmp_capture(data, _fixture("cmp381_ii.xlsx"))
        with pytest.raises(NesoCaptureFailedError, match="Ghost"):
            generated.transformers[CMP_KEY](data).run(DAY, run_id="r")
        _assert_failed(data, CMP_KEY, capture_id, "ContainerInventoryError")

    def test_x2_4b_an_unresolvable_resource(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _registry, generated = _install(monkeypatch, data, _package_doc(CMP_PKG))
        stranger = "ffffffff-0000-4000-8000-000000000001"
        capture_id = _cmp_capture(data, _fixture("cmp381_ii.xlsx"), resource_id=stranger)
        with pytest.raises(NesoCaptureFailedError, match="not a registry resource"):
            generated.transformers[CMP_KEY](data).run(DAY, run_id="r")
        _assert_failed(data, CMP_KEY, capture_id, "ContainerInventoryError")

    def test_x2_5_a_failing_member_leaves_the_family_incomplete(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry, generated = _install(monkeypatch, data, _package_doc(RS_PKG))
        name, raw = _rs_entries()[1]
        lines = raw.split(b"\n")
        cells = lines[1].split(b",")
        lines[1] = b",".join([*cells[:-1], b"bad" + (b"\r" if cells[-1].endswith(b"\r") else b"")])
        capture_id = _rs_capture(data, _rs_body(_rs_entries(**{name: b"\n".join(lines)})))
        with pytest.raises(NesoCaptureFailedError):
            generated.transformers[RS_KEY](data).run(DAY, run_id="r")
        assert _outputs(data, RS_KEY) == []
        assert read_completion(data, RS_KEY, capture_id) is None
        report = reconcile(data, registry, [RS_KEY], CUTOFF)
        assert [(gap.category, gap.capture_id) for gap in report.gaps] == [("failed", capture_id)]


class TestReplay:
    """T-X2-6 (B7): a replay and a drain after a silver wipe equal the first run."""

    def test_run_twice_and_drain_after_a_wipe(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry, generated = _install(
            monkeypatch, data, _package_doc(CMP_PKG), _package_doc(RS_PKG)
        )
        _cmp_capture(data, _fixture("cmp381_ii.xlsx"))
        _rs_capture(data, _fixture("result_summary.zip"))
        for key in (CMP_KEY, RS_KEY):
            generated.transformers[key](data).run(DAY, run_id="r1")
        before = {key: snapshot(data, key) for key in (CMP_KEY, RS_KEY)}
        for key in (CMP_KEY, RS_KEY):
            assert generated.transformers[key](data).run(DAY, run_id="r2") == 0
            assert_same_output(
                before[key], snapshot(data, key), CMP_ENTITY if key == CMP_KEY else RS_ENTITY
            )
        shutil.rmtree(data / "silver")
        after = drain(data, registry, [CMP_KEY, RS_KEY], CUTOFF, lambda: None)
        assert after.clean, after.lines()
        assert_same_output(before[CMP_KEY], snapshot(data, CMP_KEY), CMP_ENTITY)
        assert_same_output(before[RS_KEY], snapshot(data, RS_KEY), RS_ENTITY)


class TestCatalogue:
    """T-X2-10: both families register base and ``_latest`` views that serve rows."""

    def test_views_serve_the_activation_rows(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _registry, generated = _install(
            monkeypatch, data, _package_doc(CMP_PKG), _package_doc(RS_PKG)
        )
        _cmp_capture(data, _fixture("cmp381_ii.xlsx"))
        _rs_capture(data, _fixture("result_summary.zip"))
        for key in (CMP_KEY, RS_KEY):
            generated.transformers[key](data).run(DAY, run_id="r")
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        con = duckdb.connect(str(db), read_only=True)
        try:
            for key in (CMP_KEY, RS_KEY):
                for suffix in ("", "_latest"):
                    (count,) = con.execute(
                        f'SELECT count(*) FROM "silver_{SOURCE}_{key}{suffix}"'
                    ).fetchone()  # type: ignore[misc]
                    assert count == (3550 if key == CMP_KEY else 168), (key, suffix)
            latest = f'"silver_{SOURCE}_{CMP_KEY}_latest"'
            (periods,) = con.execute(
                f"SELECT count(DISTINCT settlement_period) FROM {latest} "
                "WHERE settlement_day = DATE '2022-03-27'"
            ).fetchone()  # type: ignore[misc]
            assert periods == 46
            (rows, keys) = con.execute(
                f"SELECT count(*), count(DISTINCT (settlement_day, settlement_period, run_type)) "
                f"FROM {latest}"
            ).fetchone()  # type: ignore[misc]
            assert rows == keys == 3550
        finally:
            con.close()
