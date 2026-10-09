"""The container gate: child identity, caps and the verified entry reader (ADR-037 P-3, P-4).

Rows T-X1-0, T-X1-6 (children), T-X2-1, T-X2-7 and T-X2-8 (a)-(e), (g) of the
unit X test matrix, plus the audit CLI (P-5).
"""

from __future__ import annotations

import hashlib
import io
import re
import zipfile
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
from _container_support import (  # noqa: F401 - forbid_zipfile_reads is a fixture
    FIXTURES,
    false_size_zip,
    forbid_zipfile_reads,
    patch_headers,
    workbook,
    zip_bytes,
)
from _neso_generic_support import write_capture
from _neso_registry_support import family, install_registry, package, resource, write_registry

from gridflow.silver.neso_data_portal import containers
from gridflow.silver.neso_data_portal.containers import (
    ContainerCapError,
    ContainerReadError,
    list_children,
    open_container,
    read_entry,
)

if TYPE_CHECKING:
    from gridflow.connectors.neso_data_portal.registry import Registry

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = ROOT / FIXTURES


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class TestFixtures:
    """T-X1-0: the fixtures are the real bronze bodies PROVENANCE.md names."""

    def test_every_fixture_matches_its_recorded_sha256(self) -> None:
        text = (FIXTURE_DIR / "PROVENANCE.md").read_text(encoding="utf-8")
        recorded = dict(re.findall(r"^\| `([^`]+)` \| `([0-9a-f]{64})` \|", text, re.MULTILINE))
        on_disk = sorted(p.name for p in FIXTURE_DIR.iterdir() if p.name != "PROVENANCE.md")
        assert sorted(recorded) == on_disk
        for name, sha in recorded.items():
            assert hashlib.sha256(_fixture(name)).hexdigest() == sha, name


class TestForbidFixture:
    """The P-15 fixture refuses archive reads from NESO modules only."""

    def test_neso_caller_is_refused_and_test_caller_passes(self) -> None:
        data = zip_bytes([("a.csv", b"x\n")])
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            assert archive.read("a.csv") == b"x\n"
            scope: dict[str, object] = {"__name__": "gridflow.silver.neso_data_portal.fake"}
            exec("def call(a):\n    return a.read('a.csv')\n", scope)
            with pytest.raises(AssertionError, match="read"):
                scope["call"](archive)  # type: ignore[operator]


class TestWorkbookBuilder:
    """The standard-library builder writes workbooks calamine reads."""

    def test_calamine_reads_the_builder_output(self) -> None:
        data = workbook([("S", {"A1": "h1", "B1": ("s", "h2"), "A2": "x", "B2": 3})])
        frame = pl.read_excel(
            io.BytesIO(data), engine="calamine", sheet_name="S", infer_schema_length=0
        )
        assert frame.columns == ["h1", "h2"]
        assert frame.rows() == [("x", "3")]


class TestChildIdentity:
    """T-X1-6 (children) and T-X2-7: the one definition of a body's children."""

    def test_real_fixtures(self) -> None:
        assert list_children(_fixture("cmp381_ii.xlsx")) == ("II Output", "Prior Comms")
        assert list_children(_fixture("tr129.xlsm")) == ("TR129",)
        folder = "ResultSummary 2019-11-22 to 2019-11-29/"
        assert list_children(_fixture("result_summary.zip")) == (
            f"{folder}NGESO_FRA_2019-11-22_ResultSummary.csv",
            f"{folder}NGESO_FRA_2019-11-29_ResultSummary.csv",
        )
        parts = list_children(_fixture("tnuos_gen_zones.zip"))
        assert len(parts) == 5
        assert {Path(p).suffix for p in parts} == {".cpg", ".dbf", ".prj", ".shp", ".shx"}

    def test_directory_excluded_workbook_member_expanded_nested_zip_listed(self) -> None:
        book = workbook([("One", {"A1": "h"}), ("Two", {"A1": "h"})])
        nested = zip_bytes([("inner.csv", b"a\n1\n")])
        body = zip_bytes(
            [("d/plain.csv", b"a\n1\n"), ("d/book.csv", book), ("d/nested.zip", nested)],
            dirs=("d/",),
        )
        assert list_children(body) == (
            "d/plain.csv",
            "d/book.csv::One",
            "d/book.csv::Two",
            "d/nested.zip",
        )

    def test_repeated_entry_name_is_refused(self) -> None:
        body = zip_bytes([("a.csv", b"a\n1\n"), ("a.csv", b"a\n2\n")])
        with pytest.raises(ContainerReadError, match="repeat"):
            open_container(body)
        with pytest.raises(ContainerReadError, match="repeat"):
            list_children(body)

    def test_member_name_with_separator_is_refused(self) -> None:
        with pytest.raises(ContainerReadError, match="::"):
            list_children(zip_bytes([("x::y", b"a\n1\n")]))

    def test_repeated_sheet_name_is_refused(self) -> None:
        with pytest.raises(ContainerReadError, match="sheet name repeats"):
            list_children(workbook([("S", {"A1": "h"}), ("S", {"A1": "h"})]))

    def test_doctype_in_workbook_part_is_refused(self) -> None:
        data = workbook([("S", {"A1": "h"})])
        container = open_container(data)
        raw = read_entry(container, container.info("xl/workbook.xml"))
        doctored = zip_bytes(
            [
                (info.filename, read_entry(container, info))
                if info.filename != "xl/workbook.xml"
                else (info.filename, raw.replace(b"<workbook", b"<!DOCTYPE w []><workbook", 1))
                for info in container.infos
            ]
        )
        with pytest.raises(ContainerReadError, match="DOCTYPE"):
            list_children(doctored)


class TestCaps:
    """T-X2-1: the caps apply to declared sizes before any decompression."""

    @pytest.fixture(autouse=True)
    def _no_reads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _fail(*_args: object) -> bytes:
            raise AssertionError("read_entry was called before the caps")

        monkeypatch.setattr(containers, "read_entry", _fail)

    def test_entry_count_cap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(containers, "MAX_ENTRIES", 2)
        body = zip_bytes([(f"{i}.csv", b"a\n") for i in range(3)])
        with pytest.raises(ContainerCapError, match="entries exceed"):
            containers.list_children(body)

    def test_entry_size_cap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(containers, "MAX_ENTRY_BYTES", 10)
        body = zip_bytes([("big.csv", b"a" * 11)])
        with pytest.raises(ContainerCapError, match="declares 11 B"):
            containers.list_children(body)

    def test_total_size_cap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(containers, "MAX_TOTAL_BYTES", 15)
        body = zip_bytes([("a.csv", b"a" * 8), ("b.csv", b"b" * 8)])
        with pytest.raises(ContainerCapError, match="in total"):
            containers.list_children(body)


class TestReadEntry:
    """T-X2-8 (a)-(e), (g): ``read_entry`` proves each entry exactly."""

    def test_a_corrupted_crc_is_refused(self) -> None:
        body = patch_headers(zip_bytes([("a.csv", b"a,b\n1,2\n")]), "a.csv", crc=12345)
        container = open_container(body)
        with pytest.raises(ContainerReadError, match="CRC"):
            read_entry(container, container.info("a.csv"))

    def test_b_false_size_with_prefix_crc_is_refused(self) -> None:
        archive, _content, prefix = false_size_zip()
        with zipfile.ZipFile(io.BytesIO(archive)) as stdlib:
            assert stdlib.read("data.csv") == prefix  # the attack is live
            assert stdlib.testzip() is None
        container = open_container(archive)
        with pytest.raises(ContainerReadError, match="not exactly consumed|inflated"):
            read_entry(container, container.info("data.csv"))

    def test_c_truncated_compressed_slice_is_refused(self) -> None:
        body = zip_bytes([("a.csv", b"a,b\n" * 50)])
        body = patch_headers(body, "a.csv", compress_size=len(body))
        container = open_container(body)
        with pytest.raises(ContainerReadError, match="compressed slice"):
            read_entry(container, container.info("a.csv"))

    def test_d_local_central_size_mismatch_is_refused(self) -> None:
        body = zip_bytes([("a.csv", b"a,b\n1,2\n")])
        body = patch_headers(body, "a.csv", file_size=3, central=False)
        container = open_container(body)
        with pytest.raises(ContainerReadError, match="differ from the central"):
            read_entry(container, container.info("a.csv"))

    def test_e_other_method_is_refused(self) -> None:
        body = patch_headers(zip_bytes([("a.csv", b"a,b\n1,2\n")]), "a.csv", method=12)
        container = open_container(body)
        with pytest.raises(ContainerReadError, match="method 12"):
            read_entry(container, container.info("a.csv"))

    def test_stored_and_deflated_round_trip(self) -> None:
        for method in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            body = zip_bytes([("a.csv", b"a,b\n1,2\n" * 20)], method=method)
            container = open_container(body)
            assert read_entry(container, container.info("a.csv")) == b"a,b\n1,2\n" * 20

    @pytest.mark.parametrize(
        "name", ["cmp381_ii.xlsx", "tr129.xlsm", "result_summary.zip", "tnuos_gen_zones.zip"]
    )
    def test_g_every_fixture_entry_reads_clean_at_every_level(self, name: str) -> None:
        container = open_container(_fixture(name))
        for info in container.infos:
            raw = read_entry(container, info)
            assert zlib.crc32(raw) == info.CRC
            if containers.is_zip_bytes(raw):
                inner = open_container(raw)
                for inner_info in inner.infos:
                    read_entry(inner, inner_info)


def _audit_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, children: list[str]
) -> Registry:
    files = family("zz_files", kind="files", archetype="FILE")
    entry = resource(
        "rid-1",
        "Archive",
        "zz_files",
        fmt="ZIP",
        disposition={"kind": "HOLD", "reason": "test", "unit": "T"},
        children=[{"child": c, "disposition": {"kind": "DOC"}} for c in children],
    )
    entry2 = resource(
        "rid-2",
        "Never captured",
        "zz_files",
        fmt="ZIP",
        disposition={"kind": "HOLD", "reason": "test", "unit": "T"},
        children=[{"child": "x.csv", "disposition": {"kind": "DOC"}}],
    )
    doc = package("zz-pkg", "pkg-id", [files], [entry, entry2])
    return install_registry(monkeypatch, write_registry(tmp_path / "reg", [doc]))


class TestAudit:
    """P-5's audit CLI: ok, mismatched and uncaptured resources; exit code."""

    def test_audit_reports_each_resource(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        data_dir = tmp_path / "d"
        _audit_registry(tmp_path, monkeypatch, ["a.csv", "b.csv"])
        write_capture(
            data_dir,
            "zz_files",
            package_slug="zz-pkg",
            package_id="pkg-id",
            resource_id="rid-1",
            resource_name="Archive",
            body=zip_bytes([("a.csv", b"a\n1\n"), ("b.csv", b"a\n1\n")]),
            written_at=datetime(2026, 10, 8, 9, tzinfo=UTC),
            ckan_format="ZIP",
            extension="zip",
        )
        assert containers.main(["audit", "--data-dir", str(data_dir)]) == 1
        out = capsys.readouterr().out.splitlines()
        assert out[0].startswith("OK rid-1")
        assert out[1].startswith("UNCAPTURED rid-2")
        assert out[-1] == "SUMMARY ok=1 mismatched=0 uncaptured=1"

    def test_audit_flags_an_inventory_mismatch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_dir = tmp_path / "d"
        _audit_registry(tmp_path, monkeypatch, ["a.csv", "ghost.csv"])
        write_capture(
            data_dir,
            "zz_files",
            package_slug="zz-pkg",
            package_id="pkg-id",
            resource_id="rid-1",
            resource_name="Archive",
            body=zip_bytes([("a.csv", b"a\n1\n"), ("b.csv", b"a\n1\n")]),
            written_at=datetime(2026, 10, 8, 9, tzinfo=UTC),
            ckan_format="ZIP",
            extension="zip",
        )
        lines = {line.resource_id: line for line in containers.audit(data_dir)}
        assert lines["rid-1"].status == "mismatched"
        assert "unlisted ['b.csv'] missing ['ghost.csv']" in lines["rid-1"].detail
