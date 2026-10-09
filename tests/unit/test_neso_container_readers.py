"""The ``xlsx`` and ``zip_member`` readers and member provenance (ADR-037 P-5..P-7, P-11).

Every X1 row of the unit X test matrix plus T-X2-8 (f). Real bodies are the
P-15 fixtures; negative cases are built with the standard-library workbook
builder of ``_container_support``.
"""

from __future__ import annotations

import io
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from _container_support import (  # noqa: F401 - forbid_zipfile_reads is a fixture
    FIXTURES,
    Cell,
    forbid_zipfile_reads,
    patch_headers,
    workbook,
    zip_bytes,
)
from _neso_generic_support import write_capture
from _neso_registry_support import family, install_registry, package, resource, write_registry

from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord, XlsxSpec
from gridflow.silver.neso_data_portal import readers
from gridflow.silver.neso_data_portal.casting import record_dtypes, type_child
from gridflow.silver.neso_data_portal.completion import CaptureContext
from gridflow.silver.neso_data_portal.containers import (
    ContainerReadError,
    list_children,
    open_container,
    read_entry,
)
from gridflow.silver.neso_data_portal.readers import (
    ChildTable,
    ContainerInventoryError,
    XlsxBlockError,
    read_children,
    read_csv_body,
    read_sheet,
)

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = ROOT / FIXTURES

CMP_HEADER = (
    "Settlement Day",
    "Settlement Period",
    "BSUoS Price (£/MWh Hour)",
    "Half-hourly Charge",
    "Total Daily BSUoS Charge",
    "Run Type",
    "Total Chargeable Energy Volume (MWh)",
    "CAPPED BSUoS Price (£/MWh)",
    "REVISED Half-Hourly Charge",
    "Total Net Adjustment to Invoices",
)
RS_FOLDER = "ResultSummary 2019-11-22 to 2019-11-29/"
RS_MEMBERS = (
    f"{RS_FOLDER}NGESO_FRA_2019-11-22_ResultSummary.csv",
    f"{RS_FOLDER}NGESO_FRA_2019-11-29_ResultSummary.csv",
)
CMP_ID = "88dcf101-1358-4d78-934f-527484d8789d"
RS_ID = "3928f192-97a5-4b4f-9234-0dcc9ad071a0"


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _col(
    source: str, dtype: str = "float64", nullable: bool = True, **extra: Any
) -> dict[str, Any]:
    from gridflow.connectors.neso_data_portal.profile import silver_name

    spec = {"source": source, "name": silver_name(source, 1), "dtype": dtype, "nullable": nullable}
    spec.update(extra)
    return spec


def cmp_record() -> dict[str, Any]:
    """P-12's ``current_bsuos_cap_adjustments`` record."""
    columns = [
        _col(CMP_HEADER[0], "date", False, format="%Y-%m-%d %H:%M:%S"),
        _col(CMP_HEADER[1], "int64", False, min=1, max=50),
        _col(CMP_HEADER[2]),
        _col(CMP_HEADER[3]),
        _col(CMP_HEADER[4]),
        _col(CMP_HEADER[5], "string", False),
        _col(CMP_HEADER[6]),
        _col(CMP_HEADER[7]),
        _col(CMP_HEADER[8], null_tokens=["N/A"]),
        _col(CMP_HEADER[9], null_tokens=["N/A"]),
    ]
    return {
        "version": "1",
        "reader": "xlsx",
        "encoding": "utf-8",
        "epochs": [{"header": list(CMP_HEADER), "columns": columns, "issue": {"kind": "none"}}],
        "temporal": {
            "kind": "sp_pair",
            "date_column": "settlement_day",
            "period_column": "settlement_period",
        },
        "entity_key": ["settlement_day", "settlement_period", "run_type"],
        "latest": "key_latest",
        "run_type_column": "run_type",
        "siblings": [],
        "vintage": "ckan_last_modified",
        "vintage_evidence": None,
        "xlsx": {"header_row": 8, "columns": "A:J"},
    }


def rs_record() -> dict[str, Any]:
    """P-12's ``ffr_phase2_result_summary_archive`` record."""
    header = ("Service", "Date", "EFA", "Cleared Volume", "Clearing Price")
    columns = [
        _col("Service", "string", False),
        _col("Date", "date", False, format="%Y-%m-%d"),
        _col("EFA", "int64", False),
        _col("Cleared Volume"),
        _col("Clearing Price"),
    ]
    return {
        "version": "1",
        "reader": "zip_member",
        "encoding": "utf-8",
        "epochs": [{"header": list(header), "columns": columns, "issue": {"kind": "none"}}],
        "temporal": {"kind": "date_sp1", "date_column": "date"},
        "entity_key": ["service", "date", "efa"],
        "latest": "key_latest",
        "vintage": "ckan_last_modified",
        "zip_member": {
            "member_pattern": r"[^/]+/NGESO_FRA_\d{4}-\d{2}-\d{2}_ResultSummary\.csv",
            "inner": "csv",
        },
    }


def _install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resource_id: str,
    children: list[str],
    *,
    fmt: str,
) -> None:
    files = family("zz_files", kind="files", archetype="FILE")
    entry = resource(
        resource_id,
        "Body",
        "zz_files",
        fmt=fmt,
        disposition={"kind": "HOLD", "reason": "test", "unit": "T"},
        children=[{"child": c, "disposition": {"kind": "DOC"}} for c in children],
    )
    install_registry(
        monkeypatch, write_registry(tmp_path / "reg", [package("zz-pkg", "pid", [files], [entry])])
    )


def _capture(tmp_path: Path, resource_id: str, body: bytes, extension: str) -> Path:
    path, _sidecar = write_capture(
        tmp_path / "d",
        "zz_files",
        package_slug="zz-pkg",
        package_id="pid",
        resource_id=resource_id,
        resource_name="Body",
        body=body,
        written_at=datetime(2026, 10, 8, 9, tzinfo=UTC),
        ckan_format=extension.upper(),
        extension=extension,
    )
    return path


def _sheet(data: bytes, sheet: str, spec: XlsxSpec) -> ChildTable:
    return read_sheet(open_container(data, "body"), sheet, spec, sheet)


def _ctx(path: Path) -> CaptureContext:
    return CaptureContext(
        capture_id="bronze/x/raw.xlsx",
        partition_date=datetime(2026, 10, 8).date(),
        body=path,
        sidecar=path,
        capture_written_at=datetime(2026, 10, 8, 9, tzinfo=UTC),
        resource_id=CMP_ID,
        resource_filename="f.xlsx",
        url_type="upload",
        body_sha256="",
        empty_capture=False,
        published_at=datetime(2022, 4, 11, 16, 8, tzinfo=UTC),
    )


class TestXlsxHeaderRow:
    """T-X1-1: the header row is selected and the notes above it are absent."""

    def test_cmp381_reads_3550_rows_under_the_ten_column_header(self) -> None:
        table = _sheet(
            _fixture("cmp381_ii.xlsx"), "II Output", XlsxSpec(header_row=8, columns="A:J")
        )
        assert table.header == CMP_HEADER
        assert table.frame.columns == list(CMP_HEADER)
        assert table.frame.height == 3550
        assert set(table.frame.dtypes) == {pl.Utf8}
        assert table.frame["Settlement Day"][0] == "2022-01-17 00:00:00"
        assert table.frame["Settlement Period"][0] == "1"


class TestXlsm:
    """T-X1-2: an XLSM body reads, and ``last_row`` bounds the rows."""

    def test_tr129_opens_and_calamine_reads_its_composite_header(self) -> None:
        data = _fixture("tr129.xlsm")
        open_container(data)
        assert list_children(data) == ("TR129",)
        frame = pl.read_excel(
            io.BytesIO(data),
            engine="calamine",
            sheet_name="TR129",
            read_options={"header_row": 1},
            infer_schema_length=0,
        )
        assert frame.columns[0] == "Tender Ref"
        assert any(name.startswith("__UNNAMED__") for name in frame.columns)

    def test_last_row_reads_exactly_to_last_row(self) -> None:
        cells: dict[str, Cell] = {"A1": "k", "B1": "v"}
        for row in range(2, 5):
            cells[f"A{row}"] = f"r{row}"
            cells[f"B{row}"] = row
        cells["A6"] = "footer note"
        cells["C7"] = "another note"
        table = _sheet(
            workbook([("S", cells)]), "S", XlsxSpec(header_row=1, columns="A:B", last_row=4)
        )
        assert table.frame["k"].to_list() == ["r2", "r3", "r4"]


class TestMergedRows:
    """T-X1-3: a merge touching the block fails loud (rule b)."""

    def test_tr129_header_merge(self) -> None:
        with pytest.raises(XlsxBlockError, match=r"\(b\).*A2:A4"):
            _sheet(
                _fixture("tr129.xlsm"), "TR129", XlsxSpec(header_row=2, columns="A:A", last_row=162)
            )

    def test_merge_inside_data(self) -> None:
        cells: dict[str, Cell] = {"A1": "k", "B1": "v", "A2": "x", "B2": 1, "A3": "y", "B3": 2}
        data = workbook([("S", cells)], merges={"S": ["A2:A3"]})
        with pytest.raises(XlsxBlockError, match=r"\(b\).*A2:A3"):
            _sheet(data, "S", XlsxSpec(header_row=1, columns="A:B"))

    def test_merge_outside_the_block_is_ignored(self) -> None:
        cells: dict[str, Cell] = {"A1": "title", "A3": "k", "B3": "v", "A4": "x", "B4": 1}
        data = workbook([("S", cells)], merges={"S": ["A1:D1"]})
        assert _sheet(data, "S", XlsxSpec(header_row=3, columns="A:B")).frame.height == 1


class TestNotesRows:
    """T-X1-4: notes, example rows, errors and a grown table fail loud."""

    def test_cmp381_with_header_row_1_fails_rule_a(self) -> None:
        with pytest.raises(XlsxBlockError, match=r"\(a\)"):
            _sheet(_fixture("cmp381_ii.xlsx"), "II Output", XlsxSpec(header_row=1, columns="A:J"))

    def test_tr129_example_rows_fail_rule_d(self) -> None:
        with pytest.raises(XlsxBlockError, match=r"\(d\).*row 5"):
            _sheet(
                _fixture("tr129.xlsm"), "TR129", XlsxSpec(header_row=4, columns="B:C", last_row=162)
            )

    @pytest.mark.parametrize(
        ("extra", "spec", "rule"),
        [
            ({"B3": ("e", "#DIV/0!")}, XlsxSpec(header_row=1, columns="A:B"), "c"),
            ({"B3": ("f", "SUM(B2)")}, XlsxSpec(header_row=1, columns="A:B"), "c"),
            ({"A5": "z", "B5": 5}, XlsxSpec(header_row=1, columns="A:B"), "d"),
            ({"C3": "stray"}, XlsxSpec(header_row=1, columns="A:B"), "e"),
            ({"A4": "w", "B4": 4}, XlsxSpec(header_row=1, columns="A:B", last_row=3), "f"),
            ({"B1": 7}, XlsxSpec(header_row=1, columns="A:B"), "a"),
            ({"B1": "k"}, XlsxSpec(header_row=1, columns="A:B"), "a"),
        ],
    )
    def test_synthetic_rule(self, extra: dict[str, Cell], spec: XlsxSpec, rule: str) -> None:
        cells: dict[str, Cell] = {"A1": "k", "B1": "v", "A2": "x", "B2": 1, "A3": "y", "B3": 2}
        cells.update(extra)
        with pytest.raises(XlsxBlockError, match=rf"\({rule}\)"):
            _sheet(workbook([("S", cells)]), "S", spec)

    def test_cached_formula_and_shared_string_cells_read(self) -> None:
        cells: dict[str, Cell] = {
            "A1": ("s", "k"),
            "B1": "v",
            "A2": ("s", "x"),
            "B2": ("fv", "1+1", "2"),
        }
        table = _sheet(workbook([("S", cells)]), "S", XlsxSpec(header_row=1, columns="A:B"))
        assert table.frame.rows() == [("x", "2")]


class TestZipOfCsv:
    """T-X1-5: each member is one table, with the member's CRC."""

    def test_result_summary_members(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(tmp_path, monkeypatch, RS_ID, list(RS_MEMBERS), fmt="ZIP")
        path = _capture(tmp_path, RS_ID, _fixture("result_summary.zip"), "zip")
        record = SchemaRecord.model_validate(rs_record())
        tables = list(read_children(path, record, RS_MEMBERS))
        assert [t.child_id for t in tables] == list(RS_MEMBERS)
        assert [t.frame.height for t in tables] == [84, 84]
        assert [t.crc32 for t in tables] == [2362721365, 4128616702]
        assert tables[0].header == ("Service", "Date", "EFA", "Cleared Volume", "Clearing Price")

    def test_member_outside_the_pattern_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = zip_bytes([("other.csv", b"Service,Date,EFA,Cleared Volume,Clearing Price\n")])
        _install(tmp_path, monkeypatch, RS_ID, ["other.csv"], fmt="ZIP")
        path = _capture(tmp_path, RS_ID, body, "zip")
        record = SchemaRecord.model_validate(rs_record())
        with pytest.raises(ContainerInventoryError, match="does not match"):
            list(read_children(path, record, ("other.csv",)))

    def test_nested_zip_member_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        name = f"{RS_FOLDER}NGESO_FRA_2019-11-22_ResultSummary.csv"
        body = zip_bytes([(name, zip_bytes([("x.csv", b"a\n1\n")]))])
        _install(tmp_path, monkeypatch, RS_ID, [name], fmt="ZIP")
        path = _capture(tmp_path, RS_ID, body, "zip")
        record = SchemaRecord.model_validate(rs_record())
        with pytest.raises(ContainerReadError, match="not a CSV member"):
            list(read_children(path, record, (name,)))


class TestInventoryCheck:
    """P-5 at the reader: unlisted, missing and unresolvable all fail loud."""

    def test_unlisted_member(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(tmp_path, monkeypatch, RS_ID, [RS_MEMBERS[0]], fmt="ZIP")
        path = _capture(tmp_path, RS_ID, _fixture("result_summary.zip"), "zip")
        record = SchemaRecord.model_validate(rs_record())
        with pytest.raises(ContainerInventoryError, match="unlisted"):
            list(read_children(path, record, (RS_MEMBERS[0],)))

    def test_missing_sheet(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(tmp_path, monkeypatch, CMP_ID, ["II Output", "Prior Comms", "Ghost"], fmt="XLSX")
        path = _capture(tmp_path, CMP_ID, _fixture("cmp381_ii.xlsx"), "xlsx")
        record = SchemaRecord.model_validate(cmp_record())
        with pytest.raises(ContainerInventoryError, match=r"missing \['Ghost'\]"):
            list(read_children(path, record, ("II Output",)))

    def test_unresolvable_resource(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(tmp_path, monkeypatch, "someone-else", ["II Output"], fmt="XLSX")
        path = _capture(tmp_path, CMP_ID, _fixture("cmp381_ii.xlsx"), "xlsx")
        record = SchemaRecord.model_validate(cmp_record())
        with pytest.raises(ContainerInventoryError, match="not a registry resource"):
            list(read_children(path, record, ("II Output",)))


class TestStrictTyping:
    """T-X1-7: the record's strict casts own every Excel cell."""

    def test_cmp_fixture_types_and_nulls(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(tmp_path, monkeypatch, CMP_ID, ["II Output", "Prior Comms"], fmt="XLSX")
        path = _capture(tmp_path, CMP_ID, _fixture("cmp381_ii.xlsx"), "xlsx")
        record = SchemaRecord.model_validate(cmp_record())
        (table,) = read_children(path, record, ("II Output",))
        typed = type_child(table, record, _ctx(path))
        frame = typed.frame
        assert frame.schema["settlement_day"] == pl.Date
        assert frame.schema["settlement_period"] == pl.Int64
        na = table.frame["REVISED Half-Hourly Charge"].eq("N/A").sum()
        assert 0 < na < frame.height
        assert frame["revised_half_hourly_charge"].null_count() == na
        assert typed.tally.total == 0
        assert frame["child_id"].unique().to_list() == ["II Output"]
        assert frame["child_crc32"].unique().to_list() == [table.crc32]
        assert frame.columns[-2:] == ["child_id", "child_crc32"]

    def test_a_non_numeric_period_fails_the_capture(self, tmp_path: Path) -> None:
        cells: dict[str, Cell] = {
            f"{chr(ord('A') + i)}8": name for i, name in enumerate(CMP_HEADER)
        }
        row = ["2022-01-17 00:00:00", "x", 1, 1, 1, "II", 1, 1, "N/A", "N/A"]
        cells.update({f"{chr(ord('A') + i)}9": value for i, value in enumerate(row)})
        record = SchemaRecord.model_validate(cmp_record())
        assert record.xlsx is not None
        table = _sheet(workbook([("II Output", cells)]), "II Output", record.xlsx)
        with pytest.raises(pl.exceptions.InvalidOperationError):
            type_child(table, record, _ctx(tmp_path / "x"))


class TestCsvMemberParity:
    """T-X1-8: a CSV member reads exactly as the same bytes as a CSV body."""

    def test_bom_crlf_member_equals_csv_body(self, tmp_path: Path) -> None:
        raw = b"\xef\xbb\xbfA,B\r\n1,x\r\n2,y\r\n\r\n"
        path = tmp_path / "b.csv"
        path.write_bytes(raw)
        record = SchemaRecord.model_validate(
            {
                "version": "1",
                "reader": "csv",
                "encoding": "utf-8",
                "epochs": [
                    {
                        "header": ["A", "B"],
                        "columns": [
                            {"source": "A", "name": "a", "dtype": "string", "nullable": True},
                            {"source": "B", "name": "b", "dtype": "string", "nullable": True},
                        ],
                        "issue": {"kind": "none"},
                    }
                ],
                "temporal": {"kind": "none"},
                "entity_key": ["a"],
                "latest": "key_latest",
                "vintage": "capture_fallback",
            }
        )
        (body_table,) = read_csv_body(path, record, ())
        header, frame = readers._csv_member_table(raw, record, "member")
        assert header == body_table.header
        assert frame.equals(body_table.frame)


class TestUnverifiedPartNeverReachesCalamine:
    """T-X2-8 (f): a lying worksheet part is refused before calamine is called."""

    def test_false_size_sheet_part(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _no_calamine(*_args: object, **_kwargs: object) -> pl.DataFrame:
            raise AssertionError("pl.read_excel was called on an unverified workbook")

        monkeypatch.setattr(readers.pl, "read_excel", _no_calamine)
        data = workbook([("S", {"A1": "k", "A2": "x" * 40})])
        container = open_container(data)
        info = container.info("xl/worksheets/sheet1.xml")
        prefix = read_entry(container, info)[:16]
        lying = patch_headers(data, info.filename, file_size=16, crc=zlib.crc32(prefix))
        with pytest.raises(ContainerReadError, match="sheet1.xml"):
            _sheet(lying, "S", XlsxSpec(header_row=1, columns="A:A"))


class TestRecordColumns:
    """P-11: container outputs carry ``child_crc32`` after ``child_id``; CSV outputs do not."""

    def test_container_record_dtypes(self) -> None:
        dtypes = record_dtypes(SchemaRecord.model_validate(cmp_record()))
        names = list(dtypes)
        assert names[names.index("child_id") + 1] == "child_crc32"
        assert dtypes["child_crc32"] == "int64"

    def test_csv_record_dtypes_unchanged(self) -> None:
        record = SchemaRecord.model_validate({**rs_record(), "reader": "csv", "zip_member": None})
        assert "child_crc32" not in record_dtypes(record)
        assert "child_id" not in record_dtypes(record)
