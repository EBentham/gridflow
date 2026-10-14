"""The composite-block workbook reader (AS-5c P-2..P-4; ADR-037 amendment 1).

Mechanism tests: :func:`read_block` over the five lineage cell maps (rebuilt workbooks of the
real TR122, TR112, TR126, TR108 and TR144 bodies, ``as5c/PROVENANCE.md``) and synthetic blocks
for each rule (h)-(n); :func:`read_xlsx_body`'s dispatch through a record with ``xlsx_blocks``.
The engine-level tests over the committed workbook record live in ``test_neso_as5c_records.py``.
Nothing touches ``C:/gridflow-data``.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _container_support import (  # noqa: F401 - forbid_zipfile_reads is a fixture
    Cell,
    forbid_zipfile_reads,
    lineage_cells,
    lineage_workbook,
    rezip,
    workbook,
)
from _neso_generic_support import write_capture
from _neso_registry_support import record

from gridflow.connectors.neso_data_portal.registry import SchemaRecord
from gridflow.connectors.neso_data_portal.registry.record import XlsxBlock
from gridflow.silver.neso_data_portal.containers import open_container
from gridflow.silver.neso_data_portal.readers import (
    LABEL_SEPARATOR,
    ChildTable,
    ContainerInventoryError,
    XlsxBlockError,
    read_block,
    read_xlsx_body,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

LINEAGES = ("tr122", "tr112", "tr126", "tr108", "tr144")
FFR_PKG = "firm-frequency-response-post-tender-reports"
FFR_PKG_ID = "1162a519-b66d-48f6-96ae-1a69ff17951f"
FILES_DIR = "ffr_post_tender_reports_files"
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 11, 0, 50, tzinfo=UTC)


def _block(document: dict[str, Any], **change: Any) -> XlsxBlock:
    """The lineage fixture's block: its production block with ``data_last = h + 7``."""
    h = document["header_first"]
    fields: dict[str, Any] = {
        "resource_id": document["resource_id"],
        "sheet": document["sheet"],
        "header_first": h,
        "header_last": h + 2,
        "data_first": h + 5,
        "data_last": h + 7,
        "columns": document["columns"],
    }
    fields.update(change)
    return XlsxBlock(**fields)


def _read(body: bytes, block: XlsxBlock) -> ChildTable:
    return read_block(open_container(body, "w"), block, block.sheet)


def _cells(document: dict[str, Any], row: int) -> dict[str, list[str]]:
    """The map's cells of one fixture row, by column letters."""
    out: dict[str, list[str]] = {}
    for ref, cell in document["cells"].items():
        letters = ref.rstrip("0123456789")
        if ref[len(letters) :] == str(row):
            out[letters] = cell
    return out


class TestLineage:
    """I-2 / I-3 over the five rebuilt lineage workbooks (header starts 2, 3, 4, 6; A:AP, A:AQ)."""

    @pytest.mark.parametrize("name", LINEAGES)
    def test_i2_exactly_the_tender_rows_load(self, name: str) -> None:
        """Detects a notice, header, example or footer row read as data, a tender row lost, or a
        row chosen by value: the block yields its three tender rows in order, their Tender Ref
        stored text exact, and no example (``Example Company``) or footer (``Code``) value."""
        body, document = lineage_workbook(name)
        table = _read(body, _block(document))
        h = document["header_first"]
        assert table.frame.height == 3
        assert table.header[0] == "Tender Ref"
        expected = [_cells(document, h + offset)["A"][1] for offset in (5, 6, 7)]
        assert table.frame["Tender Ref"].to_list() == expected
        assert len(table.header) == (43 if document["columns"] == "A:AQ" else 42)
        values = {v for column in table.frame.columns for v in table.frame[column].to_list()}
        assert {"Example Company", "Code", "Reason", "Rejection Codes"}.isdisjoint(values)
        example = _cells(document, h + 3)["A"][1]
        assert example not in table.frame["Tender Ref"].to_list()
        assert table.frame.schema == pl.Schema(dict.fromkeys(table.header, pl.Utf8))

    def test_i3_stored_reference_survives_byte_equal(self) -> None:
        """Detects a numeric Tender Ref rounded or re-rendered (``122.165``): the stored ``<v>``
        text ``122.16500000000073`` of TR122's last tender row is the value, byte for byte."""
        body, document = lineage_workbook("tr122")
        table = _read(body, _block(document))
        assert table.frame["Tender Ref"].to_list()[-1] == "122.16500000000073"

    def test_composite_labels_join_stacked_header_units(self) -> None:
        """Detects a label built from one header row only or a merge counted per row: TR122's
        Mon-Fri From column is the three stacked units, the Primary Response column is its
        two-row group merge then its own cell, and a plain three-row merge is one part."""
        body, document = lineage_workbook("tr122")
        header = _read(body, _block(document)).header
        assert header[11] == LABEL_SEPARATOR.join(
            ["Tendered Frames per Service Day", "Working Days \n(Mon-Fri)", "From"]
        )
        assert header[28] == LABEL_SEPARATOR.join(
            ["Dynamic Providers only", "Primary Response (max.) @ 0.2Hz (MW)"]
        )
        assert header[3] == "Company Name"

    def test_empty_and_absent_cells_are_null(self) -> None:
        """Detects a cached empty formula string or an absent cell loaded as ``""``."""
        body, document = lineage_workbook("tr122")
        frame = _read(body, _block(document)).frame
        assert frame.null_count().row(0) != (0,) * frame.width
        for column in frame.columns:
            assert "" not in frame[column].drop_nulls().to_list()


class TestChangedBlockFailsLoud:
    """I-2 / FM-1, FM-2, FM-4: a block map that disagrees with the sheet fails the read."""

    def test_data_last_one_short_is_rule_n(self) -> None:
        body, document = lineage_workbook("tr122")
        block = _block(document, data_last=document["header_first"] + 6)
        with pytest.raises(XlsxBlockError, match=r"\(n\)"):
            _read(body, block)

    def test_data_last_one_long_is_rule_l(self) -> None:
        body, document = lineage_workbook("tr122")
        block = _block(document, data_last=document["header_first"] + 8)
        with pytest.raises(XlsxBlockError, match=r"\(l\)"):
            _read(body, block)

    @pytest.mark.parametrize("name", LINEAGES)
    def test_header_rows_shifted_down_are_refused(self, name: str) -> None:
        body, document = lineage_workbook(name)
        h = document["header_first"]
        block = _block(document, header_first=h + 1, header_last=h + 3)
        with pytest.raises(XlsxBlockError, match=r"\((h|i)\)"):
            _read(body, block)

    def test_header_rows_shifted_up_are_refused(self) -> None:
        body, document = lineage_workbook("tr126")
        h = document["header_first"]
        block = _block(document, header_first=h - 1, header_last=h + 1)
        with pytest.raises(XlsxBlockError, match=r"\((h|i|j)\)"):
            _read(body, block)


def _synthetic(
    cells: dict[str, Cell], merges: list[str] | None = None, **change: Any
) -> tuple[bytes, XlsxBlock]:
    """A two-column sheet: header rows 1-2, data rows 4-5, columns A:B (row 3 unread)."""
    base: dict[str, Cell] = {
        "A1": "Group",
        "A2": "Ref",
        "B1": "Value",
        "A3": "example",
        "A4": ("n", "1.5"),
        "B4": "x",
        "A5": ("n", "2"),
        "B5": "y",
    }
    base.update(cells)
    body = workbook([("S", {k: v for k, v in base.items() if v != ""})], merges={"S": merges or []})
    fields: dict[str, Any] = {
        "resource_id": "r",
        "sheet": "S",
        "header_first": 1,
        "header_last": 2,
        "data_first": 4,
        "data_last": 5,
        "columns": "A:B",
    }
    fields.update(change)
    return body, XlsxBlock(**fields)


class TestRules:
    """A-2d: each composite rule (h)-(n) on a synthetic block."""

    def test_control(self) -> None:
        body, block = _synthetic({})
        table = _read(body, block)
        assert table.header == ("Group | Ref", "Value")
        assert table.frame.rows() == [("1.5", "x"), ("2", "y")]

    def test_h_numeric_header_cell(self) -> None:
        body, block = _synthetic({"B2": 7})
        with pytest.raises(XlsxBlockError, match=r"\(h\)"):
            _read(body, block)

    def test_i_merge_in_a_data_row(self) -> None:
        body, block = _synthetic({}, ["A4:B4"])
        with pytest.raises(XlsxBlockError, match=r"\(i\).*intersects the data rows"):
            _read(body, block)

    def test_i_merge_straddling_header_and_data(self) -> None:
        body, block = _synthetic({}, ["B1:B4"])
        with pytest.raises(XlsxBlockError, match=r"\(i\)"):
            _read(body, block)

    def test_i_header_merge_leaving_the_columns(self) -> None:
        body, block = _synthetic({}, ["B1:C1"])
        with pytest.raises(XlsxBlockError, match=r"\(i\).*leaves the header rows"):
            _read(body, block)

    def test_merges_outside_header_and_data_are_ignored(self) -> None:
        body, block = _synthetic({"A7": "notes"}, ["A3:B3", "A7:C7"])
        assert _read(body, block).frame.height == 2

    def test_j_hidden_merge_cell_with_different_text(self) -> None:
        body, block = _synthetic({"B2": "Other"}, ["B1:B2"])
        with pytest.raises(XlsxBlockError, match=r"\(j\).*differs"):
            _read(body, block)

    def test_j_hidden_merge_cell_with_equal_text_is_one_part(self) -> None:
        body, block = _synthetic({"B2": " Value"}, ["B1:B2"])
        assert _read(body, block).header == ("Group | Ref", "Value")

    def test_j_empty_label(self) -> None:
        body, block = _synthetic({"B1": ""})
        with pytest.raises(XlsxBlockError, match=r"\(j\).*empty label"):
            _read(body, block)

    def test_j_repeated_label(self) -> None:
        body, block = _synthetic({"A1": "", "A2": "Value"})
        with pytest.raises(XlsxBlockError, match=r"\(j\).*repeats"):
            _read(body, block)

    @pytest.mark.parametrize(
        "cell", [("e", "#REF!"), ("f", "SUM(A1)"), ("b", "1")], ids=["error", "uncached", "bool"]
    )
    def test_k_bad_data_cell_kind(self, cell: tuple[str, ...]) -> None:
        body, block = _synthetic({"B5": cell})
        with pytest.raises(XlsxBlockError, match=r"\(k\)"):
            _read(body, block)

    def test_l_empty_data_row(self) -> None:
        body, block = _synthetic({"A5": "", "B5": ""})
        with pytest.raises(XlsxBlockError, match=r"\(l\)"):
            _read(body, block)

    def test_m_populated_cell_right_of_the_columns(self) -> None:
        body, block = _synthetic({"C4": "stray"})
        with pytest.raises(XlsxBlockError, match=r"\(m\)"):
            _read(body, block)

    def test_n_block_stops_early(self) -> None:
        body, block = _synthetic({"A6": ("n", "3"), "B6": "z"})
        with pytest.raises(XlsxBlockError, match=r"\(n\)"):
            _read(body, block)

    def test_i4_dates_load_verbatim_in_one_raw_column(self) -> None:
        """A-6 (I-4): a TR164-shaped column holding a numeric serial and a text date loads both
        verbatim (no conversion of a subset)."""
        body, block = _synthetic({"B4": ("n", "45169"), "B5": ("s", "31/08/2023")})
        assert _read(body, block).frame["Value"].to_list() == ["45169", "31/08/2023"]

    def test_i3_numeric_text_is_not_reformatted(self) -> None:
        """A-3a (I-3): a numeric ``<v>`` with float noise is kept as stored."""
        body, block = _synthetic({"B4": ("n", "850.5949936745257"), "B5": ("fs", "RC", "1")})
        assert _read(body, block).frame["Value"].to_list() == ["850.5949936745257", "1"]


@pytest.fixture
def root() -> Iterator[Path]:
    """A short tmp data root (Windows MAX_PATH)."""
    with tempfile.TemporaryDirectory(prefix="b", ignore_cleanup_errors=True) as tmp:
        yield Path(tmp)


def _capture(
    root: Path, body: bytes, resource_id: str, *, ckan_format: str = "XLSX", extension: str = "xlsx"
) -> Path:
    path, _sidecar = write_capture(
        root,
        FILES_DIR,
        package_slug=FFR_PKG,
        package_id=FFR_PKG_ID,
        resource_id=resource_id,
        resource_name="Post Tender Report",
        body=body,
        written_at=WRITTEN,
        ckan_last_modified="2020-03-10T10:00:00.000001",
        resource_filename=f"post-tender-report.{extension}",
        ckan_format=ckan_format,
        extension=extension,
        partition=DAY,
    )
    return path


def _block_record(*blocks: XlsxBlock) -> SchemaRecord:
    document = record(
        reader="xlsx",
        epochs=[
            {
                "header": ["x"],
                "columns": [{"source": "x", "name": "x", "dtype": "string", "nullable": True}],
                "issue": {"kind": "none"},
            }
        ],
        temporal={"kind": "none"},
        entity_key=("x",),
        latest="whole_capture",
    )
    document["xlsx_blocks"] = [block.model_dump() for block in blocks]
    return SchemaRecord.model_validate(document)


class TestDispatch:
    """P-2: the body's resource picks its block; no fallback, no other sheet."""

    def test_the_resource_block_is_read(self, root: Path) -> None:
        body, document = lineage_workbook("tr122")
        block = _block(document)
        path = _capture(root, body, block.resource_id)
        other = XlsxBlock(**{**block.model_dump(), "resource_id": "other", "data_last": 99})
        (table,) = read_xlsx_body(path, _block_record(other, block), (block.sheet,))
        assert table.child_id == block.sheet and table.frame.height == 3

    def test_unlisted_resource_fails(self, root: Path) -> None:
        body, document = lineage_workbook("tr122")
        block = _block(document)
        path = _capture(root, body, block.resource_id)
        other = XlsxBlock(**{**block.model_dump(), "resource_id": "other"})
        with pytest.raises(XlsxBlockError, match="has no declared xlsx block"):
            list(read_xlsx_body(path, _block_record(other), (block.sheet,)))

    def test_another_child_fails(self, root: Path) -> None:
        body, document = lineage_workbook("tr122")
        block = _block(document)
        path = _capture(root, body, block.resource_id)
        moved = XlsxBlock(**{**block.model_dump(), "sheet": "Sheet9"})
        with pytest.raises(XlsxBlockError, match="not the declared block sheet"):
            list(read_xlsx_body(path, _block_record(moved), (block.sheet,)))

    def test_member_child_fails(self, root: Path) -> None:
        body, document = lineage_workbook("tr122")
        block = _block(document)
        path = _capture(root, body, block.resource_id)
        with pytest.raises(ContainerInventoryError, match="names a member"):
            list(read_xlsx_body(path, _block_record(block), ("a::b",)))

    def test_i10_format_by_body(self, root: Path) -> None:
        """A-10 (I-10): a macro-enabled body labelled XLSX and a plain body labelled XLSM read the
        same rows; the catalogue label and the suffix are never consulted."""
        body, document = lineage_workbook("tr122")
        block = _block(document)
        macro = rezip(body, [("xl/vbaProject.bin", b"\x00VBA")])
        first = root / "one"
        second = root / "two"
        a = _capture(first, macro, block.resource_id, ckan_format="XLSX", extension="xlsx")
        b = _capture(second, body, block.resource_id, ckan_format="XLSM", extension="xlsm")
        (left,) = read_xlsx_body(a, _block_record(block), (block.sheet,))
        (right,) = read_xlsx_body(b, _block_record(block), (block.sheet,))
        assert left.frame.equals(right.frame) and left.header == right.header


def test_lineage_cells_rebuild_every_kind() -> None:
    """Detects the builder dropping a lineage cell kind (TR144 carries error cells)."""
    _body, document = lineage_workbook("tr144")
    kinds = {cell[0] for cell in lineage_cells(document).values() if isinstance(cell, tuple)}
    assert {"n", "s", "fs", "e"} <= kinds
