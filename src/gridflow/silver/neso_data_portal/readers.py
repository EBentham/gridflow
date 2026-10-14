"""The body-reader seam of the NESO generic engine (ADR-034 P-3).

A reader turns one bronze body into :class:`ChildTable` s: one for a plain CSV
body, one per requested child for a container (an Excel workbook's sheets, a
ZIP's members). Unit X registers the container readers ``xlsx`` and
``zip_member`` (ADR-037): each resolves its body's registry resource from the
sidecar and requires the body's children (:func:`containers.list_children`)
to be exactly the resource's inventory (P-5), reads every ZIP entry through
the verified :func:`containers.read_entry` (P-4), and stamps each table with
the CRC-32 of the entry it came from (P-11). A record naming a reader no
module registered fails each capture with :class:`ReaderUnavailableError`.

Every table a reader yields is all-``Utf8`` with its columns equal to the
header it parsed (stripped), so P-4 types it by matching that header to one of
the record's epochs. A record with ``xlsx_blocks`` (AS-5c, ADR-037 amendment 1)
reads one declared composite block per resource instead: its header is one
label per column built from the stacked header rows (:func:`read_block`).
"""

from __future__ import annotations

import codecs
import io
import json
import re
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.connectors.neso_data_portal.captures import SIDECAR_SUFFIX
from gridflow.connectors.neso_data_portal.registry.record import column_index
from gridflow.silver.csv_bronze import read_csv_bronze_body
from gridflow.silver.neso_data_portal.containers import (
    CHILD_SEPARATOR,
    Container,
    ContainerReadError,
    is_workbook,
    is_zip_bytes,
    list_children,
    open_container,
    read_entry,
    read_xml_part,
    verify_all_entries,
    workbook_sheets,
)

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.registry import ResourceEntry
    from gridflow.connectors.neso_data_portal.registry.record import (
        SchemaRecord,
        XlsxBlock,
        XlsxSpec,
    )

__all__ = [
    "CONTAINER_READERS",
    "READERS",
    "BodyReader",
    "ChildTable",
    "ContainerInventoryError",
    "ReaderUnavailableError",
    "LABEL_SEPARATOR",
    "XlsxBlockError",
    "read_block",
    "read_children",
    "read_csv_body",
    "read_sheet",
    "read_xlsx_body",
    "read_zip_member_body",
]

_UTF8_BOM = b"\xef\xbb\xbf"
_UTF8_CHUNK = 1 << 20
_SHARED_STRINGS = "xl/sharedStrings.xml"

LABEL_SEPARATOR = " | "
"""Joins the stacked header parts of one composite-block column into its label (rule (j))."""


class ReaderUnavailableError(Exception):
    """The record names a reader no module has registered yet (unit X)."""


class ContainerInventoryError(Exception):
    """A container reader yielded children other than exactly those requested."""


@dataclass(frozen=True)
class ChildTable:
    """One table read from a body.

    Attributes:
        child_id: The container child id; ``""`` for a plain body.
        header: The parsed header, stripped.
        frame: The rows, all ``Utf8``, columns equal to ``header``.
        crc32: The CRC-32 of the ZIP entry a container child came from
            (P-11); ``None`` for a plain body.
    """

    child_id: str
    header: tuple[str, ...]
    frame: pl.DataFrame
    crc32: int | None = None


BodyReader = Callable[["Path", "SchemaRecord", tuple[str, ...]], Iterator[ChildTable]]
"""``(body path, record, requested child ids) -> tables``; ``()`` for a plain body."""


def _validate_utf8(raw: bytes) -> None:
    """Validate ``raw`` as strict UTF-8 in bounded chunks, keeping no decoded text.

    Args:
        raw: The body, BOM included.

    Raises:
        UnicodeDecodeError: ``raw`` is not valid UTF-8. The exception's object
            is ``raw`` itself and its positions are absolute in it, so the
            message equals the one ``raw.decode("utf-8")`` would raise.
    """
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    view = memoryview(raw)
    offset = 0
    base = 0
    try:
        while offset < len(view):
            # The decoder may hold a partial sequence from the previous chunk;
            # its error positions count from the start of that held prefix.
            base = offset - len(decoder.getstate()[0])
            decoder.decode(view[offset : offset + _UTF8_CHUNK], False)
            offset += _UTF8_CHUNK
        base = len(raw) - len(decoder.getstate()[0])
        decoder.decode(b"", True)
    except UnicodeDecodeError as exc:
        raise UnicodeDecodeError(
            exc.encoding, raw, base + exc.start, base + exc.end, exc.reason
        ) from None


def _utf8_body(raw: bytes, record: SchemaRecord) -> bytes:
    """The body as UTF-8 bytes, strictly checked against ``record.encoding``.

    A UTF-8-alias record's body is validated in bounded chunks and returned as
    the same object (no copy). Any other encoding is decoded strictly and
    re-encoded as UTF-8.

    Args:
        raw: The body as stored.
        record: The family's record.

    Returns:
        UTF-8 bytes; ``raw`` itself for a UTF-8-alias record.

    Raises:
        UnicodeDecodeError: The body is not valid in ``record.encoding``.
    """
    if codecs.lookup(record.encoding).name in ("utf-8", "utf-8-sig"):
        _validate_utf8(raw)
        return raw
    return raw.decode(record.encoding, errors="strict").encode("utf-8")


def read_csv_body(
    path: Path, record: SchemaRecord, children: tuple[str, ...]
) -> Iterator[ChildTable]:
    """Read one CSV body with the record's encoding through unit A's reader.

    The body is decoded strictly with ``record.encoding`` and re-encoded as
    UTF-8; a UTF-8 body is instead validated in bounded chunks and passed
    through uncopied, so every body that is not valid in ``record.encoding``
    raises ``UnicodeDecodeError`` before Polars sees it. The header is parsed
    first and handed to the reader as its ``expected_columns``, so unit A's
    BOM, blank-row and markup rules all apply and the header-to-epoch match
    happens in P-4.

    Args:
        path: The bronze body.
        record: The family's record.
        children: Must be empty: a CSV body has no children.

    Yields:
        Exactly one :class:`ChildTable` with ``child_id == ""``.

    Raises:
        ContainerInventoryError: Child ids were requested of a plain body.
        UnicodeDecodeError: The body is not valid in ``record.encoding``.
    """
    if children:
        raise ContainerInventoryError(f"{path}: a CSV body has no children, asked for {children}")
    raw = _utf8_body(path.read_bytes(), record)
    body = raw[len(_UTF8_BOM) :] if raw.startswith(_UTF8_BOM) else raw
    header: tuple[str, ...] = ()
    if body.strip() and not body.strip().startswith(b"<"):
        header = tuple(
            name.strip()
            for name in pl.read_csv(io.BytesIO(body), n_rows=0, infer_schema_length=0).columns
        )
    del body
    frame = read_csv_bronze_body(raw, expected_columns=header, source_label=str(path))
    del raw
    yield ChildTable(child_id="", header=header, frame=frame)


# ---------------------------------------------------------------------------
# Container readers (unit X, ADR-037 P-5..P-7)
# ---------------------------------------------------------------------------


def _resource_for(path: Path) -> ResourceEntry:
    """The registry resource of a body, from its sidecar ``resource_id`` (P-5).

    The registry is looked up through ``registry.load_registry`` at call time
    (unit A's test seam).

    Raises:
        ContainerInventoryError: The sidecar is unreadable or names no
            registry resource.
    """
    from gridflow.connectors.neso_data_portal import registry as registry_module

    sidecar = path.with_suffix(SIDECAR_SUFFIX)
    try:
        meta: Any = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContainerInventoryError(
            f"{path}: sidecar {sidecar.name} is unreadable ({exc})"
        ) from exc
    params = meta.get("request_params") if isinstance(meta, dict) else None
    resource_id = params.get("resource_id") if isinstance(params, dict) else None
    entry = registry_module.load_registry().resources.get(str(resource_id))
    if entry is None:
        raise ContainerInventoryError(
            f"{path}: sidecar resource_id {resource_id!r} is not a registry resource; its "
            "child inventory cannot be checked"
        )
    return entry[1]


def _check_inventory(path: Path, data: bytes, resource: ResourceEntry) -> None:
    """P-5: the body's children are exactly the resource's registry inventory."""
    found = set(list_children(data, str(path)))
    declared = {child.child for child in resource.children}
    if found != declared:
        raise ContainerInventoryError(
            f"{path}: resource {resource.id} body children differ from the registry "
            f"inventory: unlisted {sorted(found - declared)}, missing {sorted(declared - found)}"
        )


class XlsxBlockError(Exception):
    """A sheet's table breaks one of the ``xlsx`` block rules (P-6 (a)-(g), AS-5c (h)-(n))."""


@dataclass(frozen=True)
class _Cell:
    kind: str  # "string" (shared/inline), "text" (formula string), "number", "bool",
    # "error", "uncached", "other"
    value: str

    @property
    def populated(self) -> bool:
        return self.kind in ("error", "uncached") or self.value != ""


def _ref(ref: str) -> tuple[int, int]:
    """``"AB12"`` -> ``(row 12, column 28)``."""
    split = 0
    while split < len(ref) and ref[split].isalpha():
        split += 1
    letters, digits = ref[:split], ref[split:]
    if not letters or not digits.isdigit():
        raise XlsxBlockError(f"cell reference {ref!r} does not parse")
    return int(digits), column_index(letters.upper())


def _cell_name(row: int, column: int) -> str:
    letters = ""
    while column:
        column, rest = divmod(column - 1, 26)
        letters = chr(ord("A") + rest) + letters
    return f"{letters}{row}"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _text_of(element: ET.Element) -> str:
    """The concatenated ``<t>`` text of a string item, phonetic runs excluded."""
    parts: list[str] = []

    def walk(node: ET.Element) -> None:
        for child in node:
            name = _local(child.tag)
            if name == "rPh":
                continue
            if name == "t":
                parts.append(child.text or "")
            else:
                walk(child)

    walk(element)
    return "".join(parts)


def _shared_strings(container: Container) -> list[str]:
    if not container.has(_SHARED_STRINGS):
        return []
    root = ET.fromstring(read_xml_part(container, _SHARED_STRINGS))
    return [_text_of(item) for item in root if _local(item.tag) == "si"]


@dataclass
class _Sheet:
    cells: dict[tuple[int, int], _Cell]
    merges: list[tuple[str, int, int, int, int]]


def _parse_sheet(raw: bytes, shared: list[str], label: str) -> _Sheet:
    """Every populated cell and every merged range of one worksheet part (stdlib)."""
    cells: dict[tuple[int, int], _Cell] = {}
    merges: list[tuple[str, int, int, int, int]] = []
    row_number = 0
    column_number = 0
    try:
        for event, element in ET.iterparse(io.BytesIO(raw), events=("start", "end")):
            name = _local(element.tag)
            if event == "start":
                if name == "row":
                    given = element.get("r")
                    row_number = int(given) if given else row_number + 1
                    column_number = 0
                continue
            if name == "c":
                given = element.get("r")
                if given:
                    row, column = _ref(given)
                else:
                    row, column = row_number, column_number + 1
                column_number = column
                cell = _read_cell(element, shared, label)
                if cell is not None:
                    cells[(row, column)] = cell
                element.clear()
            elif name == "mergeCell":
                ref = element.get("ref", "")
                first, _, last = ref.partition(":")
                top, left = _ref(first)
                bottom, right = _ref(last) if last else (top, left)
                merges.append((ref, top, left, bottom, right))
            elif name == "row":
                element.clear()
    except ET.ParseError as exc:
        raise XlsxBlockError(f"{label}: worksheet does not parse ({exc})") from exc
    return _Sheet(cells, merges)


def _read_cell(element: ET.Element, shared: list[str], label: str) -> _Cell | None:
    kind = element.get("t", "n")
    value: str | None = None
    formula = False
    inline: str | None = None
    for child in element:
        name = _local(child.tag)
        if name == "v":
            value = child.text or ""
        elif name == "f":
            formula = True
        elif name == "is":
            inline = _text_of(child)
    if formula and value is None:
        return _Cell("uncached", "")
    if kind == "e":
        return _Cell("error", value or "")
    if kind == "inlineStr":
        return _Cell("string", inline or "") if inline is not None else None
    if value is None:
        return None
    if kind == "s":
        try:
            return _Cell("string", shared[int(value)])
        except (ValueError, IndexError) as exc:
            raise XlsxBlockError(f"{label}: shared string index {value!r} is invalid") from exc
    if kind == "str":
        return _Cell("text", value)
    if kind == "b":
        return _Cell("bool", value)
    if kind == "n":
        return _Cell("number", value)
    return _Cell("other", value)


def _fail(rule: str, label: str, message: str) -> XlsxBlockError:
    return XlsxBlockError(f"{label}: xlsx rule ({rule}): {message}")


def _check_block(sheet: _Sheet, spec: XlsxSpec, label: str) -> tuple[tuple[str, ...], int]:
    """P-6 (a)-(f) over the stdlib parse; returns the raw header and the end row."""
    header_row = spec.header_row
    first, last = spec.bounds
    cells = sheet.cells
    populated_rows = [row for (row, _col), cell in cells.items() if cell.populated]
    if spec.last_row is not None:
        end = spec.last_row
    else:
        end = max([row for row in populated_rows if row >= header_row], default=header_row)

    header: list[str] = []
    for column in range(first, last + 1):
        cell = cells.get((header_row, column))
        where = _cell_name(header_row, column)
        if cell is None or not cell.populated:
            raise _fail("a", label, f"header cell {where} is empty")
        if cell.kind != "string":
            raise _fail("a", label, f"header cell {where} is not a string ({cell.kind})")
        if cell.value.strip() in {name.strip() for name in header}:
            raise _fail("a", label, f"header cell {where} repeats {cell.value!r}")
        header.append(cell.value)

    for ref, top, left, bottom, right in sheet.merges:
        if top <= end and bottom >= header_row and left <= last and right >= first:
            raise _fail("b", label, f"merged range {ref} intersects the block")

    in_block = sorted(
        (row, column)
        for (row, column) in cells
        if header_row <= row <= end and first <= column <= last
    )
    for row, column in in_block:
        cell = cells[(row, column)]
        if cell.kind == "error":
            raise _fail("c", label, f"error cell {_cell_name(row, column)} ({cell.value})")
        if cell.kind == "uncached":
            raise _fail("c", label, f"formula cell {_cell_name(row, column)} has no cached value")

    filled = {row for (row, column) in in_block if cells[(row, column)].populated}
    for row in range(header_row + 1, end + 1):
        if row not in filled:
            raise _fail("d", label, f"data row {row} is empty across {spec.columns}")

    for (row, column), cell in sorted(cells.items()):
        if header_row <= row <= end and not first <= column <= last and cell.populated:
            raise _fail(
                "e", label, f"cell {_cell_name(row, column)} outside {spec.columns} is populated"
            )

    if spec.last_row is not None:
        for column in range(first, last + 1):
            cell = cells.get((spec.last_row + 1, column))
            if cell is not None and cell.populated:
                raise _fail(
                    "f",
                    label,
                    f"cell {_cell_name(spec.last_row + 1, column)} below last_row "
                    f"{spec.last_row} is populated (the table outgrew its range)",
                )
    return tuple(header), end


def _header_units(
    sheet: _Sheet, block: XlsxBlock, label: str
) -> tuple[dict[tuple[int, int], int], list[str]]:
    """Rule (i) over every merge; each header merge's cells -> its anchor value (rule (j))."""
    first, last = block.bounds
    unit_of: dict[tuple[int, int], int] = {}
    anchors: list[str] = []
    for ref, top, left, bottom, right in sheet.merges:
        if (
            top <= block.data_last
            and bottom >= block.data_first
            and left <= last
            and right >= first
        ):
            raise _fail("i", label, f"merged range {ref} intersects the data rows")
        if not (top <= block.header_last and bottom >= block.header_first):
            continue
        if not (
            block.header_first <= top
            and bottom <= block.header_last
            and first <= left
            and right <= last
        ):
            raise _fail(
                "i",
                label,
                f"merged range {ref} leaves the header rows "
                f"{block.header_first}-{block.header_last} x {block.columns}",
            )
        anchor = sheet.cells.get((top, left))
        value = anchor.value.strip() if anchor is not None else ""
        for row in range(top, bottom + 1):
            for column in range(left, right + 1):
                unit_of[(row, column)] = len(anchors)
                other = sheet.cells.get((row, column))
                if (
                    (row, column) != (top, left)
                    and other is not None
                    and other.populated
                    and other.value.strip() != value
                ):
                    raise _fail(
                        "j",
                        label,
                        f"merged cell {_cell_name(row, column)} {other.value!r} differs from "
                        f"its anchor's {value!r} ({ref})",
                    )
        anchors.append(value)
    return unit_of, anchors


def _check_composite(sheet: _Sheet, block: XlsxBlock, label: str) -> tuple[str, ...]:
    """AS-5c rules (h)-(n) over the stdlib parse; returns one label per column.

    A column's label is its header rows' units top to bottom (a header merge
    is one unit valued by its stripped anchor cell; any other cell is its own
    stripped value), the non-empty ones joined with :data:`LABEL_SEPARATOR`.
    """
    first, last = block.bounds
    cells = sheet.cells
    for (row, column), cell in sorted(cells.items()):
        if (
            block.header_first <= row <= block.header_last
            and first <= column <= last
            and cell.populated
            and cell.kind != "string"
        ):
            raise _fail(
                "h", label, f"header cell {_cell_name(row, column)} is not a string ({cell.kind})"
            )

    unit_of, anchors = _header_units(sheet, block, label)
    labels: list[str] = []
    for column in range(first, last + 1):
        parts: list[str] = []
        used: set[int] = set()
        for row in range(block.header_first, block.header_last + 1):
            unit = unit_of.get((row, column))
            if unit is not None:
                if unit in used:
                    continue
                used.add(unit)
                value = anchors[unit]
            else:
                stacked = cells.get((row, column))
                value = stacked.value.strip() if stacked is not None else ""
            if value:
                parts.append(value)
        name = LABEL_SEPARATOR.join(parts)
        where = f"{_cell_name(block.header_first, column)}:{_cell_name(block.header_last, column)}"
        if not name:
            raise _fail("j", label, f"header column {where} has an empty label")
        if name in labels:
            raise _fail("j", label, f"header column {where} repeats the label {name!r}")
        labels.append(name)

    filled: set[int] = set()
    for (row, column), cell in sorted(cells.items()):
        if (
            block.data_first <= row <= block.data_last
            and first <= column <= last
            and cell.populated
        ):
            if cell.kind not in ("string", "text", "number"):
                raise _fail(
                    "k", label, f"data cell {_cell_name(row, column)} is {cell.kind} ({cell.value})"
                )
            filled.add(row)
    for row in range(block.data_first, block.data_last + 1):
        if row not in filled:
            raise _fail("l", label, f"data row {row} is empty across {block.columns}")
    for (row, column), cell in sorted(cells.items()):
        if (
            block.header_first <= row <= block.data_last
            and not first <= column <= last
            and cell.populated
        ):
            raise _fail(
                "m", label, f"cell {_cell_name(row, column)} outside {block.columns} is populated"
            )
    below = cells.get((block.data_last + 1, first))
    if below is not None and below.populated:
        raise _fail(
            "n",
            label,
            f"cell {_cell_name(block.data_last + 1, first)} below data_last {block.data_last} "
            "is populated (the block stops early)",
        )
    return tuple(labels)


def _sheet_part(container: Container, sheet: str) -> str:
    for name, part in workbook_sheets(container):
        if name == sheet:
            if part is None:
                raise ContainerReadError(
                    f"{container.label}: sheet {sheet!r} has no worksheet part"
                )
            return part
    raise ContainerInventoryError(f"{container.label}: no sheet named {sheet!r}")


def read_sheet(
    container: Container, sheet: str, spec: XlsxSpec, child_id: str, crc32: int | None = None
) -> ChildTable:
    """Read one sheet's table under P-6 from a workbook every entry of which is verified.

    Every entry of the workbook is read through ``read_entry`` first, so a part
    with a false size, a truncated stream or a bad CRC raises before calamine
    decompresses anything.

    Args:
        container: The opened workbook.
        sheet: The sheet name.
        spec: The table's position.
        child_id: The child id the table is yielded under.
        crc32: The child's CRC; ``None`` = the worksheet part's.

    Returns:
        The all-``Utf8`` table, columns = the stripped header.

    Raises:
        ContainerReadError: A workbook entry fails verification, or a part
            carries a DOCTYPE.
        XlsxBlockError: The first P-6 rule the sheet breaks.
    """
    label = f"{container.label}::{sheet}"
    verify_all_entries(container)
    part = _sheet_part(container, sheet)
    parsed = _parse_sheet(read_xml_part(container, part), _shared_strings(container), label)
    raw_header, end = _check_block(parsed, spec, label)
    rows = end - spec.header_row
    frame = pl.read_excel(
        io.BytesIO(container.data),
        engine="calamine",
        sheet_name=sheet,
        read_options={
            "header_row": spec.header_row - 1,
            "use_columns": spec.columns,
            "n_rows": rows,
        },
        infer_schema_length=0,
        drop_empty_rows=False,
        drop_empty_cols=False,
        raise_if_empty=False,
    )
    if tuple(frame.columns) != raw_header:
        raise _fail("g", label, f"calamine header {frame.columns} differs from {list(raw_header)}")
    if frame.height != rows:
        raise _fail("g", label, f"calamine read {frame.height} rows, the block holds {rows}")
    header = tuple(name.strip() for name in raw_header)
    frame = frame.rename(dict(zip(frame.columns, header, strict=True))).cast(pl.Utf8)
    return ChildTable(
        child_id=child_id,
        header=header,
        frame=frame,
        crc32=crc32 if crc32 is not None else container.info(part).CRC,
    )


def read_block(
    container: Container, block: XlsxBlock, child_id: str, crc32: int | None = None
) -> ChildTable:
    """Read one composite block's stored cell text (AS-5c P-4), with no calamine.

    Every workbook entry is verified first. Rows are the declared data rows by
    position only, in row order; a cell's value is its stored text exactly (a
    number's ``<v>`` text, a string's decoded text, a formula string's cached
    text: no strip, no reformat). An absent cell or an empty value is null.

    Args:
        container: The opened workbook.
        block: The block's position.
        child_id: The child id the table is yielded under.
        crc32: The child's CRC; ``None`` = the worksheet part's.

    Returns:
        The all-``Utf8`` table, columns = the block's labels.

    Raises:
        ContainerReadError: A workbook entry fails verification, or a part
            carries a DOCTYPE.
        XlsxBlockError: The first rule (h)-(n) the block breaks.
    """
    label = f"{container.label}::{block.sheet}"
    verify_all_entries(container)
    part = _sheet_part(container, block.sheet)
    parsed = _parse_sheet(read_xml_part(container, part), _shared_strings(container), label)
    labels = _check_composite(parsed, block, label)
    first, _last = block.bounds
    values: list[list[str | None]] = [[] for _ in labels]
    for row in range(block.data_first, block.data_last + 1):
        for offset, column_values in enumerate(values):
            cell = parsed.cells.get((row, first + offset))
            column_values.append(cell.value if cell is not None and cell.value != "" else None)
    frame = pl.DataFrame(
        dict(zip(labels, values, strict=True)), schema=dict.fromkeys(labels, pl.Utf8)
    )
    return ChildTable(
        child_id=child_id,
        header=labels,
        frame=frame,
        crc32=crc32 if crc32 is not None else container.info(part).CRC,
    )


def _read_blocks(
    path: Path, blocks: tuple[XlsxBlock, ...], children: tuple[str, ...]
) -> Iterator[ChildTable]:
    """The declared block of the body's resource, for each requested child (AS-5c P-2)."""
    data = path.read_bytes()
    resource = _resource_for(path)
    _check_inventory(path, data, resource)
    block = next((item for item in blocks if item.resource_id == resource.id), None)
    if block is None:
        raise XlsxBlockError(
            f"{path}: resource {resource.id} has no declared xlsx block (V-20); no fallback"
        )
    container = open_container(data, str(path))
    for child in children:
        if CHILD_SEPARATOR in child:
            raise ContainerInventoryError(f"{path}: xlsx child {child!r} names a member")
        if child != block.sheet:
            raise XlsxBlockError(
                f"{path}: child {child!r} is not the declared block sheet {block.sheet!r}"
            )
        yield read_block(container, block, child)


def read_xlsx_body(
    path: Path, record: SchemaRecord, children: tuple[str, ...]
) -> Iterator[ChildTable]:
    """Read the requested sheets of one workbook body (P-5, P-6).

    A record with ``xlsx_blocks`` reads its resource's declared composite block
    instead (AS-5c P-2); a resource without a block fails, with no fallback.

    Raises:
        ContainerInventoryError: The body's sheets are not exactly the
            resource's registry inventory, or its resource is unresolvable.
        ContainerReadError: An entry fails verification.
        XlsxBlockError: A sheet breaks a P-6 rule, or a block breaks a rule
            (h)-(n), has no declaration, or is asked for another sheet.
    """
    if record.xlsx_blocks is not None:
        yield from _read_blocks(path, record.xlsx_blocks, children)
        return
    if record.xlsx is None:
        raise ContainerInventoryError(f"{path}: an xlsx record needs an xlsx spec (V-14)")
    data = path.read_bytes()
    _check_inventory(path, data, _resource_for(path))
    container = open_container(data, str(path))
    for child in children:
        if CHILD_SEPARATOR in child:
            raise ContainerInventoryError(f"{path}: xlsx child {child!r} names a member")
        yield read_sheet(container, child, record.xlsx, child)


def _csv_member_table(
    raw: bytes, record: SchemaRecord, label: str
) -> tuple[tuple[str, ...], pl.DataFrame]:
    """A CSV member's header and rows, exactly as :func:`read_csv_body` reads a body.

    Raises:
        UnicodeDecodeError: The member is not valid in ``record.encoding``.
    """
    raw = _utf8_body(raw, record)
    body = raw[len(_UTF8_BOM) :] if raw.startswith(_UTF8_BOM) else raw
    header: tuple[str, ...] = ()
    if body.strip() and not body.strip().startswith(b"<"):
        header = tuple(
            name.strip()
            for name in pl.read_csv(io.BytesIO(body), n_rows=0, infer_schema_length=0).columns
        )
    del body
    frame = read_csv_bronze_body(raw, expected_columns=header, source_label=label)
    return header, frame


def read_zip_member_body(
    path: Path, record: SchemaRecord, children: tuple[str, ...]
) -> Iterator[ChildTable]:
    """Read the requested members of one ZIP body (P-5, P-7).

    Raises:
        ContainerInventoryError: The body's children are not exactly the
            resource's registry inventory, its resource is unresolvable, or a
            requested member does not match ``member_pattern``.
        ContainerReadError: An entry fails verification, or a member is a
            nested ZIP (or not the workbook ``inner`` expects).
        XlsxBlockError: A member sheet breaks a P-6 rule.
    """
    spec = record.zip_member
    if spec is None:
        raise ContainerInventoryError(f"{path}: a zip_member record needs its spec (V-14)")
    data = path.read_bytes()
    _check_inventory(path, data, _resource_for(path))
    container = open_container(data, str(path))
    for child in children:
        member, separator, sheet = child.partition(CHILD_SEPARATOR)
        if re.fullmatch(spec.member_pattern, member) is None:
            raise ContainerInventoryError(
                f"{path}: member {member!r} does not match {spec.member_pattern!r}"
            )
        info = container.info(member)
        raw = read_entry(container, info)
        label = f"{path}::{member}"
        if spec.inner == "csv":
            if separator or is_zip_bytes(raw):
                raise ContainerReadError(f"{label}: not a CSV member (a ZIP or a workbook sheet)")
            header, frame = _csv_member_table(raw, record, label)
            del raw
            yield ChildTable(child_id=child, header=header, frame=frame, crc32=info.CRC)
            continue
        if record.xlsx is None or not separator:
            raise ContainerInventoryError(f"{label}: an xlsx member child is '<member>::<sheet>'")
        inner = open_container(raw, label) if is_zip_bytes(raw) else None
        if inner is None or not is_workbook(inner):
            raise ContainerReadError(f"{label}: member is not a workbook")
        yield read_sheet(inner, sheet, record.xlsx, child, crc32=info.CRC)


READERS: dict[str, BodyReader] = {
    "csv": read_csv_body,
    "xlsx": read_xlsx_body,
    "zip_member": read_zip_member_body,
}
"""Reader name -> reader."""

CONTAINER_READERS: frozenset[str] = frozenset({"xlsx", "zip_member"})
"""Readers whose bodies hold children; their rows carry ``child_id``."""


def read_children(
    path: Path, record: SchemaRecord, children: tuple[str, ...]
) -> Iterator[ChildTable]:
    """Read a body through its record's reader, enforcing the child contract.

    Args:
        path: The bronze body.
        record: The family's record.
        children: The child ids the inventory maps to this family (``()``
            for a plain body).

    Yields:
        Each table the reader yields, in its order.

    Raises:
        ReaderUnavailableError: No reader is registered for ``record.reader``.
        ContainerInventoryError: A container reader yielded a child twice, a
            child not requested, or missed a requested one.
    """
    reader = READERS.get(record.reader)
    if reader is None:
        raise ReaderUnavailableError(
            f"no reader is registered for {record.reader!r} (unit X); {path} cannot be read"
        )
    seen: list[str] = []
    for table in reader(path, record, children):
        if record.reader in CONTAINER_READERS and (
            table.child_id not in children or table.child_id in seen
        ):
            raise ContainerInventoryError(
                f"{path}: reader {record.reader!r} yielded child {table.child_id!r}, "
                f"requested {list(children)}"
            )
        seen.append(table.child_id)
        yield table
    if record.reader in CONTAINER_READERS and sorted(seen) != sorted(children):
        raise ContainerInventoryError(
            f"{path}: reader {record.reader!r} yielded {sorted(seen)}, requested {sorted(children)}"
        )
