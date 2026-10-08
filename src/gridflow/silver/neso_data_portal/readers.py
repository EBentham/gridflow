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
the record's epochs.
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
    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord, XlsxSpec

__all__ = [
    "CONTAINER_READERS",
    "READERS",
    "BodyReader",
    "ChildTable",
    "ContainerInventoryError",
    "ReaderUnavailableError",
    "XlsxBlockError",
    "read_children",
    "read_csv_body",
    "read_sheet",
    "read_xlsx_body",
    "read_zip_member_body",
]

_UTF8_BOM = b"\xef\xbb\xbf"
_SHARED_STRINGS = "xl/sharedStrings.xml"


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


def read_csv_body(
    path: Path, record: SchemaRecord, children: tuple[str, ...]
) -> Iterator[ChildTable]:
    """Read one CSV body with the record's encoding through unit A's reader.

    The body is decoded strictly with ``record.encoding`` and re-encoded as
    UTF-8 (a UTF-8 body is passed through: :func:`read_csv_bronze_body`
    validates it strictly itself, and a second copy of a large body would
    only cost memory). The header is parsed first and handed to the reader as
    its ``expected_columns``, so unit A's BOM, blank-row and markup rules all
    apply and the header-to-epoch match happens in P-4.

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
    raw = path.read_bytes()
    if codecs.lookup(record.encoding).name not in ("utf-8", "utf-8-sig"):
        raw = raw.decode(record.encoding, errors="strict").encode("utf-8")
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
    """A sheet's table breaks one of the ``xlsx`` block rules (P-6 (a)-(g))."""


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


def read_xlsx_body(
    path: Path, record: SchemaRecord, children: tuple[str, ...]
) -> Iterator[ChildTable]:
    """Read the requested sheets of one workbook body (P-5, P-6).

    Raises:
        ContainerInventoryError: The body's sheets are not exactly the
            resource's registry inventory, or its resource is unresolvable.
        ContainerReadError: An entry fails verification.
        XlsxBlockError: A sheet breaks a P-6 rule.
    """
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
    """A CSV member's header and rows, exactly as :func:`read_csv_body` reads a body."""
    if codecs.lookup(record.encoding).name not in ("utf-8", "utf-8-sig"):
        raw = raw.decode(record.encoding, errors="strict").encode("utf-8")
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
