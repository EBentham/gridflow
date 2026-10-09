"""Shared builders for the container tests (ADR-037 P-15).

Not a test module (no ``test_`` prefix). It holds:

- :func:`workbook` — a standard-library XLSX builder (inline-string and numeric
  cells, ``mergeCell``, ``t="e"`` cells, ``<f>`` without ``<v>``, N sheets, a
  repeated sheet name) for the negative cases;
- :func:`zip_bytes` and :func:`patch_headers` — ZIP builders and corrupters
  (repeated entry names, a wrong CRC, E-23's false size with the prefix CRC);
- :func:`forbid_zipfile_reads` — a fixture refusing every archive-module read
  whose caller is a NESO module, so a test proves X never decompresses through
  the standard archive reader (C-8).

Every test module of unit X applies the fixture with
``pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")``.
"""

from __future__ import annotations

import io
import struct
import sys
import warnings
import zipfile
import zlib
from typing import TYPE_CHECKING, Any
from xml.sax.saxutils import escape, quoteattr

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = "tests/fixtures/neso_data_portal/containers"

_GUARDED_PREFIXES = ("gridflow.silver.neso_data_portal", "gridflow.connectors.neso_data_portal")
_READ_METHODS = ("read", "open", "extract", "extractall", "testzip")
_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


@pytest.fixture
def forbid_zipfile_reads(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Refuse archive-module reads called from a NESO module; pass others through."""
    for method in _READ_METHODS:
        original = getattr(zipfile.ZipFile, method)

        def guarded(
            self: zipfile.ZipFile, *args: Any, _orig: Any = original, _name: str = method, **kw: Any
        ) -> Any:
            caller = sys._getframe(1).f_globals.get("__name__", "")
            if caller.startswith(_GUARDED_PREFIXES):
                raise AssertionError(f"{caller} called the archive module's {_name}()")
            return _orig(self, *args, **kw)

        monkeypatch.setattr(zipfile.ZipFile, method, guarded)
    yield


Cell = str | int | float | tuple[str, ...]
"""A cell value: a string (inline), a number, ``("e", "#DIV/0!")`` (an error),
``("f", "SUM(A1)")`` (a formula with no cached value), ``("fv", "SUM(A1)", "3")``
(a formula with a cached number) or ``("s", "text")`` (a shared string)."""


def _split_ref(ref: str) -> tuple[str, int]:
    letters = "".join(ch for ch in ref if ch.isalpha())
    return letters, int(ref[len(letters) :])


def _col_index(letters: str) -> int:
    index = 0
    for char in letters:
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index


def _cell_xml(ref: str, value: Cell, shared: list[str]) -> str:
    if isinstance(value, tuple):
        kind = value[0]
        if kind == "e":
            return f'<c r="{ref}" t="e"><v>{escape(value[1])}</v></c>'
        if kind == "f":
            return f'<c r="{ref}"><f>{escape(value[1])}</f></c>'
        if kind == "fv":
            return f'<c r="{ref}"><f>{escape(value[1])}</f><v>{escape(value[2])}</v></c>'
        if kind == "s":
            shared.append(value[1])
            return f'<c r="{ref}" t="s"><v>{len(shared) - 1}</v></c>'
        raise ValueError(f"unknown cell kind {kind!r}")
    if isinstance(value, str):
        return (
            f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{escape(value)}</t></is></c>'
        )
    return f'<c r="{ref}"><v>{value}</v></c>'


def _sheet_xml(cells: dict[str, Cell], merges: list[str], shared: list[str]) -> str:
    rows: dict[int, list[tuple[int, str, Cell]]] = {}
    for ref, value in cells.items():
        letters, row = _split_ref(ref)
        rows.setdefault(row, []).append((_col_index(letters), ref, value))
    body = []
    for row in sorted(rows):
        cells_xml = "".join(_cell_xml(ref, value, shared) for _i, ref, value in sorted(rows[row]))
        body.append(f'<row r="{row}">{cells_xml}</row>')
    merge_xml = ""
    if merges:
        inner = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{inner}</mergeCells>'
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(body)}</sheetData>{merge_xml}</worksheet>"
    )


def workbook(
    sheets: list[tuple[str, dict[str, Cell]]],
    *,
    merges: dict[str, list[str]] | None = None,
    doctype: bool = False,
) -> bytes:
    """Build an XLSX body with the standard library.

    Args:
        sheets: ``(sheet name, {cell ref: value})`` per sheet, in order; a
            name may repeat (for the refusal case).
        merges: Sheet name -> merged ranges.
        doctype: Put a ``<!DOCTYPE`` into the first worksheet part.

    Returns:
        The workbook bytes.
    """
    merges = merges or {}
    shared: list[str] = []
    parts: list[tuple[str, str]] = []
    sheet_entries = []
    rels = []
    overrides = []
    for index, (name, cells) in enumerate(sheets, start=1):
        xml = _sheet_xml(cells, merges.get(name, []), shared)
        if doctype and index == 1:
            xml = xml.replace("<worksheet", "<!DOCTYPE worksheet []><worksheet", 1)
        parts.append((f"xl/worksheets/sheet{index}.xml", xml))
        sheet_entries.append(f'<sheet name={quoteattr(name)} sheetId="{index}" r:id="rId{index}"/>')
        rels.append(
            f'<Relationship Id="rId{index}" '
            f'Type="{_REL}/worksheet" '
            f'Target="worksheets/sheet{index}.xml"/>'
        )
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{index}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    if shared:
        items = "".join(f'<si><t xml:space="preserve">{escape(text)}</t></si>' for text in shared)
        parts.append(
            (
                "xl/sharedStrings.xml",
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                f'count="{len(shared)}" uniqueCount="{len(shared)}">{items}</sst>',
            )
        )
        rid = len(sheets) + 1
        rels.append(
            f'<Relationship Id="rId{rid}" Type="{_REL}/sharedStrings" Target="sharedStrings.xml"/>'
        )
        overrides.append(
            '<Override PartName="/xl/sharedStrings.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
        )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" '
        'ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        f"{''.join(overrides)}</Types>"
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f"<sheets>{''.join(sheet_entries)}</sheets></workbook>"
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        f"{''.join(rels)}</Relationships>"
    )
    entries = [
        ("[Content_Types].xml", content_types),
        ("_rels/.rels", root_rels),
        ("xl/workbook.xml", workbook_xml),
        ("xl/_rels/workbook.xml.rels", workbook_rels),
        *parts,
    ]
    return zip_bytes([(name, text.encode("utf-8")) for name, text in entries])


def zip_bytes(
    entries: list[tuple[str, bytes]],
    *,
    method: int = zipfile.ZIP_DEFLATED,
    dirs: tuple[str, ...] = (),
) -> bytes:
    """Build a ZIP; a repeated name is written twice (the shadowing case)."""
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(buffer, "w", compression=method) as archive:
            for directory in dirs:
                archive.writestr(zipfile.ZipInfo(directory), b"")
            for name, data in entries:
                archive.writestr(name, data)
    return buffer.getvalue()


def _central_offsets(data: bytes) -> dict[str, tuple[int, int]]:
    """Entry name -> (central header offset, local header offset); first occurrence."""
    out: dict[str, tuple[int, int]] = {}
    position = data.find(b"PK\x01\x02")
    while position != -1:
        nlen, xlen, clen = struct.unpack_from("<HHH", data, position + 28)
        (local,) = struct.unpack_from("<I", data, position + 42)
        name = data[position + 46 : position + 46 + nlen].decode("utf-8")
        out.setdefault(name, (position, local))
        position = data.find(b"PK\x01\x02", position + 46 + nlen + xlen + clen)
    return out


def patch_headers(
    data: bytes,
    name: str,
    *,
    crc: int | None = None,
    compress_size: int | None = None,
    file_size: int | None = None,
    method: int | None = None,
    local: bool = True,
    central: bool = True,
) -> bytes:
    """Overwrite header fields of one entry in its local and/or central header."""
    buffer = bytearray(data)
    central_at, local_at = _central_offsets(data)[name]
    targets = []
    if local:
        targets.append((local_at, 8, 14, 18, 22))
    if central:
        targets.append((central_at, 10, 16, 20, 24))
    for base, method_at, crc_at, csize_at, usize_at in targets:
        if method is not None:
            struct.pack_into("<H", buffer, base + method_at, method)
        if crc is not None:
            struct.pack_into("<I", buffer, base + crc_at, crc)
        if compress_size is not None:
            struct.pack_into("<I", buffer, base + csize_at, compress_size)
        if file_size is not None:
            struct.pack_into("<I", buffer, base + usize_at, file_size)
    return bytes(buffer)


def false_size_zip() -> tuple[bytes, bytes, bytes]:
    """E-23's attack: a 23-byte deflated CSV whose headers declare 16 B and the prefix CRC.

    Returns:
        ``(archive, full content, prefix)``; the standard reader returns the prefix.
    """
    content = b"a,b\n1,2\n3,4\n5,6\n7,8\n9,0\n"[:23]
    prefix = content[:16]
    archive = zip_bytes([("data.csv", content)])
    archive = patch_headers(archive, "data.csv", file_size=16, crc=zlib.crc32(prefix))
    return archive, content, prefix


def rezip(data: bytes, extra: list[tuple[str, bytes]]) -> bytes:
    """Copy every entry of ``data`` (read with the standard module in test code) plus ``extra``."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = [(info.filename, archive.read(info)) for info in archive.infolist()]
    return zip_bytes([*entries, *extra])
