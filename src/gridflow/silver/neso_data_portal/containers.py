"""The container gate of the NESO generic engine: ZIP and workbook bodies (ADR-037).

Every ZIP an X module touches (an outer body, a workbook body, a workbook
member, a GIS member at any nesting level) is opened by :func:`open_container`
and its entries are read only by :func:`read_entry`. Python's archive module
trusts the sizes a header declares (a deflated entry whose headers declare a
prefix length and the prefix's CRC reads back as the prefix, silently, C-8), so
nothing here decompresses through it: :func:`read_entry` inflates with ``zlib``
directly and proves the stream complete, the length exact and the CRC equal.

:func:`list_children` is the one definition of a body's children (P-3); the
readers, the audit CLI and the inventory build all use it.

**Audit CLI** (read-only)::

    python -m gridflow.silver.neso_data_portal.containers audit [--data-dir D]

For every registry resource with children it reads the newest usable capture,
reads every entry at every level through :func:`read_entry`, compares
:func:`list_children` with the registry inventory and prints one line per
resource plus ``SUMMARY ok=N mismatched=M uncaptured=U``; exit 1 when
``M + U > 0``.
"""

from __future__ import annotations

import argparse
import io
import struct
import sys
import xml.etree.ElementTree as ET
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from gridflow.connectors.neso_data_portal.captures import Capture

__all__ = [
    "CHILD_SEPARATOR",
    "MAX_ENTRIES",
    "MAX_ENTRY_BYTES",
    "MAX_TOTAL_BYTES",
    "Container",
    "ContainerCapError",
    "ContainerReadError",
    "audit",
    "is_workbook",
    "is_zip_bytes",
    "list_children",
    "main",
    "open_container",
    "read_entry",
    "read_xml_part",
    "verify_all_entries",
    "workbook_sheets",
]

MAX_ENTRIES = 1_000
MAX_ENTRY_BYTES = 256 * 1024 * 1024
MAX_TOTAL_BYTES = 1024 * 1024 * 1024

CHILD_SEPARATOR = "::"
"""Joins a workbook member's name to one of its sheets in a child id."""

WORKBOOK_PART = "xl/workbook.xml"
_WORKBOOK_RELS = "xl/_rels/workbook.xml.rels"
_LOCAL_HEADER = struct.Struct("<4sHHHHHIIIHH")
_LOCAL_SIGNATURE = b"PK\x03\x04"
_ZIP_SIGNATURES = (b"PK\x03\x04", b"PK\x05\x06")
_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_REL_NS_STRICT = "http://purl.oclc.org/ooxml/officeDocument/relationships"


class ContainerReadError(Exception):
    """A container or one of its entries cannot be read exactly as declared."""


class ContainerCapError(ContainerReadError):
    """A container's declared sizes exceed the caps; nothing was decompressed."""


@dataclass(frozen=True)
class Container:
    """One opened ZIP: its bytes and its central-directory entries.

    Attributes:
        data: The whole archive.
        infos: Every entry, in central-directory order; names are unique.
        label: What the archive is, for error messages.
    """

    data: bytes
    infos: tuple[zipfile.ZipInfo, ...]
    label: str

    def info(self, name: str) -> zipfile.ZipInfo:
        """Return the entry named ``name``.

        Raises:
            ContainerReadError: No entry has that name.
        """
        for item in self.infos:
            if item.filename == name:
                return item
        raise ContainerReadError(f"{self.label}: no entry named {name!r}")

    def has(self, name: str) -> bool:
        """Whether an entry named ``name`` exists."""
        return any(item.filename == name for item in self.infos)

    def files(self) -> tuple[zipfile.ZipInfo, ...]:
        """Every entry that is not a directory, in central-directory order."""
        return tuple(item for item in self.infos if not item.is_dir())


def is_zip_bytes(data: bytes) -> bool:
    """Whether ``data`` starts with a ZIP local-header or empty-archive signature."""
    return data[:4] in _ZIP_SIGNATURES


def open_container(data: bytes, label: str = "container") -> Container:
    """Parse a ZIP's central directory and apply the caps before any decompression.

    Args:
        data: The archive bytes.
        label: Names the archive in errors.

    Returns:
        The opened container.

    Raises:
        ContainerReadError: The central directory does not parse, or two
            entries share a name (one would shadow the other).
        ContainerCapError: The entry count, an entry's declared size or the
            declared total exceeds :data:`MAX_ENTRIES`,
            :data:`MAX_ENTRY_BYTES` or :data:`MAX_TOTAL_BYTES`.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            infos = tuple(archive.infolist())
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError, ValueError, EOFError) as exc:
        raise ContainerReadError(f"{label}: not a readable ZIP ({exc})") from exc
    names = [item.filename for item in infos]
    if len(set(names)) != len(names):
        repeated = sorted({name for name in names if names.count(name) > 1})
        raise ContainerReadError(f"{label}: entry name(s) {repeated} repeat; one would shadow")
    if len(infos) > MAX_ENTRIES:
        raise ContainerCapError(f"{label}: {len(infos)} entries exceed the cap {MAX_ENTRIES}")
    total = 0
    for item in infos:
        if item.file_size > MAX_ENTRY_BYTES:
            raise ContainerCapError(
                f"{label}: entry {item.filename!r} declares {item.file_size} B, over the cap "
                f"{MAX_ENTRY_BYTES}"
            )
        total += item.file_size
    if total > MAX_TOTAL_BYTES:
        raise ContainerCapError(
            f"{label}: entries declare {total} B in total, over the cap {MAX_TOTAL_BYTES}"
        )
    return Container(data=data, infos=infos, label=label)


def read_entry(container: Container, info: zipfile.ZipInfo) -> bytes:
    """Decompress one entry and prove it exactly as the central directory declares.

    Args:
        container: The opened archive.
        info: One of its entries.

    Returns:
        The entry's bytes.

    Raises:
        ContainerReadError: The local header disagrees with the central
            directory, the entry is encrypted or uses a method other than
            STORED/DEFLATED, the compressed slice is short, the stream does
            not end exactly at the declared size, or the CRC differs.
    """
    where = f"{container.label}: entry {info.filename!r}"
    data = container.data
    offset = info.header_offset
    header = data[offset : offset + _LOCAL_HEADER.size]
    if len(header) != _LOCAL_HEADER.size:
        raise ContainerReadError(f"{where}: local header is truncated")
    (signature, _version, flags, method, _time, _date, crc, csize, usize, nlen, xlen) = (
        _LOCAL_HEADER.unpack(header)
    )
    if signature != _LOCAL_SIGNATURE:
        raise ContainerReadError(f"{where}: no local header signature at {offset}")
    if method != info.compress_type:
        raise ContainerReadError(
            f"{where}: local method {method} differs from the central {info.compress_type}"
        )
    if flags & 0x1 or info.flag_bits & 0x1:
        raise ContainerReadError(f"{where}: encrypted entries are refused")
    if not flags & 0x8 and (crc, csize, usize) != (info.CRC, info.compress_size, info.file_size):
        raise ContainerReadError(
            f"{where}: local CRC/sizes {(crc, csize, usize)} differ from the central "
            f"{(info.CRC, info.compress_size, info.file_size)}"
        )
    start = offset + _LOCAL_HEADER.size + nlen + xlen
    compressed = data[start : start + info.compress_size]
    if len(compressed) != info.compress_size:
        raise ContainerReadError(
            f"{where}: compressed slice is {len(compressed)} B, declared {info.compress_size} B"
        )
    if method == zipfile.ZIP_STORED:
        out = compressed
    elif method == zipfile.ZIP_DEFLATED:
        inflater = zlib.decompressobj(-15)
        try:
            out = inflater.decompress(compressed, info.file_size + 1)
        except zlib.error as exc:
            raise ContainerReadError(f"{where}: deflate stream is corrupt ({exc})") from exc
        if not inflater.eof or inflater.unused_data or inflater.unconsumed_tail:
            raise ContainerReadError(
                f"{where}: stream not exactly consumed (eof={inflater.eof}, "
                f"unused={len(inflater.unused_data)} B, "
                f"unconsumed={len(inflater.unconsumed_tail)} B)"
            )
    else:
        raise ContainerReadError(f"{where}: compression method {method} is refused")
    if len(out) != info.file_size:
        raise ContainerReadError(f"{where}: inflated to {len(out)} B, declared {info.file_size} B")
    if zlib.crc32(out) != info.CRC:
        raise ContainerReadError(f"{where}: CRC {zlib.crc32(out)} differs from declared {info.CRC}")
    return out


def verify_all_entries(container: Container) -> None:
    """Read every entry through :func:`read_entry`, discarding the output.

    Raises:
        ContainerReadError: Any entry fails :func:`read_entry`.
    """
    for info in container.infos:
        read_entry(container, info)


def read_xml_part(container: Container, name: str) -> bytes:
    """Read one XML part through :func:`read_entry` and refuse a DTD.

    Raises:
        ContainerReadError: The part is missing, fails :func:`read_entry`, or
            carries ``<!DOCTYPE``.
    """
    raw = read_entry(container, container.info(name))
    if b"<!DOCTYPE" in raw:
        raise ContainerReadError(f"{container.label}: part {name!r} carries a DOCTYPE")
    return raw


def is_workbook(container: Container) -> bool:
    """Whether the archive is an Excel workbook (it holds ``xl/workbook.xml``)."""
    return container.has(WORKBOOK_PART)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _resolve_target(target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    parts: list[str] = ["xl"]
    for piece in PurePosixPath(target).parts:
        if piece == "..":
            if parts:
                parts.pop()
        elif piece != ".":
            parts.append(piece)
    return "/".join(parts)


def workbook_sheets(container: Container) -> tuple[tuple[str, str | None], ...]:
    """Return ``(sheet name, worksheet part)`` per sheet, in ``workbook.xml`` order.

    The part is ``None`` when the workbook relationships do not resolve the
    sheet to an entry of the archive.

    Raises:
        ContainerReadError: ``xl/workbook.xml`` is missing or unreadable, or a
            sheet name repeats.
    """
    try:
        root = ET.fromstring(read_xml_part(container, WORKBOOK_PART))
    except ET.ParseError as exc:
        raise ContainerReadError(f"{container.label}: workbook.xml does not parse ({exc})") from exc
    targets: dict[str, str] = {}
    if container.has(_WORKBOOK_RELS):
        try:
            rels = ET.fromstring(read_xml_part(container, _WORKBOOK_RELS))
        except ET.ParseError as exc:
            raise ContainerReadError(
                f"{container.label}: workbook relationships do not parse ({exc})"
            ) from exc
        for rel in rels:
            rel_id, target = rel.get("Id"), rel.get("Target")
            if rel_id and target:
                targets[rel_id] = _resolve_target(target)
    sheets: list[tuple[str, str | None]] = []
    for element in root.iter():
        if _local(element.tag) != "sheet":
            continue
        name = element.get("name")
        if name is None:
            raise ContainerReadError(f"{container.label}: a sheet has no name")
        rel_id = element.get(f"{{{_REL_NS}}}id") or element.get(f"{{{_REL_NS_STRICT}}}id")
        part = targets.get(rel_id) if rel_id else None
        sheets.append((name, part if part is not None and container.has(part) else None))
    names = [name for name, _part in sheets]
    if len(set(names)) != len(names):
        raise ContainerReadError(f"{container.label}: a sheet name repeats in {names}")
    return tuple(sheets)


def _member_children(container: Container, info: zipfile.ZipInfo) -> list[str]:
    """The child ids one non-directory entry of a non-workbook archive contributes."""
    name = info.filename
    if CHILD_SEPARATOR in name:
        raise ContainerReadError(
            f"{container.label}: member name {name!r} contains {CHILD_SEPARATOR!r}"
        )
    raw = read_entry(container, info)
    if not is_zip_bytes(raw):
        return [name]
    inner = open_container(raw, f"{container.label}::{name}")
    if not is_workbook(inner):
        return [name]
    return [f"{name}{CHILD_SEPARATOR}{sheet}" for sheet, _part in workbook_sheets(inner)]


def list_children(data: bytes, label: str = "container") -> tuple[str, ...]:
    """Return a container body's child ids: the one definition (P-3).

    - A workbook: its sheet names, in ``workbook.xml`` order.
    - Any other ZIP: every non-directory entry; an entry that is itself a
      workbook (sniffed by content) expands to ``<member>::<sheet>`` per sheet;
      a nested non-workbook ZIP is listed as itself, never expanded.

    Args:
        data: The body bytes.
        label: Names the body in errors.

    Returns:
        The child ids, unique.

    Raises:
        ContainerReadError: The body or a member cannot be read, a member name
            holds ``::``, a sheet name repeats, or a child id repeats.
    """
    container = open_container(data, label)
    if is_workbook(container):
        children = [sheet for sheet, _part in workbook_sheets(container)]
    else:
        children = []
        for info in container.files():
            children.extend(_member_children(container, info))
    if len(set(children)) != len(children):
        raise ContainerReadError(f"{label}: a child id repeats in {children}")
    return tuple(children)


def _verify_deep(container: Container, depth: int) -> None:
    """Read every entry at every level (nested ZIPs to ``depth``) through ``read_entry``."""
    for info in container.infos:
        raw = read_entry(container, info)
        if depth > 0 and not info.is_dir() and is_zip_bytes(raw):
            _verify_deep(open_container(raw, f"{container.label}::{info.filename}"), depth - 1)


@dataclass(frozen=True)
class AuditLine:
    """One resource's audit outcome."""

    resource_id: str
    status: str
    detail: str

    def line(self) -> str:
        """The printed line."""
        return f"{self.status.upper()} {self.resource_id} {self.detail}"


def audit(data_dir: Path) -> list[AuditLine]:
    """Compare every registry inventory with its newest capture (read-only).

    Args:
        data_dir: The data root holding ``bronze/neso_data_portal``.

    Returns:
        One :class:`AuditLine` per resource with children: ``ok``,
        ``mismatched`` or ``uncaptured``.
    """
    from gridflow.connectors.neso_data_portal import registry as registry_module
    from gridflow.connectors.neso_data_portal.captures import newest_by_resource, scan_dataset
    from gridflow.storage.paths import PathBuilder

    registry = registry_module.load_registry()
    paths = PathBuilder(data_dir)
    scans: dict[str, dict[str, Capture]] = {}
    lines: list[AuditLine] = []
    for resource_id in sorted(registry.resources):
        _package, resource = registry.resources[resource_id]
        if not resource.children:
            continue
        if resource.family not in scans:
            scan = scan_dataset(
                paths.bronze_dir("neso_data_portal", resource.family),
                registry,
                require_provenance=False,
            )
            scans[resource.family] = newest_by_resource(scan.captures)
        capture = scans[resource.family].get(resource_id)
        if capture is None:
            lines.append(AuditLine(resource_id, "uncaptured", f"family {resource.family}"))
            continue
        body = capture.body
        try:
            raw = body.read_bytes()
            _verify_deep(open_container(raw, str(body)), depth=2)
            found = set(list_children(raw, str(body)))
        except ContainerReadError as exc:
            lines.append(AuditLine(resource_id, "mismatched", f"refused: {exc}"))
            continue
        declared = {child.child for child in resource.children}
        if found != declared:
            lines.append(
                AuditLine(
                    resource_id,
                    "mismatched",
                    f"unlisted {sorted(found - declared)} missing {sorted(declared - found)}",
                )
            )
        else:
            lines.append(AuditLine(resource_id, "ok", f"{len(found)} children"))
    return lines


def _iter_output(lines: Sequence[AuditLine]) -> Iterator[str]:
    yield from (item.line() for item in lines)
    ok = sum(item.status == "ok" for item in lines)
    mismatched = sum(item.status == "mismatched" for item in lines)
    uncaptured = sum(item.status == "uncaptured" for item in lines)
    yield f"SUMMARY ok={ok} mismatched={mismatched} uncaptured={uncaptured}"


def main(argv: Sequence[str] | None = None) -> int:
    """The audit CLI; returns the exit code."""
    parser = argparse.ArgumentParser(prog="python -m gridflow.silver.neso_data_portal.containers")
    sub = parser.add_subparsers(dest="command", required=True)
    audit_parser = sub.add_parser("audit", help="compare inventories with newest captures")
    audit_parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    data_dir = args.data_dir
    if data_dir is None:
        from gridflow.config.settings import load_settings

        data_dir = load_settings().pipeline.data_dir
    lines = audit(data_dir)
    for text in _iter_output(lines):
        print(text)
    return 1 if any(item.status != "ok" for item in lines) else 0


if __name__ == "__main__":
    sys.exit(main())
