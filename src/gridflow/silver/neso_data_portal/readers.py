"""The body-reader seam of the NESO generic engine (ADR-034 P-3).

A reader turns one bronze body into :class:`ChildTable` s: one for a plain CSV
body, one per requested child for a container (an Excel workbook's sheets, a
ZIP's members). Unit X registers the container readers in :data:`READERS`;
until then a record naming ``xlsx`` or ``zip_member`` fails each capture with
:class:`ReaderUnavailableError`, loudly.

Every table a reader yields is all-``Utf8`` with its columns equal to the
header it parsed (stripped), so P-4 types it by matching that header to one of
the record's epochs.
"""

from __future__ import annotations

import codecs
import io
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from gridflow.silver.csv_bronze import read_csv_bronze_body

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

__all__ = [
    "CONTAINER_READERS",
    "READERS",
    "BodyReader",
    "ChildTable",
    "ContainerInventoryError",
    "ReaderUnavailableError",
    "read_children",
    "read_csv_body",
]

_UTF8_BOM = b"\xef\xbb\xbf"


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
    """

    child_id: str
    header: tuple[str, ...]
    frame: pl.DataFrame


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


READERS: dict[str, BodyReader] = {"csv": read_csv_body}
"""Reader name -> reader. Unit X adds ``xlsx`` and ``zip_member``."""

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
