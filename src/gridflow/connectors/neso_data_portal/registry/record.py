"""The frozen schema record a generic NESO family is transformed by (ADR-034 P-1).

A family with a record is transformed by the generic silver engine; a family
without one stays ingest-only. The record is a registry artefact, so it lives
beside the package files and is validated when the registry loads:

- the **models** below check each object's own shape (Pydantic, frozen,
  ``extra="forbid"``);
- :func:`validate_record` checks the record against itself, its family and its
  package (rules V-1..V-10, V-13, V-17), and :func:`validate_silver_targets` checks
  every ``SILVER`` disposition once all files are loaded (V-11). Every failure
  raises :class:`RecordError` naming the rule; the loader re-raises it as a
  ``RegistryError`` naming the file and family.

``Eligible``, ``Held`` and ``Eligibility`` live here (re-exported by the
registry package under the same names) so a record can carry a per-output
eligibility override without an import cycle.

This module imports only Pydantic, the standard library and the leaf
``gridflow.silver.date_columns`` (V-13), never ``gridflow.silver.base``.
"""

from __future__ import annotations

import codecs
import re
from typing import Annotated, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gridflow.silver.date_columns import DATE_COL_SQL_TYPES

__all__ = [
    "CHILD_SEPARATOR",
    "RESERVED",
    "SILVER_NAME_PATTERN",
    "ColumnSpec",
    "Eligibility",
    "Eligible",
    "Held",
    "HeaderEpoch",
    "IssueRecipe",
    "RecordError",
    "SchemaRecord",
    "TemporalRecipe",
    "XlsxSpec",
    "ZipMemberSpec",
    "column_index",
    "has_issue_time",
    "silver_columns",
]

SILVER_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

CHILD_SEPARATOR = "::"
"""Joins a workbook member's name to one of its sheets in a container child id (ADR-037 P-3)."""

RESERVED: frozenset[str] = frozenset(
    {
        "year",
        "month",
        "event_time",
        "available_at",
        "published_at",
        "issue_time",
        "source_run_id",
        "dataset_version",
        "vintage_policy",
        "timestamp_utc",
        "bronze_capture_id",
        "capture_written_at",
        "child_id",
        "child_crc32",
        "resource_id",
    }
)
"""Names the engine or the catalogue writes itself (V-2).

``year``/``month`` are Hive partition names: DuckDB silently replaces a data
column of the same name with the directory value, and Polars raises (E3).
``resource_id`` is stamped by the engine on a resource-partitioned record's
rows (ADR-039).
"""

Dtype = Literal["string", "int64", "float64", "date", "datetime"]


class RecordError(ValueError):
    """A frozen schema record breaks one of the load-time rules (V-n)."""


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Eligible(_Frozen):
    """The package may be published."""

    status: Literal["eligible"]


class Held(_Frozen):
    """The package is held from publication pending a research unit."""

    status: Literal["held"]
    question: str
    unit: str


Eligibility = Annotated[Eligible | Held, Field(discriminator="status")]


class ColumnSpec(_Frozen):
    """One vendor column of one header epoch and its silver typing.

    Attributes:
        source: The vendor header name, exactly as the body carries it.
        name: The silver column name (``^[a-z][a-z0-9_]*$``).
        dtype: The silver type.
        format: A ``strptime`` format; a ``datetime`` column requires it, a
            ``date`` column requires it or ``formats_by_filename``.
        formats_by_filename: ``date`` only: exact ``(resource_filename, format)``
            pairs, for one header whose date format differs by resource
            (ADR-039). An unlisted filename fails the capture; no fallback.
        null_tokens: Vendor spellings read as null before casting.
        nullable: Whether a null survives (``False`` excludes the row, P-4).
        min: Numeric lower bound (inclusive); a breach excludes the row.
        max: Numeric upper bound (inclusive); a breach excludes the row.
        zone: ``None`` (the format carries ``%z``), ``"UTC"``, or an IANA zone.
        zone_evidence: Why an IANA zone is the vendor's (required with one).
        ambiguous: How an IANA zone's repeated hour resolves.
    """

    source: str
    name: str
    dtype: Dtype
    format: str | None = None
    formats_by_filename: tuple[tuple[str, str], ...] | None = None
    null_tokens: tuple[str, ...] = ()
    nullable: bool
    min: float | None = None
    max: float | None = None
    zone: str | None = None
    zone_evidence: str | None = None
    ambiguous: Literal["earliest", "latest", "raise"] | None = None

    @model_validator(mode="after")
    def _shape(self) -> ColumnSpec:
        if not SILVER_NAME_PATTERN.fullmatch(self.name):
            raise ValueError(f"column name {self.name!r} is not {SILVER_NAME_PATTERN.pattern}")
        if self.formats_by_filename is not None:
            self._filename_formats()
        elif self.dtype in ("date", "datetime"):
            if not self.format:
                raise ValueError(f"column {self.name!r}: a {self.dtype} column needs a format")
        elif self.format is not None:
            raise ValueError(f"column {self.name!r}: only date/datetime columns take a format")
        if (self.min is not None or self.max is not None) and self.dtype not in (
            "int64",
            "float64",
        ):
            raise ValueError(f"column {self.name!r}: min/max apply to numeric columns only")
        if self.dtype != "datetime":
            if self.zone is not None or self.zone_evidence is not None or self.ambiguous:
                raise ValueError(f"column {self.name!r}: zone rules apply to datetime only")
            return self
        assert self.format is not None
        carries_offset = "%z" in self.format
        if self.zone is None:
            if not carries_offset or self.zone_evidence is not None or self.ambiguous:
                raise ValueError(
                    f"column {self.name!r}: zone=None means the format carries %z, "
                    "with no zone_evidence or ambiguous"
                )
        elif self.zone == "UTC":
            if carries_offset or self.zone_evidence is not None or self.ambiguous:
                raise ValueError(
                    f"column {self.name!r}: zone='UTC' takes a format without %z and no "
                    "zone_evidence or ambiguous"
                )
        else:
            try:
                ZoneInfo(self.zone)
            except (ZoneInfoNotFoundError, ValueError) as exc:
                raise ValueError(f"column {self.name!r}: unknown zone {self.zone!r}") from exc
            if carries_offset or not self.zone_evidence or self.ambiguous is None:
                raise ValueError(
                    f"column {self.name!r}: an IANA zone needs a format without %z, "
                    "non-empty zone_evidence and an ambiguous rule"
                )
        return self

    def _filename_formats(self) -> None:
        """The per-filename map's shape: ``date`` only, alone, non-empty, unique."""
        mapping = self.formats_by_filename
        assert mapping is not None
        if self.dtype != "date":
            raise ValueError(
                f"column {self.name!r}: formats_by_filename applies to date columns only"
            )
        if self.format is not None:
            raise ValueError(
                f"column {self.name!r}: a date column takes exactly one of format and "
                "formats_by_filename"
            )
        if not mapping:
            raise ValueError(f"column {self.name!r}: formats_by_filename must be non-empty")
        if any(not filename or not fmt for filename, fmt in mapping):
            raise ValueError(
                f"column {self.name!r}: every formats_by_filename filename and format must be "
                "non-empty"
            )
        filenames = [filename for filename, _fmt in mapping]
        if len(set(filenames)) != len(filenames):
            raise ValueError(f"column {self.name!r}: formats_by_filename repeats a filename")

    @property
    def is_local(self) -> bool:
        """Whether this is a datetime read in an IANA (non-UTC) zone."""
        return self.dtype == "datetime" and self.zone not in (None, "UTC")


class IssueRecipe(_Frozen):
    """Where an epoch's rows take their issue instant from.

    Attributes:
        kind: ``none``, a ``data_column``, or a ``filename_token``.
        column: For ``data_column``: the silver name of a ``datetime`` column.
        pattern: For ``filename_token``: a ``fullmatch`` regex with one group,
            applied to the sidecar ``resource_filename``.
        format: For ``filename_token``: the group's ``strptime`` format (UTC).
    """

    kind: Literal["none", "data_column", "filename_token"]
    column: str | None = None
    pattern: str | None = None
    format: str | None = None

    @model_validator(mode="after")
    def _shape(self) -> IssueRecipe:
        if self.kind == "data_column":
            if not self.column or self.pattern is not None or self.format is not None:
                raise ValueError("issue data_column takes exactly a column")
        elif self.kind == "filename_token":
            if self.column is not None or not self.pattern or not self.format:
                raise ValueError("issue filename_token takes exactly a pattern and a format")
            try:
                groups = re.compile(self.pattern).groups
            except re.error as exc:
                raise ValueError(f"issue pattern does not compile ({exc})") from exc
            if groups != 1:
                raise ValueError(f"issue pattern must have one group, has {groups}")
        elif self.column is not None or self.pattern is not None or self.format is not None:
            raise ValueError("issue kind none takes no column, pattern or format")
        return self


class HeaderEpoch(_Frozen):
    """One exact, ordered vendor header and its column typing.

    Attributes:
        header: The vendor header, exact and ordered.
        columns: One :class:`ColumnSpec` per header entry, in header order.
        issue: This epoch's issue-time recipe.
    """

    header: tuple[str, ...] = Field(min_length=1)
    columns: tuple[ColumnSpec, ...]
    issue: IssueRecipe

    @model_validator(mode="after")
    def _shape(self) -> HeaderEpoch:
        if len(set(self.header)) != len(self.header):
            raise ValueError(f"epoch header {list(self.header)} repeats a name")
        if tuple(column.source for column in self.columns) != self.header:
            raise ValueError(
                f"epoch columns {[c.source for c in self.columns]} are not aligned 1:1 with "
                f"the header {list(self.header)}"
            )
        return self


class TemporalRecipe(_Frozen):
    """How ``timestamp_utc`` is derived (P-5 step 2).

    Attributes:
        kind: ``sp_pair``, ``utc_instant``, ``local_instant``, ``date_sp1``,
            ``month`` or ``none`` (the capture's own ``capture_written_at``).
        date_column: For ``sp_pair``, ``date_sp1`` and ``month``.
        period_column: For ``sp_pair``.
        column: For the two instant kinds.
    """

    kind: Literal["sp_pair", "utc_instant", "local_instant", "date_sp1", "month", "none"]
    date_column: str | None = None
    period_column: str | None = None
    column: str | None = None

    @model_validator(mode="after")
    def _shape(self) -> TemporalRecipe:
        wants = {
            "sp_pair": {"date_column", "period_column"},
            "utc_instant": {"column"},
            "local_instant": {"column"},
            "date_sp1": {"date_column"},
            "month": {"date_column"},
            "none": set(),
        }[self.kind]
        present = {
            name
            for name in ("date_column", "period_column", "column")
            if getattr(self, name) is not None
        }
        if present != wants:
            raise ValueError(f"temporal {self.kind} takes exactly {sorted(wants)}")
        return self

    @property
    def inputs(self) -> tuple[str, ...]:
        """The recipe's input columns, in declaration order."""
        return tuple(
            value
            for value in (self.date_column, self.period_column, self.column)
            if value is not None
        )


_COLUMN_RANGE = re.compile(r"^([A-Z]{1,3}):([A-Z]{1,3})$")


def column_index(letters: str) -> int:
    """The 1-based index of an Excel column name (``A`` -> 1, ``AA`` -> 27)."""
    index = 0
    for char in letters:
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index


class XlsxSpec(_Frozen):
    """Where one sheet's table sits (ADR-037 P-2, P-6).

    Attributes:
        header_row: The 1-based Excel row of the one header row.
        columns: The column range, ``"A:J"`` (left <= right).
        last_row: The 1-based last data row; ``None`` = the sheet's last
            populated row.
    """

    header_row: int = Field(ge=1)
    columns: str
    last_row: int | None = None

    @model_validator(mode="after")
    def _shape(self) -> XlsxSpec:
        match = _COLUMN_RANGE.fullmatch(self.columns)
        if match is None:
            raise ValueError(f"xlsx columns {self.columns!r} is not {_COLUMN_RANGE.pattern}")
        if column_index(match.group(1)) > column_index(match.group(2)):
            raise ValueError(f"xlsx columns {self.columns!r}: left is after right")
        return self

    @property
    def bounds(self) -> tuple[int, int]:
        """The 1-based ``(first, last)`` column indexes of :attr:`columns`."""
        left, right = self.columns.split(":")
        return column_index(left), column_index(right)


class ZipMemberSpec(_Frozen):
    """Which ZIP members a ``zip_member`` record reads and how (ADR-037 P-2, P-7).

    Attributes:
        member_pattern: ``re.fullmatch`` over the member path; must compile.
        inner: How a member's bytes are read: ``csv`` or ``xlsx``.
    """

    member_pattern: str
    inner: Literal["csv", "xlsx"]

    @model_validator(mode="after")
    def _shape(self) -> ZipMemberSpec:
        try:
            re.compile(self.member_pattern)
        except re.error as exc:
            raise ValueError(f"member_pattern does not compile ({exc})") from exc
        return self


class SchemaRecord(_Frozen):
    """One family's frozen schema: reader, epochs, clocks, key and selection.

    Attributes:
        version: The record version; part of every output's ``dataset_version``.
        reader: ``csv`` or a container reader (``xlsx``, ``zip_member``).
        encoding: The body's text encoding (checked with ``codecs.lookup``).
        epochs: Every accepted header (at least one).
        temporal: The ``timestamp_utc`` recipe.
        entity_key: The output grain within one capture.
        latest: ``key_latest`` or ``whole_capture`` revision selection.
        run_type_column: An optional settlement-run column (ranked, P-10).
        siblings: Same-package families whose bronze this family also reads.
        vintage: Where ``published_at`` comes from.
        vintage_evidence: Required for ``issue_time_evidenced`` only.
        eligibility: A per-output publication override; ``None`` inherits.
        xlsx: The sheet table spec (``reader="xlsx"``, or ``zip_member`` with
            ``inner="xlsx"``).
        zip_member: The member spec (``reader="zip_member"`` only).
        latest_partition: ``whole_capture`` only: select the newest complete
            capture per value of this completion-ledger column instead of per
            family (ADR-039); the engine stamps it on every row (V-17).
    """

    version: str = Field(min_length=1)
    reader: Literal["csv", "xlsx", "zip_member"]
    encoding: str
    epochs: tuple[HeaderEpoch, ...] = Field(min_length=1)
    temporal: TemporalRecipe
    entity_key: tuple[str, ...] = Field(min_length=1)
    latest: Literal["key_latest", "whole_capture"]
    run_type_column: str | None = None
    siblings: tuple[str, ...] = ()
    vintage: Literal["ckan_last_modified", "capture_fallback", "issue_time_evidenced"]
    vintage_evidence: str | None = None
    eligibility: Eligibility | None = None
    xlsx: XlsxSpec | None = None
    zip_member: ZipMemberSpec | None = None
    latest_partition: Literal["resource_id"] | None = None

    @model_validator(mode="after")
    def _shape(self) -> SchemaRecord:
        try:
            codecs.lookup(self.encoding)
        except LookupError as exc:
            raise ValueError(f"unknown encoding {self.encoding!r}") from exc
        if len(set(self.entity_key)) != len(self.entity_key):
            raise ValueError(f"entity_key {list(self.entity_key)} repeats a column")
        self._reader_specs()
        return self

    def _reader_specs(self) -> None:
        """V-14: the reader specs match the reader."""
        if self.reader == "csv":
            if self.xlsx is not None or self.zip_member is not None:
                raise ValueError("V-14: reader csv takes no xlsx or zip_member spec")
        elif self.reader == "xlsx":
            if self.xlsx is None or self.zip_member is not None:
                raise ValueError("V-14: reader xlsx takes an xlsx spec and no zip_member spec")
        else:
            if self.zip_member is None:
                raise ValueError("V-14: reader zip_member takes a zip_member spec")
            if (self.xlsx is not None) != (self.zip_member.inner == "xlsx"):
                raise ValueError(
                    "V-14: reader zip_member takes an xlsx spec exactly when inner is xlsx"
                )
        if (
            self.xlsx is not None
            and self.xlsx.last_row is not None
            and self.xlsx.last_row <= self.xlsx.header_row
        ):
            raise ValueError("V-14: xlsx last_row must be after header_row")


def has_issue_time(record: SchemaRecord) -> bool:
    """Whether any epoch declares an issue time (so ``issue_time`` is emitted)."""
    return any(epoch.issue.kind != "none" for epoch in record.epochs)


def silver_columns(record: SchemaRecord) -> dict[str, ColumnSpec]:
    """Silver name -> first declaring spec, in first-appearance order across epochs."""
    seen: dict[str, ColumnSpec] = {}
    for epoch in record.epochs:
        for column in epoch.columns:
            seen.setdefault(column.name, column)
    return seen


def _fail(rule: str, message: str) -> RecordError:
    return RecordError(f"{rule}: {message}")


def validate_record(
    record: SchemaRecord,
    *,
    key: str,
    kind: str,
    legacy: bool,
    package_families: dict[str, bool],
    family_url_types: frozenset[str],
) -> None:
    """Check one family's record against itself, its family and its package.

    Args:
        record: The family's record.
        key: The family key.
        kind: The family kind (``tabular``/``files``).
        legacy: Whether the family is one of the three legacy keys.
        package_families: Every family key of the package -> whether it has a
            record.
        family_url_types: The ``url_type`` of every resource the family holds
            (its own resources and every resource dispositioned to it).

    Raises:
        RecordError: The first rule broken, named ``V-n``.
    """
    if kind != "tabular" or legacy:
        raise _fail("V-9", "a record is allowed only on a tabular, non-legacy family")

    dtypes: dict[str, str] = {}
    for index, epoch in enumerate(record.epochs):
        names = [column.name for column in epoch.columns]
        if len(set(names)) != len(names):
            raise _fail("V-1", f"epoch {index} maps two vendor columns to one silver name")
        for column in epoch.columns:
            if dtypes.setdefault(column.name, column.dtype) != column.dtype:
                raise _fail(
                    "V-1",
                    f"silver column {column.name!r} is {dtypes[column.name]} in one epoch and "
                    f"{column.dtype} in another",
                )
    for name in dtypes:
        if name in RESERVED:
            raise _fail("V-2", f"silver column {name!r} is a reserved name")

    _check_recipe_columns(record)

    outputs = set(dtypes)
    issue = has_issue_time(record)
    key_set = set(record.entity_key)
    admissible = outputs | {"issue_time"} | ({"resource_id"} if record.latest_partition else set())
    if not key_set <= admissible:
        raise _fail(
            "V-4",
            f"entity_key columns {sorted(key_set - admissible)} are not outputs",
        )
    if ("issue_time" in key_set) != issue:
        raise _fail(
            "V-4", "issue_time must be in entity_key exactly when some epoch declares an issue time"
        )

    if record.run_type_column is not None and (
        dtypes.get(record.run_type_column) != "string" or record.run_type_column not in key_set
    ):
        raise _fail("V-5", "run_type_column must be a string column in entity_key")

    if record.temporal.kind == "sp_pair":
        pair = set(record.temporal.inputs)
        if not key_set > pair:
            raise _fail(
                "V-6",
                "an sp_pair entity_key must strictly contain (date_column, period_column)",
            )

    if record.vintage == "issue_time_evidenced":
        if not record.vintage_evidence or not record.vintage_evidence.strip():
            raise _fail("V-7", "issue_time_evidenced needs non-empty vintage_evidence")
        if any(epoch.issue.kind == "none" for epoch in record.epochs):
            raise _fail("V-7", "issue_time_evidenced needs an issue recipe in every epoch")
    elif record.vintage_evidence is not None:
        raise _fail("V-7", f"vintage {record.vintage} takes no vintage_evidence")

    if record.latest == "whole_capture" and record.vintage == "issue_time_evidenced":
        raise _fail("V-8", "whole_capture selection cannot take issue_time_evidenced")
    if record.vintage == "ckan_last_modified" and "datastore" in family_url_types:
        raise _fail("V-8", "a family holding a datastore resource cannot take ckan_last_modified")
    if record.vintage == "issue_time_evidenced" and "datastore" in family_url_types:
        raise _fail(
            "V-8",
            "a family holding a datastore resource takes capture_fallback until unit D's "
            "evidence switches it (ADR-035)",
        )

    if record.latest_partition is not None:
        if record.latest != "whole_capture":
            raise _fail("V-17", "latest_partition needs whole_capture")
        if record.latest_partition not in key_set or len(key_set) < 2:
            raise _fail(
                "V-17",
                "a resource-partitioned entity_key holds resource_id and the per-resource grain",
            )

    for sibling in record.siblings:
        if sibling == key or sibling not in package_families:
            raise _fail("V-10", f"sibling {sibling!r} is not another family of this package")
        if package_families[sibling]:
            raise _fail("V-10", f"sibling {sibling!r} carries its own record (single owner)")

    if record.temporal.kind in ("sp_pair", "date_sp1", "month"):
        date_column = record.temporal.date_column
        assert date_column is not None
        if DATE_COL_SQL_TYPES.get(date_column, "DATE") != "DATE":
            raise _fail(
                "V-13",
                f"date column {date_column!r} is a designated date name the manifest types "
                f"{DATE_COL_SQL_TYPES[date_column]}",
            )


def _check_recipe_columns(record: SchemaRecord) -> None:
    """V-3: recipe columns exist in every epoch, typed, temporal inputs non-nullable."""
    temporal = record.temporal
    required: dict[str, str] = {}
    if temporal.kind in ("sp_pair", "date_sp1", "month"):
        assert temporal.date_column is not None
        required[temporal.date_column] = "date"
    if temporal.kind == "sp_pair":
        assert temporal.period_column is not None
        required[temporal.period_column] = "int64"
    if temporal.kind in ("utc_instant", "local_instant"):
        assert temporal.column is not None
        required[temporal.column] = "datetime"

    for index, epoch in enumerate(record.epochs):
        by_name = {column.name: column for column in epoch.columns}
        for name, dtype in required.items():
            spec = by_name.get(name)
            if spec is None:
                raise _fail("V-3", f"temporal column {name!r} is absent from epoch {index}")
            if spec.dtype != dtype:
                raise _fail(
                    "V-3", f"temporal column {name!r} is {spec.dtype} in epoch {index}, not {dtype}"
                )
            if spec.nullable:
                raise _fail(
                    "V-3", f"temporal input {name!r} must be nullable: false (epoch {index})"
                )
            if temporal.kind == "local_instant" and not spec.is_local:
                raise _fail("V-3", f"local_instant column {name!r} needs an IANA zone")
            if temporal.kind == "utc_instant" and spec.is_local:
                raise _fail("V-3", f"utc_instant column {name!r} cannot carry an IANA zone")
        if epoch.issue.kind == "data_column":
            assert epoch.issue.column is not None
            spec = by_name.get(epoch.issue.column)
            if spec is None or spec.dtype != "datetime":
                raise _fail(
                    "V-3",
                    f"issue column {epoch.issue.column!r} is not a datetime column of epoch "
                    f"{index}",
                )
