"""The frozen schema record a generic NESO family is transformed by (ADR-034 P-1).

A family with a record is transformed by the generic silver engine; a family
without one stays ingest-only. The record is a registry artefact, so it lives
beside the package files and is validated when the registry loads:

- the **models** below check each object's own shape (Pydantic, frozen,
  ``extra="forbid"``), the record's reader specs included (V-14, and V-20 for
  an ``xlsx_blocks`` composite-block map);
- :func:`validate_record` checks the record against itself, its family and its
  package (rules V-1..V-10, V-13, V-17, V-18, V-19), :func:`validate_overlap_groups`
  checks each package's overlap groups across its records (V-21, ADR-043), and
  :func:`validate_silver_targets` checks
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
from typing import TYPE_CHECKING, Annotated, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gridflow.silver.date_columns import DATE_COL_SQL_TYPES

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "CHILD_SEPARATOR",
    "EDITION",
    "NUMERIC_TRIM_CHARS",
    "PROJECTION_YEAR",
    "RESERVED",
    "SILVER_NAME_PATTERN",
    "VALUE",
    "ColumnSpec",
    "Eligibility",
    "Eligible",
    "Held",
    "HeaderEpoch",
    "IssueRecipe",
    "RecordError",
    "SchemaRecord",
    "TemporalRecipe",
    "UnpivotSpec",
    "ValueSpec",
    "XlsxBlock",
    "XlsxSpec",
    "ZipMemberSpec",
    "column_index",
    "epoch_outputs",
    "has_issue_time",
    "silver_columns",
    "validate_overlap_groups",
]

PROJECTION_YEAR = "projection_year"
"""The silver name an unpivot epoch gives its declared year (ADR-042)."""

VALUE = "value"
"""The silver name an unpivot epoch gives its one typed value (ADR-042)."""

EDITION = "edition"
"""The silver name of the engine's edition stamp (ADR-042)."""

NUMERIC_TRIM_CHARS = " \t\n\r\v\f"
"""The ASCII whitespace a ``trim: "ascii"`` column strips from both ends of a cell.

NBSP (U+00A0) and other Unicode spaces are excluded: they are not ASCII, the
measured vendor cells carry only U+0020, and including them would widen the
cleanup past what the bodies show. This is the only definition of the set.
"""

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
        EDITION,
    }
)
"""Names the engine or the catalogue writes itself (V-2).

``year``/``month`` are Hive partition names: DuckDB silently replaces a data
column of the same name with the directory value, and Polars raises (E3).
``resource_id`` is stamped by the engine on a resource-partitioned record's
rows (ADR-039). ``edition`` is stamped by the engine on an edition-mapped
record's rows (ADR-042).
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
        trim: ``"ascii"``: strip :data:`NUMERIC_TRIM_CHARS` from both ends of
            each cell before the null-token match and the cast (numeric non-key
            columns only, V-19). ``None``, the default, is omitted from every
            ``exclude_none`` dump.
        alt_formats: ``datetime`` only: further ``strptime`` formats, tried in
            order, for a cell ``format`` does not parse (one column's rendering
            variants). A cell that no format parses fails the capture. ``None``,
            the default, is omitted from every ``exclude_none`` dump (ADR-043).
        file_formats: ``date`` only, instead of ``format``: candidate
            ``strptime`` formats for a column whose rendering changed between
            uploads of one resource. Each file takes exactly one: the single
            candidate under which every non-null cell of that file parses. No
            candidate, or more than one, fails the capture, so a day-first and a
            month-first reading are never chosen cell by cell. ``None``, the
            default, is omitted from every ``exclude_none`` dump.
    """

    source: str
    name: str
    dtype: Dtype
    format: str | None = None
    formats_by_filename: tuple[tuple[str, str], ...] | None = None
    file_formats: tuple[str, ...] | None = None
    null_tokens: tuple[str, ...] = ()
    nullable: bool
    min: float | None = None
    max: float | None = None
    zone: str | None = None
    zone_evidence: str | None = None
    ambiguous: Literal["earliest", "latest", "raise"] | None = None
    trim: Literal["ascii"] | None = None
    alt_formats: tuple[str, ...] | None = None

    @model_validator(mode="after")
    def _shape(self) -> ColumnSpec:
        if not SILVER_NAME_PATTERN.fullmatch(self.name):
            raise ValueError(f"column name {self.name!r} is not {SILVER_NAME_PATTERN.pattern}")
        if self.alt_formats is not None:
            self._alternative_formats(self.alt_formats)
        if self.file_formats is not None:
            self._file_formats(self.file_formats)
        elif self.formats_by_filename is not None:
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
        if self.trim is not None:
            if self.dtype not in ("int64", "float64"):
                raise ValueError(f"column {self.name!r}: trim applies to numeric columns only")
            if any(token != token.strip(NUMERIC_TRIM_CHARS) for token in self.null_tokens):
                raise ValueError(
                    f"column {self.name!r}: a trimmed column's null tokens carry no edge "
                    "whitespace (they could never match)"
                )
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

    def _alternative_formats(self, alternatives: tuple[str, ...]) -> None:
        """The alternative formats' shape (ADR-043): a naive datetime's, non-empty, distinct."""
        if self.dtype != "datetime":
            raise ValueError(f"column {self.name!r}: alt_formats applies to datetime columns only")
        if not self.format:
            raise ValueError(f"column {self.name!r}: alt_formats needs a format")
        if "%z" in self.format:
            raise ValueError(
                f"column {self.name!r}: alt_formats cannot extend a format carrying %z"
            )
        if not alternatives:
            raise ValueError(f"column {self.name!r}: alt_formats must be non-empty")
        for alternative in alternatives:
            if not alternative:
                raise ValueError(f"column {self.name!r}: an alt_formats entry is empty")
            if "%z" in alternative:
                raise ValueError(
                    f"column {self.name!r}: alt_formats entry {alternative!r} carries %z"
                )
            if alternative == self.format:
                raise ValueError(
                    f"column {self.name!r}: alt_formats entry {alternative!r} repeats format"
                )
        if len(set(alternatives)) != len(alternatives):
            raise ValueError(f"column {self.name!r}: alt_formats repeats an entry")

    def _file_formats(self, candidates: tuple[str, ...]) -> None:
        """The per-file candidates' shape: ``date`` only, alone, two or more, distinct."""
        if self.dtype != "date":
            raise ValueError(f"column {self.name!r}: file_formats applies to date columns only")
        if self.format is not None or self.formats_by_filename is not None:
            raise ValueError(
                f"column {self.name!r}: file_formats replaces format and formats_by_filename"
            )
        if len(candidates) < 2:
            raise ValueError(f"column {self.name!r}: file_formats needs two or more candidates")
        if any(not candidate for candidate in candidates):
            raise ValueError(f"column {self.name!r}: a file_formats entry is empty")
        if len(set(candidates)) != len(candidates):
            raise ValueError(f"column {self.name!r}: file_formats repeats an entry")
        # A body's cells cannot tell a day-first from a month-first reading of one
        # shape when every day is 12 or less, so a mixed body could fit one of them
        # and be misread without a failure. Such a pair is never a candidate set.
        swapped = {
            c.replace("%d", "\0").replace("%m", "%d").replace("\0", "%m") for c in candidates
        }
        if any(c in swapped and "%d" in c and "%m" in c for c in candidates):
            raise ValueError(
                f"column {self.name!r}: file_formats holds a day-first and a month-first "
                "reading of one shape; the cells cannot decide between them"
            )

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


class ValueSpec(_Frozen):
    """The one typed value an unpivot epoch's year columns become (ADR-042).

    Attributes:
        dtype: The silver type of ``value``.
        nullable: Whether a null survives (``False`` excludes the long row).
        null_tokens: Vendor spellings read as null before casting.
        min: Numeric lower bound (inclusive); a breach excludes the long row.
        max: Numeric upper bound (inclusive); a breach excludes the long row.
    """

    dtype: Literal["string", "int64", "float64"]
    nullable: bool
    null_tokens: tuple[str, ...] = ()
    min: float | None = None
    max: float | None = None

    @model_validator(mode="after")
    def _shape(self) -> ValueSpec:
        if (self.min is not None or self.max is not None) and self.dtype == "string":
            raise ValueError("unpivot value: min/max apply to numeric columns only")
        return self


class UnpivotSpec(_Frozen):
    """An epoch's year columns, reshaped to ``(projection_year, value)`` rows (ADR-042).

    Attributes:
        years: Exact vendor header label -> declared projection year, in header
            order. Labels are never parsed: the year is what the record declares.
        value: The typing of every year cell.
    """

    years: tuple[tuple[str, int], ...] = Field(min_length=1)
    value: ValueSpec

    @model_validator(mode="after")
    def _shape(self) -> UnpivotSpec:
        labels = [label for label, _year in self.years]
        if any(not label for label in labels):
            raise ValueError("unpivot has an empty year label")
        if len(set(labels)) != len(labels):
            raise ValueError(f"unpivot repeats a year label: {labels}")
        years = [year for _label, year in self.years]
        if len(set(years)) != len(years):
            raise ValueError(f"unpivot repeats a projection year: {years}")
        return self


class HeaderEpoch(_Frozen):
    """One exact, ordered vendor header and its column typing.

    Attributes:
        header: The vendor header, exact and ordered.
        columns: One :class:`ColumnSpec` per header entry, in header order; with
            ``unpivot``, one per header entry that is not a year label.
        issue: This epoch's issue-time recipe.
        unpivot: The year columns reshaped to long rows (ADR-042); ``None``
            keeps the epoch wide.
    """

    header: tuple[str, ...] = Field(min_length=1)
    columns: tuple[ColumnSpec, ...]
    issue: IssueRecipe
    unpivot: UnpivotSpec | None = None

    @model_validator(mode="after")
    def _shape(self) -> HeaderEpoch:
        if len(set(self.header)) != len(self.header):
            raise ValueError(f"epoch header {list(self.header)} repeats a name")
        if self.unpivot is None:
            if tuple(column.source for column in self.columns) != self.header:
                raise ValueError(
                    f"epoch columns {[c.source for c in self.columns]} are not aligned 1:1 with "
                    f"the header {list(self.header)}"
                )
            return self
        self._unpivot_shape(self.unpivot)
        return self

    def _unpivot_shape(self, unpivot: UnpivotSpec) -> None:
        """The index sources and year labels partition the header, each in header order."""
        header = list(self.header)
        sources = [column.source for column in self.columns]
        labels = [label for label, _year in unpivot.years]
        where = f"unpivot epoch header {header}"
        if set(sources) & set(labels):
            raise ValueError(
                f"{where}: columns and year labels overlap on {sorted(set(sources) & set(labels))}"
            )
        if set(sources) | set(labels) != set(header) or len(sources) + len(labels) != len(header):
            raise ValueError(
                f"{where}: columns {sources} and year labels {labels} do not cover the header "
                "exactly"
            )
        position = {name: index for index, name in enumerate(header)}
        for kind, names in (("columns", sources), ("year labels", labels)):
            indexes = [position[name] for name in names]
            if indexes != sorted(indexes):
                raise ValueError(f"{where}: {kind} {names} are not in header order")
        generated = {PROJECTION_YEAR, VALUE}
        if set(sources) & generated:
            raise ValueError(
                f"{where}: index column {sorted(set(sources) & generated)} is named like a "
                "generated column"
            )


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


class XlsxBlock(_Frozen):
    """Where one resource's composite-header table sits (ADR-037 amendment 1, V-20).

    One block per resource. Rows between ``header_last`` and ``data_first``,
    above ``header_first`` and below ``data_last`` are never read; rows are
    selected by these positions only, never by value.

    Attributes:
        resource_id: The registry resource the block belongs to.
        sheet: The exact sheet name (trailing spaces kept).
        header_first: The 1-based first composite-header row.
        header_last: The 1-based last composite-header row.
        data_first: The 1-based first data row.
        data_last: The 1-based last data row.
        columns: The column range, ``"A:AP"`` (left <= right).
    """

    resource_id: str = Field(min_length=1)
    sheet: str = Field(min_length=1)
    header_first: int = Field(ge=1)
    header_last: int
    data_first: int
    data_last: int
    columns: str

    @model_validator(mode="after")
    def _shape(self) -> XlsxBlock:
        if not (self.header_first <= self.header_last < self.data_first <= self.data_last):
            raise ValueError(
                f"xlsx block rows must be header_first <= header_last < data_first <= "
                f"data_last, got {self.header_first}, {self.header_last}, {self.data_first}, "
                f"{self.data_last}"
            )
        match = _COLUMN_RANGE.fullmatch(self.columns)
        if match is None:
            raise ValueError(f"xlsx block columns {self.columns!r} is not {_COLUMN_RANGE.pattern}")
        if column_index(match.group(1)) > column_index(match.group(2)):
            raise ValueError(f"xlsx block columns {self.columns!r}: left is after right")
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
        edition_by_filename: Exact ``(resource_filename, edition)`` pairs; the
            engine stamps the capture's edition as ``edition`` (ADR-042). An
            unlisted filename fails the capture; no fallback.
        xlsx_blocks: Reader ``xlsx`` only, instead of ``xlsx``: one exact
            block per resource (V-20); an unlisted resource fails the
            capture; no fallback.
        overlap_group: Families of one package whose ``_latest`` relations
            together form one series: reconcile checks their union for keys
            served by two resources (ADR-043, V-21). ``None``, the default, is
            omitted from every ``exclude_none`` dump.
        occurrence_column: An ``int64`` output naming each row's 1-based
            occurrence, in body order, of its other ``entity_key`` values, for
            a body that repeats a key label with different readings (October
            2014's system frequency repeats the hour 01:00 to 01:59). It is part
            of ``entity_key``; nothing about the repeat is inferred beyond its
            order. ``None``, the default, is omitted from every ``exclude_none``
            dump.
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
    edition_by_filename: tuple[tuple[str, int], ...] | None = None
    xlsx_blocks: tuple[XlsxBlock, ...] | None = None
    overlap_group: str | None = None
    occurrence_column: str | None = None

    @model_validator(mode="after")
    def _shape(self) -> SchemaRecord:
        try:
            codecs.lookup(self.encoding)
        except LookupError as exc:
            raise ValueError(f"unknown encoding {self.encoding!r}") from exc
        if self.overlap_group is not None and not SILVER_NAME_PATTERN.fullmatch(self.overlap_group):
            raise ValueError(
                f"overlap_group {self.overlap_group!r} is not {SILVER_NAME_PATTERN.pattern}"
            )
        if len(set(self.entity_key)) != len(self.entity_key):
            raise ValueError(f"entity_key {list(self.entity_key)} repeats a column")
        self._reader_specs()
        if self.edition_by_filename is not None:
            self._edition_map(self.edition_by_filename)
        return self

    @staticmethod
    def _edition_map(mapping: tuple[tuple[str, int], ...]) -> None:
        """The edition map's shape: non-empty, non-empty filenames, each once."""
        if not mapping:
            raise ValueError("edition_by_filename must be non-empty")
        if any(not filename for filename, _edition in mapping):
            raise ValueError("every edition_by_filename filename must be non-empty")
        filenames = [filename for filename, _edition in mapping]
        if len(set(filenames)) != len(filenames):
            raise ValueError("edition_by_filename repeats a filename")

    def _blocks(self, blocks: tuple[XlsxBlock, ...]) -> None:
        """V-20: a block map is reader ``xlsx``'s alone, non-empty, one block per resource."""
        if self.reader != "xlsx":
            raise ValueError(f"V-20: xlsx_blocks needs reader xlsx, not {self.reader}")
        if self.xlsx is not None:
            raise ValueError("V-20: xlsx_blocks replaces the xlsx spec; declare one, not both")
        if not blocks:
            raise ValueError("V-20: xlsx_blocks must be non-empty")
        ids = [block.resource_id for block in blocks]
        if len(set(ids)) != len(ids):
            raise ValueError("V-20: xlsx_blocks repeats a resource_id")

    def _reader_specs(self) -> None:
        """V-14: the reader specs match the reader (V-20 first, for a block map)."""
        if self.xlsx_blocks is not None:
            self._blocks(self.xlsx_blocks)
        if self.reader == "csv":
            if self.xlsx is not None or self.zip_member is not None:
                raise ValueError("V-14: reader csv takes no xlsx or zip_member spec")
        elif self.reader == "xlsx":
            if (self.xlsx is None and self.xlsx_blocks is None) or self.zip_member is not None:
                raise ValueError(
                    "V-14: reader xlsx takes an xlsx spec or xlsx_blocks (V-20) and no "
                    "zip_member spec"
                )
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


def epoch_outputs(epoch: HeaderEpoch) -> tuple[ColumnSpec, ...]:
    """The epoch's post-reshape output specs (ADR-042).

    Without ``unpivot`` this is ``epoch.columns`` itself. With it, the index
    columns, then ``projection_year`` (``int64``, non-nullable) and ``value``
    (typed by the :class:`ValueSpec`); their ``source`` is their own name, the
    column the unpivot yields.
    """
    if epoch.unpivot is None:
        return epoch.columns
    value = epoch.unpivot.value
    return (
        *epoch.columns,
        ColumnSpec(source=PROJECTION_YEAR, name=PROJECTION_YEAR, dtype="int64", nullable=False),
        ColumnSpec(
            source=VALUE,
            name=VALUE,
            dtype=value.dtype,
            nullable=value.nullable,
            null_tokens=value.null_tokens,
            min=value.min,
            max=value.max,
        ),
    )


def silver_columns(record: SchemaRecord) -> dict[str, ColumnSpec]:
    """Silver name -> first declaring post-reshape spec, in first-appearance order across epochs."""
    seen: dict[str, ColumnSpec] = {}
    for epoch in record.epochs:
        for column in epoch_outputs(epoch):
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
        specs = epoch_outputs(epoch)
        names = [column.name for column in specs]
        if len(set(names)) != len(names):
            raise _fail("V-1", f"epoch {index} maps two vendor columns to one silver name")
        for column in specs:
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
    occurrence = record.occurrence_column
    if occurrence is not None:
        if not SILVER_NAME_PATTERN.fullmatch(occurrence):
            raise _fail("V-22", f"occurrence_column {occurrence!r} is not a silver name")
        if occurrence in outputs or occurrence in RESERVED:
            raise _fail("V-22", f"occurrence_column {occurrence!r} collides with another column")
        if occurrence not in key_set:
            raise _fail("V-22", f"occurrence_column {occurrence!r} must be in entity_key")
        if not key_set & outputs:
            raise _fail(
                "V-22", "occurrence_column counts repeats of a typed entity_key column; none"
            )
    admissible = (
        outputs
        | {"issue_time"}
        | ({"resource_id"} if record.latest_partition else set())
        | ({EDITION} if record.edition_by_filename is not None else set())
        | ({occurrence} if occurrence is not None else set())
    )
    if not key_set <= admissible:
        raise _fail(
            "V-4",
            f"entity_key columns {sorted(key_set - admissible)} are not outputs",
        )
    if ("issue_time" in key_set) != issue:
        raise _fail(
            "V-4", "issue_time must be in entity_key exactly when some epoch declares an issue time"
        )
    for epoch in record.epochs:
        for spec in epoch_outputs(epoch):
            if spec.trim is not None and spec.name in key_set:
                raise _fail("V-19", f"column {spec.name!r} is in entity_key and cannot be trimmed")

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

    _check_scenario(record, key_set)

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


def validate_overlap_groups(records: Mapping[str, SchemaRecord]) -> None:
    """V-21: each overlap group of one package can be checked as one series (ADR-043).

    Every member selects per resource (``latest_partition: resource_id``), the
    group has at least two members, and every member has the same grain (its
    ``entity_key`` without ``resource_id``, in order) with the same silver dtype
    per grain column, so the members' keys are comparable values.

    Args:
        records: Every recorded family key of one package -> its record.

    Raises:
        RecordError: The first group that breaks the rule, named ``V-21``.
    """
    groups: dict[str, list[str]] = {}
    for key in sorted(records):
        group = records[key].overlap_group
        if group is not None:
            groups.setdefault(group, []).append(key)
    for group, members in sorted(groups.items()):
        for key in members:
            if records[key].latest_partition != "resource_id":
                raise _fail(
                    "V-21",
                    f"overlap group {group!r}: member {key!r} does not select per resource "
                    "(latest_partition resource_id)",
                )
        if len(members) < 2:
            raise _fail("V-21", f"overlap group {group!r} has one member ({members[0]!r})")
        first = members[0]
        grain = _grain(records[first])
        for key in members[1:]:
            other = _grain(records[key])
            if other != grain:
                raise _fail(
                    "V-21",
                    f"overlap group {group!r}: {key!r} has grain {list(other)}, {first!r} has "
                    f"{list(grain)}",
                )
            for name in grain:
                expected = _grain_dtype(records[first], name)
                actual = _grain_dtype(records[key], name)
                if actual != expected:
                    raise _fail(
                        "V-21",
                        f"overlap group {group!r}: grain column {name!r} is {actual} in {key!r} "
                        f"and {expected} in {first!r}",
                    )


def _grain(record: SchemaRecord) -> tuple[str, ...]:
    """The per-resource grain: ``entity_key`` without ``resource_id``, in order."""
    return tuple(column for column in record.entity_key if column != "resource_id")


def _grain_dtype(record: SchemaRecord, name: str) -> str:
    """A grain column's silver dtype (V-4 admits only outputs, ``issue_time`` and ``edition``)."""
    columns = silver_columns(record)
    if name in columns:
        return columns[name].dtype
    return "int64" if name in (EDITION, record.occurrence_column) else "datetime"


def _check_scenario(record: SchemaRecord, key_set: set[str]) -> None:
    """V-18: an unpivot record's year and value, and an edition record's key and selection."""
    if any(epoch.unpivot is not None for epoch in record.epochs):
        for index, epoch in enumerate(record.epochs):
            names = {column.name for column in epoch_outputs(epoch)}
            if not {PROJECTION_YEAR, VALUE} <= names:
                raise _fail(
                    "V-18",
                    f"epoch {index} does not declare {PROJECTION_YEAR!r} and {VALUE!r}, which "
                    "an unpivot record's every epoch yields",
                )
        if PROJECTION_YEAR not in key_set:
            raise _fail("V-18", f"an unpivot record's entity_key holds {PROJECTION_YEAR!r}")
    if record.edition_by_filename is not None:
        if EDITION not in key_set:
            raise _fail("V-18", f"an edition-mapped record's entity_key holds {EDITION!r}")
        if record.latest == "whole_capture" and record.latest_partition != "resource_id":
            raise _fail(
                "V-18",
                "an edition-mapped whole_capture record selects per resource "
                "(latest_partition resource_id); family scope would keep one edition",
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
        by_name = {column.name: column for column in epoch_outputs(epoch)}
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
