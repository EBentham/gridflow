"""Offline profiler of NESO Data Portal bronze, with record proposals (ADR-036).

Usage::

    python -m gridflow.connectors.neso_data_portal.profile --snapshot <dir> \
        --field-info <dir> --batches <json> --out <dir> --report <md> \
        [--data-dir <dir>] [--family KEY ...] [--sample-rows N]

Reads the swept bronze, the field-info evidence and the catalogue snapshot,
and writes, under an absent or empty ``--out``: ``families/<key>.json`` (one
per measured family), then ``summary.json``, then the ``--report`` markdown,
each through ``files.replace_atomically`` (so an interrupted run leaves no
``summary.json``). It opens no network connection, writes nothing under the
data root and never edits the registry.

**Per capture** (usable CSV captures only; sidecar ``ckan_format`` = ``CSV``):
a chunked byte pass (size, BOM, strict UTF-8, CRLF/LF/CR, cp1252 evidence),
the header Polars parses (as ``readers.read_csv_body`` does), a bounded sample
(``--sample-rows``) that classifies value shapes, then two streaming passes
over the full body: (1) rows, all-blank rows, null and blank counts, value
lengths; (2) cast failures for the shapes' candidate dtypes, candidate-key and
full-row duplicates, and settlement-period coverage. Cast failures are counted
on each cell as the silver engine casts it (unstripped; only all-blank rows,
which its reader drops, are skipped), so a drafted dtype with zero failures
casts at transform time. Shapes are classified on stripped values, as
evidence only. A body never enters
Python whole (I-1). A ragged or unparseable body is recorded as
``parse_error`` and the run continues.

**Proposals.** Each family gets ``proposal = {record_draft, todos, flags}``.
The draft follows the frozen-record shape, but every unsettled field holds
the string ``"TODO: <id>"``; ``temporal`` is always one (no zone or period
is evidenced by bronze alone), so no draft is a valid ``SchemaRecord`` until
a human settles it, and ``latest`` is always ``whole_capture`` (the profiler
cannot know a vendor-documented identity; the ``key_identity`` TODO names the
best measured candidate). TODO kinds and their eligibility consequences are
:data:`TODO_CONSEQUENCES`; a ``held`` TODO sets the draft's eligibility to
held under the package's batch.

**Counts** (the one definition site; ``v0.22-PROFILE.md`` refers here):

- *measured family count*: registry families of kind ``tabular`` with at
  least one usable CSV capture;
- *distinct headers* of a family: its header-epoch count; of a batch: the sum
  over the batch's measured families (what the batch-sizing rule counts,
  ``over_15`` when it exceeds 15).

Exit codes: 0 the run completed (a body's measurement failure is data, not an
exit code); 1 an unregistered bronze directory or a failed snapshot or
field-info check; 2 usage, or an ``--out`` that exists and is not empty.
"""

from __future__ import annotations

import argparse
import codecs
import csv
import io
import itertools
import json
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from gridflow.connectors.neso_data_portal import captures as captures_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.evidence import (
    EvidenceError,
    field_entries,
    load_field_info,
    load_snapshot,
    vendor_unit,
)
from gridflow.connectors.neso_data_portal.files import replace_atomically
from gridflow.connectors.neso_data_portal.registry.record import RESERVED

if TYPE_CHECKING:
    from collections.abc import Mapping

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry import PackageEntry, Registry

__all__ = [
    "SHAPES",
    "TODO_CONSEQUENCES",
    "main",
    "render_report",
    "silver_name",
]

SAMPLE_ROWS = 50_000
CHUNK = 8 * 1024 * 1024
KEY_POOL = 12
KEY_MAX_SIZE = 3
KEY_CANDIDATES = 3
EXAMPLES = 3
EXAMPLE_CHARS = 80
MESSAGE_CHARS = 300
UNASSIGNED = "UNASSIGNED"
BATCH_HEADER_LIMIT = 15
_UTF8_BOM = b"\xef\xbb\xbf"

SHAPES: tuple[tuple[str, str], ...] = (
    ("int", r"^-?\d+$"),
    ("decimal", r"^-?\d*\.\d+([eE][-+]?\d+)?$"),
    ("iso_date", r"^\d{4}-\d{2}-\d{2}$"),
    ("slash_date", r"^\d{2}/\d{2}/\d{4}$"),
    ("month_label", r"^[A-Z][a-z]{2}-\d{2}$"),
    ("iso_datetime_z", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?Z$"),
    ("iso_datetime_offset", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?[+-]\d{2}:?\d{2}$"),
    ("naive_datetime", r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$"),
)
"""Value shapes, ordered; the first full match wins, anything else is ``text`` (P-3)."""

DATE_FORMATS: dict[str, str] = {
    "iso_date": "%Y-%m-%d",
    "slash_date": "%d/%m/%Y",
    "month_label": "%b-%y",
}
DATETIME_SHAPES = frozenset({"iso_datetime_z", "iso_datetime_offset", "naive_datetime"})
NUMERIC_SHAPES = frozenset({"int", "decimal"})

TODO_CONSEQUENCES: dict[str, str] = {
    "encoding": "blocked",
    "parse": "blocked",
    "date_order": "blocked",
    "type_conflict": "blocked",
    "time_semantics": "held",
    "unit": "held",
    "temporal": "none",
    "key_identity": "none",
    "vendor_publication": "none",
    "multi_epoch": "none",
}
"""TODO kind -> eligibility consequence (decision 16; P-6's table)."""

SP_HEADER = re.compile(r"(?i)^(settlement[ _]?period|sp|period)$")
"""A settlement-period header (P-4); its silver column is bounded 1..50."""
_DATE_HEADER = re.compile(r"(?i)date$")
_SLASH_PARTS = r"^(\d{2})/(\d{2})/\d{4}$"


# ---------------------------------------------------------------------------
# Silver names (P-3)
# ---------------------------------------------------------------------------


def silver_name(vendor: str, index: int) -> str:
    """Draft a silver column name from a vendor header (before repeat handling).

    Lower-case; runs of non-``[a-z0-9]`` become ``_``; ``_`` stripped; a
    leading digit takes ``c_``; an empty result is ``col_<index>``; a reserved
    name takes ``_vendor``.

    Args:
        vendor: The vendor header name.
        index: The column's 1-based position.

    Returns:
        The drafted name.
    """
    name = re.sub(r"[^a-z0-9]+", "_", vendor.lower()).strip("_")
    if not name:
        return f"col_{index}"
    if name[0].isdigit():
        name = f"c_{name}"
    if name in RESERVED:
        name = f"{name}_vendor"
    return name


def _epoch_names(header: tuple[str, ...]) -> list[str]:
    names: list[str] = []
    for index, vendor in enumerate(header, start=1):
        name = silver_name(vendor, index)
        if name in names:
            name = f"{name}_{index}"
        names.append(name)
    return names


# ---------------------------------------------------------------------------
# Per-capture measurement (P-2)
# ---------------------------------------------------------------------------


def _message(exc: Exception) -> str:
    """A parse failure as bounded, printable text (a binary body echoes raw bytes)."""
    text = f"{type(exc).__name__}: {exc}"[:MESSAGE_CHARS]
    return "".join(ch if ch.isprintable() else "?" for ch in text)


def _byte_pass(body: Path) -> dict[str, Any]:
    """Chunked: size, BOM, strict UTF-8, line endings; cp1252 when not UTF-8."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    utf8 = True
    size = crlf = lf = cr = 0
    bom = False
    carry = b""
    last = b""
    with body.open("rb") as handle:
        first = True
        while chunk := handle.read(CHUNK):
            if first:
                bom = chunk.startswith(_UTF8_BOM)
                first = False
            size += len(chunk)
            last = chunk[-1:]
            if utf8:
                try:
                    decoder.decode(chunk)
                except UnicodeDecodeError:
                    utf8 = False
            data = carry + chunk
            carry = b""
            if data.endswith(b"\r"):
                carry, data = b"\r", data[:-1]
            pairs = data.count(b"\r\n")
            crlf += pairs
            lf += data.count(b"\n") - pairs
            cr += data.count(b"\r") - pairs
    if carry:
        cr += 1
    if utf8:
        try:
            decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            utf8 = False
    out: dict[str, Any] = {
        "size": size,
        "bom": bom,
        "utf8": utf8,
        "crlf": crlf,
        "lf": lf,
        "cr": cr,
        "ends_with_newline": last in (b"\n", b"\r"),
    }
    if not utf8:
        out["cp1252_clean"] = _cp1252_clean(body)
    return out


def _cp1252_clean(body: Path) -> bool:
    decoder = codecs.getincrementaldecoder("cp1252")()
    try:
        with body.open("rb") as handle:
            while chunk := handle.read(CHUNK):
                decoder.decode(chunk)
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        return False
    return True


def _first_line_repeats(body: Path) -> bool:
    """Whether the raw header line repeats a name (HeaderEpoch refuses repeats)."""
    with body.open("rb") as handle:
        head = handle.read(1024 * 1024)
    line = head.split(b"\n", 1)[0].rstrip(b"\r")
    if line.startswith(_UTF8_BOM):
        line = line[len(_UTF8_BOM) :]
    text = line.decode("utf-8", errors="replace")
    names = [name.strip() for name in next(csv.reader(io.StringIO(text, newline="")), [])]
    return len(set(names)) != len(names)


def _clean(column: str) -> pl.Expr:
    """The column's stripped value, with whitespace-only read as null."""
    stripped = pl.col(column).str.strip_chars()
    return pl.when(stripped == "").then(None).otherwise(stripped)


def _shape_expr(column: str) -> pl.Expr:
    value = _clean(column)
    expr: Any = pl.when(value.is_null()).then(None)
    for name, pattern in SHAPES:
        expr = expr.when(value.str.contains(pattern)).then(pl.lit(name))
    shaped: pl.Expr = expr.otherwise(pl.lit("text"))
    return shaped


def _classify(sample: pl.DataFrame, raw: list[str]) -> dict[str, dict[str, Any]]:
    """Per column: shape counts, up to three examples, from the sample."""
    if sample.height == 0:
        return {name: {"shapes": {}, "examples": []} for name in raw}
    shapes = sample.select(_shape_expr(name).alias(name) for name in raw)
    out: dict[str, dict[str, Any]] = {}
    for name in raw:
        counts = Counter(v for v in shapes.get_column(name).to_list() if v is not None)
        values = sample.select(_clean(name).drop_nulls().unique(maintain_order=True).head(EXAMPLES))
        out[name] = {
            "shapes": dict(sorted(counts.items())),
            "examples": [str(v)[:EXAMPLE_CHARS] for v in values.to_series().to_list()],
        }
    return out


def _first_pass(lazy: pl.LazyFrame, raw: list[str]) -> dict[str, Any]:
    """Streaming: rows, all-blank rows, per column nulls, blanks and max length."""
    blank = [pl.col(c).is_null() | (pl.col(c).str.strip_chars() == "") for c in raw]
    exprs: list[pl.Expr] = [pl.len().alias("__rows")]
    exprs.append(pl.all_horizontal(blank).sum().alias("__blank_rows") if raw else pl.lit(0))
    for index, name in enumerate(raw):
        col = pl.col(name)
        exprs += [
            col.null_count().alias(f"n{index}"),
            (col.is_not_null() & (col.str.strip_chars() == "")).sum().alias(f"b{index}"),
            col.str.len_chars().max().alias(f"m{index}"),
        ]
    row = lazy.select(exprs).collect(engine="streaming").row(0, named=True)
    return {
        "rows": int(row["__rows"]),
        "all_blank_rows": int(row["__blank_rows"] or 0),
        "columns": {
            name: {
                "null_count": int(row[f"n{index}"] or 0),
                "blank_count": int(row[f"b{index}"] or 0),
                "max_len": int(row[f"m{index}"] or 0),
            }
            for index, name in enumerate(raw)
        },
    }


@dataclass
class CaptureProfile:
    """One capture's measurements, kept small (I-1)."""

    capture: Capture
    capture_id: str
    bytes_: dict[str, Any]
    encoding: Literal["utf8", "utf8-lossy"]
    raw: list[str] = field(default_factory=list)
    header: tuple[str, ...] = ()
    duplicate_header: bool = False
    parse_error: str | None = None
    rows: int | None = None
    all_blank_rows: int | None = None
    columns: dict[str, dict[str, Any]] = field(default_factory=dict)
    sampled: bool = False
    second: dict[str, Any] = field(default_factory=dict)

    def summary(self, epoch_index: int | None) -> dict[str, Any]:
        """The JSON-ready per-capture record."""
        out: dict[str, Any] = {
            "capture_id": self.capture_id,
            "resource_id": self.capture.resource_id,
            "written_at": self.capture.written_at.isoformat(),
            "ckan_last_modified": self.capture.ckan_last_modified,
            "url_type": self.capture.url_type,
            **self.bytes_,
            "epoch": epoch_index,
            "rows": self.rows,
            "all_blank_rows": self.all_blank_rows,
            "duplicate_header": self.duplicate_header,
            "parse_error": self.parse_error,
            "sampled": self.sampled,
        }
        return out


def _measure(capture: Capture, capture_id: str, sample_rows: int) -> tuple[CaptureProfile, Any]:
    """Byte pass, header, sample, first streaming pass. Returns the sample too."""
    body = capture.body
    measured = _byte_pass(body)
    encoding: Literal["utf8", "utf8-lossy"] = "utf8" if measured["utf8"] else "utf8-lossy"
    profile = CaptureProfile(capture, capture_id, measured, encoding)
    try:
        lazy = pl.scan_csv(body, infer_schema=False, encoding=encoding)
        profile.raw = lazy.collect_schema().names()
        profile.header = tuple(name.strip() for name in profile.raw)
        profile.duplicate_header = _first_line_repeats(body)
        sample = pl.read_csv(body, n_rows=sample_rows, infer_schema_length=0, encoding=encoding)
        shapes = _classify(sample, profile.raw)
        first = _first_pass(lazy, profile.raw)
    except pl.exceptions.PolarsError as exc:
        profile.parse_error = _message(exc)
        return profile, None
    profile.rows = first["rows"]
    profile.all_blank_rows = first["all_blank_rows"]
    profile.sampled = first["rows"] > sample_rows
    profile.columns = {name: {**first["columns"][name], **shapes[name]} for name in profile.raw}
    return profile, sample


def _hash_unique(columns: list[str]) -> pl.Expr:
    return pl.struct([pl.col(c) for c in columns]).hash().n_unique()


def _second_pass(
    profile: CaptureProfile,
    formats: dict[str, list[str]],
    slash: set[str],
    keys: list[list[str]],
    sp: tuple[str, str] | None,
) -> None:
    """Streaming: cast failures, key duplicates, settlement periods (P-2 step 5, P-4)."""
    raw_of = dict(zip(profile.header, profile.raw, strict=True))
    lazy = pl.scan_csv(profile.capture.body, infer_schema=False, encoding=profile.encoding)
    exprs: list[pl.Expr] = [pl.len().alias("__rows"), _hash_unique(profile.raw).alias("__full")]
    blank_row = pl.all_horizontal(
        [pl.col(c).is_null() | (pl.col(c).str.strip_chars() == "") for c in profile.raw]
    )
    for index, vendor in enumerate(profile.header):
        value = _clean(raw_of[vendor])
        # Casts are measured on the cell exactly as the silver engine casts it:
        # unstripped, with only the rows its reader drops (all-blank) skipped.
        cell = pl.when(~blank_row).then(pl.col(raw_of[vendor]))
        exprs += [
            (cell.is_not_null() & cell.cast(pl.Int64, strict=False).is_null())
            .sum()
            .alias(f"i{index}"),
            (cell.is_not_null() & cell.cast(pl.Float64, strict=False).is_null())
            .sum()
            .alias(f"f{index}"),
        ]
        for f_index, fmt in enumerate(formats.get(vendor, [])):
            parsed = cell.str.strptime(pl.Date, fmt, strict=False)
            exprs.append((cell.is_not_null() & parsed.is_null()).sum().alias(f"d{index}_{f_index}"))
        if vendor in slash:
            parts = value.str.extract_groups(_SLASH_PARTS)
            exprs += [
                (parts.struct.field("1").cast(pl.Int64, strict=False) > 12)
                .sum()
                .alias(f"s{index}_first"),
                (parts.struct.field("2").cast(pl.Int64, strict=False) > 12)
                .sum()
                .alias(f"s{index}_second"),
            ]
    present = [k for k in keys if all(c in raw_of for c in k)]
    for k_index, key in enumerate(present):
        exprs.append(_hash_unique([raw_of[c] for c in key]).alias(f"k{k_index}"))
    pair = (raw_of[sp[0]], raw_of[sp[1]]) if sp and all(c in raw_of for c in sp) else None
    if pair is not None:
        date_col, sp_col = pair
        period = pl.col(sp_col).str.strip_chars().cast(pl.Int64, strict=False)
        exprs += [
            period.min().alias("__sp_min"),
            period.max().alias("__sp_max"),
            (period.is_not_null() & ~period.is_between(1, 50)).sum().alias("__sp_out"),
            _hash_unique([date_col, sp_col]).alias("__sp_pairs"),
        ]
    row = lazy.select(exprs).collect(engine="streaming").row(0, named=True)
    rows = int(row["__rows"])
    casts: dict[str, dict[str, int]] = {}
    for index, vendor in enumerate(profile.header):
        failures = {"int64": int(row[f"i{index}"]), "float64": int(row[f"f{index}"])}
        for f_index, fmt in enumerate(formats.get(vendor, [])):
            failures[fmt] = int(row[f"d{index}_{f_index}"])
        casts[vendor] = failures
        if vendor in slash:
            profile.columns[raw_of[vendor]]["slash_first_gt_12"] = int(row[f"s{index}_first"] or 0)
            profile.columns[raw_of[vendor]]["slash_second_gt_12"] = int(
                row[f"s{index}_second"] or 0
            )
    profile.second = {
        "cast_failures": casts,
        "full_row_duplicates": rows - int(row["__full"]),
        "key_duplicates": {
            ",".join(key): rows - int(row[f"k{k_index}"]) for k_index, key in enumerate(present)
        },
    }
    if pair is not None and sp is not None:
        grouped = (
            lazy.group_by(pl.col(pair[0]).alias("__date"))
            .agg(pl.len().alias("rows"), pl.col(pair[1]).n_unique().alias("distinct"))
            .collect(engine="streaming")
        )
        per_date = Counter(str(n) for n in grouped.get_column("rows").to_list())
        repeated = grouped.filter(pl.col("rows") > pl.col("distinct")).height
        profile.second["settlement_periods"] = {
            "date_column": sp[0],
            "period_column": sp[1],
            "min": row["__sp_min"],
            "max": row["__sp_max"],
            "out_of_range": int(row["__sp_out"]),
            "duplicated_pairs": rows - int(row["__sp_pairs"]),
            "rows_per_date": dict(sorted(per_date.items(), key=lambda kv: int(kv[0]))),
            "dates_with_repeated_sp": repeated,
        }


# ---------------------------------------------------------------------------
# Family aggregation (P-4, P-5)
# ---------------------------------------------------------------------------


@dataclass
class Epoch:
    """One distinct header of a family and its summed measurements."""

    header: tuple[str, ...]
    profiles: list[CaptureProfile] = field(default_factory=list)

    def column(self, vendor: str) -> dict[str, Any]:
        """Measurements of one column summed across the epoch's captures."""
        shapes: Counter[str] = Counter()
        examples: list[str] = []
        out: dict[str, Any] = {"null_count": 0, "blank_count": 0, "max_len": 0}
        casts: dict[str, int] = {}
        slash_first = slash_second = 0
        for profile in self.profiles:
            raw = profile.raw[profile.header.index(vendor)]
            measured = profile.columns[raw]
            out["null_count"] += measured["null_count"]
            out["blank_count"] += measured["blank_count"]
            out["max_len"] = max(out["max_len"], measured["max_len"])
            shapes.update(measured["shapes"])
            examples += [e for e in measured["examples"] if e not in examples]
            slash_first += measured.get("slash_first_gt_12", 0)
            slash_second += measured.get("slash_second_gt_12", 0)
            for name, count in profile.second.get("cast_failures", {}).get(vendor, {}).items():
                casts[name] = casts.get(name, 0) + count
        out["shapes"] = dict(sorted(shapes.items()))
        out["examples"] = examples[:EXAMPLES]
        out["cast_failures"] = dict(sorted(casts.items()))
        if "slash_date" in shapes:
            out["slash_first_gt_12"] = slash_first
            out["slash_second_gt_12"] = slash_second
        return out


def _date_formats(shapes: dict[str, int]) -> list[str]:
    return [DATE_FORMATS[s] for s in DATE_FORMATS if s in shapes]


def _sp_pair(header: tuple[str, ...], shapes: dict[str, dict[str, int]]) -> tuple[str, str] | None:
    period = next(
        (h for h in header if SP_HEADER.match(h) and shapes[h] and set(shapes[h]) == {"int"}),
        None,
    )
    if period is None:
        return None
    date = next(
        (
            h
            for h in header
            if _DATE_HEADER.search(h) and shapes[h] and set(shapes[h]) <= set(DATE_FORMATS)
        ),
        None,
    )
    return None if date is None else (date, period)


def _key_candidates(sample: pl.DataFrame, profile: CaptureProfile) -> list[list[str]]:
    """Minimal subsets (size 1..3) of the pool unique in the sample; first three."""
    if sample.height == 0:
        return []
    pool = [
        vendor
        for vendor, raw in zip(profile.header, profile.raw, strict=True)
        if profile.columns[raw]["null_count"] + profile.columns[raw]["blank_count"] == 0
    ][:KEY_POOL]
    raw_of = dict(zip(profile.header, profile.raw, strict=True))
    found: list[tuple[str, ...]] = []
    for size in range(1, KEY_MAX_SIZE + 1):
        subsets = [
            combo
            for combo in itertools.combinations(pool, size)
            if not any(set(key) <= set(combo) for key in found)
        ]
        if not subsets:
            continue
        uniques = sample.select(
            _hash_unique([raw_of[c] for c in combo]).alias(str(i))
            for i, combo in enumerate(subsets)
        ).row(0)
        for combo, unique in zip(subsets, uniques, strict=True):
            if unique == sample.height:
                found.append(combo)
        if len(found) >= KEY_CANDIDATES:
            break
    index = {vendor: i for i, vendor in enumerate(profile.header)}
    ordered = sorted(found, key=lambda k: (len(k), [index[c] for c in k]))
    return [list(k) for k in ordered[:KEY_CANDIDATES]]


# ---------------------------------------------------------------------------
# Proposal (P-3, P-6)
# ---------------------------------------------------------------------------


class _Todos:
    """The family's TODO list; ids are ``T01``, ``T02``, ... in raise order."""

    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []

    def add(self, kind: str, field_name: str, question: str) -> str:
        """Record one TODO and return the ``"TODO: <id>"`` string that marks its field."""
        todo_id = f"T{len(self.items) + 1:02d}"
        self.items.append(
            {
                "id": todo_id,
                "kind": kind,
                "field": field_name,
                "question": question,
                "consequence": TODO_CONSEQUENCES[kind],
            }
        )
        return f"TODO: {todo_id}"


def _date_shape(kinds: set[str]) -> str | None:
    """The one date shape a column's values all take, else ``None``."""
    if len(kinds) == 1 and kinds <= set(DATE_FORMATS):
        (kind,) = kinds
        return kind
    return None


@dataclass
class _Draft:
    """One drafted column plus what it contributes to the family's TODOs."""

    column: dict[str, Any]
    temporal: bool = False
    flag: str | None = None


def _draft_column(
    vendor: str,
    name: str,
    measured: dict[str, Any],
    fields: dict[str, dict[str, Any]],
    todos: _Todos,
) -> _Draft:
    """Apply the dtype draft rule (P-3, the one site) to one column of one epoch.

    1. A field-info physical type decides the dtype (``timestamp`` -> string
       with a ``time_semantics`` TODO); bronze decides a date's format.
    2. Bronze can contradict it: a declared int/numeric/date whose cast fails
       in any capture is a ``type_conflict`` TODO.
    3. Without field-info the shape decides.
    4. A ``text`` column whose every value casts to one date shape's format is
       drafted ``date`` and flagged ``text_declared_date_shaped``.
    """
    kinds = set(measured["shapes"])
    casts: dict[str, int] = measured["cast_failures"]
    declared = str((fields.get(vendor) or {}).get("type") or "").lower()
    column: dict[str, Any] = {"source": vendor, "name": name, "nullable": True}
    where = f"column {vendor!r}"
    date_kind = _date_shape(kinds)
    day_first_unproven = date_kind == "slash_date" and not measured.get("slash_first_gt_12")
    fmt = DATE_FORMATS[date_kind] if date_kind is not None else None
    casts_clean = fmt is not None and casts.get(fmt, 1) == 0

    def dated(flag: str | None = None) -> _Draft:
        if day_first_unproven:
            column["dtype"], column["format"] = (
                "date",
                todos.add(
                    "date_order",
                    f"{where}.format",
                    f"{vendor}: dd/mm/yyyy-shaped values never exceed 12 in the first field; "
                    "day-first or month-first?",
                ),
            )
        else:
            column["dtype"], column["format"] = "date", fmt
        return _Draft(column, temporal=True, flag=flag)

    if declared.startswith("timestamp") or kinds & DATETIME_SHAPES:
        column["dtype"] = "string"
        todos.add(
            "time_semantics",
            where,
            f"{vendor}: the zone, period start/end and clock of these datetimes are "
            "undocumented (local_instant is forbidden until evidenced)",
        )
        return _Draft(column, temporal=True)
    if declared:
        if declared.startswith(("numeric", "float")):
            dtype: str = "float64"
        elif declared.startswith("int"):
            dtype = "int64"
        elif declared == "date":
            dtype = "date"
        else:
            dtype = "string"
        if dtype == "date":
            if date_kind is None or (not day_first_unproven and not casts_clean):
                column["dtype"] = todos.add(
                    "type_conflict",
                    f"{where}.dtype",
                    f"{vendor}: field-info declares date but the bronze values do not all "
                    "cast to one date format",
                )
                return _Draft(column, temporal=True)
            return dated()
        if dtype in ("int64", "float64") and casts.get(dtype, 0) > 0:
            column["dtype"] = todos.add(
                "type_conflict",
                f"{where}.dtype",
                f"{vendor}: field-info declares {declared} but {casts[dtype]} value(s) do not "
                f"cast to {dtype}",
            )
            return _Draft(column)
        if dtype == "string" and date_kind is not None and not day_first_unproven and casts_clean:
            return dated(f"text_declared_date_shaped:{vendor}")
        column["dtype"] = dtype
    elif kinds and kinds <= NUMERIC_SHAPES:
        if kinds == {"int"} and casts.get("int64", 1) == 0:
            column["dtype"] = "int64"
        elif casts.get("float64", 1) == 0:
            column["dtype"] = "float64"
        else:
            column["dtype"] = "string"
    elif date_kind is not None and (day_first_unproven or casts_clean):
        return dated()
    else:
        column["dtype"] = "string"
    if column["dtype"] in ("int64", "float64") and vendor_unit(fields.get(vendor)) is None:
        todos.add("unit", f"{where}.unit", f"{vendor}: a numeric column with no vendor unit")
    temporal = bool(SP_HEADER.match(vendor)) or "month" in vendor.lower()
    return _Draft(column, temporal=temporal)


def _propose(
    package: PackageEntry,
    key: str,
    epochs: list[Epoch],
    measured: list[list[dict[str, Any]]],
    keys: dict[str, Any],
    newest_header: tuple[str, ...],
    profiles: list[CaptureProfile],
    fields: dict[str, dict[str, Any]],
    batch: str,
) -> dict[str, Any]:
    """Build ``{record_draft, todos, flags}`` for one family (P-6).

    The draft is deliberately not a valid ``SchemaRecord``: ``temporal`` is
    always a ``TODO`` string (no recipe is evidenced by bronze alone), as is
    any other unsettled field; ``latest`` is always ``whole_capture``.
    """
    todos = _Todos()
    flags: list[str] = []
    temporal_columns: list[str] = []
    draft_epochs: list[dict[str, Any]] = []
    for epoch, columns in zip(epochs, measured, strict=True):
        drafted: list[dict[str, Any]] = []
        for vendor, name, column in zip(
            epoch.header, _epoch_names(epoch.header), columns, strict=True
        ):
            drafted_column = _draft_column(vendor, name, column, fields, todos)
            drafted.append(drafted_column.column)
            if drafted_column.temporal and vendor not in temporal_columns:
                temporal_columns.append(vendor)
            if drafted_column.flag is not None:
                flags.append(drafted_column.flag)
        draft_epochs.append(
            {"header": list(epoch.header), "columns": drafted, "issue": {"kind": "none"}}
        )

    encoding = "utf-8"
    if any(not p.bytes_["utf8"] for p in profiles):
        encoding = todos.add(
            "encoding",
            "encoding",
            "a capture is not valid UTF-8 (see cp1252_clean); the vendor's encoding is unevidenced",
        )
    if any(p.parse_error or p.duplicate_header for p in profiles):
        todos.add("parse", "epochs", "a capture is ragged, unparseable or repeats a header name")
    temporal = todos.add(
        "temporal",
        "temporal",
        "no temporal recipe is evidenced; timestamp_utc stays the capture time. Candidate "
        "columns: " + (", ".join(temporal_columns) or "none"),
    )

    names = dict(zip(newest_header, _epoch_names(newest_header), strict=True))
    unique = next((c for c in keys["candidates"] if c["max_duplicates"] == 0), None)
    if unique is not None:
        entity_key = [names[vendor] for vendor in unique["columns"]]
        evidence = f"measured unique in {unique['captures_checked']} capture(s)"
    else:
        entity_key = list(names.values())
        evidence = "no candidate measured unique, so every column"
    todos.add(
        "key_identity",
        "entity_key",
        f"a vendor-documented identity for {entity_key} ({evidence}); without one, latest "
        "stays whole_capture",
    )
    dump = any(r.url_type == "datastore" for r in package.resources if r.family == key)
    if dump:
        todos.add(
            "vendor_publication",
            "vintage",
            "available_at is gridflow capture time for the dump; a vendor instant is held (D)",
        )
    if len(epochs) > 1:
        todos.add("multi_epoch", "epochs", f"{len(epochs)} header epochs; review each in its batch")

    draft: dict[str, Any] = {
        "version": "1",
        "reader": "csv",
        "encoding": encoding,
        "epochs": draft_epochs,
        "temporal": temporal,
        "entity_key": entity_key,
        "latest": "whole_capture",
        "vintage": "capture_fallback" if dump else "ckan_last_modified",
    }
    held = [todo for todo in todos.items if todo["consequence"] == "held"]
    if held:
        draft["eligibility"] = {
            "status": "held",
            "question": "; ".join(todo["question"] for todo in held),
            "unit": batch,
        }
    return {"record_draft": draft, "todos": todos.items, "flags": sorted(set(flags))}


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


def _round(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {k: _round(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_round(v) for v in value]
    return value


def _dump(document: Any) -> bytes:
    text = json.dumps(_round(document), sort_keys=True, indent=2, ensure_ascii=False)
    return (text + "\n").encode("utf-8")


def _ckan_format(capture: Capture) -> str:
    meta: Any = json.loads(capture.sidecar.read_text(encoding="utf-8"))
    params = meta.get("request_params") if isinstance(meta, dict) else None
    value = params.get("ckan_format") if isinstance(params, dict) else None
    return str(value or "").upper()


def _relative(path: Path, data_dir: Path) -> str:
    return path.relative_to(data_dir).as_posix()


def _snapshot_view(snapshot_package: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot_package is None:
        return {}
    organization = snapshot_package.get("organization")
    return {
        "title": snapshot_package.get("title"),
        "organization": organization.get("title") if isinstance(organization, dict) else None,
        "license_title": snapshot_package.get("license_title"),
        "extras": snapshot_package.get("extras", []),
    }


def _field_info_view(header: tuple[str, ...], fields: dict[str, dict[str, Any]]) -> dict[str, Any]:
    columns: dict[str, Any] = {}
    for vendor in header:
        entry = fields.get(vendor)
        if entry is None:
            continue
        raw_info = entry.get("info")
        info: dict[str, Any] = raw_info if isinstance(raw_info, dict) else {}
        columns[vendor] = {
            "type": entry.get("type"),
            "unit": vendor_unit(entry),
            "title": info.get("title"),
            "description": (str(info.get("description") or "").strip()[:MESSAGE_CHARS] or None),
        }
    return {
        "columns": columns,
        "header_not_in_field_info": [v for v in header if v not in fields] if fields else [],
        "field_info_not_in_header": sorted(set(fields) - set(header)),
    }


def profile_family(
    key: str,
    registry: Registry,
    usable: list[Capture],
    other: int,
    data_dir: Path,
    sample_rows: int,
    field_doc: dict[str, Any] | None,
    snapshot_package: dict[str, Any] | None,
    batch: str,
) -> dict[str, Any]:
    """Measure one family's CSV captures and draft its proposal.

    Args:
        key: The family key.
        registry: The loaded registry.
        usable: The family's usable CSV captures.
        other: How many usable non-CSV captures it holds (listed, never profiled).
        data_dir: The data root (capture ids are relative to it).
        sample_rows: The sample bound.
        field_doc: The family's field-info document, when present.
        snapshot_package: The package's snapshot entry.
        batch: The package's batch.

    Returns:
        The family document.
    """
    package, family = registry.families[key]
    ordered = sorted(usable, key=lambda c: (c.written_at, c.body.name))
    profiles: list[CaptureProfile] = []
    newest: tuple[CaptureProfile, Any] | None = None
    for capture in ordered:
        profile, sample = _measure(capture, _relative(capture.body, data_dir), sample_rows)
        profiles.append(profile)
        if sample is not None:
            newest = (profile, sample)
        del sample

    epochs: list[Epoch] = []
    for profile in profiles:
        if profile.parse_error is not None:
            continue
        match = next((e for e in epochs if e.header == profile.header), None)
        if match is None:
            match = Epoch(profile.header)
            epochs.append(match)
        match.profiles.append(profile)

    candidates: list[list[str]] = []
    key_header: tuple[str, ...] | None = None
    if newest is not None:
        candidates = _key_candidates(newest[1], newest[0])
        key_header = newest[0].header
    newest = None

    epoch_plans: list[tuple[dict[str, list[str]], set[str], tuple[str, str] | None]] = []
    for epoch in epochs:
        shapes = {vendor: epoch.column(vendor)["shapes"] for vendor in epoch.header}
        formats = {vendor: _date_formats(s) for vendor, s in shapes.items()}
        slash = {vendor for vendor, s in shapes.items() if "slash_date" in s}
        epoch_plans.append((formats, slash, _sp_pair(epoch.header, shapes)))
        for profile in epoch.profiles:
            try:
                _second_pass(profile, formats, slash, candidates, epoch_plans[-1][2])
            except pl.exceptions.PolarsError as exc:
                profile.parse_error = _message(exc)

    fields = field_entries(field_doc)
    measured = [[epoch.column(v) for v in epoch.header] for epoch in epochs]
    keys: dict[str, Any] = {"candidates": [], "full_row_max_duplicates": 0}
    checked = [p for p in profiles if p.second]
    keys["full_row_max_duplicates"] = max(
        (p.second["full_row_duplicates"] for p in checked), default=0
    )
    for columns in candidates:
        label = ",".join(columns)
        values = [
            p.second["key_duplicates"][label]
            for p in checked
            if label in p.second.get("key_duplicates", {})
        ]
        keys["candidates"].append(
            {
                "columns": columns,
                "max_duplicates": max(values, default=0),
                "captures_checked": len(values),
            }
        )

    flags: list[str] = []
    if len(epochs) > 1:
        flags.append("multi_epoch")
    if any(not p.bytes_["utf8"] for p in profiles):
        flags.append("not_utf8")
    if any(p.parse_error for p in profiles):
        flags.append("parse_error")
    if any(p.duplicate_header for p in profiles):
        flags.append("duplicate_header")
    if any(p.sampled for p in profiles):
        flags.append("sampled")
    if other:
        flags.append(f"non_csv_captures:{other}")
    if batch == UNASSIGNED:
        flags.append("unassigned_batch")

    proposal: dict[str, Any] | None = None
    if epochs:
        newest_header = key_header if key_header is not None else epochs[-1].header
        proposal = _propose(
            package, key, epochs, measured, keys, newest_header, profiles, fields, batch
        )
    epoch_index = {id(p): i for i, e in enumerate(epochs) for p in e.profiles}
    return {
        "key": key,
        "package": package.package,
        "batch": batch,
        "archetype": family.archetype,
        "refresh": family.refresh,
        "snapshot": _snapshot_view(snapshot_package),
        "captures": [p.summary(epoch_index.get(id(p))) for p in profiles],
        "other_captures": other,
        "epochs": [
            {
                "header": list(epoch.header),
                "captures": len(epoch.profiles),
                "first_written_at": epoch.profiles[0].capture.written_at.isoformat(),
                "last_written_at": epoch.profiles[-1].capture.written_at.isoformat(),
                "resource_ids": sorted({p.capture.resource_id for p in epoch.profiles}),
                "rows": sum(p.rows or 0 for p in epoch.profiles),
                "columns": dict(zip(epoch.header, columns, strict=True)),
                "field_info": _field_info_view(epoch.header, fields),
                "settlement_periods": [
                    {"capture_id": p.capture_id, **p.second["settlement_periods"]}
                    for p in epoch.profiles
                    if "settlement_periods" in p.second
                ],
            }
            for epoch, columns in zip(epochs, measured, strict=True)
        ],
        "keys": keys,
        "flags": flags,
        "proposal": proposal,
    }


def _sibling_flags(documents: dict[str, dict[str, Any]]) -> None:
    """Flag families of one package that share an identical epoch header (never merged)."""
    by_header: dict[tuple[str, tuple[str, ...]], set[str]] = {}
    for key, document in documents.items():
        for epoch in document["epochs"]:
            by_header.setdefault((document["package"], tuple(epoch["header"])), set()).add(key)
    for members in by_header.values():
        if len(members) < 2:
            continue
        for key in members:
            flag = "sibling_candidate:" + ",".join(sorted(members - {key}))
            if flag not in documents[key]["flags"]:
                documents[key]["flags"].append(flag)
    for document in documents.values():
        document["flags"].sort()
        if document["proposal"] is not None:
            document["proposal"]["flags"] = sorted(
                set(document["proposal"]["flags"]) | set(document["flags"])
            )


def _todo_tally(document: dict[str, Any]) -> dict[str, int]:
    tally = dict.fromkeys(("blocked", "held", "none"), 0)
    for todo in (document["proposal"] or {}).get("todos", []):
        tally[todo["consequence"]] += 1
    return tally


def build_summary(
    documents: dict[str, dict[str, Any]],
    registry: Registry,
    batches: dict[str, str],
    inputs: dict[str, Any],
    unusable: list[dict[str, str]],
    unprofiled: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """The ``summary.json`` document (P-7), from the family documents only.

    Args:
        documents: Family key -> family document.
        registry: The loaded registry (every package counts toward its batch).
        batches: The batch map; a package missing from it is ``UNASSIGNED``.
        inputs: The run's recorded inputs (no host or clock values).
        unusable: Unusable sidecars, data-root-relative.
        unprofiled: Usable captures listed but never profiled, by kind.

    Returns:
        ``{inputs, totals, batches, families, unusable}``.
    """
    batch_of = {p.package: batches.get(p.package, UNASSIGNED) for p in registry.packages}
    table: dict[str, dict[str, Any]] = {}
    for package in registry.packages:
        row = table.setdefault(
            batch_of[package.package],
            {"packages": 0, "families_with_csv": 0, "distinct_headers": 0},
        )
        row["packages"] += 1
    for document in documents.values():
        row = table[document["batch"]]
        row["families_with_csv"] += 1
        row["distinct_headers"] += len(document["epochs"])
    for row in table.values():
        row["over_15"] = row["distinct_headers"] > BATCH_HEADER_LIMIT
    families = {
        key: {
            "package": d["package"],
            "batch": d["batch"],
            "csv_captures": len(d["captures"]),
            "other_captures": d["other_captures"],
            "epochs": len(d["epochs"]),
            "rows_newest": d["captures"][-1]["rows"] if d["captures"] else None,
            "flags": d["flags"],
            "todos_by_consequence": _todo_tally(d),
        }
        for key, d in sorted(documents.items())
    }
    consequence = Counter[str]()
    for row in families.values():
        consequence.update(row["todos_by_consequence"])
    sibling_groups = {
        tuple(sorted({key, *flag.split(":", 1)[1].split(",")}))
        for key, d in documents.items()
        for flag in d["flags"]
        if flag.startswith("sibling_candidate:")
    }
    totals = {
        "measured_families": len(documents),
        "csv_captures": sum(len(d["captures"]) for d in documents.values()),
        "non_csv_captures": (unprofiled or {}).get(
            "non_csv_captures", sum(d["other_captures"] for d in documents.values())
        ),
        "files_family_csv_captures": (unprofiled or {}).get("files_family_csv_captures", 0),
        "unusable_captures": len(unusable),
        "multi_epoch_families": sum("multi_epoch" in d["flags"] for d in documents.values()),
        "sibling_candidate_groups": len(sibling_groups),
        "not_utf8_captures": sum(not c["utf8"] for d in documents.values() for c in d["captures"]),
        "parse_failures": sum(
            c["parse_error"] is not None for d in documents.values() for c in d["captures"]
        ),
        "distinct_headers": sum(len(d["epochs"]) for d in documents.values()),
        "todos_by_consequence": {k: consequence.get(k, 0) for k in ("blocked", "held", "none")},
    }
    return {
        "inputs": inputs,
        "totals": totals,
        "batches": dict(sorted(table.items())),
        "families": families,
        "unusable": unusable,
    }


def render_report(summary: dict[str, Any]) -> str:
    """Render ``v0.22-PROFILE.md`` from ``summary.json`` alone (P-7).

    Args:
        summary: The parsed ``summary.json``.

    Returns:
        The markdown, LF line endings.
    """
    inputs, totals = summary["inputs"], summary["totals"]
    todos = totals["todos_by_consequence"]
    lines = [
        "# v0.22 NESO bronze profile",
        "",
        "<!-- Generated by `python -m gridflow.connectors.neso_data_portal.profile` from"
        " `summary.json`. Do not edit by hand. -->",
        "",
        f"Inputs: snapshot `{inputs['snapshot_id']}`, field-info run `{inputs['field_info']}`,"
        f" sample rows {inputs['sample_rows']}. The count definitions are stated once, in the"
        " docstring of `gridflow.connectors.neso_data_portal.profile`: the measured family"
        " count is registry `tabular` families with at least one usable CSV capture; a batch's"
        " distinct headers are the sum of its families' header epochs.",
        "",
        "## Headline",
        "",
        f"- Measured family count: **{totals['measured_families']}**",
        f"- CSV captures profiled: {totals['csv_captures']} (non-CSV listed, not profiled:"
        f" {totals['non_csv_captures']}; CSV captures of files families, not profiled:"
        f" {totals['files_family_csv_captures']}; unusable: {totals['unusable_captures']})",
        f"- Distinct headers: {totals['distinct_headers']}; multi-epoch families:"
        f" {totals['multi_epoch_families']}; sibling-candidate groups:"
        f" {totals['sibling_candidate_groups']}",
        f"- Captures not valid UTF-8: {totals['not_utf8_captures']}; parse failures:"
        f" {totals['parse_failures']}",
        f"- TODOs: blocked {todos['blocked']}, held {todos['held']}, none {todos['none']}",
        "",
        "## Batches",
        "",
        "| Batch | Packages | CSV families | Distinct headers | Over 15 |",
        "|---|---|---|---|---|",
    ]
    for batch, row in summary["batches"].items():
        lines.append(
            f"| {batch} | {row['packages']} | {row['families_with_csv']} |"
            f" {row['distinct_headers']} | {'yes' if row['over_15'] else 'no'} |"
        )
    lines += [
        "",
        "## Families",
        "",
        "| Family | Package | Batch | Captures | Epochs | Rows (newest) | Flags |"
        " TODO blocked/held/none |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for key, row in summary["families"].items():
        tally = row["todos_by_consequence"]
        flags = ", ".join(row["flags"]).replace("|", "\\|") or "—"
        rows = "—" if row["rows_newest"] is None else str(row["rows_newest"])
        lines.append(
            f"| `{key}` | `{row['package']}` | {row['batch']} | {row['csv_captures']} |"
            f" {row['epochs']} | {rows} | {flags} |"
            f" {tally['blocked']}/{tally['held']}/{tally['none']} |"
        )
    lines.append("")
    return "\n".join(lines)


def _usage(message: str) -> int:
    print(message, file=sys.stderr)
    return 2


def main(argv: list[str] | None = None) -> int:
    """Entry point. Exit 0 completed, 1 failed evidence or unregistered bronze, 2 usage."""
    parser = argparse.ArgumentParser(prog="python -m gridflow.connectors.neso_data_portal.profile")
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--field-info", type=Path, required=True)
    parser.add_argument("--batches", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--family", action="append", default=None)
    parser.add_argument("--sample-rows", type=int, default=SAMPLE_ROWS)
    args = parser.parse_args(argv)
    started = time.perf_counter()

    out: Path = args.out
    if out.exists() and (not out.is_dir() or any(out.iterdir())):
        return _usage(f"--out {out} exists and is not empty")
    if args.sample_rows < 1:
        return _usage("--sample-rows must be at least 1")
    try:
        batches: Any = json.loads(Path(args.batches).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return _usage(f"cannot read --batches {args.batches}: {exc}")
    if not isinstance(batches, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in batches.items()
    ):
        return _usage(f"--batches {args.batches} is not a {{slug: batch}} object")

    try:
        snapshot = load_snapshot(args.snapshot)
        field_info = load_field_info(args.field_info, str(snapshot.get("snapshot_id")))
    except EvidenceError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    data_dir: Path
    if args.data_dir is not None:
        data_dir = args.data_dir
    else:
        from gridflow.config.settings import load_settings

        data_dir = load_settings().pipeline.data_dir
    registry = registry_module.load_registry()
    try:
        captures_module.assert_bronze_dirs_registered(data_dir, registry)
    except captures_module.RegistryFreezeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    root = captures_module.bronze_source_dir(data_dir)
    wanted = set(args.family) if args.family else None
    by_slug = {str(p.get("name")): p for p in snapshot.get("packages", []) if isinstance(p, dict)}
    documents: dict[str, dict[str, Any]] = {}
    unusable: list[dict[str, str]] = []
    capture_count = 0
    unprofiled: Counter[str] = Counter()
    dirs = sorted(p for p in root.iterdir() if p.is_dir()) if root.is_dir() else []
    for dataset_dir in dirs:
        key = dataset_dir.name
        if wanted is not None and key not in wanted:
            continue
        package, family = registry.families[key]
        scan = captures_module.scan_dataset(dataset_dir, registry)
        unusable += [
            {"sidecar": _relative(item.sidecar, data_dir), "reason": item.reason}
            for item in scan.unusable
        ]
        csv_captures = [c for c in scan.captures if _ckan_format(c) == "CSV"]
        unprofiled["non_csv_captures"] += len(scan.captures) - len(csv_captures)
        if family.kind != "tabular":
            unprofiled["files_family_csv_captures"] += len(csv_captures)
            continue
        if not csv_captures:
            continue
        capture_count += len(csv_captures)
        documents[key] = profile_family(
            key,
            registry,
            csv_captures,
            len(scan.captures) - len(csv_captures),
            data_dir,
            args.sample_rows,
            field_info.get(key),
            by_slug.get(package.package),
            batches.get(package.package, UNASSIGNED),
        )
    _sibling_flags(documents)

    inputs = {
        "snapshot_id": snapshot.get("snapshot_id"),
        "field_info": Path(args.field_info).name,
        "sample_rows": args.sample_rows,
    }
    summary = build_summary(
        documents,
        registry,
        batches,
        inputs,
        sorted(unusable, key=lambda u: u["sidecar"]),
        unprofiled,
    )
    (out / "families").mkdir(parents=True, exist_ok=True)
    for key, document in sorted(documents.items()):
        replace_atomically(out / "families" / f"{key}.json", _dump(document))
    replace_atomically(out / "summary.json", _dump(summary))
    report: Path = args.report
    report.parent.mkdir(parents=True, exist_ok=True)
    replace_atomically(report, render_report(summary).encode("utf-8"))
    print(
        f"profile: families {len(documents)} captures {capture_count} "
        f"seconds {time.perf_counter() - started:.1f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
