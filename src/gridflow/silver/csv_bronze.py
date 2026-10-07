"""CSV bronze-body reader — the repo's first (D-19, phase ``neso-data-portal``).

Every other gridflow source is a JSON or XML query API, so every existing
``read_bronze()`` does ``json.loads`` or an XML parse. The NESO Data Portal is
a *file-download* API: a resource is discovered in a CKAN catalogue and a CSV
file is downloaded whole. This module is the one place that turns those bytes
into a frame.

**Two callers, deliberately** (D-19):

1. the connector's D-36 *admission* check, at fetch time — a body that does not
   parse against its dataset's ``expected_columns`` never reaches **immutable**
   bronze, where it could not be corrected by re-running;
2. each transformer's ``read_bronze_file``, at transform time.

That is why these are free functions in their own module rather than methods on
``BaseSilverTransformer``: a connector importing the transformer base class
would be a layering inversion, and that class is already shared by 60+
transformers. Nothing here imports from ``silver/base.py``.

Design rules, all from D-19:

- **BOM is stripped explicitly, per file.** Its presence is a per-*resource*
  property, not a per-vendor constant (present on NESO's ``embedded-register``
  and ``interconnector-register``, absent on the three datasets this phase
  takes), so uniformity is never assumed. Left in place it becomes a U+FEFF
  prefix on the first column name and a correct body fails the header contract.
- **Decoding is strict.** A body that is not valid UTF-8 raises rather than
  being silently repaired into a replacement-character frame.
- **The header contract is exact and ordered.** Vendor schema drift fails loud;
  it is never absorbed by a rename map. Tolerating a reorder would silently
  re-map every column.
- **Every column is read as ``Utf8``** (``infer_schema_length=0``). The
  transformer casts explicitly with ``strict=True``, matching the
  ``BaseSchema(strict=True)`` convention in ``schemas/common.py``.
- **Rows with no content are dropped.** NESO's files can end with a line holding
  only ``\\r`` (``...6 \\n \\r \\n``), which Polars reads as a row whose every
  cell is null. That is a vendor line terminator, not data and not a validation
  failure, so it is dropped here (and counted in an INFO log) before it can
  reach silver as a null-key row.
"""

from __future__ import annotations

import io
import logging

import polars as pl

__all__ = [
    "CsvBronzeError",
    "CsvHeaderDriftError",
    "NotCsvBodyError",
    "read_csv_bronze_body",
]

logger = logging.getLogger(__name__)

_UTF8_BOM = b"\xef\xbb\xbf"


class CsvBronzeError(Exception):
    """Base class for every failure this reader raises."""


class NotCsvBodyError(CsvBronzeError):
    """The body is not CSV at all — HTML, a JSON envelope, or not UTF-8.

    Raised before any parse is attempted, so a vendor error page can never be
    mistaken for a zero-row capture.
    """


class CsvHeaderDriftError(CsvBronzeError):
    """The parsed header does not equal ``expected_columns`` exactly, in order."""


def read_csv_bronze_body(
    raw: bytes,
    *,
    expected_columns: tuple[str, ...],
    source_label: str,
    schema_overrides: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    """Parse a CSV bronze body into an all-``Utf8`` Polars frame.

    Args:
        raw: The vendor's bytes, exactly as they were downloaded or as they are
            stored in bronze. Never modified in place.
        expected_columns: The dataset's header contract — the exact column
            names, in the exact order, the body must carry.
        source_label: Names the body in error messages only. A URL at fetch
            time, a bronze path at transform time; the reader does not care
            which and never opens it.
        schema_overrides: Optional per-column dtype escape hatch, passed
            straight to Polars. Unused by default: the reader's contract is
            all-``Utf8`` and the transformer owns casting. It exists for the
            62 MB ``historic_generation_mix`` body should memory measurement
            demand it.

    Returns:
        A frame whose columns are exactly ``expected_columns``. A header-only
        body yields an empty frame rather than an error — the connector's
        definitive-absent guard (D-14) is what makes that unreachable in
        practice, and crashing here would turn a vendor edge case into a
        transform-time traceback. Rows in which every cell is null or
        empty/whitespace-only (NESO's trailing lone-``\\r`` line) are dropped
        and counted in one INFO record naming ``source_label``; nothing is
        logged when none drop. Any row with at least one non-blank cell is
        kept with its values untouched.

    Raises:
        NotCsvBodyError: The body is empty, starts (after whitespace) with
            ``<``, or is not valid UTF-8.
        CsvHeaderDriftError: The header differs from ``expected_columns`` in
            content or in order.
    """
    body = raw[len(_UTF8_BOM) :] if raw.startswith(_UTF8_BOM) else raw

    stripped = body.strip()
    if not stripped:
        raise NotCsvBodyError(
            f"empty CSV body from {source_label}: no non-whitespace content to parse"
        )
    if stripped.startswith(b"<"):
        raise NotCsvBodyError(
            f"non-CSV body from {source_label}: starts with '<', which is an HTML or "
            "XML document rather than the expected CSV"
        )

    try:
        body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NotCsvBodyError(f"non-CSV body from {source_label}: not valid UTF-8 ({exc})") from exc

    frame = pl.read_csv(
        io.BytesIO(body),
        infer_schema_length=0,
        schema_overrides=schema_overrides,
        encoding="utf8",
    )

    _assert_header_contract(frame.columns, expected_columns, source_label)
    # The contract passed against stripped names, so this is a whitespace
    # normalisation and never a rename: the caller is handed the column names
    # it declared, so a downstream cast-by-name cannot miss on a stray space.
    frame = frame.rename(dict(zip(frame.columns, expected_columns, strict=True)))
    return _drop_blank_rows(frame, source_label)


def _drop_blank_rows(frame: pl.DataFrame, source_label: str) -> pl.DataFrame:
    """Drop rows whose every cell is null or empty/whitespace-only.

    Only the blank test strips; kept rows are returned exactly as parsed.
    """
    if frame.width == 0 or frame.height == 0:
        return frame
    blank_cells = [
        pl.col(name).str.strip_chars().fill_null("").eq("")
        if dtype == pl.Utf8
        else pl.col(name).is_null()
        for name, dtype in frame.schema.items()
    ]
    is_blank_row = pl.all_horizontal(blank_cells)
    kept = frame.filter(~is_blank_row)
    dropped = frame.height - kept.height
    if dropped:
        logger.info(
            "dropped %d blank row(s) (every cell null or empty) from %s: "
            "vendor line terminator, not data",
            dropped,
            source_label,
        )
    return kept


def _assert_header_contract(
    actual: list[str],
    expected: tuple[str, ...],
    source_label: str,
) -> None:
    """Raise unless ``actual`` equals ``expected`` exactly, in order.

    Names are compared stripped: NESO resources mix ``\\n`` and ``\\r\\n`` line
    endings, and a stray space around a header cell is not schema drift.
    """
    normalised = tuple(name.strip() for name in actual)
    if normalised == tuple(expected):
        return

    missing = [name for name in expected if name not in normalised]
    unexpected = [name for name in normalised if name not in expected]
    detail = (
        f"missing={missing}, unexpected={unexpected}"
        if (missing or unexpected)
        else "same columns in a different ORDER"
    )
    raise CsvHeaderDriftError(
        f"CSV header drift in {source_label}: expected {list(expected)}, "
        f"got {list(normalised)} ({detail})"
    )
