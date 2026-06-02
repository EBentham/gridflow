"""VTA-SCHEMA-01: fail-soft full-frame Pydantic validation in ``BaseSilverTransformer.run()``.

The keystone guarantee: every row of the ``transform()`` output is validated against the
transformer's ``schema_cls``; failures are logged, **counted** (``last_validation_failure_count``),
and surfaced by the CLI as ``completed_with_warnings`` — **never raised, never dropped**. These
tests prove the central mechanism in isolation with a minimal transformer + schema, including the
case the audit cared about most: a bad **non-first** row (row-0-only validation would miss it).
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl
from pydantic import Field

from gridflow.schemas.common import BaseSchema
from gridflow.silver.base import BaseSilverTransformer

if TYPE_CHECKING:
    from pathlib import Path


class _TinySchema(BaseSchema):
    """Minimal strict schema with a bounded field (inherits strict=True from BaseSchema)."""

    value: int = Field(ge=0, le=10)


class _ValidatingTransformer(BaseSilverTransformer):
    """Stub transformer whose ``transform()`` output rows are set per test via ``rows``."""

    source = "test_schema"
    dataset = "tiny"
    schema_cls = _TinySchema
    rows: tuple[int, ...] = ()

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        if not self.rows:
            return pl.DataFrame()
        return pl.DataFrame({"value": list(self.rows)})

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df


class _NoSchemaTransformer(_ValidatingTransformer):
    """Mirrors a generic/dynamic transformer (ENTSO-G generic incl. CMP, GIE generic): no schema."""

    dataset = "tiny_noschema"
    schema_cls = None


class TestFailSoftSchemaEnforcement:
    def test_bad_nonfirst_row_surfaced_as_warning_not_raised(self, tmp_path: Path) -> None:
        """Row 0 valid, a LATER row invalid: run() must not raise, must write ALL rows, and
        must surface the bad row as a counted warning (the keystone VTA-SCHEMA-01 acceptance)."""
        t = _ValidatingTransformer(tmp_path)
        t.rows = (5, 7, 99)  # 99 breaches le=10, at index 2 (a non-first row)
        rows_written = t.run(date(2026, 1, 1), run_id="test")
        assert rows_written == 3  # nothing dropped — fail-soft
        assert t.last_validation_failure_count == 1  # the bad non-first row surfaced

    def test_bad_first_row_also_failsoft(self, tmp_path: Path) -> None:
        """A bad row 0 must ALSO be fail-soft — proves the old row-0 hard-raise path is gone."""
        t = _ValidatingTransformer(tmp_path)
        t.rows = (99, 5, 5)
        rows_written = t.run(date(2026, 1, 1), run_id="test")
        assert rows_written == 3
        assert t.last_validation_failure_count == 1

    def test_clean_frame_no_warnings(self, tmp_path: Path) -> None:
        t = _ValidatingTransformer(tmp_path)
        t.rows = (1, 2, 3)
        rows_written = t.run(date(2026, 1, 1), run_id="test")
        assert rows_written == 3
        assert t.last_validation_failure_count == 0

    def test_all_rows_invalid_still_written(self, tmp_path: Path) -> None:
        """Even a fully-invalid frame is written (fail-soft never drops); the count == row count."""
        t = _ValidatingTransformer(tmp_path)
        t.rows = (50, 60, 70)
        rows_written = t.run(date(2026, 1, 1), run_id="test")
        assert rows_written == 3
        assert t.last_validation_failure_count == 3

    def test_none_schema_skips_validation(self, tmp_path: Path) -> None:
        """schema_cls=None (generic/dynamic/CMP families) is a no-op: no counting, no raise."""
        t = _NoSchemaTransformer(tmp_path)
        t.rows = (99, 99)
        rows_written = t.run(date(2026, 1, 1), run_id="test")
        assert rows_written == 2
        assert t.last_validation_failure_count == 0

    def test_counter_reset_after_dirty_then_clean_run(self, tmp_path: Path) -> None:
        """run() resets the counter before work, so a clean run after a dirty one reports 0."""
        t = _ValidatingTransformer(tmp_path)
        t.rows = (99, 99)
        t.run(date(2026, 1, 1), run_id="test")
        assert t.last_validation_failure_count == 2
        t.rows = (1, 2)
        t.run(date(2026, 1, 2), run_id="test")
        assert t.last_validation_failure_count == 0

    def test_empty_bronze_does_not_carry_prior_count(self, tmp_path: Path) -> None:
        """A date with no bronze early-returns but must reset the counter (not charge prior date)."""
        t = _ValidatingTransformer(tmp_path)
        t.rows = (99,)
        t.run(date(2026, 1, 1), run_id="test")
        assert t.last_validation_failure_count == 1
        t.rows = ()  # empty bronze -> early return at the is_empty guard
        t.run(date(2026, 1, 2), run_id="test")
        assert t.last_validation_failure_count == 0
