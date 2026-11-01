"""Unit tests for the shared ``with_published_at`` vintage parser.

The nominal form and two unambiguous UTC variants parse; every other present
value is nulled, counted and warned about once, naming the dataset.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import polars as pl

from gridflow.silver.published_at import with_published_at

if TYPE_CHECKING:
    import pytest

_WELL_FORMED = "2024-01-14T12:00:00Z"
_EXPECTED = datetime(2024, 1, 14, 12, 0, tzinfo=UTC)


def _df(values: list[str | None]) -> pl.DataFrame:
    return pl.DataFrame({"document_created_at": values})


class TestWellFormedAndUnambiguousVariants:
    """The nominal form plus the two unambiguous variants must parse to the
    same instant, with zero warnings."""

    def test_well_formed_stamp_no_warning(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(_df([_WELL_FORMED]), dataset="test_dataset")
        assert result["published_at"].to_list() == [_EXPECTED]
        assert result["published_at"].dtype == pl.Datetime("us", "UTC")
        assert not caplog.records

    def test_fractional_seconds_variant_parses_no_warning(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(_df(["2024-01-14T12:00:00.000Z"]), dataset="test_dataset")
        assert result["published_at"].to_list() == [_EXPECTED]
        assert not caplog.records

    def test_explicit_offset_variant_parses_no_warning(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(_df(["2024-01-14T12:00:00+00:00"]), dataset="test_dataset")
        assert result["published_at"].to_list() == [_EXPECTED]
        assert not caplog.records


class TestMalformedFormsCountedAndWarned:
    """Genuinely malformed/ambiguous forms must null AND be counted into a
    single WARNING naming the dataset — never a silent null."""

    def test_space_separated_nulls_with_warning(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(
                _df([_WELL_FORMED, "2024-01-14 12:00:00Z"]), dataset="my_dataset"
            )
        assert result["published_at"].to_list() == [_EXPECTED, None]
        assert result["published_at"].dtype == pl.Datetime("us", "UTC")
        assert len(caplog.records) == 1
        assert "my_dataset" in caplog.records[0].message
        assert "1" in caplog.records[0].message

    def test_missing_z_marker_nulls_with_warning_when_mixed(self, caplog: pytest.LogCaptureFixture):
        """A genuinely bare timestamp (no Z, no offset) mixed with a
        well-formed row must null and warn, not silently guess UTC."""
        with caplog.at_level(logging.WARNING):
            result = with_published_at(
                _df([_WELL_FORMED, "2024-01-14T12:00:00"]), dataset="my_dataset"
            )
        assert result["published_at"].to_list() == [_EXPECTED, None]
        assert len(caplog.records) == 1
        assert "my_dataset" in caplog.records[0].message

    def test_garbage_string_nulls_with_warning(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(_df(["not-a-timestamp"]), dataset="my_dataset")
        assert result["published_at"].to_list() == [None]
        assert len(caplog.records) == 1

    def test_warning_count_matches_failed_row_count(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(
                _df([_WELL_FORMED, "bad-1", "bad-2", "bad-3"]), dataset="my_dataset"
            )
        assert result["published_at"].null_count() == 3
        assert len(caplog.records) == 1
        assert "3" in caplog.records[0].message

    def test_elexon_column_parses_in_place(self, caplog: pytest.LogCaptureFixture):
        frame = pl.DataFrame({"published_at": [_WELL_FORMED, "bad"], "x": [1, 2]})
        with caplog.at_level(logging.WARNING):
            result = with_published_at(frame, "published_at", dataset="agpt")
        assert result.columns == ["published_at", "x"]
        assert result["published_at"].to_list() == [_EXPECTED, None]
        assert len(caplog.records) == 1
        assert "agpt" in caplog.records[0].message


class TestNoFalsePositiveWarnings:
    """Absent-column and empty-string cases are the pre-existing typed-null
    contract, not parse failures — must never warn."""

    def test_missing_source_column_no_warning(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(pl.DataFrame({"other": [1, 2]}), dataset="my_dataset")
        assert result["published_at"].null_count() == 2
        assert result["published_at"].dtype == pl.Datetime("us", "UTC")
        assert not caplog.records

    def test_empty_string_no_warning(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            result = with_published_at(_df(["", "   ", None]), dataset="my_dataset")
        assert result["published_at"].null_count() == 3
        assert not caplog.records
