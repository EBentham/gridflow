"""T-03 (D-19): the repo's first CSV bronze-body reader.

Eight behaviours, one per bullet of the plan's ``T-03`` ``<behavior>`` block.
The reader has two callers — the connector's D-36 admission check at fetch
time and each transformer's ``read_bronze_file`` at transform time — so the
error messages carry a caller-supplied ``source_label`` (a URL at fetch time,
a path at transform time) rather than assuming either.
"""

from __future__ import annotations

import polars as pl
import pytest

from gridflow.silver.csv_bronze import (
    CsvHeaderDriftError,
    NotCsvBodyError,
    read_csv_bronze_body,
)

EXPECTED = ("BMU_ID", "Date", "MW")

CLEAN_LF = b"BMU_ID,Date,MW\nT_ABC-1,2026-08-16,120.5\nT_ABC-2,2026-08-16,98\n"
CLEAN_CRLF = b"BMU_ID,Date,MW\r\nT_ABC-1,2026-08-16,120.5\r\nT_ABC-2,2026-08-16,98\r\n"
BOM = b"\xef\xbb\xbf"


class TestHappyPath:
    """A clean body parses to an all-``Utf8`` frame (D-19: ``infer_schema_length=0``)."""

    def test_clean_utf8_csv_parses_to_all_utf8_frame(self) -> None:
        df = read_csv_bronze_body(CLEAN_LF, expected_columns=EXPECTED, source_label="unit://clean")

        assert df.columns == list(EXPECTED)
        assert df.height == 2
        assert set(df.schema.values()) == {pl.Utf8}, (
            "D-19: every column is read as Utf8 so the transformer casts explicitly "
            f"with strict=True; got {df.schema}"
        )
        assert df.row(0) == ("T_ABC-1", "2026-08-16", "120.5")

    def test_utf8_bom_is_stripped_and_does_not_contaminate_the_first_column_name(
        self,
    ) -> None:
        """D-19: the BOM is a per-resource property, stripped explicitly per file.

        Left in place it becomes a U+FEFF prefix on the first column name, which
        would fail the header contract for a body that is in fact correct.
        """
        plain = read_csv_bronze_body(
            CLEAN_LF, expected_columns=EXPECTED, source_label="unit://plain"
        )
        with_bom = read_csv_bronze_body(
            BOM + CLEAN_LF, expected_columns=EXPECTED, source_label="unit://bom"
        )

        assert with_bom.columns[0] == plain.columns[0] == "BMU_ID"
        assert "﻿" not in with_bom.columns[0]
        assert with_bom.equals(plain)

    def test_crlf_line_endings_parse_identically_to_lf(self) -> None:
        lf = read_csv_bronze_body(CLEAN_LF, expected_columns=EXPECTED, source_label="unit://lf")
        crlf = read_csv_bronze_body(
            CLEAN_CRLF, expected_columns=EXPECTED, source_label="unit://crlf"
        )

        assert crlf.columns == lf.columns
        assert crlf.equals(lf)

    def test_header_only_body_returns_an_empty_frame(self) -> None:
        """D-14's connector guard makes this unreachable in practice; the reader
        must not crash on it."""
        df = read_csv_bronze_body(
            b"BMU_ID,Date,MW\n", expected_columns=EXPECTED, source_label="unit://header-only"
        )

        assert df.height == 0
        assert df.columns == list(EXPECTED)


class TestNonCsvGuard:
    """A body that is not CSV must fail loudly, never become a garbage frame."""

    def test_body_starting_with_an_angle_bracket_raises_naming_the_source(self) -> None:
        body = b"<!DOCTYPE html>\n<html><body>503 Service Unavailable</body></html>"

        with pytest.raises(NotCsvBodyError) as excinfo:
            read_csv_bronze_body(
                body, expected_columns=EXPECTED, source_label="https://example.invalid/x.csv"
            )

        assert "https://example.invalid/x.csv" in str(excinfo.value)

    def test_leading_whitespace_before_the_angle_bracket_is_still_rejected(self) -> None:
        """The guard is on the first NON-WHITESPACE byte."""
        with pytest.raises(NotCsvBodyError):
            read_csv_bronze_body(
                b"\r\n  \n<html/>", expected_columns=EXPECTED, source_label="unit://ws-html"
            )

    def test_invalid_utf8_raises_rather_than_producing_replacement_characters(self) -> None:
        """A latin-1 byte in a body we decode strictly: raise, never silently
        substitute U+FFFD and hand a corrupted frame to bronze."""
        body = b"BMU_ID,Date,MW\nT_\xff\xfe-1,2026-08-16,120.5\n"

        with pytest.raises(NotCsvBodyError) as excinfo:
            read_csv_bronze_body(body, expected_columns=EXPECTED, source_label="unit://bad-utf8")

        assert "unit://bad-utf8" in str(excinfo.value)


class TestHeaderContract:
    """Vendor schema drift fails loud; it is never absorbed by a rename map."""

    def test_renamed_column_lists_missing_and_unexpected_names(self) -> None:
        body = b"BMU_ID,Date,MW_CAPACITY\nT_ABC-1,2026-08-16,120.5\n"

        with pytest.raises(CsvHeaderDriftError) as excinfo:
            read_csv_bronze_body(body, expected_columns=EXPECTED, source_label="unit://renamed")

        message = str(excinfo.value)
        assert "MW" in message
        assert "MW_CAPACITY" in message
        assert "unit://renamed" in message

    def test_extra_column_raises(self) -> None:
        body = b"BMU_ID,Date,MW,EXTRA\nT_ABC-1,2026-08-16,120.5,x\n"

        with pytest.raises(CsvHeaderDriftError) as excinfo:
            read_csv_bronze_body(body, expected_columns=EXPECTED, source_label="unit://extra")

        assert "EXTRA" in str(excinfo.value)

    def test_reordered_columns_raise(self) -> None:
        """The contract is exact AND in order — a reorder silently re-maps every
        column if it is tolerated."""
        body = b"Date,BMU_ID,MW\n2026-08-16,T_ABC-1,120.5\n"

        with pytest.raises(CsvHeaderDriftError):
            read_csv_bronze_body(body, expected_columns=EXPECTED, source_label="unit://reordered")
