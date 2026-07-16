from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from gridflow.silver.base import BaseSilverTransformer

if TYPE_CHECKING:
    from pathlib import Path


class _StubTransformer(BaseSilverTransformer):
    source = "test_source"
    dataset = "test_dataset"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame()


class _EntsoeStubTransformer(BaseSilverTransformer):
    """P0.8: pins the exact-partition-only policy for source == 'entsoe'."""

    source = "entsoe"
    dataset = "test_dataset"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame()


def _make_raw_file(partition_dir: Path) -> None:
    """Create a minimal raw file so glob("raw_*") matches."""
    partition_dir.mkdir(parents=True, exist_ok=True)
    (partition_dir / "raw_20260510T000000Z_abcd1234.json").write_text("{}")


class TestBronzePathForDate:
    def test_exact_partition_used_when_present(self, tmp_path: Path) -> None:
        t = _StubTransformer(tmp_path)
        exact = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "05" / "10"
        _make_raw_file(exact)
        result = t._bronze_path_for_date(date(2026, 5, 10))
        assert result == exact

    def test_fallback_to_prior_partition(self, tmp_path: Path) -> None:
        t = _StubTransformer(tmp_path)
        prior = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "05" / "10"
        _make_raw_file(prior)
        # target_date is 2026-05-11, exact partition is absent
        result = t._bronze_path_for_date(date(2026, 5, 11))
        assert result == prior

    def test_none_when_nothing_found(self, tmp_path: Path) -> None:
        t = _StubTransformer(tmp_path)
        result = t._bronze_path_for_date(date(2026, 5, 11))
        assert result is None

    def test_fallback_does_not_exceed_max_lookback(self, tmp_path: Path) -> None:
        t = _StubTransformer(tmp_path)
        # Create a file 40 days prior — beyond max_lookback_days=35
        far_past = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "04" / "01"
        _make_raw_file(far_past)
        result = t._bronze_path_for_date(date(2026, 5, 11), max_lookback_days=35)
        assert result is None

    def test_exact_preferred_over_fallback(self, tmp_path: Path) -> None:
        t = _StubTransformer(tmp_path)
        prior = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "05" / "10"
        exact = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "05" / "11"
        _make_raw_file(prior)
        _make_raw_file(exact)
        result = t._bronze_path_for_date(date(2026, 5, 11))
        assert result == exact


class TestBronzeDateDirsFallback:
    def test_fallback_included_when_exact_missing(self, tmp_path: Path) -> None:
        t = _StubTransformer(tmp_path)
        prior = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "05" / "10"
        _make_raw_file(prior)
        dirs = t._bronze_date_dirs(date(2026, 5, 11))
        assert prior in dirs

    def test_exact_returned_when_present(self, tmp_path: Path) -> None:
        t = _StubTransformer(tmp_path)
        exact = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "05" / "11"
        _make_raw_file(exact)
        dirs = t._bronze_date_dirs(date(2026, 5, 11))
        assert exact in dirs
        assert len(dirs) == 1


class TestEntsoeExactPartitionOnlyPolicy:
    """P0.8: source == 'entsoe' never uses the covering-partition fallback."""

    def test_exact_partition_used_when_present(self, tmp_path: Path) -> None:
        t = _EntsoeStubTransformer(tmp_path)
        exact = tmp_path / "bronze" / "entsoe" / "test_dataset" / "2026" / "05" / "10"
        _make_raw_file(exact)
        result = t._bronze_path_for_date(date(2026, 5, 10))
        assert result == exact

    def test_none_when_only_prior_partition_exists(self, tmp_path: Path) -> None:
        """Inverted `test_fallback_to_prior_partition`: entsoe gets None, not the fallback."""
        t = _EntsoeStubTransformer(tmp_path)
        prior = tmp_path / "bronze" / "entsoe" / "test_dataset" / "2026" / "05" / "10"
        _make_raw_file(prior)
        result = t._bronze_path_for_date(date(2026, 5, 11))
        assert result is None

    def test_bronze_date_dirs_empty_when_only_prior_partition_exists(self, tmp_path: Path) -> None:
        t = _EntsoeStubTransformer(tmp_path)
        prior = tmp_path / "bronze" / "entsoe" / "test_dataset" / "2026" / "05" / "10"
        _make_raw_file(prior)
        dirs = t._bronze_date_dirs(date(2026, 5, 11))
        assert dirs == []

    def test_non_entsoe_source_fallback_unaffected(self, tmp_path: Path) -> None:
        """Pins that test_source's fallback stays intact (decision 3 evidence)."""
        t = _StubTransformer(tmp_path)
        prior = tmp_path / "bronze" / "test_source" / "test_dataset" / "2026" / "05" / "10"
        _make_raw_file(prior)
        result = t._bronze_path_for_date(date(2026, 5, 11))
        assert result == prior
