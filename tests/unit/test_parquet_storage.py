"""Tests for Polars-backed Parquet read helpers."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl
import pytest
from polars.exceptions import PolarsError
from polars.testing import assert_frame_equal

from gridflow.storage.parquet import (
    read_parquet,
    read_parquet_dir,
    scan_parquet_dir,
    scan_parquet_range,
)

if TYPE_CHECKING:
    from pathlib import Path


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _write_month(
    dataset_dir: Path, year: int, month: int, rows: list[date], extra: dict | None = None
) -> None:
    """Write a Hive ``year=/month=`` partition file with one row per date.

    ``extra`` adds an extra column (for the mixed-schema null-fill test).
    """
    data: dict = {"settlement_date": rows, "value": list(range(len(rows)))}
    if extra:
        data.update(extra)
    part = dataset_dir / f"year={year}" / f"month={month:02d}"
    _write_parquet(pl.DataFrame(data), part / f"system_prices_{year}{month:02d}.parquet")


def test_read_parquet_dir_tolerates_mixed_pre_and_post_f0_schemas(
    tmp_path: Path,
) -> None:
    """A narrow pre-F0 file beside a wide post-F0 file in one tree null-fills.

    Load-bearing: the NARROW file MUST sort lexically before the WIDE one
    (``a_narrow`` < ``b_wide``). Polars scans glob matches in sorted order and
    resolves the schema from the first match, so only narrow-then-wide order
    triggers the extra-column raise on unfixed code. Reversing the names lets
    ``missing_columns='insert'`` absorb the drift and the test passes on the bug
    (proving nothing) — mirroring ``scan_parquet_range``'s within-month test.
    """
    dataset_dir = tmp_path / "silver" / "elexon" / "mixed" / "year=2024" / "month=01"
    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "a_narrow.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "available_at": ["2024-01-16T00:00:00Z"]}),
        dataset_dir / "b_wide.parquet",
    )

    df = read_parquet_dir(tmp_path / "silver" / "elexon" / "mixed").sort("value")

    assert df["value"].to_list() == [1, 2]
    assert df["available_at"].to_list() == [None, "2024-01-16T00:00:00Z"]


def test_read_parquet_glob_tolerates_mixed_schemas(tmp_path: Path) -> None:
    """read_parquet(glob) null-fills within-glob drift (narrow-then-wide order).

    Same load-bearing ordering as the tree test above: ``a_narrow`` (no extra
    column) MUST sort before ``b_wide`` so the unfixed single-glob read resolves
    the schema from the narrow file and raises on the wide file's extra column.
    """
    _write_parquet(pl.DataFrame({"value": [1]}), tmp_path / "a_narrow.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "source_run_id": ["run-xyz"]}),
        tmp_path / "b_wide.parquet",
    )

    df = read_parquet(str(tmp_path / "*.parquet")).sort("value")

    assert df["value"].to_list() == [1, 2]
    assert df["source_run_id"].to_list() == [None, "run-xyz"]


def test_read_parquet_dir_returns_empty_when_no_files(tmp_path: Path) -> None:
    df = read_parquet_dir(tmp_path / "does_not_exist")
    assert df.is_empty()

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    df = read_parquet_dir(empty_dir)
    assert df.is_empty()


def test_read_parquet_dir_propagates_schema_errors(tmp_path: Path) -> None:
    """Genuine schema corruption must surface, not be swallowed as 'no files'."""
    dataset_dir = tmp_path / "broken"
    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "a.parquet")
    (dataset_dir / "b.parquet").write_bytes(b"not a parquet file")

    with pytest.raises(PolarsError):
        read_parquet_dir(dataset_dir)


# --- scan_parquet_dir / scan_parquet_range (CH3-01) ---


def test_scan_parquet_dir_returns_lazyframe_equivalent_to_read(tmp_path: Path) -> None:
    """scan_parquet_dir().collect() must equal read_parquet_dir()."""
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 1, [date(2024, 1, 5), date(2024, 1, 6)])

    lf = scan_parquet_dir(dataset_dir)
    assert isinstance(lf, pl.LazyFrame)
    assert_frame_equal(
        lf.collect().sort("settlement_date"),
        read_parquet_dir(dataset_dir).sort("settlement_date"),
        check_column_order=False,
    )


def test_scan_parquet_dir_skips_transient_tmp_writes(tmp_path: Path) -> None:
    """A ``.tmp_*.parquet`` sibling (in-flight/torn write) is never scanned.

    ``write_parquet`` writes to a ``.tmp_<name>`` sibling then ``os.replace``s it
    into place, so a ``.tmp_`` parquet is always an in-flight or torn write that
    a concurrent read must not pick up. Glob-based ``read_parquet`` already skips
    dotfiles; ``scan_parquet_dir`` must agree (the file is a valid parquet here so
    the assertion isolates the skip, not a parse error).
    """
    part = tmp_path / "ds" / "year=2024" / "month=02"
    _write_parquet(
        pl.DataFrame({"settlement_date": [date(2024, 2, 1)], "value": [1]}), part / "real.parquet"
    )
    _write_parquet(
        pl.DataFrame({"settlement_date": [date(2024, 2, 2)], "value": [99]}),
        part / ".tmp_real.parquet",
    )

    df = read_parquet_dir(tmp_path / "ds")
    assert df["value"].to_list() == [1]


def test_scan_parquet_dir_empty_and_absent(tmp_path: Path) -> None:
    """Empty/absent dir -> empty LazyFrame whose collect is empty."""
    absent = scan_parquet_dir(tmp_path / "nope")
    assert isinstance(absent, pl.LazyFrame)
    assert absent.collect().is_empty()

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    assert scan_parquet_dir(empty_dir).collect().is_empty()


def test_scan_parquet_range_prunes_to_overlapping_month_glob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The globs handed to pl.scan_parquet must include only month=02.

    Primary pruning assertion: spy on pl.scan_parquet, capture each glob source
    it is called with, and assert the set includes a ``month=02`` entry and NO
    ``month=01``/``month=03`` entry. Non-overlapping months' files are never
    opened.
    """
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 1, [date(2024, 1, 15)])
    _write_month(dataset_dir, 2024, 2, [date(2024, 2, 14)])
    _write_month(dataset_dir, 2024, 3, [date(2024, 3, 10)])

    captured: list[str] = []
    real_scan = pl.scan_parquet

    def _spy(source, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured.append(str(source))
        return real_scan(source, *args, **kwargs)

    monkeypatch.setattr(pl, "scan_parquet", _spy)

    lf = scan_parquet_range(dataset_dir, date(2024, 2, 1), date(2024, 2, 28))
    lf.collect()

    assert captured, "pl.scan_parquet was not called"
    assert any("month=02" in g for g in captured)
    assert not any("month=01" in g for g in captured)
    assert not any("month=03" in g for g in captured)


def test_scan_parquet_range_equivalent_to_eager_then_filter(tmp_path: Path) -> None:
    """Lazy-pruned collect == eager read_parquet_dir().filter(range).

    Schema is held constant across all three months so the comparison varies
    only dates (pruning), never columns.
    """
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 1, [date(2024, 1, 31)])
    _write_month(dataset_dir, 2024, 2, [date(2024, 2, 1), date(2024, 2, 28)])
    _write_month(dataset_dir, 2024, 3, [date(2024, 3, 1)])

    start, end = date(2024, 2, 1), date(2024, 2, 28)
    lazy = scan_parquet_range(dataset_dir, start, end).collect()
    eager = read_parquet_dir(dataset_dir).filter(
        (pl.col("settlement_date") >= start) & (pl.col("settlement_date") <= end)
    )

    assert_frame_equal(
        lazy.sort("settlement_date"),
        eager.sort("settlement_date"),
        check_column_order=False,
    )
    assert lazy["settlement_date"].to_list() == [date(2024, 2, 1), date(2024, 2, 28)]


def test_scan_parquet_range_boundary_predicate_trims_partial_month(tmp_path: Path) -> None:
    """Residual predicate trims boundary days within an overlapping month."""
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 2, [date(2024, 2, 1), date(2024, 2, 14), date(2024, 2, 28)])

    df = scan_parquet_range(dataset_dir, date(2024, 2, 10), date(2024, 2, 20)).collect()
    assert df["settlement_date"].to_list() == [date(2024, 2, 14)]


def test_scan_parquet_range_none_bounds_delegate_to_dir(tmp_path: Path) -> None:
    """Both bounds None -> whole-tree scan (delegates to scan_parquet_dir)."""
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 1, [date(2024, 1, 5)])
    _write_month(dataset_dir, 2024, 2, [date(2024, 2, 5)])

    df = scan_parquet_range(dataset_dir, None, None).collect()
    assert sorted(df["settlement_date"].to_list()) == [date(2024, 1, 5), date(2024, 2, 5)]


def test_scan_parquet_range_no_overlapping_partitions_is_empty(tmp_path: Path) -> None:
    """A range with no on-disk overlapping partitions -> empty (no scan error)."""
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 2, [date(2024, 2, 14)])

    df = scan_parquet_range(dataset_dir, date(2025, 6, 1), date(2025, 6, 30)).collect()
    assert df.is_empty()


def test_scan_parquet_range_skips_empty_in_range_partition_dir(tmp_path: Path) -> None:
    """An empty-but-existing in-range partition dir is skipped, not fatal.

    An interrupted write can leave ``year=/month=`` with no parquet file; the
    whole-tree glob skips it silently, so the range path must too (not raise on
    a zero-match glob).
    """
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 1, [date(2024, 1, 10)])
    _write_month(dataset_dir, 2024, 3, [date(2024, 3, 10)])
    (dataset_dir / "year=2024" / "month=02").mkdir(parents=True, exist_ok=True)

    df = scan_parquet_range(dataset_dir, date(2024, 1, 1), date(2024, 3, 31)).collect()
    assert sorted(df["settlement_date"].to_list()) == [date(2024, 1, 10), date(2024, 3, 10)]


def test_scan_parquet_range_mixed_schema_null_fills_in_range(tmp_path: Path) -> None:
    """missing_columns='insert' tolerance holds under lazy scan, in-range.

    Both differing files are inside the scanned range so schema drift never
    crosses month-pruning.
    """
    dataset_dir = tmp_path / "ds"
    _write_month(dataset_dir, 2024, 1, [date(2024, 1, 10)])
    _write_month(
        dataset_dir, 2024, 2, [date(2024, 2, 10)], extra={"available_at": ["2024-02-10T00:00:00Z"]}
    )

    df = scan_parquet_range(dataset_dir, date(2024, 1, 1), date(2024, 2, 28)).collect()
    df = df.sort("settlement_date")
    assert df["settlement_date"].to_list() == [date(2024, 1, 10), date(2024, 2, 10)]
    assert df["available_at"].to_list() == [None, "2024-02-10T00:00:00Z"]


def test_scan_parquet_range_within_month_schema_drift_null_fills(tmp_path: Path) -> None:
    """Two drifting files in the SAME month null-fill the union, not SchemaError.

    A partial re-transform can leave a narrow pre-F0 file beside a wide post-F0
    file in one ``year=/month=`` partition. A single per-partition glob scan
    resolves the schema from the first file and raises ``SchemaError`` on the
    later file's extra column. The fix unions per-file via diagonal concat.

    Load-bearing: the narrow file MUST sort lexically *before* the wide one
    (``a_narrow`` < ``b_wide``). Polars scans glob matches in sorted order and
    resolves the schema from the first match, so only a narrow-then-wide order
    triggers the extra-column raise. Reversing the names would let
    ``missing_columns='insert'`` absorb it and the test would pass on unfixed
    code — proving nothing.
    """
    dataset_dir = tmp_path / "ds"
    part = dataset_dir / "year=2024" / "month=02"
    _write_parquet(
        pl.DataFrame({"settlement_date": [date(2024, 2, 1)], "value": [1]}),
        part / "a_narrow.parquet",
    )
    _write_parquet(
        pl.DataFrame(
            {
                "settlement_date": [date(2024, 2, 2)],
                "value": [2],
                "available_at": ["2024-02-02T00:00:00Z"],
            }
        ),
        part / "b_wide.parquet",
    )

    df = scan_parquet_range(dataset_dir, date(2024, 2, 1), date(2024, 2, 28)).collect()
    df = df.sort("settlement_date")
    assert df["settlement_date"].to_list() == [date(2024, 2, 1), date(2024, 2, 2)]
    assert df["available_at"].to_list() == [None, "2024-02-02T00:00:00Z"]


def test_scan_parquet_range_propagates_schema_errors_on_collect(tmp_path: Path) -> None:
    """Schema corruption inside an in-range partition surfaces on collect."""
    dataset_dir = tmp_path / "ds"
    part = dataset_dir / "year=2024" / "month=02"
    _write_parquet(
        pl.DataFrame({"settlement_date": [date(2024, 2, 1)], "value": [1]}),
        part / "good.parquet",
    )
    (part / "bad.parquet").write_bytes(b"not a parquet file")

    with pytest.raises(PolarsError):
        scan_parquet_range(dataset_dir, date(2024, 2, 1), date(2024, 2, 28)).collect()
