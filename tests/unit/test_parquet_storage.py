"""Tests for Polars-backed Parquet read helpers."""

from __future__ import annotations

import logging
import os
import re
import stat
import subprocess
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from polars.exceptions import PolarsError
from polars.testing import assert_frame_equal

from gridflow.storage.parquet import (
    _is_link_or_junction,
    is_reserved_temp_path,
    read_parquet,
    read_parquet_dir,
    scan_parquet_dir,
    scan_parquet_range,
    sweep_orphan_temp_files,
    write_parquet,
)


def _create_directory_link(link: Path, target: Path, kind: str) -> None:
    if kind == "symlink":
        try:
            link.symlink_to(target, target_is_directory=True)
        except OSError as exc:
            pytest.skip(f"directory symlink unavailable: {exc}")
        return
    if os.name != "nt":
        pytest.skip("directory junctions are Windows-only")
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"directory junction unavailable: {result.stderr or result.stdout}")


def _remove_directory_link(link: Path, kind: str) -> None:
    if not link.exists() and not link.is_symlink():
        return
    if kind == "junction":
        os.rmdir(link)
    else:
        link.unlink()


def _create_file_symlink(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"file symlink unavailable: {exc}")


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


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (".tmp_file.parquet", True),
        (".tmp_file.csv", True),
        ("file.parquet.tmp_0123456789abcdef", True),
        (".ordinary.parquet", False),
        ("file.tmp_note.parquet", False),
        ("file.parquet.tmp_0123456789ABCDEF", False),
        ("file.parquet.tmp_01234567", False),
        ("file.parquet.tmp_0123456789abcdef0", False),
    ],
)
def test_reserved_temp_predicate(name: str, expected: bool) -> None:
    assert is_reserved_temp_path(name) is expected


def test_write_parquet_uses_unique_non_parquet_suffix_temps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    final = tmp_path / "result.parquet"
    seen: list[Path] = []
    real_write = pl.DataFrame.write_parquet

    def _spy(self: pl.DataFrame, path: Path, *args: object, **kwargs: object) -> None:
        seen.append(Path(path))
        real_write(self, path, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _spy)
    df = pl.DataFrame({"value": [1]})

    write_parquet(df, final)
    write_parquet(df, final)

    assert len(seen) == 2
    assert seen[0] != seen[1]
    for temp in seen:
        assert temp.parent == final.parent
        assert re.fullmatch(r"result\.parquet\.tmp_[0-9a-f]{16}", temp.name)
        assert len(temp.name) - len(final.name) == 21
        legacy = final.parent / f".tmp_{final.name}"
        assert len(temp.name) - len(legacy.name) == 16
        assert not temp.match("*.parquet")


@pytest.mark.parametrize("name", [".tmp_result.parquet", "result.parquet.tmp_0123456789abcdef"])
def test_write_parquet_rejects_reserved_final_before_parent_creation(
    tmp_path: Path, name: str
) -> None:
    final = tmp_path / "absent" / name
    with pytest.raises(ValueError, match="reserved temporary"):
        write_parquet(pl.DataFrame({"value": [1]}), final)
    assert not final.parent.exists()


def test_write_failure_removes_temp_and_preserves_original_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    final = tmp_path / "result.parquet"

    def _fail_replace(source: Path, destination: Path) -> None:
        raise PermissionError("replace denied")

    monkeypatch.setattr(os, "replace", _fail_replace)
    with pytest.raises(PermissionError, match="replace denied"):
        write_parquet(pl.DataFrame({"value": [1]}), final)

    assert list(tmp_path.iterdir()) == []


def test_partial_temp_write_failure_cleans_temp_and_preserves_original_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    final = tmp_path / "result.parquet"

    def _partial_then_fail(self: pl.DataFrame, path: Path, *args: object, **kwargs: object) -> None:
        Path(path).write_bytes(b"partial")
        raise RuntimeError("partial write failed")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _partial_then_fail)
    with pytest.raises(RuntimeError, match="partial write failed"):
        write_parquet(pl.DataFrame({"value": [1]}), final)

    assert list(tmp_path.iterdir()) == []


def test_cleanup_failure_warns_without_masking_writer_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    final = tmp_path / "result.parquet"
    real_unlink = Path.unlink

    def _fail_replace(source: Path, destination: Path) -> None:
        raise PermissionError("replace denied")

    def _fail_temp_unlink(path: Path, *args: object, **kwargs: object) -> None:
        if is_reserved_temp_path(path):
            raise PermissionError("cleanup denied")
        real_unlink(path, *args, **kwargs)

    with monkeypatch.context() as context:
        context.setattr(os, "replace", _fail_replace)
        context.setattr(Path, "unlink", _fail_temp_unlink)
        with (
            caplog.at_level(logging.WARNING),
            pytest.raises(PermissionError, match="replace denied"),
        ):
            write_parquet(pl.DataFrame({"value": [1]}), final)

    leftovers = list(tmp_path.iterdir())
    assert len(leftovers) == 1 and is_reserved_temp_path(leftovers[0])
    assert "cleanup denied" in caplog.text
    os.utime(leftovers[0], (0, 0))
    assert sweep_orphan_temp_files(tmp_path, tmp_path / "gold", now_epoch=100_000) == 1
    assert not leftovers[0].exists()


def test_read_surfaces_ignore_reserved_temps(tmp_path: Path) -> None:
    part = tmp_path / "ds" / "year=2024" / "month=01"
    canonical = part / "canonical.parquet"
    legacy = part / ".tmp_corrupt.parquet"
    suffix = part / "ignored.parquet.tmp_0123456789abcdef"
    _write_parquet(pl.DataFrame({"settlement_date": [date(2024, 1, 1)], "value": [1]}), canonical)
    legacy.write_bytes(b"corrupt")
    suffix.write_bytes(b"corrupt")

    assert read_parquet(str(part / "*.parquet"))["value"].to_list() == [1]
    assert read_parquet_dir(tmp_path / "ds")["value"].to_list() == [1]
    assert scan_parquet_range(tmp_path / "ds", date(2024, 1, 1), date(2024, 1, 31)).collect()[
        "value"
    ].to_list() == [1]
    with pytest.raises(ValueError, match="reserved temporary"):
        read_parquet(legacy)


def test_reserved_only_reads_are_empty(tmp_path: Path) -> None:
    part = tmp_path / "ds" / "year=2024" / "month=01"
    _write_parquet(pl.DataFrame({"value": [1]}), part / ".tmp_only.parquet")

    assert read_parquet(str(part / ".tmp_*.parquet")).is_empty()
    assert read_parquet_dir(tmp_path / "ds").is_empty()
    assert (
        scan_parquet_range(tmp_path / "ds", date(2024, 1, 1), date(2024, 1, 31))
        .collect()
        .is_empty()
    )


def test_sweep_removes_only_aged_reserved_files(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    bronze = tmp_path / "bronze"
    now = 200_000.0
    aged = [
        silver / "source" / "dataset" / ".tmp_old.parquet",
        gold / "dataset" / "old.parquet.tmp_0123456789abcdef",
        silver / "source" / "dataset" / ".tmp_old.csv",
    ]
    fresh = silver / "source" / "dataset" / ".tmp_fresh.parquet"
    future = silver / "source" / "dataset" / ".tmp_future.parquet"
    ordinary = gold / "dataset" / "canonical.parquet"
    bronze_temp = bronze / ".tmp_old.parquet"
    reserved_directory = silver / ".tmp_directory"
    for candidate in [*aged, fresh, future, ordinary, bronze_temp]:
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_bytes(b"x")
    reserved_directory.mkdir(parents=True)
    for candidate in aged:
        os.utime(candidate, (now - 100, now - 100))
    os.utime(fresh, (now - 99, now - 99))
    os.utime(future, (now + 100, now + 100))

    removed = sweep_orphan_temp_files(silver, gold, max_age_seconds=100, now_epoch=now)

    assert removed == 3
    assert all(not candidate.exists() for candidate in aged)
    assert fresh.exists() and future.exists() and ordinary.exists() and bronze_temp.exists()
    assert reserved_directory.is_dir()


def test_sweep_missing_roots_and_negative_age(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    assert sweep_orphan_temp_files(silver, gold, now_epoch=100) == 0
    assert not silver.exists() and not gold.exists()
    with pytest.raises(ValueError, match="non-negative"):
        sweep_orphan_temp_files(silver, gold, max_age_seconds=-1)


def test_link_or_junction_helper_recognizes_both_file_types() -> None:
    symlink_stat = SimpleNamespace(st_mode=stat.S_IFLNK, st_file_attributes=0)
    junction_stat = SimpleNamespace(st_mode=stat.S_IFDIR, st_file_attributes=0x400)
    ordinary_stat = SimpleNamespace(st_mode=stat.S_IFDIR, st_file_attributes=0)

    assert _is_link_or_junction(symlink_stat)  # type: ignore[arg-type]
    assert _is_link_or_junction(junction_stat)  # type: ignore[arg-type]
    assert not _is_link_or_junction(ordinary_stat)  # type: ignore[arg-type]


@pytest.mark.parametrize("root_name", ["silver", "gold"])
@pytest.mark.parametrize("link_kind", ["symlink", "junction"])
def test_sweep_rejects_linked_authoritative_root(
    tmp_path: Path,
    root_name: str,
    link_kind: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / ".tmp_external.parquet"
    sentinel.write_bytes(b"must survive")
    os.utime(sentinel, (0, 0))
    linked_root = tmp_path / root_name
    _create_directory_link(linked_root, external, link_kind)
    other_root = tmp_path / ("gold" if root_name == "silver" else "silver")
    roots = (linked_root, other_root) if root_name == "silver" else (other_root, linked_root)
    try:
        with caplog.at_level(logging.WARNING):
            assert sweep_orphan_temp_files(*roots, now_epoch=100_000) == 0
        assert sentinel.read_bytes() == b"must survive"
        assert "linked temporary-file sweep root" in caplog.text
    finally:
        _remove_directory_link(linked_root, link_kind)


@pytest.mark.parametrize("root_name", ["silver", "gold"])
@pytest.mark.parametrize("link_kind", ["symlink", "junction"])
def test_sweep_does_not_follow_nested_link_escape(
    tmp_path: Path,
    root_name: str,
    link_kind: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    selected_root = silver if root_name == "silver" else gold
    selected_root.mkdir(parents=True)
    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / ".tmp_external.parquet"
    sentinel.write_bytes(b"must survive")
    os.utime(sentinel, (0, 0))
    escape = selected_root / "escape"
    _create_directory_link(escape, external, link_kind)
    try:
        with caplog.at_level(logging.WARNING):
            assert sweep_orphan_temp_files(silver, gold, now_epoch=100_000) == 0
        assert sentinel.read_bytes() == b"must survive"
        assert "Skipping linked directory" in caplog.text
    finally:
        _remove_directory_link(escape, link_kind)


@pytest.mark.parametrize("root_name", ["silver", "gold"])
@pytest.mark.parametrize("target_scope", ["external", "in_root"])
def test_sweep_skips_reserved_named_file_symlink(
    tmp_path: Path,
    root_name: str,
    target_scope: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    selected_root = silver if root_name == "silver" else gold
    selected_root.mkdir(parents=True)
    target_parent = tmp_path / "external" if target_scope == "external" else selected_root
    target_parent.mkdir(parents=True, exist_ok=True)
    target = target_parent / "canonical.parquet"
    target.write_bytes(b"must survive")
    os.utime(target, (0, 0))
    link = selected_root / ".tmp_link.parquet"
    _create_file_symlink(link, target)

    with caplog.at_level(logging.WARNING):
        assert sweep_orphan_temp_files(silver, gold, now_epoch=100_000) == 0

    assert target.read_bytes() == b"must survive"
    assert link.is_symlink() and link.exists()
    expected_warning = (
        "outside root" if target_scope == "external" else "linked temporary-file candidate"
    )
    assert expected_warning in caplog.text


def test_sweep_races_and_unlink_error_continue_with_exact_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    candidates = {
        name: silver / "source" / "dataset" / f".tmp_{name}.parquet"
        for name in ("vanish", "refresh", "denied", "removed")
    }
    for candidate in candidates.values():
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_bytes(b"x")
        os.utime(candidate, (0, 0))

    real_stat = os.stat
    real_unlink = Path.unlink

    def _race_stat(path: Path, *, follow_symlinks: bool = True):  # type: ignore[no-untyped-def]
        candidate = Path(path)
        if not follow_symlinks and candidate == candidates["vanish"]:
            real_unlink(candidate)
            raise FileNotFoundError(candidate)
        if not follow_symlinks and candidate == candidates["refresh"]:
            os.utime(candidate, (100_000, 100_000))
        return real_stat(candidate, follow_symlinks=follow_symlinks)

    def _selective_unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path == candidates["denied"]:
            raise PermissionError("unlink denied")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", _race_stat)
    monkeypatch.setattr(Path, "unlink", _selective_unlink)
    with caplog.at_level(logging.WARNING):
        removed = sweep_orphan_temp_files(silver, gold, max_age_seconds=100, now_epoch=100_000)

    assert removed == 1
    assert not candidates["vanish"].exists()
    assert candidates["refresh"].exists()
    assert candidates["denied"].exists()
    assert not candidates["removed"].exists()
    assert "unlink denied" in caplog.text


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
