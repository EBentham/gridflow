"""Tests for the standalone published-at normalization script."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from types import ModuleType


def _load_script() -> ModuleType:
    script = Path(__file__).resolve().parents[2] / "scripts" / "normalize_published_at.py"
    spec = importlib.util.spec_from_file_location("normalize_published_at", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_partition_files_excludes_both_reserved_temp_grammars(tmp_path: Path) -> None:
    module = _load_script()
    partition = tmp_path / "silver" / "elexon" / "indo" / "year=2024" / "month=01"
    partition.mkdir(parents=True)
    canonical = partition / "indo.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(canonical)
    (partition / ".tmp_indo.parquet").write_bytes(b"corrupt")
    (partition / "indo.parquet.tmp_0123456789abcdef").write_bytes(b"corrupt")

    assert module._partition_files(tmp_path, "indo") == [canonical]
