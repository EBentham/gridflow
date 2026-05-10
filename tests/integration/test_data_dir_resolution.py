"""Integration test: data_dir resolves to project root, not CWD (WBH-04, ADR-001).

The `_project_root()` + `@model_validator` port from gridflow_models
(D-WORKTREE-PATH, commit 3e9b7e8) makes relative `data_dir`, `duckdb_path`,
and `log_dir` resolve against the project root rather than the current
working directory. This eliminates the "writes data to notebooks/data/"
silent-bug class hit on 2026-05-10.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gridflow.config import settings as settings_module
from gridflow.config.settings import PipelineSettings, _project_root


@pytest.fixture(autouse=True)
def _clear_project_root_cache() -> None:
    # WHY: _project_root() is @cache'd; clear before every test so prior
    # state never leaks across test boundaries.
    _project_root.cache_clear()


def test_project_root_returns_parent_of_pyproject() -> None:
    """Sanity check: _project_root() points at a directory containing pyproject.toml."""
    root = _project_root()
    assert (root / "pyproject.toml").exists(), f"_project_root={root}"


def test_relative_data_dir_resolves_to_project_root_regardless_of_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Constructing PipelineSettings with relative paths from a foreign CWD
    must produce absolute paths anchored at the gridflow project root, not
    the (foreign) CWD."""
    nested = tmp_path / "subdir" / "deeper"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    # Direct construction with the YAML-style relative defaults.
    pipeline = PipelineSettings(
        data_dir=Path("./data"),
        log_dir=Path("./logs"),
        duckdb_path=Path("./data/gridflow.duckdb"),
    )

    assert pipeline.data_dir.is_absolute()
    assert pipeline.duckdb_path.is_absolute()
    assert pipeline.log_dir.is_absolute()
    # The resolved paths must NOT contain the foreign CWD components —
    # that would mean the bug is unfixed.
    assert "subdir" not in str(pipeline.data_dir)
    assert "deeper" not in str(pipeline.data_dir)
    # And they MUST sit under a directory containing pyproject.toml
    # (the project root).
    expected_root = _project_root()
    assert pipeline.data_dir == (expected_root / "data").resolve()
    assert pipeline.log_dir == (expected_root / "logs").resolve()
    assert pipeline.duckdb_path == (expected_root / "data" / "gridflow.duckdb").resolve()


def test_absolute_data_dir_passes_through_unchanged(tmp_path: Path) -> None:
    """An absolute path supplied at construction must round-trip unchanged
    — the validator only rewrites relative paths."""
    abs_data = tmp_path / "absolute_data_dir"
    abs_data.mkdir()
    abs_logs = tmp_path / "absolute_logs"
    abs_logs.mkdir()
    abs_duck = tmp_path / "absolute_data_dir" / "x.duckdb"

    pipeline = PipelineSettings(
        data_dir=abs_data,
        log_dir=abs_logs,
        duckdb_path=abs_duck,
    )

    assert pipeline.data_dir == abs_data
    assert pipeline.log_dir == abs_logs
    assert pipeline.duckdb_path == abs_duck


def test_load_settings_resolves_yaml_paths_to_project_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end load_settings(): YAML's relative `./data` must resolve
    to <project_root>/data even when invoked from a foreign CWD.

    Uses a clean tmp config dir to avoid coupling to the gridflow repo's
    own config/settings.yaml — we want to verify the validator runs, not
    that any particular value is read.
    """
    # Populate a minimal config tree at tmp_path/config/.
    cfg_root = tmp_path / "config"
    cfg_root.mkdir()
    (cfg_root / "settings.yaml").write_text(
        "pipeline:\n"
        "  data_dir: ./data\n"
        "  log_dir: ./logs\n"
        "  duckdb_path: ./data/gridflow.duckdb\n"
    )
    (cfg_root / "sources.yaml").write_text("sources: {}\n")

    monkeypatch.setattr(settings_module, "_find_config_dir", lambda: cfg_root)

    # CWD is foreign — settings.yaml's `./data` must NOT anchor here.
    monkeypatch.chdir(tmp_path)

    config = settings_module.load_settings()
    assert config.pipeline.data_dir.is_absolute()
    expected_root = _project_root()
    assert config.pipeline.data_dir == (expected_root / "data").resolve()
