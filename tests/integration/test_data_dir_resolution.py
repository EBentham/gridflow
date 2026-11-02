"""Integration test: where gridflow puts its data.

One variable, ``GRIDFLOW_DATA_DIR``, picks the data root, defaulting to
``~/gridflow-data`` on every OS so a checkout, an installed package, a notebook
and a downstream repo all find the same data. The catalogue and the logs sit
under it unless set on their own. Relative values resolve against the current
folder, as any command-line tool's would.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.config import settings as settings_module
from gridflow.config.settings import PipelineSettings, _project_root
from gridflow.serving.client import GridflowClient


@pytest.fixture(autouse=True)
def _clear_project_root_cache() -> None:
    # WHY: _project_root() is @cache'd; clear before every test so prior
    # state never leaks across test boundaries.
    _project_root.cache_clear()


def _fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``Path.home()`` at a tmp folder: it reads USERPROFILE on Windows, HOME elsewhere."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    return home


def test_project_root_returns_parent_of_pyproject() -> None:
    """Sanity check: _project_root() points at a directory containing pyproject.toml."""
    root = _project_root()
    assert (root / "pyproject.toml").exists(), f"_project_root={root}"


def test_default_data_root_is_home_gridflow_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With nothing set, data, catalogue and logs go under ``~/gridflow-data``, from any folder."""
    home = _fake_home(tmp_path, monkeypatch)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    pipeline = settings_module.load_settings().pipeline

    assert pipeline.data_dir == home / "gridflow-data"
    assert pipeline.duckdb_path == home / "gridflow-data" / "gridflow.duckdb"
    assert pipeline.log_dir == home / "gridflow-data" / "logs"


def test_duckdb_path_follows_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Setting only ``GRIDFLOW_DATA_DIR`` moves the catalogue and the logs with it."""
    root = tmp_path / "root"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(root))

    pipeline = settings_module.load_settings().pipeline

    assert pipeline.data_dir == root
    assert pipeline.duckdb_path == root / "gridflow.duckdb"
    assert pipeline.log_dir == root / "logs"


def test_relative_data_dir_resolves_against_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A relative value means the current folder, never the checkout gridflow came from."""
    nested = tmp_path / "subdir" / "deeper"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    pipeline = PipelineSettings(data_dir=Path("data"), log_dir=Path("./logs"))

    assert pipeline.data_dir == (nested / "data").resolve()
    assert pipeline.duckdb_path == (nested / "data" / "gridflow.duckdb").resolve()
    assert pipeline.log_dir == (nested / "logs").resolve()


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


@pytest.mark.parametrize("field", ["data_dir", "duckdb_path", "log_dir"])
def test_drive_relative_path_is_refused(field: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """`C:x` has a drive but no root, so it is relative and would be joined
    to the current folder with no error. An unquoted `C:\\gridflow-data\\x` set
    in a POSIX shell arrives in exactly this form."""
    monkeypatch.setenv(f"GRIDFLOW_{field.upper()}", "C:gridflow-x.duckdb")

    with pytest.raises(ValueError, match="drive-relative.*e.g. D:/gridflow-data"):
        PipelineSettings()


def test_env_beats_yaml_beats_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``GRIDFLOW_DATA_DIR`` wins over ``config/settings.yaml`` in the current folder,
    which wins over ``~/gridflow-data``."""
    home = _fake_home(tmp_path, monkeypatch)
    yaml_root = tmp_path / "yaml-root"
    env_root = tmp_path / "env-root"
    with_yaml = tmp_path / "with-yaml"
    (with_yaml / "config").mkdir(parents=True)
    (with_yaml / "config" / "settings.yaml").write_text(
        f"pipeline:\n  data_dir: {yaml_root.as_posix()}\n", encoding="utf-8"
    )
    without_yaml = tmp_path / "without-yaml"
    without_yaml.mkdir()

    monkeypatch.chdir(without_yaml)
    assert settings_module.load_settings().pipeline.data_dir == home / "gridflow-data"

    monkeypatch.chdir(with_yaml)
    yaml_pipeline = settings_module.load_settings().pipeline
    assert yaml_pipeline.data_dir == yaml_root
    assert yaml_pipeline.duckdb_path == yaml_root / "gridflow.duckdb"

    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(env_root))
    env_pipeline = settings_module.load_settings().pipeline
    assert env_pipeline.data_dir == env_root
    assert env_pipeline.duckdb_path == env_root / "gridflow.duckdb"


def test_missing_root_message_names_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing catalogue names the path tried and the variable that moves it."""
    catalogue = tmp_path / "no-such-root" / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(catalogue.parent))

    with pytest.raises(FileNotFoundError) as excinfo:
        GridflowClient()
    assert str(catalogue) in str(excinfo.value)
    assert "GRIDFLOW_DATA_DIR" in str(excinfo.value)

    result = CliRunner().invoke(app, ["status"])
    assert result.exit_code == 1
    assert str(catalogue) in result.output
    assert "GRIDFLOW_DATA_DIR" in result.output
    assert not catalogue.parent.exists()
