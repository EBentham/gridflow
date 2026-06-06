"""CH1-03 (CH-SEC-03): reset containment guard + --dry-run.

The `reset` command recursively deletes the data dir and DuckDB path. Without a
containment guard it would happily wipe the repo, the user home dir, or the
filesystem root if `data_dir` resolved there. These tests assert:

  - an out-of-bounds target (data_dir == a fake home dir) is REFUSED with a
    non-zero exit and nothing is deleted;
  - `--dry-run` lists targets but deletes nothing (exit 0).

RED before CH1-03: there is no guard and no `--dry-run` flag, so the refusal
test sees `exit_code == 0` and the dry-run flag is rejected by Typer.

Safety: the refusal test points `Path.home()` and `GRIDFLOW_DATA_DIR` at the
same pytest tmp dir, so even the *unguarded* (RED) run only rglobs an empty
`tmp_home/bronze` — it can never touch the real home dir or repo.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from gridflow.cli import _is_dangerous_delete_target, app

runner = CliRunner()


def test_predicate_refuses_catastrophic_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pure containment predicate refuses root / home / repo-ancestor.

    This is the security crux of CH-SEC-03 — covering the
    ``project_root.is_relative_to(target)`` (ancestor) branch directly so a
    future flip to the wrong direction can never pass silently.
    """
    repo = tmp_path / "repo" / "gridflow"
    repo.mkdir(parents=True)
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: fake_home))

    fs_root = Path(repo.resolve().anchor) if repo.resolve().anchor else Path("/")

    # REFUSE: filesystem root, home, repo's parent (ancestor), and the repo itself.
    assert _is_dangerous_delete_target(fs_root, repo) is True
    assert _is_dangerous_delete_target(fake_home, repo) is True
    assert _is_dangerous_delete_target(repo.parent, repo) is True
    assert _is_dangerous_delete_target(repo, repo) is True


def test_predicate_allows_repo_data_and_tmp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The predicate allows a normal repo/data dir and an unrelated tmp dir."""
    repo = tmp_path / "repo" / "gridflow"
    repo.mkdir(parents=True)
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: fake_home))

    # ALLOW: data dir inside the repo, and an unrelated tmp location.
    assert _is_dangerous_delete_target(repo / "data", repo) is False
    assert _is_dangerous_delete_target(tmp_path / "elsewhere" / "data", repo) is False


def _isolated_env(data_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point gridflow at a tmp data_dir / duckdb / log dir for the duration."""
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(data_dir / "gridflow.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    # init_catalogue references gold SQL views absent from a tmp data_dir.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)


@pytest.mark.integration
def test_reset_refuses_out_of_bounds_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """reset --yes must refuse a data_dir that resolves to the (fake) home dir.

    The guard refuses targets that are the filesystem root, the home dir, or an
    ancestor of the repo. Here we monkeypatch `Path.home()` to a tmp dir and set
    `data_dir` to that same dir, so the target == home → must be refused.

    RED today: no guard, exit_code == 0, sentinel deleted (well, absent because
    nothing is written — so we assert exit code + that the dir is untouched).
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    # data_dir IS the fake home dir → deleting it would wipe "home".
    _isolated_env(fake_home, tmp_path, monkeypatch)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: fake_home))

    sentinel = fake_home / "bronze" / "keepme.txt"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("precious")

    result = runner.invoke(app, ["reset", "--yes"])

    assert result.exit_code != 0, result.output
    assert sentinel.exists(), "guard must not delete an out-of-bounds target"


@pytest.mark.integration
def test_reset_dry_run_deletes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """reset --dry-run lists targets but removes no files (exit 0)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    sentinel = data_dir / "bronze" / "elexon" / "raw.json"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("{}")

    result = runner.invoke(app, ["reset", "--yes", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert sentinel.exists(), "--dry-run must not delete anything"
    assert "DRY RUN" in result.output or "dry run" in result.output.lower()
    # The would-be-deleted file should be named in the preview.
    assert "raw.json" in result.output


@pytest.mark.integration
def test_reset_yes_deletes_in_bounds_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """reset --yes against an allowed tmp data_dir actually deletes the file.

    Covers the happy path of the dir_targets deletion loop so the guard /
    dry-run refactor cannot silently break real resets.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    sentinel = data_dir / "bronze" / "elexon" / "raw.json"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("{}")

    result = runner.invoke(app, ["reset", "--bronze", "--yes"])

    assert result.exit_code == 0, result.output
    assert not sentinel.exists(), "reset --yes should delete in-bounds bronze data"
