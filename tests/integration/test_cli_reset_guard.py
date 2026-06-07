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

import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from gridflow.cli import _is_dangerous_delete_target, _realpath_within, app

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


# --- R-1: source/dataset CLI args must not escape data_dir -------------------
# The actual delete targets are data_dir/<layer>/<source>/<dataset>, where
# source/dataset are user CLI args. An absolute source, or a `../..` climb,
# resolves outside data_dir and must be refused before any deletion (and before
# the --dry-run preview).


@pytest.mark.integration
def test_reset_refuses_absolute_source_arg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """reset with an absolute `source` arg escaping data_dir is refused.

    `data_dir/bronze / "<abs>"` collapses to the absolute path, so the delete
    target lands outside data_dir entirely. A sentinel staged in that external
    dir must survive and the command must exit non-zero.

    RED before R-1: the guard only checks data_dir/duckdb_path, not the
    source-derived targets, so the external sentinel is deleted and exit == 0.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "precious.txt"
    sentinel.write_text("precious")

    result = runner.invoke(app, ["reset", str(external), "--bronze", "--yes"])

    assert result.exit_code != 0, result.output
    assert sentinel.exists(), "guard must not delete a target outside data_dir"


@pytest.mark.integration
def test_reset_refuses_climbing_source_arg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """reset with a `../..`-style climbing source arg is refused.

    A single `..` would resolve back to data_dir (still contained), so the
    attack needs to climb above data_dir; `../..` lands outside.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    # data_dir/bronze/../../<x> -> tmp_path/<x>, outside data_dir.
    sentinel = tmp_path / "climbed.txt"
    sentinel.write_text("precious")

    result = runner.invoke(app, ["reset", "../..", "--bronze", "--yes"])

    assert result.exit_code != 0, result.output
    assert sentinel.exists(), "guard must not delete a climbed-to target"


@pytest.mark.integration
def test_reset_dry_run_refuses_escaped_source_arg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--dry-run must also refuse an escaped source arg (no preview of it).

    The containment check runs before the dry-run preview, so an out-of-bounds
    target is refused even in preview mode.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "precious.txt"
    sentinel.write_text("precious")

    result = runner.invoke(app, ["reset", str(external), "--bronze", "--yes", "--dry-run"])

    assert result.exit_code != 0, result.output
    assert sentinel.exists(), "--dry-run must refuse (not preview) an escaped target"


# --- R-2: _wipe_dir must not follow junctions/symlinks out of the tree -------


def test_realpath_within_excludes_external_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The realpath-containment helper excludes a path whose realpath escapes.

    A junction/symlink reports a benign path under data_dir but its realpath
    resolves outside; the helper must return False so the wipe loop skips it.
    Tested directly via a monkeypatched os.path.realpath so no real junction is
    needed (Windows junction creation is privileged/flaky in CI).
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    inside = data_dir / "bronze" / "raw.json"
    outside = tmp_path / "external" / "raw.json"

    # An ordinary file under data_dir is contained.
    assert _realpath_within(inside, data_dir) is True

    # Simulate a junction: the apparent path is under data_dir, but its realpath
    # points outside the tree -> must be excluded.
    real = os.path.realpath

    def fake_realpath(p: object, *args: object, **kwargs: object) -> str:
        if str(p) == str(inside):
            return str(outside)
        return real(p, *args, **kwargs)

    monkeypatch.setattr("gridflow.cli.os.path.realpath", fake_realpath)
    assert _realpath_within(inside, data_dir) is False
