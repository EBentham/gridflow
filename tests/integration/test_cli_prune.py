"""CH4-04 (CH-ARCH-04 / C4-7): retention `prune` command.

`prune LAYER [SOURCE] [DATASET]` deletes partitions strictly older than an age
cutoff (``--older-than YYYY-MM-DD`` or ``--keep-days N``) for the selected scope.
It reuses the `reset` containment guards so it can never escape the workspace:

  - `_assert_safe_delete_target` (catastrophic root/home/repo-ancestor),
  - the R-1 ``is_relative_to(data_dir)`` scope check on every target, and
  - the per-file ``_realpath_within`` junction guard in the delete loop.

These tests assert:

  - `--older-than` removes ONLY partitions strictly older than the cutoff;
    newer partitions and the straddling-cutoff partition are kept;
  - dry-run (the default, no ``--execute``) deletes NOTHING but lists targets;
  - a scope resolving outside the workspace (data_dir == fake home) is REFUSED;
  - SOURCE/DATASET scope touches only that dataset's tree.

RED before CH4-04: there is no `prune` command, so Typer exits non-zero with a
"No such command" usage error for every invocation.

Safety: the containment test points ``Path.home()`` and ``GRIDFLOW_DATA_DIR`` at
the same pytest tmp dir, so even an unguarded run only rglobs an empty tmp tree.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from gridflow.cli import app

runner = CliRunner()


def _isolated_env(data_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point gridflow at a tmp data_dir / duckdb / log dir for the duration."""
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(data_dir / "gridflow.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))


def _stage_silver_months(
    data_dir: Path, source: str, dataset: str, months: list[str]
) -> dict[str, Path]:
    """Create ``silver/<source>/<dataset>/year=YYYY/month=MM`` dirs with a file each.

    ``months`` is a list of ``"YYYY-MM"``. Returns a map month -> a staged file
    inside that partition so tests can assert existence after pruning.
    """
    staged: dict[str, Path] = {}
    for ym in months:
        year, month = ym.split("-")
        part = data_dir / "silver" / source / dataset / f"year={year}" / f"month={month}"
        part.mkdir(parents=True, exist_ok=True)
        f = part / f"{dataset}_{year}{month}01.parquet"
        f.write_bytes(b"PAR1")
        staged[ym] = f
    return staged


def _stage_bronze_days(
    data_dir: Path, source: str, dataset: str, days: list[str]
) -> dict[str, Path]:
    """Create ``bronze/<source>/<dataset>/YYYY/MM/DD`` dirs with a file each.

    ``days`` is a list of ``"YYYY-MM-DD"``. Returns a map day -> staged file.
    """
    staged: dict[str, Path] = {}
    for ymd in days:
        year, month, day = ymd.split("-")
        part = data_dir / "bronze" / source / dataset / year / month / day
        part.mkdir(parents=True, exist_ok=True)
        f = part / "raw.json"
        f.write_text("{}")
        staged[ymd] = f
    return staged


def _stage_gold_years(data_dir: Path, gold_dataset: str, years: list[str]) -> dict[str, Path]:
    """Create ``gold/<dataset>/year=YYYY`` dirs with a file each."""
    staged: dict[str, Path] = {}
    for y in years:
        part = data_dir / "gold" / gold_dataset / f"year={y}"
        part.mkdir(parents=True, exist_ok=True)
        f = part / f"{gold_dataset}_{y}0101.parquet"
        f.write_bytes(b"PAR1")
        staged[y] = f
    return staged


@pytest.mark.integration
def test_prune_silver_older_than_selects_strictly_older(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Silver prune removes only months whose entire span precedes the cutoff.

    Cutoff 2026-03-15: Jan + Feb 2026 are wholly before it (removed); March
    straddles the cutoff (kept) and April is newer (kept).
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    staged = _stage_silver_months(
        data_dir, "elexon", "system_prices", ["2026-01", "2026-02", "2026-03", "2026-04"]
    )

    result = runner.invoke(
        app,
        ["prune", "silver", "elexon", "system_prices", "--older-than", "2026-03-15", "--execute"],
    )

    assert result.exit_code == 0, result.output
    assert not staged["2026-01"].exists(), "Jan is wholly before cutoff -> removed"
    assert not staged["2026-02"].exists(), "Feb is wholly before cutoff -> removed"
    assert staged["2026-03"].exists(), "March straddles the cutoff -> kept"
    assert staged["2026-04"].exists(), "April is newer -> kept"


@pytest.mark.integration
def test_prune_bronze_older_than_day_granularity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bronze prune removes day-dirs strictly before the cutoff; cutoff day kept."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    staged = _stage_bronze_days(
        data_dir,
        "elexon",
        "system_prices",
        ["2026-03-13", "2026-03-14", "2026-03-15", "2026-03-16"],
    )

    result = runner.invoke(
        app,
        ["prune", "bronze", "elexon", "system_prices", "--older-than", "2026-03-15", "--execute"],
    )

    assert result.exit_code == 0, result.output
    assert not staged["2026-03-13"].exists(), "before cutoff -> removed"
    assert not staged["2026-03-14"].exists(), "before cutoff -> removed"
    assert staged["2026-03-15"].exists(), "cutoff day itself is not older -> kept"
    assert staged["2026-03-16"].exists(), "newer -> kept"


@pytest.mark.integration
def test_prune_gold_older_than_year_granularity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Gold prune is year-partitioned: delete year Y iff Y < cutoff.year."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    staged = _stage_gold_years(data_dir, "price_curve", ["2024", "2025", "2026"])

    result = runner.invoke(
        app, ["prune", "gold", "price_curve", "--older-than", "2026-03-15", "--execute"]
    )

    assert result.exit_code == 0, result.output
    assert not staged["2024"].exists(), "wholly before cutoff year -> removed"
    assert not staged["2025"].exists(), "wholly before cutoff year -> removed"
    assert staged["2026"].exists(), "cutoff year straddles -> kept"


@pytest.mark.integration
def test_prune_dry_run_is_default_and_deletes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without ``--execute`` prune previews targets but removes nothing (exit 0)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    staged = _stage_silver_months(data_dir, "elexon", "system_prices", ["2026-01", "2026-02"])

    result = runner.invoke(
        app, ["prune", "silver", "elexon", "system_prices", "--older-than", "2026-03-15"]
    )

    assert result.exit_code == 0, result.output
    assert staged["2026-01"].exists(), "dry-run (default) must delete nothing"
    assert staged["2026-02"].exists(), "dry-run (default) must delete nothing"
    assert "DRY RUN" in result.output or "dry run" in result.output.lower()
    # The would-be-deleted partitions are named in the preview.
    assert "month=01" in result.output


@pytest.mark.integration
def test_prune_keep_days_cutoff(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``--keep-days N`` maps to a now-UTC minus N days cutoff."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    today = datetime.now(UTC).date()
    old = today - timedelta(days=400)
    old_ym = f"{old.year:04d}-{old.month:02d}"
    cur_ym = f"{today.year:04d}-{today.month:02d}"
    staged = _stage_silver_months(data_dir, "elexon", "system_prices", [old_ym, cur_ym])

    result = runner.invoke(
        app, ["prune", "silver", "elexon", "system_prices", "--keep-days", "30", "--execute"]
    )

    assert result.exit_code == 0, result.output
    assert not staged[old_ym].exists(), "month ~400d old is past a 30d retention -> removed"
    assert staged[cur_ym].exists(), "current month straddles cutoff -> kept"


@pytest.mark.integration
def test_prune_requires_exactly_one_cutoff(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Neither or both of --older-than / --keep-days is a usage error (no delete)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)
    staged = _stage_silver_months(data_dir, "elexon", "system_prices", ["2020-01"])

    neither = runner.invoke(app, ["prune", "silver", "elexon", "system_prices"])
    assert neither.exit_code != 0, neither.output
    assert staged["2020-01"].exists()

    both = runner.invoke(
        app,
        [
            "prune",
            "silver",
            "elexon",
            "system_prices",
            "--older-than",
            "2026-01-01",
            "--keep-days",
            "5",
        ],
    )
    assert both.exit_code != 0, both.output
    assert staged["2020-01"].exists()


@pytest.mark.integration
def test_prune_rejects_unknown_layer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An unknown LAYER is refused before any work."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    result = runner.invoke(app, ["prune", "platinum", "--older-than", "2026-01-01", "--execute"])
    assert result.exit_code != 0, result.output


@pytest.mark.integration
def test_prune_scope_isolates_dataset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Scoping to one dataset leaves a sibling dataset's tree untouched."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    target = _stage_silver_months(data_dir, "elexon", "system_prices", ["2020-01"])
    sibling = _stage_silver_months(data_dir, "elexon", "fuelhh", ["2020-01"])

    result = runner.invoke(
        app,
        ["prune", "silver", "elexon", "system_prices", "--older-than", "2026-01-01", "--execute"],
    )

    assert result.exit_code == 0, result.output
    assert not target["2020-01"].exists(), "scoped dataset pruned"
    assert sibling["2020-01"].exists(), "sibling dataset untouched"


@pytest.mark.integration
def test_prune_refuses_out_of_bounds_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """prune must refuse a data_dir that resolves to the (fake) home dir.

    Same containment guarantee as `reset`: the catastrophic-target guard refuses
    a data_dir equal to the home dir, so nothing is deleted.
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    _isolated_env(fake_home, tmp_path, monkeypatch)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: fake_home))

    staged = _stage_silver_months(fake_home, "elexon", "system_prices", ["2020-01"])

    result = runner.invoke(
        app,
        ["prune", "silver", "elexon", "system_prices", "--older-than", "2026-01-01", "--execute"],
    )

    assert result.exit_code != 0, result.output
    assert staged["2020-01"].exists(), "guard must not delete an out-of-bounds target"


@pytest.mark.integration
def test_prune_refuses_escaping_source_arg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A SOURCE arg that climbs out of data_dir is refused (R-1 scope guard)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "precious.txt"
    sentinel.write_text("precious")

    result = runner.invoke(
        app, ["prune", "silver", str(external), "--older-than", "2026-01-01", "--execute"]
    )

    assert result.exit_code != 0, result.output
    assert sentinel.exists(), "guard must not delete a target outside data_dir"


@pytest.mark.integration
def test_prune_dry_run_refuses_escaping_source_arg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An escaping SOURCE arg is refused even in dry-run (no preview of it).

    The R-1 scope guard runs before the dry-run branch, so an out-of-bounds
    target is refused without being previewed — parity with reset's
    ``test_reset_dry_run_refuses_escaped_source_arg``.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _isolated_env(data_dir, tmp_path, monkeypatch)

    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "precious.txt"
    sentinel.write_text("precious")

    # No --execute: still must refuse (guard precedes the dry-run preview).
    result = runner.invoke(app, ["prune", "silver", str(external), "--older-than", "2026-01-01"])

    assert result.exit_code != 0, result.output
    assert sentinel.exists(), "dry-run must refuse (not preview) an escaped target"
