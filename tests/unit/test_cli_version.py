"""``gridflow --version`` prints the installed version and exits 0."""

from __future__ import annotations

from importlib.metadata import version

from typer.testing import CliRunner

from gridflow.cli import app


def test_version_prints_the_installed_version() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == f"gridflow {version('gridflow')}"


def test_version_runs_before_any_command() -> None:
    """Eager: a command after ``--version`` is not run and needs none of its arguments."""
    result = CliRunner().invoke(app, ["--version", "backfill"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == f"gridflow {version('gridflow')}"
