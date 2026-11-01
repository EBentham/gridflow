"""Run the data-layer notebooks top to bottom against the sample store.

Ported from gridflow-models' ``tests/integration/test_notebooks.py``. Each
numbered notebook runs as a script in its own interpreter, from ``notebooks/``,
with the data root at the ``sample_store`` fixture and the notebook window on
its fixture day. A run fails on a non-zero exit, a missing end marker, or a
``NameError``, ``UnboundLocalError`` or ``AttributeError`` in either stream:
the notebooks catch nothing, so a name error a cell printed instead of raising
is a real failure too.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from _bronze_fixtures import FIXTURE_DATE

NOTEBOOKS_DIR = Path(__file__).resolve().parents[2] / "notebooks"
NOTEBOOKS = sorted(NOTEBOOKS_DIR.glob("[0-9][0-9]_*.py"))

# Printed after the last cell; its absence means the run stopped early.
_END_MARKER = "notebook run reached the end"
_FORBIDDEN_SIGNALS = ("NameError", "UnboundLocalError", "AttributeError")

# What `show(view, query)` prints: a row count when the view is there, else a skip line.
_SHOWN = re.compile(r"^((?:silver|gold)_\w+): (\d+) rows$", re.MULTILINE)
_SKIPPED = re.compile(r"^skipped: ((?:silver|gold)_\w+) not ingested", re.MULTILINE)

# 00_overview.py reads system prices; 01_sources.py reads 11 datasets, and the
# sample store holds 9 of those (Open-Meteo wind and solar have no fixture).
_MAX_SKIPPED = 3
_MIN_SHOWN = {"00_overview.py": 1, "01_sources.py": 8}


def notebook_env(sample_store: Path) -> dict[str, str]:
    """The environment a notebook runs in: the sample store, over its fixture day."""
    return {
        **os.environ,
        "GRIDFLOW_DATA_DIR": str(sample_store),
        "GRIDFLOW_DUCKDB_PATH": str(sample_store / "gridflow.duckdb"),
        "GRIDFLOW_LOG_DIR": str(sample_store / "logs"),
        "GRIDFLOW_NB_START": FIXTURE_DATE.isoformat(),
        "GRIDFLOW_NB_END": FIXTURE_DATE.isoformat(),
        "PYTHONIOENCODING": "utf-8",
    }


def run_notebook(path: Path, env: dict[str, str]) -> str:
    """Run one notebook to the end and return its stdout, or fail the test."""
    run_to_end = (
        f"import runpy; runpy.run_path({str(path)!r}, run_name='__main__'); print({_END_MARKER!r})"
    )
    proc = subprocess.run(
        [sys.executable, "-c", run_to_end],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=NOTEBOOKS_DIR,
        env=env,
        timeout=300,
        check=False,
    )
    streams = f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    for forbidden in _FORBIDDEN_SIGNALS:
        assert forbidden not in proc.stdout + proc.stderr, (
            f"{path.name} hit a {forbidden}.\n{streams}"
        )
    assert proc.returncode == 0 and _END_MARKER in proc.stdout, (
        f"{path.name} did not run to the end (exit {proc.returncode}).\n{streams}"
    )
    return proc.stdout


def _datasets(views: set[str]) -> set[str]:
    """One name per dataset: a `_latest` view counts as its base view."""
    return {view.removesuffix("_latest") for view in views}


def test_data_layer_notebooks_are_found() -> None:
    assert {p.name for p in NOTEBOOKS} >= {"00_overview.py", "01_sources.py"}


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_runs_on_the_sample_store(
    path: Path, sample_store: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    stdout = run_notebook(path, notebook_env(sample_store))
    empty = {view for view, rows in _SHOWN.findall(stdout) if rows == "0"}
    shown = _datasets({view for view, rows in _SHOWN.findall(stdout) if rows != "0"})
    skipped = _datasets(set(_SKIPPED.findall(stdout)))
    with capsys.disabled():
        print(f"\n{path.name}: {len(shown)} shown, {len(skipped)} skipped")

    assert not empty, f"{path.name} read no rows from {sorted(empty)} on {FIXTURE_DATE}"
    assert len(skipped) <= _MAX_SKIPPED, f"{path.name} skipped {sorted(skipped)}"
    assert len(shown) >= _MIN_SHOWN.get(path.name, 0), f"{path.name} showed {sorted(shown)}"


@pytest.mark.parametrize(
    "injected",
    [
        pytest.param("undefined_name\n", id="raised"),
        pytest.param(
            "try:\n    undefined_name\nexcept Exception as exc:\n    print(repr(exc))\n",
            id="printed",
        ),
    ],
)
def test_harness_reports_injected_name_error(
    tmp_path: Path, sample_store: Path, injected: str
) -> None:
    # A copy of a real notebook with one bad cell after the bootstrap, raised or swallowed.
    source = (NOTEBOOKS_DIR / "00_overview.py").read_text(encoding="utf-8")
    marker = "# %% [Parameters]\n"
    assert marker in source
    broken = tmp_path / "00_overview.py"
    broken.write_text(source.replace(marker, f"# %%\n{injected}\n{marker}", 1), encoding="utf-8")

    with pytest.raises(AssertionError, match="hit a NameError"):
        run_notebook(broken, notebook_env(sample_store))
