"""Static checks on the data-layer notebooks in ``notebooks/``.

The notebooks are jupytext ``py:percent`` scripts. Each one must compile, must
run on gridflow alone (no ``gridflow_models``) and on Polars alone (no
``pandas``), and may name only views the catalogue can register: a silver view
per registered transformer, its ``_latest`` projection, or a gold SQL view.
These checks read the files only; running the notebooks needs a data root.
"""

from __future__ import annotations

import ast
import functools
import re
from pathlib import Path

import pytest

from gridflow.gold.contracts import VIEWS_DIR
from gridflow.pipeline.runner import import_transformers
from gridflow.silver.latest_views import LATEST_VIEW_SPECS
from gridflow.silver.registry import list_transformers

NOTEBOOKS_DIR = Path(__file__).resolve().parents[2] / "notebooks"
NOTEBOOKS = sorted(NOTEBOOKS_DIR.glob("*.py"))

_FORBIDDEN_IMPORTS = {"gridflow_models", "pandas"}
_VIEW_NAME = re.compile(r"\b(?:silver|gold)_\w+")
_GOLD_VIEW_DDL = re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)", re.IGNORECASE)


@functools.cache
def _registered_views() -> frozenset[str]:
    import_transformers()
    pairs = list_transformers()
    silver = {f"silver_{source}_{dataset}" for source, dataset in pairs}
    latest = {
        f"silver_{source}_{dataset}_latest"
        for source, dataset in pairs
        if (source, dataset) in LATEST_VIEW_SPECS
    }
    gold = {
        name
        for sql_file in VIEWS_DIR.glob("*.sql")
        for name in _GOLD_VIEW_DDL.findall(sql_file.read_text(encoding="utf-8"))
    }
    return frozenset(silver | latest | gold)


def _imported_packages(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module.split(".")[0])
    return names


def test_data_layer_notebooks_are_present() -> None:
    assert {"00_overview.py", "01_sources.py", "_TEMPLATE.py"} <= {p.name for p in NOTEBOOKS}


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_compiles_on_gridflow_and_polars_alone(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    compile(tree, str(path), "exec")
    forbidden = _imported_packages(tree) & _FORBIDDEN_IMPORTS
    assert not forbidden, f"{path.name} imports {sorted(forbidden)}"


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_names_only_registered_views(path: Path) -> None:
    used = set(_VIEW_NAME.findall(path.read_text(encoding="utf-8")))
    unknown = used - _registered_views()
    assert not unknown, f"{path.name} names views the catalogue never registers: {sorted(unknown)}"


def test_view_check_rejects_an_unregistered_name() -> None:
    # Guards the check itself: a misspelt view must not pass as registered.
    assert "silver_elexon_system_price" not in _registered_views()
    assert {
        "silver_elexon_fuelhh",
        "silver_elexon_system_prices_latest",
        "gold_eu_gas_storage",
    } <= _registered_views()
