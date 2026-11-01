"""Every name in ``gridflow.contract`` still resolves, so a rename fails here first.

The lists live in ``src/gridflow/contract.py`` so they install with the
package and gridflow_models can check its own usage against them; that
module says why each one is pinned.
"""

from __future__ import annotations

import dataclasses
import importlib

import pytest

from gridflow import contract
from gridflow.serving.client import GridflowClient
from gridflow.silver.schema_manifest import SilverSchemaEntry, get_silver_schema_manifest


def test_contract_lists_ship_with_the_package() -> None:
    imported = contract.IMPORTED_NAMES
    assert (len(imported), sum(map(len, imported.values()))) == (9, 31)
    assert len(contract.CLIENT_METHODS) == 12
    assert len(contract.MANIFEST_FIELDS) == 10
    assert len(contract.HANDLE_SOURCES) == 6


@pytest.mark.parametrize("module_name", sorted(contract.IMPORTED_NAMES))
def test_every_imported_name_exists(module_name: str) -> None:
    module = importlib.import_module(module_name)
    missing = [name for name in contract.IMPORTED_NAMES[module_name] if not hasattr(module, name)]
    assert not missing, f"{module_name} no longer exports {missing}"


def test_client_methods_exist() -> None:
    missing = [
        name
        for name in contract.CLIENT_METHODS
        if not callable(getattr(GridflowClient, name, None))
    ]
    assert not missing, f"GridflowClient lost {missing}"


def test_private_require_con_alias_kept_for_models() -> None:
    # gridflow_models (84c6dde) still calls _require_con; drop this with the alias in 0.24.
    assert GridflowClient._require_con is GridflowClient.require_con


def test_manifest_entry_fields_exist() -> None:
    fields = {field.name for field in dataclasses.fields(SilverSchemaEntry)}
    assert set(contract.MANIFEST_FIELDS) <= fields, sorted(set(contract.MANIFEST_FIELDS) - fields)


def test_each_client_handle_has_its_manifest_row_and_source() -> None:
    handles = {
        entry.dataset: entry.source
        for entry in get_silver_schema_manifest(include_serving_aliases=True)
        if entry.relation_kind != "silver"
    }
    assert {name: handles.get(name, "missing") for name in contract.HANDLE_SOURCES} == (
        contract.HANDLE_SOURCES
    )
