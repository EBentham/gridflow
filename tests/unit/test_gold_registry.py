"""Unit tests for the gold builder registry (CH4-02 / CH-ARCH-02, C4-5).

The registry mirrors the connector/silver registries: importing
:mod:`gridflow.gold` populates it, the lookup returns the registered builder,
and an unknown dataset raises a clear ``ValueError``. A guard test also pins
that the registry stays in lockstep with the runner's ``GOLD_DATASETS`` name
tuple, and that the deleted Phase-3 stubs are gone (C4-6).
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

# Importing the package triggers builder auto-registration.
import gridflow.gold  # noqa: F401
from gridflow.gold.base import BaseGoldBuilder
from gridflow.gold.registry import (
    get_builder,
    get_builder_class,
    list_gold_datasets,
)
from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder
from gridflow.pipeline import runner


def test_system_marginal_price_is_registered() -> None:
    """The one real builder is discoverable via the registry lookup."""
    assert "system_marginal_price" in list_gold_datasets()


def test_get_builder_class_resolves_real_builder() -> None:
    """The lookup returns the registered builder class itself."""
    assert get_builder_class("system_marginal_price") is SystemMarginalPriceBuilder


def test_get_builder_constructs_instance(tmp_path: Path) -> None:
    """get_builder returns a constructed BaseGoldBuilder instance."""
    builder = get_builder("system_marginal_price", tmp_path)
    assert isinstance(builder, SystemMarginalPriceBuilder)
    assert isinstance(builder, BaseGoldBuilder)


def test_unknown_dataset_raises_clear_error() -> None:
    """An unregistered name raises ValueError naming the dataset + alternatives."""
    with pytest.raises(ValueError, match="Unknown gold dataset: merit_order"):
        get_builder_class("merit_order")
    with pytest.raises(ValueError, match="Unknown gold dataset: nope"):
        get_builder("nope", Path("."))


def test_lookup_resolves_module_attribute_at_call_time(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Patching the builder symbol on its module is honoured by the lookup.

    This is the contract the golden CliRunner tests rely on: they monkeypatch
    ``gridflow.gold.system_marginal_price.SystemMarginalPriceBuilder`` and the
    runner's build path (now registry-backed) must pick up the patched class.
    """

    class _Sentinel(SystemMarginalPriceBuilder):
        pass

    monkeypatch.setattr("gridflow.gold.system_marginal_price.SystemMarginalPriceBuilder", _Sentinel)
    assert get_builder_class("system_marginal_price") is _Sentinel
    assert isinstance(get_builder("system_marginal_price", tmp_path), _Sentinel)


def test_registry_matches_runner_gold_datasets() -> None:
    """The registry's dataset set equals the runner's GOLD_DATASETS tuple."""
    assert set(list_gold_datasets()) == set(runner.GOLD_DATASETS)


def test_dead_stub_modules_are_deleted() -> None:
    """merit_order / demand_features stub modules no longer import (C4-6)."""
    for stub in ("gridflow.gold.merit_order", "gridflow.gold.demand_features"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(stub)
