"""Gold builder registry — maps gold dataset names to builder classes.

Mirrors the connector (:mod:`gridflow.connectors.registry`) and silver
(:mod:`gridflow.silver.registry`) registries: a module-level dict plus a
``get_*``/``list_*`` lookup pair. Builders register themselves on import via
:func:`register_builder`, so importing :mod:`gridflow.gold` (which imports every
builder module) is what populates the registry.

The registry stores each builder's ``(module, qualified-name)`` rather than the
class object, and :func:`get_builder` resolves the class via ``getattr`` at
lookup time. This keeps the lookup honest when a test monkeypatches a builder
symbol on its defining module (the established pattern in
``test_cli_runner_golden.py``) — the resolved class is always whatever the
module currently exposes, never a stale reference captured at registration.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.gold.base import BaseGoldBuilder

# Registry of gold dataset name -> (module path, attribute name) of the builder.
_REGISTRY: dict[str, tuple[str, str]] = {}


def register_builder(dataset: str, builder_cls: type[BaseGoldBuilder]) -> None:
    """Register a gold builder class for a dataset name.

    The builder is recorded by its defining module and attribute name (not the
    class object) so the lookup re-resolves it at call time.

    Args:
        dataset: Gold dataset name (e.g. ``"system_marginal_price"``).
        builder_cls: The :class:`~gridflow.gold.base.BaseGoldBuilder` subclass.
    """
    _REGISTRY[dataset] = (builder_cls.__module__, builder_cls.__qualname__)


def get_builder_class(dataset: str) -> type[BaseGoldBuilder]:
    """Resolve the registered builder *class* for a dataset (no instance).

    Args:
        dataset: Gold dataset name.

    Returns:
        The currently-registered builder class, resolved from its defining
        module at call time.

    Raises:
        ValueError: If no builder is registered for ``dataset``.
    """
    if dataset not in _REGISTRY:
        raise ValueError(
            f"Unknown gold dataset: {dataset}. "
            f"Available: {sorted(_REGISTRY)}. "
            f"Did you forget to register the builder?"
        )
    module_path, qualname = _REGISTRY[dataset]
    module = importlib.import_module(module_path)
    builder_cls: type[BaseGoldBuilder] = getattr(module, qualname)
    return builder_cls


def get_builder(dataset: str, data_dir: Path) -> BaseGoldBuilder:
    """Create a gold builder instance for the given dataset.

    Args:
        dataset: Gold dataset name.
        data_dir: Pipeline data directory passed to the builder constructor.

    Returns:
        A constructed :class:`~gridflow.gold.base.BaseGoldBuilder` instance.

    Raises:
        ValueError: If no builder is registered for ``dataset``.
    """
    return get_builder_class(dataset)(data_dir)


def list_gold_datasets() -> list[str]:
    """Return all registered gold dataset names, sorted."""
    return sorted(_REGISTRY)
