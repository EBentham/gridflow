"""Silver transformer registry — maps (source, dataset) to transformer classes."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from gridflow.silver.base import BaseSilverTransformer

# Registry of (source, dataset) -> transformer class
_REGISTRY: dict[tuple[str, str], type[BaseSilverTransformer]] = {}


def register_transformer(
    source: str, dataset: str, transformer_cls: type[BaseSilverTransformer]
) -> None:
    """Register a transformer class for a (source, dataset) pair."""
    _REGISTRY[(source, dataset)] = transformer_cls


def get_transformer(source: str, dataset: str, data_dir: Path) -> BaseSilverTransformer:
    """Create a transformer instance for the given source/dataset."""
    key = (source, dataset)
    if key not in _REGISTRY:
        raise ValueError(
            f"No transformer registered for {source}/{dataset}. "
            f"Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[key](data_dir)


def list_transformers(source: str | None = None) -> list[tuple[str, str]]:
    """Return all registered (source, dataset) pairs, optionally filtered by source."""
    if source:
        return [(s, d) for s, d in _REGISTRY if s == source]
    return list(_REGISTRY.keys())
