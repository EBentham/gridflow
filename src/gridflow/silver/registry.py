"""Silver transformer registry — maps (source, dataset) to transformer classes."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

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
            f"No transformer registered for {source}/{dataset}. Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[key](data_dir)


def get_transformer_class(source: str, dataset: str) -> type[BaseSilverTransformer] | None:
    """Return the registered transformer CLASS for source/dataset, or ``None``.

    Class-attribute reads only — no instantiation, no filesystem access, no
    ``data_dir`` required. Used by the F-16 duplicate-quality-check
    (``cli.py``) to resolve ``ENTITY_KEY_COLUMNS``/``OPTIONAL_ENTITY_KEY_COLUMNS``
    without constructing a transformer for a dataset it is merely reading a
    quality-report frame for (T-R2A-04: no dynamic import from a
    data-derived name, no SQL from ``source``/``dataset`` either).
    """
    return _REGISTRY.get((source, dataset))


def list_transformers(source: str | None = None) -> list[tuple[str, str]]:
    """Return all registered (source, dataset) pairs, optionally filtered by source."""
    if source:
        return [(s, d) for s, d in _REGISTRY if s == source]
    return list(_REGISTRY.keys())
