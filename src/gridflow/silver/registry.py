"""Silver transformer registry — maps (source, dataset) to transformer classes."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.silver.base import BaseSilverTransformer

PostRunHook = Callable[["BaseSilverTransformer", date], None]
"""``fn(transformer, target_date)``, called by ``run_transform`` after ``run()``."""

# Registry of (source, dataset) -> transformer class
_REGISTRY: dict[tuple[str, str], type[BaseSilverTransformer]] = {}

# (source, dataset) -> post-run hooks, in registration order (ADR-034 P-8).
_POST_RUN_HOOKS: dict[tuple[str, str], list[PostRunHook]] = {}

# (source, dataset) -> (reason, warn) for a dataset transform skips (ADR-034 P-13).
_INGEST_ONLY: dict[tuple[str, str], tuple[str, bool]] = {}


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


def register_post_run_hook(source: str, dataset: str, fn: PostRunHook) -> None:
    """Run ``fn(transformer, target_date)`` after each ``run()`` of ``source/dataset``.

    ``run_transform`` calls it inside the same ``try`` as ``run()``, so a
    raising hook fails the dataset (ADR-034 P-8: the bespoke NESO transformers
    gain completion records without their modules changing).
    """
    hooks = _POST_RUN_HOOKS.setdefault((source, dataset), [])
    if fn not in hooks:
        hooks.append(fn)


def post_run_hooks(source: str, dataset: str) -> tuple[PostRunHook, ...]:
    """Return the post-run hooks registered for ``source/dataset``."""
    return tuple(_POST_RUN_HOOKS.get((source, dataset), ()))


def register_ingest_only(source: str, dataset: str, reason: str, *, warn: bool = True) -> None:
    """Mark ``source/dataset`` as having no silver transform (ADR-034 P-13).

    ``run_transform`` skips it with ``reason``: with ``warn`` the dataset
    reports ``completed_with_warnings`` (a tabular family still waiting for its
    frozen record, loud by design), without it ``success`` (a non-tabular
    family that is catalogue-only by nature).
    """
    _INGEST_ONLY[(source, dataset)] = (reason, warn)


def ingest_only_reason(source: str, dataset: str) -> tuple[str, bool] | None:
    """Return ``(reason, warn)`` when ``source/dataset`` is ingest-only, else ``None``."""
    return _INGEST_ONLY.get((source, dataset))
