"""Gold layer — modelling-ready dataset builders.

Importing this package imports every builder module, which triggers each
builder's ``register_builder`` call so the registry is populated. Mirrors the
connector/silver auto-registration-on-import pattern.
"""

from __future__ import annotations

# Import builder modules to trigger registration (side-effecting import).
from gridflow.gold import system_marginal_price  # noqa: E402,F401  # registers on import
from gridflow.gold.registry import get_builder, list_gold_datasets, register_builder

__all__ = ["get_builder", "list_gold_datasets", "register_builder"]
