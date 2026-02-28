"""Connector registry — maps source names to connector classes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gridflow.connectors.base import BaseConnector

if TYPE_CHECKING:
    from gridflow.config.settings import SourceConfig

# Registry of source name -> connector class
_REGISTRY: dict[str, type[BaseConnector]] = {}


def register_connector(source_name: str, connector_cls: type[BaseConnector]) -> None:
    """Register a connector class for a source name."""
    _REGISTRY[source_name] = connector_cls


def get_connector(source_name: str, config: SourceConfig) -> BaseConnector:
    """Create a connector instance for the given source."""
    if source_name not in _REGISTRY:
        raise ValueError(
            f"Unknown source: {source_name}. "
            f"Available: {list(_REGISTRY.keys())}. "
            f"Did you forget to register the connector?"
        )
    return _REGISTRY[source_name](config)


def list_sources() -> list[str]:
    """Return all registered source names."""
    return list(_REGISTRY.keys())
