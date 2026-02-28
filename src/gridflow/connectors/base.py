"""Abstract base connector and RawResponse data class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from gridflow.config.settings import SourceConfig


@dataclass(frozen=True)
class RawResponse:
    """Immutable container for a raw API response + provenance metadata."""

    body: bytes
    content_type: str  # application/json, text/xml, text/csv
    source: str  # e.g. "elexon"
    dataset: str  # e.g. "system_prices"
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    request_url: str = ""
    request_params: dict[str, Any] = field(default_factory=dict)
    api_version: str = ""
    page: int = 1
    total_pages: int = 1
    http_status: int = 200


class BaseConnector(ABC):
    """Abstract base for all API connectors."""

    source_name: str  # Must be set by subclasses

    def __init__(self, config: SourceConfig):
        self.config = config
        self._client: httpx.AsyncClient | None = None
        self._semaphore: Any = None  # Set in __aenter__

    @abstractmethod
    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch raw data for a date range. Returns list of raw responses
        (one per API page/call). Each RawResponse includes body + metadata."""
        ...

    @abstractmethod
    def list_datasets(self) -> list[str]:
        """Return available datasets for this source."""
        ...

    async def __aenter__(self) -> BaseConnector:
        import asyncio

        self._semaphore = asyncio.Semaphore(self.config.rate_limit_per_second)
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            headers=self._auth_headers(),
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._client:
            await self._client.aclose()

    def _auth_headers(self) -> dict[str, str]:
        """Build auth headers from config. Override for non-header auth."""
        if self.config.api_key and self.config.api_key_header:
            return {self.config.api_key_header: self.config.api_key}
        return {}
