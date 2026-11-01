"""The bronze body parser every Elexon Insights transformer shares."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from gridflow.silver.base import BaseSilverTransformer

if TYPE_CHECKING:
    from pathlib import Path


class ElexonTransformer(BaseSilverTransformer):
    """An Elexon transformer: each body is ``{"data": [...]}`` or a bare list."""

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        # Bytes, not read_text(): json decodes UTF-8 itself, so silver no longer
        # depends on the machine's default encoding (cp1252 turned ’ into â€™).
        data = json.loads(path.read_bytes())
        records = data.get("data") if isinstance(data, dict) else data
        if not isinstance(records, list):
            # A dict without a "data" list is an error reply, not empty data.
            keys = f" with keys {sorted(data)}" if isinstance(data, dict) else ""
            raise ValueError(
                f"expected a list or {{'data': [...]}}, got {type(data).__name__}{keys}"
            )
        return records
