"""Elexon API response parsing utilities."""

from __future__ import annotations

import json
import logging
from typing import Any, cast

logger = logging.getLogger(__name__)


def parse_json_response(body: bytes) -> dict[str, Any]:
    """Parse a JSON API response body."""
    try:
        # json.loads is typed as Any; callers rely on the dict shape of Elexon responses.
        return cast("dict[str, Any]", json.loads(body))
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        return {}


def extract_data_records(response_body: bytes) -> list[dict[str, Any]]:
    """Extract data records from Elexon Insights API response.

    The Insights API wraps results in {"data": [...]} format.
    """
    parsed = parse_json_response(response_body)
    if isinstance(parsed, dict):
        return cast("list[dict[str, Any]]", parsed.get("data", []))
    if isinstance(parsed, list):
        return parsed
    return []


def get_pagination_info(response_body: bytes) -> tuple[int, int]:
    """Extract current page and total pages from response metadata.

    Returns (current_page, total_pages).
    """
    parsed = parse_json_response(response_body)
    if not isinstance(parsed, dict):
        return 1, 1

    # Elexon Insights API uses metadata field for pagination
    meta = parsed.get("meta", parsed.get("metadata", {}))
    if isinstance(meta, dict):
        current: Any = meta.get("page", meta.get("currentPage", 1))
        total: Any = meta.get("totalPages", meta.get("lastPage", 1))
        return int(current), int(total)
    return 1, 1


# Settlement run type precedence (higher = more final)
RUN_PRECEDENCE: dict[str, int] = {
    "II": 1,  # Initial Indicative
    "SF": 2,  # System Frequency
    "R1": 3,  # Reconciliation Run 1
    "R2": 4,  # Reconciliation Run 2
    "R3": 5,  # Reconciliation Run 3
    "RF": 6,  # Final Reconciliation
    "DF": 7,  # Dispute Final
}
