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


def pagination_from(parsed: dict[str, Any]) -> tuple[int, int]:
    """Extract (current_page, total_pages) from an ALREADY-PARSED response body.

    D-18: the pure half of the parse-once split -- callers that already hold
    the parsed body (from a single ``parse_json_response`` call) derive
    pagination from it directly instead of re-parsing.
    """
    if not isinstance(parsed, dict):
        return 1, 1

    # Elexon Insights API uses metadata field for pagination
    meta = parsed.get("meta", parsed.get("metadata", {}))
    if isinstance(meta, dict):
        current: Any = meta.get("page", meta.get("currentPage", 1))
        total: Any = meta.get("totalPages", meta.get("lastPage", 1))
        return int(current), int(total)
    return 1, 1


def record_count_from(parsed: dict[str, Any]) -> int | None:
    """Return the record count from an ALREADY-PARSED response body, or None.

    C-8/D-8: ``None`` unless ``parsed`` is a dict whose ``data`` field is a
    list -- a parse failure (``parse_json_response`` returns ``{}``) or any
    unexpected shape means the count is UNAVAILABLE, never zero. Only a
    genuinely parsed, empty list yields ``0``.
    """
    if not isinstance(parsed, dict):
        return None
    data = parsed.get("data")
    if not isinstance(data, list):
        return None
    return len(data)


def get_pagination_info(response_body: bytes) -> tuple[int, int]:
    """Extract current page and total pages from response metadata.

    Returns (current_page, total_pages). Thin wrapper over
    ``pagination_from`` (D-18) -- contract unchanged; retained for callers
    that have not materialised a parse of their own.
    """
    return pagination_from(parse_json_response(response_body))


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
