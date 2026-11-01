"""One provenance rule, one site (D-23).

The capture scan and the generic engine need the same things out of a bronze
body's ``.meta.json`` sidecar: which CKAN resource it came from and when NESO
published it (``ckan_last_modified`` → ``published_at``, D-22/D-15). Deriving
that in two places would be two chances to derive it differently, and the
failure mode is silent: a fabricated vintage looks exactly like a real one once
it is in silver.

So the rule lives here. **Nothing is ever defaulted, substituted or back-filled
from the fetch clock**: when a required element is missing or unparseable,
:func:`provenance_for` returns ``None`` and logs a WARNING naming the file and
the missing key. The capture scan then reports the sidecar unusable, and a
``ckan_last_modified`` record's capture fails (``CaptureContextError``) instead
of landing with a substitute vintage.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

__all__ = ["NesoProvenance", "provenance_for"]

# The D-12 keys the connector writes into `request_params`. Required for every
# dataset: a body whose sidecar cannot say which resource it is, or when the
# vendor published it, has no honest vintage.
_REQUIRED_KEYS = (
    "package",
    "resource_id",
    "resource_name",
    "resource_filename",
    "ckan_last_modified",
)


@dataclass(frozen=True)
class NesoProvenance:
    """What a bronze body's sidecar says about the capture behind it.

    Frozen because it is evidence: a caller that could mutate a field would be
    able to change the recorded vintage of rows it is about to write.

    Attributes:
        package: The CKAN package slug the resource was selected from.
        resource_id: The CKAN resource UUID — provenance only, never a
            selector (D-03).
        resource_name: The exact ``resources[].name`` that was matched (D-04).
        published_at: NESO's publication instant, tz-aware UTC, parsed from
            CKAN's naive ``last_modified`` and read as UTC per D-15. This is
            what ADR-025 §3 turns into each row's ``available_at``.
    """

    package: str
    resource_id: str
    resource_name: str
    published_at: datetime


def provenance_for(raw_path: Path) -> NesoProvenance | None:
    """Read the provenance of one bronze body from its sidecar.

    Args:
        raw_path: The bronze BODY path (``raw_*.csv``). Its sidecar is
            ``raw_path.with_suffix(".meta.json")`` — the same spelling
            ``silver/base.py``'s per-file vintage loop uses, so the two cannot
            disagree about which file they are reading.

    Returns:
        The parsed provenance, or ``None`` when the sidecar is absent,
        unreadable, not a JSON object, carries no ``request_params`` object, is
        missing any of the D-12 required keys, or carries a
        ``ckan_last_modified`` that does not parse. Every one of those logs a
        WARNING naming the file and the problem.
    """
    meta_path = raw_path.with_suffix(".meta.json")
    params = _read_request_params(meta_path)
    if params is None:
        return None

    values: dict[str, str] = {}
    for key in _REQUIRED_KEYS:
        raw_value = params.get(key)
        if not isinstance(raw_value, str) or not raw_value:
            logger.warning(
                "NESO Data Portal provenance unusable for %s: sidecar %s has no usable "
                "request_params[%r] (got %r); refusing to transform a body whose vintage "
                "cannot be established",
                raw_path,
                meta_path,
                key,
                raw_value,
            )
            return None
        values[key] = raw_value

    published_at = _parse_ckan_timestamp(values["ckan_last_modified"])
    if published_at is None:
        logger.warning(
            "NESO Data Portal provenance unusable for %s: sidecar %s carries an "
            "unparseable ckan_last_modified %r; published_at is never substituted from "
            "the fetch clock (D-23/FM-05)",
            raw_path,
            meta_path,
            values["ckan_last_modified"],
        )
        return None

    return NesoProvenance(
        package=values["package"],
        resource_id=values["resource_id"],
        resource_name=values["resource_name"],
        published_at=published_at,
    )


def _read_request_params(meta_path: Path) -> dict[str, Any] | None:
    """Return the sidecar's ``request_params`` object, or ``None`` loudly."""
    try:
        payload: Any = json.loads(meta_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning(
            "NESO Data Portal provenance unusable: no sidecar at %s. The body is durable "
            "before its sidecar by write order, so this is the FM-01 crash window rather "
            "than a corrupt capture",
            meta_path,
        )
        return None
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning(
            "NESO Data Portal provenance unusable: sidecar %s could not be read as JSON (%s)",
            meta_path,
            exc,
        )
        return None

    if not isinstance(payload, dict):
        logger.warning(
            "NESO Data Portal provenance unusable: sidecar %s is a %s, not a JSON object",
            meta_path,
            type(payload).__name__,
        )
        return None

    params = payload.get("request_params")
    if not isinstance(params, dict):
        logger.warning(
            "NESO Data Portal provenance unusable: sidecar %s carries no request_params "
            "object (got %r), so none of the D-12 provenance keys are readable",
            meta_path,
            type(params).__name__,
        )
        return None
    return params


def _parse_ckan_timestamp(raw_value: str) -> datetime | None:
    """Parse CKAN's ``last_modified`` as UTC (D-15), or ``None``.

    CKAN emits a naive ISO-8601 stamp (``2026-08-16T18:25:03.877001``). D-15
    records the three independent observations that establish it as UTC and
    pins the claim with a live-marked re-check; it is corroborated, never a
    vendor contract, so it is read here and nowhere else.
    """
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
