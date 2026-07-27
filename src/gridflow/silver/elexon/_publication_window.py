"""Elexon publication-window scope, column override, and exemptions (R2-A Task 2).

Confines the ``silver -> connectors`` import to this one module (R-8;
precedent ``silver/openmeteo/historical.py:29-38``): everything else in
``silver.elexon`` and ``silver.base`` reasons about scope through
:func:`publication_window_params` and the two dicts below, never by
importing ``connectors.elexon.endpoints`` directly.

**Scope (G-2, ``R2-A-PLAN.md`` S1.7).** 24 of the 33 registered Elexon
datasets are in scope — ``PUBLISH_DATETIME`` param style with the DEFAULT
``publishDateTimeFrom``/``publishDateTimeTo`` param names (not an override
like ``from``/``to`` or ``measurementDateTimeFrom/To``, whose semantics are
undocumented or actively different from the default's). Of those 24, 22 are
filtered on ``published_at`` and one — ``remit`` — is filtered on
``timestamp_utc`` (its own derived, deterministic copy of the same publish
instant; S-2). The remaining 2 in-scope-but-unusable datasets and the 9
out-of-scope datasets make up :data:`PUBLICATION_WINDOW_EXEMPT` (11 entries,
each machine-checked and reasoned) — see ``A-11``.

:func:`publication_window_params` derives the 22 filtered + 2 exempt
"in scope" set directly from ``ENDPOINTS`` rather than hand-listing it a
second time, so a future new dataset is classified correctly by construction
(right param style + right param names) without needing this module edited
— only a genuinely NEW exemption class needs a
:data:`PUBLICATION_WINDOW_EXEMPT` entry.
"""

from __future__ import annotations

from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle

#: Reasons are recorded verbatim from the plan's measurement (S1.7) so a
#: reviewer can audit each exemption against the endpoint registry directly.
#: Combines the 2 in-scope-but-unusable datasets (N-12) and the 9
#: out-of-scope datasets (wrong param style or an undocumented param-name
#: override) into one machine-checked table (A-11).
PUBLICATION_WINDOW_EXEMPT: dict[str, str] = {
    # --- N-12: PUBLISH_DATETIME + default param names, but no usable dimension ---
    "fuelinst": (
        "maps the publish field then drops it at select (fuelinst.py) — "
        "an unfixed F-08-class instance; no bronze exposure on disk (N-12)"
    ),
    "temp": (
        "maps publish fields directly onto timestamp_utc (temp.py), "
        "collapsing the publication dimension; semantics unverified "
        "in-repo; no bronze exposure on disk (N-12)"
    ),
    # --- Out of scope: PUBLISH_DATETIME but an undocumented param override ---
    "boal": (
        "bare from/to params (endpoints.py) — undocumented "
        "(TODO in endpoints.py); do not invent semantics for the override"
    ),
    "disbsad": (
        "bare from/to params (endpoints.py) — undocumented "
        "(TODO in endpoints.py); do not invent semantics for the override"
    ),
    "mid": (
        "bare from/to params (endpoints.py) — undocumented "
        "(TODO in endpoints.py); do not invent semantics for the override"
    ),
    "netbsad": (
        "bare from/to params (endpoints.py) — undocumented "
        "(TODO in endpoints.py); do not invent semantics for the override"
    ),
    "freq": (
        "overrides to measurementDateTimeFrom/To — sending "
        "publishDateTimeFrom/To causes the API to silently ignore the "
        "window and return the latest ~5761 samples instead"
    ),
    # --- Out of scope: not PUBLISH_DATETIME at all ---
    "market_depth": "DATE_PATH param style — no publication window to filter",
    "system_prices": "DATE_PATH param style — no publication window to filter",
    "pn": "SETTLEMENT_DATE_PERIOD param style — no publication window to filter",
    "bmunits_reference": ("NO_PARAMS param style — static reference data, no window at all"),
}

#: Filter column override, keyed by dataset. Default is ``published_at``;
#: ``remit`` requires ``published_at`` to derive ``timestamp_utc``
#: deterministically (``remit.py``) and drops ``published_at`` itself at
#: select, so ``timestamp_utc`` carries the same publish instant onward
#: (S-2) — no vintage-emission change needed.
PUBLICATION_WINDOW_COLUMN: dict[str, str] = {
    "remit": "timestamp_utc",
}

_DEFAULT_COLUMN = "published_at"


def publication_window_column(dataset: str) -> str:
    """Return the filter-column name for ``dataset`` (default ``published_at``)."""
    return PUBLICATION_WINDOW_COLUMN.get(dataset, _DEFAULT_COLUMN)


def publication_window_params(dataset: str) -> tuple[str, str] | None:
    """Return ``(from_param, to_param)`` for ``dataset`` if it is in scope, else ``None``.

    ``None`` for: any dataset in :data:`PUBLICATION_WINDOW_EXEMPT`; any
    dataset absent from ``ENDPOINTS`` (unregistered, e.g. ``bod``); any
    dataset whose ``param_style`` is not ``PUBLISH_DATETIME``; and any
    dataset whose ``from_param``/``to_param`` diverge from the
    ``publishDateTimeFrom``/``publishDateTimeTo`` defaults (an undocumented
    override — do not invent its semantics).
    """
    if dataset in PUBLICATION_WINDOW_EXEMPT:
        return None

    endpoint = ENDPOINTS.get(dataset)
    if endpoint is None or endpoint.param_style is not ParamStyle.PUBLISH_DATETIME:
        return None

    if endpoint.from_param != "publishDateTimeFrom" or endpoint.to_param != "publishDateTimeTo":
        return None

    return (endpoint.from_param, endpoint.to_param)
