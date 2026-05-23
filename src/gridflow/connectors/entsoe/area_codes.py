"""ENTSO-E area code (EIC mRID) → human-readable area name lookup.

G9 ENTSOE-03: schemas declaring `area_name` previously carried empty
strings for every row because no lookup table existed. This module
provides the canonical mapping from the EIC mRIDs in
``connectors/entsoe/endpoints.BIDDING_ZONES`` to user-friendly names.

The mapping is intentionally narrow — it covers the bidding zones the
connector actually targets via ``DEFAULT_ZONES`` / ``DEFAULT_CONTROL_AREAS``.
Codes outside this set resolve to an empty string so downstream
validators still see a `str` rather than `None`.
"""

from __future__ import annotations

# Canonical area-code → friendly-name map. Keyed by ENTSO-E EIC mRID.
# Sourced from the BIDDING_ZONES table in `connectors/entsoe/endpoints.py`;
# names match the ENTSO-E Transparency Platform's published bidding-zone
# labels.
_AREA_CODE_TO_NAME: dict[str, str] = {
    "10YGB----------A": "Great Britain",
    "10Y1001A1001A82H": "Germany / Luxembourg",
    "10YFR-RTE------C": "France",
    "10YNL----------L": "Netherlands",
    "10YBE----------2": "Belgium",
    "10YES-REE------0": "Spain",
    "10YIT-GRTN-----B": "Italy (North)",
    "10YDK-1--------W": "Denmark (DK1)",
    "10YDK-2--------M": "Denmark (DK2)",
    "10YNO-1--------2": "Norway (NO1)",
    "10Y1001A1001A44P": "Sweden (SE1)",
    "10Y1001A1001A59C": "Ireland (SEM)",
}


def area_name_for(code: str) -> str:
    """Return the friendly name for an ENTSO-E EIC mRID, or empty string.

    Args:
        code: EIC mRID (e.g. ``"10YGB----------A"``).

    Returns:
        Human-readable name (e.g. ``"Great Britain"``), or an empty
        string when the code is not in the canonical lookup. Empty
        string is returned by design — downstream schemas declare
        ``area_name: str = ""`` so the default already accepts it.
    """
    if not code:
        return ""
    return _AREA_CODE_TO_NAME.get(code, "")
