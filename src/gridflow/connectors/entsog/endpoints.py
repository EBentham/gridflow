"""ENTSO-G Transparency Platform API endpoint constants."""

from __future__ import annotations

# ENTSO-G Transparency Platform API
ENTSOG_API_PATH = "/operationaldata"

# Standard query parameters for physical flow data
PHYSICAL_FLOW_INDICATOR = "Physical Flow"
DEFAULT_PERIOD_TYPE = "day"

# ENTSO-G requires "UCT" not "UTC" in their timezone parameter
ENTSOG_TIMEZONE = "UCT"

# Return all records in a single page
ENTSOG_ALL_RECORDS_LIMIT = -1

# Key border interconnection points (by pointKey) — GB-centric
# These are the main entry/exit points connecting GB to Europe
KEY_POINT_KEYS: list[str] = [
    "IUK",       # Interconnector UK (GB<->BE)
    "BBL",       # BBL Pipeline (GB<->NL)
    "FRAN",      # France interconnection
    "IRL",       # Ireland (Moffat)
    "NIRL",      # Northern Ireland interconnection
    "NORI",      # Norway (FLAGS, VESTERLED, FRIGG)
]
