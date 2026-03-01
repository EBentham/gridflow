"""GIE AGSI+ / ALSI API endpoint constants."""

from __future__ import annotations

# GIE API path (same for AGSI and ALSI)
GIE_API_PATH = "/api"

# Countries to fetch for AGSI (gas storage) — EU + GB focus
AGSI_COUNTRIES: list[str] = ["AT", "BE", "DE", "ES", "FR", "GB", "IT", "NL", "PL"]

# Countries to fetch for ALSI (LNG terminals) — EU + GB LNG importers
ALSI_COUNTRIES: list[str] = ["BE", "ES", "FR", "GB", "IT", "NL", "PL", "PT"]

# GIE pagination defaults
DEFAULT_PAGE_SIZE = 300
