"""CKAN dataset table for the NESO Open Data Portal (D-28, D-03, D-24).

CKAN identity lives **in code, not in YAML** (D-28). ``DatasetConfig`` and
``SourceConfig`` are declared with ``extra="ignore"``, so an unrecognised YAML
key is dropped in silence — a package slug or a resource name that silently
vanished would turn into a fetch against the wrong resource with no error
anywhere. Keeping the table here also avoids a shared-model change for one
source, exactly as every other vendor keeps its endpoint table in code.

Resource selection is by **exact ``resources[].name`` string match** (D-04) and
never by UUID or by a hardcoded download URL: the raw filenames are date-stamped
and change on every refresh (``embedded-register-14-august-2026.csv``), and the
``url`` field is a 302 redirector to a presigned URL with a 7-day expiry. The
UUIDs below are deliberately absent — they are recorded as fetch-time
provenance only (D-12).
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["CKAN_ACTION_PREFIX", "DATASETS", "CkanDataset", "build_action_url"]

CKAN_ACTION_PREFIX = "/api/3/action"

_MIB = 1024 * 1024


@dataclass(frozen=True)
class CkanDataset:
    """One gridflow dataset's CKAN identity and download contract.

    Attributes:
        package: The CKAN package slug, passed as ``package_show?id=``.
        resource_name: The exact ``resources[].name`` to select (D-04). Zero
            matches or more than one is a hard error — no fuzzy match, no
            "Archive"-substring fallback, no ``last_modified`` tie-break.
        expected_format: The CKAN ``format`` the selected resource must
            declare. ``RawResponse.content_type`` is stamped from this, never
            from the response header (D-10), because the presigned host serves
            ``application/octet-stream`` and a ``.bin`` bronze body would be
            invisible to the transformer's ``raw_*.csv`` glob.
        expected_columns: The CSV header contract, enforced at fetch time by
            D-36's admission parse and again at transform time. Exact and
            ordered; drift fails loud.
        max_download_bytes: The streaming size cap (T-NDP-02). An order of
            magnitude of headroom over the observed size, so a legitimate
            publication growth does not false-refuse while a vendor-controlled
            unbounded body still cannot exhaust memory.
    """

    package: str
    resource_name: str
    expected_format: str
    expected_columns: tuple[str, ...]
    max_download_bytes: int


# The 34-column ``historic-generation-mix`` header, read verbatim from the
# Stage-A capture ``_probe/sample_historic-generation-mix.csv``. (D-24's prose
# describes it as 37 columns; the file is the cited authority and the decision's
# own instruction is "read verbatim", so the file wins — see the unit report.)
_HISTORIC_GENERATION_MIX_COLUMNS: tuple[str, ...] = (
    "DATETIME",
    "GAS",
    "COAL",
    "NUCLEAR",
    "WIND",
    "WIND_EMB",
    "HYDRO",
    "IMPORTS",
    "BIOMASS",
    "OTHER",
    "SOLAR",
    "STORAGE",
    "GENERATION",
    "CARBON_INTENSITY",
    "LOW_CARBON",
    "ZERO_CARBON",
    "RENEWABLE",
    "FOSSIL",
    "GAS_perc",
    "COAL_perc",
    "NUCLEAR_perc",
    "WIND_perc",
    "WIND_EMB_perc",
    "HYDRO_perc",
    "IMPORTS_perc",
    "BIOMASS_perc",
    "OTHER_perc",
    "SOLAR_perc",
    "STORAGE_perc",
    "GENERATION_perc",
    "LOW_CARBON_perc",
    "ZERO_CARBON_perc",
    "RENEWABLE_perc",
    "FOSSIL_perc",
)


DATASETS: dict[str, CkanDataset] = {
    "daily_wind_availability": CkanDataset(
        package="daily-wind-availability",
        resource_name="Daily Wind Availability",
        expected_format="CSV",
        expected_columns=("BMU_ID", "Date", "MW"),
        max_download_bytes=8 * _MIB,
    ),
    "historic_generation_mix": CkanDataset(
        package="historic-generation-mix",
        resource_name="Historic GB Generation Mix",
        expected_format="CSV",
        expected_columns=_HISTORIC_GENERATION_MIX_COLUMNS,
        max_download_bytes=256 * _MIB,
    ),
    "embedded_wind_solar_forecast": CkanDataset(
        package="embedded-wind-and-solar-forecasts",
        resource_name="Embedded Solar and Wind Forecast",
        expected_format="CSV",
        expected_columns=(
            "DATE_GMT",
            "TIME_GMT",
            "SETTLEMENT_DATE",
            "SETTLEMENT_PERIOD",
            "EMBEDDED_WIND_FORECAST",
            "EMBEDDED_WIND_CAPACITY",
            "EMBEDDED_SOLAR_FORECAST",
            "EMBEDDED_SOLAR_CAPACITY",
        ),
        max_download_bytes=8 * _MIB,
    ),
}


def build_action_url(action: str, **params: str) -> tuple[str, dict[str, str]]:
    """Build the path and query dict for one CKAN action call.

    Args:
        action: The CKAN action name, e.g. ``package_show``.
        **params: Query parameters, e.g. ``id="daily-wind-availability"``.

    Returns:
        A ``(path, params)`` pair. The path is relative, so httpx resolves it
        against the client's ``base_url`` — no URL is ever hand-built, and no
        URL taken from a response body is ever fetched (D-39 §1a).
    """
    return f"{CKAN_ACTION_PREFIX}/{action}", dict(params)
