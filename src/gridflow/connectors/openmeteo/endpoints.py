"""Open-Meteo API location, variable, and dataset-spec definitions.

F7.5 split:
- Three location lists: ``DEMAND_LOCATIONS`` (population centres, unchanged
  from F0), ``WIND_LOCATIONS`` (12 capacity-weighted GB wind sites), and
  ``SOLAR_LOCATIONS`` (6 capacity-weighted GB solar sites).
- Four variable tuples: demand, wind-archive, wind-forecast, solar.
  ``WIND_ARCHIVE_VARS`` deliberately omits 80m, 120m, 180m heights — verified
  2026-05-09 against ERA5 archive at Hornsea (53.88, 1.79) and Whitelee
  (55.69, -4.27): those heights return ``units: "undefined"`` and all-null.
  ``WIND_FORECAST_VARS`` includes the wider hub-height set; the underlying
  forecast model nulls fields it does not carry.
- ``DATASET_SPECS`` — six dataset keys mapping to a frozen
  ``WeatherDatasetSpec``. Keys: ``historical_demand``, ``historical_wind``,
  ``historical_solar``, ``forecast_demand``, ``forecast_wind``,
  ``forecast_solar``.
- Per-location bronze dataset names use ``f"{dataset}__{loc.name}"`` (double
  underscore separator) to disambiguate against multi-word dataset prefixes.

Solar GTI fetches add ``tilt=35&azimuth=0`` query params, a UK fixed-tilt
representative geometry (tilt ≈ latitude minus 15 degrees, facing due south).
Open-Meteo's GTI azimuth is PV-convention (0°=South, ±180°=North), not
compass bearing — so due south is ``azimuth=0`` (see ``_SOLAR_GTI_PARAMS``).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class WeatherLocation:
    """A named geographic location to fetch weather data for."""

    name: str  # used in bronze dataset names: "historical_demand__london"
    latitude: float
    longitude: float
    timezone: str = "UTC"


@dataclass(frozen=True)
class WeatherDatasetSpec:
    """Per-dataset endpoint spec consumed by ``OpenMeteoConnector``.

    ``extra_params`` is a tuple of ``(key, value)`` pairs (not a dict) so the
    dataclass stays ``frozen=True``. The connector materialises it back to
    a dict at request time.
    """

    locations: tuple[WeatherLocation, ...]
    hourly: tuple[str, ...]
    extra_params: tuple[tuple[str, str], ...] = field(default_factory=tuple)


# Major UK population centres (preserved from F0 — used by demand model)
DEMAND_LOCATIONS: tuple[WeatherLocation, ...] = (
    WeatherLocation("london", 51.5074, -0.1278),
    WeatherLocation("birmingham", 52.4862, -1.8904),
    WeatherLocation("manchester", 53.4808, -2.2426),
    WeatherLocation("leeds", 53.8008, -1.5491),
    WeatherLocation("glasgow", 55.8642, -4.2518),
    WeatherLocation("cardiff", 51.4816, -3.1791),
    WeatherLocation("belfast", 54.5973, -5.9301),
)

# Capacity-weighted GB wind sites (approximate centroids; see ADR-020)
WIND_LOCATIONS: tuple[WeatherLocation, ...] = (
    # Offshore — southern North Sea
    WeatherLocation("dogger_bank", 54.95, 1.95),
    WeatherLocation("hornsea", 53.88, 1.79),
    WeatherLocation("east_anglia", 52.50, 2.50),
    WeatherLocation("triton_knoll", 53.45, 0.42),
    # Offshore — Irish Sea
    WeatherLocation("walney", 54.04, -3.52),
    WeatherLocation("gwynt_y_mor", 53.46, -3.59),
    # Offshore — Moray / Forth
    WeatherLocation("beatrice", 58.26, -2.89),
    WeatherLocation("seagreen", 56.59, -1.93),
    # Onshore — Scotland
    WeatherLocation("highland_central", 57.20, -4.40),
    WeatherLocation("borders_crystalrig", 55.85, -2.50),
    WeatherLocation("whitelee", 55.69, -4.27),
    # Onshore — Wales
    WeatherLocation("pen_y_cymoedd", 51.69, -3.61),
)

# Capacity-weighted GB solar sites (approximate centroids; see ADR-020)
SOLAR_LOCATIONS: tuple[WeatherLocation, ...] = (
    WeatherLocation("east_anglia_norfolk", 52.62, 1.05),
    WeatherLocation("wiltshire_somerset", 51.20, -2.50),
    WeatherLocation("kent", 51.20, 0.70),
    WeatherLocation("cornwall", 50.30, -5.00),
    WeatherLocation("sussex", 50.95, -0.10),
    WeatherLocation("oxfordshire", 51.75, -1.25),
)


# Demand variable list — F0 set + winter-peak snow variables
DEMAND_HOURLY_VARS: tuple[str, ...] = (
    "temperature_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "relative_humidity_2m",
    "precipitation",
    "shortwave_radiation",
    "surface_pressure",
    "snowfall",
    "snow_depth",
)

# Wind archive variable list — ERA5 carries 10m and 100m natively;
# 80/120/180m return all-null on archive (verified 2026-05-09).
WIND_ARCHIVE_VARS: tuple[str, ...] = (
    "temperature_2m",
    "surface_pressure",
    "wind_speed_10m",
    "wind_speed_100m",
    "wind_direction_10m",
    "wind_direction_100m",
    "wind_gusts_10m",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "dew_point_2m",
    "precipitation",
)

# Wind forecast variable list — UKMO/ECMWF carry the wider hub-height set.
# Open-Meteo nulls fields the underlying model does not carry.
WIND_FORECAST_VARS: tuple[str, ...] = WIND_ARCHIVE_VARS + (
    "wind_speed_80m",
    "wind_speed_120m",
    "wind_speed_180m",
    "wind_direction_80m",
    "wind_direction_120m",
    "wind_direction_180m",
)

# Solar variable list — same on archive and forecast.
SOLAR_HOURLY_VARS: tuple[str, ...] = (
    "temperature_2m",
    "shortwave_radiation",
    "direct_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "global_tilted_irradiance",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "snowfall",
    "snow_depth",
)


# UK fixed-tilt representative geometry: tilt 35° (≈ latitude 51-52° minus
# ~15°) facing due south. Open-Meteo's GTI azimuth is PV-convention
# (0°=South, -90°=East, +90°=West, ±180°=North) — NOT compass bearing — so
# due south is azimuth=0, not 180. Verified by live probe; see
# test_solar_gti_params_face_south. azimuth=180 would request a north-facing
# panel and silently halve GTI (OM-04).
_SOLAR_GTI_PARAMS: tuple[tuple[str, str], ...] = (
    ("tilt", "35"),
    ("azimuth", "0"),
)


DATASET_SPECS: dict[str, WeatherDatasetSpec] = {
    "historical_demand": WeatherDatasetSpec(
        DEMAND_LOCATIONS,
        DEMAND_HOURLY_VARS,
    ),
    "forecast_demand": WeatherDatasetSpec(
        DEMAND_LOCATIONS,
        DEMAND_HOURLY_VARS,
    ),
    "historical_wind": WeatherDatasetSpec(
        WIND_LOCATIONS,
        WIND_ARCHIVE_VARS,
    ),
    "forecast_wind": WeatherDatasetSpec(
        WIND_LOCATIONS,
        WIND_FORECAST_VARS,
    ),
    "historical_solar": WeatherDatasetSpec(
        SOLAR_LOCATIONS,
        SOLAR_HOURLY_VARS,
        extra_params=_SOLAR_GTI_PARAMS,
    ),
    "forecast_solar": WeatherDatasetSpec(
        SOLAR_LOCATIONS,
        SOLAR_HOURLY_VARS,
        extra_params=_SOLAR_GTI_PARAMS,
    ),
}


# Open-Meteo base URLs (archive uses a different host)
ARCHIVE_BASE_URL = "https://archive-api.open-meteo.com/v1"
FORECAST_BASE_URL = "https://api.open-meteo.com/v1"
