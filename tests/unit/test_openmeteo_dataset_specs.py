"""Contract tests for the openmeteo ``DATASET_SPECS`` table.

These assert structural invariants that the connector and transformers
both rely on. If a future edit breaks one of them, the failure is loud
and immediate.
"""

from __future__ import annotations

from gridflow.connectors.openmeteo.endpoints import (
    DATASET_SPECS,
    DEMAND_LOCATIONS,
    SOLAR_LOCATIONS,
    WIND_ARCHIVE_VARS,
    WIND_FORECAST_VARS,
    WIND_LOCATIONS,
)


EXPECTED_DATASET_KEYS = {
    "historical_demand",
    "historical_wind",
    "historical_solar",
    "forecast_demand",
    "forecast_wind",
    "forecast_solar",
}


def test_six_dataset_keys_present() -> None:
    assert set(DATASET_SPECS.keys()) == EXPECTED_DATASET_KEYS


def test_demand_specs_use_demand_locations() -> None:
    for ds in ("historical_demand", "forecast_demand"):
        assert DATASET_SPECS[ds].locations == DEMAND_LOCATIONS


def test_wind_specs_use_wind_locations() -> None:
    for ds in ("historical_wind", "forecast_wind"):
        assert DATASET_SPECS[ds].locations == WIND_LOCATIONS


def test_solar_specs_use_solar_locations() -> None:
    for ds in ("historical_solar", "forecast_solar"):
        assert DATASET_SPECS[ds].locations == SOLAR_LOCATIONS


def test_wind_archive_and_forecast_vars_differ() -> None:
    # The archive list deliberately omits hub heights ERA5 doesn't carry.
    # The forecast list includes them. Catches a future mis-edit that
    # accidentally collapses the two.
    assert set(WIND_ARCHIVE_VARS) < set(WIND_FORECAST_VARS)
    diff = set(WIND_FORECAST_VARS) - set(WIND_ARCHIVE_VARS)
    assert {"wind_speed_80m", "wind_speed_120m", "wind_speed_180m"} <= diff


def test_no_location_name_contains_double_underscore() -> None:
    # Double underscore is the bronze-naming separator
    # (``f"{dataset}__{loc.name}"``). A location name containing ``__``
    # would make the resulting bronze identifier ambiguous to parse.
    for locations in (DEMAND_LOCATIONS, WIND_LOCATIONS, SOLAR_LOCATIONS):
        for loc in locations:
            assert "__" not in loc.name, loc.name


def test_extra_params_iff_gti_in_hourly() -> None:
    # tilt/azimuth are only meaningful when global_tilted_irradiance is
    # requested. Catches a future mis-edit that attaches the params to
    # a non-GTI dataset (Open-Meteo would 400 the request).
    for ds, spec in DATASET_SPECS.items():
        has_gti = "global_tilted_irradiance" in spec.hourly
        has_extra = bool(spec.extra_params)
        assert has_gti == has_extra, ds


def test_solar_extra_params_are_uk_fixed_tilt() -> None:
    expected = (("tilt", "35"), ("azimuth", "180"))
    assert DATASET_SPECS["historical_solar"].extra_params == expected
    assert DATASET_SPECS["forecast_solar"].extra_params == expected


def test_dataset_keys_split_cleanly_by_prefix() -> None:
    historical = {k for k in DATASET_SPECS if k.startswith("historical_")}
    forecast = {k for k in DATASET_SPECS if k.startswith("forecast_")}
    assert len(historical) == 3
    assert len(forecast) == 3
    assert historical | forecast == EXPECTED_DATASET_KEYS
