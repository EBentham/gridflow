"""Unit tests for Open-Meteo connector, silver transformers, and schemas.

F7.5: tests cover the role-split (demand / wind / solar) at three location
lists. The double-underscore bronze-naming convention is exercised here
via the demand reingest test.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.openmeteo.client import OpenMeteoConnector
from gridflow.connectors.openmeteo.endpoints import (
    ARCHIVE_BASE_URL,
    DATASET_SPECS,
    DEMAND_HOURLY_VARS,
    DEMAND_LOCATIONS,
    SOLAR_HOURLY_VARS,
    SOLAR_LOCATIONS,
    WIND_ARCHIVE_VARS,
    WIND_FORECAST_VARS,
    WIND_LOCATIONS,
)
from gridflow.schemas.weather import DemandWeather, SolarWeather, WindWeather
from gridflow.silver.openmeteo.forecast import ForecastDemandWeather
from gridflow.silver.openmeteo.historical import (
    HistoricalDemandWeather,
    _pivot_openmeteo_json,
)
from gridflow.storage.parquet import read_parquet

FIXTURES = Path(__file__).parent.parent / "fixtures" / "openmeteo"


# ---------------------------------------------------------------------------
# Location lists
# ---------------------------------------------------------------------------

class TestDemandLocations:
    def test_seven_locations_defined(self):
        assert len(DEMAND_LOCATIONS) == 7

    def test_location_names(self):
        names = {loc.name for loc in DEMAND_LOCATIONS}
        assert "london" in names
        assert "birmingham" in names
        assert "glasgow" in names
        assert "belfast" in names

    def test_all_have_valid_coordinates(self):
        for loc in DEMAND_LOCATIONS:
            assert -90 <= loc.latitude <= 90
            assert -180 <= loc.longitude <= 180


class TestWindLocations:
    def test_twelve_capacity_weighted_sites(self):
        assert len(WIND_LOCATIONS) == 12

    def test_offshore_north_sea_present(self):
        names = {loc.name for loc in WIND_LOCATIONS}
        assert {"dogger_bank", "hornsea", "east_anglia", "triton_knoll"} <= names

    def test_no_double_underscore_in_names(self):
        # Asserts the parser invariant for the double-underscore separator.
        for loc in WIND_LOCATIONS:
            assert "__" not in loc.name


class TestSolarLocations:
    def test_six_capacity_weighted_sites(self):
        assert len(SOLAR_LOCATIONS) == 6

    def test_southern_uk_bias(self):
        # Solar capacity is south of a Bristol-Norwich line; assert all
        # solar sites are below 53° N.
        for loc in SOLAR_LOCATIONS:
            assert loc.latitude < 53.0, loc

    def test_no_double_underscore_in_names(self):
        for loc in SOLAR_LOCATIONS:
            assert "__" not in loc.name


# ---------------------------------------------------------------------------
# Variable lists
# ---------------------------------------------------------------------------

class TestVariableLists:
    def test_demand_hourly_vars(self):
        assert "temperature_2m" in DEMAND_HOURLY_VARS
        assert "wind_speed_10m" in DEMAND_HOURLY_VARS
        assert "precipitation" in DEMAND_HOURLY_VARS
        # F7.5-VARS-05: snow vars added to demand for winter peak.
        assert "snowfall" in DEMAND_HOURLY_VARS
        assert "snow_depth" in DEMAND_HOURLY_VARS

    def test_wind_archive_excludes_uninterpolated_heights(self):
        # Verified 2026-05-09 against ERA5 at Hornsea (53.88, 1.79) and
        # Whitelee (55.69, -4.27): 80m / 120m / 180m return all-null.
        assert "wind_speed_80m" not in WIND_ARCHIVE_VARS
        assert "wind_speed_120m" not in WIND_ARCHIVE_VARS
        assert "wind_speed_180m" not in WIND_ARCHIVE_VARS

    def test_wind_archive_includes_10m_and_100m(self):
        assert "wind_speed_10m" in WIND_ARCHIVE_VARS
        assert "wind_speed_100m" in WIND_ARCHIVE_VARS
        assert "wind_direction_10m" in WIND_ARCHIVE_VARS
        assert "wind_direction_100m" in WIND_ARCHIVE_VARS

    def test_wind_archive_includes_gusts_and_cloud_decomp(self):
        assert "wind_gusts_10m" in WIND_ARCHIVE_VARS
        for cc in ["cloud_cover", "cloud_cover_low",
                   "cloud_cover_mid", "cloud_cover_high"]:
            assert cc in WIND_ARCHIVE_VARS

    def test_wind_archive_includes_dew_point(self):
        # F7.5-VARS-05: dew_point_2m on wind dataset for icing risk.
        assert "dew_point_2m" in WIND_ARCHIVE_VARS

    def test_wind_forecast_includes_full_height_set(self):
        for h in ("10m", "80m", "100m", "120m", "180m"):
            assert f"wind_speed_{h}" in WIND_FORECAST_VARS, h
        for h in ("10m", "80m", "100m", "120m", "180m"):
            assert f"wind_direction_{h}" in WIND_FORECAST_VARS, h

    def test_wind_forecast_is_superset_of_archive(self):
        assert set(WIND_ARCHIVE_VARS) <= set(WIND_FORECAST_VARS)

    def test_solar_irradiance_components(self):
        # F7.5-VARS-03: full irradiance components.
        assert "shortwave_radiation" in SOLAR_HOURLY_VARS  # GHI
        assert "direct_radiation" in SOLAR_HOURLY_VARS
        assert "direct_normal_irradiance" in SOLAR_HOURLY_VARS  # DNI
        assert "diffuse_radiation" in SOLAR_HOURLY_VARS
        assert "global_tilted_irradiance" in SOLAR_HOURLY_VARS  # GTI

    def test_solar_no_surface_pressure(self):
        # Solar transformer does not derive air density (no fetch of
        # surface pressure on solar dataset).
        assert "surface_pressure" not in SOLAR_HOURLY_VARS


# ---------------------------------------------------------------------------
# DATASET_SPECS contract
# ---------------------------------------------------------------------------

class TestOpenMeteoLocationRetry:
    """_fetch_location must retry a transient error, matching every other
    connector's @RETRY_POLICY-decorated request path (issue 13).

    Pre-fix: _fetch_location issued its GET with no retry, so a single 429 /
    archive-host timeout silently dropped a capacity-weighted location from the
    run. Post-fix: a 429 followed by a 200 recovers and the location's
    RawResponse is present.

    The companion test (criterion 4) proves the opposite edge: a location that
    fails *after* retries are exhausted surfaces as a raised error from
    ``fetch`` rather than being silently dropped to a warning.
    """

    @pytest.fixture(autouse=True)
    def _instant_retry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Neutralise tenacity's exponential backoff so the 5-attempt retry path
        # runs instantly. NB: patching ``RETRY_POLICY.wait`` does not work —
        # ``RETRY_POLICY`` is the decorator closure, not the ``AsyncRetrying``
        # controller (the wait lives on the per-function ``_request.retry``
        # instance), so that ``raising=False`` setattr was a silent no-op.
        # Patching the actual sleep is the reliable, decorator-agnostic route.
        async def _no_sleep(*_args: object, **_kwargs: object) -> None:
            return None

        monkeypatch.setattr("asyncio.sleep", _no_sleep)

    def _config(self) -> SourceConfig:
        return SourceConfig(
            base_url="",
            rate_limit_per_second=100,
            timeout=5,
            datasets={"historical_demand": DatasetConfig(endpoint="/archive")},
        )

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_location_retries_429_then_succeeds(self) -> None:
        body = json.dumps({"hourly": {"time": [], "temperature_2m": []}})
        route = respx.get(url__startswith=ARCHIVE_BASE_URL).mock(
            side_effect=[
                httpx.Response(429, text="rate limited"),
                httpx.Response(200, text=body),
            ]
        )

        location = DATASET_SPECS["historical_demand"].locations[0]
        async with OpenMeteoConnector(self._config()) as connector:
            resp = await connector._fetch_location(
                "historical_demand",
                location,
                datetime(2024, 1, 15, tzinfo=UTC),
                datetime(2024, 1, 15, tzinfo=UTC),
            )

        assert route.call_count == 2, "expected one retry after the 429"
        assert resp.http_status == 200
        assert resp.dataset == f"historical_demand__{location.name}"
        assert resp.source == "open_meteo"

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_surfaces_location_failure_after_retries(self) -> None:
        """A location that fails persistently (retries exhausted) must surface
        as a raised error from ``fetch``, not be silently dropped to a warning.

        Open-Meteo locations are capacity-weighted; silently dropping one
        re-weights the run's aggregate with no error. FAILS against the pre-fix
        ``except Exception: logger.warning`` swallow, which returned the
        surviving locations and raised nothing.
        """
        spec = DATASET_SPECS["historical_demand"]
        failing = spec.locations[0]
        ok_body = json.dumps({"hourly": {"time": [], "temperature_2m": []}})

        def handler(request: httpx.Request) -> httpx.Response:
            lat = request.url.params.get("latitude")
            if lat is not None and abs(float(lat) - failing.latitude) < 1e-9:
                return httpx.Response(500, text="persistent upstream error")
            return httpx.Response(200, text=ok_body)

        respx.get(url__startswith=ARCHIVE_BASE_URL).mock(side_effect=handler)

        async with OpenMeteoConnector(self._config()) as connector:
            with pytest.raises(httpx.HTTPStatusError):
                await connector.fetch(
                    "historical_demand",
                    datetime(2024, 1, 15, tzinfo=UTC),
                    datetime(2024, 1, 15, tzinfo=UTC),
                )


class TestDatasetSpecs:
    def test_six_dataset_keys(self):
        assert set(DATASET_SPECS.keys()) == {
            "historical_demand", "historical_wind", "historical_solar",
            "forecast_demand", "forecast_wind", "forecast_solar",
        }

    def test_demand_specs_use_demand_locations(self):
        assert DATASET_SPECS["historical_demand"].locations == DEMAND_LOCATIONS
        assert DATASET_SPECS["forecast_demand"].locations == DEMAND_LOCATIONS

    def test_wind_specs_use_wind_locations(self):
        assert DATASET_SPECS["historical_wind"].locations == WIND_LOCATIONS
        assert DATASET_SPECS["forecast_wind"].locations == WIND_LOCATIONS

    def test_solar_specs_use_solar_locations(self):
        assert DATASET_SPECS["historical_solar"].locations == SOLAR_LOCATIONS
        assert DATASET_SPECS["forecast_solar"].locations == SOLAR_LOCATIONS

    def test_solar_extra_params_carry_tilt_azimuth(self):
        for ds in ("historical_solar", "forecast_solar"):
            assert DATASET_SPECS[ds].extra_params == (
                ("tilt", "35"),
                ("azimuth", "180"),
            )

    def test_non_solar_specs_have_no_extra_params(self):
        for ds in ("historical_demand", "forecast_demand",
                   "historical_wind", "forecast_wind"):
            assert DATASET_SPECS[ds].extra_params == ()

    def test_extra_params_iff_gti_in_hourly(self):
        # Contract: extra_params is non-empty if and only if global_tilted_irradiance
        # is requested. Catches a future spec-table mis-edit that attaches
        # tilt/azimuth to a non-GTI dataset.
        for ds, spec in DATASET_SPECS.items():
            has_gti = "global_tilted_irradiance" in spec.hourly
            has_extra = bool(spec.extra_params)
            assert has_gti == has_extra, ds


# ---------------------------------------------------------------------------
# Pivot helper
# ---------------------------------------------------------------------------

class TestPivotOpenMeteoJson:
    def _fixture_data(self) -> dict:
        return json.loads((FIXTURES / "historical_london_response.json").read_text())

    def test_basic_pivot(self):
        data = self._fixture_data()
        rows = _pivot_openmeteo_json(data, "london", DEMAND_HOURLY_VARS)
        assert len(rows) == 4  # 4 hourly entries

    def test_row_has_all_variables(self):
        data = self._fixture_data()
        rows = _pivot_openmeteo_json(data, "london", DEMAND_HOURLY_VARS)
        row = rows[0]
        assert row["location"] == "london"
        assert row["latitude"] == 51.5074
        assert "temperature_2m" in row
        assert "wind_speed_10m" in row

    def test_unfetched_variable_is_none(self):
        # Demand vars include snowfall but the fixture predates F7.5 — that
        # column should pivot to None rather than KeyError.
        data = self._fixture_data()
        rows = _pivot_openmeteo_json(data, "london", DEMAND_HOURLY_VARS)
        assert all(row["snowfall"] is None for row in rows)

    def test_empty_data_returns_empty(self):
        rows = _pivot_openmeteo_json({}, "london", DEMAND_HOURLY_VARS)
        assert rows == []

    def test_missing_hourly_block(self):
        rows = _pivot_openmeteo_json(
            {"latitude": 51.5, "longitude": -0.1},
            "london",
            DEMAND_HOURLY_VARS,
        )
        assert rows == []


# ---------------------------------------------------------------------------
# HistoricalDemandWeather
# ---------------------------------------------------------------------------

class TestHistoricalDemandWeather:
    def setup_method(self):
        self.t = HistoricalDemandWeather.__new__(HistoricalDemandWeather)
        self.t.data_dir = Path("/tmp/test")
        self.t.bronze_dir = Path("/tmp/test/bronze/open_meteo/historical_demand")
        self.t.silver_dir = Path("/tmp/test/silver/open_meteo/historical_demand")

    def _make_raw_df(self) -> pl.DataFrame:
        data = json.loads((FIXTURES / "historical_london_response.json").read_text())
        rows = _pivot_openmeteo_json(data, "london", DEMAND_HOURLY_VARS)
        return pl.DataFrame(rows)

    def test_dataset_name(self):
        assert HistoricalDemandWeather.dataset == "historical_demand"

    def test_dataset_version_bumped(self):
        assert HistoricalDemandWeather.DATASET_VERSION == "2.0.0"

    def test_transform_basic(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "location" in result.columns
        assert "temperature_2m_c" in result.columns  # F15-B: canonical rename
        assert result["data_provider"][0] == "open_meteo"

    def test_timestamp_is_utc_datetime(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_hdd_derived_from_temperature(self):
        # temperature_2m = 5.2, so HDD = max(0, 15.5 - 5.2) = 10.3
        # F15-B: derived column renamed hdd -> hdd_k
        raw = self._make_raw_df()
        result = self.t.transform(raw).sort("timestamp_utc")
        assert "hdd_k" in result.columns
        expected_hdd = max(0.0, 15.5 - 5.2)
        assert abs(result["hdd_k"][0] - expected_hdd) < 0.01

    def test_cdd_zero_in_winter(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert "cdd_k" in result.columns  # F15-B: renamed cdd -> cdd_k
        assert all(v == 0.0 for v in result["cdd_k"].to_list())

    def test_air_density_derived_from_pressure_and_temperature(self):
        # Fixture row 0: T=5.2°C, P=1015.2 hPa
        # rho = 101520 / (287.05 * 278.35) = 1.270 kg/m^3
        raw = self._make_raw_df()
        result = self.t.transform(raw).sort("timestamp_utc")
        assert "air_density_kg_m3" in result.columns
        first = result["air_density_kg_m3"][0]
        assert first is not None
        assert 1.25 < first < 1.30, first

    def test_dedup_on_timestamp_location(self):
        data = json.loads((FIXTURES / "historical_london_response.json").read_text())
        rows = _pivot_openmeteo_json(data, "london", DEMAND_HOURLY_VARS) * 2
        raw = pl.DataFrame(rows)
        result = self.t.transform(raw)
        assert len(result) == 4

    def test_output_columns_ordered(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result.columns[0] == "timestamp_utc"
        assert result.columns[1] == "location"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# ForecastDemandWeather
# ---------------------------------------------------------------------------

class TestForecastDemandWeather:
    def setup_method(self):
        self.t = ForecastDemandWeather.__new__(ForecastDemandWeather)
        self.t.data_dir = Path("/tmp/test")
        self.t.bronze_dir = Path("/tmp/test/bronze/open_meteo/forecast_demand")
        self.t.silver_dir = Path("/tmp/test/silver/open_meteo/forecast_demand")

    def test_dataset_attribute(self):
        assert ForecastDemandWeather.dataset == "forecast_demand"

    def test_transform_reuses_historical_logic(self):
        data = json.loads((FIXTURES / "historical_london_response.json").read_text())
        rows = _pivot_openmeteo_json(data, "london", DEMAND_HOURLY_VARS)
        raw = pl.DataFrame(rows)
        result = self.t.transform(raw)
        assert "temperature_2m_c" in result.columns  # F15-B: canonical rename
        assert "hdd_k" in result.columns  # F15-B: renamed hdd -> hdd_k

    def test_reingest_uses_forecast_location_sidecar(self, tmp_path: Path):
        target_date = date(2024, 1, 15)
        forecast_time = datetime(2024, 1, 16, 9, 30, tzinfo=UTC)
        historical_time = datetime(2024, 1, 17, 9, 30, tzinfo=UTC)
        payload = json.loads((FIXTURES / "historical_london_response.json").read_text())

        for dataset, fetched_at in [
            ("forecast_demand__london", forecast_time),
            ("historical_demand__london", historical_time),
        ]:
            bronze_dir = (
                tmp_path
                / "bronze"
                / "open_meteo"
                / dataset
                / str(target_date.year)
                / f"{target_date.month:02d}"
                / f"{target_date.day:02d}"
            )
            bronze_dir.mkdir(parents=True, exist_ok=True)
            (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
            (bronze_dir / "raw_test.meta.json").write_text(
                json.dumps(
                    {
                        "source": "open_meteo",
                        "dataset": dataset,
                        "fetched_at": fetched_at.isoformat(),
                        "data_date": target_date.isoformat(),
                    }
                )
            )

        ForecastDemandWeather(tmp_path).run(
            target_date,
            run_id="test-run-id",
            reingest=True,
        )
        df = read_parquet(
            tmp_path / "silver" / "open_meteo" / "forecast_demand" / "**" / "*.parquet"
        )

        assert set(df["available_at"].to_list()) == {forecast_time}

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# Schema tests — DemandWeather / WindWeather / SolarWeather
# ---------------------------------------------------------------------------

class _SchemaTestBase:
    """Shared test cases for the three role schemas."""

    schema_cls: type
    location_name: str
    coords: tuple[float, float]
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_minimal_record(self):
        r = self.schema_cls(
            timestamp_utc=self._TS,
            location=self.location_name,
            latitude=self.coords[0],
            longitude=self.coords[1],
        )
        assert r.location == self.location_name
        assert r.data_provider == "open_meteo"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self.schema_cls(
                timestamp_utc=datetime(2024, 1, 15, 0, 0),
                location=self.location_name,
                latitude=self.coords[0],
                longitude=self.coords[1],
            )

    def test_extra_fields_ignored(self):
        r = self.schema_cls(
            timestamp_utc=self._TS,
            location=self.location_name,
            latitude=self.coords[0],
            longitude=self.coords[1],
            extra="ignored",  # type: ignore[call-arg]
        )
        assert not hasattr(r, "extra")


class TestDemandWeatherSchema(_SchemaTestBase):
    schema_cls = DemandWeather
    location_name = "london"
    coords = (51.5074, -0.1278)

    def test_demand_specific_fields(self):
        r = DemandWeather(
            timestamp_utc=self._TS, location="london",
            latitude=51.5, longitude=-0.1,
            temperature_2m=5.2, wind_speed_10m=12.5,
            hdd=10.3, cdd=0.0,
            snowfall=0.0, snow_depth=0.0,
            air_density_kg_m3=1.27,
        )
        assert r.snowfall == 0.0
        assert r.air_density_kg_m3 == 1.27


class TestWindWeatherSchema(_SchemaTestBase):
    schema_cls = WindWeather
    location_name = "hornsea"
    coords = (53.88, 1.79)

    def test_hub_height_fields_optional(self):
        r = WindWeather(
            timestamp_utc=self._TS, location="hornsea",
            latitude=53.88, longitude=1.79,
            wind_speed_10m=8.5, wind_speed_100m=14.2,
        )
        # 80m, 120m, 180m unset — schema permits null.
        assert r.wind_speed_80m is None
        assert r.wind_speed_120m is None
        assert r.wind_speed_180m is None
        assert r.wind_speed_100m == 14.2

    def test_full_hub_height_set(self):
        r = WindWeather(
            timestamp_utc=self._TS, location="hornsea",
            latitude=53.88, longitude=1.79,
            wind_speed_10m=8.5, wind_speed_80m=12.0,
            wind_speed_100m=14.2, wind_speed_120m=15.3, wind_speed_180m=16.1,
        )
        assert r.wind_speed_180m == 16.1


class TestSolarWeatherSchema(_SchemaTestBase):
    schema_cls = SolarWeather
    location_name = "kent"
    coords = (51.20, 0.70)

    def test_irradiance_components(self):
        r = SolarWeather(
            timestamp_utc=self._TS, location="kent",
            latitude=51.2, longitude=0.7,
            shortwave_radiation=600.0,
            direct_radiation=450.0,
            direct_normal_irradiance=750.0,
            diffuse_radiation=150.0,
            global_tilted_irradiance=720.0,
        )
        assert r.shortwave_radiation == 600.0
        assert r.direct_normal_irradiance == 750.0
        assert r.global_tilted_irradiance == 720.0
