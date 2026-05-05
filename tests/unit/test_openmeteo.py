"""Unit tests for Open-Meteo connector, silver transformers, and schema."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from gridflow.connectors.openmeteo.endpoints import (
    HOURLY_VARIABLES,
    LOCATIONS,
)
from gridflow.schemas.weather import WeatherObservation
from gridflow.silver.openmeteo.forecast import ForecastWeatherTransformer
from gridflow.silver.openmeteo.historical import (
    HistoricalWeatherTransformer,
    _pivot_openmeteo_json,
)
from gridflow.storage.parquet import read_parquet

FIXTURES = Path(__file__).parent.parent / "fixtures" / "openmeteo"


# ---------------------------------------------------------------------------
# Endpoint definitions
# ---------------------------------------------------------------------------

class TestWeatherLocations:
    def test_seven_locations_defined(self):
        assert len(LOCATIONS) == 7

    def test_location_names(self):
        names = {loc.name for loc in LOCATIONS}
        assert "london" in names
        assert "birmingham" in names
        assert "glasgow" in names
        assert "belfast" in names

    def test_all_have_valid_coordinates(self):
        for loc in LOCATIONS:
            assert -90 <= loc.latitude <= 90
            assert -180 <= loc.longitude <= 180

    def test_hourly_variables_defined(self):
        assert "temperature_2m" in HOURLY_VARIABLES
        assert "wind_speed_10m" in HOURLY_VARIABLES
        assert "precipitation" in HOURLY_VARIABLES
        assert len(HOURLY_VARIABLES) == 7


# ---------------------------------------------------------------------------
# Pivot helper
# ---------------------------------------------------------------------------

class TestPivotOpenMeteoJson:
    def _fixture_data(self) -> dict:
        return json.loads((FIXTURES / "historical_london_response.json").read_text())

    def test_basic_pivot(self):
        data = self._fixture_data()
        rows = _pivot_openmeteo_json(data, "london")
        assert len(rows) == 4  # 4 hourly entries

    def test_row_has_all_variables(self):
        data = self._fixture_data()
        rows = _pivot_openmeteo_json(data, "london")
        row = rows[0]
        assert row["location"] == "london"
        assert row["latitude"] == 51.5074
        assert "temperature_2m" in row
        assert "wind_speed_10m" in row

    def test_empty_data_returns_empty(self):
        rows = _pivot_openmeteo_json({}, "london")
        assert rows == []

    def test_missing_hourly_block(self):
        rows = _pivot_openmeteo_json({"latitude": 51.5, "longitude": -0.1}, "london")
        assert rows == []


# ---------------------------------------------------------------------------
# HistoricalWeatherTransformer
# ---------------------------------------------------------------------------

class TestHistoricalWeatherTransformer:
    def setup_method(self):
        self.t = HistoricalWeatherTransformer.__new__(HistoricalWeatherTransformer)
        self.t.data_dir = Path("/tmp/test")
        self.t.bronze_dir = Path("/tmp/test/bronze/open_meteo/historical")
        self.t.silver_dir = Path("/tmp/test/silver/open_meteo/historical")

    def _make_raw_df(self) -> pl.DataFrame:
        data = json.loads((FIXTURES / "historical_london_response.json").read_text())
        rows = _pivot_openmeteo_json(data, "london")
        return pl.DataFrame(rows)

    def test_transform_basic(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "location" in result.columns
        assert "temperature_2m" in result.columns
        assert result["data_provider"][0] == "open_meteo"

    def test_timestamp_is_utc_datetime(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_hdd_derived_from_temperature(self):
        """HDD = max(0, 15.5 - temperature_2m)."""
        # temperature_2m = 5.2, so HDD = 15.5 - 5.2 = 10.3
        raw = self._make_raw_df()
        result = self.t.transform(raw).sort("timestamp_utc")
        assert "hdd" in result.columns
        expected_hdd = max(0.0, 15.5 - 5.2)
        assert abs(result["hdd"][0] - expected_hdd) < 0.01

    def test_cdd_zero_in_winter(self):
        """CDD should be 0 for cold UK winter temperatures."""
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert "cdd" in result.columns
        assert all(v == 0.0 for v in result["cdd"].to_list())

    def test_dedup_on_timestamp_location(self):
        """Duplicate (timestamp, location) rows are deduplicated."""
        data = json.loads((FIXTURES / "historical_london_response.json").read_text())
        rows = _pivot_openmeteo_json(data, "london") * 2  # duplicate all rows
        raw = pl.DataFrame(rows)
        result = self.t.transform(raw)
        assert len(result) == 4  # original 4 unique timestamps

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
# ForecastWeatherTransformer
# ---------------------------------------------------------------------------

class TestForecastWeatherTransformer:
    def setup_method(self):
        self.t = ForecastWeatherTransformer.__new__(ForecastWeatherTransformer)
        self.t.data_dir = Path("/tmp/test")
        self.t.bronze_dir = Path("/tmp/test/bronze/open_meteo/forecast")
        self.t.silver_dir = Path("/tmp/test/silver/open_meteo/forecast")

    def test_transform_reuses_historical_logic(self):
        """ForecastWeatherTransformer should use the same transform logic."""
        data = json.loads((FIXTURES / "historical_london_response.json").read_text())
        rows = _pivot_openmeteo_json(data, "london")
        raw = pl.DataFrame(rows)
        result = self.t.transform(raw)
        assert "temperature_2m" in result.columns
        assert "hdd" in result.columns

    def test_dataset_attribute(self):
        assert ForecastWeatherTransformer.dataset == "forecast"

    def test_reingest_uses_forecast_location_sidecar(self, tmp_path: Path):
        target_date = date(2024, 1, 15)
        forecast_time = datetime(2024, 1, 16, 9, 30, tzinfo=UTC)
        historical_time = datetime(2024, 1, 17, 9, 30, tzinfo=UTC)
        payload = json.loads((FIXTURES / "historical_london_response.json").read_text())

        for dataset, fetched_at in [
            ("forecast_london", forecast_time),
            ("historical_london", historical_time),
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

        ForecastWeatherTransformer(tmp_path).run(
            target_date,
            run_id="test-run-id",
            reingest=True,
        )
        df = read_parquet(tmp_path / "silver" / "open_meteo" / "forecast" / "**" / "*.parquet")

        assert set(df["available_at"].to_list()) == {forecast_time}

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# WeatherObservation schema
# ---------------------------------------------------------------------------

class TestWeatherObservationSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = WeatherObservation(
            timestamp_utc=self._TS,
            location="london",
            latitude=51.5074,
            longitude=-0.1278,
            temperature_2m=5.2,
            wind_speed_10m=12.5,
            hdd=10.3,
            cdd=0.0,
        )
        assert r.location == "london"
        assert r.data_provider == "open_meteo"

    def test_all_weather_fields_optional(self):
        r = WeatherObservation(
            timestamp_utc=self._TS,
            location="glasgow",
            latitude=55.8,
            longitude=-4.2,
        )
        assert r.temperature_2m is None
        assert r.hdd is None
        assert r.cdd is None

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            WeatherObservation(
                timestamp_utc=datetime(2024, 1, 15, 0, 0),  # naive
                location="london",
                latitude=51.5,
                longitude=-0.1,
            )

    def test_extra_fields_ignored(self):
        r = WeatherObservation(
            timestamp_utc=self._TS,
            location="london",
            latitude=51.5,
            longitude=-0.1,
            extra="ignored",  # type: ignore[call-arg]
        )
        assert not hasattr(r, "extra")
