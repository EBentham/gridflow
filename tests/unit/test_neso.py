"""Unit tests for NESO Carbon Intensity connector, schema, and transformer."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from gridflow.schemas.neso import CarbonIntensity
from gridflow.silver.neso.carbon_intensity import CarbonIntensityTransformer

FIXTURES = Path(__file__).parent.parent / "fixtures" / "neso"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_transformer() -> CarbonIntensityTransformer:
    t = CarbonIntensityTransformer.__new__(CarbonIntensityTransformer)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/neso/carbon_intensity")
    t.silver_dir = Path("/tmp/test/silver/neso/carbon_intensity")
    return t


def _load_fixture_records() -> list[dict]:
    payload = json.loads((FIXTURES / "carbon_intensity_response.json").read_text())
    raw_records = payload.get("data", [])
    rows = []
    for record in raw_records:
        intensity = record.get("intensity", {}) or {}
        rows.append(
            {
                "from": record.get("from"),
                "to": record.get("to"),
                "forecast": intensity.get("forecast"),
                "actual": intensity.get("actual"),
                "index": intensity.get("index", ""),
            }
        )
    return rows


def _make_raw_df() -> pl.DataFrame:
    return pl.DataFrame(_load_fixture_records())


# ---------------------------------------------------------------------------
# CarbonIntensityTransformer
# ---------------------------------------------------------------------------


class TestCarbonIntensityTransformer:
    def setup_method(self):
        self.t = _make_transformer()

    def test_transform_basic(self):
        result = self.t.transform(_make_raw_df())
        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "forecast_gco2_kwh" in result.columns
        assert "actual_gco2_kwh" in result.columns

    def test_timestamp_dtype(self):
        result = self.t.transform(_make_raw_df())
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_first_timestamp(self):
        result = self.t.transform(_make_raw_df()).sort("timestamp_utc")
        expected = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        assert result["timestamp_utc"][0] == expected

    def test_half_hour_intervals(self):
        result = self.t.transform(_make_raw_df()).sort("timestamp_utc")
        ts = result["timestamp_utc"].to_list()
        delta = ts[1] - ts[0]
        assert delta.total_seconds() == 30 * 60

    def test_four_records(self):
        result = self.t.transform(_make_raw_df())
        assert len(result) == 4

    def test_forecast_values(self):
        result = self.t.transform(_make_raw_df()).sort("timestamp_utc")
        assert abs(result["forecast_gco2_kwh"][0] - 245.0) < 0.1
        assert abs(result["forecast_gco2_kwh"][1] - 250.0) < 0.1

    def test_actual_null_for_forecast_only(self):
        result = self.t.transform(_make_raw_df()).sort("timestamp_utc")
        # First two records have actual, last two are null
        assert result["actual_gco2_kwh"][0] is not None
        assert result["actual_gco2_kwh"][2] is None

    def test_intensity_index_populated(self):
        result = self.t.transform(_make_raw_df()).sort("timestamp_utc")
        assert result["intensity_index"][0] == "moderate"
        assert result["intensity_index"][2] == "high"

    def test_data_provider(self):
        result = self.t.transform(_make_raw_df())
        assert all(v == "neso" for v in result["data_provider"].to_list())

    def test_dedup(self):
        raw = _make_raw_df()
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_sorted_output(self):
        result = self.t.transform(_make_raw_df())
        ts = result["timestamp_utc"].to_list()
        assert ts == sorted(ts)

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_column_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# CarbonIntensity schema
# ---------------------------------------------------------------------------


class TestCarbonIntensitySchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = CarbonIntensity(
            timestamp_utc=self._TS,
            forecast_gco2_kwh=245.0,
            actual_gco2_kwh=239.0,
            intensity_index="moderate",
        )
        assert r.data_provider == "neso"
        assert r.forecast_gco2_kwh == 245.0

    def test_optional_fields_none(self):
        r = CarbonIntensity(timestamp_utc=self._TS)
        assert r.forecast_gco2_kwh is None
        assert r.actual_gco2_kwh is None
        assert r.intensity_index == ""

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            CarbonIntensity(
                timestamp_utc=datetime(2024, 1, 15, 0, 0),  # naive
            )

    def test_data_provider_default(self):
        r = CarbonIntensity(timestamp_utc=self._TS)
        assert r.data_provider == "neso"
