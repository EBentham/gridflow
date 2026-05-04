"""Unit tests for ENTSO-G connector, schemas, and silver transformers."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

import gridflow.silver.entsog  # noqa: F401
from gridflow.config.settings import load_settings
from gridflow.connectors.entsog.endpoints import (
    DEFAULT_POINT_DIRECTIONS,
    ENDPOINTS,
    ENTSOG_ALL_RECORDS_LIMIT,
    ENTSOG_API_PATH,
    ENTSOG_TIMEZONE,
    ENTSOG_TIMEZONE_PARAM,
    OPERATIONAL_INDICATORS,
    PHYSICAL_FLOW_INDICATOR,
    build_params,
)
from gridflow.schemas.entsog import EntsogPhysicalFlow
from gridflow.silver.entsog.physical_flows import (
    PhysicalFlowsTransformer,
    _normalise_to_gwh_day,
)
from gridflow.silver.registry import list_transformers

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsog"
START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------


class TestEntsogEndpoints:
    def test_api_path_is_operational_data(self):
        assert ENTSOG_API_PATH == "/operationalData"

    def test_timezone_is_uct(self):
        assert ENTSOG_TIMEZONE == "UCT"
        assert ENTSOG_TIMEZONE_PARAM == "timeZone"

    def test_limit_is_minus_one(self):
        assert ENTSOG_ALL_RECORDS_LIMIT == -1

    def test_physical_flow_indicator(self):
        assert PHYSICAL_FLOW_INDICATOR == "Physical Flow"

    def test_active_inventory_matches_config_and_silver_registry(self):
        configured = set(load_settings().get_source_config("entsog").datasets)
        endpoint_datasets = set(ENDPOINTS)
        transformers = {dataset for _, dataset in list_transformers("entsog")}

        assert endpoint_datasets == configured
        assert endpoint_datasets <= transformers

    def test_operational_indicator_values_are_exact_case(self):
        assert OPERATIONAL_INDICATORS["nominations"] == "Nomination"
        assert OPERATIONAL_INDICATORS["methane_content"] == "Methane Content"
        assert OPERATIONAL_INDICATORS["hydrogen_content"] == "Hydrogen Content"
        assert OPERATIONAL_INDICATORS["oxygen_content"] == "Oxygen Content"

    def test_build_params_for_operational_dataset(self):
        endpoint = ENDPOINTS["physical_flows"]
        params = build_params(endpoint, start=START, end=END)

        assert params["from"] == "2024-01-15"
        assert params["to"] == "2024-01-16"
        assert params["indicator"] == "Physical Flow"
        assert params["periodType"] == "day"
        assert params["timeZone"] == "UCT"
        assert params["limit"] == -1
        assert params["pointDirection"] == ",".join(DEFAULT_POINT_DIRECTIONS)

    def test_build_params_allows_live_limit_override(self):
        endpoint = ENDPOINTS["operators"]
        params = build_params(endpoint, start=START, end=END, limit=1)

        assert "from" not in params
        assert "to" not in params
        assert params["hasData"] == 1
        assert params["limit"] == 1


# ---------------------------------------------------------------------------
# Unit normalisation helper
# ---------------------------------------------------------------------------


class TestNormaliseToGwhDay:
    def test_kwh_d_to_gwh_d(self):
        # 1e6 kWh/d = 1 GWh/d
        assert abs(_normalise_to_gwh_day(1_000_000.0, "kWh/d") - 1.0) < 1e-9

    def test_kwh_h_to_gwh_d(self):
        # 1e6 kWh/h * 24 = 24 GWh/d
        assert abs(_normalise_to_gwh_day(1_000_000.0, "kWh/h") - 24.0) < 1e-9

    def test_default_assumed_kwh_d(self):
        # Unknown unit falls back to kWh/d
        assert abs(_normalise_to_gwh_day(1_000_000.0, "unknown") - 1.0) < 1e-9

    def test_zero_value(self):
        assert _normalise_to_gwh_day(0.0, "kWh/d") == 0.0

    def test_large_value(self):
        # 15 billion kWh/d = 15,000 GWh/d
        result = _normalise_to_gwh_day(15_000_000_000.0, "kWh/d")
        assert abs(result - 15_000.0) < 0.01


# ---------------------------------------------------------------------------
# Helper to build transformer instances bypassing __init__
# ---------------------------------------------------------------------------


def _make_transformer() -> PhysicalFlowsTransformer:
    t = PhysicalFlowsTransformer.__new__(PhysicalFlowsTransformer)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/entsog/physical_flows")
    t.silver_dir = Path("/tmp/test/silver/entsog/physical_flows")
    return t


def _load_fixture_df() -> pl.DataFrame:
    payload = json.loads((FIXTURES / "physical_flows_response.json").read_text())
    records = payload.get("operationalData", [])
    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# PhysicalFlowsTransformer
# ---------------------------------------------------------------------------


class TestPhysicalFlowsTransformer:
    def setup_method(self):
        self.t = _make_transformer()

    def test_transform_basic(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "point_key" in result.columns
        assert "flow_gwh_per_day" in result.columns

    def test_filters_to_physical_flow_only(self):
        """Non-Physical-Flow records (e.g. 'Other Indicator') are excluded."""
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        # Fixture has 3 Physical Flow records + 1 'Other Indicator' (NORI)
        assert len(result) == 3

    def test_timestamp_dtype(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_timestamp_value(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw).sort("timestamp_utc", "point_key")
        expected = datetime(2024, 1, 15, 6, 0, 0, tzinfo=UTC)
        assert result["timestamp_utc"][0] == expected

    def test_kwh_d_normalised_to_gwh_d(self):
        """15_000_000_000 kWh/d becomes 15,000 GWh/d for IUK entry."""
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        iuk_entry = result.filter(
            (pl.col("point_key") == "IUK") & (pl.col("direction_key") == "entry")
        )
        assert len(iuk_entry) == 1
        assert abs(iuk_entry["flow_gwh_per_day"][0] - 15_000.0) < 0.01

    def test_point_key_preserved(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        point_keys = set(result["point_key"].to_list())
        assert "IUK" in point_keys
        assert "BBL" in point_keys

    def test_direction_key_preserved(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        assert "direction_key" in result.columns
        directions = set(result["direction_key"].to_list())
        assert "entry" in directions
        assert "exit" in directions

    def test_data_provider(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        assert all(v == "entsog" for v in result["data_provider"].to_list())

    def test_dedup(self):
        raw = _load_fixture_df()
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 3

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_sorted_output(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        ts_list = result["timestamp_utc"].to_list()
        assert ts_list == sorted(ts_list)


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestEntsogPhysicalFlowSchema:
    _TS = datetime(2024, 1, 15, 6, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsogPhysicalFlow(
            timestamp_utc=self._TS,
            point_key="IUK",
            point_label="Interconnector UK",
            direction_key="entry",
            flow_gwh_per_day=15_000.0,
        )
        assert r.data_provider == "entsog"
        assert r.flow_gwh_per_day == 15_000.0

    def test_optional_fields_have_defaults(self):
        r = EntsogPhysicalFlow(timestamp_utc=self._TS, point_key="BBL")
        assert r.point_label == ""
        assert r.direction_key == ""
        assert r.flow_gwh_per_day == 0.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsogPhysicalFlow(
                timestamp_utc=datetime(2024, 1, 15, 6, 0, 0),  # naive
                point_key="IUK",
            )
