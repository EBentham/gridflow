"""Unit tests for GIE AGSI+/ALSI connector, schemas, and silver transformers."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from gridflow.connectors.gie.endpoints import AGSI_COUNTRIES, ALSI_COUNTRIES
from gridflow.schemas.gie import GasStorage, LNGTerminal
from gridflow.silver.gie.agsi import GasStorageTransformer
from gridflow.silver.gie.alsi import LNGTerminalTransformer

FIXTURES = Path(__file__).parent.parent / "fixtures" / "gie"


# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------


class TestGieEndpoints:
    def test_agsi_countries_not_empty(self):
        assert len(AGSI_COUNTRIES) > 0

    def test_alsi_countries_not_empty(self):
        assert len(ALSI_COUNTRIES) > 0

    def test_gb_in_agsi_countries(self):
        assert "GB" in AGSI_COUNTRIES

    def test_gb_in_alsi_countries(self):
        assert "GB" in ALSI_COUNTRIES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_transformer(cls):
    t = cls.__new__(cls)
    ds = cls.dataset
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path(f"/tmp/test/bronze/{cls.source}/{ds}")
    t.silver_dir = Path(f"/tmp/test/silver/{cls.source}/{ds}")
    return t


def _load_fixture_records(filename: str) -> list[dict]:
    payload = json.loads((FIXTURES / filename).read_text())
    return payload.get("data", [])


# ---------------------------------------------------------------------------
# GasStorageTransformer (AGSI)
# ---------------------------------------------------------------------------


class TestGasStorageTransformer:
    def setup_method(self):
        self.t = _make_transformer(GasStorageTransformer)

    def _make_raw_df(self) -> pl.DataFrame:
        records = _load_fixture_records("agsi_gb_response.json")
        return pl.DataFrame(records)

    def test_transform_basic(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "gas_day" in result.columns
        assert "gas_in_storage_gwh" in result.columns

    def test_gas_day_dtype(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["gas_day"].dtype == pl.Date

    def test_country_code_populated(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert "country_code" in result.columns
        assert result["country_code"][0] == "GB"

    def test_storage_pct_full(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw).sort("gas_day", descending=True)
        assert "storage_pct_full" in result.columns
        assert abs(result["storage_pct_full"][0] - 81.4) < 0.1

    def test_numeric_columns_are_float(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["gas_in_storage_gwh"].dtype == pl.Float64
        assert result["withdrawal_gwh"].dtype == pl.Float64

    def test_data_provider(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "gie_agsi"

    def test_three_records(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert len(result) == 3

    def test_dedup(self):
        raw = self._make_raw_df()
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 3  # deduplicated back to 3

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_column_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_sorted_by_gas_day(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        days = result["gas_day"].to_list()
        assert days == sorted(days)


# ---------------------------------------------------------------------------
# LNGTerminalTransformer (ALSI)
# ---------------------------------------------------------------------------


class TestLNGTerminalTransformer:
    def setup_method(self):
        self.t = _make_transformer(LNGTerminalTransformer)

    def _make_raw_df(self) -> pl.DataFrame:
        records = _load_fixture_records("alsi_gb_response.json")
        return pl.DataFrame(records)

    def test_transform_basic(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "gas_day" in result.columns
        assert "lng_in_storage_gwh" in result.columns

    def test_gas_day_dtype(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["gas_day"].dtype == pl.Date

    def test_country_code_populated(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["country_code"][0] == "GB"

    def test_data_provider(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "gie_alsi"

    def test_two_records(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert len(result) == 2

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_sorted_by_gas_day(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        days = result["gas_day"].to_list()
        assert days == sorted(days)


# ---------------------------------------------------------------------------
# GasStorage schema
# ---------------------------------------------------------------------------


class TestGasStorageSchema:
    _DATE = date(2024, 1, 15)

    def test_valid_record(self):
        r = GasStorage(
            gas_day=self._DATE,
            country_code="GB",
            gas_in_storage_gwh=28500.5,
            storage_pct_full=81.4,
        )
        assert r.data_provider == "gie_agsi"
        assert r.country_code == "GB"

    def test_optional_fields_default_none(self):
        r = GasStorage(gas_day=self._DATE, country_code="GB")
        assert r.gas_in_storage_gwh is None
        assert r.withdrawal_gwh is None
        assert r.storage_pct_full is None

    def test_pct_full_clamped(self):
        r = GasStorage(gas_day=self._DATE, country_code="GB", storage_pct_full=110.0)
        assert r.storage_pct_full == 100.0

    def test_negative_pct_clamped(self):
        r = GasStorage(gas_day=self._DATE, country_code="GB", storage_pct_full=-5.0)
        assert r.storage_pct_full == 0.0


# ---------------------------------------------------------------------------
# LNGTerminal schema
# ---------------------------------------------------------------------------


class TestLNGTerminalSchema:
    _DATE = date(2024, 1, 15)

    def test_valid_record(self):
        r = LNGTerminal(
            gas_day=self._DATE,
            country_code="GB",
            lng_in_storage_gwh=5200.4,
            dtrs_pct_full=74.3,
        )
        assert r.data_provider == "gie_alsi"
        assert r.country_code == "GB"

    def test_optional_fields_default_none(self):
        r = LNGTerminal(gas_day=self._DATE, country_code="GB")
        assert r.lng_in_storage_gwh is None
        assert r.dtrs_pct_full is None

    def test_pct_full_clamped_high(self):
        r = LNGTerminal(gas_day=self._DATE, country_code="GB", dtrs_pct_full=105.0)
        assert r.dtrs_pct_full == 100.0
