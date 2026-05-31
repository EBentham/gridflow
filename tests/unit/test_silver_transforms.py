"""Unit tests for silver-layer transformers."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from gridflow.silver.elexon.agpt import AGPTTransformer
from gridflow.silver.elexon.agws import AGWSTransformer
from gridflow.silver.elexon.atl import ATLTransformer
from gridflow.silver.elexon.bmunits import BMUnitsTransformer
from gridflow.silver.elexon.boal import BOALTransformer
from gridflow.silver.elexon.bod import BODTransformer
from gridflow.silver.elexon.demand_forecast import DemandForecastTransformer, NDFDTransformer
from gridflow.silver.elexon.disbsad import DISBSADTransformer
from gridflow.silver.elexon.fou2t14d import FOU2T14DTransformer
from gridflow.silver.elexon.freq import FreqTransformer
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.fuelinst import FuelInstTransformer
from gridflow.silver.elexon.imbalngc import ImbalNGCTransformer
from gridflow.silver.elexon.inddem import INDDEMTransformer
from gridflow.silver.elexon.indgen import INDGENTransformer
from gridflow.silver.elexon.indo import INDOTransformer
from gridflow.silver.elexon.indod import INDODTransformer
from gridflow.silver.elexon.itsdo import ITSDOTransformer
from gridflow.silver.elexon.lolpdrm import LOLPDRMTransformer
from gridflow.silver.elexon.melngc import MelNGCTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.netbsad import NETBSADTransformer
from gridflow.silver.elexon.nonbm import NONBMTransformer
from gridflow.silver.elexon.pn import PNTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.elexon.temp import TempTransformer
from gridflow.silver.elexon.tsdfd import TSDFDTransformer
from gridflow.silver.elexon.uou2t14d import UOU2T14DTransformer
from gridflow.silver.elexon.wind_forecast import WindForecastTransformer
from gridflow.utils.time import settlement_period_to_utc

FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"


def _make_transformer(cls, dataset: str | None = None):
    """Instantiate a transformer bypassing __init__ (data_dir not needed for transform())."""
    t = cls.__new__(cls)
    ds = dataset or cls.dataset
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path(f"/tmp/test/bronze/elexon/{ds}")
    t.silver_dir = Path(f"/tmp/test/silver/elexon/{ds}")
    return t


class TestSystemPriceTransformer:
    def setup_method(self):
        """Fresh transformer for each test (data_dir doesn't matter for transform())."""
        self.transformer = SystemPriceTransformer.__new__(SystemPriceTransformer)
        self.transformer.data_dir = Path("/tmp/test")
        self.transformer.bronze_dir = Path("/tmp/test/bronze/elexon/system_prices")
        self.transformer.silver_dir = Path("/tmp/test/silver/elexon/system_prices")

    def _make_raw_df(self, records: list[dict]) -> pl.DataFrame:
        return pl.DataFrame(records)

    def test_transform_basic(self):
        """Basic transform with standard field names."""
        raw = self._make_raw_df(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "systemSellPrice": 45.50,
                    "systemBuyPrice": 55.00,
                    "netImbalanceVolume": -120.5,
                    "settlementRunType": "SF",
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 2,
                    "systemSellPrice": 46.75,
                    "systemBuyPrice": 56.25,
                    "netImbalanceVolume": 80.3,
                    "settlementRunType": "SF",
                },
            ]
        )
        result = self.transformer.transform(raw)

        assert len(result) == 2
        assert "timestamp_utc" in result.columns
        assert "system_sell_price" in result.columns
        assert "data_provider" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_run_type_resolution(self):
        """Later run types should supersede earlier ones."""
        raw = self._make_raw_df(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "systemSellPrice": 44.00,
                    "systemBuyPrice": 54.00,
                    "netImbalanceVolume": -115.0,
                    "settlementRunType": "II",
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "systemSellPrice": 45.50,
                    "systemBuyPrice": 55.00,
                    "netImbalanceVolume": -120.5,
                    "settlementRunType": "SF",
                },
            ]
        )
        result = self.transformer.transform(raw)

        # Should keep SF (precedence 2) over II (precedence 1)
        assert len(result) == 1
        assert result["run_type"][0] == "SF"
        assert result["system_sell_price"][0] == 45.50

    def test_empty_input(self):
        """Empty DataFrame should return empty DataFrame."""
        raw = pl.DataFrame()
        result = self.transformer.transform(raw)
        assert result.is_empty()

    def test_price_derivation_code_maps_to_own_column(self):
        """V2-FIX-04: live `/balancing/settlement/system-prices/{date}`
        returns priceDerivationCode in {'N','P'}. Pre-V2 the silver
        renamed it to `run_type` and the Pydantic regex
        `^(II|SF|R[1-3]|RF|DF)$` rejected it. Post-V2 it lands in a
        dedicated `price_derivation_code` column with no constraint;
        `run_type` stays absent because this endpoint exposes no
        run-type field."""
        raw = self._make_raw_df(
            [
                {
                    "settlementDate": "2026-05-06",
                    "settlementPeriod": 1,
                    "systemSellPrice": 96.79,
                    "systemBuyPrice": 96.79,
                    "netImbalanceVolume": -37.99,
                    "priceDerivationCode": "N",
                },
                {
                    "settlementDate": "2026-05-06",
                    "settlementPeriod": 2,
                    "systemSellPrice": 92.10,
                    "systemBuyPrice": 92.10,
                    "netImbalanceVolume": 12.5,
                    "priceDerivationCode": "P",
                },
            ]
        )
        result = self.transformer.transform(raw)

        assert "price_derivation_code" in result.columns, (
            "priceDerivationCode must map to a dedicated "
            "`price_derivation_code` column, not `run_type`"
        )
        assert "run_type" not in result.columns, (
            "this endpoint exposes no run-type field — "
            "`run_type` must not be populated from priceDerivationCode"
        )
        codes = sorted(result["price_derivation_code"].to_list())
        assert codes == ["N", "P"]

    def test_missing_columns_returns_empty(self):
        """Missing required columns should return empty DataFrame."""
        raw = self._make_raw_df([{"foo": "bar"}])
        result = self.transformer.transform(raw)
        assert result.is_empty()

    def test_timestamp_utc_winter(self):
        """SP1 on a winter date should be 00:00 UTC."""
        raw = self._make_raw_df(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "systemSellPrice": 45.50,
                    "systemBuyPrice": 55.00,
                    "netImbalanceVolume": 0.0,
                    "settlementRunType": "SF",
                },
            ]
        )
        result = self.transformer.transform(raw)
        ts = result["timestamp_utc"][0]
        # SP1 on winter day = 00:00 UTC
        assert ts.hour == 0
        assert ts.minute == 0


# ---------------------------------------------------------------------------
# FuelHH
# ---------------------------------------------------------------------------


class TestFuelHHTransformer:
    def setup_method(self):
        self.t = _make_transformer(FuelHHTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "fuelhh_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "fuel_type" in result.columns
        assert "generation_mw" in result.columns
        assert "timestamp_utc" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_fuel_type(self):
        """Duplicate (date, period, fuel_type) rows should be deduplicated."""
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "fuelType": "CCGT",
                    "generation": 12000.0,
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "fuelType": "CCGT",
                    "generation": 12500.0,
                },  # duplicate
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_timestamp_sp1_winter(self):
        """SP1 on winter date maps to 00:00 UTC."""
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "fuelType": "WIND",
                    "generation": 3000.0,
                },
            ]
        )
        result = self.t.transform(raw)
        ts = result["timestamp_utc"][0]
        assert ts.hour == 0
        assert ts.minute == 0

    def test_published_at_emitted_when_bronze_carries_it(self):
        """G5-W2.2: ElexonFuelHH.published_at is declared in the schema and
        is the documented PIT field. Before G5 the rename map produced it
        from publishDateTime but output_cols dropped it before write — schema
        and silver disagreed. With the fix, published_at survives as a
        UTC-aware datetime when bronze carries publishDateTime."""
        data = json.loads((FIXTURES / "fuelhh_response_v2.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "published_at" in result.columns, (
            "G5-W2.2 regression: published_at dropped before write"
        )
        assert result["published_at"].null_count() == 0
        assert result["published_at"].dtype == pl.Datetime("us", "UTC")


# ---------------------------------------------------------------------------
# BOAL
# ---------------------------------------------------------------------------


class TestBOALTransformer:
    def setup_method(self):
        self.t = _make_transformer(BOALTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "boal_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "bm_unit_id" in result.columns
        assert "acceptance_number" in result.columns
        assert "timestamp_utc" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_output_sorted_by_timestamp_bm_unit(self):
        data = json.loads((FIXTURES / "boal_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)
        timestamps = result["timestamp_utc"].to_list()
        assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# BOD
# ---------------------------------------------------------------------------


class TestBODTransformer:
    def setup_method(self):
        self.t = _make_transformer(BODTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "bod_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "bm_unit_id" in result.columns
        assert "bid_price" in result.columns
        assert "offer_price" in result.columns
        assert "timestamp_utc" in result.columns

    def test_dedup_on_pair_number(self):
        """Same (date, period, bm_unit, pair_number) rows are deduplicated."""
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "bmUnit": "T_DRAXX-1",
                    "bidOfferPairNumber": 1,
                    "bidPrice": -50.0,
                    "offerPrice": 75.0,
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "bmUnit": "T_DRAXX-1",
                    "bidOfferPairNumber": 1,
                    "bidPrice": -55.0,
                    "offerPrice": 80.0,
                },  # duplicate
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# MID
# ---------------------------------------------------------------------------


class TestMIDTransformer:
    def setup_method(self):
        self.t = _make_transformer(MIDTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "mid_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "data_provider_id" in result.columns
        assert "market_index_price" in result.columns
        assert "timestamp_utc" in result.columns

    def test_price_cast_to_float(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "dataProviderId": "N2EX",
                    "midPrice": "65.32",
                    "volume": "12000",
                },
            ]
        )
        result = self.t.transform(raw)
        assert result["market_index_price"].dtype == pl.Float64

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_current_api_field_names_populate(self):
        """G5-W1.2: live API (verified 2026-05-08) renamed dataProviderId→dataProvider
        and midPrice→price. The transformer must populate data_provider_id and
        market_index_price from current field names — silent-null was the bug."""
        data = json.loads((FIXTURES / "mid_response_v2.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert result["data_provider_id"].null_count() == 0, (
            "G5-W1.2 regression: data_provider_id silent-null from current-API bronze"
        )
        assert result["market_index_price"].null_count() == 0, (
            "G5-W1.2 regression: market_index_price silent-null from current-API bronze"
        )
        assert "N2EXMIP" in result["data_provider_id"].to_list()


# ---------------------------------------------------------------------------
# FREQ
# ---------------------------------------------------------------------------


class TestFreqTransformer:
    def setup_method(self):
        self.t = _make_transformer(FreqTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "freq_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "frequency_hz" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_parses_iso_datetime(self):
        raw = pl.DataFrame(
            [
                {"reportDateTime": "2024-01-15T00:00:00Z", "frequency": 50.01},
            ]
        )
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_dedup_on_timestamp(self):
        raw = pl.DataFrame(
            [
                {"reportDateTime": "2024-01-15T00:00:00Z", "frequency": 50.01},
                {"reportDateTime": "2024-01-15T00:00:00Z", "frequency": 50.02},  # dup
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# DemandForecast (NDF / NDFD)
# ---------------------------------------------------------------------------


class TestDemandForecastTransformer:
    def setup_method(self):
        self.t = _make_transformer(DemandForecastTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "ndf_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "national_demand_mw" in result.columns
        assert "forecast_type" in result.columns
        assert result["forecast_type"][0] == "day_ahead"

    def test_ndfd_forecast_type(self):
        """NDFDTransformer should label rows as 2_14_day."""
        t = NDFDTransformer.__new__(NDFDTransformer)
        t.data_dir = Path("/tmp/test")
        t.bronze_dir = Path("/tmp/test/bronze/elexon/ndfd")
        t.silver_dir = Path("/tmp/test/silver/elexon/ndfd")

        raw = pl.DataFrame(
            [
                {"settlementDate": "2024-01-15", "settlementPeriod": 1, "nationalDemand": 29000.0},
            ]
        )
        result = t.transform(raw)
        assert result["forecast_type"][0] == "2_14_day"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_preserves_multiple_published_at_vintages_per_period(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "nationalDemand": 28500.0,
                    "publishDateTime": "2024-01-14T09:30:00Z",
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "nationalDemand": 28600.0,
                    "publishDateTime": "2024-01-14T10:00:00Z",
                },
            ]
        )

        result = self.t.transform(raw)

        assert len(result) == 2
        assert sorted(result["published_at"].to_list()) == [
            datetime(2024, 1, 14, 9, 30, tzinfo=UTC),
            datetime(2024, 1, 14, 10, 0, tzinfo=UTC),
        ]


# ---------------------------------------------------------------------------
# WindForecast
# ---------------------------------------------------------------------------


class TestWindForecastTransformer:
    def setup_method(self):
        self.t = _make_transformer(WindForecastTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "windfor_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "initial_forecast_mw" in result.columns
        assert "latest_forecast_mw" in result.columns
        assert "timestamp_utc" in result.columns

    def test_dedup_on_settlement_date_period(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "initialForecast": 4500.0,
                    "latestForecast": 4300.0,
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "initialForecast": 4550.0,
                    "latestForecast": 4350.0,
                },  # dup
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_preserves_multiple_published_at_vintages_per_period(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "initialForecast": 4500.0,
                    "latestForecast": 4300.0,
                    "publishDateTime": "2024-01-14T08:00:00Z",
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "initialForecast": 4550.0,
                    "latestForecast": 4350.0,
                    "publishDateTime": "2024-01-14T09:00:00Z",
                },
            ]
        )

        result = self.t.transform(raw)

        assert len(result) == 2
        assert sorted(result["published_at"].to_list()) == [
            datetime(2024, 1, 14, 8, 0, tzinfo=UTC),
            datetime(2024, 1, 14, 9, 0, tzinfo=UTC),
        ]

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# PN
# ---------------------------------------------------------------------------


class TestPNTransformer:
    def setup_method(self):
        self.t = _make_transformer(PNTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "pn_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "bm_unit_id" in result.columns
        assert "level_from" in result.columns
        assert "level_to" in result.columns
        assert "timestamp_utc" in result.columns

    def test_dedup_on_date_period_bm_unit(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "bmUnit": "T_DRAXX-1",
                    "levelFrom": 0.0,
                    "levelTo": 400.0,
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "bmUnit": "T_DRAXX-1",
                    "levelFrom": 10.0,
                    "levelTo": 410.0,
                },  # dup
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# DISBSAD
# ---------------------------------------------------------------------------


class TestDISBSADTransformer:
    def setup_method(self):
        self.t = _make_transformer(DISBSADTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "disbsad_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "adjustment_action_id" in result.columns
        assert "cost" in result.columns
        assert "volume" in result.columns
        assert "timestamp_utc" in result.columns

    def test_cost_volume_cast_to_float(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "id": "X1",
                    "soFlag": True,
                    "storProviderFlag": False,
                    "component": "ENERGY",
                    "cost": "1250.5",
                    "volume": "15.0",
                },
            ]
        )
        result = self.t.transform(raw)
        assert result["cost"].dtype == pl.Float64
        assert result["volume"].dtype == pl.Float64

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_current_api_service_field_populates_component(self):
        """G5-W1.3: live API (verified 2026-05-08) renamed `component` to
        `service`. The silver column stays `component` (downstream contract);
        the transformer must map the current `service` key to it."""
        data = json.loads((FIXTURES / "disbsad_response_v2.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert result["component"].null_count() == 0, (
            "G5-W1.3 regression: component silent-null from current-API bronze"
        )
        assert "ENERGY" in result["component"].to_list()


# ---------------------------------------------------------------------------
# BMUnits (reference data)
# ---------------------------------------------------------------------------


class TestBMUnitsTransformer:
    def setup_method(self):
        self.t = _make_transformer(BMUnitsTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "bmunits_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "bm_unit_id" in result.columns
        assert "fuel_type" in result.columns
        assert "registered_capacity_mw" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_bm_unit_id(self):
        """Duplicate bm_unit_id rows should be deduplicated."""
        raw = pl.DataFrame(
            [
                {"bmUnit": "T_DRAXX-1", "name": "Drax 1 v1", "fuelType": "COAL"},
                {"bmUnit": "T_DRAXX-1", "name": "Drax 1 v2", "fuelType": "BIOMASS"},  # dup
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_sorted_by_bm_unit_id(self):
        data = json.loads((FIXTURES / "bmunits_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)
        ids = result["bm_unit_id"].to_list()
        assert ids == sorted(ids)

    def test_capacity_cast_to_float(self):
        raw = pl.DataFrame(
            [
                {"bmUnit": "T_TEST-1", "fuelType": "GAS", "registeredCapacity": "300.0"},
            ]
        )
        result = self.t.transform(raw)
        assert result["registered_capacity_mw"].dtype == pl.Float64

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_bm_unit_id_returns_empty(self):
        raw = pl.DataFrame([{"fuelType": "GAS", "name": "No ID"}])
        assert self.t.transform(raw).is_empty()


# === New transformer tests ===


class TestNETBSADTransformer:
    def setup_method(self):
        self.t = _make_transformer(NETBSADTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "netbsad_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "net_buy_price_adjustment" in result.columns
        assert "net_sell_volume_adjustment" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_settlement_period(self):
        """Duplicate (date, period) rows should keep last."""
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "netBuyPriceAdjustment": 2.50,
                    "netSellPriceAdjustment": 1.80,
                    "netBuyVolumeAdjustment": 150.0,
                    "netSellVolumeAdjustment": -75.0,
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "netBuyPriceAdjustment": 2.55,
                    "netSellPriceAdjustment": 1.85,
                    "netBuyVolumeAdjustment": 155.0,
                    "netSellVolumeAdjustment": -70.0,
                },
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"netBuyPriceAdjustment": 2.50}])
        assert self.t.transform(raw).is_empty()

    def test_current_api_8_field_decomposition_populates(self):
        """G5-W1.1: the 2026+ NETBSAD API replaced 4 coarse adjustment fields
        with 8 finer-grained ones (cost vs volume × energy vs system × buy
        vs sell). The transformer must emit all 8 silver columns when
        current-API bronze is present, with no silent nulls."""
        data = json.loads((FIXTURES / "netbsad_response_v2.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        new_cols = [
            "net_buy_price_cost_adjustment_energy",
            "net_buy_price_volume_adjustment_energy",
            "net_buy_price_volume_adjustment_system",
            "buy_price_price_adjustment",
            "net_sell_price_cost_adjustment_energy",
            "net_sell_price_volume_adjustment_energy",
            "net_sell_price_volume_adjustment_system",
            "sell_price_price_adjustment",
        ]
        for col in new_cols:
            assert col in result.columns, f"G5-W1.1 regression: {col} missing"
            assert result[col].null_count() == 0, (
                f"G5-W1.1 regression: {col} silent-null from current-API bronze"
            )

    def test_legacy_4_field_bronze_still_ingests(self):
        """G5-W1.1: legacy pre-2026 bronze (4 adjustment fields) must still
        round-trip cleanly through the transformer so historical re-ingest
        is not broken by the schema expansion."""
        data = json.loads((FIXTURES / "netbsad_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        legacy_cols = [
            "net_buy_price_adjustment",
            "net_sell_price_adjustment",
            "net_buy_volume_adjustment",
            "net_sell_volume_adjustment",
        ]
        for col in legacy_cols:
            assert col in result.columns
            assert result[col].null_count() == 0


class TestFuelInstTransformer:
    def setup_method(self):
        self.t = _make_transformer(FuelInstTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "fuelinst_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "fuel_type" in result.columns
        assert "generation_mw" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_timestamp_fuel(self):
        """Duplicate (timestamp, fuel_type) should keep last."""
        raw = pl.DataFrame(
            [
                {
                    "publishDateTime": "2024-01-15T00:00:00Z",
                    "fuelType": "CCGT",
                    "generation": 100.0,
                },
                {
                    "publishDateTime": "2024-01-15T00:00:00Z",
                    "fuelType": "CCGT",
                    "generation": 110.0,
                },
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"publishDateTime": "2024-01-15T00:00:00Z"}])
        assert self.t.transform(raw).is_empty()


class TestImbalNGCTransformer:
    def setup_method(self):
        self.t = _make_transformer(ImbalNGCTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "imbalngc_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "indicated_imbalance" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_settlement_period(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "publishDateTime": "2024-01-15T00:15:00Z",
                    "indicatedImbalance": -250.0,
                },
                {
                    "settlementDate": "2024-01-15",
                    "settlementPeriod": 1,
                    "publishDateTime": "2024-01-15T00:20:00Z",
                    "indicatedImbalance": -240.0,
                },
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"settlementDate": "2024-01-15"}])
        assert self.t.transform(raw).is_empty()


class TestMelNGCTransformer:
    def setup_method(self):
        self.t = _make_transformer(MelNGCTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "melngc_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "indicated_margin" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_settlement_period(self):
        raw = pl.DataFrame(
            [
                {"settlementDate": "2024-01-15", "settlementPeriod": 1, "indicatedMargin": 3500.0},
                {"settlementDate": "2024-01-15", "settlementPeriod": 1, "indicatedMargin": 3600.0},
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"settlementDate": "2024-01-15"}])
        assert self.t.transform(raw).is_empty()


class TestFOU2T14DTransformer:
    def setup_method(self):
        self.t = _make_transformer(FOU2T14DTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "fou2t14d_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "fuel_type" in result.columns
        assert "output_usable_mw" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_period_fuel(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-17",
                    "settlementPeriod": 1,
                    "fuelType": "CCGT",
                    "outputUsable": 22000.0,
                },
                {
                    "settlementDate": "2024-01-17",
                    "settlementPeriod": 1,
                    "fuelType": "CCGT",
                    "outputUsable": 22100.0,
                },
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"settlementDate": "2024-01-17"}])
        assert self.t.transform(raw).is_empty()


class TestUOU2T14DTransformer:
    def setup_method(self):
        self.t = _make_transformer(UOU2T14DTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "uou2t14d_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "bm_unit_id" in result.columns
        assert "output_usable_mw" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_dedup_on_period_unit(self):
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2024-01-17",
                    "settlementPeriod": 1,
                    "bmUnit": "T_DRAXX-1",
                    "outputUsable": 645.0,
                },
                {
                    "settlementDate": "2024-01-17",
                    "settlementPeriod": 1,
                    "bmUnit": "T_DRAXX-1",
                    "outputUsable": 640.0,
                },
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"settlementDate": "2024-01-17"}])
        assert self.t.transform(raw).is_empty()

    def test_self_describing_fuel_and_grid_unit(self):
        """G5-W2.3: UOU2T14D is self-describing — when bronze carries
        nationalGridBmUnit and fuelType, the rename map snake-cases both
        but output_cols previously dropped them, forcing a bmunits join
        for fuel context. With the fix both survive to silver."""
        data = json.loads((FIXTURES / "uou2t14d_response_v2.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "fuel_type" in result.columns, "G5-W2.3 regression: fuel_type dropped before write"
        assert "national_grid_bm_unit" in result.columns, (
            "G5-W2.3 regression: national_grid_bm_unit dropped before write"
        )
        assert result["fuel_type"].null_count() == 0
        assert result["national_grid_bm_unit"].null_count() == 0
        assert "BIOMASS" in result["fuel_type"].to_list()


class TestTempTransformer:
    def setup_method(self):
        self.t = _make_transformer(TempTransformer)

    def test_transform_basic(self):
        data = json.loads((FIXTURES / "temp_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "temperature" in result.columns
        assert "normal_temperature" in result.columns
        assert "low_temperature" in result.columns
        assert "high_temperature" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_sorted_by_timestamp(self):
        data = json.loads((FIXTURES / "temp_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)
        ts = result["timestamp_utc"].to_list()
        assert ts == sorted(ts)

    def test_dedup_on_timestamp(self):
        raw = pl.DataFrame(
            [
                {"publishDateTime": "2024-01-15T00:00:00Z", "temperature": 5.0},
                {"publishDateTime": "2024-01-15T00:00:00Z", "temperature": 5.5},
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"normal": 6.0}])
        assert self.t.transform(raw).is_empty()

    def test_measurement_date_survives_to_silver(self):
        """G5-W1.4: when bronze includes `measurementDate`, the silver row
        must carry the original vendor measurement date as `measurement_date`.
        Previously it was renamed then dropped by output_cols, so vault docs
        and code disagreed."""
        data = json.loads((FIXTURES / "temp_response_v2.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)

        assert not result.is_empty()
        assert "measurement_date" in result.columns, (
            "G5-W1.4 regression: measurement_date dropped before write"
        )
        assert result["measurement_date"].null_count() == 0
        assert result["measurement_date"].dtype == pl.Date


# TestGenerationByFuelTransformer removed — generation_by_fuel was a duplicate of fuelhh.
# Both used /datasets/FUELHH; use fuelhh tests instead.


# ---------------------------------------------------------------------------
# G6 — published_at survives to silver on the 12 W2.2-pattern transformers
# ---------------------------------------------------------------------------
# Each entry: (transformer_cls, publish_field_name, minimal_record_excluding_publish_field).
# The publish field maps to `published_at` via the transformer's rename map.
# The record contains the minimum required-field set that produces a non-empty
# silver row; the publish field is injected by the parametrised test below.
_G6_TRANSFORMER_CASES: list[tuple[type, str, dict[str, object]]] = [
    (
        ImbalNGCTransformer,
        "publishDateTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "indicatedImbalance": -250.0},
    ),
    (
        MelNGCTransformer,
        "publishDateTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "indicatedMargin": 3500.0},
    ),
    (
        INDDEMTransformer,
        "publishTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "demand": 28000.0},
    ),
    (
        INDOTransformer,
        "publishTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "demand": 28500.0},
    ),
    (
        ITSDOTransformer,
        "publishTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "demand": 28100.0},
    ),
    (
        INDGENTransformer,
        "publishTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "generation": 30000.0},
    ),
    (
        AGPTTransformer,
        "publishTime",
        {
            "settlementDate": "2024-01-15",
            "settlementPeriod": 1,
            "psrType": "B16",
            "quantity": 1200.0,
        },
    ),
    (
        AGWSTransformer,
        "publishTime",
        {
            "settlementDate": "2024-01-15",
            "settlementPeriod": 1,
            "psrType": "B16",
            "quantity": 1100.0,
        },
    ),
    (
        ATLTransformer,
        "publishTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "quantity": 31000.0},
    ),
    (
        NONBMTransformer,
        "publishTime",
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "generation": 50.0},
    ),
    (
        FOU2T14DTransformer,
        "publishDateTime",
        {
            "settlementDate": "2024-01-17",
            "settlementPeriod": 1,
            "fuelType": "CCGT",
            "outputUsable": 22000.0,
        },
    ),
    (
        LOLPDRMTransformer,
        "publishTime",
        {
            "settlementDate": "2024-01-15",
            "settlementPeriod": 1,
            "lossOfLoadProbability": 0.001,
            "deratedMargin": 5000.0,
        },
    ),
]


@pytest.mark.parametrize(
    "transformer_cls, publish_field, base_record",
    _G6_TRANSFORMER_CASES,
    ids=[c[0].__name__ for c in _G6_TRANSFORMER_CASES],
)
def test_g6_published_at_emitted_when_bronze_carries_it(
    transformer_cls: type, publish_field: str, base_record: dict[str, object]
):
    """G6 (W2.2 pattern): each of the 12 affected Elexon transformers must
    emit `published_at` to silver as a UTC-aware datetime when bronze
    carries the corresponding publishDateTime / publishTime field.

    Before G6 the rename map produced `published_at` but output_cols
    dropped it before write, so silver Parquet was missing the column
    even though the Pydantic schema declared it. This regression test
    pins the W2.2 fix shape across all 12 transformers.
    """
    record = {**base_record, publish_field: "2024-01-15T00:15:00Z"}
    raw = pl.DataFrame([record])
    t = _make_transformer(transformer_cls)
    result = t.transform(raw)

    assert not result.is_empty(), (
        f"{transformer_cls.__name__} produced empty silver for minimal record"
    )
    assert "published_at" in result.columns, (
        f"G6 regression: {transformer_cls.__name__} dropped published_at "
        f"before write (W2.2 pattern). Rename map produced it but "
        f"output_cols omitted it."
    )
    assert result["published_at"].null_count() == 0, (
        f"{transformer_cls.__name__}: published_at survived to silver "
        f"but is null — cast or rename failed"
    )
    assert result["published_at"].dtype == pl.Datetime("us", "UTC"), (
        f"{transformer_cls.__name__}: published_at dtype is "
        f"{result['published_at'].dtype}, expected pl.Datetime('us', 'UTC')"
    )


def test_indo_published_at_typed_null_when_bronze_lacks_publish_time():
    """F24 drift fix (inverse of G6): INDO must emit `published_at` even when
    bronze carries no `publishTime` — as a typed-null column, not a dropped one.

    The G6 cohort above pins the *present* case. The *absent* case is the one
    that drifted: when bronze lacked publishTime the column was omitted from
    silver entirely (ElexonINDO declares it always-present-nullable), so a
    `SELECT *` partition glob spanning publishTime-present files (2024) and
    publishTime-absent files (2026) raised a DuckDB schema mismatch. With the
    fix the silver schema is deterministic regardless of bronze.
    """
    raw = pl.DataFrame(
        [
            {"settlementDate": "2026-04-14", "settlementPeriod": 1, "demand": 28500.0},
        ]
    )
    result = _make_transformer(INDOTransformer).transform(raw)

    assert not result.is_empty()
    assert "published_at" in result.columns, (
        "drift regression: INDO dropped published_at when bronze lacked publishTime"
    )
    assert result["published_at"].null_count() == result.height, (
        "published_at must be all-null when bronze carries no publishTime"
    )
    assert result["published_at"].dtype == pl.Datetime("us", "UTC")


# ---------------------------------------------------------------------------
# F24 cohort: published_at emitted typed-null when bronze lacks the publish
# field, across every Elexon transformer that publishes it as a contract
# column. Reuses the G6 present-case records (publish field dropped) plus the
# five transformers outside the G6 list that also output published_at —
# FuelHH, UOU2T14D, TSDFD, and the WindForecast/DemandForecast forecast
# transformers (switched from emitting issue_time to published_at).
# ---------------------------------------------------------------------------
_PUBLISHED_AT_ABSENT_CASES: list[tuple[type, dict[str, object]]] = [
    (cls, base_record) for cls, _publish_field, base_record in _G6_TRANSFORMER_CASES
] + [
    (
        FuelHHTransformer,
        {
            "settlementDate": "2024-01-15",
            "settlementPeriod": 1,
            "fuelType": "CCGT",
            "generation": 12000.0,
        },
    ),
    (
        UOU2T14DTransformer,
        {
            "settlementDate": "2024-01-17",
            "settlementPeriod": 1,
            "bmUnit": "T_DRAXX-1",
            "outputUsable": 645.0,
        },
    ),
    (
        TSDFDTransformer,
        {"forecastDate": "2024-01-17", "demand": 35000.0},
    ),
    (
        WindForecastTransformer,
        {
            "settlementDate": "2024-01-15",
            "settlementPeriod": 1,
            "initialForecast": 4500.0,
            "latestForecast": 4300.0,
        },
    ),
    (
        DemandForecastTransformer,
        {"settlementDate": "2024-01-15", "settlementPeriod": 1, "nationalDemand": 28500.0},
    ),
]


@pytest.mark.parametrize(
    "transformer_cls, base_record",
    _PUBLISHED_AT_ABSENT_CASES,
    ids=[c[0].__name__ for c in _PUBLISHED_AT_ABSENT_CASES],
)
def test_published_at_typed_null_when_bronze_lacks_publish_field(
    transformer_cls: type, base_record: dict[str, object]
):
    """F24 drift fix (inverse of G6): every published_at-emitting Elexon
    transformer must emit `published_at` as a typed-null column when bronze
    lacks the publish field — not drop it.

    A dropped column makes the silver schema non-deterministic and breaks
    `SELECT *` partition globs spanning files that do carry it (the
    elexon/indo 2024<->2026 break). The G6 cohort above pins the present
    case; this pins the absent case across the whole cohort.
    """
    result = _make_transformer(transformer_cls).transform(pl.DataFrame([base_record]))

    assert not result.is_empty(), (
        f"{transformer_cls.__name__} produced empty silver for minimal record"
    )
    assert "published_at" in result.columns, (
        f"drift regression: {transformer_cls.__name__} dropped published_at "
        f"when bronze lacked the publish field"
    )
    assert result["published_at"].null_count() == result.height, (
        f"{transformer_cls.__name__}: published_at must be all-null when bronze "
        f"carries no publish field"
    )
    assert result["published_at"].dtype == pl.Datetime("us", "UTC"), (
        f"{transformer_cls.__name__}: published_at dtype is "
        f"{result['published_at'].dtype}, expected pl.Datetime('us', 'UTC')"
    )


# ---------------------------------------------------------------------------
# INDOD (daily) timestamp_utc convention
# ---------------------------------------------------------------------------


class TestINDODTimestampConvention:
    """INDOD is a *daily* dataset. Its `timestamp_utc` must mark the
    settlement-day START (SP1 = 00:00 UK local), so it aligns with its own
    half-hourly INDO series rather than sitting 1h off during BST.
    """

    def setup_method(self):
        self.t = _make_transformer(INDODTransformer)

    def _transform(self, settlement_date: str):
        raw = pl.DataFrame([{"settlementDate": settlement_date, "demand": 30000.0}])
        return self.t.transform(raw)

    def test_daily_timestamp_is_settlement_day_start_winter(self):
        """Winter (GMT) day start is 00:00 UTC -- unchanged from before."""
        result = self._transform("2024-01-15")
        ts = result["timestamp_utc"][0]
        assert ts == datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_daily_timestamp_is_settlement_day_start_bst(self):
        """BST day start is 23:00 UTC the previous calendar day.

        FAILS under the old UTC-midnight cast, which stamped 2024-07-15T00:00Z
        (1h after the real local-midnight start) and so disagreed with the
        half-hourly INDO series for the same date.
        """
        result = self._transform("2024-07-15")
        ts = result["timestamp_utc"][0]
        assert ts == datetime(2024, 7, 14, 23, 0, tzinfo=UTC)

    def test_daily_timestamp_matches_halfhourly_sp1(self):
        """The daily roll-up's timestamp equals INDO's SP1 for the same date."""
        for d in ["2024-01-15", "2024-07-15", "2024-10-27", "2024-03-31"]:
            daily_ts = self._transform(d)["timestamp_utc"][0]
            sp1_ts = settlement_period_to_utc(date.fromisoformat(d), 1)
            assert daily_ts == sp1_ts, f"INDOD daily vs INDO SP1 mismatch on {d}"


# ---------------------------------------------------------------------------
# Gold day_of_week convention
# ---------------------------------------------------------------------------


class TestDayOfWeekConvention:
    """Pin the gold `day_of_week` index so the cross-repo seam is explicit.

    Gold uses Polars `dt.weekday()` (ISO: 1=Mon..7=Sun). The gridflow_models
    calendar feature uses Python `weekday()` (0=Mon..6=Sun). They differ by one
    and the mismatch must be reconciled at the seam, not silently. This test
    fails if gold's convention drifts.
    """

    def test_day_of_week_convention_iso(self):
        # 2024-01-15 is a Monday; 2024-01-21 is a Sunday.
        df = pl.DataFrame(
            {
                "timestamp_utc": [
                    datetime(2024, 1, 15, 12, 0, tzinfo=UTC),  # Monday
                    datetime(2024, 1, 21, 12, 0, tzinfo=UTC),  # Sunday
                ]
            }
        )
        result = df.with_columns(pl.col("timestamp_utc").dt.weekday().alias("day_of_week"))
        dow = result["day_of_week"].to_list()
        assert dow == [1, 7], (
            "gold day_of_week must be ISO (Mon=1..Sun=7); the gridflow_models "
            "calendar consumer is Python weekday() (Mon=0..Sun=6) -- reconcile "
            "at the seam"
        )
