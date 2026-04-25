"""Unit tests for silver-layer transformers."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from gridflow.silver.elexon.bmunits import BMUnitsTransformer
from gridflow.silver.elexon.boal import BOALTransformer
from gridflow.silver.elexon.bod import BODTransformer
from gridflow.silver.elexon.demand_forecast import DemandForecastTransformer, NDFDTransformer
from gridflow.silver.elexon.disbsad import DISBSADTransformer
from gridflow.silver.elexon.fou2t14d import FOU2T14DTransformer
from gridflow.silver.elexon.freq import FreqTransformer
from gridflow.silver.elexon.fuelinst import FuelInstTransformer
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.imbalngc import ImbalNGCTransformer
from gridflow.silver.elexon.melngc import MelNGCTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.netbsad import NETBSADTransformer
from gridflow.silver.elexon.pn import PNTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.elexon.temp import TempTransformer
from gridflow.silver.elexon.uou2t14d import UOU2T14DTransformer
from gridflow.silver.elexon.wind_forecast import WindForecastTransformer

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
        raw = self._make_raw_df([
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
        ])
        result = self.transformer.transform(raw)

        assert len(result) == 2
        assert "timestamp_utc" in result.columns
        assert "system_sell_price" in result.columns
        assert "data_provider" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_run_type_resolution(self):
        """Later run types should supersede earlier ones."""
        raw = self._make_raw_df([
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
        ])
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

    def test_missing_columns_returns_empty(self):
        """Missing required columns should return empty DataFrame."""
        raw = self._make_raw_df([{"foo": "bar"}])
        result = self.transformer.transform(raw)
        assert result.is_empty()

    def test_timestamp_utc_winter(self):
        """SP1 on a winter date should be 00:00 UTC."""
        raw = self._make_raw_df([
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "systemSellPrice": 45.50,
                "systemBuyPrice": 55.00,
                "netImbalanceVolume": 0.0,
                "settlementRunType": "SF",
            },
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "fuelType": "CCGT", "generation": 12000.0},
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "fuelType": "CCGT", "generation": 12500.0},  # duplicate
        ])
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_timestamp_sp1_winter(self):
        """SP1 on winter date maps to 00:00 UTC."""
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "fuelType": "WIND", "generation": 3000.0},
        ])
        result = self.t.transform(raw)
        ts = result["timestamp_utc"][0]
        assert ts.hour == 0
        assert ts.minute == 0


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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "bmUnit": "T_DRAXX-1", "bidOfferPairNumber": 1,
             "bidPrice": -50.0, "offerPrice": 75.0},
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "bmUnit": "T_DRAXX-1", "bidOfferPairNumber": 1,
             "bidPrice": -55.0, "offerPrice": 80.0},  # duplicate
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "dataProviderId": "N2EX", "midPrice": "65.32", "volume": "12000"},
        ])
        result = self.t.transform(raw)
        assert result["market_index_price"].dtype == pl.Float64

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


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
        raw = pl.DataFrame([
            {"reportDateTime": "2024-01-15T00:00:00Z", "frequency": 50.01},
        ])
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_dedup_on_timestamp(self):
        raw = pl.DataFrame([
            {"reportDateTime": "2024-01-15T00:00:00Z", "frequency": 50.01},
            {"reportDateTime": "2024-01-15T00:00:00Z", "frequency": 50.02},  # dup
        ])
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

        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "nationalDemand": 29000.0},
        ])
        result = t.transform(raw)
        assert result["forecast_type"][0] == "2_14_day"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "initialForecast": 4500.0, "latestForecast": 4300.0},
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "initialForecast": 4550.0, "latestForecast": 4350.0},  # dup
        ])
        result = self.t.transform(raw)
        assert len(result) == 1

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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "bmUnit": "T_DRAXX-1", "levelFrom": 0.0, "levelTo": 400.0},
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "bmUnit": "T_DRAXX-1", "levelFrom": 10.0, "levelTo": 410.0},  # dup
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "id": "X1", "soFlag": True, "storProviderFlag": False,
             "component": "ENERGY", "cost": "1250.5", "volume": "15.0"},
        ])
        result = self.t.transform(raw)
        assert result["cost"].dtype == pl.Float64
        assert result["volume"].dtype == pl.Float64

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


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
        raw = pl.DataFrame([
            {"bmUnit": "T_DRAXX-1", "name": "Drax 1 v1", "fuelType": "COAL"},
            {"bmUnit": "T_DRAXX-1", "name": "Drax 1 v2", "fuelType": "BIOMASS"},  # dup
        ])
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_sorted_by_bm_unit_id(self):
        data = json.loads((FIXTURES / "bmunits_response.json").read_text())
        raw = pl.DataFrame(data["data"])
        result = self.t.transform(raw)
        ids = result["bm_unit_id"].to_list()
        assert ids == sorted(ids)

    def test_capacity_cast_to_float(self):
        raw = pl.DataFrame([
            {"bmUnit": "T_TEST-1", "fuelType": "GAS", "registeredCapacity": "300.0"},
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "netBuyPriceAdjustment": 2.50, "netSellPriceAdjustment": 1.80,
             "netBuyVolumeAdjustment": 150.0, "netSellVolumeAdjustment": -75.0},
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "netBuyPriceAdjustment": 2.55, "netSellPriceAdjustment": 1.85,
             "netBuyVolumeAdjustment": 155.0, "netSellVolumeAdjustment": -70.0},
        ])
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"netBuyPriceAdjustment": 2.50}])
        assert self.t.transform(raw).is_empty()


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
        raw = pl.DataFrame([
            {"publishDateTime": "2024-01-15T00:00:00Z", "fuelType": "CCGT", "generation": 100.0},
            {"publishDateTime": "2024-01-15T00:00:00Z", "fuelType": "CCGT", "generation": 110.0},
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "publishDateTime": "2024-01-15T00:15:00Z", "indicatedImbalance": -250.0},
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "publishDateTime": "2024-01-15T00:20:00Z", "indicatedImbalance": -240.0},
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "indicatedMargin": 3500.0},
            {"settlementDate": "2024-01-15", "settlementPeriod": 1,
             "indicatedMargin": 3600.0},
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-17", "settlementPeriod": 1,
             "fuelType": "CCGT", "outputUsable": 22000.0},
            {"settlementDate": "2024-01-17", "settlementPeriod": 1,
             "fuelType": "CCGT", "outputUsable": 22100.0},
        ])
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
        raw = pl.DataFrame([
            {"settlementDate": "2024-01-17", "settlementPeriod": 1,
             "bmUnit": "T_DRAXX-1", "outputUsable": 645.0},
            {"settlementDate": "2024-01-17", "settlementPeriod": 1,
             "bmUnit": "T_DRAXX-1", "outputUsable": 640.0},
        ])
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"settlementDate": "2024-01-17"}])
        assert self.t.transform(raw).is_empty()


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
        raw = pl.DataFrame([
            {"publishDateTime": "2024-01-15T00:00:00Z", "temperature": 5.0},
            {"publishDateTime": "2024-01-15T00:00:00Z", "temperature": 5.5},
        ])
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_returns_empty(self):
        raw = pl.DataFrame([{"normal": 6.0}])
        assert self.t.transform(raw).is_empty()


# TestGenerationByFuelTransformer removed — generation_by_fuel was a duplicate of fuelhh.
# Both used /datasets/FUELHH; use fuelhh tests instead.
