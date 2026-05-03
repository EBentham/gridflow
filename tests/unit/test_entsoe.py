"""Unit tests for ENTSO-E connector, parsers, schemas, and silver transformers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from gridflow.connectors.entsoe.endpoints import (
    BIDDING_ZONES,
    DEFAULT_CONTROL_AREAS,
    DEFAULT_ZONES,
    DOC_TYPES,
    EntsoeDocType,
)
from gridflow.connectors.entsoe.parsers import (
    parse_generation_units_master_data_xml,
    parse_timeseries_xml,
)
from gridflow.schemas.entsoe import (
    EntsoeActivatedBalancingPrices,
    EntsoeActivatedBalancingQty,
    EntsoeActualGeneration,
    EntsoeActualGenerationUnits,
    EntsoeActualLoad,
    EntsoeContractedReserves,
    EntsoeCrossborderFlow,
    EntsoeDayAheadPrice,
    EntsoeForecastMargin,
    EntsoeGenerationForecast,
    EntsoeGenerationUnitsMasterData,
    EntsoeImbalancePrices,
    EntsoeImbalanceVolume,
    EntsoeInstalledCapacity,
    EntsoeInstalledCapacityUnits,
    EntsoeLoadForecast,
    EntsoeLoadForecastWeekly,
    EntsoeNetTransferCapacity,
    EntsoeOutagesGeneration,
    EntsoeTransmissionMarketAmount,
    EntsoeTransmissionMarketQuantity,
    EntsoeWaterReservoirs,
    EntsoeWindSolarForecast,
)
from gridflow.silver.entsoe.activated_balancing_prices import ActivatedBalancingPricesTransformer
from gridflow.silver.entsoe.activated_balancing_qty import ActivatedBalancingQtyTransformer
from gridflow.silver.entsoe.actual_generation import ActualGenerationTransformer
from gridflow.silver.entsoe.actual_generation_units import ActualGenerationUnitsTransformer
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.entsoe.contracted_reserves import ContractedReservesTransformer
from gridflow.silver.entsoe.cross_border_flows import CrossBorderFlowsTransformer
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer
from gridflow.silver.entsoe.forecast_margin import ForecastMarginTransformer
from gridflow.silver.entsoe.generation_forecast import GenerationForecastTransformer
from gridflow.silver.entsoe.generation_units_master_data import (
    GenerationUnitsMasterDataTransformer,
)
from gridflow.silver.entsoe.h6_market import (
    AuctionRevenueTransformer,
    CommercialSchedulesTransformer,
    CongestionManagementCostsTransformer,
    DcLinkIntradayTransferLimitsTransformer,
    OfferedTransferCapacityContinuousTransformer,
)
from gridflow.silver.entsoe.imbalance_prices import ImbalancePricesTransformer
from gridflow.silver.entsoe.imbalance_volume import ImbalanceVolumeTransformer
from gridflow.silver.entsoe.installed_capacity import InstalledCapacityTransformer
from gridflow.silver.entsoe.installed_capacity_units import InstalledCapacityUnitsTransformer
from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.entsoe.load_forecast_monthly import LoadForecastMonthlyTransformer
from gridflow.silver.entsoe.load_forecast_weekly import LoadForecastWeeklyTransformer
from gridflow.silver.entsoe.load_forecast_yearly import LoadForecastYearlyTransformer
from gridflow.silver.entsoe.net_transfer_capacity import NetTransferCapacityTransformer
from gridflow.silver.entsoe.outages_generation import OutagesGenerationTransformer
from gridflow.silver.entsoe.water_reservoirs import WaterReservoirsTransformer
from gridflow.silver.entsoe.wind_solar_forecast import WindSolarForecastTransformer

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"


# ---------------------------------------------------------------------------
# Endpoints / registry
# ---------------------------------------------------------------------------


class TestEntsoeEndpoints:
    def test_doc_types_populated(self):
        assert "day_ahead_prices" in DOC_TYPES
        assert "actual_load" in DOC_TYPES
        assert "actual_generation" in DOC_TYPES
        assert "cross_border_flows" in DOC_TYPES

    def test_phase2_doc_types_populated(self):
        assert "generation_forecast" in DOC_TYPES
        assert "load_forecast_weekly" in DOC_TYPES
        assert "load_forecast_monthly" in DOC_TYPES
        assert "load_forecast_yearly" in DOC_TYPES
        assert "forecast_margin" in DOC_TYPES
        assert "net_transfer_capacity" in DOC_TYPES

    def test_generation_forecast_doc_type(self):
        gf = DOC_TYPES["generation_forecast"]
        assert gf.document_type == "A71"
        assert gf.process_type == "A01"

    def test_load_forecast_weekly_doc_type(self):
        lfw = DOC_TYPES["load_forecast_weekly"]
        assert lfw.document_type == "A65"
        assert lfw.process_type == "A31"

    def test_load_forecast_monthly_doc_type(self):
        lfm = DOC_TYPES["load_forecast_monthly"]
        assert lfm.document_type == "A65"
        assert lfm.process_type == "A32"
        assert lfm.domain_style == "out_bidding_zone"

    def test_load_forecast_yearly_doc_type(self):
        lfy = DOC_TYPES["load_forecast_yearly"]
        assert lfy.document_type == "A65"
        assert lfy.process_type == "A33"
        assert lfy.domain_style == "out_bidding_zone"

    def test_forecast_margin_doc_type(self):
        fm = DOC_TYPES["forecast_margin"]
        assert fm.document_type == "A70"
        assert fm.process_type == "A33"
        assert fm.domain_style == "out_bidding_zone"

    def test_net_transfer_capacity_doc_type(self):
        ntc = DOC_TYPES["net_transfer_capacity"]
        assert ntc.document_type == "A61"
        assert ntc.process_type is None
        assert ntc.domain_style == "zone_pair"
        assert ntc.extra_params == {"contract_MarketAgreement.Type": "A01"}

    def test_doc_type_fields(self):
        dap = DOC_TYPES["day_ahead_prices"]
        assert isinstance(dap, EntsoeDocType)
        assert dap.document_type == "A44"
        assert dap.process_type is None

    def test_actual_load_has_process_type(self):
        al = DOC_TYPES["actual_load"]
        assert al.document_type == "A65"
        assert al.process_type == "A16"

    def test_bidding_zones_has_gb(self):
        assert "GB" in BIDDING_ZONES
        assert BIDDING_ZONES["GB"] == "10YGB----------A"

    def test_default_zones_subset_of_bidding_zones(self):
        for zone in DEFAULT_ZONES:
            assert zone in BIDDING_ZONES

    def test_default_zones_includes_gb_and_fr(self):
        assert "GB" in DEFAULT_ZONES
        assert "FR" in DEFAULT_ZONES


# ---------------------------------------------------------------------------
# XML Parser
# ---------------------------------------------------------------------------


class TestParseTimeseriesXml:
    def _load(self, filename: str) -> bytes:
        return (FIXTURES / filename).read_bytes()

    def test_parse_day_ahead_prices(self):
        xml = self._load("day_ahead_prices_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="price.amount")
        assert len(records) == 4  # 4 hourly points

    def test_record_has_expected_keys(self):
        xml = self._load("day_ahead_prices_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="price.amount")
        record = records[0]
        assert "timestamp_utc" in record
        assert "value" in record
        assert "in_domain" in record
        assert "out_domain" in record
        assert "resolution" in record
        assert "unit_mrid" in record
        assert "unit_name" in record

    def test_unit_mrid_unit_name_empty_for_non_a80(self):
        """Non-A80 fixtures have no RegisteredResource elements."""
        xml = self._load("load_forecast_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="quantity")
        assert len(records) > 0, "fixture must produce at least one record"
        for record in records:
            assert record["unit_mrid"] == "", (
                f"expected unit_mrid=='' but got {record['unit_mrid']!r}"
            )
            assert record["unit_name"] == "", (
                f"expected unit_name=='' but got {record['unit_name']!r}"
            )

    def test_timestamps_are_utc_datetimes(self):
        xml = self._load("day_ahead_prices_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="price.amount")
        for r in records:
            assert isinstance(r["timestamp_utc"], datetime)
            assert r["timestamp_utc"].tzinfo is not None

    def test_first_timestamp_correct(self):
        xml = self._load("day_ahead_prices_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="price.amount")
        assert records[0]["timestamp_utc"] == datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_second_timestamp_offset_by_resolution(self):
        xml = self._load("day_ahead_prices_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="price.amount")
        assert records[1]["timestamp_utc"] == datetime(2024, 1, 15, 1, 0, tzinfo=UTC)

    def test_price_values_correct(self):
        xml = self._load("day_ahead_prices_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="price.amount")
        assert abs(records[0]["value"] - 85.50) < 0.01
        assert abs(records[1]["value"] - 82.30) < 0.01

    def test_in_domain_populated(self):
        xml = self._load("day_ahead_prices_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="price.amount")
        assert records[0]["in_domain"] == "10YGB----------A"

    def test_parse_actual_load(self):
        xml = self._load("actual_load_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="quantity")
        assert len(records) == 3  # 3 half-hour points
        assert abs(records[0]["value"] - 28500) < 0.1

    def test_parse_actual_generation_two_timeseries(self):
        xml = self._load("actual_generation_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="quantity")
        # 2 TimeSeries * 2 points each = 4 records
        assert len(records) == 4

    def test_parse_actual_generation_production_type(self):
        xml = self._load("actual_generation_gb.xml")
        records = parse_timeseries_xml(xml, value_tag="quantity")
        prod_types = {r["production_type"] for r in records}
        assert "B01" in prod_types
        assert "B19" in prod_types

    def test_parse_cross_border_flows(self):
        xml = self._load("cross_border_flows_gb_fr.xml")
        records = parse_timeseries_xml(xml, value_tag="quantity")
        assert len(records) == 3
        assert records[0]["in_domain"] == "10YGB----------A"
        assert records[0]["out_domain"] == "10YFR-RTE------C"

    def test_empty_bytes_returns_empty(self):
        records = parse_timeseries_xml(b"<root></root>", value_tag="price.amount")
        assert records == []

    def test_invalid_xml_returns_empty(self):
        records = parse_timeseries_xml(b"not xml at all <<<", value_tag="price.amount")
        assert records == []


# ---------------------------------------------------------------------------
# Helper to build transformer instances bypassing __init__
# ---------------------------------------------------------------------------


def _make_entsoe_transformer(cls, dataset: str | None = None):
    t = cls.__new__(cls)
    ds = dataset or cls.dataset
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path(f"/tmp/test/bronze/entsoe/{ds}")
    t.silver_dir = Path(f"/tmp/test/silver/entsoe/{ds}")
    return t


def _make_df_from_xml(filename: str, value_tag: str) -> pl.DataFrame:
    xml = (FIXTURES / filename).read_bytes()
    records = parse_timeseries_xml(xml, value_tag=value_tag)
    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# DayAheadPricesTransformer
# ---------------------------------------------------------------------------


class TestDayAheadPricesTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(DayAheadPricesTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("day_ahead_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "area_code" in result.columns
        assert "price_eur_mwh" in result.columns

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("day_ahead_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_data_provider(self):
        raw = _make_df_from_xml("day_ahead_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_price_values(self):
        raw = _make_df_from_xml("day_ahead_prices_gb.xml", "price.amount")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["price_eur_mwh"][0] - 85.50) < 0.01

    def test_dedup(self):
        raw = _make_df_from_xml("day_ahead_prices_gb.xml", "price.amount")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4  # original 4 unique points

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# ActualLoadTransformer
# ---------------------------------------------------------------------------


class TestActualLoadTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ActualLoadTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("actual_load_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "load_mw" in result.columns
        assert "area_code" in result.columns

    def test_load_values(self):
        raw = _make_df_from_xml("actual_load_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["load_mw"][0] - 28500) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("actual_load_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_30min_resolution(self):
        raw = _make_df_from_xml("actual_load_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        ts = result["timestamp_utc"].to_list()
        # Half-hour intervals
        delta = ts[1] - ts[0]
        assert delta.total_seconds() == 30 * 60

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# ActualGenerationTransformer
# ---------------------------------------------------------------------------


class TestActualGenerationTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ActualGenerationTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("actual_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "generation_mw" in result.columns
        assert "production_type" in result.columns
        assert "area_code" in result.columns

    def test_two_production_types(self):
        raw = _make_df_from_xml("actual_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        prod_types = set(result["production_type"].to_list())
        assert "B01" in prod_types
        assert "B19" in prod_types

    def test_four_records(self):
        raw = _make_df_from_xml("actual_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4  # 2 types × 2 points

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# CrossBorderFlowsTransformer
# ---------------------------------------------------------------------------


class TestCrossBorderFlowsTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(CrossBorderFlowsTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("cross_border_flows_gb_fr.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "flow_mw" in result.columns
        assert "in_area_code" in result.columns
        assert "out_area_code" in result.columns

    def test_domain_codes_preserved(self):
        raw = _make_df_from_xml("cross_border_flows_gb_fr.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert result["in_area_code"][0] == "10YGB----------A"
        assert result["out_area_code"][0] == "10YFR-RTE------C"

    def test_three_records(self):
        raw = _make_df_from_xml("cross_border_flows_gb_fr.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 3

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_columns_returns_empty(self):
        raw = pl.DataFrame([{"timestamp_utc": datetime(2024, 1, 15, tzinfo=UTC), "value": 100.0}])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestEntsoeDayAheadPriceSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeDayAheadPrice(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            price_eur_mwh=85.50,
        )
        assert r.data_provider == "entsoe"
        assert r.price_eur_mwh == 85.50

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeDayAheadPrice(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                price_eur_mwh=85.50,
            )


class TestEntsoeActualLoadSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeActualLoad(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            load_mw=28500.0,
        )
        assert r.data_provider == "entsoe"
        assert r.load_mw == 28500.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeActualLoad(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                load_mw=28500.0,
            )


class TestEntsoeActualGenerationSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeActualGeneration(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            production_type="B19",
            generation_mw=5400.0,
        )
        assert r.data_provider == "entsoe"
        assert r.production_type == "B19"


class TestEntsoeCrossborderFlowSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeCrossborderFlow(
            timestamp_utc=self._TS,
            in_area_code="10YGB----------A",
            out_area_code="10YFR-RTE------C",
            flow_mw=1500.0,
        )
        assert r.data_provider == "entsoe"
        assert r.flow_mw == 1500.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeCrossborderFlow(
                timestamp_utc=datetime(2024, 1, 15),
                in_area_code="10YGB----------A",
                out_area_code="10YFR-RTE------C",
                flow_mw=1500.0,
            )


# ---------------------------------------------------------------------------
# LoadForecastTransformer
# ---------------------------------------------------------------------------


class TestLoadForecastTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(LoadForecastTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "load_forecast_mw" in result.columns
        assert "area_code" in result.columns

    def test_three_records(self):
        raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 3

    def test_forecast_values(self):
        raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["load_forecast_mw"][0] - 29100) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_30min_resolution(self):
        raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        ts = result["timestamp_utc"].to_list()
        assert (ts[1] - ts[0]).total_seconds() == 30 * 60

    def test_data_provider(self):
        raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_forecast_horizon_day_ahead(self):
        raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "forecast_horizon" in result.columns
        assert result["forecast_horizon"][0] == "day_ahead"


# ---------------------------------------------------------------------------
# WindSolarForecastTransformer
# ---------------------------------------------------------------------------


class TestWindSolarForecastTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(WindSolarForecastTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "generation_forecast_mw" in result.columns
        assert "production_type" in result.columns
        assert "area_code" in result.columns

    def test_two_production_types(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        prod_types = set(result["production_type"].to_list())
        assert "B19" in prod_types
        assert "B18" in prod_types

    def test_four_records(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4  # 2 types × 2 points

    def test_forecast_values(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        result = self.t.transform(raw).filter(
            pl.col("production_type") == "B19"
        ).sort("timestamp_utc")
        assert abs(result["generation_forecast_mw"][0] - 3200) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_data_provider(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_dedup(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_generation_forecast_mw_column_name(self):
        raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "generation_forecast_mw" in result.columns
        assert "forecast_mw" not in result.columns

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# OutagesGenerationTransformer
# ---------------------------------------------------------------------------


class TestOutagesGenerationTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(OutagesGenerationTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "unavailable_mw" in result.columns
        assert "unit_mrid" in result.columns
        assert "unit_name" in result.columns
        assert "outage_type" in result.columns
        assert "area_code" in result.columns
        assert "production_type" not in result.columns
        assert "available_capacity_mw" not in result.columns

    def test_unit_mrid_values(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        mrids = set(result["unit_mrid"].to_list())
        assert mrids == {"UNIT-DRAX-3", "UNIT-HEYSHAM-2"}

    def test_unit_name_values(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        names = set(result["unit_name"].to_list())
        assert "Drax Unit 3" in names
        assert "Heysham 2" in names

    def test_outage_type_mapping(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        types = set(result["outage_type"].to_list())
        assert types == {"planned", "unplanned"}
        drax = result.filter(pl.col("unit_mrid") == "UNIT-DRAX-3")
        assert drax["outage_type"][0] == "planned"
        heysham = result.filter(pl.col("unit_mrid") == "UNIT-HEYSHAM-2")
        assert heysham["outage_type"][0] == "unplanned"

    def test_four_records(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4

    def test_unavailable_mw_values(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw).sort(["unit_mrid", "timestamp_utc"])
        drax = result.filter(pl.col("unit_mrid") == "UNIT-DRAX-3")
        assert abs(drax["unavailable_mw"][0] - 800) < 0.1
        heysham = result.filter(pl.col("unit_mrid") == "UNIT-HEYSHAM-2")
        assert abs(heysham["unavailable_mw"][0] - 1200) < 0.1

    def test_dedup_on_timestamp_unit(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_ingested_at_present(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "ingested_at" in result.columns

    def test_data_provider(self):
        raw = _make_df_from_xml("outages_generation_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# InstalledCapacityTransformer
# ---------------------------------------------------------------------------


class TestInstalledCapacityTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(InstalledCapacityTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "capacity_mw" in result.columns
        assert "production_type" in result.columns
        assert "area_code" in result.columns

    def test_two_production_types(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw)
        prod_types = set(result["production_type"].to_list())
        assert "B19" in prod_types
        assert "B18" in prod_types

    def test_two_records(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 2

    def test_capacity_values(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw).filter(
            pl.col("production_type") == "B19"
        )
        assert abs(result["capacity_mw"][0] - 15200) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_yearly_resolution_parsed(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw)
        # P1Y maps to 365 days timedelta — check resolution string is present
        assert "resolution" in result.columns

    def test_data_provider(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_capacity_mw_column_name(self):
        raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "capacity_mw" in result.columns
        assert "installed_capacity_mw" not in result.columns

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# New schema validation
# ---------------------------------------------------------------------------


class TestEntsoeLoadForecastSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeLoadForecast(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            load_forecast_mw=29100.0,
        )
        assert r.data_provider == "entsoe"
        assert r.load_forecast_mw == 29100.0
        assert r.forecast_horizon == "day_ahead"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeLoadForecast(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                load_forecast_mw=29100.0,
            )


class TestEntsoeWindSolarForecastSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeWindSolarForecast(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            production_type="B19",
            generation_forecast_mw=3200.0,
        )
        assert r.data_provider == "entsoe"
        assert r.production_type == "B19"
        assert r.generation_forecast_mw == 3200.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeWindSolarForecast(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                production_type="B19",
                generation_forecast_mw=3200.0,
            )


class TestEntsoeOutagesGenerationSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeOutagesGeneration(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            unit_mrid="UNIT-DRAX-3",
            unit_name="Drax Unit 3",
            outage_type="planned",
            unavailable_mw=800.0,
        )
        assert r.data_provider == "entsoe"
        assert r.unit_mrid == "UNIT-DRAX-3"
        assert r.unit_name == "Drax Unit 3"
        assert r.outage_type == "planned"
        assert r.unavailable_mw == 800.0

    def test_unit_name_optional(self):
        r = EntsoeOutagesGeneration(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            unit_mrid="UNIT-DRAX-3",
            outage_type="planned",
            unavailable_mw=800.0,
        )
        assert r.unit_name == ""

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeOutagesGeneration(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                unit_mrid="UNIT-DRAX-3",
                outage_type="planned",
                unavailable_mw=800.0,
            )


class TestEntsoeInstalledCapacitySchema:
    _TS = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeInstalledCapacity(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            production_type="B19",
            capacity_mw=15200.0,
        )
        assert r.data_provider == "entsoe"
        assert r.capacity_mw == 15200.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeInstalledCapacity(
                timestamp_utc=datetime(2024, 1, 1),
                area_code="10YGB----------A",
                production_type="B19",
                capacity_mw=15200.0,
            )


# ---------------------------------------------------------------------------
# GenerationForecastTransformer
# ---------------------------------------------------------------------------


class TestGenerationForecastTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(GenerationForecastTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("generation_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "generation_forecast_mw" in result.columns
        assert "production_type" in result.columns
        assert "area_code" in result.columns

    def test_two_production_types(self):
        raw = _make_df_from_xml("generation_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        prod_types = set(result["production_type"].to_list())
        assert "B01" in prod_types
        assert "B16" in prod_types

    def test_four_records(self):
        raw = _make_df_from_xml("generation_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4  # 2 types × 2 points

    def test_forecast_values(self):
        raw = _make_df_from_xml("generation_forecast_gb.xml", "quantity")
        result = self.t.transform(raw).filter(
            pl.col("production_type") == "B01"
        ).sort("timestamp_utc")
        assert abs(result["generation_forecast_mw"][0] - 1100) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("generation_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_data_provider(self):
        raw = _make_df_from_xml("generation_forecast_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# LoadForecastWeeklyTransformer
# ---------------------------------------------------------------------------


class TestLoadForecastWeeklyTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(LoadForecastWeeklyTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("load_forecast_weekly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "load_forecast_mw" in result.columns
        assert "area_code" in result.columns

    def test_one_record(self):
        raw = _make_df_from_xml("load_forecast_weekly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 1

    def test_forecast_value(self):
        raw = _make_df_from_xml("load_forecast_weekly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert abs(result["load_forecast_mw"][0] - 31500) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("load_forecast_weekly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_data_provider(self):
        raw = _make_df_from_xml("load_forecast_weekly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_forecast_horizon_week_ahead(self):
        raw = _make_df_from_xml("load_forecast_weekly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "forecast_horizon" in result.columns
        assert result["forecast_horizon"][0] == "week_ahead"

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# LoadForecastMonthlyTransformer
# ---------------------------------------------------------------------------


class TestLoadForecastMonthlyTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(LoadForecastMonthlyTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("load_forecast_monthly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "load_forecast_mw" in result.columns
        assert "forecast_horizon" in result.columns
        assert "area_code" in result.columns

    def test_forecast_horizon_month_ahead(self):
        raw = _make_df_from_xml("load_forecast_monthly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["forecast_horizon"][0] == "month_ahead"

    def test_forecast_values(self):
        raw = _make_df_from_xml("load_forecast_monthly_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["load_forecast_mw"][0] - 30200) < 0.1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# LoadForecastYearlyTransformer
# ---------------------------------------------------------------------------


class TestLoadForecastYearlyTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(LoadForecastYearlyTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("load_forecast_yearly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "load_forecast_mw" in result.columns
        assert "forecast_horizon" in result.columns
        assert "area_code" in result.columns

    def test_forecast_horizon_year_ahead(self):
        raw = _make_df_from_xml("load_forecast_yearly_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["forecast_horizon"][0] == "year_ahead"

    def test_forecast_values(self):
        raw = _make_df_from_xml("load_forecast_yearly_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["load_forecast_mw"][0] - 31500) < 0.1

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# ForecastMarginTransformer
# ---------------------------------------------------------------------------


class TestForecastMarginTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ForecastMarginTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("forecast_margin_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "forecast_margin_mw" in result.columns
        assert "area_code" in result.columns

    def test_forecast_margin_values(self):
        raw = _make_df_from_xml("forecast_margin_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["forecast_margin_mw"][0] - 4200) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("forecast_margin_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# NetTransferCapacityTransformer
# ---------------------------------------------------------------------------


class TestNetTransferCapacityTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(NetTransferCapacityTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("net_transfer_capacity_gb_fr.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "ntc_mw" in result.columns
        assert "in_area_code" in result.columns
        assert "out_area_code" in result.columns

    def test_domain_codes_preserved(self):
        raw = _make_df_from_xml("net_transfer_capacity_gb_fr.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert result["in_area_code"][0] == "10YGB----------A"
        assert result["out_area_code"][0] == "10YFR-RTE------C"

    def test_three_records(self):
        raw = _make_df_from_xml("net_transfer_capacity_gb_fr.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 3

    def test_ntc_values(self):
        raw = _make_df_from_xml("net_transfer_capacity_gb_fr.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["ntc_mw"][0] - 2000) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("net_transfer_capacity_gb_fr.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_out_domain_returns_empty(self):
        raw = pl.DataFrame([{
            "timestamp_utc": datetime(2024, 1, 15, tzinfo=UTC),
            "value": 2000.0,
            "in_domain": "10YGB----------A",
            "resolution": "1:00:00",
        }])
        assert self.t.transform(raw).is_empty()


# ---------------------------------------------------------------------------
# Phase 2 schema validation
# ---------------------------------------------------------------------------


class TestEntsoeGenerationForecastSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeGenerationForecast(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            production_type="B16",
            generation_forecast_mw=4200.0,
        )
        assert r.data_provider == "entsoe"
        assert r.generation_forecast_mw == 4200.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeGenerationForecast(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                production_type="B16",
                generation_forecast_mw=4200.0,
            )


class TestEntsoeLoadForecastWeeklySchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeLoadForecastWeekly(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            load_forecast_mw=31500.0,
        )
        assert r.data_provider == "entsoe"
        assert r.load_forecast_mw == 31500.0
        assert r.forecast_horizon == "week_ahead"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeLoadForecastWeekly(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                load_forecast_mw=31500.0,
            )


class TestEntsoeForecastMarginSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeForecastMargin(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            forecast_margin_mw=4200.0,
        )
        assert r.data_provider == "entsoe"
        assert r.forecast_margin_mw == 4200.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeForecastMargin(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                forecast_margin_mw=4200.0,
            )


class TestEntsoeNetTransferCapacitySchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeNetTransferCapacity(
            timestamp_utc=self._TS,
            in_area_code="10YGB----------A",
            out_area_code="10YFR-RTE------C",
            ntc_mw=2000.0,
        )
        assert r.data_provider == "entsoe"
        assert r.ntc_mw == 2000.0

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeNetTransferCapacity(
                timestamp_utc=datetime(2024, 1, 15),
                in_area_code="10YGB----------A",
                out_area_code="10YFR-RTE------C",
                ntc_mw=2000.0,
            )


# ---------------------------------------------------------------------------
# Phase 3 — parser: new fields (control_area_domain, business_type,
#            flow_direction) and backward-compatibility
# ---------------------------------------------------------------------------


class TestParserPhase3Fields:
    """Parser must emit new fields; existing fixtures get empty strings."""

    def _load(self, filename: str) -> bytes:
        return (FIXTURES / filename).read_bytes()

    def test_existing_fixture_has_new_keys(self):
        """Backward-compatible: price fixture now returns 3 new keys."""
        records = parse_timeseries_xml(
            self._load("day_ahead_prices_gb.xml"), value_tag="price.amount"
        )
        record = records[0]
        assert "control_area_domain" in record
        assert "business_type" in record
        assert "flow_direction" in record

    def test_existing_fixture_control_area_empty(self):
        """Zone-domain fixtures have no controlArea_Domain.mRID → empty string."""
        records = parse_timeseries_xml(
            self._load("actual_load_gb.xml"), value_tag="quantity"
        )
        assert records[0]["control_area_domain"] == ""

    def test_existing_fixture_flow_direction_empty(self):
        """Zone-domain fixtures have no flowDirection.direction → empty string."""
        records = parse_timeseries_xml(
            self._load("actual_load_gb.xml"), value_tag="quantity"
        )
        assert records[0]["flow_direction"] == ""

    def test_imbalance_prices_control_area_populated(self):
        records = parse_timeseries_xml(
            self._load("imbalance_prices_gb.xml"), value_tag="price.amount"
        )
        assert records[0]["control_area_domain"] == "10YGB----------A"

    def test_imbalance_prices_in_domain_empty(self):
        """A85 XML has no in_Domain.mRID → in_domain should be empty."""
        records = parse_timeseries_xml(
            self._load("imbalance_prices_gb.xml"), value_tag="price.amount"
        )
        assert records[0]["in_domain"] == ""

    def test_imbalance_prices_business_types(self):
        records = parse_timeseries_xml(
            self._load("imbalance_prices_gb.xml"), value_tag="price.amount"
        )
        business_types = {r["business_type"] for r in records}
        assert "A19" in business_types
        assert "A20" in business_types

    def test_imbalance_prices_four_records(self):
        """2 TimeSeries × 2 points = 4 records."""
        records = parse_timeseries_xml(
            self._load("imbalance_prices_gb.xml"), value_tag="price.amount"
        )
        assert len(records) == 4

    def test_imbalance_prices_values(self):
        records = parse_timeseries_xml(
            self._load("imbalance_prices_gb.xml"), value_tag="price.amount"
        )
        a19_records = [r for r in records if r["business_type"] == "A19"]
        assert abs(a19_records[0]["value"] - 95.50) < 0.01

    def test_imbalance_volume_flow_direction(self):
        records = parse_timeseries_xml(
            self._load("imbalance_volume_gb.xml"), value_tag="quantity"
        )
        directions = {r["flow_direction"] for r in records}
        assert "A01" in directions
        assert "A02" in directions

    def test_activated_balancing_qty_business_types(self):
        records = parse_timeseries_xml(
            self._load("activated_balancing_qty_gb.xml"), value_tag="quantity"
        )
        business_types = {r["business_type"] for r in records}
        assert "A95" in business_types
        assert "A96" in business_types

    def test_contracted_reserves_control_area(self):
        records = parse_timeseries_xml(
            self._load("contracted_reserves_gb.xml"), value_tag="quantity"
        )
        assert records[0]["control_area_domain"] == "10YGB----------A"


# ---------------------------------------------------------------------------
# Phase 3 — endpoints
# ---------------------------------------------------------------------------


class TestPhase3Endpoints:
    def test_phase3_doc_types_populated(self):
        for name in (
            "imbalance_prices",
            "imbalance_volume",
            "activated_balancing_prices",
            "contracted_reserves",
        ):
            assert name in DOC_TYPES, f"{name} missing from DOC_TYPES"

    def test_imbalance_prices_doc_type(self):
        ip = DOC_TYPES["imbalance_prices"]
        assert ip.document_type == "A85"
        assert ip.process_type is None
        assert ip.domain_style == "control_area"

    def test_imbalance_volume_doc_type(self):
        iv = DOC_TYPES["imbalance_volume"]
        assert iv.document_type == "A86"
        assert iv.process_type is None
        assert iv.domain_style == "control_area"
        assert iv.extra_params == {"businessType": "A19"}

    def test_activated_balancing_qty_doc_type(self):
        assert "activated_balancing_qty" not in DOC_TYPES

    def test_activated_balancing_prices_doc_type(self):
        ab = DOC_TYPES["activated_balancing_prices"]
        assert ab.document_type == "A84"
        assert ab.domain_style == "control_area"
        assert ab.extra_params == {"businessType": "A96"}

    def test_contracted_reserves_doc_type(self):
        cr = DOC_TYPES["contracted_reserves"]
        assert cr.document_type == "A81"
        assert cr.process_type == "A52"
        assert cr.domain_style == "control_area"
        assert cr.extra_params == {"businessType": "B95", "Type_MarketAgreement.Type": "A01"}

    def test_endpoint_domain_styles_match_documented_params(self):
        expected = {
            "day_ahead_prices": "zone",
            "actual_load": "out_bidding_zone",
            "load_forecast": "out_bidding_zone",
            "load_forecast_weekly": "out_bidding_zone",
            "load_forecast_monthly": "out_bidding_zone",
            "load_forecast_yearly": "out_bidding_zone",
            "forecast_margin": "out_bidding_zone",
            "actual_generation": "in_domain",
            "wind_solar_forecast": "in_domain",
            "installed_capacity": "in_domain",
            "generation_forecast": "in_domain",
            "cross_border_flows": "zone_pair",
            "net_transfer_capacity": "zone_pair",
            "outages_generation": "bidding_zone",
        }
        for name, domain_style in expected.items():
            assert DOC_TYPES[name].domain_style == domain_style

    def test_default_control_areas_has_gb(self):
        assert "GB" in DEFAULT_CONTROL_AREAS

    def test_default_control_areas_subset_of_bidding_zones(self):
        for area in DEFAULT_CONTROL_AREAS:
            assert area in BIDDING_ZONES


# ---------------------------------------------------------------------------
# Phase 3 — ImbalancePricesTransformer
# ---------------------------------------------------------------------------


class TestImbalancePricesTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ImbalancePricesTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "price_eur_mwh" in result.columns
        assert "area_code" in result.columns
        assert "direction" in result.columns

    def test_four_records(self):
        """2 business types × 2 points = 4 records."""
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert len(result) == 4

    def test_direction_values(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        dirs = set(result["direction"].to_list())
        assert "long" in dirs
        assert "short" in dirs

    def test_control_area_becomes_area_code(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert result["area_code"][0] == "10YGB----------A"

    def test_price_values(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw).filter(
            pl.col("direction") == "long"
        ).sort("timestamp_utc")
        assert abs(result["price_eur_mwh"][0] - 95.50) < 0.01

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_data_provider(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_ingested_at_present(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert "ingested_at" in result.columns

    def test_dedup(self):
        raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# Phase 3 — ImbalanceVolumeTransformer
# ---------------------------------------------------------------------------


class TestImbalanceVolumeTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ImbalanceVolumeTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "volume_mwh" in result.columns
        assert "direction" in result.columns
        assert "area_code" in result.columns

    def test_four_records(self):
        raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4

    def test_direction_values(self):
        raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
        result = self.t.transform(raw)
        dirs = set(result["direction"].to_list())
        assert "long" in dirs
        assert "short" in dirs

    def test_volume_values(self):
        raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
        result = self.t.transform(raw).filter(
            pl.col("direction") == "long"
        ).sort("timestamp_utc")
        assert abs(result["volume_mwh"][0] - 150) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_ingested_at_present(self):
        raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "ingested_at" in result.columns

    def test_dedup(self):
        raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# Phase 3 — ActivatedBalancingQtyTransformer
# ---------------------------------------------------------------------------


class TestActivatedBalancingQtyTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ActivatedBalancingQtyTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "quantity_mwh" in result.columns
        assert "reserve_type" in result.columns
        assert "direction" in result.columns

    def test_four_records(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4

    def test_reserve_type_values(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        result = self.t.transform(raw)
        rtypes = set(result["reserve_type"].to_list())
        assert "fcr" in rtypes
        assert "afrr" in rtypes

    def test_direction_values(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        result = self.t.transform(raw)
        dirs = set(result["direction"].to_list())
        assert "up" in dirs
        assert "down" in dirs

    def test_fcr_up_qty_values(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        result = self.t.transform(raw).filter(
            (pl.col("reserve_type") == "fcr") & (pl.col("direction") == "up")
        ).sort("timestamp_utc")
        assert abs(result["quantity_mwh"][0] - 320) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_ingested_at_present(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "ingested_at" in result.columns

    def test_dedup(self):
        raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# Phase 3 — ActivatedBalancingPricesTransformer
# ---------------------------------------------------------------------------


class TestActivatedBalancingPricesTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ActivatedBalancingPricesTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "price_eur_mwh" in result.columns
        assert "reserve_type" in result.columns
        assert "direction" in result.columns

    def test_four_records(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert len(result) == 4

    def test_reserve_type_values(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        rtypes = set(result["reserve_type"].to_list())
        assert "fcr" in rtypes
        assert "afrr" in rtypes

    def test_direction_values(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        dirs = set(result["direction"].to_list())
        assert "up" in dirs
        assert "down" in dirs

    def test_fcr_up_price_values(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        result = self.t.transform(raw).filter(
            (pl.col("reserve_type") == "fcr") & (pl.col("direction") == "up")
        ).sort("timestamp_utc")
        assert abs(result["price_eur_mwh"][0] - 110.00) < 0.01

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_ingested_at_present(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        result = self.t.transform(raw)
        assert "ingested_at" in result.columns

    def test_dedup(self):
        raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# Phase 3 — ContractedReservesTransformer
# ---------------------------------------------------------------------------


class TestContractedReservesTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ContractedReservesTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "quantity_mw" in result.columns
        assert "reserve_type" in result.columns

    def test_four_records(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4

    def test_reserve_type_values(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        result = self.t.transform(raw)
        rtypes = set(result["reserve_type"].to_list())
        assert "fcr" in rtypes
        assert "afrr" in rtypes

    def test_quantity_values(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        result = self.t.transform(raw).filter(
            pl.col("reserve_type") == "fcr"
        ).sort("timestamp_utc")
        assert abs(result["quantity_mw"][0] - 500) < 0.1

    def test_timestamp_dtype(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_data_provider(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "entsoe"

    def test_ingested_at_present(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert "ingested_at" in result.columns

    def test_dedup(self):
        raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 4

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# Phase 3 — schema validation
# ---------------------------------------------------------------------------


class TestEntsoeImbalancePricesSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeImbalancePrices(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            direction="long",
            price_eur_mwh=95.50,
        )
        assert r.data_provider == "entsoe"
        assert r.price_eur_mwh == 95.50
        assert r.direction == "long"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeImbalancePrices(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                direction="long",
                price_eur_mwh=95.50,
            )


class TestEntsoeImbalanceVolumeSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeImbalanceVolume(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            direction="long",
            volume_mwh=150.0,
        )
        assert r.data_provider == "entsoe"
        assert r.volume_mwh == 150.0
        assert r.direction == "long"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeImbalanceVolume(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                direction="long",
                volume_mwh=150.0,
            )


class TestEntsoeActivatedBalancingQtySchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeActivatedBalancingQty(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            reserve_type="fcr",
            direction="up",
            quantity_mwh=320.0,
        )
        assert r.data_provider == "entsoe"
        assert r.quantity_mwh == 320.0
        assert r.reserve_type == "fcr"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeActivatedBalancingQty(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                reserve_type="fcr",
                direction="up",
                quantity_mwh=320.0,
            )


class TestEntsoeActivatedBalancingPricesSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeActivatedBalancingPrices(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            reserve_type="fcr",
            direction="up",
            price_eur_mwh=110.0,
        )
        assert r.data_provider == "entsoe"
        assert r.price_eur_mwh == 110.0
        assert r.reserve_type == "fcr"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeActivatedBalancingPrices(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                reserve_type="fcr",
                direction="up",
                price_eur_mwh=110.0,
            )


class TestEntsoeContractedReservesSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsoeContractedReserves(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            reserve_type="fcr",
            quantity_mw=500.0,
        )
        assert r.data_provider == "entsoe"
        assert r.quantity_mw == 500.0
        assert r.reserve_type == "fcr"

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            EntsoeContractedReserves(
                timestamp_utc=datetime(2024, 1, 15),
                area_code="10YGB----------A",
                reserve_type="fcr",
                quantity_mw=500.0,
            )


# ---------------------------------------------------------------------------
# Phase H5 - generation unit sources
# ---------------------------------------------------------------------------


class TestPhaseH5Endpoints:
    def test_h5_doc_types_populated(self):
        for name in (
            "installed_capacity_units",
            "actual_generation_units",
            "water_reservoirs",
            "generation_units_master_data",
        ):
            assert name in DOC_TYPES, f"{name} missing from DOC_TYPES"

    def test_installed_capacity_units_doc_type(self):
        doc_type = DOC_TYPES["installed_capacity_units"]
        assert doc_type.document_type == "A71"
        assert doc_type.process_type == "A33"
        assert doc_type.domain_style == "in_domain"

    def test_actual_generation_units_doc_type(self):
        doc_type = DOC_TYPES["actual_generation_units"]
        assert doc_type.document_type == "A73"
        assert doc_type.process_type == "A16"
        assert doc_type.domain_style == "in_domain"

    def test_water_reservoirs_doc_type(self):
        doc_type = DOC_TYPES["water_reservoirs"]
        assert doc_type.document_type == "A72"
        assert doc_type.process_type == "A16"
        assert doc_type.domain_style == "in_domain"

    def test_generation_units_master_data_doc_type(self):
        doc_type = DOC_TYPES["generation_units_master_data"]
        assert doc_type.document_type == "A95"
        assert doc_type.process_type is None
        assert doc_type.domain_style == "bidding_zone"
        assert doc_type.extra_params == {"BusinessType": "B11"}
        assert doc_type.date_param == "Implementation_DateAndOrTime"


class TestGenerationUnitsMasterDataParser:
    def test_parse_generation_units_master_data(self):
        xml = (FIXTURES / "generation_units_master_data_gb.xml").read_bytes()
        records = parse_generation_units_master_data_xml(xml)

        assert len(records) == 2
        assert records[0]["area_code"] == "10YGB----------A"
        assert records[0]["unit_mrid"] == "UNIT-DRAX-3"
        assert records[0]["unit_name"] == "Drax Unit 3"
        assert records[0]["production_type"] == "B02"
        assert records[0]["implementation_datetime_utc"] == datetime(
            2024, 1, 1, tzinfo=UTC
        )


class TestInstalledCapacityUnitsTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(InstalledCapacityUnitsTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("installed_capacity_units_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "capacity_mw" in result.columns
        assert "unit_mrid" in result.columns
        assert "unit_name" in result.columns

    def test_unit_identity_preserved(self):
        raw = _make_df_from_xml("installed_capacity_units_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert set(result["unit_mrid"].to_list()) == {"UNIT-DRAX-3", "UNIT-HEYSHAM-2"}
        assert "Drax Unit 3" in set(result["unit_name"].to_list())


class TestActualGenerationUnitsTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(ActualGenerationUnitsTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("actual_generation_units_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "generation_mw" in result.columns
        assert "unit_mrid" in result.columns
        assert "production_type" in result.columns

    def test_four_records(self):
        raw = _make_df_from_xml("actual_generation_units_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert len(result) == 4


class TestWaterReservoirsTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(WaterReservoirsTransformer)

    def test_transform_basic(self):
        raw = _make_df_from_xml("water_reservoirs_gb.xml", "quantity")
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "reservoir_mwh" in result.columns
        assert "area_code" in result.columns

    def test_reservoir_values(self):
        raw = _make_df_from_xml("water_reservoirs_gb.xml", "quantity")
        result = self.t.transform(raw).sort("timestamp_utc")
        assert abs(result["reservoir_mwh"][0] - 18000) < 0.1


class TestGenerationUnitsMasterDataTransformer:
    def setup_method(self):
        self.t = _make_entsoe_transformer(GenerationUnitsMasterDataTransformer)

    def test_transform_basic(self):
        records = parse_generation_units_master_data_xml(
            (FIXTURES / "generation_units_master_data_gb.xml").read_bytes()
        )
        result = self.t.transform(pl.DataFrame(records))
        assert not result.is_empty()
        assert "unit_mrid" in result.columns
        assert "unit_name" in result.columns
        assert "production_type" in result.columns

    def test_two_units(self):
        records = parse_generation_units_master_data_xml(
            (FIXTURES / "generation_units_master_data_gb.xml").read_bytes()
        )
        result = self.t.transform(pl.DataFrame(records))
        assert set(result["unit_mrid"].to_list()) == {"UNIT-DRAX-3", "UNIT-HEYSHAM-2"}


class TestEntsoeInstalledCapacityUnitsSchema:
    _TS = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        record = EntsoeInstalledCapacityUnits(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            production_type="B02",
            unit_mrid="UNIT-DRAX-3",
            unit_name="Drax Unit 3",
            capacity_mw=660.0,
        )
        assert record.data_provider == "entsoe"
        assert record.unit_mrid == "UNIT-DRAX-3"


class TestEntsoeActualGenerationUnitsSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        record = EntsoeActualGenerationUnits(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            production_type="B02",
            unit_mrid="UNIT-DRAX-3",
            generation_mw=610.0,
        )
        assert record.data_provider == "entsoe"
        assert record.generation_mw == 610.0


class TestEntsoeWaterReservoirsSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        record = EntsoeWaterReservoirs(
            timestamp_utc=self._TS,
            area_code="10YGB----------A",
            reservoir_mwh=18000.0,
        )
        assert record.data_provider == "entsoe"
        assert record.reservoir_mwh == 18000.0


class TestEntsoeGenerationUnitsMasterDataSchema:
    def test_valid_record(self):
        record = EntsoeGenerationUnitsMasterData(
            area_code="10YGB----------A",
            unit_mrid="UNIT-DRAX-3",
            unit_name="Drax Unit 3",
            production_type="B02",
            implementation_datetime_utc=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert record.data_provider == "entsoe"
        assert record.production_type == "B02"


# ---------------------------------------------------------------------------
# Phase H6 - transmission and market sources
# ---------------------------------------------------------------------------


class TestPhaseH6Endpoints:
    def test_h6_doc_types_populated(self):
        for name in (
            "dc_link_intraday_transfer_limits",
            "commercial_schedules",
            "commercial_schedules_net_positions",
            "redispatching_cross_border",
            "redispatching_internal",
            "countertrading",
            "congestion_management_costs",
            "offered_transfer_capacity_continuous",
            "offered_transfer_capacity_implicit",
            "offered_transfer_capacity_explicit",
            "auction_revenue",
            "transfer_capacity_use",
            "total_nominated_capacity",
            "total_capacity_allocated",
            "congestion_income",
            "net_positions",
        ):
            assert name in DOC_TYPES, f"{name} missing from DOC_TYPES"

    def test_h6_mixed_case_domain_params_preserved_in_metadata(self):
        doc_type = DOC_TYPES["offered_transfer_capacity_continuous"]

        assert doc_type.domain_params == ("In_Domain", "Out_Domain")
        assert doc_type.optional_params == (
            "Auction.Type",
            "Contract_MarketAgreement.Type",
            "Update_DateAndOrTime",
        )

    def test_h6_business_type_variants_are_metadata(self):
        assert DOC_TYPES["redispatching_cross_border"].extra_params == {
            "businessType": "A46"
        }
        assert DOC_TYPES["redispatching_internal"].extra_params == {
            "businessType": "A85"
        }
        assert DOC_TYPES["auction_revenue"].extra_params == {
            "businessType": "B07",
            "contract_MarketAgreement.Type": "A01",
        }


class TestPhaseH6Parser:
    def test_parser_accepts_mixed_case_domain_tags(self):
        records = parse_timeseries_xml(
            (FIXTURES / "h6_market_quantity_gb_fr.xml").read_bytes(),
            value_tag="quantity",
        )

        assert len(records) == 3
        assert records[0]["in_domain"] == "10YGB----------A"
        assert records[0]["out_domain"] == "10YFR-RTE------C"
        assert records[0]["business_type"] == "B05"


class TestPhaseH6QuantityTransformers:
    def test_transform_basic(self):
        raw = _make_df_from_xml("h6_market_quantity_gb_fr.xml", "quantity")
        transformer = _make_entsoe_transformer(DcLinkIntradayTransferLimitsTransformer)

        result = transformer.transform(raw)

        assert not result.is_empty()
        assert {"in_area_code", "out_area_code", "quantity_mw"}.issubset(
            result.columns
        )
        assert abs(result["quantity_mw"][0] - 1200) < 0.1

    def test_dedup(self):
        raw = _make_df_from_xml("h6_market_quantity_gb_fr.xml", "quantity")
        doubled = pl.concat([raw, raw])
        transformer = _make_entsoe_transformer(CommercialSchedulesTransformer)

        result = transformer.transform(doubled)

        assert len(result) == 3

    def test_mixed_case_market_transformer(self):
        raw = _make_df_from_xml("h6_market_quantity_gb_fr.xml", "quantity")
        transformer = _make_entsoe_transformer(
            OfferedTransferCapacityContinuousTransformer
        )

        result = transformer.transform(raw)

        assert result["in_area_code"][0] == "10YGB----------A"
        assert result["out_area_code"][0] == "10YFR-RTE------C"


class TestPhaseH6AmountTransformers:
    def test_transform_basic(self):
        raw = _make_df_from_xml("h6_market_price_gb_fr.xml", "price.amount")
        transformer = _make_entsoe_transformer(CongestionManagementCostsTransformer)

        result = transformer.transform(raw)

        assert not result.is_empty()
        assert {"in_area_code", "out_area_code", "amount_eur"}.issubset(
            result.columns
        )
        assert abs(result["amount_eur"][0] - 42.50) < 0.01

    def test_price_family_is_separate_from_quantity_family(self):
        raw = _make_df_from_xml("h6_market_price_gb_fr.xml", "price.amount")
        transformer = _make_entsoe_transformer(AuctionRevenueTransformer)

        result = transformer.transform(raw)

        assert "amount_eur" in result.columns
        assert "quantity_mw" not in result.columns


class TestEntsoeTransmissionMarketQuantitySchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        record = EntsoeTransmissionMarketQuantity(
            timestamp_utc=self._TS,
            in_area_code="10YGB----------A",
            out_area_code="10YFR-RTE------C",
            quantity_mw=1200.0,
            business_type="B05",
        )

        assert record.data_provider == "entsoe"
        assert record.quantity_mw == 1200.0


class TestEntsoeTransmissionMarketAmountSchema:
    _TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        record = EntsoeTransmissionMarketAmount(
            timestamp_utc=self._TS,
            in_area_code="10YGB----------A",
            out_area_code="10YFR-RTE------C",
            amount_eur=42.50,
            business_type="B10",
        )

        assert record.data_provider == "entsoe"
        assert record.amount_eur == 42.50
