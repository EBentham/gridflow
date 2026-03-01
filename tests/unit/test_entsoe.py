"""Unit tests for ENTSO-E connector, parsers, schemas, and silver transformers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from gridflow.connectors.entsoe.endpoints import (
    BIDDING_ZONES,
    DEFAULT_ZONES,
    DOC_TYPES,
    EntsoeDocType,
)
from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import (
    EntsoeActualGeneration,
    EntsoeActualLoad,
    EntsoeCrossborderFlow,
    EntsoeDayAheadPrice,
)
from gridflow.silver.entsoe.actual_generation import ActualGenerationTransformer
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.entsoe.cross_border_flows import CrossBorderFlowsTransformer
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer

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
