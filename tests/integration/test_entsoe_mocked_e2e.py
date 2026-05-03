"""Mocked ENTSO-E E2E tests for URL construction and bronze-to-silver flow."""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.client import EntsoeConnector
from gridflow.connectors.entsoe.endpoints import DOC_TYPES
from gridflow.silver.entsoe.actual_generation_units import ActualGenerationUnitsTransformer
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.entsoe.cross_border_flows import CrossBorderFlowsTransformer
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer
from gridflow.silver.entsoe.forecast_margin import ForecastMarginTransformer
from gridflow.silver.entsoe.generation_units_master_data import (
    GenerationUnitsMasterDataTransformer,
)
from gridflow.silver.entsoe.h6_market import (
    AuctionRevenueTransformer,
    CommercialSchedulesNetPositionsTransformer,
    CommercialSchedulesTransformer,
    CongestionIncomeTransformer,
    CongestionManagementCostsTransformer,
    CountertradingTransformer,
    DcLinkIntradayTransferLimitsTransformer,
    NetPositionsTransformer,
    OfferedTransferCapacityContinuousTransformer,
    OfferedTransferCapacityExplicitTransformer,
    OfferedTransferCapacityImplicitTransformer,
    RedispatchingCrossBorderTransformer,
    RedispatchingInternalTransformer,
    TotalCapacityAllocatedTransformer,
    TotalNominatedCapacityTransformer,
    TransferCapacityUseTransformer,
)
from gridflow.silver.entsoe.imbalance_prices import ImbalancePricesTransformer
from gridflow.silver.entsoe.installed_capacity_units import InstalledCapacityUnitsTransformer
from gridflow.silver.entsoe.load_forecast_monthly import LoadForecastMonthlyTransformer
from gridflow.silver.entsoe.load_forecast_yearly import LoadForecastYearlyTransformer
from gridflow.silver.entsoe.outages_h7 import (
    OutagesConsumptionTransformer,
    OutagesOffshoreGridTransformer,
    OutagesProductionTransformer,
    OutagesTransmissionTransformer,
)
from gridflow.silver.entsoe.water_reservoirs import WaterReservoirsTransformer

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"
ENTSOE_BASE = "https://web-api.tp.entsoe.eu"
TARGET_DATE = date(2024, 1, 15)
START = datetime(2024, 1, 15, tzinfo=UTC)
END = datetime(2024, 1, 16, tzinfo=UTC)
ZONE_PAIR_DATASETS = {
    "auction_revenue",
    "commercial_schedules",
    "commercial_schedules_net_positions",
    "congestion_income",
    "countertrading",
    "cross_border_flows",
    "dc_link_intraday_transfer_limits",
    "net_transfer_capacity",
    "outages_transmission",
    "offered_transfer_capacity_continuous",
    "offered_transfer_capacity_explicit",
    "offered_transfer_capacity_implicit",
    "redispatching_cross_border",
    "redispatching_internal",
    "total_capacity_allocated",
    "total_nominated_capacity",
    "transfer_capacity_use",
}
DOMAIN_PARAM_KEYS = {
    "BiddingZone_Domain",
    "In_Domain",
    "Out_Domain",
    "controlArea_Domain",
    "in_Domain",
    "outBiddingZone_Domain",
    "out_Domain",
}
LEGACY_DOMAIN_PARAM_KEYS = {
    "BiddingZone_Domain.mRID",
    "In_Domain.mRID",
    "Out_Domain.mRID",
    "controlArea_Domain.mRID",
    "in_Domain.mRID",
    "outBiddingZone_Domain.mRID",
    "out_Domain.mRID",
}


@pytest.fixture
def entsoe_source_config() -> SourceConfig:
    source = load_settings().get_source_config("entsoe")
    return source.model_copy(update={"api_key": "test-token", "timeout": 5})


def _silver_path(data_dir: Path, dataset: str, target_date: date) -> Path:
    return (
        data_dir
        / "silver"
        / "entsoe"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )


def _write_fixture_to_bronze(tmp_data_dir: Path, dataset: str, fixture_name: str) -> Path:
    body = (FIXTURES / fixture_name).read_bytes()
    response = RawResponse(
        body=body,
        content_type="text/xml",
        source="entsoe",
        dataset=dataset,
        fetched_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        request_url=f"{ENTSOE_BASE}/api",
        request_params={"documentType": DOC_TYPES[dataset].document_type},
        api_version="v1",
        http_status=200,
        data_date=TARGET_DATE,
    )
    return BronzeWriter(tmp_data_dir).write(response)


class TestEntsoeUrlConstructionAllDatasets:
    def test_config_and_doc_types_cover_same_datasets(self) -> None:
        configured_datasets = load_settings().get_source_config("entsoe").datasets
        configured = set(configured_datasets)
        registered = set(DOC_TYPES)

        assert configured == registered
        for dataset, doc_type in DOC_TYPES.items():
            dataset_config = configured_datasets[dataset]
            assert dataset_config.document_type == doc_type.document_type
            assert dataset_config.process_type == doc_type.process_type

    @respx.mock
    @pytest.mark.asyncio
    @pytest.mark.parametrize("dataset", sorted(DOC_TYPES))
    async def test_url_shape_for_every_dataset(
        self,
        dataset: str,
        entsoe_source_config: SourceConfig,
    ) -> None:
        route = respx.get(f"{ENTSOE_BASE}/api").mock(
            return_value=httpx.Response(
                200,
                content=b"<root />",
                headers={"content-type": "text/xml"},
            )
        )

        async with EntsoeConnector(entsoe_source_config) as connector:
            responses = await connector.fetch(dataset=dataset, start=START, end=END)

        assert responses
        assert route.calls

        doc_type = DOC_TYPES[dataset]
        for call in route.calls:
            params = dict(call.request.url.params)

            assert params["documentType"] == doc_type.document_type
            if doc_type.date_param:
                assert params[doc_type.date_param] == "2024-01-15"
                assert "periodStart" not in params
                assert "periodEnd" not in params
            else:
                assert params["periodStart"] == "202401150000"
                assert params["periodEnd"] == "202401160000"
            assert params["securityToken"] == "test-token"

            if doc_type.process_type is None:
                assert "processType" not in params
            else:
                assert params["processType"] == doc_type.process_type

            assert not LEGACY_DOMAIN_PARAM_KEYS.intersection(params)
            assert doc_type.extra_params.items() <= params.items()

            expected_domain_params = (
                set(doc_type.domain_params)
                if doc_type.domain_params
                else set()
            )
            if doc_type.domain_style == "control_area":
                assert "controlArea_Domain" in params
                assert DOMAIN_PARAM_KEYS.intersection(params) == {"controlArea_Domain"}
            elif expected_domain_params:
                assert expected_domain_params <= set(params)
                assert DOMAIN_PARAM_KEYS.intersection(params) == expected_domain_params
            elif doc_type.domain_style in {"zone", "zone_pair"}:
                assert "in_Domain" in params
                assert "out_Domain" in params
                assert DOMAIN_PARAM_KEYS.intersection(params) == {"in_Domain", "out_Domain"}
            elif doc_type.domain_style == "in_domain":
                assert "in_Domain" in params
                assert DOMAIN_PARAM_KEYS.intersection(params) == {"in_Domain"}
            elif doc_type.domain_style == "out_bidding_zone":
                assert "outBiddingZone_Domain" in params
                assert DOMAIN_PARAM_KEYS.intersection(params) == {"outBiddingZone_Domain"}
            elif doc_type.domain_style == "bidding_zone":
                assert "BiddingZone_Domain" in params
                assert DOMAIN_PARAM_KEYS.intersection(params) == {"BiddingZone_Domain"}
            else:
                pytest.fail(f"Unhandled ENTSO-E domain style: {doc_type.domain_style}")

        if dataset in ZONE_PAIR_DATASETS:
            in_key, out_key = doc_type.domain_params or ("in_Domain", "out_Domain")
            assert any(
                dict(call.request.url.params)[in_key]
                != dict(call.request.url.params)[out_key]
                for call in route.calls
            )

    @respx.mock
    @pytest.mark.asyncio
    async def test_connector_sets_data_date_from_period_start(
        self,
        entsoe_source_config: SourceConfig,
    ) -> None:
        respx.get(f"{ENTSOE_BASE}/api").mock(
            return_value=httpx.Response(
                200,
                content=b"<root />",
                headers={"content-type": "text/xml"},
            )
        )

        async with EntsoeConnector(entsoe_source_config) as connector:
            responses = await connector.fetch(
                dataset="actual_load",
                start=START,
                end=END,
            )

        assert responses
        assert {response.data_date for response in responses} == {TARGET_DATE}

    @respx.mock
    @pytest.mark.asyncio
    async def test_h6_optional_filters_preserve_documented_casing(
        self,
        entsoe_source_config: SourceConfig,
    ) -> None:
        route = respx.get(f"{ENTSOE_BASE}/api").mock(
            return_value=httpx.Response(
                200,
                content=b"<root />",
                headers={"content-type": "text/xml"},
            )
        )

        async with EntsoeConnector(entsoe_source_config) as connector:
            await connector.fetch(
                dataset="offered_transfer_capacity_continuous",
                start=START,
                end=END,
                **{
                    "Auction.Type": "A01",
                    "Contract_MarketAgreement.Type": "A01",
                    "Update_DateAndOrTime": "2024-01-15T00:00Z",
                    "auction.Type": "ignored-wrong-case",
                },
            )

        params = dict(route.calls[0].request.url.params)

        assert params["In_Domain"] == "10YGB----------A"
        assert params["Out_Domain"] == "10YFR-RTE------C"
        assert params["Auction.Type"] == "A01"
        assert params["Contract_MarketAgreement.Type"] == "A01"
        assert params["Update_DateAndOrTime"] == "2024-01-15T00:00Z"
        assert "auction.Type" not in params

    @respx.mock
    @pytest.mark.asyncio
    async def test_h7_outage_filters_preserve_documented_casing(
        self,
        entsoe_source_config: SourceConfig,
    ) -> None:
        route = respx.get(f"{ENTSOE_BASE}/api").mock(
            return_value=httpx.Response(
                200,
                content=b"<root />",
                headers={"content-type": "text/xml"},
            )
        )

        async with EntsoeConnector(entsoe_source_config) as connector:
            await connector.fetch(
                dataset="outages_transmission",
                start=START,
                end=END,
                **{
                    "DocStatus": "A05",
                    "mRID": "fixture-transmission-outage-gb-fr-20240115",
                    "PeriodStartUpdate": "2024-01-15T00:00Z",
                    "docStatus": "ignored-wrong-case",
                },
            )

        params = dict(route.calls[0].request.url.params)

        assert params["In_Domain"] == "10YGB----------A"
        assert params["Out_Domain"] == "10YFR-RTE------C"
        assert params["BusinessType"] == "A53"
        assert params["DocStatus"] == "A05"
        assert params["mRID"] == "fixture-transmission-outage-gb-fr-20240115"
        assert params["PeriodStartUpdate"] == "2024-01-15T00:00Z"
        assert "docStatus" not in params

    @respx.mock
    @pytest.mark.asyncio
    async def test_connector_expands_zip_xml_responses(
        self,
        entsoe_source_config: SourceConfig,
    ) -> None:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as archive:
            archive.writestr("001-outage.xml", "<root><TimeSeries /></root>")
            archive.writestr("notes.txt", "ignored")

        respx.get(f"{ENTSOE_BASE}/api").mock(
            return_value=httpx.Response(
                200,
                content=zip_buffer.getvalue(),
                headers={"content-type": "application/zip"},
            )
        )

        async with EntsoeConnector(entsoe_source_config) as connector:
            responses = await connector.fetch(
                dataset="outages_generation",
                start=START,
                end=END,
            )

        assert len(responses) == 6
        assert {response.content_type for response in responses} == {"text/xml"}
        assert {response.data_date for response in responses} == {TARGET_DATE}
        assert all(
            response.request_params["zip_entry"] == "001-outage.xml"
            for response in responses
        )

    def test_bronze_writer_partitions_by_data_date_not_fetched_at(
        self,
        tmp_data_dir: Path,
    ) -> None:
        response = RawResponse(
            body=b"<root />",
            content_type="text/xml",
            source="entsoe",
            dataset="actual_load",
            fetched_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
            request_url=f"{ENTSOE_BASE}/api",
            request_params={"documentType": "A65"},
            http_status=200,
            data_date=TARGET_DATE,
        )

        path = BronzeWriter(tmp_data_dir).write(response)

        assert path.parent == (
            tmp_data_dir / "bronze" / "entsoe" / "actual_load" / "2024" / "01" / "15"
        )
        assert not (
            tmp_data_dir / "bronze" / "entsoe" / "actual_load" / "2026" / "05" / "03"
        ).exists()


class TestEntsoeBronzeToSilverPipeline:
    @pytest.mark.parametrize(
        ("dataset", "fixture_name", "transformer_cls", "required_columns"),
        [
            pytest.param(
                "day_ahead_prices",
                "day_ahead_prices_gb.xml",
                DayAheadPricesTransformer,
                {"timestamp_utc", "area_code", "price_eur_mwh"},
                id="day_ahead_prices",
            ),
            pytest.param(
                "actual_load",
                "actual_load_gb.xml",
                ActualLoadTransformer,
                {"timestamp_utc", "area_code", "load_mw"},
                id="actual_load",
            ),
            pytest.param(
                "cross_border_flows",
                "cross_border_flows_gb_fr.xml",
                CrossBorderFlowsTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "flow_mw"},
                id="cross_border_flows",
            ),
            pytest.param(
                "imbalance_prices",
                "imbalance_prices_gb.xml",
                ImbalancePricesTransformer,
                {"timestamp_utc", "area_code", "direction", "price_eur_mwh"},
                id="imbalance_prices",
            ),
            pytest.param(
                "load_forecast_monthly",
                "load_forecast_monthly_gb.xml",
                LoadForecastMonthlyTransformer,
                {"timestamp_utc", "area_code", "load_forecast_mw", "forecast_horizon"},
                id="load_forecast_monthly",
            ),
            pytest.param(
                "load_forecast_yearly",
                "load_forecast_yearly_gb.xml",
                LoadForecastYearlyTransformer,
                {"timestamp_utc", "area_code", "load_forecast_mw", "forecast_horizon"},
                id="load_forecast_yearly",
            ),
            pytest.param(
                "forecast_margin",
                "forecast_margin_gb.xml",
                ForecastMarginTransformer,
                {"timestamp_utc", "area_code", "forecast_margin_mw"},
                id="forecast_margin",
            ),
            pytest.param(
                "installed_capacity_units",
                "installed_capacity_units_gb.xml",
                InstalledCapacityUnitsTransformer,
                {"timestamp_utc", "area_code", "unit_mrid", "capacity_mw"},
                id="installed_capacity_units",
            ),
            pytest.param(
                "actual_generation_units",
                "actual_generation_units_gb.xml",
                ActualGenerationUnitsTransformer,
                {"timestamp_utc", "area_code", "unit_mrid", "generation_mw"},
                id="actual_generation_units",
            ),
            pytest.param(
                "water_reservoirs",
                "water_reservoirs_gb.xml",
                WaterReservoirsTransformer,
                {"timestamp_utc", "area_code", "reservoir_mwh"},
                id="water_reservoirs",
            ),
            pytest.param(
                "generation_units_master_data",
                "generation_units_master_data_gb.xml",
                GenerationUnitsMasterDataTransformer,
                {"area_code", "unit_mrid", "unit_name", "production_type"},
                id="generation_units_master_data",
            ),
            pytest.param(
                "outages_consumption",
                "outages_consumption_gb.xml",
                OutagesConsumptionTransformer,
                {"timestamp_utc", "area_code", "unavailable_mw", "document_mrid"},
                id="outages_consumption",
            ),
            pytest.param(
                "outages_transmission",
                "outages_transmission_gb_fr.xml",
                OutagesTransmissionTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "asset_mrid"},
                id="outages_transmission",
            ),
            pytest.param(
                "outages_offshore_grid",
                "outages_offshore_grid_gb.xml",
                OutagesOffshoreGridTransformer,
                {"timestamp_utc", "area_code", "asset_mrid", "document_mrid"},
                id="outages_offshore_grid",
            ),
            pytest.param(
                "outages_production",
                "outages_production_gb.xml",
                OutagesProductionTransformer,
                {"timestamp_utc", "area_code", "unit_mrid", "production_type"},
                id="outages_production",
            ),
            pytest.param(
                "dc_link_intraday_transfer_limits",
                "h6_market_quantity_gb_fr.xml",
                DcLinkIntradayTransferLimitsTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="dc_link_intraday_transfer_limits",
            ),
            pytest.param(
                "commercial_schedules",
                "h6_market_quantity_gb_fr.xml",
                CommercialSchedulesTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="commercial_schedules",
            ),
            pytest.param(
                "commercial_schedules_net_positions",
                "h6_market_quantity_gb_fr.xml",
                CommercialSchedulesNetPositionsTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="commercial_schedules_net_positions",
            ),
            pytest.param(
                "redispatching_cross_border",
                "h6_market_quantity_gb_fr.xml",
                RedispatchingCrossBorderTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="redispatching_cross_border",
            ),
            pytest.param(
                "redispatching_internal",
                "h6_market_quantity_gb_fr.xml",
                RedispatchingInternalTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="redispatching_internal",
            ),
            pytest.param(
                "countertrading",
                "h6_market_quantity_gb_fr.xml",
                CountertradingTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="countertrading",
            ),
            pytest.param(
                "offered_transfer_capacity_continuous",
                "h6_market_quantity_gb_fr.xml",
                OfferedTransferCapacityContinuousTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="offered_transfer_capacity_continuous",
            ),
            pytest.param(
                "offered_transfer_capacity_implicit",
                "h6_market_quantity_gb_fr.xml",
                OfferedTransferCapacityImplicitTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="offered_transfer_capacity_implicit",
            ),
            pytest.param(
                "offered_transfer_capacity_explicit",
                "h6_market_quantity_gb_fr.xml",
                OfferedTransferCapacityExplicitTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="offered_transfer_capacity_explicit",
            ),
            pytest.param(
                "transfer_capacity_use",
                "h6_market_quantity_gb_fr.xml",
                TransferCapacityUseTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="transfer_capacity_use",
            ),
            pytest.param(
                "total_nominated_capacity",
                "h6_market_quantity_gb_fr.xml",
                TotalNominatedCapacityTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="total_nominated_capacity",
            ),
            pytest.param(
                "total_capacity_allocated",
                "h6_market_quantity_gb_fr.xml",
                TotalCapacityAllocatedTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="total_capacity_allocated",
            ),
            pytest.param(
                "net_positions",
                "h6_market_quantity_gb_fr.xml",
                NetPositionsTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "quantity_mw"},
                id="net_positions",
            ),
            pytest.param(
                "congestion_management_costs",
                "h6_market_price_gb_fr.xml",
                CongestionManagementCostsTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "amount_eur"},
                id="congestion_management_costs",
            ),
            pytest.param(
                "auction_revenue",
                "h6_market_price_gb_fr.xml",
                AuctionRevenueTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "amount_eur"},
                id="auction_revenue",
            ),
            pytest.param(
                "congestion_income",
                "h6_market_price_gb_fr.xml",
                CongestionIncomeTransformer,
                {"timestamp_utc", "in_area_code", "out_area_code", "amount_eur"},
                id="congestion_income",
            ),
        ],
    )
    def test_realistic_fixture_runs_bronze_to_silver(
        self,
        tmp_data_dir: Path,
        dataset: str,
        fixture_name: str,
        transformer_cls: type,
        required_columns: set[str],
    ) -> None:
        bronze_path = _write_fixture_to_bronze(tmp_data_dir, dataset, fixture_name)
        meta_path = bronze_path.with_suffix("").with_suffix(".meta.json")

        assert bronze_path.exists()
        assert bronze_path.suffix == ".xml"
        assert meta_path.exists()

        transformer = transformer_cls(tmp_data_dir)
        rows = transformer.run(TARGET_DATE)

        assert rows > 0

        silver_path = _silver_path(tmp_data_dir, dataset, TARGET_DATE)
        assert silver_path.exists()

        df = pl.read_parquet(silver_path)
        assert len(df) == rows
        assert required_columns.issubset(df.columns)
        if "data_provider" in df.columns:
            assert df["data_provider"].unique().to_list() == ["entsoe"]
