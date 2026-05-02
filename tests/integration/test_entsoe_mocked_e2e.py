"""Mocked ENTSO-E E2E tests for URL construction and bronze-to-silver flow."""

from __future__ import annotations

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
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.entsoe.cross_border_flows import CrossBorderFlowsTransformer
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer
from gridflow.silver.entsoe.imbalance_prices import ImbalancePricesTransformer

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"
ENTSOE_BASE = "https://web-api.tp.entsoe.eu"
TARGET_DATE = date(2024, 1, 15)
START = datetime(2024, 1, 15, tzinfo=UTC)
END = datetime(2024, 1, 16, tzinfo=UTC)
ZONE_PAIR_DATASETS = {"cross_border_flows", "net_transfer_capacity"}


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
    def test_config_and_doc_types_cover_same_16_datasets(self) -> None:
        configured = set(load_settings().get_source_config("entsoe").datasets)
        registered = set(DOC_TYPES)

        assert len(configured) == 16
        assert len(registered) == 16
        assert configured == registered

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
            assert params["periodStart"] == "202401150000"
            assert params["periodEnd"] == "202401160000"
            assert params["securityToken"] == "test-token"

            if doc_type.process_type is None:
                assert "processType" not in params
            else:
                assert params["processType"] == doc_type.process_type

            if doc_type.domain_style == "control_area":
                assert "controlArea_Domain" in params
                assert "controlArea_Domain.mRID" not in params
                assert "in_Domain" not in params
                assert "in_Domain.mRID" not in params
                assert "out_Domain" not in params
                assert "out_Domain.mRID" not in params
            else:
                assert "in_Domain" in params
                assert "out_Domain" in params
                assert "in_Domain.mRID" not in params
                assert "out_Domain.mRID" not in params
                assert "controlArea_Domain" not in params
                assert "controlArea_Domain.mRID" not in params

        if dataset in ZONE_PAIR_DATASETS:
            assert any(
                dict(call.request.url.params)["in_Domain"]
                != dict(call.request.url.params)["out_Domain"]
                for call in route.calls
            )


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
