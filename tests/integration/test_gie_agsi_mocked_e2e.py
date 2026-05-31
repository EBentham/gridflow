"""Fixture-backed AGSI bronze-to-silver integration tests."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import yaml

import gridflow.silver.gie  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import load_settings
from gridflow.connectors.base import RawResponse
from gridflow.silver.registry import get_transformer, list_transformers

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "gie"
CATALOG = ROOT / "docs" / "gie_agsi_endpoint_catalog.yaml"
TARGET_DATE = date(2026, 5, 1)
FETCHED_AT = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


def _fixture_bytes(filename: str) -> bytes:
    return (FIXTURES / filename).read_bytes()


def _raw_response(
    dataset: str,
    body: bytes,
    *,
    request_params: dict[str, Any] | None = None,
    data_date: date | None = TARGET_DATE,
    fetched_at: datetime = FETCHED_AT,
    page: int = 1,
    total_pages: int = 1,
) -> RawResponse:
    return RawResponse(
        body=body,
        content_type="application/json",
        source="gie_agsi",
        dataset=dataset,
        fetched_at=fetched_at,
        request_url=f"https://agsi.gie.eu/{dataset}",
        request_params=request_params or {},
        api_version="v1",
        page=page,
        total_pages=total_pages,
        http_status=200,
        data_date=data_date,
    )


def _silver_path(data_dir: Path, dataset: str, target_date: date = TARGET_DATE) -> Path:
    return (
        data_dir
        / "silver"
        / "gie_agsi"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )


def _assert_bronze_sidecar(bronze_path: Path, dataset: str) -> None:
    sidecar = bronze_path.with_name(f"{bronze_path.stem}.meta.json")
    assert sidecar.exists()
    metadata = json.loads(sidecar.read_text())
    for key in {
        "source",
        "dataset",
        "request_url",
        "request_params",
        "data_date",
        "api_version",
        "http_status",
        "body_sha256",
        "body_size_bytes",
        "page",
        "total_pages",
    }:
        assert key in metadata
    assert metadata["source"] == "gie_agsi"
    assert metadata["dataset"] == dataset
    assert metadata["http_status"] == 200
    assert metadata["body_size_bytes"] > 0


def _run_fixture(tmp_data_dir: Path, dataset: str, filename: str) -> pl.DataFrame:
    response = _raw_response(dataset, _fixture_bytes(filename))
    bronze_path = BronzeWriter(tmp_data_dir).write(response)

    assert bronze_path.exists()
    assert "bronze" in bronze_path.parts
    assert "gie_agsi" in bronze_path.parts
    assert dataset in bronze_path.parts
    assert "2026" in bronze_path.parts
    assert "05" in bronze_path.parts
    assert "01" in bronze_path.parts
    _assert_bronze_sidecar(bronze_path, dataset)

    transformer = get_transformer("gie_agsi", dataset, tmp_data_dir)
    rows = transformer.run(TARGET_DATE)
    parquet_path = _silver_path(tmp_data_dir, dataset)

    assert rows > 0
    assert parquet_path.exists()
    assert "year=2026" in parquet_path.parts
    df = pl.read_parquet(parquet_path)
    assert len(df) == rows
    assert df["data_provider"].unique().to_list() == ["gie_agsi"]
    return df


def test_storage_reports_fixture_preserves_required_fields(tmp_data_dir: Path) -> None:
    df = _run_fixture(
        tmp_data_dir,
        "storage_reports",
        "agsi_storage_reports_response.json",
    )

    assert {
        "gas_day",
        "gas_day_end",
        "updated_at",
        "entity_level",
        "entity_code",
        "entity_name",
        "entity_url",
        "gas_in_storage_gwh",
        "injection_gwh",
        "withdrawal_gwh",
        "net_withdrawal_gwh",
        "working_gas_volume_gwh",
        "available_capacity_gwh_per_day",
        "storage_pct_full",
        "status",
        "info",
    } <= set(df.columns)
    assert len(df) == 4
    assert set(df["entity_code"].to_list()) == {
        "EU",
        "DE",
        "21X-DEMO-ALPHA",
        "21W-DEMO-ALPHA-1",
    }


def test_about_summary_fixture_reaches_silver(tmp_data_dir: Path) -> None:
    df = _run_fixture(
        tmp_data_dir,
        "about_summary",
        "agsi_about_summary_response.json",
    )

    assert {"platform", "dataset", "updated_at", "total_companies"} <= set(df.columns)


def test_about_summary_nested_live_shape_reaches_silver(tmp_data_dir: Path) -> None:
    payload = {
        "SSO": {
            "Europe": {
                "Austria": [
                    {
                        "short_name": "Demo Storage",
                        "name": "Demo Storage GmbH",
                        "eic": "25X-DEMO",
                        "publication_link": [{"url": "https://example.test"}],
                        "facilities": [
                            {
                                "eic": "21W-DEMO",
                                "name": "Demo Facility",
                                "country": {"code": "AT", "name": "Austria"},
                                "type": "DSR",
                                "operational_start_date": "2020-01-01",
                            }
                        ],
                        "data": {
                            "type": "SSO",
                            "country": {"code": "AT", "name": "Austria"},
                            "code": "EU",
                            "name": "Europe",
                        },
                    }
                ]
            }
        }
    }
    BronzeWriter(tmp_data_dir).write(_raw_response("about_summary", json.dumps(payload).encode()))

    rows = get_transformer("gie_agsi", "about_summary", tmp_data_dir).run(TARGET_DATE)
    df = pl.read_parquet(_silver_path(tmp_data_dir, "about_summary"))

    assert rows == 2
    assert set(df["entity_level"].to_list()) == {"company", "facility"}
    assert set(df["entity_code"].to_list()) == {"25X-DEMO", "21W-DEMO"}
    assert "publication_link" in df.columns


def test_about_listing_fixture_reaches_silver(tmp_data_dir: Path) -> None:
    response = _raw_response("about_listing", _fixture_bytes("agsi_listing_response.json"))
    bronze_path = BronzeWriter(tmp_data_dir).write(response)

    _assert_bronze_sidecar(bronze_path, "about_listing")
    rows = get_transformer("gie_agsi", "about_listing", tmp_data_dir).run(TARGET_DATE)
    df = pl.read_parquet(_silver_path(tmp_data_dir, "about_listing"))

    assert rows == 7
    assert set(df["entity_level"].to_list()) == {"company", "facility"}
    assert {"entity_code", "entity_name", "country_code", "company_code"} <= set(df.columns)


def test_reference_transformer_falls_back_to_latest_bronze_partition(
    tmp_data_dir: Path,
) -> None:
    response = _raw_response(
        "about_listing",
        _fixture_bytes("agsi_listing_response.json"),
        data_date=None,
        fetched_at=datetime(2026, 5, 4, 12, 0, tzinfo=UTC),
    )
    bronze_path = BronzeWriter(tmp_data_dir).write(response)

    assert "2026" in bronze_path.parts
    assert "04" in bronze_path.parts
    rows = get_transformer("gie_agsi", "about_listing", tmp_data_dir).run(TARGET_DATE)
    df = pl.read_parquet(_silver_path(tmp_data_dir, "about_listing"))

    assert rows == 7
    assert set(df["entity_level"].to_list()) == {"company", "facility"}


def test_news_fixture_reaches_silver(tmp_data_dir: Path) -> None:
    df = _run_fixture(tmp_data_dir, "news", "agsi_news_response.json")

    assert {"url", "title", "summary", "details", "start_at", "end_at", "entities"} <= set(
        df.columns
    )


def test_news_item_fixture_reaches_silver(tmp_data_dir: Path) -> None:
    df = _run_fixture(tmp_data_dir, "news_item", "agsi_news_item_response.json")

    assert {"turl", "title", "summary", "details", "entities"} <= set(df.columns)


def test_news_item_listing_shape_is_ignored(tmp_data_dir: Path) -> None:
    BronzeWriter(tmp_data_dir).write(
        _raw_response("news_item", _fixture_bytes("agsi_news_response.json"))
    )

    rows = get_transformer("gie_agsi", "news_item", tmp_data_dir).run(TARGET_DATE)

    assert rows == 0
    assert not _silver_path(tmp_data_dir, "news_item").exists()


def test_unavailability_fixture_reaches_silver(tmp_data_dir: Path) -> None:
    df = _run_fixture(
        tmp_data_dir,
        "unavailability",
        "agsi_unavailability_response.json",
    )

    assert {
        "id",
        "status",
        "country",
        "company",
        "facility",
        "event_start",
        "event_end",
        "unavailable_capacity",
        "reason",
        "info",
    } <= set(df.columns)


def test_unavailability_range_bronze_reaches_each_target_day(tmp_data_dir: Path) -> None:
    writer = BronzeWriter(tmp_data_dir)
    writer.write(
        _raw_response(
            "unavailability",
            _fixture_bytes("agsi_unavailability_response.json"),
            request_params={"start": "2026-05-01", "end": "2026-05-02"},
            data_date=TARGET_DATE,
        )
    )

    target_date = date(2026, 5, 2)
    rows = get_transformer("gie_agsi", "unavailability", tmp_data_dir).run(target_date)
    df = pl.read_parquet(_silver_path(tmp_data_dir, "unavailability", target_date))

    assert rows == 1
    assert df["id"].to_list() == ["unav-demo-1"]


def test_storage_scope_responses_for_same_day_are_count_preserving(tmp_data_dir: Path) -> None:
    payload = json.loads(_fixture_bytes("agsi_storage_reports_response.json"))
    rows = payload["data"]
    writer = BronzeWriter(tmp_data_dir)
    scoped = [
        ("aggregate_type", rows[0], {"type": "EU"}),
        ("country", rows[1], {"country": "DE"}),
        ("company", rows[2], {"country": "DE", "company": "21X-DEMO-ALPHA"}),
        (
            "facility",
            rows[3],
            {
                "country": "DE",
                "company": "21X-DEMO-ALPHA",
                "facility": "21W-DEMO-ALPHA-1",
            },
        ),
    ]
    for _, row, request_params in scoped:
        body = json.dumps({"last_page": 1, "total": 1, "data": [row]}).encode()
        writer.write(_raw_response("storage_reports", body, request_params=request_params))

    rows_written = get_transformer("gie_agsi", "storage_reports", tmp_data_dir).run(TARGET_DATE)
    df = pl.read_parquet(_silver_path(tmp_data_dir, "storage_reports"))

    assert rows_written == 4
    assert set(df["entity_level"].to_list()) == {
        "aggregate_type",
        "country",
        "company",
        "facility",
    }


def test_active_agsi_datasets_have_registered_silver_transformers_or_deferral() -> None:
    catalog = yaml.safe_load(CATALOG.read_text())
    active_catalog_ids = {row["id"] for row in catalog["endpoints"] if row["status"] == "active"}
    active_config_ids = set(load_settings().get_source_config("gie_agsi").datasets)
    registered = {dataset for _, dataset in list_transformers("gie_agsi")}

    assert active_catalog_ids <= active_config_ids
    assert active_catalog_ids <= registered
    assert "storage" in active_config_ids
