"""Opt-in live ENTSO-E tests.

Run with:
    uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q
"""

from __future__ import annotations

import os
import shutil
import tomllib
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest
import typer
import yaml
from typer.testing import CliRunner

from gridflow.bronze.writer import BronzeWriter
from gridflow.cli import app, ingest
from gridflow.config.settings import (
    DatasetConfig,
    GridflowConfig,
    PipelineSettings,
    QualityConfig,
    SourceConfig,
    load_settings,
)
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.client import (
    EntsoeConnector,
    _extract_acknowledgement_reason,
)
from gridflow.connectors.entsoe.endpoints import DOC_TYPES
from gridflow.silver.registry import get_transformer

pytestmark = pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTSOE_API_KEY_ENV = "ENTSOE_API_KEY"
LIVE_TARGET_DATE = date(2024, 1, 15)
LIVE_START = datetime(2024, 1, 15, tzinfo=UTC)
LIVE_END = datetime(2024, 1, 16, tzinfo=UTC)
REQUEST_SHAPE_START = datetime(2026, 4, 15, tzinfo=UTC)
REQUEST_SHAPE_END = datetime(2026, 4, 16, tzinfo=UTC)


def _has_entsoe_api_key() -> bool:
    return bool(os.environ.get(ENTSOE_API_KEY_ENV))


requires_entsoe_api_key = pytest.mark.skipif(
    not _has_entsoe_api_key(),
    reason="ENTSOE_API_KEY is required for live ENTSO-E tests",
)


def _redact(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 8:
        return "<redacted>"
    return f"{value[:2]}...{value[-2:]}"


def _diagnostic_context(dataset: str, stage: str, exc: Exception | None = None) -> str:
    detail = f"entsoe/{dataset} failed during {stage}"
    if exc is not None:
        detail = f"{detail}: {type(exc).__name__}: {exc}"

    api_key = os.environ.get(ENTSOE_API_KEY_ENV, "")
    if api_key:
        detail = detail.replace(api_key, _redact(api_key))
    return detail


def _copy_config_with_temp_paths(tmp_path: Path, tmp_data_dir: Path) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    shutil.copy(PROJECT_ROOT / "config" / "sources.yaml", config_dir / "sources.yaml")

    settings = {
        "pipeline": {
            "data_dir": str(tmp_data_dir),
            "log_dir": str(tmp_path / "logs"),
            "duckdb_path": str(tmp_path / "gridflow.duckdb"),
            "default_lookback_hours": 24,
            "max_concurrent_requests": 5,
            "log_level": "INFO",
            "console_log_level": "WARNING",
        },
        "quality": {
            "null_rate_threshold": 0.05,
            "enable_outlier_detection": True,
        },
    }
    (config_dir / "settings.yaml").write_text(yaml.safe_dump(settings, sort_keys=False))
    return config_dir


def _assert_live_responses(dataset: str, responses: list[RawResponse]) -> None:
    assert responses, _diagnostic_context(dataset, "fetch: no responses returned")

    api_key = os.environ.get(ENTSOE_API_KEY_ENV, "")
    for response in responses:
        body_size = len(response.body)
        context = (
            f"{_diagnostic_context(dataset, 'fetch')} "
            f"status={response.http_status} content_type={response.content_type} "
            f"body_size={body_size} request_params={_safe_params(response.request_params)}"
        )
        if api_key:
            assert api_key not in context
        assert response.http_status == 200, context
        assert body_size > 0, context


def _safe_params(params: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _redact(str(value)) if key == "securityToken" else value
        for key, value in params.items()
    }


def _is_no_matching_data_response(response: RawResponse) -> bool:
    reason = _extract_acknowledgement_reason(response.body)
    return "No matching data found" in reason


def _write_live_responses_to_bronze(
    tmp_data_dir: Path,
    dataset: str,
    responses: list[RawResponse],
    target_date: date,
) -> list[Path]:
    writer = BronzeWriter(tmp_data_dir)
    paths: list[Path] = []
    for response in responses:
        response_for_partition = replace(response, data_date=target_date)
        path = writer.write(response_for_partition)
        meta_path = path.with_suffix("").with_suffix(".meta.json")
        assert path.exists(), _diagnostic_context(dataset, "bronze write")
        assert path.suffix == ".xml", _diagnostic_context(dataset, "bronze content type")
        assert meta_path.exists(), _diagnostic_context(dataset, "bronze metadata")
        paths.append(path)
    return paths


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


def _assert_silver_output(
    tmp_data_dir: Path,
    dataset: str,
    target_date: date,
    rows: int,
) -> None:
    assert rows > 0, _diagnostic_context(dataset, "transform: zero rows")
    path = _silver_path(tmp_data_dir, dataset, target_date)
    assert path.exists(), _diagnostic_context(dataset, f"silver missing: {path}")

    df = pl.read_parquet(path)
    # Data may span multiple date-partitioned files (e.g. CET vs UTC boundary),
    # so verify total row count across all silver files for this dataset.
    silver_base = tmp_data_dir / "silver" / "entsoe" / dataset
    total_silver_rows = sum(
        len(pl.read_parquet(p)) for p in silver_base.rglob("*.parquet")
    )
    assert total_silver_rows == rows, _diagnostic_context(dataset, "silver row count mismatch")
    if "data_provider" in df.columns:
        assert df["data_provider"].unique().to_list() == ["entsoe"]


def _entsoe_live_config() -> SourceConfig:
    config = load_settings().get_source_config("entsoe")
    if not config.api_key:
        pytest.fail("ENTSOE_API_KEY is required for live ENTSO-E tests")
    return config


class TestEntsoeLivePrerequisites:
    def test_live_marker_registered(self) -> None:
        data = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
        markers = data["tool"]["pytest"]["ini_options"]["markers"]

        assert any(marker.startswith("live:") for marker in markers)

    def test_entsoe_config_and_doc_types_cover_same_datasets(self) -> None:
        configured_datasets = load_settings().get_source_config("entsoe").datasets
        configured = set(configured_datasets)
        registered = set(DOC_TYPES)

        assert configured == registered
        for dataset, doc_type in DOC_TYPES.items():
            dataset_config = configured_datasets[dataset]
            assert dataset_config.document_type == doc_type.document_type
            assert dataset_config.process_type == doc_type.process_type

    def test_api_key_redaction_never_returns_secret(self, monkeypatch: pytest.MonkeyPatch) -> None:
        secret = "super-secret-entsoe-token"
        monkeypatch.setenv(ENTSOE_API_KEY_ENV, secret)

        redacted = _redact(secret)
        diagnostic = _diagnostic_context("day_ahead_prices", "fetch", RuntimeError(secret))

        assert redacted != secret
        assert secret not in redacted
        assert secret not in diagnostic

    def test_temp_config_points_pipeline_paths_at_tmp_dir(
        self,
        tmp_path: Path,
        tmp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _copy_config_with_temp_paths(tmp_path, tmp_data_dir)
        monkeypatch.chdir(tmp_path)

        settings = load_settings()

        assert settings.pipeline.data_dir == tmp_data_dir
        assert settings.pipeline.log_dir == tmp_path / "logs"
        assert settings.pipeline.duckdb_path == tmp_path / "gridflow.duckdb"

    def test_diagnostic_context_includes_dataset_and_stage(self) -> None:
        diagnostic = _diagnostic_context(
            "actual_load",
            "transform",
            RuntimeError("boom"),
        )

        assert "entsoe/actual_load" in diagnostic
        assert "transform" in diagnostic
        assert "boom" in diagnostic


class TestEntsoeCliFailurePropagation:
    def test_ingest_raises_nonzero_after_dataset_failure(
        self,
        tmp_path: Path,
        tmp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        settings = GridflowConfig(
            pipeline=PipelineSettings(
                data_dir=tmp_data_dir,
                log_dir=tmp_path / "logs",
                duckdb_path=tmp_path / "gridflow.duckdb",
                default_lookback_hours=24,
                log_level="INFO",
                console_log_level="WARNING",
            ),
            quality=QualityConfig(),
            sources={
                "entsoe": SourceConfig(
                    base_url="https://web-api.tp.entsoe.eu",
                    api_key="test-token",
                    timeout=5,
                    datasets={
                        "day_ahead_prices": DatasetConfig(),
                        "actual_load": DatasetConfig(),
                    },
                )
            },
        )

        class FakeConnector:
            async def __aenter__(self) -> FakeConnector:
                return self

            async def __aexit__(self, *exc: object) -> None:
                return None

            async def fetch(
                self,
                dataset: str,
                start: datetime,
                end: datetime,
            ) -> list[RawResponse]:
                if dataset == "actual_load":
                    raise RuntimeError("forced actual_load failure")
                return [
                    RawResponse(
                        body=b"<root />",
                        content_type="text/xml",
                        source="entsoe",
                        dataset=dataset,
                        request_url="https://web-api.tp.entsoe.eu/api",
                        request_params={"documentType": "A44"},
                        http_status=200,
                    )
                ]

        monkeypatch.setattr("gridflow.config.settings.load_settings", lambda: settings)
        monkeypatch.setattr("gridflow.cli._import_connectors", lambda: None)
        monkeypatch.setattr(
            "gridflow.connectors.registry.get_connector",
            lambda source, source_config: FakeConnector(),
        )

        with pytest.raises(typer.Exit) as exc_info:
            ingest(
                source="entsoe",
                dataset="all",
                start="2024-01-15",
                end="2024-01-15",
                last=None,
                all_datasets=False,
            )

        captured = capsys.readouterr()

        assert exc_info.value.exit_code == 1
        assert "actual_load" in captured.err
        assert "forced actual_load failure" in captured.err


class TestEntsoeLiveAllDatasets:
    @requires_entsoe_api_key
    @pytest.mark.live
    def test_live_config_and_doc_types_cover_same_datasets(self) -> None:
        configured_datasets = load_settings().get_source_config("entsoe").datasets
        configured = set(configured_datasets)
        registered = set(DOC_TYPES)

        assert configured == registered
        for dataset, doc_type in DOC_TYPES.items():
            dataset_config = configured_datasets[dataset]
            assert dataset_config.document_type == doc_type.document_type
            assert dataset_config.process_type == doc_type.process_type

    @requires_entsoe_api_key
    @pytest.mark.live
    @pytest.mark.asyncio
    @pytest.mark.parametrize("dataset", sorted(DOC_TYPES))
    async def test_live_fetch_returns_real_xml_for_every_dataset(self, dataset: str) -> None:
        async with EntsoeConnector(_entsoe_live_config()) as connector:
            responses = await connector.fetch(dataset=dataset, start=LIVE_START, end=LIVE_END)

        _assert_live_responses(dataset, responses)

    @requires_entsoe_api_key
    @pytest.mark.live
    @pytest.mark.parametrize("dataset", sorted(DOC_TYPES))
    def test_live_fetch_writes_bronze_and_transforms_to_silver(
        self,
        tmp_data_dir: Path,
        dataset: str,
    ) -> None:
        import gridflow.silver.entsoe  # noqa: F401

        async def _fetch() -> list[RawResponse]:
            async with EntsoeConnector(_entsoe_live_config()) as connector:
                return await connector.fetch(dataset=dataset, start=LIVE_START, end=LIVE_END)

        import asyncio

        responses = asyncio.run(_fetch())
        _assert_live_responses(dataset, responses)
        if all(_is_no_matching_data_response(response) for response in responses):
            pytest.skip(
                f"ENTSO-E returned no matching data for entsoe/{dataset} "
                f"on {LIVE_TARGET_DATE:%Y-%m-%d}"
            )
        _write_live_responses_to_bronze(tmp_data_dir, dataset, responses, LIVE_TARGET_DATE)

        transformer = get_transformer("entsoe", dataset, tmp_data_dir)
        rows = transformer.run(LIVE_TARGET_DATE)

        _assert_silver_output(tmp_data_dir, dataset, LIVE_TARGET_DATE, rows)

    @requires_entsoe_api_key
    @pytest.mark.live
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "dataset",
        [
            "day_ahead_prices",
            "actual_load",
            "actual_generation",
            "actual_generation_units",
            "cross_border_flows",
            "outages_generation",
            "imbalance_prices",
            "generation_units_master_data",
            "dc_link_intraday_transfer_limits",
            "offered_transfer_capacity_continuous",
            "auction_revenue",
        ],
    )
    async def test_live_request_shape_uses_supported_domain_params(
        self,
        dataset: str,
    ) -> None:
        try:
            async with EntsoeConnector(_entsoe_live_config()) as connector:
                responses = await connector.fetch(
                    dataset=dataset,
                    start=REQUEST_SHAPE_START,
                    end=REQUEST_SHAPE_END,
                )
        except httpx.HTTPStatusError as exc:
            assert "Input parameter does not exist" not in str(exc)
            raise

        assert responses
        for response in responses:
            params = response.request_params
            assert "in_Domain.mRID" not in params
            assert "out_Domain.mRID" not in params
            assert "In_Domain.mRID" not in params
            assert "Out_Domain.mRID" not in params
            assert "controlArea_Domain.mRID" not in params
            assert "outBiddingZone_Domain.mRID" not in params
            assert "BiddingZone_Domain.mRID" not in params
            assert (
                "in_Domain" in params
                or "In_Domain" in params
                or "out_Domain" in params
                or "Out_Domain" in params
                or "outBiddingZone_Domain" in params
                or "BiddingZone_Domain" in params
                or "controlArea_Domain" in params
            )


class TestEntsoeLiveCliCommands:
    @requires_entsoe_api_key
    @pytest.mark.live
    def test_pipeline_entsoe_all_last_24h_live(
        self,
        tmp_path: Path,
        tmp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _copy_config_with_temp_paths(tmp_path, tmp_data_dir)
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        result = runner.invoke(app, ["pipeline", "entsoe", "all", "--last", "24h"])

        assert os.environ.get(ENTSOE_API_KEY_ENV, "") not in result.output
        assert result.exit_code == 0, result.output
        assert "FAILED" not in result.output
        _assert_dataset_dirs_exist(tmp_data_dir / "bronze" / "entsoe")
        _assert_any_dataset_dirs_exist(tmp_data_dir / "silver" / "entsoe")

    @requires_entsoe_api_key
    @pytest.mark.live
    def test_ingest_entsoe_all_last_24h_live(
        self,
        tmp_path: Path,
        tmp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _copy_config_with_temp_paths(tmp_path, tmp_data_dir)
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        result = runner.invoke(app, ["ingest", "entsoe", "all", "--last", "24h"])

        assert os.environ.get(ENTSOE_API_KEY_ENV, "") not in result.output
        assert result.exit_code == 0, result.output
        assert "FAILED" not in result.output
        _assert_dataset_dirs_exist(tmp_data_dir / "bronze" / "entsoe")

    @requires_entsoe_api_key
    @pytest.mark.live
    def test_transform_entsoe_all_last_24h_live(
        self,
        tmp_path: Path,
        tmp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _copy_config_with_temp_paths(tmp_path, tmp_data_dir)
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        ingest_result = runner.invoke(app, ["ingest", "entsoe", "all", "--last", "24h"])
        assert ingest_result.exit_code == 0, ingest_result.output

        transform_result = runner.invoke(app, ["transform", "entsoe", "all", "--last", "24h"])

        assert os.environ.get(ENTSOE_API_KEY_ENV, "") not in transform_result.output
        assert transform_result.exit_code == 0, transform_result.output
        assert "FAILED" not in transform_result.output
        _assert_any_dataset_dirs_exist(tmp_data_dir / "silver" / "entsoe")


def _assert_dataset_dirs_exist(source_dir: Path) -> None:
    missing = [dataset for dataset in DOC_TYPES if not (source_dir / dataset).exists()]
    assert not missing, f"Missing ENTSO-E dataset output directories: {missing}"


def _assert_any_dataset_dirs_exist(source_dir: Path) -> None:
    existing = [dataset for dataset in DOC_TYPES if (source_dir / dataset).exists()]
    assert existing, f"No ENTSO-E dataset output directories found under {source_dir}"
