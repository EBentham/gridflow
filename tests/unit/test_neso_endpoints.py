"""Unit tests for NESO Carbon Intensity endpoint metadata."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import yaml

from gridflow.config.settings import load_settings
from gridflow.connectors.neso.endpoints import (
    DEFAULT_POSTCODE,
    DEFAULT_REGION_ID,
    DEFAULT_STATS_BLOCK_HOURS,
    ENDPOINTS,
    ParserFamily,
    build_path,
)

REF_START = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
REF_END = datetime(2026, 2, 2, 0, 0, tzinfo=UTC)
CATALOG_PATH = Path(__file__).resolve().parents[2] / "docs" / "neso_endpoint_catalog.yaml"


def test_active_neso_datasets_match_configured_inventory() -> None:
    configured = set(load_settings().get_source_config("neso").datasets)
    assert set(ENDPOINTS) == configured


def test_all_documented_endpoint_variants_are_registered() -> None:
    assert len(ENDPOINTS) == 33
    assert {endpoint.parser_family for endpoint in ENDPOINTS.values()} == {
        ParserFamily.INTENSITY,
        ParserFamily.FACTORS,
        ParserFamily.STATS,
        ParserFamily.GENERATION,
        ParserFamily.REGIONAL,
    }


def test_carbon_intensity_range_path() -> None:
    path, values = build_path(ENDPOINTS["carbon_intensity"], start=REF_START, end=REF_END)

    assert path == "/intensity/2026-02-01T00:00Z/2026-02-02T00:00Z"
    assert values == {
        "from_dt": "2026-02-01T00:00Z",
        "to_dt": "2026-02-02T00:00Z",
    }


def test_period_path_uses_default_settlement_period() -> None:
    path, values = build_path(ENDPOINTS["intensity_period"], start=REF_START, end=REF_END)

    assert path == "/intensity/date/2026-02-01/1"
    assert values == {"date": "2026-02-01", "period": 1}
    assert ENDPOINTS["intensity_period"].settlement_period_iteration is True


def test_regional_postcode_defaults_to_known_valid_postcode() -> None:
    path, values = build_path(ENDPOINTS["regional_postcode"], start=REF_START, end=REF_END)

    assert path == f"/regional/postcode/{DEFAULT_POSTCODE}"
    assert values == {"postcode": DEFAULT_POSTCODE}


def test_regional_regionid_defaults_to_london_region() -> None:
    path, values = build_path(
        ENDPOINTS["regional_intensity_regionid"],
        start=REF_START,
        end=REF_END,
    )

    assert path.endswith(f"/regionid/{DEFAULT_REGION_ID}")
    assert values["regionid"] == DEFAULT_REGION_ID


def test_path_overrides_are_recorded() -> None:
    path, values = build_path(
        ENDPOINTS["regional_intensity_postcode"],
        start=REF_START,
        end=REF_END,
        postcode="M1",
    )

    assert path == "/regional/intensity/2026-02-01T00:00Z/2026-02-02T00:00Z/postcode/M1"
    assert values["postcode"] == "M1"


def test_defaulted_path_variables_have_explicit_iteration_semantics() -> None:
    defaulted_variables = {
        dataset: endpoint.default_values
        for dataset, endpoint in ENDPOINTS.items()
        if endpoint.default_values
    }

    assert defaulted_variables == {
        "intensity_period": {"period": 1},
        "intensity_stats_block": {"block": DEFAULT_STATS_BLOCK_HOURS},
        "regional_postcode": {"postcode": DEFAULT_POSTCODE},
        "regional_regionid": {"regionid": DEFAULT_REGION_ID},
        "regional_intensity_fw24h_postcode": {"postcode": DEFAULT_POSTCODE},
        "regional_intensity_fw24h_regionid": {"regionid": DEFAULT_REGION_ID},
        "regional_intensity_fw48h_postcode": {"postcode": DEFAULT_POSTCODE},
        "regional_intensity_fw48h_regionid": {"regionid": DEFAULT_REGION_ID},
        "regional_intensity_pt24h_postcode": {"postcode": DEFAULT_POSTCODE},
        "regional_intensity_pt24h_regionid": {"regionid": DEFAULT_REGION_ID},
        "regional_intensity_postcode": {"postcode": DEFAULT_POSTCODE},
        "regional_intensity_regionid": {"regionid": DEFAULT_REGION_ID},
    }
    assert [
        dataset
        for dataset, endpoint in ENDPOINTS.items()
        if endpoint.settlement_period_iteration
    ] == ["intensity_period"]


def test_all_neso_transformers_are_registered() -> None:
    import gridflow.silver.neso  # noqa: F401
    from gridflow.silver.registry import list_transformers

    registered = {dataset for _, dataset in list_transformers("neso")}
    assert registered == set(ENDPOINTS)


def test_catalog_file_matches_endpoint_registry() -> None:
    catalog = yaml.safe_load(CATALOG_PATH.read_text())
    by_dataset = {entry["dataset"]: entry for entry in catalog["endpoints"]}

    assert set(by_dataset) == set(ENDPOINTS)
    for dataset, endpoint in ENDPOINTS.items():
        entry = by_dataset[dataset]
        assert entry["category"] == endpoint.category
        assert entry["parser_family"] == endpoint.parser_family.value
