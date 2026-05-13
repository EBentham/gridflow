"""Parametrised alignment test: each registered transformer's silver output must match
CANONICAL_SCHEMA.yaml.

State after F15-A (Wave 1, pre-F15-B):
- 155 non-Open-Meteo entries have TODO_HUMAN_FILL_COLUMNS: true → all skip.
- 6 Open-Meteo entries are authored with canonical post-F15-B names.
  These tests are expected to FAIL until F15-B lands the unit-rename patch
  (BaseOpenMeteoTransformer._UNIT_CONVERSIONS / _PURE_RENAMES).
  Track with xfail markers removed once F15-B is complete.

Module-level note: F15-B is the follow-on plan in the same phase; its edit
site is BaseOpenMeteoTransformer.transform() in silver/openmeteo/historical.py.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

# Registry side-effects (Pitfall 1: must import sub-packages explicitly)
import gridflow.silver.elexon  # noqa: F401
import gridflow.silver.entsoe  # noqa: F401
import gridflow.silver.entsog  # noqa: F401
import gridflow.silver.gie  # noqa: F401
import gridflow.silver.neso  # noqa: F401
import gridflow.silver.openmeteo  # noqa: F401

from gridflow.silver.registry import get_transformer, list_transformers

_CANONICAL_PATH = Path(__file__).resolve().parent.parent.parent / "docs" / "CANONICAL_SCHEMA.yaml"
_CANONICAL: dict[str, Any] = yaml.safe_load(_CANONICAL_PATH.read_text(encoding="utf-8"))


def _load_canonical(source: str, dataset: str) -> dict[str, set[str]] | None:
    """Return column sets from YAML entry, or None if TODO_HUMAN_FILL_COLUMNS."""
    key = f"{source}/{dataset}"
    entry = _CANONICAL["datasets"].get(key)
    if entry is None:
        return None
    biz = entry.get("business_columns", {})
    if biz.get("TODO_HUMAN_FILL_COLUMNS"):
        return None
    bitemporal = set(entry.get("bitemporal_columns", {}).keys())
    business = {k for k in biz if k != "TODO_HUMAN_FILL_COLUMNS"}
    metadata = set(entry.get("metadata_columns", {}).keys())
    return {
        "bitemporal": bitemporal,
        "business": business,
        "metadata": metadata,
        "all": bitemporal | business | metadata,
    }


def _synthetic_bronze_openmeteo(source: str, dataset: str) -> pl.DataFrame:
    """Build a minimal synthetic bronze DataFrame for an Open-Meteo transformer."""
    from gridflow.connectors.openmeteo.endpoints import (
        DEMAND_HOURLY_VARS,
        SOLAR_HOURLY_VARS,
        WIND_ARCHIVE_VARS,
        WIND_FORECAST_VARS,
    )

    var_map = {
        "historical_demand": DEMAND_HOURLY_VARS,
        "forecast_demand": DEMAND_HOURLY_VARS,
        "historical_wind": WIND_ARCHIVE_VARS,
        "forecast_wind": WIND_FORECAST_VARS,
        "historical_solar": SOLAR_HOURLY_VARS,
        "forecast_solar": SOLAR_HOURLY_VARS,
    }
    hourly_vars = var_map[dataset]
    row: dict[str, Any] = {
        "time": "2026-05-01T12:00",
        "location": "london",
        "latitude": 51.5074,
        "longitude": -0.1278,
    }
    for var in hourly_vars:
        row[var] = 10.0
    return pl.DataFrame([row])


def _run_transform_and_add_bitemporal(
    source: str, dataset: str, data_dir: Path
) -> pl.DataFrame | None:
    """Instantiate transformer, transform synthetic bronze, add bitemporal columns.

    Returns None if transform yields empty (acceptable; test will skip).
    """
    transformer = get_transformer(source, dataset, data_dir)

    if source == "open_meteo":
        raw = _synthetic_bronze_openmeteo(source, dataset)
    else:
        return None  # No generic synthetic bronze; entry will be skipped

    clean = transformer.transform(raw)
    if clean.is_empty():
        return None

    return transformer._add_bitemporal_columns(
        clean,
        target_date=date(2026, 5, 1),
        run_id="test-fixture",
        available_at=datetime(2026, 5, 2, 0, 0, 0, tzinfo=UTC),
    )


@pytest.mark.integration
@pytest.mark.parametrize("source,dataset", sorted(list_transformers()))
def test_silver_schema_matches_canonical(
    tmp_path: Path, source: str, dataset: str
) -> None:
    """Each registered transformer's emitted silver schema matches CANONICAL_SCHEMA.yaml.

    Entries with TODO_HUMAN_FILL_COLUMNS: true are skipped (curation pass pending).
    Open-Meteo entries are expected to fail until F15-B lands (xfail once confirmed).
    """
    canonical = _load_canonical(source, dataset)
    if canonical is None:
        pytest.skip(f"{source}/{dataset}: YAML entry not yet curated (TODO_HUMAN_FILL_COLUMNS)")

    (tmp_path / "bronze").mkdir()
    (tmp_path / "silver").mkdir()
    (tmp_path / "gold").mkdir()

    df = _run_transform_and_add_bitemporal(source, dataset, tmp_path)
    if df is None:
        pytest.skip(f"{source}/{dataset}: synthetic bronze yielded zero rows after transform")

    actual = set(df.columns)
    expected = canonical["all"]
    missing = expected - actual
    extra = actual - expected

    assert not missing and not extra, (
        f"{source}/{dataset}: schema mismatch\n"
        f"  missing from silver: {sorted(missing)}\n"
        f"  extra in silver:     {sorted(extra)}\n"
        f"  expected: {sorted(expected)}\n"
        f"  actual:   {sorted(actual)}"
    )
