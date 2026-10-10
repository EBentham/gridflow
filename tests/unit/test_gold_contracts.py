"""The NESO forecast-versus-outturn gold view contracts (v0.22-G, ADR-041).

``BASE_GOLD_COLUMNS`` is the manifest projection of the three pre-existing gold
views, recorded on the untouched base (``6085571``) before any ``src/`` edit, so
the projection-parser extraction is pinned byte-for-byte (I-7).
"""

from __future__ import annotations

import pytest

from gridflow.silver.schema_manifest import _gold_sql_columns

BASE_GOLD_COLUMNS: dict[str, tuple[str, ...]] = {
    "gold_gb_day_ahead_benchmark": (
        "timestamp_utc",
        "settlement_date",
        "settlement_period",
        "benchmark_price_gbp_mwh",
        "benchmark_volume_mwh",
        "data_provider_id",
        "available_at",
        "vintage_policy",
    ),
    "gold_uk_imbalance_context": (
        "timestamp_utc",
        "settlement_date",
        "settlement_period",
        "system_sell_price",
        "system_buy_price",
        "net_imbalance_volume",
        "price_derivation_code",
        "available_at",
        "vintage_policy",
        "carbon_intensity_forecast_gco2_kwh",
        "carbon_intensity_actual_gco2_kwh",
        "intensity_index",
    ),
    "gold_eu_gas_storage": (
        "gas_day",
        "country_code",
        "country_name",
        "gas_in_storage_gwh",
        "withdrawal_gwh",
        "injection_gwh",
        "working_gas_volume_gwh",
        "storage_pct_full",
        "trend",
        "data_provider",
        "ingested_at",
    ),
}


@pytest.mark.parametrize("relation", sorted(BASE_GOLD_COLUMNS))
def test_t_c6_existing_gold_manifest_columns_unchanged(relation: str) -> None:
    """T-C6 (I-7): detects the projection-parser extraction changing the manifest
    columns of an existing gold view."""
    assert _gold_sql_columns(relation) == BASE_GOLD_COLUMNS[relation]
