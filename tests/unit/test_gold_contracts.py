"""The NESO forecast-versus-outturn gold view contracts (v0.22-G, ADR-041).

``BASE_GOLD_COLUMNS`` is the manifest projection of the three pre-existing gold
views, recorded on the untouched base (``6085571``) before any ``src/`` edit, so
the projection-parser extraction is pinned byte-for-byte (I-7).
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any

import pytest

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Eligible, Held
from gridflow.gold import contracts
from gridflow.gold.contracts import (
    GOLD_VIEW_CONTRACTS,
    HELD_DIR,
    POINT_IN_TIME_SELECTOR,
    VIEWS_DIR,
    GoldViewContract,
    HoldReason,
    contract_for,
    hold_reasons,
    is_published,
    sql_path,
)
from gridflow.silver.date_columns import DATE_COL_SQL_TYPES
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.schema_manifest import (
    _SERVING_ALIASES,
    _gold_sql_columns,
    get_silver_schema_manifest,
)

SOURCE = "neso_data_portal"
WIND = "gold_gb_wind_forecast_vs_outturn"
IC = "gold_gb_interconnector_limits"
IC_INPUTS = (
    "eleclink",
    "ifa_itl",
    "ifa2_ifa_itl",
    "nemolink_ntc",
    "nsl",
    "viking_link_ntc",
    "brit_ned",
)

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


def _fake_registry(keys: tuple[str, ...]) -> Any:
    """A registry stand-in in which every key's package and record are eligible."""
    package = SimpleNamespace(eligibility=Eligible(status="eligible"))
    family = SimpleNamespace(record=SimpleNamespace(eligibility=None))
    return SimpleNamespace(families={key: (package, family) for key in keys})


def test_t_c1_hold_reasons_are_the_registry_questions_verbatim() -> None:
    """T-C1 (I-4): detects a hold reason that is paraphrased, reordered, dropped, or
    sourced anywhere but the registry's ``Held`` (and the G-2 pairing hold)."""
    loaded = registry_module.load_registry()
    ic = contract_for(IC)
    assert ic.inputs == IC_INPUTS
    expected = []
    for key in IC_INPUTS:
        held = effective_eligibility(*loaded.families[key])
        assert isinstance(held, Held), key
        expected.append(HoldReason(key, held.question, held.unit))
    assert hold_reasons(ic) == tuple(expected)

    wind = contract_for(WIND)
    assert wind.pairing_hold is not None
    assert hold_reasons(wind) == (wind.pairing_hold,)
    assert wind.pairing_hold.subject == "pairing"
    assert wind.pairing_hold.unit == "G-R"
    assert wind.pairing_hold.question.startswith("TODO: Obtain a primary NESO definition")
    assert wind.pairing_hold.question.endswith("prove that equivalence locally.")
    assert "monthly output\u2019s contributing fleet" in wind.pairing_hold.question


@pytest.mark.parametrize("relation", [WIND, IC])
def test_t_c2_held_sql_location_and_no_serving_row(relation: str) -> None:
    """T-C2 (I-4): detects a held view's SQL in the registered directory (or missing
    from the held one), or a serving-alias / manifest row that names it."""
    contract = contract_for(relation)
    assert not is_published(contract)
    path = sql_path(contract)
    assert path == HELD_DIR / f"{contract.sql_stem}.sql"
    assert path.is_file()
    assert not (VIEWS_DIR / f"{contract.sql_stem}.sql").exists()
    assert relation not in {spec.relation_name for spec in _SERVING_ALIASES}
    assert relation not in {entry.relation_name for entry in get_silver_schema_manifest()}


def test_t_c3_a_lifted_hold_moves_the_sql_path() -> None:
    """T-C3 (FM-7): detects ``sql_path`` ignoring the registry, so a lifted upstream
    hold would leave the view held (or the pairing hold would be lost)."""
    ic = contract_for(IC)
    wind = contract_for(WIND)
    fake = _fake_registry(ic.inputs + wind.inputs)
    assert hold_reasons(ic, fake) == ()
    assert is_published(ic, fake)
    assert sql_path(ic, fake) == VIEWS_DIR / "gb_interconnector_limits.sql"
    assert hold_reasons(wind, fake) == (wind.pairing_hold,)
    assert not is_published(wind, fake)
    assert sql_path(wind, fake) == HELD_DIR / "gb_wind_forecast_vs_outturn.sql"


def _subsequence(short: tuple[str, ...], long: tuple[str, ...]) -> bool:
    remaining = iter(long)
    return all(item in remaining for item in short)


def test_t_c4_point_in_time_specs_derive_from_the_inputs() -> None:
    """T-C4 (P-6): detects a G spec whose key, order or tie-break drifts from the
    inputs' generated specs, or that leaks into the silver ``_latest`` registry."""
    wind = contract_for(WIND)
    forecast = LATEST_VIEW_SPECS[(SOURCE, "da_wind_forecast_day_ahead")]
    assert forecast.key_columns == ("datetime_gmt",)
    assert wind.point_in_time.key_columns == ("timestamp_utc",)
    assert wind.point_in_time.order_columns == forecast.order_columns

    ic = contract_for(IC)
    union: set[str] = set()
    for key in ic.inputs:
        generated = LATEST_VIEW_SPECS[(SOURCE, key)]
        union.update(generated.key_columns)
        assert _subsequence(generated.order_columns, ic.point_in_time.order_columns), key
    assert set(ic.point_in_time.key_columns) == {"family"} | union
    assert len(set(ic.point_in_time.key_columns)) == len(ic.point_in_time.key_columns)

    latest_names = {f"silver_{source}_{dataset}" for source, dataset in LATEST_VIEW_SPECS}
    latest_names |= {f"{name}_latest" for name in latest_names}
    for contract in GOLD_VIEW_CONTRACTS:
        spec = contract.point_in_time
        assert spec.mode == "key_latest"
        assert spec.tiebreak_columns is generic._TIEBREAK
        assert spec.completion_relation is None
        assert contract.relation_name not in latest_names
        assert all(dataset != contract.relation_name for _source, dataset in LATEST_VIEW_SPECS)


def test_t_c5_an_ingest_only_input_raises() -> None:
    """T-C5 (FM-14): detects an ingest-only input (no silver record) reading as
    eligible through ``effective_eligibility``'s default rule."""
    loaded = registry_module.load_registry()
    ingest_only = next(
        key
        for key, (_package, family) in sorted(loaded.families.items())
        if family.record is None and family.kind == "tabular" and not family.legacy
    )
    contract = GoldViewContract(
        relation_name="gold_probe",
        inputs=(ingest_only,),
        designated_date_col="settlement_date",
        date_col_sql_type="DATE",
        point_in_time=contract_for(WIND).point_in_time,
    )
    with pytest.raises(ValueError, match=f"{ingest_only} has no silver record"):
        hold_reasons(contract)
    missing = GoldViewContract(
        relation_name="gold_probe",
        inputs=("no_such_family",),
        designated_date_col="settlement_date",
        date_col_sql_type="DATE",
        point_in_time=contract_for(WIND).point_in_time,
    )
    with pytest.raises(KeyError):
        hold_reasons(missing)


def test_t_c7_contract_lookup_and_selector_name() -> None:
    """T-C7 (I-5): detects a contract that cannot be found by relation, an unknown
    relation that does not fail, or a selector name that does not resolve."""
    for contract in GOLD_VIEW_CONTRACTS:
        assert contract_for(contract.relation_name) is contract
        assert contract.sql_stem == contract.relation_name.removeprefix("gold_")
    with pytest.raises(KeyError, match=WIND):
        contract_for("gold_no_such_view")
    module_name, _, name = POINT_IN_TIME_SELECTOR.rpartition(".")
    assert getattr(importlib.import_module(module_name), name) is select_latest_vintage
    wind = contract_for(WIND)
    assert (wind.designated_date_col, wind.date_col_sql_type) == ("settlement_date", "DATE")
    assert DATE_COL_SQL_TYPES[wind.designated_date_col] == wind.date_col_sql_type
    ic = contract_for(IC)
    assert (ic.designated_date_col, ic.date_col_sql_type) == (
        "operational_period_start_gmt",
        "TIMESTAMPTZ",
    )
    assert contracts.POINT_IN_TIME_SELECTOR == POINT_IN_TIME_SELECTOR
