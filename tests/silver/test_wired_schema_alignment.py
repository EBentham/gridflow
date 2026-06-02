"""VT1 Wave 2: the newly-wired ``schema_cls`` must MATCH its transformer output.

The Wave-1 central validator (``BaseSilverTransformer._validate_against_schema``) is
**fail-soft**: a mis-wired schema (e.g. a required field the transformer never emits) is
counted + logged but never raises, so it would ship silently as permanent
``completed_with_warnings`` and quietly defeat the keystone. The Elexon datasets already have
that guard (``tests/contracts/test_elexon_schema_alignment.py``); the ENTSO-E ``none`` set,
``entsog/physical_flows``, ``gie`` storage/lng, and the NESO families had **no** such check —
green there meant "nothing raised", not "schema matches output".

This module closes that gap: for each newly-wired non-Elexon dataset it loads the real bronze
fixture, runs ``transform()``, then drives the output through the **exact** central validator and
asserts **zero** validation failures on healthy data. A required-field miss or a type mismatch
fails loudly here even though ``run()`` itself stays fail-soft. (Open-Meteo is already covered via
``run()`` in ``tests/unit/test_openmeteo_canonical_schema.py``; Elexon via the contract test.)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

# Registry side-effects: importing each silver sub-package triggers
# register_transformer() for its datasets (Pitfall 1 — same as the canonical
# alignment test). Without this, get_transformer("entsoe", ...) fails when this
# module runs in isolation.
import gridflow.silver.entsoe  # noqa: F401
import gridflow.silver.entsog  # noqa: F401
import gridflow.silver.gie  # noqa: F401
import gridflow.silver.neso  # noqa: F401
from gridflow.connectors.entsoe.parsers import (
    parse_generation_units_master_data_xml,
    parse_timeseries_xml,
)
from gridflow.connectors.neso.endpoints import ParserFamily
from gridflow.silver.entsog.physical_flows import PhysicalFlowsTransformer
from gridflow.silver.gie.agsi import GasStorageTransformer
from gridflow.silver.gie.alsi import LNGTerminalTransformer
from gridflow.silver.neso.carbon_intensity import (
    GenericNesoJsonTransformer,
    _extract_rows,
)
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from gridflow.silver.base import BaseSilverTransformer

_FIXTURES = Path(__file__).parent.parent / "fixtures"


def _make(cls: type[BaseSilverTransformer], source: str, dataset: str) -> BaseSilverTransformer:
    """Instantiate a transformer bypassing __init__ (transform() needs no real paths)."""
    t = cls.__new__(cls)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path(f"/tmp/test/bronze/{source}/{dataset}")
    t.silver_dir = Path(f"/tmp/test/silver/{source}/{dataset}")
    return t


def _assert_clean_validation(transformer: BaseSilverTransformer, raw: pl.DataFrame) -> None:
    """transform() on a healthy fixture, then the central validator must report 0 failures.

    Uses the production code path (``_validate_against_schema``) so a mis-wired ``schema_cls``
    (required field the transformer never emits, or a strict-type mismatch on healthy data) is
    caught here even though that path is fail-soft inside ``run()``.
    """
    assert transformer.schema_cls is not None, "schema_cls must be wired for this dataset"
    clean = transformer.transform(raw)
    assert not clean.is_empty(), "fixture produced no rows — cannot verify schema alignment"
    failures = transformer._validate_against_schema(clean)
    assert failures == 0, (
        f"{transformer.source}/{transformer.dataset}: {failures}/{len(clean)} healthy row(s) "
        f"failed {transformer.schema_cls.__name__} — schema does not match transformer output "
        f"(output cols: {sorted(clean.columns)})"
    )


# --- ENTSO-E: the 16 previously-`none` datasets (now wired) -----------------
# (registry dataset, fixture xml, value tag). load_forecast_monthly/yearly inherit
# load_forecast's schema and have their own fixtures.
_ENTSOE_TIMESERIES = [
    ("day_ahead_prices", "day_ahead_prices_gb.xml", "price.amount"),
    ("actual_load", "actual_load_gb.xml", "quantity"),
    ("actual_generation", "actual_generation_gb.xml", "quantity"),
    ("actual_generation_units", "actual_generation_units_gb.xml", "quantity"),
    ("cross_border_flows", "cross_border_flows_gb_fr.xml", "quantity"),
    ("net_transfer_capacity", "net_transfer_capacity_gb_fr.xml", "quantity"),
    ("generation_forecast", "generation_forecast_gb.xml", "quantity"),
    ("wind_solar_forecast", "wind_solar_forecast_gb.xml", "quantity"),
    ("installed_capacity", "installed_capacity_gb.xml", "quantity"),
    ("water_reservoirs", "water_reservoirs_gb.xml", "quantity"),
    ("load_forecast", "load_forecast_gb.xml", "quantity"),
    ("load_forecast_weekly", "load_forecast_weekly_gb.xml", "quantity"),
    ("load_forecast_monthly", "load_forecast_monthly_gb.xml", "quantity"),
    ("load_forecast_yearly", "load_forecast_yearly_gb.xml", "quantity"),
]


@pytest.mark.parametrize(
    "dataset,fixture,value_tag",
    _ENTSOE_TIMESERIES,
    ids=[d for d, _, _ in _ENTSOE_TIMESERIES],
)
def test_entsoe_timeseries_output_matches_schema(
    dataset: str, fixture: str, value_tag: str
) -> None:
    records = parse_timeseries_xml((_FIXTURES / "entsoe" / fixture).read_bytes(), value_tag)
    raw = pl.DataFrame(records)
    _assert_clean_validation(get_transformer("entsoe", dataset, Path("/tmp/x")), raw)


def test_entsoe_installed_capacity_units_output_matches_schema() -> None:
    # Units variant carries unit_mrid; parsed by the same time-series parser.
    records = parse_timeseries_xml(
        (_FIXTURES / "entsoe" / "installed_capacity_units_gb.xml").read_bytes(), "quantity"
    )
    raw = pl.DataFrame(records)
    transformer = get_transformer("entsoe", "installed_capacity_units", Path("/tmp/x"))
    _assert_clean_validation(transformer, raw)


def test_entsoe_generation_units_master_data_output_matches_schema() -> None:
    # Master data uses a dedicated parser (not the time-series one).
    records = parse_generation_units_master_data_xml(
        (_FIXTURES / "entsoe" / "generation_units_master_data_gb.xml").read_bytes()
    )
    raw = pl.DataFrame(records)
    _assert_clean_validation(
        get_transformer("entsoe", "generation_units_master_data", Path("/tmp/x")), raw
    )


# --- ENTSO-G physical_flows -------------------------------------------------
def test_entsog_physical_flows_output_matches_schema() -> None:
    payload = json.loads(
        (_FIXTURES / "entsog" / "physical_flows_response.json").read_text(encoding="utf-8")
    )
    raw = pl.DataFrame(payload.get("operationalData", []))
    _assert_clean_validation(_make(PhysicalFlowsTransformer, "entsog", "physical_flows"), raw)


# --- GIE storage + lng ------------------------------------------------------
def test_gie_storage_output_matches_schema() -> None:
    payload = json.loads((_FIXTURES / "gie" / "agsi_gb_response.json").read_text(encoding="utf-8"))
    raw = pl.DataFrame(payload.get("data", []))
    _assert_clean_validation(_make(GasStorageTransformer, "gie_agsi", "storage"), raw)


def test_gie_lng_output_matches_schema() -> None:
    payload = json.loads((_FIXTURES / "gie" / "alsi_gb_response.json").read_text(encoding="utf-8"))
    raw = pl.DataFrame(payload.get("data", []))
    _assert_clean_validation(_make(LNGTerminalTransformer, "gie_alsi", "lng"), raw)


# --- NESO: one dataset per parser family ------------------------------------
# Each family resolves to a different schema (the family->schema map). We use a
# representative registered dataset per family and shape bronze rows with the
# module's own _extract_rows so the input matches what read_bronze() produces.
_NESO_FAMILY_DATASET = {
    ParserFamily.INTENSITY: "intensity_current",
    ParserFamily.STATS: "intensity_stats",
    ParserFamily.FACTORS: "intensity_factors",
    ParserFamily.GENERATION: "generation_current",
    ParserFamily.REGIONAL: "regional_current",
}

_INTENSITY_PAYLOAD = {
    "data": [
        {"from": "2024-01-15T00:00Z", "to": "2024-01-15T00:30Z",
         "intensity": {"forecast": 245, "actual": 250, "index": "moderate"}},
        {"from": "2024-01-15T00:30Z", "to": "2024-01-15T01:00Z",
         "intensity": {"forecast": 260, "actual": None, "index": "moderate"}},
    ]
}
_STATS_PAYLOAD = {
    "data": [
        {"from": "2024-01-15T00:00Z", "to": "2024-01-16T00:00Z",
         "intensity": {"max": 300, "average": 250, "min": 200, "index": "moderate"}},
    ]
}
_FACTORS_PAYLOAD = {"data": [{"Biomass": 120, "Coal": 937, "Solar": 0}]}
_GENERATION_PAYLOAD = {
    "data": [
        {"from": "2024-01-15T00:00Z", "to": "2024-01-15T00:30Z",
         "generationmix": [{"fuel": "gas", "perc": 40.0}, {"fuel": "wind", "perc": 30.0}]},
    ]
}
_REGIONAL_PAYLOAD = {
    "data": [
        {"from": "2024-01-15T00:00Z", "to": "2024-01-15T00:30Z",
         "regions": [
             {"regionid": 1, "dnoregion": "Scotland", "shortname": "North Scotland",
              "intensity": {"forecast": 80, "index": "low"},
              "generationmix": [{"fuel": "wind", "perc": 70.0}]},
         ]},
    ]
}
_NESO_PAYLOADS = {
    ParserFamily.INTENSITY: _INTENSITY_PAYLOAD,
    ParserFamily.STATS: _STATS_PAYLOAD,
    ParserFamily.FACTORS: _FACTORS_PAYLOAD,
    ParserFamily.GENERATION: _GENERATION_PAYLOAD,
    ParserFamily.REGIONAL: _REGIONAL_PAYLOAD,
}


@pytest.mark.parametrize(
    "family",
    list(_NESO_FAMILY_DATASET),
    ids=[f.value for f in _NESO_FAMILY_DATASET],
)
def test_neso_family_output_matches_schema(family: ParserFamily) -> None:
    dataset = _NESO_FAMILY_DATASET[family]
    transformer = get_transformer("neso", dataset, Path("/tmp/x"))
    assert isinstance(transformer, GenericNesoJsonTransformer)
    assert transformer.parser_family is family
    rows = _extract_rows(_NESO_PAYLOADS[family], family)
    raw = pl.DataFrame(rows, infer_schema_length=None)
    _assert_clean_validation(transformer, raw)
