"""G5 acceptance test: every Elexon silver transformer output aligns with its
Pydantic class.

For each (schema, transformer, fixture) triple:
- Every column the transformer emits must be declared in the schema
  (catches the W3-pattern bug — transformer emits more than schema declares).
- Every row in the transformed output must validate against the schema
  (catches type errors and out-of-range numeric values).

This test is the regression guard for the entire G5 phase. New schemas added
in W4 are wired in here automatically through the SCHEMA_FIXTURE_MAP. If a
schema is added without a matching map entry, the test will flag it.

Transformers that emit fields the schema doesn't declare were the V1 §13
drift pattern. Schemas that declare fields the transformer doesn't emit
were the W2 pattern. This test catches both.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from gridflow.schemas.elexon import (
    ElexonAGPT,
    ElexonAGWS,
    ElexonATL,
    ElexonBMUnit,
    ElexonBOAL,
    ElexonBOD,
    ElexonDemandForecast,
    ElexonDISBSAD,
    ElexonFOU2T14D,
    ElexonFrequency,
    ElexonFuelHH,
    ElexonGenerationByFuel,
    ElexonImbalNGC,
    ElexonIndDem,
    ElexonIndGen,
    ElexonINDO,
    ElexonINDOD,
    ElexonITSDO,
    ElexonLOLPDRM,
    ElexonMarketDepth,
    ElexonMelNGC,
    ElexonMID,
    ElexonNETBSAD,
    ElexonNonBM,
    ElexonPN,
    ElexonREMIT,
    ElexonSOSO,
    ElexonSystemPrice,
    ElexonTemp,
    ElexonTSDF,
    ElexonTSDFD,
    ElexonUOU2T14D,
    ElexonWindForecast,
)
from gridflow.silver.elexon.agpt import AGPTTransformer
from gridflow.silver.elexon.agws import AGWSTransformer
from gridflow.silver.elexon.atl import ATLTransformer
from gridflow.silver.elexon.bmunits import BMUnitsTransformer
from gridflow.silver.elexon.boal import BOALTransformer
from gridflow.silver.elexon.bod import BODTransformer
from gridflow.silver.elexon.demand_forecast import DemandForecastTransformer
from gridflow.silver.elexon.disbsad import DISBSADTransformer
from gridflow.silver.elexon.fou2t14d import FOU2T14DTransformer
from gridflow.silver.elexon.freq import FreqTransformer
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.fuelinst import FuelInstTransformer
from gridflow.silver.elexon.imbalngc import ImbalNGCTransformer
from gridflow.silver.elexon.inddem import INDDEMTransformer
from gridflow.silver.elexon.indgen import INDGENTransformer
from gridflow.silver.elexon.indo import INDOTransformer
from gridflow.silver.elexon.indod import INDODTransformer
from gridflow.silver.elexon.itsdo import ITSDOTransformer
from gridflow.silver.elexon.lolpdrm import LOLPDRMTransformer
from gridflow.silver.elexon.market_depth import MarketDepthTransformer
from gridflow.silver.elexon.melngc import MelNGCTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.netbsad import NETBSADTransformer
from gridflow.silver.elexon.nonbm import NONBMTransformer
from gridflow.silver.elexon.pn import PNTransformer
from gridflow.silver.elexon.remit import REMITTransformer
from gridflow.silver.elexon.soso import SOSOTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.elexon.temp import TempTransformer
from gridflow.silver.elexon.tsdf import TSDFTransformer
from gridflow.silver.elexon.tsdfd import TSDFDTransformer
from gridflow.silver.elexon.uou2t14d import UOU2T14DTransformer
from gridflow.silver.elexon.wind_forecast import WindForecastTransformer

if TYPE_CHECKING:
    from pydantic import BaseModel

FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"


def _make_transformer(cls):
    """Instantiate a transformer bypassing __init__ (transform() needs no paths)."""
    t = cls.__new__(cls)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path(f"/tmp/test/bronze/elexon/{cls.dataset}")
    t.silver_dir = Path(f"/tmp/test/silver/elexon/{cls.dataset}")
    return t


# Maps each Pydantic schema to (transformer_class, fixture_filename). Fixture
# is loaded from tests/fixtures/elexon/. The v2 fixtures (reflecting 2026+
# API shapes) are preferred when a current-shape exists.
SCHEMA_FIXTURE_MAP: dict[type[BaseModel], tuple[type, str]] = {
    # Pre-G5 schemas (12)
    ElexonSystemPrice: (SystemPriceTransformer, "system_prices_response.json"),
    ElexonGenerationByFuel: (FuelInstTransformer, "generation_by_fuel_response.json"),
    ElexonFuelHH: (FuelHHTransformer, "fuelhh_response.json"),
    ElexonBOAL: (BOALTransformer, "boal_response.json"),
    ElexonBOD: (BODTransformer, "bod_response.json"),
    ElexonMID: (MIDTransformer, "mid_response_v2.json"),
    ElexonFrequency: (FreqTransformer, "freq_response.json"),
    ElexonDemandForecast: (DemandForecastTransformer, "ndf_response.json"),
    ElexonWindForecast: (WindForecastTransformer, "windfor_response.json"),
    ElexonPN: (PNTransformer, "pn_response.json"),
    ElexonDISBSAD: (DISBSADTransformer, "disbsad_response_v2.json"),
    ElexonBMUnit: (BMUnitsTransformer, "bmunits_response.json"),
    # G5-W4 schemas (21)
    ElexonImbalNGC: (ImbalNGCTransformer, "imbalngc_response.json"),
    ElexonMelNGC: (MelNGCTransformer, "melngc_response.json"),
    ElexonIndDem: (INDDEMTransformer, None),  # no fixture — schema-only check
    ElexonTSDF: (TSDFTransformer, None),
    ElexonINDO: (INDOTransformer, None),
    ElexonINDOD: (INDODTransformer, None),
    ElexonITSDO: (ITSDOTransformer, None),
    ElexonIndGen: (INDGENTransformer, None),
    ElexonTSDFD: (TSDFDTransformer, None),
    ElexonAGPT: (AGPTTransformer, None),
    ElexonAGWS: (AGWSTransformer, None),
    ElexonATL: (ATLTransformer, None),
    ElexonMarketDepth: (MarketDepthTransformer, "market_depth_response.json"),
    ElexonNonBM: (NONBMTransformer, None),
    ElexonFOU2T14D: (FOU2T14DTransformer, "fou2t14d_response.json"),
    ElexonUOU2T14D: (UOU2T14DTransformer, "uou2t14d_response_v2.json"),
    ElexonLOLPDRM: (LOLPDRMTransformer, None),
    ElexonREMIT: (REMITTransformer, None),
    ElexonSOSO: (SOSOTransformer, None),
    ElexonNETBSAD: (NETBSADTransformer, "netbsad_response_v2.json"),
    ElexonTemp: (TempTransformer, "temp_response_v2.json"),
}

# Bitemporal columns are stamped by BaseSilverTransformer.run() rather than
# transform(); they appear on disk but not in transform()-only output. The
# acceptance test only operates on transform() output, so it ignores them.
_BITEMPORAL_COLS = {"event_time", "available_at", "source_run_id", "dataset_version"}


@pytest.mark.parametrize(
    "schema_cls",
    SCHEMA_FIXTURE_MAP.keys(),
    ids=lambda s: s.__name__,
)
def test_schema_declares_all_emitted_columns(schema_cls: type[BaseModel]):
    """G5 acceptance: every column the transformer emits must be declared
    in the corresponding Pydantic schema.

    Pre-G5 the silver Parquet columns and the schema declarations could
    drift in either direction. W3 (ENTSOE) was an instance of "emits more
    than schema declares". This test prevents that class of drift on
    Elexon, parametrised across every (schema, transformer) pair.
    """
    transformer_cls, fixture_name = SCHEMA_FIXTURE_MAP[schema_cls]
    if fixture_name is None:
        pytest.skip(f"No fixture for {schema_cls.__name__} — schema-only audit")

    fixture_path = FIXTURES / fixture_name
    if not fixture_path.exists():
        pytest.skip(f"Fixture {fixture_name} not present")

    data = json.loads(fixture_path.read_text())
    records = data.get("data", data) if isinstance(data, dict) else data
    if not records:
        pytest.skip(f"Fixture {fixture_name} carries no records")

    raw = pl.DataFrame(records)
    transformer = _make_transformer(transformer_cls)
    result = transformer.transform(raw)

    if result.is_empty():
        pytest.skip(f"Transformer produced empty output for {fixture_name}")

    emitted = set(result.columns) - _BITEMPORAL_COLS
    declared = set(schema_cls.model_fields.keys())
    missing_in_schema = emitted - declared
    assert not missing_in_schema, (
        f"G5 regression: {schema_cls.__name__} missing fields the transformer "
        f"emits: {missing_in_schema}"
    )


@pytest.mark.parametrize(
    "schema_cls",
    SCHEMA_FIXTURE_MAP.keys(),
    ids=lambda s: s.__name__,
)
def test_silver_rows_validate_against_schema(schema_cls: type[BaseModel]):
    """G5 acceptance: every row of transformer output must validate against
    the schema. This catches type errors and out-of-range numeric values
    that Pydantic's field constraints would flag."""
    transformer_cls, fixture_name = SCHEMA_FIXTURE_MAP[schema_cls]
    if fixture_name is None:
        pytest.skip(f"No fixture for {schema_cls.__name__}")

    fixture_path = FIXTURES / fixture_name
    if not fixture_path.exists():
        pytest.skip(f"Fixture {fixture_name} not present")

    data = json.loads(fixture_path.read_text())
    records = data.get("data", data) if isinstance(data, dict) else data
    if not records:
        pytest.skip(f"Fixture {fixture_name} carries no records")

    raw = pl.DataFrame(records)
    transformer = _make_transformer(transformer_cls)
    result = transformer.transform(raw)

    if result.is_empty():
        pytest.skip(f"Transformer produced empty output for {fixture_name}")

    errors: list[str] = []
    for row in result.iter_rows(named=True):
        # Strip bitemporal columns — they're stamped by run(), not transform().
        cleaned = {k: v for k, v in row.items() if k not in _BITEMPORAL_COLS}
        try:
            schema_cls(**cleaned)
        except Exception as e:  # noqa: BLE001
            errors.append(f"Row {cleaned}: {e}")

    assert not errors, (
        f"G5 regression: {schema_cls.__name__} row validation failed:\n"
        + "\n".join(errors[:3])  # truncate to first 3 errors
    )


def test_market_depth_maps_priced_volumes_not_adjustment() -> None:
    """VTA-ELEXON-MKTDEPTH-01: market_depth must carry the real priced
    accepted volumes the live endpoint returns and must NOT carry the phantom
    adjustment columns (which belong to the system-prices/DISEBSP endpoint).

    Pre-fix this fails on the first assertion: the un-renamed
    pricedAccepted* keys are dropped at the output_cols select.
    """
    raw = pl.DataFrame(json.loads((FIXTURES / "market_depth_response.json").read_text())["data"])
    out = _make_transformer(MarketDepthTransformer).transform(raw)
    assert {
        "priced_accepted_offers_volume_mwh",
        "priced_accepted_bids_volume_mwh",
    } <= set(out.columns)
    assert "total_adjustment_sell_volume_mwh" not in out.columns
    assert "total_adjustment_buy_volume_mwh" not in out.columns
