"""V2-FIX-02 regression: NESO regional silver field-extraction.

Live evidence (V1 neso-VALIDATION Findings §2):

For the period-keyed regional payloads
(/regional, /regional/intensity/{from}/{fw24h|fw48h|pt24h},
/regional/intensity/{from}/{to}), the API places `intensity` and
`generationmix` on each *region* dict, not on the *period* dict that
encloses them.

The previous `_rows_from_region_period(region, period)` read those
fields from `period` unconditionally. For the period-keyed branch the
lookup yielded None / empty, so silver rows for the 5 affected
datasets landed with null forecast / actual / index / fuel /
generation_percentage.

The fix reads from `region` first, falling back to `period` so the
region-keyed branch (where data lives on `period`) keeps working.

Fixtures captured live 2026-05-08 from
api.carbonintensity.org.uk/regional/intensity/2026-05-06T00:00Z/fw24h
and the regionid/13 variant.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from gridflow.silver.neso.carbon_intensity import (
    _extract_regional_rows,
    _transform_regional,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso"


def _silver_df(fixture_name: str) -> pl.DataFrame:
    payload = json.loads((FIXTURES / fixture_name).read_text())
    rows = _extract_regional_rows(payload)
    return _transform_regional(pl.DataFrame(rows, infer_schema_length=None))


class TestPeriodKeyedRegionalLivePayload:
    """Live shape: data[i] = {from, to, regions:[{regionid, intensity, generationmix, ...}, ...]}.

    intensity + generationmix live on each region. The pre-V2 silver
    code read them from `period`, dropping all carbon/mix data.
    """

    def test_forecast_intensity_populated_from_region(self) -> None:
        df = _silver_df("regional_intensity_fw24h_period_keyed.json")
        assert df.height > 0, "no rows extracted from period-keyed payload"
        assert df["forecast_gco2_kwh"].null_count() < df.height, (
            "every forecast value is null — _rows_from_region_period is "
            "still reading intensity from `period` instead of `region`"
        )

    def test_generation_mix_populated_from_region(self) -> None:
        df = _silver_df("regional_intensity_fw24h_period_keyed.json")
        assert df.height > 0
        assert df["fuel"].null_count() < df.height, (
            "every fuel value is null — _generation_mix_rows(period) "
            "did not fall back to _generation_mix_rows(region)"
        )
        assert df["generation_percentage"].null_count() < df.height

    def test_18_regions_per_period(self) -> None:
        """Sanity: GB has 17 DNO regions + 1 national row = 18 region
        rows per period in the period-keyed shape."""
        df = _silver_df("regional_intensity_fw24h_period_keyed.json")
        regionids = sorted(df["regionid"].unique().to_list())
        assert len(regionids) >= 17, (
            f"expected ≥17 regionids; got {regionids}"
        )


class TestRegionKeyedRegionalLivePayload:
    """Live shape (regionid/postcode variants): data = {regionid, ...,
    data:[{from, to, intensity, generationmix}, ...]}.

    intensity + generationmix live on each period nested under the
    region. This branch worked correctly before V2; the fix must not
    regress it.
    """

    def test_forecast_intensity_populated_from_period(self) -> None:
        df = _silver_df("regional_intensity_fw24h_region_keyed.json")
        assert df.height > 0, "no rows extracted from region-keyed payload"
        assert df["forecast_gco2_kwh"].null_count() < df.height

    def test_generation_mix_populated_from_period(self) -> None:
        df = _silver_df("regional_intensity_fw24h_region_keyed.json")
        assert df["fuel"].null_count() < df.height
        assert df["generation_percentage"].null_count() < df.height

    def test_single_region_only(self) -> None:
        df = _silver_df("regional_intensity_fw24h_region_keyed.json")
        regionids = df["regionid"].unique().to_list()
        assert regionids == [13], (
            f"region-keyed payload for regionid/13 should yield only "
            f"regionid 13 in silver; got {regionids}"
        )
