"""Annotation-correctness guard for ENTSO-E PSR code glosses (VTA-ENTSOE-PSR-01).

The data is unaffected — gridflow stores raw PSR codes with no normalisation —
so a value-level test cannot catch a wrong human-readable gloss. This pins the
official code list (entsoe-codes.md §6 / entsoe-py PSRTYPE_MAPPINGS) so a future
regression of the rotated gloss fails.
"""

from __future__ import annotations

import inspect

from gridflow.schemas import entsoe
from gridflow.silver.entsoe import wind_solar_forecast


def test_psr_labels_match_official_codelist() -> None:
    for text in (
        entsoe.EntsoeWindSolarForecast.__doc__,
        inspect.getsource(entsoe.EntsoeWindSolarForecast),  # catches the inline comment
        wind_solar_forecast.WindSolarForecastTransformer.__doc__,
    ):
        assert text is not None
        low = text.lower()
        assert "b16 = solar" in low or "b16=solar" in low
        assert "b18 = wind offshore" in low or "b18=wind offshore" in low
        assert "b19 = wind onshore" in low or "b19=wind onshore" in low
        # guard against the rotated (wrong) gloss returning
        assert "b16=wind" not in low and "b16 = wind" not in low
        assert "b19=solar" not in low and "b19 = solar" not in low
