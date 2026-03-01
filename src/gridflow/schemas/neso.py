"""Pydantic v2 schemas for NESO / Carbon Intensity silver-layer data contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema

# Carbon intensity index values from the API
IntensityIndex = Literal["very low", "low", "moderate", "high", "very high"]


class CarbonIntensity(BaseSchema):
    """Silver-layer schema for NESO/National Grid carbon intensity data."""

    timestamp_utc: datetime          # Start of the half-hour period
    forecast_gco2_kwh: float | None = None   # Forecast intensity (gCO2/kWh)
    actual_gco2_kwh: float | None = None     # Actual intensity (gCO2/kWh)
    intensity_index: str = ""                # "very low" … "very high"
    data_provider: str = Field(default="neso")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
