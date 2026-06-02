"""Pydantic v2 schemas for NESO Carbon Intensity silver data contracts."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 - Pydantic needs this at runtime.
from typing import Literal

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema

IntensityIndex = Literal["very low", "low", "moderate", "high", "very high"]


class _NesoBase(BaseSchema):
    data_provider: str = Field(default="neso")


class _TimestampedNesoBase(_NesoBase):
    timestamp_utc: datetime
    period_end_utc: datetime | None = None

    @field_validator("timestamp_utc", "period_end_utc")
    @classmethod
    def must_be_utc(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("NESO timestamps must be timezone-aware (UTC)")
        return value


class CarbonIntensity(_TimestampedNesoBase):
    """Silver schema for national Carbon Intensity records."""

    forecast_gco2_kwh: float | None = None
    actual_gco2_kwh: float | None = None
    intensity_index: str = ""


class CarbonIntensityStats(_TimestampedNesoBase):
    """Silver schema for national Carbon Intensity statistics."""

    max_gco2_kwh: float | None = None
    average_gco2_kwh: float | None = None
    min_gco2_kwh: float | None = None
    intensity_index: str = ""


class CarbonIntensityFactor(_NesoBase):
    """Silver schema for generation fuel emission factors."""

    fuel: str
    factor_gco2_kwh: float | None = None


class GenerationMix(_TimestampedNesoBase):
    """Silver schema for national generation mix rows."""

    fuel: str
    generation_percentage: float | None = None


class RegionalIntensity(_TimestampedNesoBase):
    """Silver schema for regional intensity and generation mix rows."""

    regionid: int | None = None
    dnoregion: str = ""
    shortname: str = ""
    # The all-regions endpoints (regional_current, regional_intensity_fw*, …)
    # emit no postcode — the transformer carries it through as null — while the
    # postcode-specific endpoints populate it. Nullable to match what the
    # transformer actually emits (VTA-SCHEMA-01: schema describes real output),
    # consistent with regionid above. Avoids a 100%-of-rows fail-soft warning on
    # every all-regions run.
    postcode: str | None = None
    forecast_gco2_kwh: float | None = None
    actual_gco2_kwh: float | None = None
    intensity_index: str = ""
    fuel: str = ""
    generation_percentage: float | None = None
