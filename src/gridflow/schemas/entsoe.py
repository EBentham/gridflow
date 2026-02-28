"""Pydantic v2 schemas for ENTSO-E silver-layer data contracts."""

from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class EntsoeActualGeneration(BaseSchema):
    """Silver-layer schema for ENTSO-E actual generation per type."""

    timestamp_utc: datetime
    area_code: str
    area_name: str = ""
    production_type: str
    generation_mw: float
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeCrossborderFlow(BaseSchema):
    """Silver-layer schema for ENTSO-E cross-border physical flows."""

    timestamp_utc: datetime
    in_area_code: str
    out_area_code: str
    flow_mw: float
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
