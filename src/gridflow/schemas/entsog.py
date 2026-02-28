"""Pydantic v2 schemas for ENTSO-G silver-layer data contracts."""

from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class EntsogPhysicalFlow(BaseSchema):
    """Silver-layer schema for ENTSO-G physical gas flows."""

    timestamp_utc: datetime
    point_key: str
    point_label: str = ""
    operator_key: str = ""
    operator_label: str = ""
    direction_key: str = ""  # entry | exit
    flow_kwh_per_day: float = 0.0
    gcv_kwh_per_m3: float = 0.0
    data_provider: str = Field(default="entsog")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
