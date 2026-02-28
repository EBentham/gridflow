"""Shared types and base schema utilities for gridflow data contracts."""

from __future__ import annotations

from datetime import date, datetime, timezone

from pydantic import BaseModel, field_validator


class BaseSchema(BaseModel):
    """Base schema for all gridflow silver-layer models."""

    model_config = {"strict": True, "extra": "ignore"}


class TimestampMixin(BaseModel):
    """Mixin requiring UTC timestamps."""

    timestamp_utc: datetime

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class SettlementPeriodMixin(BaseModel):
    """Mixin for settlement period data."""

    settlement_date: date
    settlement_period: int
