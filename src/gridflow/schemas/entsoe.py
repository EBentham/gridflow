"""Pydantic v2 schemas for ENTSO-E silver-layer data contracts."""

from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class EntsoeDayAheadPrice(BaseSchema):
    """Silver-layer schema for ENTSO-E day-ahead market prices."""

    timestamp_utc: datetime
    area_code: str  # EIC bidding zone mRID
    price_eur_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeActualLoad(BaseSchema):
    """Silver-layer schema for ENTSO-E actual total load."""

    timestamp_utc: datetime
    area_code: str
    load_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


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


class EntsoeLoadForecast(BaseSchema):
    """Silver-layer schema for ENTSO-E day-ahead load forecast (A65/A01)."""

    timestamp_utc: datetime
    area_code: str
    load_forecast_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeWindSolarForecast(BaseSchema):
    """Silver-layer schema for ENTSO-E wind and solar generation forecast (A69/A01).

    production_type: EIC PSR type code (B16=Wind offshore, B18=Wind onshore, B19=Solar).
    """

    timestamp_utc: datetime
    area_code: str
    production_type: str  # B16=Wind offshore, B18=Wind onshore, B19=Solar
    forecast_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeOutagesGeneration(BaseSchema):
    """Silver-layer schema for ENTSO-E unavailability of generation units (A80).

    available_capacity_mw: MW of available (non-unavailable) capacity during the interval.
    production_type: EIC PSR type code; empty string when not present in the document.
    """

    timestamp_utc: datetime
    area_code: str
    production_type: str = ""
    available_capacity_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeInstalledCapacity(BaseSchema):
    """Silver-layer schema for ENTSO-E installed generation capacity aggregated (A68/A33).

    production_type: EIC PSR type code.
    installed_capacity_mw: Total installed capacity in MW.
    """

    timestamp_utc: datetime
    area_code: str
    production_type: str
    installed_capacity_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
