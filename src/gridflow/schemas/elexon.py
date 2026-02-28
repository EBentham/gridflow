"""Pydantic v2 schemas for Elexon silver-layer data contracts."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class ElexonSystemPrice(BaseSchema):
    """Silver-layer schema for Elexon System Buy/Sell Prices."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    system_sell_price: float = Field(ge=-500, le=10000)  # GBP/MWh
    system_buy_price: float = Field(ge=-500, le=10000)
    net_imbalance_volume: float  # MWh
    run_type: str = Field(pattern=r"^(II|SF|R[1-3]|RF|DF)$")
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonGenerationByFuel(BaseSchema):
    """Silver-layer schema for Elexon generation outturn by fuel type."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    fuel_type: str
    generation_mw: float
    data_provider: str = Field(default="elexon")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
