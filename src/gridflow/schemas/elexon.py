"""Pydantic v2 schemas for Elexon silver-layer data contracts."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class SettlementRunType(str, Enum):
    """Elexon settlement run type precedence (higher = more final)."""

    II = "II"  # Interim Initial
    SF = "SF"  # Settlement Final
    R1 = "R1"  # Reconciliation Run 1
    R2 = "R2"  # Reconciliation Run 2
    R3 = "R3"  # Reconciliation Run 3
    RF = "RF"  # Reconciliation Final
    DF = "DF"  # Dispute Final


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


class ElexonFuelHH(BaseSchema):
    """Silver schema: Half-hourly generation outturn by fuel type (FUELHH)."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    fuel_type: str
    generation_mw: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonBOAL(BaseSchema):
    """Silver schema: Bid/Offer Acceptance Levels."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    bm_unit_id: str
    acceptance_number: int | None = None
    acceptance_time: datetime | None = None
    deem_flag: bool = False
    so_flag: bool = False
    stor_flag: bool = False
    rr_flag: bool = False
    bid_offer_level_from: float | None = None
    bid_offer_level_to: float | None = None
    bid_offer_acceptance_number: int | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonBOD(BaseSchema):
    """Silver schema: Bid/Offer Data."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    bm_unit_id: str
    bid_offer_pair_number: int | None = None
    bid_offer_level_from: float | None = None
    bid_offer_level_to: float | None = None
    bid_price: float | None = None
    offer_price: float | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonMID(BaseSchema):
    """Silver schema: Market Index Data."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    data_provider_id: str | None = None
    market_index_price: float | None = None
    market_index_volume: float | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonFrequency(BaseSchema):
    """Silver schema: System Frequency (FREQ)."""

    timestamp_utc: datetime
    frequency_hz: float = Field(ge=49.0, le=51.0)
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonDemandForecast(BaseSchema):
    """Silver schema: National Demand Forecast (NDF / NDFD)."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    forecast_type: str  # "day_ahead" or "2_14_day"
    national_demand_mw: float
    transmission_demand_mw: float | None = None
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonWindForecast(BaseSchema):
    """Silver schema: Wind Generation Forecast (WINDFOR)."""

    settlement_date: date | None = None
    settlement_period: int | None = Field(default=None, ge=1, le=50)
    timestamp_utc: datetime
    initial_forecast_mw: float | None = None
    latest_forecast_mw: float | None = None
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonPN(BaseSchema):
    """Silver schema: Physical Notifications."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    bm_unit_id: str
    level_from: float | None = None
    level_to: float | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonDISBSAD(BaseSchema):
    """Silver schema: Disaggregated Balancing Services Adjustment Data."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    adjustment_action_id: str | None = None
    so_flag: bool = False
    stor_flag: bool = False
    component: str | None = None
    cost: float | None = None
    volume: float | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonBMUnit(BaseSchema):
    """Silver schema: BM Unit Reference Data (static/slowly changing)."""

    bm_unit_id: str
    bm_unit_name: str | None = None
    fuel_type: str | None = None
    registered_capacity_mw: float | None = None
    company_name: str | None = None
    gsp_group_id: str | None = None
    national_grid_bm_unit: str | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None
