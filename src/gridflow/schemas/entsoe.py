"""Pydantic v2 schemas for ENTSO-E silver-layer data contracts."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 - Pydantic resolves model annotations at runtime.

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
    forecast_horizon: str = "day_ahead"
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
    generation_forecast_mw: float
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

    One row per (timestamp_utc, unit_mrid). unit_mrid uniquely identifies the
    generation unit (from RegisteredResource.mRID); unit_name is the
    human-readable name when present. outage_type is the mapped form of
    ENTSO-E businessType: A53 -> "planned", A54 -> "unplanned".
    unavailable_mw is the MW unavailable during the interval (XML <quantity>).
    """

    timestamp_utc: datetime
    area_code: str            # control area / bidding zone EIC mRID
    unit_mrid: str            # RegisteredResource mRID — unit identity
    unit_name: str = ""       # human-readable unit name; may be absent
    outage_type: str          # "planned" (A53) | "unplanned" (A54)
    unavailable_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeInstalledCapacity(BaseSchema):
    """Silver-layer schema for ENTSO-E installed generation capacity aggregated (A68/A33).

    production_type: EIC PSR type code.
    capacity_mw: Total installed capacity in MW.
    """

    timestamp_utc: datetime
    area_code: str
    production_type: str
    capacity_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeInstalledCapacityUnits(BaseSchema):
    """Silver-layer schema for ENTSO-E installed capacity per production unit."""

    timestamp_utc: datetime
    area_code: str
    production_type: str
    unit_mrid: str
    unit_name: str = ""
    capacity_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeGenerationForecast(BaseSchema):
    """Silver-layer schema for ENTSO-E day-ahead generation forecast aggregated (A71/A01).

    production_type: EIC PSR type code.
    generation_forecast_mw: Forecasted generation in MW.
    """

    timestamp_utc: datetime
    area_code: str
    production_type: str
    generation_forecast_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeActualGenerationUnits(BaseSchema):
    """Silver-layer schema for ENTSO-E actual generation per generation unit."""

    timestamp_utc: datetime
    area_code: str
    production_type: str
    unit_mrid: str
    unit_name: str = ""
    generation_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeWaterReservoirs(BaseSchema):
    """Silver-layer schema for ENTSO-E water reservoirs and hydro storage plants."""

    timestamp_utc: datetime
    area_code: str
    reservoir_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeGenerationUnitsMasterData(BaseSchema):
    """Silver-layer schema for ENTSO-E production/generation unit reference data."""

    area_code: str
    unit_mrid: str
    unit_name: str = ""
    production_type: str = ""
    implementation_datetime_utc: datetime | None = None
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("implementation_datetime_utc")
    @classmethod
    def optional_datetime_must_be_utc(cls, v: datetime | None) -> datetime | None:
        if v is not None and v.tzinfo is None:
            raise ValueError("implementation_datetime_utc must be timezone-aware (UTC)")
        return v


class EntsoeLoadForecastWeekly(BaseSchema):
    """Silver-layer schema for ENTSO-E week-ahead load forecast (A65/A31)."""

    timestamp_utc: datetime
    area_code: str
    load_forecast_mw: float
    resolution: str = ""
    forecast_horizon: str = "week_ahead"
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeForecastMargin(BaseSchema):
    """Silver-layer schema for ENTSO-E year-ahead forecast margin (A70/A33)."""

    timestamp_utc: datetime
    area_code: str
    forecast_margin_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeNetTransferCapacity(BaseSchema):
    """Silver-layer schema for ENTSO-E net transfer capacity day-ahead (A61/A01).

    ntc_mw: Net transfer capacity in MW between the two zones.
    """

    timestamp_utc: datetime
    in_area_code: str
    out_area_code: str
    ntc_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeImbalancePrices(BaseSchema):
    """Silver-layer schema for ENTSO-E imbalance prices (A85).

    direction: "long" = system surplus (A19), "short" = system deficit (A20).
    price_eur_mwh: Imbalance settlement price in EUR/MWh.
    """

    timestamp_utc: datetime
    area_code: str  # control area EIC mRID
    direction: str  # "long" | "short"
    price_eur_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeImbalanceVolume(BaseSchema):
    """Silver-layer schema for ENTSO-E imbalance volumes (A86/A16).

    direction: "long" (A01=generation excess) | "short" (A02=consumption excess).
    volume_mwh: Imbalance volume in MWh.
    """

    timestamp_utc: datetime
    area_code: str
    direction: str  # "long" | "short"
    volume_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeActivatedBalancingQty(BaseSchema):
    """Silver-layer schema for ENTSO-E activated balancing energy quantity (A83/A16).

    reserve_type: "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98).
    direction: "up"(A01=upward activation) | "down"(A02=downward activation).
    quantity_mwh: Activated quantity in MWh.
    """

    timestamp_utc: datetime
    area_code: str
    reserve_type: str  # "fcr" | "afrr" | "mfrr" | "rr"
    direction: str     # "up" | "down"
    quantity_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeActivatedBalancingPrices(BaseSchema):
    """Silver-layer schema for ENTSO-E activated balancing energy prices (A84/A16).

    reserve_type: "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98).
    direction: "up"(A01) | "down"(A02).
    price_eur_mwh: Activation price in EUR/MWh.
    """

    timestamp_utc: datetime
    area_code: str
    reserve_type: str  # "fcr" | "afrr" | "mfrr" | "rr"
    direction: str     # "up" | "down"
    price_eur_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeContractedReserves(BaseSchema):
    """Silver-layer schema for ENTSO-E contracted reserves (A81).

    reserve_type: "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98).
    quantity_mw: Contracted reserve quantity in MW.
    """

    timestamp_utc: datetime
    area_code: str
    reserve_type: str  # "fcr" | "afrr" | "mfrr" | "rr"
    quantity_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeTransmissionMarketQuantity(BaseSchema):
    """Silver-layer schema for H6 transmission/market quantity time series."""

    timestamp_utc: datetime
    in_area_code: str
    out_area_code: str
    quantity_mw: float
    business_type: str = ""
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class EntsoeTransmissionMarketAmount(BaseSchema):
    """Silver-layer schema for H6 transmission/market monetary time series."""

    timestamp_utc: datetime
    in_area_code: str
    out_area_code: str
    amount_eur: float
    business_type: str = ""
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
