"""Pydantic v2 schemas for GIE AGSI+ / ALSI silver-layer data contracts."""

from __future__ import annotations

from datetime import date

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class GasStorage(BaseSchema):
    """Silver-layer schema for GIE AGSI+ gas storage data (country level)."""

    gas_day: date
    country_code: str
    country_name: str = ""
    gas_in_storage_gwh: float | None = None
    withdrawal_gwh: float | None = None
    injection_gwh: float | None = None
    working_gas_volume_gwh: float | None = None
    storage_pct_full: float | None = None  # 0-100
    trend: float | None = None
    data_provider: str = Field(default="gie_agsi")

    @field_validator("storage_pct_full")
    @classmethod
    def clamp_pct(cls, v: float | None) -> float | None:
        if v is not None:
            return max(0.0, min(100.0, v))
        return v


class LNGTerminal(BaseSchema):
    """Silver-layer schema for GIE ALSI LNG terminal data (country level)."""

    gas_day: date
    country_code: str
    country_name: str = ""
    lng_in_storage_gwh: float | None = None
    send_out_gwh: float | None = None
    injection_gwh: float | None = None
    dtrs_pct_full: float | None = None  # 0-100
    trend: float | None = None
    data_provider: str = Field(default="gie_alsi")

    @field_validator("dtrs_pct_full")
    @classmethod
    def clamp_pct(cls, v: float | None) -> float | None:
        if v is not None:
            return max(0.0, min(100.0, v))
        return v
