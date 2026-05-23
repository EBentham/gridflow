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
    """Silver-layer schema for Elexon System Buy/Sell Prices.

    `run_type` is the BSC settlement run identifier (II/SF/R1-R3/RF/DF)
    used for bitemporal precedence in `_resolve_runs`. The current
    `/balancing/settlement/system-prices/{date}` endpoint does not
    expose any field that maps to this concept, so live silver from
    that endpoint has `run_type=None`. Older fixtures and any future
    endpoint that surfaces `settlementRunType` will populate it.

    `price_derivation_code` is the live API's `priceDerivationCode`
    field — describes how the SBP/SSP was derived for the period.
    Observed values include 'N' (normal) and 'P' (provisional). No
    regex constraint because the value list is vendor-managed and
    open-ended (V2-FIX-04).
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    system_sell_price: float = Field(ge=-500, le=10000)  # GBP/MWh
    system_buy_price: float = Field(ge=-500, le=10000)
    net_imbalance_volume: float  # MWh
    run_type: str | None = Field(default=None, pattern=r"^(II|SF|R[1-3]|RF|DF)$")
    price_derivation_code: str | None = None
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
    """Silver schema: Bid/Offer Acceptance Levels.

    G5-W2.1 (2026-05): `bid_offer_acceptance_number` removed. It was
    declared as a future per-pair link column but the Elexon BOAL API
    does not surface a corresponding field, so the column was always
    None — schema and silver disagreed (declared but never emitted).
    Re-add this field if/when the API provides a source key for it.
    """

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


# ---------------------------------------------------------------------------
# G5-W4 (2026-05) — Pydantic classes for the 21 Elexon silver datasets
# that previously had no schema. Bringing elexon coverage to ~100% in line
# with every other vendor (entsoe, entsog, gie, neso, open-meteo all at
# ~100% per dataset). See .planning/phases/G5-elexon-schema-drift-fix/.
#
# Pattern per class: mirror the transformer's output_cols exactly. The
# field order matches output_cols, types match the transformer's casts.
# Future Pydantic-class additions should use the same pattern.
# ---------------------------------------------------------------------------


class ElexonImbalNGC(BaseSchema):
    """Silver schema: National Indicated Imbalance (IMBALNGC).

    indicated_imbalance is in MW (negative = system short, positive = long).
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    indicated_imbalance: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonMelNGC(BaseSchema):
    """Silver schema: National Indicated Margin (MELNGC).

    indicated_margin is the difference (MW) between system available
    generation and demand at each settlement period.
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    indicated_margin: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonIndDem(BaseSchema):
    """Silver schema: Day and Day-Ahead Indicated Demand (INDDEM)."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    indicated_demand_mw: float
    boundary: str | None = None
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonTSDF(BaseSchema):
    """Silver schema: Transmission System Demand Forecast (TSDF).

    Half-hourly forecast demand on the National Grid Electricity
    Transmission System.
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    forecast_demand_mw: float
    boundary: str | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonINDO(BaseSchema):
    """Silver schema: Initial National Demand Outturn (INDO).

    Half-hourly initial estimate of national demand actual (MW).
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    initial_demand_outturn_mw: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonINDOD(BaseSchema):
    """Silver schema: Initial National Demand Outturn Daily (INDOD).

    Daily aggregate variant of INDO; one row per settlement_date.
    timestamp_utc is midnight UTC of settlement_date.
    """

    settlement_date: date
    timestamp_utc: datetime
    initial_demand_outturn_mw: float
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonITSDO(BaseSchema):
    """Silver schema: Initial Transmission System Demand Outturn (ITSDO)."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    initial_transmission_system_demand_outturn_mw: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonIndGen(BaseSchema):
    """Silver schema: Day and Day-Ahead Indicated Generation (INDGEN)."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    indicated_generation_mw: float
    boundary: str | None = None
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonTSDFD(BaseSchema):
    """Silver schema: 2-14 Day Ahead Transmission System Demand Forecast (TSDFD).

    Daily forecast; one row per forecast_date. timestamp_utc is midnight
    UTC of forecast_date. Unlike most Elexon datasets this one emits
    published_at via the rename map (publishTime → published_at).
    """

    forecast_date: date
    timestamp_utc: datetime
    forecast_demand_mw: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonAGPT(BaseSchema):
    """Silver schema: Actual Aggregated Generation Per Type (AGPT / B1620).

    psr_type is the EIC PSR code (e.g. B16=wind offshore, B18=wind onshore,
    B19=solar, B11=hydro). Multi-row per settlement period — one row per
    psr_type per period.
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    psr_type: str
    generation_mw: float
    business_type: str | None = None
    document_id: str | None = None
    document_revision: int | None = None
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonAGWS(BaseSchema):
    """Silver schema: Actual/Estimated Wind and Solar Power Generation (AGWS / B1630).

    Same shape as AGPT but limited to wind/solar PSR types.
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    psr_type: str
    generation_mw: float
    business_type: str | None = None
    document_id: str | None = None
    document_revision: int | None = None
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonATL(BaseSchema):
    """Silver schema: Actual Total Load Per Bidding Zone (ATL / B0610)."""

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    total_load_mw: float
    business_type: str | None = None
    document_id: str | None = None
    document_revision: int | None = None
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonMarketDepth(BaseSchema):
    """Silver schema: Settlement Market Depth.

    Per-settlement-period balancing market depth metrics — bid/offer
    volumes, accepted volumes, and adjustment volumes (all in MWh).
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    indicated_imbalance_mwh: float | None = None
    offer_volume_mwh: float | None = None
    bid_volume_mwh: float | None = None
    total_accepted_offer_volume_mwh: float | None = None
    total_accepted_bid_volume_mwh: float | None = None
    total_adjustment_sell_volume_mwh: float | None = None
    total_adjustment_buy_volume_mwh: float | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonNonBM(BaseSchema):
    """Silver schema: Non-BM STOR Generation (NONBM).

    Half-hourly generation from non-BM Short-Term Operating Reserve (STOR)
    providers.
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
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


class ElexonFOU2T14D(BaseSchema):
    """Silver schema: 2-14 Day Generation Availability by Fuel Type (FOU2T14D).

    Forward-looking generation availability. settlement_period may be absent
    when bronze used `forecastDate` only — timestamp_utc is midnight UTC of
    settlement_date in that case.
    """

    settlement_date: date
    settlement_period: int | None = Field(default=None, ge=1, le=50)
    timestamp_utc: datetime
    fuel_type: str
    output_usable_mw: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonUOU2T14D(BaseSchema):
    """Silver schema: 2-14 Day Generation Availability by BM Unit (UOU2T14D).

    Per-unit forward availability. G5-W2.3 made this self-describing —
    fuel_type and national_grid_bm_unit travel with the row instead of
    requiring a bmunits_reference join. settlement_period may be absent
    when bronze used `forecastDate` only.
    """

    settlement_date: date
    settlement_period: int | None = Field(default=None, ge=1, le=50)
    timestamp_utc: datetime
    bm_unit_id: str
    national_grid_bm_unit: str | None = None
    fuel_type: str | None = None
    output_usable_mw: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonLOLPDRM(BaseSchema):
    """Silver schema: Loss of Load Probability and De-rated Margin (LOLPDRM).

    loss_of_load_probability is a unitless probability in [0, 1].
    derated_margin_mw is the system margin in MW after de-rating capacity
    for unavailability risk.
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime
    loss_of_load_probability: float = Field(ge=0.0, le=1.0)
    derated_margin_mw: float
    published_at: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonREMIT(BaseSchema):
    """Silver schema: REMIT Outage and Unavailability Messages.

    Append-only — every revision is preserved (F7 / DATASET_VERSION 2.0.0).
    Latest-revision selection happens at read time per `(mrid, revision_number)`.
    """

    mrid: str
    revision_number: int | None = None
    timestamp_utc: datetime
    message_type: str | None = None
    message_heading: str | None = None
    event_type: str | None = None
    unavailability_type: str | None = None
    participant_id: str | None = None
    registration_code: str | None = None
    asset_id: str | None = None
    asset_type: str | None = None
    affected_unit: str | None = None
    affected_unit_eic: str | None = None
    bidding_zone: str | None = None
    fuel_type: str | None = None
    normal_capacity_mw: float | None = None
    available_capacity_mw: float | None = None
    unavailable_capacity_mw: float | None = None
    event_status: str | None = None
    event_start_time: datetime | None = None
    event_end_time: datetime | None = None
    cause: str | None = None
    related_information: str | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonSOSO(BaseSchema):
    """Silver schema: SO-SO Prices (cross-border interconnector trading).

    System Operator-to-System Operator trades. timestamp_utc is derived from
    settlement_period when present, else from start_time, else midnight of
    settlement_date.
    """

    settlement_date: date
    settlement_period: int | None = Field(default=None, ge=1, le=50)
    timestamp_utc: datetime
    contract_identification: str
    sender_identification: str | None = None
    receiver_identification: str | None = None
    resource_provider: str | None = None
    trade_direction: str | None = None
    trade_quantity_mw: float | None = None
    trade_price: float | None = None
    trader_unit: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonNETBSAD(BaseSchema):
    """Silver schema: Net Bid-Offer Settlement Adjustment Data (NETBSAD).

    G5-W1.1 (2026-05): the live API replaced 4 coarse adjustment fields
    with 8 finer-grained ones. The schema accepts both column sets;
    pre-2026 bronze populates the legacy 4, post-2026 bronze populates
    the new 8. All adjustment columns are Optional because only one set
    is populated per bronze era.
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    timestamp_utc: datetime

    # Legacy 4 (pre-2026 bronze)
    net_buy_price_adjustment: float | None = None
    net_sell_price_adjustment: float | None = None
    net_buy_volume_adjustment: float | None = None
    net_sell_volume_adjustment: float | None = None

    # Current 8 (2026+ bronze) — buy side
    net_buy_price_cost_adjustment_energy: float | None = None
    net_buy_price_volume_adjustment_energy: float | None = None
    net_buy_price_volume_adjustment_system: float | None = None
    buy_price_price_adjustment: float | None = None

    # Current 8 (2026+ bronze) — sell side
    net_sell_price_cost_adjustment_energy: float | None = None
    net_sell_price_volume_adjustment_energy: float | None = None
    net_sell_price_volume_adjustment_system: float | None = None
    sell_price_price_adjustment: float | None = None

    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class ElexonTemp(BaseSchema):
    """Silver schema: Temperature Data (TEMP).

    Vendor-supplied temperature readings. measurement_date carries the
    vendor's original measurement date (separate from timestamp_utc, the
    publish-time-derived timestamp) when bronze includes it.
    """

    timestamp_utc: datetime
    measurement_date: date | None = None
    temperature: float
    normal_temperature: float | None = None
    low_temperature: float | None = None
    high_temperature: float | None = None
    data_provider: str = Field(default="elexon")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
