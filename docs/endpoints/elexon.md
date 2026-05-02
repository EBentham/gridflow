# Elexon BMRS — Dataset Reference

**Source:** `elexon`
**Base URL:** `https://data.elexon.co.uk/bmrs/api/v1`
**Authentication:** None (public API)
**Settlement period:** 30 minutes; SP1 = 00:00–00:30 UTC, SP48 = 23:30–24:00 UTC

This document mirrors the navigation structure of the [Elexon BMRS Insights Solution](https://bmrs.elexon.co.uk/).

---

# REMIT

REMIT (Regulation on Energy Market Integrity and Transparency) messages report planned and unplanned outages of generation assets, as required under EU Regulation 1227/2011.

---

## remit

REMIT Outage and Unavailability Messages. Market participants publish notifications of planned and unplanned generation unavailability, including affected asset, capacity impact, and event timeline. Critical for forward supply analysis and outage tracking.

**API path:** `/datasets/REMIT`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** As published by market participants
**Silver key columns:** `mrid`, `revision_number`, `timestamp_utc`, `message_type`, `event_type`, `affected_unit`, `fuel_type`, `normal_capacity_mw`, `available_capacity_mw`, `unavailable_capacity_mw`, `event_status`, `event_start_time`, `event_end_time`, `cause`

| mrid | revision_number | message_type | event_type | affected_unit | fuel_type | normal_capacity_mw | available_capacity_mw | unavailable_capacity_mw | event_status |
|------|---------------:|--------------|------------|---------------|-----------|------------------:|-----------------------:|--------------------------:|------------|
| MSG-001 | 1 | Unavailability | Unplanned | T_DRAXX-1 | Biomass | 645.0 | 0.0 | 645.0 | Active |
| MSG-002 | 2 | Unavailability | Planned | T_SIZB-1 | Nuclear | 1198.0 | 600.0 | 598.0 | Active |
| MSG-003 | 1 | Unavailability | Planned | T_COTPS-1 | CCGT | 500.0 | 500.0 | 0.0 | Dismissed |

> Capacity in MW. Each message has a unique `mrid`; later `revision_number` values supersede earlier ones.

---

# Generation

Generation data covers actual and forecast output from GB power stations, broken down by fuel type, technology, or individual BM unit.

---

## fuelhh

Half-hourly Generation Outturn by Fuel Type (FUELHH). Actual generation dispatched from each fuel technology in each settlement period, aggregated across all units of that type. The primary dataset for tracking the GB generation mix.

**API path:** `/datasets/FUELHH`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Every 30 minutes
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `fuel_type`, `generation_mw`

| settlement_date | settlement_period | timestamp_utc          | fuel_type | generation_mw |
|----------------|-------------------|------------------------|-----------|-------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | CCGT      |       8420.0 |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | NUCLEAR   |       5820.0 |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | WIND      |       7150.0 |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | SOLAR     |          0.0 |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | BIOMASS   |       2100.0 |

> Generation in MW. Fuel types include: CCGT, NUCLEAR, WIND, SOLAR, BIOMASS, HYDRO, COAL, OIL, NPSHYD, OCGT, OTHER, INTFR, INTIRL, INTNED, INTEW, INTEM.

---

## fuelinst

Instantaneous Generation Outturn by Fuel Type (FUELINST). Higher-frequency snapshot of generation by fuel type, updated every few minutes. Use `fuelhh` for settled half-hourly data; use this for near-real-time monitoring.

**API path:** `/datasets/FUELINST`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Every ~5 minutes
**Silver key columns:** `timestamp_utc`, `fuel_type`, `generation_mw`

| timestamp_utc          | fuel_type | generation_mw |
|------------------------|-----------|-------------:|
| 2024-06-15 00:05:00+00 | CCGT      |       8510.0 |
| 2024-06-15 00:05:00+00 | NUCLEAR   |       5810.0 |
| 2024-06-15 00:05:00+00 | WIND      |       7200.0 |
| 2024-06-15 00:05:00+00 | SOLAR     |          0.0 |
| 2024-06-15 00:10:00+00 | CCGT      |       8490.0 |

> Generation in MW. Not settled — use `fuelhh` for definitive half-hourly totals.

---

## agpt

Actual Aggregated Generation Per Type (AGPT / B1620). ENTSO-E transparency regulation dataset reporting actual generation output aggregated by PSR (Production/Storage Resource) type for each settlement period. Uses the European PSR type classification rather than Elexon's fuel type codes.

**API path:** `/datasets/AGPT`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `psr_type`, `generation_mw`, `business_type`

| settlement_date | settlement_period | timestamp_utc          | psr_type             | generation_mw | business_type          |
|----------------|-------------------|------------------------|----------------------|-------------:|------------------------|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Fossil Gas           |       8420.0 | Actual generation      |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Nuclear              |       5820.0 | Actual generation      |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Wind Onshore         |       4200.0 | Actual generation      |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Wind Offshore        |       2950.0 | Actual generation      |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Solar                |          0.0 | Actual generation      |

> Generation in MW. PSR types follow ENTSO-E classification (e.g. "Fossil Gas" not "CCGT", "Wind Onshore"/"Wind Offshore" not "WIND").

---

## agws

Actual or Estimated Wind and Solar Power Generation (AGWS / B1630). ENTSO-E transparency regulation dataset reporting actual or estimated wind and solar generation, separated by onshore wind, offshore wind, and solar PV.

**API path:** `/datasets/AGWS`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `psr_type`, `generation_mw`, `business_type`

| settlement_date | settlement_period | timestamp_utc          | psr_type        | generation_mw | business_type                        |
|----------------|-------------------|------------------------|-----------------|-------------:|--------------------------------------|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Wind Offshore   |       2950.0 | Solar and wind actual generation     |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Wind Onshore    |       4200.0 | Solar and wind actual generation     |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | Solar           |          0.0 | Solar and wind actual generation     |

> Generation in MW. `business_type` distinguishes actual from estimated values.

---

## windfor

Wind Generation Forecast (WINDFOR). National Grid's initial and latest forecasts of total GB wind generation for each settlement period. The gap between initial and latest forecasts reflects intra-day forecast updates as weather models improve.

**API path:** `/datasets/WINDFOR`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Hourly
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `initial_forecast_mw`, `latest_forecast_mw`

| settlement_date | settlement_period | timestamp_utc          | initial_forecast_mw | latest_forecast_mw |
|----------------|-------------------|------------------------|--------------------:|-------------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |              7100.0 |             7250.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |              7050.0 |             7180.0 |
| 2024-06-15     | 3                 | 2024-06-15 01:00:00+00 |              6980.0 |             7100.0 |

> Generation in MW. `initial_forecast_mw` is published the day before; `latest_forecast_mw` is updated closer to real time.

---

## nonbm

Non-BM STOR Generation (NONBM). Generation from Short Term Operating Reserve (STOR) providers that are not Balancing Mechanism units. These are typically smaller embedded generators contracted to provide reserve capacity.

**API path:** `/datasets/NONBM`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `generation_mw`

| settlement_date | settlement_period | timestamp_utc          | generation_mw |
|----------------|-------------------|------------------------|-------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |         85.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |         92.0 |
| 2024-06-15     | 3                 | 2024-06-15 01:00:00+00 |         78.0 |

> Generation in MW. Typically much smaller volumes than BM generation.

---

## Interconnector Flows

*Not yet implemented.* Real-time and historical cross-border electricity flows via interconnectors (IFA, BritNed, Moyle, EWIC, NSL, ElecLink, Viking Link). Available as opinionated endpoint `/generation/interconnectors`.

---

## Generation Forecast for Wind & Solar

*Not yet implemented.* Day-ahead forecast of wind and solar generation. Available via opinionated endpoints `/forecast/generation/wind-and-solar/day-ahead` and the wind-specific family (`/forecast/generation/wind/earliest`, `/latest`, `/peak`, `/evolution`).

---

## Day-Ahead Aggregated Generation

*Not yet implemented.* Day-ahead forecast of total generation aggregated by fuel type.

---

## Generation Availability — 2 to 14 Days

### fou2t14d

2 to 14 Day Ahead Generation Availability by Fuel Type (FOU2T14D). Forward-looking availability of generation capacity broken down by fuel type. Used by market participants to anticipate future supply.

**API path:** `/datasets/FOU2T14D`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `fuel_type`, `output_usable_mw`

| settlement_date | settlement_period | timestamp_utc          | fuel_type | output_usable_mw |
|----------------|-------------------|------------------------|-----------|----------------:|
| 2024-06-17     | 1                 | 2024-06-17 00:00:00+00 | CCGT      |         22000.0 |
| 2024-06-17     | 1                 | 2024-06-17 00:00:00+00 | NUCLEAR   |          5500.0 |
| 2024-06-17     | 2                 | 2024-06-17 00:30:00+00 | CCGT      |         21500.0 |

> Usable output in MW. Published on day D, covering settlement dates D+2 through D+14.

---

### uou2t14d

2 to 14 Day Ahead Generation Availability by BM Unit (UOU2T14D). Same data as `fou2t14d` but at individual BM unit level. Enables unit-specific outage and capacity analysis.

**API path:** `/datasets/UOU2T14D`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Chunk limit:** Max 4-hour request window (API restriction)
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `bm_unit_id`, `output_usable_mw`

| settlement_date | settlement_period | timestamp_utc          | bm_unit_id | output_usable_mw |
|----------------|-------------------|------------------------|------------|----------------:|
| 2024-06-17     | 1                 | 2024-06-17 00:00:00+00 | T_DRAXX-1  |           645.0 |
| 2024-06-17     | 1                 | 2024-06-17 00:00:00+00 | T_COTPS-1  |           500.0 |
| 2024-06-17     | 2                 | 2024-06-17 00:30:00+00 | T_DRAXX-1  |           640.0 |

> Usable output in MW. Deduplicated on `(settlement_date, settlement_period, bm_unit_id)`.

---

## Generation Availability — 2 to 156 Weeks

### FOU2T3YW

*Not yet implemented.* 2 to 156 Week Ahead Generation Availability by Fuel Type. Long-range capacity outlook by fuel type, published weekly.

**API path:** `/datasets/FOU2T3YW`
**Fields:** `fuelType`, `publishTime`, `calendarWeekNumber`, `year`, `outputUsable`

### UOU2T3YW

*Not yet implemented.* 2 to 156 Week Ahead Generation Availability by BM Unit. Long-range capacity outlook per unit, published weekly. Very high volume dataset.

**API path:** `/datasets/UOU2T3YW`
**Fields:** `fuelType`, `bmUnit`, `publishTime`, `week`, `year`, `outputUsable`

---

# Demand

Demand data covers actual outturn demand, forecasts at multiple horizons, and indicated supply/demand data.

---

## indo

Initial National Demand Outturn (INDO). The first published estimate of actual GB electricity demand for each settlement period. "National demand" is metered generation minus station transformer losses and pump storage demand.

**API path:** `/datasets/INDO`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Hourly (published shortly after each SP completes)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `initial_demand_outturn_mw`

| settlement_date | settlement_period | timestamp_utc          | initial_demand_outturn_mw |
|----------------|-------------------|------------------------|-------------------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |                   23500.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |                   23200.0 |
| 2024-06-15     | 3                 | 2024-06-15 01:00:00+00 |                   22900.0 |

> Demand in MW. This is the "initial" estimate; later settlement runs may revise the figure.

---

## itsdo

Initial Transmission System Demand Outturn (ITSDO). The first published estimate of demand measured at grid supply points (transmission-level). Higher than national demand because it excludes netting from embedded generation.

**API path:** `/datasets/ITSDO`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Hourly
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `initial_transmission_system_demand_outturn_mw`

| settlement_date | settlement_period | timestamp_utc          | initial_transmission_system_demand_outturn_mw |
|----------------|-------------------|------------------------|-----------------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |                30700.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |                30400.0 |
| 2024-06-15     | 3                 | 2024-06-15 01:00:00+00 |                30100.0 |

> Demand in MW. Transmission-level demand is higher than national demand due to embedded generation netting.

---

## indod

Initial National Demand Outturn — Daily Total (INDOD). The total national demand aggregated for the entire settlement day. One row per day rather than per settlement period.

**API path:** `/datasets/INDOD`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `timestamp_utc`, `initial_demand_outturn_mw`

| settlement_date | timestamp_utc          | initial_demand_outturn_mw |
|----------------|------------------------|--------------------------:|
| 2024-06-15     | 2024-06-15 00:00:00+00 |                   680000.0 |
| 2024-06-16     | 2024-06-16 00:00:00+00 |                   620000.0 |

> Demand in MW (daily aggregate — not the same unit scale as half-hourly INDO).

---

## atl

Actual Total Load Per Bidding Zone (ATL / B0610). ENTSO-E transparency regulation dataset reporting total electricity consumption within the GB bidding zone per settlement period. Uses the European reporting framework.

**API path:** `/datasets/ATL`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `total_load_mw`

| settlement_date | settlement_period | timestamp_utc          | total_load_mw |
|----------------|-------------------|------------------------|-------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |      30700.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |      30400.0 |

> Load in MW. Comparable to ITSDO but published under ENTSO-E transparency regulation framework.

---

## ndf

National Demand Forecast — Day-Ahead (NDF). National Grid's day-ahead forecast of GB electricity demand for each settlement period of the following day.

**API path:** `/datasets/NDF`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily (published afternoon for next day)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `forecast_type`, `national_demand_mw`, `transmission_demand_mw`

| settlement_date | settlement_period | timestamp_utc          | forecast_type | national_demand_mw | transmission_demand_mw |
|----------------|-------------------|------------------------|---------------|------------------:|-----------------------:|
| 2024-06-16     | 1                 | 2024-06-16 00:00:00+00 | day_ahead     |           24100.0 |                22800.0 |
| 2024-06-16     | 2                 | 2024-06-16 00:30:00+00 | day_ahead     |           23850.0 |                22550.0 |

> Demand in MW. `national_demand_mw` includes embedded generation netting; `transmission_demand_mw` is at grid supply points.

---

## ndfd

National Demand Forecast — 2 to 14 Days Ahead (NDFD). Medium-range demand forecast extending up to two weeks, used for generator outage planning and longer-horizon balancing.

**API path:** `/datasets/NDFD`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `forecast_type`, `national_demand_mw`, `transmission_demand_mw`

> Same schema as `ndf`; settlement dates are 2–14 days from the publish date.

---

## inddem

Day and Day-Ahead Indicated Demand (INDDEM). National Grid's indicated demand forecast published day-ahead and updated on the day itself. Provides the SO's view of expected demand at boundary (national vs transmission level).

**API path:** `/datasets/INDDEM`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily (multiple updates per day)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `indicated_demand_mw`, `boundary`

| settlement_date | settlement_period | timestamp_utc          | indicated_demand_mw | boundary     |
|----------------|-------------------|------------------------|--------------------:|-------------|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |            24100.0  | National    |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |            30700.0  | Transmission|
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |            23850.0  | National    |

> Demand in MW. `boundary` distinguishes national demand (net of embedded gen) from transmission-level demand.

---

## indgen

Day and Day-Ahead Indicated Generation (INDGEN). National Grid's indicated total generation forecast at national and transmission boundaries. The supply-side counterpart to `inddem`.

**API path:** `/datasets/INDGEN`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily (multiple updates per day)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `indicated_generation_mw`, `boundary`

| settlement_date | settlement_period | timestamp_utc          | indicated_generation_mw | boundary     |
|----------------|-------------------|------------------------|------------------------:|-------------|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |                24200.0  | National    |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |                30900.0  | Transmission|
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |                24000.0  | National    |

> Generation in MW. Deduplicated on `(settlement_date, settlement_period, boundary)`.

---

## tsdf

Transmission System Demand Forecast (TSDF). National Grid's forecast of transmission-level demand for each settlement period, published day-ahead. Similar to INDDEM but specifically for transmission system demand.

**API path:** `/datasets/TSDF`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `forecast_demand_mw`, `boundary`

| settlement_date | settlement_period | timestamp_utc          | forecast_demand_mw | boundary     |
|----------------|-------------------|------------------------|-------------------:|-------------|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |            30700.0 | Transmission|
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |            30400.0 | Transmission|

> Demand in MW.

---

## tsdfd

2 to 14 Day Ahead Transmission System Demand Forecast (TSDFD). Medium-range daily peak demand forecast for the transmission system, extending 2–14 days ahead. One value per forecast date (daily resolution, not half-hourly).

**API path:** `/datasets/TSDFD`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `forecast_date`, `timestamp_utc`, `forecast_demand_mw`

| forecast_date | timestamp_utc          | forecast_demand_mw |
|---------------|------------------------|-----------------:|
| 2024-06-17    | 2024-06-17 00:00:00+00 |           37750.0 |
| 2024-06-18    | 2024-06-18 00:00:00+00 |           38200.0 |
| 2024-06-19    | 2024-06-19 00:00:00+00 |           36800.0 |

> Demand in MW. One row per forecast date (daily peak). Published on day D, covering D+2 through D+14.

---

## TSDFW

*Not yet implemented.* 2 to 52 Week Ahead Transmission System Demand Forecast. Weekly peak demand forecast extending up to a year ahead. Currently returns empty data from the API.

**API path:** `/datasets/TSDFW`

---

# Balancing

Balancing data covers the commercial mechanisms through which the System Operator keeps supply and demand in balance: system prices, bid/offer acceptances, physical notifications, and market adjustment data.

---

## system_prices

System Buy Price (SBP) and System Sell Price (SSP) for each half-hour settlement period. Parties who are short (consumed more than contracted) pay the SBP; parties who are long receive the SSP. The spread funds the SO's balancing costs.

**API path:** `/balancing/settlement/system-prices/{YYYY-MM-DD}`
**Param style:** Date embedded in URL path
**Update frequency:** Hourly (multiple settlement runs: II, SF, R1–R3, RF, DF)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `system_sell_price`, `system_buy_price`, `net_imbalance_volume`, `run_type`

| settlement_date | settlement_period | timestamp_utc          | system_sell_price | system_buy_price | net_imbalance_volume | run_type |
|----------------|-------------------|------------------------|------------------:|----------------:|---------------------:|----------|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |             82.45 |           94.10 |               -145.2 | II       |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |             78.20 |           89.55 |                210.8 | II       |

> Prices in £/MWh. Volume in MWh. `run_type`: II = Initial Interim, SF = Settlement Final, R1–R3 = Reconciliation runs.

---

## market_depth

Settlement Market Depth. Detailed bid and offer volumes per settlement period, showing the total volume of accepted offers, accepted bids, and the net indicated imbalance position. Provides visibility into how deeply the SO had to go into the bid/offer stack.

**API path:** `/balancing/settlement/market-depth/{YYYY-MM-DD}`
**Param style:** Date embedded in URL path
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `indicated_imbalance_mwh`, `offer_volume_mwh`, `bid_volume_mwh`, `total_accepted_offer_volume_mwh`, `total_accepted_bid_volume_mwh`

| settlement_date | settlement_period | timestamp_utc          | indicated_imbalance_mwh | offer_volume_mwh | bid_volume_mwh | total_accepted_offer_volume_mwh |
|----------------|-------------------|------------------------|------------------------:|-----------------:|---------------:|--------------------------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |                  -145.2 |           1250.0 |         -820.0 |                           430.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |                   210.8 |            980.0 |         -650.0 |                           330.0 |

> Volumes in MWh. Positive imbalance = system long; negative = system short.

---

## mid

Market Index Data (MID). Volume-weighted average price and total volume traded across the day-ahead and intraday electricity markets for each settlement period, as reported by Market Index Data Providers.

**API path:** `/datasets/MID`
**Param style:** `?from=ISO&to=ISO`
**Update frequency:** Hourly
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `data_provider_id`, `market_index_price`, `market_index_volume`

| settlement_date | settlement_period | timestamp_utc          | data_provider_id | market_index_price | market_index_volume |
|----------------|-------------------|------------------------|------------------|-----------------:|-------------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | APXMIDP          |             77.50 |             1250.0 |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | N2EXMIDP         |             77.80 |              820.0 |

> Price in £/MWh; volume in MWh. Used alongside `system_prices` to calculate BSUoS imbalance charges.

---

## boal

Bid/Offer Acceptance Levels Final (BOALF). Records each instruction from the SO to a BM unit, specifying the MW level the unit must reach. Positive levels indicate generation increases (offer acceptances); negative indicate reductions (bid acceptances).

**API path:** `/datasets/BOALF`
**Param style:** `?from=ISO&to=ISO`
**Update frequency:** Hourly (as acceptances are issued)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `bm_unit_id`, `acceptance_number`, `bid_offer_level_from`, `bid_offer_level_to`, `so_flag`, `stor_flag`

| settlement_date | settlement_period | timestamp_utc          | bm_unit_id  | acceptance_number | bid_offer_level_from | bid_offer_level_to | so_flag |
|----------------|-------------------|------------------------|-------------|------------------:|---------------------:|------------------:|---------|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | T_DRAXX-1   |                 1 |                  0.0 |             300.0 | true    |
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | T_COTPS-3   |                 1 |                  0.0 |             -50.0 | false   |

> Levels in MW. `so_flag`: true = System Operator action (not commercial dispatch).

---

## pn

Physical Notifications (PN). The MW output level a BM unit declares it intends to operate at for each settlement period. Submitted by generators/demand units up to gate closure. Deviations from PN incur imbalance charges.

**API path:** `/datasets/PN`
**Param style:** `?settlementDate=YYYY-MM-DD&settlementPeriod=N`
**Update frequency:** Hourly (up to gate closure)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `bm_unit_id`, `level_from`, `level_to`

| settlement_date | settlement_period | timestamp_utc          | bm_unit_id | level_from | level_to |
|----------------|-------------------|------------------------|------------|----------:|---------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | T_DRAXX-1  |      620.0 |    620.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 | T_DRAXX-1  |      620.0 |    650.0 |

> Levels in MW. `level_from` = start of period; `level_to` = end. Equal values indicate flat output.

---

## disbsad

Disaggregated Balancing Services Adjustment Data (DISBSAD). Individual cost and volume components of the BSUoS charge, broken down by action. Enables granular analysis of what drove balancing costs in each period.

**API path:** `/datasets/DISBSAD`
**Param style:** `?from=ISO&to=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `adjustment_action_id`, `so_flag`, `stor_flag`, `cost`, `volume`

| settlement_date | settlement_period | timestamp_utc          | adjustment_action_id | so_flag | cost    | volume |
|----------------|-------------------|------------------------|---------------------|---------|--------:|-------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 | 1001                | true    |  1250.0 |   15.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 | 1003                | true    |   875.0 |   10.5 |

> Cost in £; volume in MWh. Positive = payment to provider; negative = clawback.

---

## netbsad

Net Balancing Services Adjustment Data (NETBSAD). Aggregated net cost and volume adjustments to the BSUoS charge per settlement period (the sum of all DISBSAD actions).

**API path:** `/datasets/NETBSAD`
**Param style:** `?from=ISO&to=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `net_buy_price_adjustment`, `net_sell_price_adjustment`, `net_buy_volume_adjustment`, `net_sell_volume_adjustment`

| settlement_date | settlement_period | timestamp_utc          | net_buy_price_adjustment | net_sell_price_adjustment | net_buy_volume_adjustment | net_sell_volume_adjustment |
|----------------|-------------------|------------------------|------------------------:|-------------------------:|-------------------------:|---------------------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |                    12.50 |                     -8.30 |                     150.0 |                      -95.0 |

> Price adjustments in £/MWh; volume adjustments in MWh.

---

## soso

SO-SO Prices (SOSO). Records of cross-border interconnector trades between system operators, including direction, quantity, and price for each contract.

**API path:** `/datasets/SOSO`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `contract_identification`, `sender_identification`, `receiver_identification`, `trade_direction`, `trade_quantity_mw`, `trade_price`

| settlement_date | contract_identification | sender_identification | trade_direction | trade_quantity_mw | trade_price |
|----------------|------------------------|----------------------|-----------------|------------------:|------------:|
| 2024-06-15     | NGET-RTE-001           | NGET                 | Up              |            500.0  |       85.00 |
| 2024-06-15     | NGET-TSO-002           | NGET                 | Down            |            300.0  |       72.50 |

> Quantity in MW; price in £/MWh. `trade_direction`: Up = import to GB; Down = export from GB.

---

## BOD (Bid/Offer Data)

*Endpoint decommissioned by Elexon (returns 404).* Previously provided bid/offer price pairs per BM unit per settlement period.

---

## AOBE (Accepted Offered Balancing Energy)

*Not yet implemented.* ENTSO-E transparency dataset recording accepted balancing energy volumes. Sparse data — only available for recent periods.

**API path:** `/datasets/AOBE`

---

## ABUC (Balancing Reserves Under Contract)

*Not yet implemented.* Amount of Balancing Reserves Under Contract (B1720). Currently returns empty data from the API.

**API path:** `/datasets/ABUC`

---

## CCM (Cost of Congestion Management)

*Not yet implemented.* Cost of Congestion Management (B1330). Currently returns empty data from the API.

**API path:** `/datasets/CCM`

---

## Balancing Physical Data

*Not yet implemented.* Physical BM data per unit (MW levels, time-from/to). Available via opinionated endpoint `/balancing/physical` but requires per-unit queries (`?bmUnit=...`).

---

## SEL / SIL (Stable Export/Import Limits)

*Not yet implemented.* Stable Export Limit (SEL) and Stable Import Limit (SIL) per BM unit — the maximum MW a unit can stably export to or import from the grid.

**API paths:** `/datasets/SEL`, `/datasets/SIL`
**Fields:** `settlementDate`, `settlementPeriod`, `level`, `bmUnit`

---

# Notices

System notices provide forward-looking indicators of grid security and supply adequacy.

---

## lolpdrm

Loss of Load Probability and De-rated Margin (LOLPDRM). Probabilistic assessment of supply adequacy for each settlement period. LOLP indicates the probability that demand will exceed available supply; de-rated margin is the expected MW surplus after accounting for forced outage rates.

**API path:** `/datasets/LOLPDRM`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily (multiple updates)
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `loss_of_load_probability`, `derated_margin_mw`

| settlement_date | settlement_period | timestamp_utc          | loss_of_load_probability | derated_margin_mw |
|----------------|-------------------|------------------------|-------------------------:|------------------:|
| 2024-06-15     | 35                | 2024-06-15 17:00:00+00 |                 0.000012 |            4250.0 |
| 2024-06-15     | 36                | 2024-06-15 17:30:00+00 |                 0.000018 |            3980.0 |
| 2024-06-15     | 37                | 2024-06-15 18:00:00+00 |                 0.000025 |            3500.0 |

> LOLP is a probability (0.0–1.0); de-rated margin in MW. Values > 0.05 LOLP or < 500 MW margin indicate supply stress.

---

## imbalngc

Indicated Imbalance (IMBALNGC). National Grid's forecast of the expected GB system imbalance for each settlement period. Positive = surplus generation; negative = deficit.

**API path:** `/datasets/IMBALNGC`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Hourly
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `indicated_imbalance`

| settlement_date | settlement_period | timestamp_utc          | indicated_imbalance |
|----------------|-------------------|------------------------|--------------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |               -85.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |               120.5 |

> Volume in MWh. Positive = long (excess generation); negative = short (deficit).

---

## melngc

Indicated Margin (MELNGC). National Grid's forecast of the available generation margin — spare capacity above expected demand — for each settlement period. An early warning indicator of potential supply tightness.

**API path:** `/datasets/MELNGC`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Hourly
**Silver key columns:** `settlement_date`, `settlement_period`, `timestamp_utc`, `indicated_margin`

| settlement_date | settlement_period | timestamp_utc          | indicated_margin |
|----------------|-------------------|------------------------|----------------:|
| 2024-06-15     | 1                 | 2024-06-15 00:00:00+00 |          4250.0 |
| 2024-06-15     | 2                 | 2024-06-15 00:30:00+00 |          3980.0 |

> Margin in MW. Values < 1,000 MW are considered tight; negative indicates potential shortfall.

---

## Surplus Forecasts

### OCNMFD

*Not yet implemented.* Generating Plant Operating Surplus Forecast (Daily). Daily peak surplus forecast for 2–14 days ahead.

**API path:** `/datasets/OCNMFD`
**Fields:** `publishTime`, `forecastDate`, `surplus`

### OCNMF3Y

*Not yet implemented.* Generating Plant Operating Surplus Forecast (3-Year Weekly). Weekly surplus forecast for 2–156 weeks ahead.

**API path:** `/datasets/OCNMF3Y`
**Fields:** `publishTime`, `week`, `year`, `surplus`

---

## System Warnings

*Not yet implemented.* System warnings and emergency notices issued by the SO. Available via opinionated endpoint `/system/warnings`.

---

## CDN (Credit Default Notices)

*Not yet implemented.* Credit default notifications. Currently returns empty data from the API.

**API path:** `/datasets/CDN`

---

# Transmission

Transmission-level data covers system frequency and temperature measurements used in demand forecasting.

---

## freq

System Frequency (FREQ). Near-real-time measurements of GB grid frequency, sampled every few seconds. Nominal is 50.0 Hz with operational limits of ±0.5 Hz.

**API path:** `/datasets/FREQ`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Every ~15 seconds
**Silver key columns:** `timestamp_utc`, `frequency_hz`

| timestamp_utc          | frequency_hz |
|------------------------|-------------:|
| 2024-06-15 00:00:00+00 |       49.998 |
| 2024-06-15 00:00:15+00 |       50.002 |
| 2024-06-15 00:00:30+00 |       50.001 |

> Frequency in Hz. ±0.2 Hz from 50.0 Hz is normal; ±0.5 Hz triggers automatic response.

---

## temp

Temperature Data (TEMP). Recorded temperature observations — actual, normal (seasonal average), and high/low bounds — used in demand forecasting models.

**API path:** `/datasets/TEMP`
**Param style:** `?publishDateTimeFrom=ISO&publishDateTimeTo=ISO`
**Update frequency:** Daily
**Silver key columns:** `timestamp_utc`, `temperature`, `normal_temperature`, `low_temperature`, `high_temperature`

| timestamp_utc          | temperature | normal_temperature | low_temperature | high_temperature |
|------------------------|------------:|------------------:|----------------:|-----------------:|
| 2024-06-15 00:00:00+00 |         5.2 |               6.0 |             3.0 |              8.0 |
| 2024-06-15 06:00:00+00 |         3.8 |               5.5 |             2.0 |              7.5 |
| 2024-06-15 12:00:00+00 |         7.1 |               7.0 |             4.0 |             10.0 |

> Temperatures in °C. `normal_temperature` is the seasonal climatological mean; `low`/`high` are forecast bounds.

---

# Data Services

Reference data and static lookups.

---

## bmunits_reference

BM Unit Reference Data. Static lookup of all registered Balancing Mechanism units, including fuel type, registered capacity, and owning company. Refreshed weekly.

**API path:** `/reference/bmunits/all`
**Param style:** No parameters (full dataset returned in one call)
**Silver output path:** `data/silver/elexon/bmunits_reference/bmunits_reference.parquet`
**Silver key columns:** `bm_unit_id`, `bm_unit_name`, `fuel_type`, `registered_capacity_mw`, `company_name`, `gsp_group_id`

| bm_unit_id | bm_unit_name               | fuel_type | registered_capacity_mw | company_name         |
|------------|----------------------------|-----------|----------------------:|----------------------|
| E_HUMR-1   | Humber Power Station 1     | CCGT      |                 800.0 | Uniper               |
| T_COTPS-1  | Cottam Power Station 1     | COAL      |                 500.0 | EDF Energy           |
| T_DRAXX-1  | Drax Power Station 1       | BIOMASS   |                 645.0 | Drax Group           |
| T_SIZB-1   | Sizewell B Nuclear         | NUCLEAR   |                1198.0 | EDF Energy           |

> Reference data only. Not date-partitioned. Re-run ingest to refresh.

---

## IGCPU (Installed Generation Capacity Per Unit)

*Not yet implemented.* Installed generation capacity per BM unit (B1420). Currently returns empty data from the API.

**API path:** `/datasets/IGCPU`

---

## PPBR

*Not yet implemented.* Purpose and content unknown — currently returns empty data from the API.

**API path:** `/datasets/PPBR`
