# ENTSO-E — Dataset Reference

**Source:** `entsoe`
**Base URL:** `https://web-api.tp.entsoe.eu`
**Authentication:** API key required (`?securityToken=...`)
**Coverage:** GB, FR, NL, BE, DE-LU, IE-SEM (default zones; configurable)
**Timestamp format:** All timestamps are UTC, resolution-aligned (PT15M, PT30M, or PT60M)

---

## day_ahead_prices

Day-ahead electricity market clearing prices for each bidding zone. Published by noon D-1 following the EUPHEMIA auction. The primary reference price for forward contracts and imbalance settlement in many European markets.

**Document type:** `A44`
**Silver key columns:** `timestamp_utc`, `area_code`, `price_eur_mwh`, `resolution`

| timestamp_utc          | area_code | price_eur_mwh | resolution |
|------------------------|-----------|-------------:|------------|
| 2024-06-15 22:00:00+00 | GB        |         82.50 | PT30M      |
| 2024-06-15 22:30:00+00 | GB        |         79.10 | PT30M      |
| 2024-06-15 23:00:00+00 | GB        |         75.80 | PT30M      |
| 2024-06-15 22:00:00+00 | FR        |         78.20 | PT60M      |
| 2024-06-15 22:00:00+00 | DE-LU     |         76.40 | PT60M      |

> Prices in €/MWh. GB uses 30-min resolution; Continental Europe uses 60-min. `area_code` maps to EIC bidding zone codes internally but is stored as the human-readable zone label.

---

## actual_load

Actual total electricity consumption (load) per bidding zone, measured after each interval. The definitive outturn of demand, used for market settlement and demand analysis.

**Document type:** `A65` / Process type `A16` (Realised)
**Silver key columns:** `timestamp_utc`, `area_code`, `load_mw`, `resolution`

| timestamp_utc          | area_code | load_mw | resolution |
|------------------------|-----------|--------:|------------|
| 2024-06-15 22:00:00+00 | GB        | 27450.0 | PT30M      |
| 2024-06-15 22:30:00+00 | GB        | 26980.0 | PT30M      |
| 2024-06-15 23:00:00+00 | GB        | 26320.0 | PT30M      |
| 2024-06-15 22:00:00+00 | FR        | 42100.0 | PT60M      |
| 2024-06-15 22:00:00+00 | DE-LU     | 55800.0 | PT60M      |

> Load in MW. GB values reflect transmission-level demand. One row per zone per interval; deduplicated on `(timestamp_utc, area_code)`.

---

## actual_generation

Actual generation output disaggregated by production type within each bidding zone. Covers all registered generation technologies. Used to analyse the generation mix, calculate renewable penetration, and compare against capacity.

**Document type:** `A75` / Process type `A16` (Realised)
**Silver key columns:** `timestamp_utc`, `area_code`, `production_type`, `generation_mw`, `resolution`

| timestamp_utc          | area_code | production_type         | generation_mw | resolution |
|------------------------|-----------|-------------------------|-------------:|------------|
| 2024-06-15 22:00:00+00 | GB        | Fossil Gas              |       8200.0 | PT30M      |
| 2024-06-15 22:00:00+00 | GB        | Nuclear                 |       5800.0 | PT30M      |
| 2024-06-15 22:00:00+00 | GB        | Wind Onshore            |       3100.0 | PT30M      |
| 2024-06-15 22:00:00+00 | GB        | Wind Offshore           |       4050.0 | PT30M      |
| 2024-06-15 22:00:00+00 | GB        | Solar                   |          0.0 | PT30M      |

> Generation in MW. `production_type` uses ENTSO-E standard taxonomy (e.g., "Fossil Gas", "Nuclear", "Wind Offshore"). Null production types are filled with `"unknown"`. One row per zone × production_type × timestamp.

---

## cross_border_flows

Actual physical electricity flows between adjacent bidding zones. Positive values represent net flow in the `in_area_code → out_area_code` direction. Used to assess interconnector utilisation and analyse European grid integration.

**Document type:** `A88`
**Coverage:** GB↔FR, GB↔NL, GB↔BE, GB↔IE-SEM, FR↔BE, FR↔DE-LU, NL↔DE-LU, NL↔BE
**Silver key columns:** `timestamp_utc`, `in_area_code`, `out_area_code`, `flow_mw`, `resolution`

| timestamp_utc          | in_area_code | out_area_code | flow_mw | resolution |
|------------------------|--------------|---------------|--------:|------------|
| 2024-06-15 22:00:00+00 | GB           | FR            |   850.0 | PT60M      |
| 2024-06-15 22:00:00+00 | GB           | NL            |   420.0 | PT60M      |
| 2024-06-15 22:00:00+00 | GB           | BE            |  -120.0 | PT60M      |
| 2024-06-15 22:00:00+00 | GB           | IE-SEM        |   310.0 | PT60M      |
| 2024-06-15 22:00:00+00 | FR           | BE            |  1200.0 | PT60M      |

> Flow in MW. Positive = export from `in_area_code`; negative = import. One row per interconnector per timestamp; deduplicated on `(timestamp_utc, in_area_code, out_area_code)`.
