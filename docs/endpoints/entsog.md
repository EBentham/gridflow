# ENTSO-G — Dataset Reference

**Source:** `entsog`
**Base URL:** `https://transparency.entsog.eu/api/v1`
**Authentication:** None (public API)
**Coverage:** Full public ENTSO-G physical-flow response for the requested date window
**Normalisation:** All volumes converted to GWh/day regardless of source unit

---

## physical_flows

Daily physical gas flows returned by ENTSO-G for the requested date window. Each row captures the volume of gas that physically crossed a border or transit point on a given day, in a specific direction.

**API path:** `/operationalData` (exact-case; the API rejects a lower-cased path)
**Param style:** `?from=YYYY-MM-DD&to=YYYY-MM-DD&indicator=Physical Flow&periodType=day`
**Silver key columns:** `timestamp_utc`, `point_key`, `point_label`, `operator_key`, `operator_label`, `direction_key`, `flow_gwh_per_day`, `unit`

| timestamp_utc          | point_key | point_label                | direction_key | flow_gwh_per_day |
|------------------------|-----------|----------------------------|---------------|----------------:|
| 2024-06-15 04:00:00+00 | IUK       | Interconnector UK–Belgium  | entry         |           485.2 |
| 2024-06-15 04:00:00+00 | BBL       | Balgzand-Bacton Line       | entry         |           310.8 |
| 2024-06-15 04:00:00+00 | FRAN      | France–UK                  | exit          |           -42.0 |
| 2024-06-15 04:00:00+00 | IRL       | GB–Ireland                 | exit          |           128.5 |
| 2024-06-15 04:00:00+00 | NORI      | Norway–UK (NORPIPE)        | entry         |           920.0 |

> Flow in GWh/day; the silver `unit` column reads `GWh/d` to match (the raw vendor unit, e.g. `kWh/d`, is normalised away). `direction_key`: `entry` = gas entering the reported system; `exit` = gas leaving the reported system. The gas day starts at 06:00 local time, so a gas-day-boundary timestamp lands at 04:00 UTC (summer) / 05:00 UTC (winter), not 00:00. Raw data may be in kWh/h or kWh/day — the transformer normalises all values to GWh/day. Deduplicated on `(timestamp_utc, point_key, operator_key, direction_key)`.
