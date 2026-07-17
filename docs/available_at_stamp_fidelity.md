# `available_at` stamp-fidelity

**Date:** 2026-07-17 · **Authority:** [ADR-025](DECISION_LOG/ADR-025-temporal-vintage-and-revision-capture.md) §3 · **Item:** v0.17 P1.1 (R1-F07 + R5-F04)

`available_at` is derived at the silver-write boundary
(`silver/base.py::_add_bitemporal_columns`) as a **row-wise coalesce**:

```
available_at = coalesce(published_at, ingest_time)
```

When a transformer emits a non-null `published_at` (the vendor publication
vintage) for a row, that row's `available_at` is the vintage; otherwise it falls
back to the ingest/reingest clock. This table records, per dataset, whether
`available_at` is a genuine vendor vintage or an honest ingest-time fallback — the
contract downstream consumers (the `gridflow_models` point-in-time barrier;
review items P2.30/P2.32) read to know which datasets support honest historical
`as_of` queries.

## Fidelity labels

- **true vintage** — `published_at` is sourced from a vendor publication stamp
  whose semantics are established (a forecast/auction issue time, or an Elexon
  `publishTime`); `available_at` is a genuine "when this was knowable" instant.
- **unverified vintage** — `published_at` is mechanically wired from the ENTSO-E
  document `createdDateTime`, but that field's provenance for this document family
  is unconfirmed (submission-time vs. serialization-time). Pending an authorized
  live-sample check comparing `createdDateTime` to the bronze sidecar `fetched_at`.
  Treat as ingest-grade until verified.
- **ingest-time fallback** — no vendor stamp is emitted (the feed has none, or the
  transformer consumes `publishTime` as its event/timestamp axis rather than
  emitting `published_at`); `available_at` is the ingest/reingest clock, honestly.

## Elexon

| Dataset | Fidelity | Source field | Notes |
|---|---|---|---|
| agpt, agws, atl, demand_forecast, fou2t14d, fuelhh, imbalngc, inddem, indgen, indo, itsdo, lolpdrm, melngc, nonbm, tsdfd, uou2t14d, **windfor** | **true vintage** | `publishTime` / `publishDateTime` → `published_at` | 17 emitters. `windfor` is the only one with on-disk silver — re-transformed under 4.1b so its `available_at` reflects the forecast-issue spread. |
| system_prices | ingest-time fallback | — | live DISEBSP DATE_PATH feed exposes no `publishTime` (ADR-025 §Context); honest fallback, not a gap. |
| bmunits_reference | ingest-time fallback | — | static reference snapshot; `publishTime` not mapped. |
| remit, fuelinst, soso, indod, tsdf | ingest-time fallback | `publishTime` → `timestamp_utc` | map `publishTime` as their event/timestamp axis, do NOT emit a separate `published_at`. (Emitting it for `remit` would invert `event_time <= available_at` — advance outage notices publish before the event window.) |
| temp, and all other Elexon datasets | ingest-time fallback | — | no `publishTime` emitted as `published_at`. |

## ENTSO-E

Every timeseries-parsed dataset (25 of 26 modules) now emits `published_at` from
the document `createdDateTime` (typed-null when the document lacks it). Fidelity is
split by whether `createdDateTime` is an established vendor vintage.

| Dataset(s) | Fidelity | Notes |
|---|---|---|
| generation_forecast, load_forecast, load_forecast_weekly, load_forecast_monthly, load_forecast_yearly, wind_solar_forecast | **true vintage** | forecast issue-time; established semantics. |
| day_ahead_prices, forecast_margin, contracted_reserves, net_transfer_capacity, installed_capacity, installed_capacity_units | **true vintage** | ex-ante / day-ahead products; `installed_capacity*` noted "thin spread — annual cadence". |
| actual_load, actual_generation, actual_generation_units, cross_border_flows, imbalance_prices, imbalance_volume, activated_balancing_prices, activated_balancing_qty, outages_generation, water_reservoirs; the 4 outages (h7) datasets; the 15 transmission/market (h6) datasets; the 6 balancing (h8) datasets | **unverified (createdDateTime provenance unconfirmed — pending an authorized live-sample check)** | observational / mixed families. Wired mechanically (safe), but whether `createdDateTime` is a genuine publication vintage or a request/serialization artifact is unverifiable locally (no ENTSO-E bronze on disk; fixtures are synthetic). Do NOT treat as leakage-safe until the live check upgrades the label. |
| generation_units_master_data | ingest-time fallback (structural) | its parser carries no `createdDateTime` field; deliberately NOT wired. |

## Open-Meteo

| Dataset(s) | Fidelity | Notes |
|---|---|---|
| all Open-Meteo datasets | ingest-time fallback | the API exposes no document-level publication timestamp; no coalesce input exists. |

## Upgrade path

The **unverified** ENTSO-E families upgrade to **true vintage** with one authorized
live-sample check per document family: fetch one live document, compare its
`createdDateTime` to the bronze sidecar `fetched_at`/`written_at`. A distinct,
plausibly-earlier `createdDateTime` confirms a genuine vendor vintage; a value that
tracks fetch time indicates a request/serialization artifact (label stays
ingest-grade). No live ingestion was performed for this table — labels are honest,
conservative, and revisable.

Downstream: P2.30 (per-fold publication cutoff) and P2.32 (tie-break dedupe on
`available_at` / `published_at`) consume the **true vintage** column — do not weaken
a label to make the table look more complete.
