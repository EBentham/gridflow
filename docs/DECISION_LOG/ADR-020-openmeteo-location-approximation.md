# ADR-020: Open-Meteo capacity-weighted location lists use approximate centroids

**Status:** Accepted
**Date:** 2026-05-09
**Phase:** F7.5 — Open-Meteo Connector Extension for Renewable Forecasting
**Supersedes:** None

## Context

F7.5 introduces three role-specific location lists for the Open-Meteo
connector:

- `DEMAND_LOCATIONS` — 7 UK population centres (preserved from F0).
- `WIND_LOCATIONS` — 12 capacity-weighted GB wind sites covering the
  southern North Sea (Dogger Bank, Hornsea, East Anglia, Triton Knoll),
  Irish Sea (Walney, Gwynt y Môr), Moray Firth / Forth (Beatrice,
  Seagreen), onshore Scotland (Highland Central, Borders Crystal Rig,
  Whitelee), and onshore Wales (Pen y Cymoedd).
- `SOLAR_LOCATIONS` — 6 capacity-weighted GB solar sites covering East
  Anglia (Norfolk), Wiltshire/Somerset, Kent, Cornwall, Sussex, and
  Oxfordshire.

The exact `(latitude, longitude)` for each wind and solar site is an
**approximate centroid** drawn from public capacity registers (TEC, DUKES,
GB installed-capacity reports) rather than vendor coordinates from
operator NRO submissions. For example, `WeatherLocation("hornsea", 53.88, 1.79)`
points at the rough centre of the Hornsea wind farm cluster, not the
specific turbine string mid-point that Ørsted's NRO record carries.

## Decision

**Ship the approximate centroids as the F7.5 location list.** Per-site
spatial precision is acceptable for the boostable feature inputs that
F7.5 silver supports.

Rationale:

1. Open-Meteo's underlying weather models (ERA5 archive at ~25 km native
   resolution; UKMO UKV / ECMWF IFS / GFS at 2-15 km on the forecast
   side) operate at spatial scales coarser than the 1-2 km positional
   uncertainty introduced by using a centroid. The grid-cell that any
   centroid lands in will, in practice, be the same cell as the precise
   NRO coordinates for that wind farm or solar park.
2. NRO submissions are not consistently available in machine-readable
   form across all 18 sites. Sourcing them would extend F7.5 by a
   research workstream with no expected downstream signal benefit.
3. Locations are constants in `endpoints.py` and trivial to update in a
   follow-up phase if a downstream consumer surfaces coordinate
   sensitivity in residuals.

## Consequences

- **Positive:** F7.5 ships in one focused phase with no upstream research
  blocker. The 18-location weather sample remains a representative
  capacity-weighted picture of GB renewable generation drivers.
- **Negative:** A model that surfaces residual structure correlated with
  fine-grained spatial location (e.g., a per-turbine wake-effect
  feature) would not be served well by the current sample. This is not
  expected to be a near-term modelling concern.
- **Reversal cost:** Low. The location lists are six tuples of
  dataclass instances; swapping to precise coordinates is one PR with
  no schema or migration impact.

## Re-evaluation triggers

Reopen this decision if **any** of the following occur:

1. A downstream feature pipeline shows that wind silver's residuals are
   correlated with the offset between the approximate centroid and the
   true site polygon centroid for the same farm.
2. Open-Meteo introduces a higher-spatial-resolution forecast model
   (e.g., a 1 km native resolution weather product) where centroid vs
   precise coordinates would resolve to different grid cells.
3. The capacity-weighting scheme itself shifts (e.g., a new ROC-eligible
   farm comes online in a region not currently covered) — that re-write
   is a natural moment to also tighten coordinate fidelity.

## References

- Verified hub-height availability data (the basis for `WIND_ARCHIVE_VARS`
  scope) lives in `.planning/phases/F7.5-open-meteo-renewable-extension/F7.5-CONTEXT.md`
  under "Pre-planning verifications".
- F7.5 phase scope and requirements: `.planning/phases/F7.5-open-meteo-renewable-extension/F7.5-01-PLAN.md`.
- The 18 location coordinates are codified in
  `src/gridflow/connectors/openmeteo/endpoints.py` (`WIND_LOCATIONS`
  and `SOLAR_LOCATIONS` tuples).
