# NESO Endpoint Iteration Audit

**Date:** 2026-05-04
**Scope:** Check whether the `intensity_period` bug repeats across other NESO
Carbon Intensity API endpoints.

## Conclusion

The repeated-risk pattern is defaulted path variables that look like a single
example value but actually represent an enumerable data axis. In the official
NESO Carbon Intensity API definitions, only `/intensity/date/{date}/{period}`
has that shape for Gridflow's dataset semantics.

## Findings

- `period` is a required half-hour settlement period selector. Gridflow now
  treats `intensity_period` as an exhaustive per-date dataset by iterating every
  valid GB settlement period.
- `block` on `/intensity/stats/{from}/{to}/{block}` controls aggregation size.
  It is not a data axis to exhaust; requesting every block length would create
  duplicate alternative aggregations rather than missing records.
- `regionid` endpoints are single-region drill-down variants. Gridflow also
  registers all-region routes such as `/regional`, `/regional/intensity/{from}/{to}`,
  and the all-region fw24h/fw48h/pt24h routes, so exhaustive regional data does
  not depend on iterating `regionid`.
- `postcode` endpoints are outward-postcode drill-down variants. There is no
  finite official postcode list in this API definition, and they are not a
  replacement for the all-region routes.

## Guardrail Added

`tests/unit/test_neso_endpoints.py` now asserts the complete set of defaulted
path variables and verifies that only `intensity_period` is marked for
settlement-period iteration.

## Follow-up Fix From Audit

The review also found a same-day range-window variant of the same class of bug:
range endpoints with `{from}/{to}` were building zero-length URLs when users ran
commands with the same start and end date. The NESO request planner now expands
same-day range requests to a one-day API window, and mocked E2E tests cover the
national, statistics, generation, and regional range endpoint families.
