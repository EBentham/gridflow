# ADR-017 — `APPEND_ONLY` is a class attribute on `BaseSilverTransformer`

Status: Accepted (F7, 2026-05-07)

## Context

Phase F7 introduces revision-preserving silver storage for two datasets
(REMIT outage messages and FOU2T14D forward availability) that publish
genuine revisions over time. Most other gridflow datasets (system prices,
fuel-mix half-hourly, demand forecasts, weather, ENTSO-G capacities, etc.)
do not benefit from per-run history retention; the F0 atomic-replace
pattern is correct for them and turning append-only behaviour on
universally would fragment silver and complicate every read path without
delivering value.

## Decision

`BaseSilverTransformer` exposes `APPEND_ONLY: ClassVar[bool] = False`. Each
transformer subclass overrides the flag if and only if revision history is
material. The base class `run()` and `_write_silver()` branch on the flag:
default `False` keeps the F0 atomic-replace behaviour unchanged, while
`True` writes to a run-suffixed filename (see ADR-018) so successive
runs coexist in the partition directory.

## Consequences

- Per-dataset opt-in: REMIT and FOU2T14D set `APPEND_ONLY = True`; every
  other transformer inherits the default.
- The flag is a static contract checked at class load time, not a runtime
  parameter — there is no risk of a code path forgetting to thread it.
- Existing F0 tests pass without modification because the default branch
  is byte-for-byte identical to the previous implementation.
- Future revision-publishing datasets only need to set the flag and (if
  desired) supply per-dataset partition columns to the read-time
  `QUALIFY` clause; no further base-class changes are required.
