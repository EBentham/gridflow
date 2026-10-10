# ADR-041 — Gold forecast-versus-outturn contracts and held gold views

**Status:** accepted; G-1 disposition ruled by the seat 2026-10-10 (not built; research unit G-R)
**Date:** 2026-10-10
**Phase:** v0.22 unit G (gold: demand forecast vs outturn, wind forecast vs outturn,
interconnector limits unified)
**Cross-references:** ADR-024 (manifest-derived serving relations), ADR-025 (gold views are
not point-in-time surfaces; :236-243), ADR-034 (generic silver, `LatestViewSpec`, the two
clocks), ADR-039 (per-resource `_latest`), ADR-040 (no invented overlap precedence),
RULINGS 466 (all-vintage views; as-of only through `select_latest_vintage`; table macro
vetoed), 588 (no date-plus-interval recipe in G), 589 (held-input disposition).

## Context

Unit G asked for three gold relations over NESO inputs: a day-ahead demand forecast versus
its outturn (G-1), the day-ahead national wind forecast versus metered wind output (G-2), and
one relation of interconnector capacity limits across the links (G-3). Each must keep every
forecast vintage, carry an ex-post outturn with a fail-closed cutoff, take point-in-time as a
query parameter, and publish only when every input is eligible (decision 16).

Research found:

- **G-1.** The only eligible day-ahead demand forecast holds cardinal points (peaks, troughs,
  fixed times over local-clock windows), not settlement periods. Its `date_sp1` anchor gives
  every point the SP1 instant of its target date, so a settlement-pair join would compare
  every point with SP1. NESO documents no mapping from a cardinal point to outturn periods,
  and every half-hourly demand forecast family is held.
- **G-2.** Both sides are half-hourly MW, but NESO does not document that the monthly
  operational-metered output and the national day-ahead forecast cover the same fleet. The
  per-BMU forecast has no generator-level outturn.
- **G-3.** Every interconnector family is held (dump past targets unevidenced; archive
  calendars, rollover and fold labels undocumented). Limits are separate directional maxima,
  not a signed flow. The dumps' `available_at` is gridflow capture time.

Before this unit the gold layer had no way to keep a built view off every consumer path:
`_register_gold_views` executes every top-level `gold/views/*.sql`, and the manifest's gold
rows carry no selector or held-status field.

## Decision

**D-1 — contracts module.** `gridflow.gold.contracts` holds one `GoldViewContract` per view:
relation name, input family keys, designated date column and SQL type, and the `key_latest`
`LatestViewSpec` that `POINT_IN_TIME_SELECTOR`
(`gridflow.silver.latest_views.select_latest_vintage`) applies over the view's rows. This is
the machine-readable point-in-time path ADR-024's manifest cannot yet carry. The G specs never
enter `LATEST_VIEW_SPECS` (that dict drives silver `_latest` registration).

**D-2 — specs.** G-2 keys on `timestamp_utc` (the forecast's generated key `datetime_gmt`,
equal to `timestamp_utc` for a `utc_instant` record); G-3 keys on `family` plus the union of
the seven inputs' generated keys. Both order by the inputs' generated order (issue time where
declared, then `available_at`) and break ties on `capture_written_at`, then
`bronze_capture_id`.

**D-3 — G-3 inputs.** One current family per link: `eleclink`, `ifa_itl`, `ifa2_ifa_itl`,
`nemolink_ntc`, `nsl`, `viking_link_ntc`, `brit_ned`. The weekly archives and
`nemolink_intraday` stay out: their only target is a calendar label with no instant, which
waits on calendars (RULINGS 588). BritNed is its link's only family; its flows stay text.
`operational_period_start_gmt` is the target instant where the vendor gives one and NULL for
label-only rows; the silver `timestamp_utc` of a temporal-`none` family is capture time and is
never projected. `available_at_basis` labels each row's clock with the eligibility ledger's
label.

**D-4 — G-2 outturn.** The outturn is metered wind output's `_latest`, aggregated per
settlement pair. A pair published by more than one resource (the 2025-26 / 2026-27 overlap,
ADR-040) gets every outturn value NULL and `outturn_rows > 1`: no precedence is invented, and
the forecast row is never duplicated. The outturn columns' comments say ex-post; the
`outturn_available_at` cutoff is fail-closed, not historical point-in-time.

## Held representation and promotion

**Unregistered until eligible.** A contract with any `hold_reasons()` keeps its SQL at
`gold/views/held/<stem>.sql`. The default glob is top-level only, so `refresh_views` never
registers it; it has no `_SERVING_ALIASES` row, so no manifest row and no SDK handle; it is in
no eligibility report. `hold_reasons()` reads each input's `Held` question and unit from the
registry at call time, verbatim, then appends the contract's pairing hold. An input without a
silver record raises, because the eligibility rule's default would call it eligible. SQL
comments carry no hold text.

**Promotion.** When `is_published()` turns true, the lifting unit moves the file to
`gold/views/`, adds the `_SERVING_ALIASES` gold row (relation, designated date column), adds
the SDK handle the serving-constant test demands, and adds the date column to
`DATE_COL_SQL_TYPES` if it is new. The location test goes red until it does.

At this ADR both views are held: G-2 on its pairing hold (the fleet question, research unit
`G-R`), G-3 on its seven inputs' holds.

## Point-in-time semantics

- **Per target.** "The latest forecast for target T available at `as_of`" is a per-target
  selection (`key_latest`). A newer capture that omits an older target does not retract that
  target's last forecast. Each input's silver `_latest` stays whole-capture (unchanged).
- **Complete captures only.** A silver row enters a G view only when its capture has a
  `populated` completion record whose `row_count` equals the capture's silver rows: the
  populated arm of the silver whole-capture eligibility, rendered as a semi-join. The silver
  selector also admits a `valid_empty` / 0 completion without comparing silver rows, so when
  such a completion coexists with silver rows the silver selector returns them and the G views
  do not. That state is a reconcile gap; G is deliberately stricter and the silver selector is
  not touched.
- **Static.** Each view is one `CREATE OR REPLACE VIEW` with no parameter. The as-of bound
  exists only in the selector: Polars `select_latest_vintage(lf, spec, as_of=...)` or SQL
  `latest_select_sql(relation, spec, columns, as_of_param=True)` (RULINGS 466).
- **Projection parsing.** `select_list_columns` (extracted from `_gold_sql_columns`, output
  unchanged) reads the first `SELECT ... FROM`. G SQL keeps its public projection first (no
  CTE), one column per line, no comment inside it, and no standalone word `select` in its
  header comments.

## G-1 disposition

G-1 (`gold_gb_demand_forecast_vs_outturn`) is not built. A settlement-pair view over cardinal
points would encode an invented comparison, and a forecast-only or cardinal-grain view would
be a different contract. Its unknowns go to a research unit: a NESO-supported
cardinal-point-to-outturn contract, or an eligible half-hourly day-ahead National Demand
forecast. Elexon INDO is the measure-matched outturn candidate; its missing `_latest` belongs
to that unit.

## Consequences

- No G view is published; no manifest row, SDK handle or default registration is added, and
  the manifest frame of every existing row is unchanged.
- The held views are built and tested on fixtures through the real transformers and
  `refresh_views`, so a lifted hold needs only the promotion steps.
- FM-15 (a newer capture omitting an older target keeps that target's last forecast) is
  accepted by design.
- A future `SilverSchemaEntry` field for the selector or held status remains open; adding one
  for zero eligible views would change every manifest row.
