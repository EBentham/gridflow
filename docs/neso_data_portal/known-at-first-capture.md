# NESO Data Portal: can gridflow's first capture serve as a row's known-at time?

Research note, 2026-10-24. Decision record: [ADR-044](../DECISION_LOG/ADR-044-neso-observed-before-target.md)
(proposed; Bobbo rules). Evidence, queries and the two Sol reviews are local under
`.planning/research/neso-known-at-20261024/` (git-ignored).

## Answer

Using the time gridflow first captured a row as its known-at is honest and leakage-safe, but **on its
own it makes none of the 48 packages eligible**. gridflow already stamps capture time on every
datastore-dump family (`capture_fallback`, ADR-035), and 28 held outputs use it and are still held.
The holds do not say "we don't know when this was known": any later stamp is safe. They say "a row
whose target had already started when it was stamped may be a revised or retrospective value"
(the forward-target rule of RULINGS 529, applied to whole files and enforced by tests only).

What does hold is that rule **applied per row** with known-at = first capture: a row version gridflow
saw before its target started is content observed ahead of its target, so it cannot be hindsight.
That is ADR-044. It makes **9 of the 48 packages partly eligible** (10 tables). The other 38 stay
held, and `transmission-losses` was already eligible in gridflow.

- Eligible under the rule (rows observed before their target start): `14-days-ahead-wind-forecasts`,
  `bsuos-fixed-tariffs`, `country-carbon-intensity-forecast`, `daily-opmr`, `ifa`, `ifa2`,
  `national-carbon-intensity-forecast`, `system-operating-plan-sop`, `viking`. For `ifa`, `ifa2` and
  `viking` only the table with an upload-time column qualifies; their weekly tables stay held.
- Eligible with a better vendor time: none (see unknown 5).
- Already eligible in gridflow, labelled held on the site: `transmission-losses` (both tables).
  `skip-rates` also has two eligible tables (summary, stage 5 PSA technology) beside seven held ones.

The front end's single reason for all 48 ("the files don't say when each row was first published")
fits only a minority: 12 tables in 11 packages were held only on publication time, 15 tables mix it
with another open question, 40 are held only for other reasons (units, year labels, period start or
end, mock data, licence), and 25 have no silver table. Corrected per-table reasons went to the
front-end session (`neso-held-reasons-corrected.md` in the shared notes).

## The named unknowns

1. **Current definition.** Three clocks per record (`registry/record.py:643`): `ckan_last_modified`
   (uploads: `published_at` = `available_at` = the file's CKAN `last_modified`), `capture_fallback`
   (dumps: `available_at` = capture time), `issue_time_evidenced`. Silver keeps every capture; `_latest`
   is the newest complete capture; as-of is capture-level (`silver/latest_views.py`,
   `select_latest_vintage`). Eligibility (`eligibility.py` `effective_eligibility`) gates only the gold
   contracts (`gold/contracts.py` `hold_reasons` / `sql_path`; both NESO gold views held) and the
   docs; `_latest` views and the front-end build ignore it.
2. **Capture history.** Bronze is append-only, one file plus a `.meta.json` sidecar per capture (time,
   CKAN `last_modified`, sha256, resource id, upload or dump). A rewritten file keeps its old capture.
   Uploads are skipped when `last_modified` is unchanged, dumps when their bytes are. Only two runs
   exist: the 2026-10-08 sweep (1,378 captures) and the 2026-10-16 refresh (109). For the 48 packages:
   462 resources, 499 captures; 425 resources captured once, 37 twice (36 of them changed). Dump
   `last_modified` does not move when content changes (IFA, Viking, NSL and others still say
   2026-09-17 after gaining rows; SOP says 2023).
3. **Revisions.** A row version is (family, resource, every non-lineage column); its known-at is the
   first complete capture holding it. Seen between the two captures: appends (IFA ITL +216, SOP +36,
   NemoLink +408), in-place revisions (national CI rewrote 100 forecasts and 30 actuals and filled
   402 gaps in 8 days; daily demand update turned 366 forecasts into actuals) and rolling
   replacements (14-day wind reissues 672 rows and drops the oldest). A point-in-time read must pick
   the capture first and then the rows: the first draft filtered rows by first-seen alone, and Sol
   showed it leaked later captures' lineage. The implemented `observed_snapshot` picks the newest
   complete capture written at or before `as_of` (per resource where the record selects per
   resource), then keeps its rows first seen before their target. `observed_archive` returns each
   qualifying version once, from the capture that first held it.
4. **Rows dated before the first capture.** Their known-at is 2026-10-08 at the earliest, so they are
   never "observed before target" unless their target is later. For every package that means no
   backtest data before 8 October 2026: in the ten tables, 99%+ of rows are history and stay held.
   Between captures a row is placed only within the 8-day window, and any issue NESO published and
   replaced between captures is lost. The readable slice grows only if captures continue.
5. **Better vendor times.** Upload-time columns (IFA, IFA2, Viking, ElecLink, NemoLink, NSL), voltage
   `last_updated` and IC auction `published_datetime` passed every check over the one window (no stamp
   after first sight; every new row stamped inside its window; no revision under an unchanged stamp),
   but that cannot show the pre-8-October history was unchanged since its stamp, which is the open
   question; Sol agreed. Disproved as row availability times: SOP report creation time (a report
   stamped 09:24Z was missing from the 11:38Z dump), DC `forecast_created` (mixed date formats; rows
   dated 17 Oct already in the 16 Oct capture) and demand-forecast performance `publish_datetime`
   (the forecast's issue time). Daily OPMR `publish_date` holds only at whole-day grain. Tariff tables
   (BSUoS fixed, AAHEDC, TNUoS) carry a `Published Date`, but it is a date with no time and BSUoS has
   one capture, so it cannot be tested yet. None adopted; re-test after more captures.

## Rows that become readable

Counts from live silver, read-only, 2026-10-24: archive versions (every version ever observed before
its target) / rows in today's snapshot.

| Table | Archive / snapshot |
|---|---|
| `wind_forecast_14d_day_ahead_bmu` | 306,195 / 154,558 |
| `national_ci_forecast` | 1,942 / 1,439 |
| `wind_forecast_14d_days_ahead` | 1,338 / 669 |
| `daily_opmr` | 213 / 200 |
| `country_ci_forecast` | 202 / 101 |
| `viking_link_ntc` | 69 / 69 |
| `ifa_itl` | 11 / 11 |
| `ifa2_ifa_itl` | 11 / 11 |
| `bsuos_fixed_tariffs` | 2 / 2 |
| `system_operating_plan` | 1 / 1 |
| **Total** | **309,984 / 157,061** |

The claim is "gridflow observed this content before its target started", not "this is a forecast":
each table keeps its own meaning (tariff Draft/Final status, issued limits, plan snapshots). Excluded
after review: `dc_forecast_4d_history` (its date labels a delivery day whose first block starts at
23:00 the evening before, so "target start" is not evidenced) and `ic_requirement_auction` (rows mix
requirements with auction results).

## What changed in code

- `registry/_observed_before_target.json`: the ten tables, validated at registry load (the table must
  have a record, be held, sit in an unheld package, have an evidenced target start, not be
  label-anchored, and not already use vendor issue times). Schema records, pins and silver outputs
  are unchanged.
- `silver/neso_data_portal/first_seen.py`: `observed_snapshot` and `observed_archive`, both refusing
  any table not in the ledger; `with_first_seen` annotates without any eligibility claim.
- The eligibility report and vault skeleton name the readable rows; gold contracts stay held.

## Per-table disposition (all 48 packages)

| Package | Table | Disposition | Rows observed before target (archive versions / in today's snapshot) | Why (open question) |
|---|---|---|---|---|
| `contract-transfer-of-obligation` | `contract_transfer_obligation` | no silver table | - | no record, bronze only (ledger: RULINGS 538; CONTRACT-TRANSFER-IDENTITY / -PERIOD TODOs) |
| `disaggregated-bsad` | `disaggregated_bsad` | no silver table | - | no record, ingest-only (ledger: identical repeated rows; 14 resources in bronze) |
| `dynamic-containment-4-day-forecast` | `dc_forecast_4d` | still held (not a time question) | - | Date order dd/mm vs mm/dd ambiguous (DC-DATE-ORDER) |
| `dynamic-containment-4-day-forecast` | `dc_forecast_4d_history` | still held | - | excluded from the rule: its date is a label anchor (EFA 1 starts 23:00 the evening before), so "target start" is not evidenced |
| `eac-mock-auction-results` | `eac_mock_results_buy_orders` | still held (not a time question) | - | EAC-MOCK-CLOCK (naive timestamp clock unknown) + package hold EAC-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-mock-auction-results` | `eac_mock_results_daily_buy_orders` | no silver table | - | ingest-only, no record (ledger: 4 daily resources header-only at vendor since 2026-06-04, RULINGS 734); package also held (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-mock-auction-results` | `eac_mock_results_daily_by_unit` | no silver table | - | ingest-only, no record (ledger: 4 daily resources header-only at vendor since 2026-06-04, RULINGS 734); package also held (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-mock-auction-results` | `eac_mock_results_daily_summary` | no silver table | - | ingest-only, no record (ledger: 4 daily resources header-only at vendor since 2026-06-04, RULINGS 734); package also held (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-mock-auction-results` | `eac_mock_results_daily_sell_orders` | no silver table | - | ingest-only, no record (ledger: 4 daily resources header-only at vendor since 2026-06-04, RULINGS 734); package also held (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-mock-auction-results` | `eac_mock_results_by_unit` | still held (not a time question) | - | EAC-MOCK-CLOCK (naive timestamp clock unknown) + package hold EAC-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-mock-auction-results` | `eac_mock_results_summary` | still held (not a time question) | - | EAC-MOCK-CLOCK (naive timestamp clock unknown) + package hold EAC-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-mock-auction-results` | `eac_mock_results_sell_orders` | still held (not a time question) | - | EAC-MOCK-CLOCK (naive timestamp clock unknown) + package hold EAC-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-3b) |
| `eac-br-mock-auction-results` | `eac_br_mock_results_buy_orders` | still held (not a time question) | - | BR-MOCK-EXPORT-CLOCK (naive timestamp clock unknown) + package hold EAC-BR-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-4b) |
| `eac-br-mock-auction-results` | `eac_br_mock_results_by_unit` | still held (not a time question) | - | BR-MOCK-EXPORT-CLOCK (naive timestamp clock unknown) + package hold EAC-BR-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-4b) |
| `eac-br-mock-auction-results` | `eac_br_mock_results_summary` | still held (not a time question) | - | BR-MOCK-EXPORT-CLOCK (naive timestamp clock unknown) + package hold EAC-BR-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-4b) |
| `eac-br-mock-auction-results` | `eac_br_mock_results_sell_orders` | still held (not a time question) | - | BR-MOCK-EXPORT-CLOCK (naive timestamp clock unknown) + package hold EAC-BR-MOCK-TEST-DATA (mock data) (package HELD, unit v0.22-K-AS-4b) |
| `long-term-forecasts-for-dc-dm-dr-requirements` | `dc_dm_dr_long_term_fc` | still held (time + another question) | - | availability of creation-dated forecast (324 rows target creation month) [non-time: Interval band unit and inclusive endpoint] |
| `obligatory-reactive-power-service-orps-utilisation` | `orps_utilisation_historic_reactive` | still held (not a time question) | - | padded '-' cell meaning; duplicate identity (ORPS-HIST-DASH/IDENTITY) |
| `obligatory-reactive-power-service-orps-utilisation` | `orps_utilisation_reactive_default` | still held (not a time question) | - | HPIm/PAPIm/PPIm price-input units (ORPS-PRICE-INPUT-UNITS) |
| `obligatory-reactive-power-service-orps-utilisation` | `orps_utilisation_reactive` | still held (not a time question) | - | archive superseded? blank vs zero (ORPS-REACTIVE-ARCHIVE) |
| `obligatory-reactive-power-service-orps-utilisation` | `orps_utilisation_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `bsuos-fixed-tariffs` | `bsuos_fixed_tariffs` | eligible under the rule (observed rows only) | 2 / 2 | Rows first seen after their target started stay held: publication date is not an evidenced issue instant; 15/17 targets start on/before vintage so forward-target rule fails |
| `gb-system-inertia-bid-and-offer-costs` | `inertia_bid_offer_costs` | still held (not a time question) | - | unit denominator GVA vs GVAs, averaging window, meaning of zero |
| `skip-rates` | `skip_rates_in_merit_psa_dsf_unit` | no silver table | - | no record, ingest-only (ledger: exact duplicate rows; RULINGS 690) |
| `skip-rates` | `skip_rates_exclusion_reasons` | still held (not a time question) | - | five-minute date stamp: slice start or end (SKIP-DATE-ENDPOINT) |
| `skip-rates` | `skip_rates_in_merit_all_bm` | still held (not a time question) | - | SKIP-DATE-ENDPOINT + ALL-BM-SEPTEMBER-COVERAGE (one resource holds 30 Sep 2026 only) |
| `skip-rates` | `skip_rates_in_merit_psa` | still held (not a time question) | - | SKIP-DATE-ENDPOINT + PSA coverage truncation (3.2M-row cap; 30 Sep 2026 only resource) |
| `skip-rates` | `skip_rates_stage6_acceptance` | still held (not a time question) | - | five-minute date stamp: slice start or end (SKIP-DATE-ENDPOINT) |
| `skip-rates` | `skip_rates_summary` | already eligible in gridflow | - | no hold |
| `skip-rates` | `skip_rates_behind_constraints_bmu` | still held (not a time question) | - | rate denominator (in-merit vs accepted volume); no UTC month-boundary recipe |
| `skip-rates` | `skip_rates_behind_constraints_cons` | still held (not a time question) | - | rate denominator (in-merit vs accepted volume); no UTC month-boundary recipe |
| `skip-rates` | `skip_rates_stage5_dsf_technology` | no silver table | - | no record, ingest-only (ledger: exact duplicate rows; RULINGS 690) |
| `skip-rates` | `skip_rates_stage6_psa` | still held (not a time question) | - | five-minute date stamp: slice start or end (SKIP-DATE-ENDPOINT) |
| `skip-rates` | `skip_rates_stage5_psa_technology` | already eligible in gridflow | - | no hold |
| `carbon-intensity-of-balancing-actions` | `ci_balancing_actions` | still held (not a time question) | - | DATETIME marks period start or end |
| `carbon-intensity-of-balancing-actions` | `ci_balancing_actions_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `country-carbon-intensity-forecast` | `country_ci_forecast` | eligible under the rule (observed rows only) | 202 / 101 | Rows first seen after their target started stay held: no issue column; which forecast issue a past target reflects, and post-period revision, undocumented |
| `national-carbon-intensity-forecast` | `national_ci_forecast` | eligible under the rule (observed rows only) | 1,942 / 1,439 | Rows first seen after their target started stay held: no issue column; which forecast issue a past target reflects, and post-period revision, undocumented |
| `national-carbon-intensity-forecast` | `national_ci_forecast_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `regional-carbon-intensity-forecast` | `regional_ci_forecast` | still held (time + another question) | - | no issue column / post-period revision undocumented [non-time: period start vs end of datetime; meaning of negative intensities] |
| `regional-carbon-intensity-forecast` | `regional_ci_forecast_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `day-ahead-constraint-flows-and-limits` | `da_constraint_flows_limits` | still held (time + another question) | - | no issue column (732,332 targets precede vintage) [non-time: period start/end and DST fold, Flow_MW unit, blank/99999 meaning, row grain] |
| `day-ahead-constraint-flows-and-limits` | `da_constraint_flows_limits_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `operational-transparency-forum-network-congestion-data` | `otf_network_congestion` | still held (time + another question) | - | when each forecast was issued (26 targets <= vintage) [non-time: Date as week start/end/label; how actuals are calculated] |
| `voltage-requirement` | `voltage_requirement` | still held (time + another question) | - | is Last Updated an original issue time with row immutable [non-time: overnight window and endpoint inclusivity, reversed ranges, V_ group codes] |
| `voltage-requirement` | `voltage_requirement_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `day-ahead-half-hourly-demand-forecast-performance` | `da_demand_fc_performance` | still held (time + another question) | - | is Publish_Datetime the publication instant [non-time: Z vs GMT/BST zone, period start/end, repeated SP4/SP5 on fold days] |
| `daily-demand-update` | `daily_demand_update` | still held (not a time question) | - | zero ND/TSD on forecast rows: zero vs no forecast |
| `daily-demand-update` | `daily_demand_update_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `historic-demand-data` | `historic_demand` | still held (not a time question) | - | literal NA meaning (missing / not applicable / zero) |
| `historic-demand-data` | `historic_demand_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `long-term-2-52-weeks-ahead-national-demand-forecast` | `demand_forecast_2_52w` | still held (not a time question) | - | ESI week definition; duplicate 2027 week 29 |
| `school-holiday-percentages` | `school_holiday_percentages` | still held (not a time question) | - | numeric scale of Total rows (fraction vs percent) |
| `transmission-losses` | `transmission_losses_financial_year` | already eligible in gridflow | - | no hold |
| `transmission-losses` | `transmission_losses_main` | already eligible in gridflow | - | no hold |
| `building-heat-model` | `building_heat_model_example_output_half` | still held (not a time question) | - | unit/scale of avg_COP; blank-cell meaning |
| `building-heat-model` | `building_heat_model_list_input` | no silver table | - | no record, ingest-only (ledger: 'no record (538)') |
| `building-heat-model` | `building_heat_model_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `fes-electricity-demand-summary-data-table-ed1` | `fes_ed1_electricity_demand` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-electricity-demand-summary-data-table-ed1` | `fes_ed1_electricity_demand_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `future-energy-scenario-electricity-supply-data-table-es1` | `fes_es1_electricity_supply` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-european-electricity-supply-data-table-es2` | `fes_es2_european_supply` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-flexibility-data-table-data-table-flx1` | `fes_flx1_flexibility` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-natural-gas-residential-and-non-domestic-i-c-heat-demand-summary-data-table-ed3` | `fes_ed3_heat_demand_sum_ed` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-natural-gas-residential-and-non-domestic-i-c-heat-demand-summary-data-table-ed3` | `fes_ed3_heat_demand_natural_gas` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-natural-gas-residential-and-non-domestic-i-c-heat-demand-summary-data-table-ed3` | `fes_ed3_heat_demand_natural_gas_sum_ed` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-road-transport-summary-data-table-ed5` | `fes_ed5_road_transport` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-whole-system-gas-supply-emissions-data-table-ws2` | `fes_ws2_gas_emissions` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `fes-whole-system-gas-supply-data-table-ws1` | `fes_ws1_gas_supply` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `levelised-cost-of-green-hydrogen` | `lcoh_capex_opex_input_assumptions_pem` | no silver table | - | package HELD(N) licence carve-out (RULINGS 477); ingest-only, catalogue page only (package HELD, unit N) |
| `levelised-cost-of-green-hydrogen` | `lcoh_levelised_cost_blue_hydrogen` | no silver table | - | package HELD(N) licence carve-out (RULINGS 477); ingest-only, catalogue page only (package HELD, unit N) |
| `levelised-cost-of-green-hydrogen` | `lcoh_levelised_cost_green_hydrogen` | no silver table | - | package HELD(N) licence carve-out (RULINGS 477); ingest-only, catalogue page only (package HELD, unit N) |
| `local-authority-level-spatial-heat-model-outputs-fes` | `fes_la_heat_model_domestic` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `local-authority-level-spatial-heat-model-outputs-fes` | `fes_la_heat_model_residential` | still held (not a time question) | - | year-label meaning (period / start vs end), blank-projection-cell meaning, units/scale (see hold text) |
| `14-days-ahead-wind-forecasts` | `wind_forecast_14d_day_ahead_bmu` | eligible under the rule (observed rows only) | 306,195 / 154,558 | Rows first seen after their target started stay held: 693 target rows at/before vintage, no issue column, no evidence of when each value was available |
| `14-days-ahead-wind-forecasts` | `wind_forecast_14d_days_ahead` | eligible under the rule (observed rows only) | 1,338 / 669 | Rows first seen after their target started stay held: 3 targets precede stated production instant and vintage; unclear if ForecastDateTime dates availability of every row |
| `daily-opmr` | `daily_opmr` | eligible under the rule (observed rows only) | 213 / 200 | Rows first seen after their target started stay held: history-bearing body (26,709 targets <= vintage); Publish Date is a date with no issue instant or zone |
| `wind-bmu-boa-volumes` | `wind_bmu_boa_volumes` | still held (not a time question) | - | row identity of repeated rows; positive volumes vs negative-curtailment definition |
| `brit-ned` | `brit_ned` | still held (time + another question) | - | bodies carry no issue time, so no label can be dated as issued [non-time: operational-date rollover, GMT/BST mapping, (a)/(b) fold markers] |
| `eleclink` | `eleclink` | still held (time + another question) | - | past targets not evidenced as unchanged since upload time [non-time: operational-date calendar/rollover; Version meaning] |
| `ifa` | `ifa_da_id_weekly_itls` | still held (time + another question) | - | forward-target rule cannot apply (every target precedes last_modified) [non-time: operational-date rollover, GMT/BST, (a)/(b) labels] |
| `ifa` | `ifa_itl` | eligible under the rule (observed rows only) | 11 / 11 | Rows first seen after their target started stay held: dump returns past targets; NESO does not say a row is unchanged since its upload time |
| `ifa2` | `ifa2_ifa_da_id_weekly_itls` | still held (time + another question) | - | forward-target rule cannot apply (every target precedes last_modified) [non-time: operational-date rollover, GMT/BST] |
| `ifa2` | `ifa2_ifa_itl` | eligible under the rule (observed rows only) | 11 / 11 | Rows first seen after their target started stay held: dump returns past targets; NESO does not say a row is unchanged since its upload time |
| `nemolink` | `nemolink_nemo_da_id_weekly_ntcs` | still held (time + another question) | - | forward-target rule cannot apply (every target precedes last_modified) [non-time: operational-date rollover, GMT/BST] |
| `nemolink` | `nemolink_intraday` | still held (not a time question) | - | body has no date; resource lifecycle and name date undocumented (target time absent; not an issue-time question) |
| `nemolink` | `nemolink_ntc` | still held (time + another question) | - | past targets not evidenced as unchanged since upload time [non-time: operational-date calendar/rollover; Version meaning] |
| `nsl` | `nsl` | still held (time + another question) | - | past targets not evidenced as unchanged since upload time [non-time: operational-date calendar/rollover; Version meaning] |
| `viking` | `viking_link_ntc` | eligible under the rule (observed rows only) | 69 / 69 | Rows first seen after their target started stay held: dump returns past targets; NESO does not say a row is unchanged since its upload time |
| `viking` | `viking_ntc` | still held (time + another question) | - | forward-target rule cannot apply (every target precedes last_modified) [non-time: operational-date rollover, GMT/BST] |
| `gis-boundaries-for-gb-dno-license-areas` | `gis_dno_license_areas_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) (package HELD, unit N) |
| `system-operating-plan-sop` | `system_operating_plan` | eligible under the rule (observed rows only) | 1 / 1 | Rows first seen after their target started stay held: dump returns latest plan version; not stated fixed/public at creation; all targets precede capture, forward-target rule cannot apply |
| `system-operating-plan-sop` | `system_operating_plan_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `interconnector-requirement-and-auction-summary-data` | `ic_requirement_auction` | still held | - | excluded from the rule: rows mix requirements with auction results; a capture just after clearing would certify a result |
| `interconnector-requirement-and-auction-summary-data` | `ic_requirement_auction_files` | no silver table | - | catalogue only: files family (DOC/HOLD/GIS resources only, no tabular silver) |
| `upcoming-trades` | `upcoming_trades` | still held (time + another question) | - | Last_Updated clock undocumented [non-time: Volume unit MW vs MWh (formula)] |
