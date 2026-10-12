# Provenance: skip-rate batch fixtures (v0.22-K-BAL-2b)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line (a BOM stays); only the line
selection is ours. `git` normalises the committed line endings (the four BOM bodies are CRLF, the
others LF), so the tests rebuild CRLF for exactly those four. The "sha8" is the first eight hex
digits of the original body's SHA-256, the suffix of the bronze file name. "Lines" are 1-based line
numbers of the original body (1 is the header). Nothing is repaired or re-decoded.

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---:|---|
| `su.csv` | `skip_rates_summary` | `e4129210-ea22-4a36-aa2f-ff7bd6147675` | `1_skip_rates_summary.csv` | `d05800a9` | 190,368 | 1-8 (first period, stages 0-5, then stage 0 of the next), 24896 (the wholly blank measure row, 2025-03-11T10:30:00 stage 0) |
| `tp.csv` | `skip_rates_stage5_psa_technology` | `007c5786-bd34-4a09-8bdf-be5d63eda16a` | `5_fuel_type_skip_rate.csv` | `9818bf02` | 132,139 | 1-6, 17 (two blank-fuel rows) |
| `bm.csv` | `skip_rates_behind_constraints_bmu` | `26f3a3de-e5fd-43e5-9d8a-d89a9c786812` | `skips_behind_constraints_in_merit_bmu_level.csv` | `73112eba` | 5,871 | 1-5 (BOM; line 5 a zero rate), 10 (blank `fuel_type`) |
| `co.csv` | `skip_rates_behind_constraints_cons` | `a88adc4c-d3a6-440d-946b-4e5a3e7ea1c5` | `skips_behind_constraints_in_merit_constraint_level.csv` | `9d1ac736` | 236 | 1-4 (BOM), 57 (a zero rate) |
| `s6a.csv` | `skip_rates_stage6_acceptance` | `235e5cf1-745a-46e3-9e64-f7c8532abdc1` | `9_s6_acceptances.csv` | `0305f9aa` | 2,909,408 | 1-4 (BOM), 182 (`pair_id` -1), 3724 (blank fuel) |
| `s6p.csv` | `skip_rates_stage6_psa` | `32072115-34bd-4838-92ab-96803e1b692d` | `8_in_merit_s6_data_post_system_actions_skips.csv` | `ef4ed615` | 5,535,746 | 1-4 (BOM), 34 (accepted volume non-zero), 61 (blank fuel), 215 (`pair_id` -1) |
| `abz.csv` | `skip_rates_in_merit_all_bm` | `ca3587a2-ae0d-47db-b0a1-4d681af8bced` | `2_in_merit_data_bm_skips_09_2026.csv` | `9c33c700` | 387,189 | 1-4, 6, 141, 371, 846 (`Z` spelling; stage 0, 1 and 2, negative pair ids, a blank fuel) |
| `abn.csv` | `skip_rates_in_merit_all_bm` | `fb15911f-ddda-4b87-8334-471430c0e70f` | `2_in_merit_data_bm_skips_10_2026.csv` | `2bfd8b61` | 1,369,735 | 1-4, 160 (naive spelling; a blank fuel) |
| `pa.csv` | `skip_rates_in_merit_psa` | `3a4863d1-220e-43e5-9049-94db1045d92c` | `3_in_merit_data_post_system_actions_skips_01_2025.csv` | `ba432343` | 1,987,991 | 1-4, 36, 174 (`Z`; a blank fuel, a stage 1 row) |
| `ex.csv` | `skip_rates_exclusion_reasons` | `f8110eb8-f4ad-4288-87d5-37ab434b55b5` | `4_exclusions_10_2026.csv` | `0e635ffa` | 319,032 | 1-4, 17, 151, 3307 (naive; a `pair_id` -1 row at exclusion stage 2, a blank fuel, a zero average price) |

The sidecar facts the tests write (package slug and id, resource name, `ckan_last_modified`, the
`written_at` capture time) are the originals', in `test_neso_bal2b_records.py::CAPTURES`.

The row totals the records must reproduce (the seat's full-body check, RULINGS 690, `BAL-2b-BRONZE-CHECK.json`;
the bodies are not re-scanned): summary 190,368; stage 5 PSA technology 132,139; behind-constraints
5,871 and 236; all-BM 84,931,005 (23 resources); in-merit PSA 65,776,812 (23); exclusion reasons
15,575,765 (23); stage 6 acceptance 2,909,408; stage 6 PSA 5,535,746. The two DSF families (431,215
and 7,202 rows) get no record.

## Open TODOs the records carry (the record model has no notes field)

- SKIP-DATE-ENDPOINT (the five five-minute families): the single UTC `date` stamp's endpoint
  (slice start or end) is undocumented; the label is kept as the raw string `date_vendor`, temporal
  `none`. (`date` is a designated DATE name and may not be reused for a string column.)
- CONSTRAINT-RATE-DENOMINATOR (both behind-constraints families): the dictionary says percentage of
  in-merit volume skipped, the methodology presentation says accepted volume; the published value is
  kept as Percentage. The UTC `month` is kept as the raw `YYYY-MM` label `month_vendor` (`month` is a
  reserved Hive partition name and the `month` recipe anchors a London midnight, not a UTC month).
- ALL-BM-SEPTEMBER-COVERAGE, PSA-MARCH/SEPTEMBER-COVERAGE: resources `ca3587a2...` and `38421f3d...`
  hold 30 September 2026 only; `a04c4c6e...` stops at exactly 3,200,000 rows on 2026-03-24T15:45:00.
  Whole-month coverage is not claimed.
- STAGE6-PAIR-ID-UNIT (`skip_rates_stage6_acceptance`): the dictionary gives `pair_id` the unit MWh
  while describing a signed identifier; kept an int64 identifier with no unit (conflict noted).
- SUMMARY-NO-STAGE-6 (`skip_rates_summary`): the summary has stages 0-5 only and no stage-6 rows; the
  separate stage-6 resources are not implied by it.
- `pair_id = -1` is an identifier (negative pairs are below FPN, positive above), not a missing
  sentinel; stage 0 is a methodology stage. Neither is a settlement `run_type`.
- `skip_rates_in_merit_psa_dsf_unit` (7,830 excess) and `skip_rates_stage5_dsf_technology` (192
  excess) have no record: the newest bodies repeat whole rows, no key passes, no ordinal or dedup is
  invented (RULINGS 538). Vendor question: are the repeated rows intended?
