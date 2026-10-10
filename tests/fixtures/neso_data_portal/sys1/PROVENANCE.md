# Provenance: system batch fixtures (v0.22-K-SYS-1)

Every fixture is a cut of a captured NESO Data Portal body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (the voltage utilisation body under
`.../voltage_units_utilisation/2026/10/10/`), read-only. A cut keeps the original header and the
original bytes of each kept line (a leading BOM stays); only the line selection is ours. `git`
normalises the committed line endings (the originals are CRLF throughout, with no embedded line
feeds), so the tests rebuild CRLF with `body()`. The "sha8" is the first eight hex digits of the
original body's SHA-256, the suffix of the bronze file name. "Lines" are 1-based line numbers of the
original body (1 is the header).

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---:|---|
| `v.csv` | `voltage_units_utilisation` | `e13539d0-eed0-4561-82a4-4517c14253c1` | `utilisation-report.csv` | `556901ab` | 1 | whole body |
| `m.csv` | `stability_midterm_y1` | `e0c86d21-8ad1-4ba3-a112-b2bbcf5277ce` | `stability.csv` | `998df585` | 61 | whole body |
| `u23.csv` | `stability_pathfinder_utilisation_report` | `52af0f68-e1c3-45dd-ae7f-54f68e32e0c6` | `stability-pathfinder-utilisation-report-2023.csv` | `e96ce2e1` | 110,891 | 1-4, 85, 95, 129, 141 (service cease / start), 92410, 92411 (non-blank inertia), 92611, 92612 (zero inertia) |
| `u25.csv` | `stability_pathfinder_utilisation_report` | `fc08cdba-1a86-460b-85c4-3d8c0d3d4e42` | `stability-pathfinder-utilisation-report-2025.csv` | `e7ac1433` | 258,388 | 1-4, 30386-30391 (the identical `THRSC-1` pair is lines 30388 and 30389) |
| `u26.csv` | `stability_pathfinder_utilisation_report` | `75996c0a-70fb-4ddb-a345-bb1e8d39de35` | `stability-pathfinder-utilisation-report-2026.csv` | `f736cc2e` | 123,987 | 1-4, 4541, 4542, 15041, 15377 |
| `a23.csv` | `stability_pathfinder_availability_report` | `a4fd8208-3e36-4f9c-bdc7-435c3734a2da` | `stability-pathfinder-availability-report-2023.csv` | `eeaf24d9` | 110,887 | 1-4, 40995, 44019 (the identical `RASSP-1` pair) |
| `a26.csv` | `stability_pathfinder_availability_report` | `690a9e42-20e2-4b0e-b74f-f3e37cac4bac` | `stability-pathfinder-availability-report-2026.csv` | `cdf0f939` | 132,011 | 1-4, 128501, 128503 (the one real `Unvailable` row), 128505, 128506; no identical rows |
| `i21.csv` | `system_inertia` | `55161fb4-1396-46e2-9250-2e2b9df904bf` | `inertia.csv` | `4f2f6191` | 17,520 | 1-4, plus every row of 2021-10-31 (50 periods) and 2022-03-27 (46 periods) |
| `i26.csv` | `system_inertia` | `3ff8b466-5c16-4713-abfe-ad332298f15f` | `inertia.csv` | `a35f3073` | 4,788 | 1-5 |
| `c17.csv` | `system_inertia_cost` | `28dc603e-3472-45cd-8e7b-998efc674084` | `inertia_costs17.csv` | `4d37dd62` | 365 | 1-7 (header `Settlement Date,Cost`, DMY dates) |
| `c22.csv` | `system_inertia_cost` | `8a3a4233-e9aa-45c2-b0c4-28b8d614165b` | `inertia_costs22.csv` | `eb4859d0` | 362 | 1-7 (header `Cost_per_GVAs`, ISO dates) |
| `c24.csv` | `system_inertia_cost` | `91947c0c-bbbf-4d55-a29d-dc610d91d075` | `inertia_costs.csv` | `fb49c31e` | 365 | 1-7 (header `Cost_per_GVAs`, ISO dates; a 2284 cost is line 7) |
| `c26.csv` | `system_inertia_cost` | `6295f4ed-b43d-4a80-8ca9-c27c9fa16517` | `inertia_costs.csv` | `1f7fa566` | 1 | whole body (`01/04/2026`, the resource-level HOLD) |
| `h14.csv` | `outturn_voltage_costs_historical` | `fae5a592-deb4-4c66-8d51-d6ef450ccd95` | `voltagecsv-2014_15.csv` | `7a56317e` | 228 | 1-9 (DMY dates) |
| `h15.csv` | `outturn_voltage_costs_historical` | `035ab58a-7b96-4e10-bf51-cf3a7e64f6a0` | `voltagecsv-2015_16.csv` | `f95af6ec` | 228 | 1-9 (ISO dates) |
| `h24.csv` | `outturn_voltage_costs_historical` | `3eced73f-b7e6-4974-8d3c-3ebd48eba74c` | `voltagecsv-2024_25.csv` | `b494b559` | 228 | 1-9 (DMY dates) |
| `vm.csv` | `outturn_voltage_costs_main` | `073f9ffa-05d5-47e5-8835-e1ac31b7656d` | `voltagecsv-2025_26.csv` | `da93654c` | 38 | whole body |

The sidecar facts the tests write (package slug and id, resource name, `ckan_last_modified`, the
`written_at` capture time) are the originals', in `test_neso_sys1_records.py::CAPTURES`.

## Open TODOs the records carry (the record model has no notes field)

- SYS0-KEY, SYS0-REPLACEMENT, SYS0-AGGREGATION (`voltage_units_utilisation`): no vendor primary-key
  guarantee for BMU and month; whether a later monthly upload replaces or appends; the sign and
  aggregation convention of the signed MVAr totals.
- MIDTERM-DURATION (`stability_midterm_y1`): the duration text's format, rounding and four
  disagreeing rows; `Hours Utilised` stays a string.
- INERTIA-SPARSITY (`system_inertia`): why 2022-23, 2023-24 and 2026-27 have 2-period days; nothing
  is filled.
- INERTIA-COST-ZERO (`system_inertia_cost`): what a zero cost means; zeros stay numeric.
- VOLTAGE-COST-ZERO (`outturn_voltage_costs_*`): whether a zero cost has a special meaning; zeros
  stay numeric.
- Pathfinder (`stability_pathfinder_*`): the clock of the four time columns, the unit of
  `Inertia`, the meaning of a blank `Inertia`, and the vendor row identity; the held questions in
  the records carry them.
