# K-SCN-2b fixtures

Cut from the 2026-10-08 swept bronze under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (read-only; a scratch script that splits
each body on its CRLF record terminators and keeps each record's physical bytes, not committed). Every
ED3 / ED7 fixture is the body's header (BOM and CRLF kept) plus whole vendor records, byte for byte; the
four ED4 fixtures are the whole bodies. `.gitattributes` (`*.csv -text`) keeps the CRLF terminators and
BOMs through git.

| Fixture | Family | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---:|---|
| `ed3c_23.csv` | `fes_ed3_heat_demand_natural_gas` | 1f1de28d-eff7-4c5b-83fa-33cd7ad1bd39 | `fes2023_ed3_v001.csv` | 2023-07-10T03:31:06.331229 | 2026-10-08T10:53:59.217853+00:00 | 199414 | a001581d |
| `ed3c_24.csv` | `fes_ed3_heat_demand_natural_gas` | fb452e53-881c-4d4e-a236-a5b6b50f2840 | `fes2024_ed3_v005.csv` | 2025-02-17T17:12:48.492536 | 2026-10-08T10:54:01.922700+00:00 | 246977 | 8d2da99d |
| `ed3g_25.csv` | `fes_ed3_heat_demand_natural_gas_sum_ed` | f33e8ded-d302-40c4-be10-c7ca3a04b401 | `fes2025_ed3_v006.csv` | 2025-12-10T16:50:20.589918 | 2026-10-08T10:54:06.022320+00:00 | 21287 | 224f836d |
| `ed3g_26.csv` | `fes_ed3_heat_demand_natural_gas_sum_ed` | c3d4acaf-5cd5-47f8-81a9-a4c10e16d55a | `10yo2026_ed3_v001.csv` | 2026-09-16T14:54:22.558692 | 2026-10-08T10:54:08.488330+00:00 | 3159 | 0c370a56 |
| `ed7_25.csv` | `fes_ed3_heat_demand_sum_ed` | 2019e900-11eb-40ce-ae54-a49a2abb5d4b | `fes2025_ed7_v006.csv` | 2025-12-10T16:51:16.997421 | 2026-10-08T10:54:12.302570+00:00 | 294208 | b001ee88 |
| `ed7_26.csv` | `fes_ed3_heat_demand_sum_ed` | 9394f025-8454-4db0-b73c-083e301a1e74 | `10yo2026_ed7_v001.csv` | 2026-09-16T14:55:40.190751 | 2026-10-08T10:54:15.301755+00:00 | 40211 | 84188f8b |
| `ed4_23.csv` | `fes_ed4_gas_demand_defs` | 60a8299f-9ba1-4663-9ece-8005a8a42fa2 | `fes2023_ed4_v001.csv` | 2023-07-10T03:31:27.976626 | 2026-10-08T10:54:18.774686+00:00 | 1515 | d8eeb11e |
| `ed4_24.csv` | `fes_ed4_gas_demand_defs` | a014dfba-0728-448a-a2cc-d34fec0703af | `fes2024_ed4_v001.csv` | 2024-07-15T06:40:17.398169 | 2026-10-08T10:54:21.628757+00:00 | 1570 | 853e2821 |
| `ed4_25.csv` | `fes_ed4_gas_demand_defs` | 74485527-6c09-4c1a-bf8c-8569810eb918 | `fes2025_ed4_v006.csv` | 2025-12-10T16:56:34.330431 | 2026-10-08T10:54:24.609913+00:00 | 2062 | 00d5b089 |
| `ed4_26.csv` | `fes_ed4_gas_demand_defs` | b5ae9932-2ce9-42f0-a820-15a13f0eedd0 | `10yo2026_ed4_v001.csv` | 2026-09-16T14:59:52.804239 | 2026-10-08T10:54:27.264484+00:00 | 2488 | 1b5845f9 |

Packages (every capture `url_type: upload`):
`fes-natural-gas-residential-and-non-domestic-i-c-heat-demand-summary-data-table-ed3` (id
`2fd7b713-9a85-45d8-87d8-cb3f01fd0e89`; the three ED3 / ED7 families),
`fes-natural-gas-demand-definitions-ed4` (`af30ab4c-eb46-487c-80bd-84a57d7d5f18`).

## What each slice carries

Record numbers are the body's data-record positions (header = 0).

- `ed3c_23` (records 1, 571, 581, 746, 906, 960): the first heat row, a `Savings from Behaviour and
  Insulation` row with a blank `Fuel`, a `number` row, an `uptake` row, a gas-summary row (`Annuals`,
  `GWh`) and the `Peak 1-in-20` row labelled `GWh` with a blank `2028` tail. No BOM.
- `ed3c_24` (BOM; records 1, 16, 83, 683, 1219, 1223): a heat row, a zero-valued `DH` row, a
  blank-`2050` row, a blank-`Fuel` `number` row and two `Peak 1in20` rows labelled `GWh/day` (the last
  with the blank `2030` / `2050` cells).
- `ed3g_25` (records 1, 8, 11, 50, 55): `Annuals` `GWh` rows (two descriptions end in a space), the
  `Peak 1in20` `GWh/day` row and two `Ten Year Forecast` rows with the blank `2036`-`2050` tail.
- `ed3g_26` (records 1, 3, 4, 11): `Ten Year Outlook` `Annuals` rows and the `Peak 1in20` row with
  the blank `2037`-`2050` tail.
- `ed7_25` (records 1, 243, 439, 838): a `TWh` row, a blank-`Fuel` `uptake` row, a blank-`Fuel` `stock`
  row and a `Ten Year Forecast` `GW` row with the blank `2036`-`2050` tail.
- `ed7_26` (records 1, 47, 85): a `TWh` row of zeros, a blank-`Fuel` `uptake` row and a `stock` row;
  the `2023`, `2024` and `2037`-`2050` columns are blank in every row, as in all 249 of the real body.
- `ed4_23` .. `ed4_26`: the whole bodies (10 / 10 / 11 / 11 items); `ed4_24` has the BOM, `ed4_25` the
  one embedded-newline definition (item 10, the peak 1-in-20 definition), `ed4_26` the 2026
  network-constraint / non-comparability electricity-generation definition (item 5).

Unit evidence (FACTS l.373-395, l.422-438, l.473-493): the vendor's row `Units` column carries the unit
verbatim, including the non-physical `number`, `uptake` and `stock`; the 2023 combined table labels the
peak daily gas-demand item `GWh`, the later tables `GWh/day`. No conversion, no unit column invented,
no global unit.

Three-family evidence (FACTS l.338-360): the 2023 / 2024 combined table holds 905 / 1,168 heat rows and
55 / 55 gas-summary rows; the 2025 / 2026 tables split them (the natural-gas summary 55 / 11 wide rows,
the heat summary 1,297 / 249) but not row for row (one changed description, renamed heat sectors,
technologies and timeframes, `number` -> `stock`), with zero identical identifier tuples between the
later gas and heat tables. The families are never added, unioned or summed across families or editions.

## Held questions (verbatim, as in the registry)

- ED3 combined (`fes_ed3_heat_demand_natural_gas`): TODO: For each heat and gas measure, define its year
  interval or stock/snapshot date and whether the integer labels a starting or ending year. Define the
  scale and denominator of `uptake`, the basis of `number`, and the timeframe labels including cumulative
  and peak outputs. Confirm the reported unit for the peak daily gas-demand item in the earlier table.
  What does each blank projection cell mean?
- ED3 gas summary (`fes_ed3_heat_demand_natural_gas_sum_ed`): TODO: What interval does each `Annuals`
  GWh value cover, and how does its integer year label identify that interval? What does a blank
  projection cell mean? Please confirm the edition-specific coverage of electricity-generation gas demand
  and the network-constraint treatment described in ED4.
- ED7 (`fes_ed3_heat_demand_sum_ed`): TODO: For every ED7 unit and data type, what interval or
  observation date does a year header denote? What are the scale and denominator of `uptake`, the
  counting basis and snapshot date of `stock`, and the start/end convention for peak-year labels? How are
  savings related to the reported technology demand? What does a blank projection cell mean?

## Non-holding TODOs

- Vendor-declared composite identity (ED3 combined, gas summary, ED7) is unresolved; the measured keys
  pass every body with zero duplicates (FACTS l.756-760).
- ED4's peak 1-in-20 definition establishes a gas year starting in October for that item only; it is
  not applied to `Annuals` or to any heat measure.

## Full-body figures

A fixture-free run of the generated transformers over the 10 real bodies, copied to a scratch data root
(`C:/gridflow-wt/_scratch/s2b1`, outside the repository): ED3 combined 2023 29,760 + 2024 37,913 =
67,673; ED3 gas summary 2025 1,540 + 2026 308 = 1,848; ED7 2025 36,316 + 2026 6,972 = 43,288; ED4
2023 10 + 2024 10 + 2025 11 + 2026 11 = 42; total 112,851 written, zero rows excluded, zero failures.
Blank projection cells written as null values: ED3 combined 9,288, gas summary 319, ED7 9,639. Zero
projection cells: 12,851 / 59 / 4,946.
