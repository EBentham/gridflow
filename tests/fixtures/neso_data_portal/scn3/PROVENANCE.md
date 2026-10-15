# K-SCN-3 fixtures

Cut from the 2026-10-08 swept bronze under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (read-only; a scratch script that splits
each body on its CRLF record terminators and keeps each record's physical bytes, not committed). Every
fixture is the body's header (BOM and CRLF kept) plus whole vendor records, byte for byte; no record is
edited. `.gitattributes` (`*.csv -text`) keeps the CRLF terminators and BOMs through git.

| Fixture | Family | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---:|---|
| `b2.csv` | `building_heat_model_example_output_half` | 27522340-008d-4c82-97c1-17eeb7ed1b6a | `example_profiles-output_data.csv` | 2026-07-30T15:36:52.510328 | 2026-10-08T09:14:01.292897+00:00 | 10034 | 8cc7d9aa |
| `b1.csv` | `building_heat_model_list_input` | b809e59a-e681-4ecb-af07-80997abbbd6f | `example_profiles-list_of_input_parameters.csv` | 2026-07-30T14:01:53.418825 | 2026-10-08T09:14:12.279856+00:00 | 9219 | 784fcc2b |
| `w1.csv` | `fes_ws1_gas_supply` | b04e227d-f329-4ccf-8e25-821ea6ea03ca | `fes2023_ws1_v001.csv` | 2023-07-10T03:30:11.101381 | 2026-10-08T10:58:02.247022+00:00 | 84644 | 9b408650 |
| `w2.csv` | `fes_ws1_gas_supply` | fa5697c5-9810-480f-b4c5-ab41dc53777a | `fes2024_ws1_v001.csv` | 2024-07-15T06:19:57.831578 | 2026-10-08T10:58:06.175269+00:00 | 115812 | 6dafbd6b |
| `w3.csv` | `fes_ws1_gas_supply` | ebf4f503-0a13-45e6-89ad-b707748613b6 | `fes2025_ws1_v006.csv` | 2025-12-10T16:57:40.652852 | 2026-10-08T10:58:08.633236+00:00 | 112615 | d2b55ccd |
| `w4.csv` | `fes_ws1_gas_supply` | 0dd30611-5b9f-452d-bd34-966737d14fca | `10yo2026_ws1_v001.csv` | 2026-09-16T14:49:53.687177 | 2026-10-08T10:58:11.248826+00:00 | 9840 | 01da6a23 |
| `e1.csv` | `fes_ws2_gas_emissions` | 5f8f7293-4894-40c5-8f5c-e096b19e8198 | `fes2023_ws2_v001.csv` | 2023-07-10T03:31:44.136572 | 2026-10-08T10:58:14.923059+00:00 | 17121 | f5f6607f |
| `e2.csv` | `fes_ws2_gas_emissions` | 31188b9d-620d-497b-80c0-83fa4fa70320 | `fes2024_ws2_v002.csv` | 2024-07-16T11:16:11.122722 | 2026-10-08T10:58:17.249461+00:00 | 28559 | a4440fa4 |
| `e3.csv` | `fes_ws2_gas_emissions` | a80a94cc-b349-412c-83a0-0f5f9eaf4e7e | `fes2025_ws2_v006.csv` | 2025-12-10T16:59:01.412328 | 2026-10-08T10:58:19.525633+00:00 | 26962 | 075074a4 |
| `e4.csv` | `fes_ws2_gas_emissions` | 3aba22fd-1934-4654-a33a-f3273c973560 | `10yo2026_ws2_v001.csv` | 2026-09-16T14:51:40.910171 | 2026-10-08T10:58:21.823686+00:00 | 3255 | 12322a99 |
| `d1.csv` | `fes_la_heat_model_domestic` | c3dd66ab-cb07-4e4d-bbab-d79074ea1713 | `fes22_la_level_outputs_2020_domestic_all_scenarios.csv` | 2022-08-31T07:33:03.509314 | 2026-10-08T10:55:39.546223+00:00 | 577488 | 97db8d4f |
| `d2.csv` | `fes_la_heat_model_domestic` | 52baf0e4-b02b-4f69-8cd5-ca52c97bf872 | `fes22_la_level_outputs_2025_domestic_all_scenarios.csv` | 2022-08-31T07:34:02.299496 | 2026-10-08T10:55:42.677271+00:00 | 800029 | 2a99da98 |
| `d3.csv` | `fes_la_heat_model_domestic` | f41f65e2-9f55-496b-afaa-76efcee4cda2 | `fes22_la_level_outputs_2030_domestic_all_scenarios.csv` | 2022-08-31T07:35:07.017785 | 2026-10-08T10:55:45.372469+00:00 | 831861 | 65939b24 |
| `r1.csv` | `fes_la_heat_model_residential` | 31d45b63-e70d-4cf1-a912-918bf3de58d5 | `fes24_la_level_outputs_2020_residential_all_scenarios.csv` | 2025-11-25T14:57:39.152279 | 2026-10-08T10:56:01.676213+00:00 | 207400 | 6d8031f6 |
| `r8.csv` | `fes_la_heat_model_residential` | e2215328-bd74-47c2-a22f-3878bb0fbbd5 | `fes25_la_level_outputs_2025_residential_all_scenarios.csv` | 2025-11-25T15:04:38.401027 | 2026-10-08T10:56:21.276371+00:00 | 223306 | e8d527b7 |
| `a1.csv` | `resource_adequacy_2030s_portfolio` | c2bba528-b75a-42c5-bb98-3af3a5b3f720 | `portfolio-capacity-dataset.csv` | 2025-12-11T13:47:53.306382 | 2026-10-08T11:17:08.685157+00:00 | 18166 | d2ffb943 |
| `a2.csv` | `resource_adequacy_2030s_peak_demand` | 5ca210f2-96cb-4fcd-939d-a9f34b2b8174 | `peak-demand-dataset.csv` | 2025-12-12T12:36:08.151108 | 2026-10-08T11:17:05.133341+00:00 | 2409 | 3e5a1793 |
| `a3.csv` | `resource_adequacy_2030s_results` | 71221dca-c792-4fb6-b6b4-893f392d893a | `resource-adequacy-in-the-2030s-results-dataset-.csv` | 2025-12-12T12:34:52.812310 | 2026-10-08T11:17:13.510244+00:00 | 112157 | ef3f6cb7 |

Packages (every capture `url_type: upload`): `building-heat-model` (id `14d99094-165e-4de1-9452-137c2835b102`; the two CSV families, the workbook and user guide stay DOC), `fes-whole-system-gas-supply-data-table-ws1` (`8cbe76c7-6971-40ed-8e0c-2d8c8520cd0c`), `fes-whole-system-gas-supply-emissions-data-table-ws2` (`cd6e42b0-13de-4f2a-bd0d-08e31bdde67f`), `local-authority-level-spatial-heat-model-outputs-fes` (`bfa95333-542e-4625-b5d0-17ced50a89ed`; the domestic and the residential families), `resource-adequacy-in-2030s` (`531e616c-7192-443f-bee0-ed067d55b182`; three families).

## What each slice carries

Record numbers are the body's data-record positions (header = 0); a CSV line number is the record number plus one.

- `b2` (records 1, 24, 48, 49, 50, 57, 96): both runs (`run_1` the gas combi boiler, `run_2` the air-to-water heat pump) of the average January day: periods 1, 24 and 48 of `run_1` (blank COP cells) and periods 1, 2, 9 and 48 of `run_2`, where gas and COP cells are blank or populated. Every archetype ends in a space. BOM.
- `b1` (records 1, 16, 17, 29, 166): the first parameter row, the repeated heat-loss label (CSV lines 17 and 18, two different values) and the repeated identical `DOMESTIC HOT WATER` heading (CSV lines 30 and 167, both runs blank). BOM. This is the family with no record (RULINGS 538); the fixture is run through the engine's own typing and duplicate guard under hypothetical keys.
- `w1` (records 1, 343, 378, 468, 498): WS1 2023 (BOM): a `TWh` row with a blank `2020` and blank `Feedstock`, a `TWh` row with a blank `Fuel`, a `GW` row (blank `Fuel` / `Feedstock`, `2020` = `0`), a `bcm/yr` row and a blank-`Sector` row.
- `w2` (records 76): WS1 2024 (BOM): the first row whose `2023`-`2049` cells are the literal `N/A` (CSV line 77). The resource is HOLD; the fixture proves the strict-cast failure.
- `w3` (records 79, 149, 492): WS1 2025: the Electric Engagement Bioenergy / Waste / `MSW, C&I (biogenic)` pair (CSV lines 80 and 150, 2050 `0.0` vs `13.465`) plus one Holistic Transition `GW` row. The pair fails the duplicate guard.
- `w4` (records 1, 19, 43): WS1 2026: a `TWh`, a `GW` and a `bcm/yr` row; `2037`-`2050` are blank in every row, as in all 52 of the real body.
- `e1` (records 1, 5, 11, 14): WS2 2023 (`Scenario` / `Sector`): the first row, a row with the blank `2029`-`2050` tail, a negative row and a zero row.
- `e2` (records 1, 5, 11, 14): WS2 2024 (BOM, `Pathway` / `Sector`): the same four kinds of row.
- `e3` (records 1, 5, 11, 14): WS2 2025 (`Pathway` / `Sector`, the same header as 2024): the same four kinds of row.
- `e4` (records 1, 3, 6): WS2 2026 (`Scenario` / `Category` / `Sector`): three rows, `Category` = `Emissions (MtCO2e)`, a negative row, `2037`-`2050` blank in every row.
- `d1` (records 1, 231, 371, 741): Domestic 2020 (header D0, no hydrogen or hybrid columns): the first row, a zero `stock Gas boiler`, an `LW` row with zero `stock Electric resistive` and an `FS` row. No Isles of Scilly rows (absent in 2020).
- `d2` (records 1, 166, 907, 1113): Domestic 2025 (header D5): the first row, the `E06000053` Isles of Scilly rows for `CT` and `LW` (the only two scenarios that have them) and an `ST` row.
- `d3` (records 1, 166, 372, 742): Domestic 2030 (header D30): the first row (blank hydrogen and hybrid-hydrogen cells), the Scilly `CT` row, a row with populated hydrogen cells and an `FS` row.
- `r1` (records 1, 166, 743, 1114): Residential 2024, projection year 2020 (header R24): the first row, the Scilly `Counterfactual` row, a `Hydrogen Evolution` row and a zero `stock Electric resistive` row. The biofuel, biomass, hybrid and hydrogen columns are all zero, as in all 1,484 rows of the real body.
- `r8` (records 1, 166, 372): Residential 2025, projection year 2025 (header R25): the first row, the Scilly `Electric Engagement` row and a `Falling Behind` row.
- `a1` (records 1, 7, 12, 220): Portfolio capacities: the first row, a row whose `Technology` ends in a space, an all-zero capacity / stored-energy row and a `2040/41` row with stored energy.
- `a2` (records 1, 2, 69): Peak demand: the first two rows and a `2040/41` row.
- `a3` (records 1, 16, 511, 560): Results: the first row, two `1999/2000` weather-year rows (one with a zero `LOLE`) and a row with a long simulation name.

## Unit evidence

Unit evidence (FACTS l.376-385, l.266-271, l.464, l.624, l.710-715, l.771-776): WS1 carries the unit verbatim per row in `Units` (`TWh`, `GW`, `bcm/yr`; no cell is blank), WS2 has no unit column (only the 2025 dictionary states `MTCO2e`, docs only), the B2 energy columns are kWh per the dictionary (docs only) and its COP columns declare no unit, the local-authority stock columns are numbers of buildings and the proportions vendor-scale proportions, and Resource Adequacy carries MW, MWh, h/year and MWh/year in the column names. No conversion, no unit column invented, no global unit.

## Held questions

Each held family's question, verbatim as committed in its package registry record (unit `E-SEM`):

`building_heat_model_example_output_half`:

```text
TODO: What unit or scale does NESO declare for `avg_COP_space_heat` and `avg_COP_dhw`? What does a blank gas, electricity or COP cell mean in the example profiles (blank is not zero)?
```

`fes_ws1_gas_supply`:

```text
TODO: For each WS1 edition and quantity class (`TWh`, `GW`, `bcm/yr` rows), what interval or assessment date does a year header denote, and is the integer a starting or ending label? What does a blank projection cell mean? What does the literal `N/A` in `fes2024_ws1_v001.csv` mean? Which omitted dimension distinguishes the four repeated Bioenergy / Waste / `MSW, C&I (biogenic)` rows per pathway in `fes2025_ws1_v006.csv`, and can NESO publish a corrected file?
```

`fes_ws2_gas_emissions`:

```text
TODO: What emissions unit do the 2023, 2024 and 2026 WS2 year columns carry (their dictionaries give `N/A` or no unit; only 2025 states `MTCO2e`)? For each edition, is a year label a calendar or financial year, and a starting or ending label? What does a blank projection cell mean?
```

`fes_la_heat_model_domestic`:

```text
TODO: What stock assessment date does `Year` denote in the FES 2022 domestic outputs, and is it a start- or end-of-year snapshot? Is `stock Oil boiler` a number of buildings or a proportion, as the dictionary states? What does a blank hydrogen or hybrid-hydrogen cell in the 2030-2050 slices mean?
```

`fes_la_heat_model_residential`:

```text
TODO: What exact stock assessment date does `Year` represent in the FES 2024 and FES 2025 residential outputs, and is it a start- or end-of-year convention?
```

## Resource HOLD (WS1 2024)

Resource `fa5697c5-9810-480f-b4c5-ab41dc53777a` (`fes2024_ws1_v001.csv`), unit `E-SEM`, no edition entry; reason verbatim:

```text
the 2024 body fails the strict float64 cast on 7128 projection cells, every one the literal `N/A` (264 per column 2023-2049), whose meaning NESO does not document; declaring it a null token would assert an undocumented semantic; re-disposition only after NESO defines `N/A` (then an exact `null_tokens` declaration) or uploads a corrected file (a new last_modified)
```

## Recorded failure (WS1 2025)

One ADR-040 `failed` entry (`DuplicateEntityKeyError`, ruling 839) on capture `bronze/neso_data_portal/fes_ws1_gas_supply/2026/10/08/raw_20261008T105808Z_ebf4f503-0a13-45e6-89ad-b707748613b6_d2b55ccd.csv`.

Reason, verbatim:

```text
the 2025 body repeats four fuel type / units / category / sector / fuel / feedstock / category lookup tuples (Bioenergy, TWh, Supply, Waste, Waste, `MSW, C&I (biogenic)`, blank) once per pathway with conflicting values (e.g. Electric Engagement on CSV lines 80 and 150: 2050 `0.0` vs `13.465`), so 248 unpivoted rows share 124 keys and the duplicate guard fails the capture; it is never deduplicated and stays unloaded with its failure record
```

Question, verbatim:

```text
Which omitted dimension distinguishes the repeated Bioenergy / Waste / MSW, C&I (biogenic) rows of FES 2025 WS1, and can NESO publish a corrected file?
```

## Non-holding TODOs (Resource Adequacy, eligible)

- EDITION-EVIDENCE: the results table (`resource-adequacy-in-the-2030s-results-dataset-.csv`) has no resource-specific edition statement, so it carries no `edition_by_filename` and no `edition` in its key; adding one later needs direct evidence and `edition` in the key (FACTS l.690, l.785). The portfolio and peak-demand editions (2025) come from their captured resource descriptions.
- VENDOR-UNIQUENESS: the entity keys of all eight families are measured unique over the real bodies, not vendor-guaranteed (FACTS l.53, l.952); whole-capture selection per `resource_id` asserts no keywise revision semantics.

## Families without a record

`building_heat_model_list_input` (269 rows) repeats an identical vendor row (`DOMESTIC HOT WATER`, CSV lines 30 and 167, both runs blank) that defeats every key including the full row (RULINGS 538): no record, no row ordinal, no forward-fill, no dedup. The resource stays SILVER (ingest-only), not DOC.
