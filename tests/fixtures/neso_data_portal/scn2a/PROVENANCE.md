# K-SCN-2a fixtures

Cut from the 2026-10-08 swept bronze under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (read-only; a scratch script that streams
`csv.reader` records and keeps each record's physical bytes, not committed). Every fixture is the body's
header (BOM and CRLF kept) plus whole vendor records, byte for byte. `.gitattributes` (`*.csv -text`)
keeps the CRLF terminators and BOMs through git.

| Fixture | Family | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---:|---:|---|
| `es2_23.csv` | `fes_es2_european_supply` | 6563801b-6da4-46e7-b147-3d81c0237779 | `fes2023_es2_v001.csv` | 2023-07-10T03:32:05.482290 | 2026-10-08T10:55:14.398706+00:00 | 180217 | 6313b18a |
| `es2_24.csv` | `fes_es2_european_supply` | 7902b197-ada0-47ae-bd2e-db566a8d0fab | `fes2024_es2_v001.csv` | 2024-07-15T06:30:08.953371 | 2026-10-08T10:55:17.318725+00:00 | 212993 | 3ae4e6be |
| `es2_25.csv` | `fes_es2_european_supply` | fbfdc6c6-52b6-4e3a-96b9-f4e7941f61ec | `fes2025_es2_v006.csv` | 2025-12-10T16:55:04.054020 | 2026-10-08T10:55:20.326635+00:00 | 192774 | 3f5a30e5 |
| `es2_26.csv` | `fes_es2_european_supply` | e8ed65c6-6785-4e2b-84a5-696057f103c9 | `10yo2026_es2_v001.csv` | 2026-09-16T14:33:44.536460 | 2026-10-08T10:55:23.249005+00:00 | 2179 | 2b1f3ebb |
| `flx1_23.csv` | `fes_flx1_flexibility` | e86745fd-1ef2-4112-a14c-84682a387b68 | `fes2023_flx1_v001.csv` | 2023-07-10T03:27:12.556782 | 2026-10-08T10:55:27.105892+00:00 | 56451 | 06273710 |
| `flx1_24.csv` | `fes_flx1_flexibility` | c7d7a19d-255f-4585-b53b-487891aeda54 | `fes2024_flx1_v003.csv` | 2025-06-27T09:22:49.054408 | 2026-10-08T10:55:29.882933+00:00 | 67498 | f784da9c |
| `flx1_25.csv` | `fes_flx1_flexibility` | 299bc6c8-7608-4946-ab0a-5bc22129c897 | `fes2025_flx1_v006.csv` | 2025-12-10T17:00:25.208610 | 2026-10-08T10:55:33.057523+00:00 | 58939 | a5049beb |
| `flx1_26.csv` | `fes_flx1_flexibility` | 0ff1a94f-224d-4863-8137-7d9a0d2cd7f8 | `10yo2026_flx1_v001.csv` | 2026-09-16T14:35:58.106567 | 2026-10-08T10:55:35.461289+00:00 | 9396 | c2418403 |
| `ed5_23.csv` | `fes_ed5_road_transport` | 076c5c72-ecea-47c9-81b6-e93fe824d5d4 | `fes2023_ed5_v002.csv` | 2025-07-01T17:03:39.781640 | 2026-10-08T10:54:30.701995+00:00 | 95142 | 846de247 |
| `ed5_24.csv` | `fes_ed5_road_transport` | 7189f5d3-aed3-41b7-962f-fdb9fe11cbb7 | `fes2024_ed5_v003.csv` | 2025-07-01T17:04:57.258965 | 2026-10-08T10:54:34.085427+00:00 | 89997 | 3de73781 |
| `ed5_25.csv` | `fes_ed5_road_transport` | ce76da34-6eb0-43a1-8e93-3cc833d6d1b4 | `fes2025_ed5_v006.csv` | 2025-12-10T17:01:17.874457 | 2026-10-08T10:54:36.640837+00:00 | 90217 | b59c8415 |
| `ed5_26.csv` | `fes_ed5_road_transport` | 1b59c48f-f51a-4e32-a9e7-baca0b78a042 | `10yo2026_ed5_v001.csv` | 2026-09-16T14:30:16.337617 | 2026-10-08T10:54:39.292172+00:00 | 13578 | 7b086859 |
| `ed6_23.csv` | `fes_ed6_road_transport_notes` | 30012023-6c0b-4da5-bd77-4f2b244a950d | `fes2023_ed6_v001.csv` | 2023-07-10T03:26:39.716174 | 2026-10-08T10:54:43.214013+00:00 | 8076 | 16de6ee5 |
| `ed6_24.csv` | `fes_ed6_road_transport_notes` | 7fe93ddc-7a88-478b-8f60-4531c15bd538 | `fes2024_ed6_v001.csv` | 2024-07-15T06:41:16.480938 | 2026-10-08T10:54:46.048424+00:00 | 7984 | ea44b94e |
| `ed6_25.csv` | `fes_ed6_road_transport_notes` | 43add83b-bf94-493b-8f5d-de7fdbefc5bb | `fes2025_ed6_v006.csv` | 2025-12-10T17:02:44.975794 | 2026-10-08T10:54:48.733314+00:00 | 8146 | b2008380 |
| `ed6_26.csv` | `fes_ed6_road_transport_notes` | 06601add-c47b-427c-bdde-845b270e72e1 | `10yo2026_ed6_v001.csv` | 2026-09-16T15:01:14.222912 | 2026-10-08T10:54:51.332378+00:00 | 8165 | 517aea06 |

Packages (every capture `url_type: upload`):
`fes-european-electricity-supply-data-table-es2` (id `bd83ce0b-7b1e-4ff2-89e8-12d524c34d99`),
`fes-flexibility-data-table-data-table-flx1` (`2e3275e2-dd6a-4c2e-8cfb-eeb9b4320dcb`),
`fes-road-transport-summary-data-table-ed5` (`28303fe9-62d4-46c5-ad15-a1d0e59f98cb`),
`fes-road-transport-notes-data-table-ed6` (`5a3b9d32-5da8-4237-8468-dfb7bb4ebe78`).

## What each slice carries

- `es2_23`: a `Demand (TWh)` row with blank `SubType` / `SubSubType`, an `Ireland (IAI)` row, a
  comma-bearing `FES Scenario Alignment` cell and a row with blank year cells (no BOM).
- `es2_24` (BOM): an `SEM` row, the blank `2023` cell row and zeros. `es2_25`: a row with a blank
  `2036`-`2050` tail, a comma-bearing `FES Pathway Alignment` cell and zeros.
- `es2_26` (HOLD): the notes body's header (`Notes`, `Description`) and its first two records.
- `flx1_24` (BOM): the blank `2021` row, GW, GWh and TWh rows, negatives and a zero. `flx1_25`: GW,
  GWh and TWh rows with blank tails. `flx1_26`: `Ten Year Outlook` rows, a blank `2024` cell and the
  blank `2037`-`2050` tail.
- `flx1_23` (HOLD): two interconnector rows whose every projection cell carries trailing
  ASCII spaces (`6.06 `).
- `ed5_23` .. `ed5_26`: one GW peak row, one `# Cars`, one `# Vans` and one `TWh` row per epoch
  (`Scenario` in 2023 / 2026, `Pathway` in 2024 / 2025); the `# Cars` row keeps the body's blank `2050`
  tail.
- `ed6_23`: four populated records followed by three all-blank separator records (the real body has
  nine, trailing, after 27 populated rows); `ed6_24` (BOM), `ed6_25`, `ed6_26`: three records each,
  including a description with an embedded newline and one with commas.

Unit evidence (FACTS g4): ES2 `Variable` carries the unit (`Capacity (MW)`, `Demand (TWh)`); FLX1
`Unit` is GW / GWh / TWh per row (interconnector rows GW or TWh by `Detail`); ED5 `Unit` is GW, TWh,
`# Cars`, `# Vans`, `# Motorbikes`, `# Buses`, `# HGVs`, `# Vehicles` per row. No conversion, no unit
column invented, no global unit.

## Resource HOLDs (E-SEM)

- ES2 2026 (`e8ed65c6-6785-4e2b-84a5-696057f103c9`, `10yo2026_es2_v001.csv`): the 2026 body is an
  11-row `Notes`, `Description` notes table, not the European electricity supply table: it has no
  projection-year columns and no values, so it cannot be an epoch of the unpivot record (V-18);
  re-disposition only after NESO confirms the resource's intent (DOC or a reference family) or
  uploads a quantitative ES2 2026 table (a new last_modified). No edition entry.
- FLX1 2023 (`e86745fd-1ef2-4112-a14c-84682a387b68`, `fes2023_flx1_v001.csv`): the 2023 body fails
  the strict float64 cast on 1152 projection cells, every one a number with trailing ASCII spaces
  (e.g. `6.06 `; 45 per column 2021-2028, 36 per column 2029-2050) and an unpivot value has no trim
  declaration (`ValueSpec` has no `trim`); re-disposition only after a declared unpivot-value trim
  mechanism or a corrected NESO upload (a new last_modified). No edition entry; the ES1 2021 / 2022
  precedent (RULINGS 608). Not an ADR-040 entry.

## Held questions (verbatim, as in the registry)

- ES2: TODO: For each quantitative ES2 edition, what interval does `Demand (TWh)` cover, and what
  observation date does `Capacity (MW)` represent? Does a year label denote the starting or ending
  year? What does a blank projection cell mean? Is `10yo2026_es2_v001.csv` intentionally a notes
  resource, or was the European electricity supply table replaced by an incorrect upload? Please
  identify or republish the quantitative European supply resource for this edition.
- FLX1: TODO: For each FLX1 edition and measure, what interval or observation date does its year
  label denote, including annual interconnector energy, storage energy potential, installed
  capacity and flexibility at peak? Does a winter-related label identify the starting or ending
  year? What does a blank projection cell mean? Can NESO republish `fes2023_flx1_v001.csv` without
  numeric edge whitespace, or will a separately authorized engine change permit explicitly declared
  ASCII trimming of unpivot values?
- ED5: TODO: For each ED5 annual-energy, vehicle-count and capacity item, what interval or
  observation date does the year label denote? For winter-peak items, does the integer label
  identify the starting or ending winter year? What does a blank projection cell mean?

## Non-holding TODOs

- GEOGRAPHY-MAPPING (ES2): `Country` strings are NESO's (`Ireland (IAI)` in 2023, `SEM` from 2024),
  no ISO code; obtain a NESO definition of each and any prescribed external codes. Kept verbatim.
- VENDOR-UNIQUENESS (ED6): explicit vendor uniqueness of (`Column`, `Category`) is not stated; the
  key is measured (zero duplicates in every body after the reader drops the all-blank separators). A
  future repeated populated key fails the guard.
- Vendor-declared composite identity (ES2, FLX1, ED5) is unresolved; the measured keys pass every
  SILVER body with zero duplicates.

## Full-body figures

A fixture-free run of the generated transformers over the 16 real bodies, copied to a scratch data root
(`C:/gridflow-wt/_scratch/e2`, outside the repository): ES2 2023 22,997 + 2024 24,640 + 2025 20,331 =
67,968; FLX1 2024 5,550 + 2025 4,860 + 2026 972 = 11,382; ED5 2023 10,710 + 2024 9,765 + 2025 8,820 +
2026 1,764 = 31,059; ED6 2023 27 + 2024 27 + 2025 26 + 2026 26 = 106; total 110,515 written, zero
duplicate keys, zero failures, reconcile with zero gaps (the ES2 2026 and FLX1 2023 captures have no
completion and no failure). Blank projection cells written as null values: ES2 2,608, FLX1 1,848,
ED5 4,446.
