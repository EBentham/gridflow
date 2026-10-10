# Provenance: balancing-costs batch fixtures (v0.22-K-BAL-1)

Every fixture is a cut of a captured NESO Data Portal body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/`, read-only. A cut keeps the original
header and the original bytes of each kept line (a leading BOM stays); only the line selection is
ours. `git` normalises the committed line endings (the originals are CRLF throughout, with no
embedded line feeds), so the tests rebuild CRLF with `body()`. The "sha8" is the first eight hex
digits of the original body's SHA-256, the suffix of the bronze file name. "Lines" are 1-based line
numbers of the original body (1 is the header).

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---:|---|
| `ii.csv` | `current_bsuos_ii` | `0eda5e28-1dc6-48da-8663-c00e12f2a1e2` | `current_ii_bsuos_data.csv` | `9423ae5f` | 8,736 | 1-97 (2026-04-01 and 2026-04-02) |
| `sf.csv` | `current_bsuos_sf` | `f0060fd0-1fc9-4288-a0b3-4af9b592b0cf` | `current_sf_bsuos_data.csv` | `e998c68b` | 8,016 | 1-97 |
| `rf.csv` | `current_bsuos_rf` | `26b0f410-27d4-448a-9437-45277818b838` | `current_rf_bsuos_data.csv` | `c7c54b3c` | 6,672 | 1-97 |
| `hi1.csv` | `current_bsuos_historic_ii` | `3372646d-419f-4599-97a9-6bb4e7e32862` | `2017-2023-ii.csv` | `70d3733d` | 105,167 | 1-49, 66436-66482 (every row of 2021-01-14: 47 periods, SP48 absent from the body) |
| `hi2.csv` | `current_bsuos_historic_ii` | `d151c80a-f6a8-4b79-9387-1a68cd445af5` | `2023-2024-ii.csv` | `5c71a538` | 18,384 | 1-49, 11572-11619 (every row of 2023-11-28, a leading-space date) |
| `hi3.csv` | `current_bsuos_historic_ii` | `ea47c8e4-caa3-49c7-a442-e9644d330f63` | `current_ii_bsuos_data.csv` | `fb7560ad` | 624 | whole body (named 2024-2025, dated 2025-04-01 to 2025-04-13) |
| `hi4.csv` | `current_bsuos_historic_ii` | `45e87b67-ed5d-49ea-a149-2b37f467e542` | `2025-2026-ii.csv` | `f9a1b348` | 17,520 | 1-673 (2025-04-01 to 2025-04-14; the first 624 rows are HI3's keys with equal values) |
| `hs1.csv` | `current_bsuos_historic_sf` | `241b40c3-1f20-4607-b329-0466d215871d` | `2017-2023-sf.csv` | `428bf216` | 105,168 | 1-49 |
| `hs4.csv` | `current_bsuos_historic_sf` | `927aac83-c218-476c-9c20-29cf20cee448` | `2025-2026-sf.csv` | `1fd8f5fe` | 17,520 | 1-49, 5426-5473 (every row of 2025-07-23, a leading-space date) |
| `hr1.csv` | `current_bsuos_historic_rf` | `2e8b2ea6-cdb5-4936-8636-4ab5a3f7e350` | `2016-2023-rf.csv` | `efec77b5` | 122,690 | 1-49, 104932-104979 (every row of 2022-03-27: 46 valid periods and the two invalid pairs SP47 and SP48), 114580-114627 (every row of 2022-10-14, a leading-space date) |
| `hr2.csv` | `current_bsuos_historic_rf` | `47642de9-0738-47df-b230-94a097b61ae7` | `2023-2024-rf.csv` | `4e7203cf` | 17,568 | 1-49, 5810-5857 (every row of 2023-07-31, a trailing-space date) |
| `cb1.csv` | `constraint_breakdown` | `3651e9e3-52a8-46b6-a675-3a4c2aedc813` | `constraint-breakdown-2017-2018.csv` | `89199455` | 365 | 1-6, 263 (2017-12-18, thermal volume `-1`) |
| `cb9.csv` | `constraint_breakdown` | `6afe1c2b-6d70-4e76-8e74-0952b0a2beab` | `constraint-breakdown-2025-2026.csv` | `5e8f8062` | 364 | 1-11 (2025-04-01 to 2025-04-09; 2025-04-08 is absent from the body) |
| `t.csv` | `bsuos_fixed_tariffs` | `4dfa533f-bec6-491b-a3f1-7ce92449bc9a` | `bsuos-fixed-tariffs-data-portal-new-header.csv` | `1dbeb874` | 17 | whole body |
| `in.csv` | `inertia_bid_offer_costs` | `8da765a1-004f-46a5-8b3f-0e5b1787fcb1` | `inertia_costs_methods.csv` | `abe236d5` | 1,096 | 1-21 (17 of the 20 rows have all three methods zero; 2019-04-14 has a zero method A beside non-zero B and C) |

The sidecar facts the tests write (package slug and id, resource name, `ckan_last_modified`, the
`written_at` capture time) are the originals', in `test_neso_bal1_records.py::CAPTURES`.

## Open TODOs the records carry (the record model has no notes field)

- BSUOS-AGGREGATION (`current_bsuos_ii`, `_sf`, `_rf`, the historic families): the dictionary calls
  total recovery and actual cost whole-day quantities but both vary within every observed day; the
  values are kept as published, never summed or de-duplicated.
- BSUOS-FUND-BLANK (the historic families): a blank `BSUoS Fund Tariff` is null (missing or not
  applicable is undocumented), the recorded zero `BSUoS Fund Recovery` stays numeric zero.
- BSUOS-HI3-LABEL (`current_bsuos_historic_ii`): HI3 is named 2024-2025 but holds 2025-04-01 to
  2025-04-13, all of HI4's keys with equal values; both resources are served (ADR-039, ADR-040
  overlap, ruling 653); NESO is to confirm or correct the label.
- CB-WINDOW, CB-RETAG (`constraint_breakdown`): the exact daily accounting window; NESO permits
  post-event retagging, so the vintage is the capture's.
- Tariffs and inertia are held (E-SEM): see the questions in the records.
- `daily_balancing_costs` and `daily_balancing_volume` have no record (no run identifier in the
  body; RULINGS 538, 653).
