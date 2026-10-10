# K-SCN-1d fixtures

Cut from the 2026-10-08 swept bronze under
`C:/gridflow-data/bronze/neso_data_portal/fes_es1_electricity_supply/2026/10/08/` (read-only, a scratch
script that is not committed). Each fixture is a slice of whole vendor rows, kept byte for byte:

- `e23`-`e26` (the four SILVER editions, one per header epoch): the header and about a dozen rows
  covering a blank `Type` and `SubType` (the 2023 CO2 rows), a blank `SubType` only, a negative value
  (exports, net flows, CO2), a zero, blank projection cells, a second scenario / pathway
  (`Five Year Forecast`, `Counterfactual`, `Ten Year Forecast`) and one row for each of the `(MW)`,
  `(GWh)`, `(TWh)` and CO2 variable labels. `e26` keeps the real body's all-blank `2024` column and
  `2037`-`2050` columns.
- `e20` (held): the header and six rows with thousands-separated numbers (`4,750.00`) and literal
  `N/A` cells (the `Five Year Forecast` row ends in 25 of them).
- `e21`, `e22` (held): the nine-line title and notes preamble, the table header on physical line 10 and
  three whitespace-padded data rows.

A BOM stays where the original had one (2020-2024). `git` normalises line endings, so
`tests/unit/test_neso_scn1d_records.py::body` restores the bronze CRLF.

Package `future-energy-scenario-electricity-supply-data-table-es1`, package id
`549b0667-b533-4748-95bd-f6e13933a47d`; `url_type` is `upload` for every capture.

| Fixture | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---:|---|
| `e20.csv` | 40f40b39-5eba-4479-94b1-328ea9b8eefe | `fes_es1.csv` | 2020-12-03T11:58:42.191637 | 2026-10-08T10:54:55.231873+00:00 | 142553 | c671c92b |
| `e21.csv` | bca9679e-9860-4efd-9145-220d7dc4b912 | `fes2021_es1.csv` | 2021-07-12T10:24:36.047429 | 2026-10-08T10:54:57.796823+00:00 | 148082 | ee47ec88 |
| `e22.csv` | 90c4a0e8-22fd-4bda-b5bd-14a540893a98 | `fes2022_es1_v001.csv` | 2022-07-17T23:22:23.498433 | 2026-10-08T10:55:00.561891+00:00 | 182476 | fee0604a |
| `e23.csv` | 86812136-3f52-43e5-8f7c-7e4f6d5f95fc | `fes2023_es1_v002.csv` | 2023-08-24T10:35:22.143299 | 2026-10-08T10:55:02.875326+00:00 | 163928 | 8c7c01d2 |
| `e24.csv` | 8c8a436d-408a-441b-8c7a-84249805772c | `fes2024_es1_v002.csv` | 2024-08-02T11:01:29.547730 | 2026-10-08T10:55:05.578005+00:00 | 171283 | 6dfbf220 |
| `e25.csv` | 6c78a777-b885-4bb6-bc35-8100f9e137a2 | `fes2025_es1_v006.csv` | 2025-12-10T16:48:54.482473 | 2026-10-08T10:55:08.139618+00:00 | 163030 | 7b795744 |
| `e26.csv` | b3bf8ac0-d27a-447c-975c-208ae92c0fa5 | `10yo2026_es1_v001.csv` | 2026-09-16T14:27:45.949534 | 2026-10-08T10:55:10.553294+00:00 | 22912 | a88fd382 |

Full-body figures behind the record (a fixture-free run of the generated transformer over the seven
real bodies in a scratch copy): 2023 17052, 2024 17192, 2025 16740 and 2026 3375 long rows (54359
written, zero rows excluded, zero duplicate keys); the 2020-2022 captures have no completion and no
failure, and reconcile reports no gap.
