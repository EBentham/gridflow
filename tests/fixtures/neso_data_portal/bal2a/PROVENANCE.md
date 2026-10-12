# bal2a fixtures (v0.22-K-BAL-2a)

Each `*.csv` is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/bsuos_monthly_forecast_fc_summary/2026/10/08/` (swept
2026-10-08): the vendor header exactly, the first three data rows, and every wholly blank row of
the original. Bytes inside a kept line are untouched (BOM, `0xA3`, padding). git normalises line
endings, so the tests rebuild the originals' CRLF convention. Nothing is repaired or re-decoded.

| Fixture | Resource id | Vendor filename | SHA-256 prefix | Raw header epoch | Why |
|---|---|---|---|---|---|
| `f4.csv` | `98fea024-d154-4f4b-840e-5e84e5c649ff` | `bsuos-forecast-february-2022-csv.csv` | `5f63104e` | F4 | BOM, `Restoration`, trailing anonymous field, a wholly blank row, `%b-%y` months |
| `f5.csv` | `db352475-1dc7-462a-9f57-cfcf269fa5f2` | `bsuos-forecast-march-2022-csv.csv` | `17988755` | F5 | F4 without `Restoration`, a blank row |
| `f7.csv` | `aef8c119-d9e3-496a-8185-e1be35f6f71b` | `bsuos-forecast-june-2022.csv` | `1727d85a` | F7 | no BOM, `G` shape |
| `f8.csv` | `8ddd61ed-1003-479a-bc78-6dfe0f91da6b` | `bsuos-forecast-october-2022.csv` | `bb656637` | F8 | `Winter Contingency Cost (Central)` |
| `f10.csv` | `3ed31178-bd64-41ce-8665-85b77db7aaaf` | `bsuos-forecast-july-2023.csv` | `b8712273` | F10 | bare `Volume` (T127), `%d/%m/%Y` months |
| `f11.csv` | `60ee18d3-b999-4413-aac5-82a30d2a982c` | `bsuos-forecast-aug-2023.csv` | `8c474091` | F11 | `Estimated BSUoS Volume (TWh)`, `%d/%m/%Y` months |
| `f12.csv` | `963cf73a-138e-4521-986b-12079f45b78d` | `bsuos-forecast-feb-2024.csv` | `6aad5684` | F12 | `21/22 Under-Recovered Costs (£m)`, plain cells |
| `f12p.csv` | `9120ae56-f4b5-4512-baaa-e20f8b31a014` | `bsuos-forecast-mar-2024.csv` | `6fa32059` | F12 | March 2024: same header as `f12`; **padded numerals (` 4.0 `) and the ` -   ` token** |
| `f12q.csv` | `0bef1780-082d-46ec-90af-62b3d0586061` | `bsuos-forecast-apr-2024.csv` | `ffdaf329` | F12 | April 2024: one padded numeral ` 4.23 `, the ` -   ` token, a blank row |
| `f12r.csv` | `ebdd84ac-02cb-4552-a427-9b0dc56f5482` | `bsuos-forecast-may-2024.csv` | `78eccd25` | F12 | May 2024: the ` -   ` token only |
| `f13.csv` | `2d5f0be1-2aab-4ab7-af28-c67a2f38db6b` | `bsuos-forecast-august-2024.csv` | `6b685bfe` | F13 | final shape, unpadded headers, `Additional NESO Framework Costs (£m)` |
| `f14.csv` | `495879f8-4605-4ade-b4d1-7b1d37a163fa` | `bsuos-forecast-march-2025.csv` | `3bcad14f` | F14 | final shape, one leading and one trailing space on the last six headers, a blank row, `%d/%m/%Y` |
| `f15.csv` | `b458c4e5-ab80-43e5-907e-6c7069350390` | `bsuos-forecast-october-2026.csv` | `e402fce2` | F15 | the newest body; exact F15 padding |
| `bad.csv` | `ef004890-679a-4d28-ac00-427ff6e7260d` | `monthly-bsuos-forecast-summary-mar_20.csv` | `d99b04d9` | F0 | invalid UTF-8: standalone `0xA3` bytes in the currency headers |

Reader epochs: F13, F14 and F15 are one reader epoch (the reader strips header-edge whitespace).
The 53 valid-UTF-8 resources fall in 8 reader epochs; the 22 invalid ones (F0-F3, F6, F9) fail the
UTF-8 gate before epoch matching.

## Seat ruling 678 / BAL-2aH: the three March-May 2024 captures

FACTS T128/T129 say no nonblank numeric token fails a `float64` cast. Three F12 resources did:
March, April and May 2024 (`9120ae56`, `0bef1780`, `ebdd84ac`) carry the accounting-format token
` -   ` in `Winter Security of Supply Cost (£m)` and `21/22 Under-Recovered Costs (£m)` (105 cells,
beside `0` in the neighbouring February 2024 and January 2024 editions for the same target months),
and March and April 2024 also pad numerals (` 4.0 `). Under ruling 678 they first failed the strict
cast with `InvalidOperationError` (no ADR-040 cause, no adjudication entry). Since BAL-2aH (ADR-034
§Amendment 1) those two F12 columns declare `trim: "ascii"` and the null token `-`: a padded numeral
is edge-trimmed and the dash loads as null. Its meaning (zero or not-available) stays open in the
record's held question; a zero ruling re-transforms the three captures from bronze. Any other value
still fails the capture. `f12p.csv`, `f12q.csv` and `f12r.csv` pin the load. Real-body count: 75
captures = 22 adjudicated `UnicodeDecodeError` + 53 loaded, 1,272 rows.
