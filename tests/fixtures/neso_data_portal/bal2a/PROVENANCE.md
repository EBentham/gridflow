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
| `f12p.csv` | `9120ae56-f4b5-4512-baaa-e20f8b31a014` | `bsuos-forecast-mar-2024.csv` | `6fa32059` | F12 | same header as `f12`; **padded numerals (` 4.0 `) and the ` -   ` token** |
| `f13.csv` | `2d5f0be1-2aab-4ab7-af28-c67a2f38db6b` | `bsuos-forecast-august-2024.csv` | `6b685bfe` | F13 | final shape, unpadded headers, `Additional NESO Framework Costs (£m)` |
| `f14.csv` | `495879f8-4605-4ade-b4d1-7b1d37a163fa` | `bsuos-forecast-march-2025.csv` | `3bcad14f` | F14 | final shape, one leading and one trailing space on the last six headers, a blank row, `%d/%m/%Y` |
| `f15.csv` | `b458c4e5-ab80-43e5-907e-6c7069350390` | `bsuos-forecast-october-2026.csv` | `e402fce2` | F15 | the newest body; exact F15 padding |
| `bad.csv` | `ef004890-679a-4d28-ac00-427ff6e7260d` | `monthly-bsuos-forecast-summary-mar_20.csv` | `d99b04d9` | F0 | invalid UTF-8: standalone `0xA3` bytes in the currency headers |

Reader epochs: F13, F14 and F15 are one reader epoch (the reader strips header-edge whitespace).
The 53 valid-UTF-8 resources fall in 8 reader epochs; the 22 invalid ones (F0-F3, F6, F9) fail the
UTF-8 gate before epoch matching.

## Seat ruling 678: the three strict-cast failures stay open

FACTS T128/T129 say no nonblank numeric token fails a `float64` cast. Three F12 resources do:
March, April and May 2024 (`9120ae56`, `0bef1780`, `ebdd84ac`) carry the accounting-format token
` -   ` in `Winter Security of Supply Cost (£m)` and `21/22 Under-Recovered Costs (£m)` (105 cells,
beside `0` in the neighbouring February 2024 and January 2024 editions for the same target months),
and March and April 2024 also pad numerals (` 4.0 `, 27 cells). The engine's cast is strict, has no
trim and a null token would turn a probable zero into null, so those three captures fail with
`InvalidOperationError` and load nothing (D-41). The fixture `f12p.csv` pins that. Ruling 678: no adjudication entry
(`InvalidOperationError` is not an ADR-040 cause) and no new mechanism; the three stay open `failed`
gaps until the follow-up unit BAL-2aH adds lexical numeric cleanup. Real-body count: 75 captures = 22
adjudicated `UnicodeDecodeError` + 3 open `InvalidOperationError` + 50 loaded.
