# Provenance: trading batch fixtures (v0.22-K-TRD)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line (a BOM stays); only the line
selection is ours. The three GTMA upload bodies (`gu5`, `gu6`, `gp`) carry a BOM and CRLF; the ten
datastore bodies have neither a BOM nor CRLF. `git` normalises the committed line endings, so the
tests rebuild CRLF for the uploads only. The "sha8" is the first eight hex digits of the original
body's SHA-256, the suffix of the bronze file name. "Lines" are 1-based line numbers of the original
body (1 is the header). Nothing is repaired or re-decoded.

| Fixture | Family | Resource id | Vendor file / resource name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---:|---|
| `ba1.csv` | `bs_contract_enactment_balancing` | `84837a48-8f5d-40d8-bc9f-53dd5e519447` | Balancing Contract Enactment FY 21-22 | `71c1c12f` | 72 | 2-4 (all zero volume / price / cost; one blank `Reason`) |
| `ba2.csv` | `bs_contract_enactment_balancing` | `ed753a4b-1b8c-4b07-9997-b5c0e09da041` | Balancing Contract Enactment FY 22-23 | `53eb3e34` | 3 | 2-4 (one zero row, two negative volumes at price 124) |
| `bl.csv` | `bs_contract_enactment_balancing_service` | `f75a1dcb-de6e-4c20-8407-e37d5e67cf79` | Balancing Service Contract Enactment 21-22 (Pre June 2021) | `d89097a1` | 79 | 2-3, 8 (`2021_04_07-1`, end `2020-04-08T05:00:00` before start `2021-04-07`), 69-71 (negative volumes, one price 150) |
| `gu5.csv` | `gtma_trades_data` | `6a03c415-475f-4005-bf55-b9108291c511` | `gtma_trades_2019-20.csv` | `9845ed60` | 32,973 | 2, 305 (zero price), 751 (cost `-1`), 2824 (price `-1`), 9400 (volume `-1`), 29192 (end before start, `ES00049438`), 32481 (equal window, `ES00052927`) |
| `gu6.csv` | `gtma_trades_data` | `8e14ae8e-2091-4abc-a2e6-a4b82e5519ab` | `gtma_trades_2020-21.csv` | `4bd1f961` | 55,542 | 50105, 50106, 50116 (`ES00104405`, then `ES00104406` and its identical repeat) |
| `gp.csv` | `gtma_trades_data_pre` | `8cf964b6-b94c-4d35-88f4-312834aa6e8c` | `gtma_trades_2021-22.csv` | `1e3abed0` | 7,470 | 2, 443 (zero price), 569 and 575 (price `-1`) |
| `gf1.csv` | `gtma_trades_fy` | `11a2f532-a7ce-46f8-9d82-88b3289daa82` | Historic GTMA Trades FY 21-22 | `0bfba80a` | 45,799 | 2, 45792-45793 (`ES00165376` and `ES00165377`, shared with GF2) |
| `gf2.csv` | `gtma_trades_fy` | `83b1d432-f8b4-4393-a8fd-bcaeb6bd7620` | Historic GTMA Trades FY 22-23 | `cf692e7b` | 65,409 | 2-3 (`ES00165376`, `ES00165377`, identical to GF1's on every column), 11 (`ES00165386`) |
| `il.csv` | `index_linked_contract_volume` | `8f694d01-68a5-41b9-9542-dd27c42fdbbf` | Index Linked Contract Enactment FY 21-22 | `5d65334d` | 12 | 2-4 |
| `ic.csv` | `ic_requirement_auction` | `6a928369-bed3-445f-af8a-69cdb2cc5089` | Interconnector Requirements and Auction Summary | `e87ac116` | 28,328 | 2, 2272 (blank prices and volumes), 26827 (literal `null` System Flag, zero clearing price), 28054 (end before start), 28328 (future-start lot `2026_10_08-11_43-1`, delivery 13:00 UTC after the 11:08:50 capture) |
| `se_legacy.csv` | `ssel_contract_enactment` | `835d50b9-2d7c-4318-bd04-c7f94b149041` | Super Stable Export Limit Contract Enactment 20-21 | `ab93d260` | 20 | 2-4 (legacy `Record,Unit,Start,End,Reduction (MW),Price (£/MW/hr)` header) |
| `se_modern.csv` | `ssel_contract_enactment` | `34f185bc-087a-41b2-b514-305e31158e5a` | Super Stable Export Limit Contract Enactment FY 22-23 | `3e4e6917` | 2 | 2-3 (the whole body: modern 13-column header) |
| `ut.csv` | `upcoming_trades` | `0b7e84ec-eded-4458-b111-d29ba4508e85` | Upcoming Trades | `7441677b` | 28 | 2-3, 10-11 (`ES00380892` appears for periods 29 and 30) |

The sidecar facts the tests write (package slug and id, resource name, vendor file name,
`ckan_last_modified`, the `written_at` capture time) are the originals', in
`test_neso_trd_records.py::CAPTURES` and `PACKAGES`. `ut` is written at
`2026-10-08T11:40:28.001597+00:00` (the sidecar, not the filename's `11:40:27`).

The row totals the records must reproduce (K-TRD-FACTS section 2; the bodies were re-run through
the real generic engine in a scratch data root, 0 validation failures, 0 blank rows excluded):
83 + 79 + 51,230 (GU1-GU5, GU6's 55,542 is held) + 7,470 + 237,735 + 16 + 28,328 + 27 + 28 =
**324,996** silver rows.

Quirk kept as published: the auction `Notes` cell is written quoted-empty (`""`); the CSV reader keeps
a quoted empty as an empty string, while an unquoted blank string cell elsewhere is null.
