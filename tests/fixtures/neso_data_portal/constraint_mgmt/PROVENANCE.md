# K-CON fixtures

The directory is `constraint_mgmt`, not `con`: `CON` is a reserved device name on Windows, where git
cannot open any file under a directory of that name.

Cut from the 2026-10-08 swept bronze under `C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/`
(read-only) by a scratch script that is not committed. Each CSV fixture is the vendor header plus whole
vendor rows (physical lines kept byte for byte, CRLF; a BOM stays where the original had one) chosen
for the oddity a test names; `git` normalises their line endings, so
`tests/unit/test_neso_con_records.py::body` restores the bronze CRLF.

| Fixture | Family | Package id | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---|---:|---|
| `l.csv` | `constraint_limits_24m` | d515b4a9-60a1-489c-a126-004efc04f121 | 3c359e33-3dac-4bdd-87d1-efbf4cbc2f07 | `24-months-ahead-constraint-limit_sept26.csv` | 2026-09-10T09:48:47.500911 | 2026-10-08T09:15:45.347537+00:00 | 7406 | 3eef5064db597f3e |
| `m1.csv` | `cmis_intertrip` | 7c20761d-3aab-4e9f-926e-6117fa8c4524 | 60b4055c-d87e-4ebe-8d21-e023f506e461 | `cmp-management-intertrip-arming-2022-2023.csv` | 2024-03-12T14:16:03.129176 | 2026-10-08T09:14:57.004056+00:00 | 33675 | bf43e3695f987447 |
| `m3.csv` | `cmis_intertrip` | 7c20761d-3aab-4e9f-926e-6117fa8c4524 | 2f0777a3-719c-4ffe-96c2-61117a5ec468 | `cmp-intertrip-arming-2024-2025.csv` | 2025-05-06T10:00:06.772385 | 2026-10-08T09:15:02.989951+00:00 | 18916 | c10165870796a0a1 |
| `m4.csv` | `cmis_intertrip` | 7c20761d-3aab-4e9f-926e-6117fa8c4524 | c6f0c279-87c9-4123-a4d1-b2d6d98b43a7 | `cmp-intertrip-arming-2025-2026.csv` | 2026-04-28T16:25:43.722522 | 2026-10-08T09:15:05.121807+00:00 | 12900 | c156410df256eb76 |
| `d_clean.csv`, `d_collide.csv` | `da_constraint_flows_limits` | cf3cbc92-2d5d-4c2b-bd29-e11a21070b26 | 38a18ec1-9e40-465d-93fb-301e80fd1352 | `day-ahead-constraints-limits-and-flow-output-v1.5.csv` | 2026-10-07T17:28:41.804546 | 2026-10-08T09:16:59.251115+00:00 | 28054626 | 437e9f613ba42a1a |
| `o.csv` | `otf_network_congestion` | a30dacc7-af6e-465b-ad96-eb2383376ac9 | aa9d4303-b7ec-4881-be07-16bad8824ab6 | `otf_constraint_data_07-10-2026.csv` | 2026-10-07T10:43:02.910511 | 2026-10-08T11:16:09.107077+00:00 | 5706 | dae268465de48b3a |
| `t1.csv` | `thermal_constraint_costs` | f0055054-c55c-4068-a01c-61da4334e58f | 4357dd3b-5c7a-4caa-8d1a-8cf848521143 | `outturn-system-costs-2021-2022.csv` | 2022-04-08T15:22:06.203234 | 2026-10-08T11:38:31.219010+00:00 | 48560 | e23b439ea1f709fb |
| `t2.csv` | `thermal_constraint_costs` | f0055054-c55c-4068-a01c-61da4334e58f | 476b8d39-5eda-425c-9756-73ddfd36dc4d | `outturn-system-costs-2022-2023.csv` | 2023-04-17T16:03:55.101783 | 2026-10-08T11:38:34.163169+00:00 | 47648 | f645929174112827 |
| `x1.xlsx` | `thermal_constraint_costs_files` (sheet `Data`) | f0055054-c55c-4068-a01c-61da4334e58f | d195f1d8-7d9e-46f1-96a6-4251e75e9bd0 | `map-of-outturn-system-costs-19-20.xlsx` | 2020-06-12T14:41:30.987252 | 2026-10-08T11:38:49.949763+00:00 | 1383884 | ccd3dcaaa73c405b |
| `v.csv` | `voltage_requirement` | 9f4acccb-bf79-452b-aa77-a680ae728722 | 00881643-7a5e-4eed-b144-06423a88202b | `overnightvoltagerequirement20-26_1.csv` | 2026-10-02T13:22:22.101474 | 2026-10-08T11:41:05.238883+00:00 | 288655 | 5c883743df935c62 |

`url_type` is `upload` for every capture.

## What each cut keeps

- `l.csv`: bronze lines 2-4 (2026 weeks 40-42), 14-16 (the 2026/2027 year boundary), 67-68 (2028 weeks
  1-2) and 106 (2028 week 40); the file keeps its UTF-8 BOM.
- `m1.csv` (older `£ / SP` header epoch): lines 2-7. `m3.csv` (newer `£ / MWH` + `B6/EC5` epoch): lines
  2-7 plus the `WHILW-2` zero-cost row and the original's trailing wholly blank record. `m4.csv`:
  lines 2-5 plus its zero-cost row (`FALGW-1`), covering both `B6` and `EC5`.
- `d_clean.csv`: bronze lines 2-4 (`ESTEX` 2023-01-01), 214, 386 (`99999`), 914 (blank limit and flow),
  8307 (`-1` flow), 137857-137859 (the minute-only `...T00:00` spelling) and 468128 (`ERROEX`
  2025-08-12T00:00:00, 265 and -3). `d_collide.csv` adds line 468656 (the whole-row repeat of 468128)
  and lines 318029-318030 (`ERROEX` 2024-10-27T01:00:00 twice with flows 77 and 83, the differing-value
  fold collision).
- `o.csv`: lines 2-3, 27-30 (last populated actual 2026-10-10 and the first blank actuals), 53-54.
- `t1.csv` (the HOLD resource): lines 2-7 plus 23, 29, 30, the quoted comma costs `"2,150,551"`,
  `"1,390,386"` and `"819,640"`. `t2.csv`: lines 2-8, 15 (a large cost) and 559 (`SWALEX` -1556).
- `v.csv`: lines 2-4, the two note rows 1903 and 2170, 4159-4162 (the repeated `V_North` 2025-08-19
  pair's first row and the reversed 2025-08-23 to 2025-08-18 range) and 4177 (the pair's second row).
- `x1.xlsx`: the real workbook with its `Data` sheet cut to the header and 21 data rows (the first 18
  data rows and sheet rows 601-603), `dimension` and table `ref` set to `A1:C22`; every other part is
  kept so the body still has the registry's five sheets (P-5). The three large embedded images
  (`xl/media/*`) are replaced by the workbook's 840-byte PNG to keep the fixture small; no test reads a
  drawing. The date cells keep their numeric Excel date style, which is what the calamine reader turns
  into the spelling the record freezes (`2019-08-01 00:00:00`).
