# K-SCN-1b fixtures

Cut from the 2026-10-08 swept bronze under `C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/`
(read-only, a scratch script that is not committed). Each fixture is a slice: the header, the first one
or two locations of the body, the year labels 20/21/22/23/35/50 that the capture holds, every scenario
and technology of those, plus the vendor oddities the tests name (blank `DemandPk` and `B_EXTRA_1`
rows of 2021 demand; blank-measure rows and `TOWH` / `TONG1` of sub-1 MW generation; the `add GSPs`
row of the 2022 above-1 MW generation file with its four unnamed trailing columns; the six `#N/A`
coordinate rows of the 2021 and 2022 GSP lookups and the 2022 lookup's `0x92` byte row; blank
`summam` / `summpm` rows of 2022 above-1 MW storage; the 2021 above-1 MW storage's two preamble lines
and its three repeated `CF,CAES,25,NORT_1` rows). A BOM stays where the original had one. `git`
normalises line endings, so `tests/unit/test_neso_scn1b_records.py::body` restores the bronze CRLF.
The 2022 GSP lookup fixture is deliberately not valid UTF-8 (`0x92`, Windows-1252); do not re-encode it.

Package `regional-breakdown-of-fes-data-electricity`, package id `963525d6-5d83-4448-a99c-663f1c76330a`;
`url_type` is `upload` for every capture.

| Fixture | Family | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---:|---|
| `dem21.csv` | `fes_regional_demand_active_power` | 3360c832-acc3-4656-8a38-f0bdf57bde88 | `fes2021_regional_breakdown_active_power.csv` | 2021-07-06T11:06:43.990644 | 2026-10-08T10:56:39.787331+00:00 | 10986770 | e8fb5c3be3d67214 |
| `dem24.csv` | `fes_regional_demand_active_power` | 7bd643c0-6aad-41ac-aa59-92b68992d9a4 | `fes2024_regional_breakdown_active_power.csv` | 2024-07-15T06:43:07.255666 | 2026-10-08T10:56:48.413285+00:00 | 8153655 | bb6d9dba0dd3c983 |
| `dgg21.csv` | `fes_regional_dg_gt_1mw` | fc6e9d6e-0995-447a-8819-2099c845ad7e | `fes2021_regional_breakdown_distributed_generation.csv` | 2021-07-06T10:59:37.680231 | 2026-10-08T10:56:52.270524+00:00 | 4055763 | 47ca03ae82727478 |
| `dgg22.csv` | `fes_regional_dg_gt_1mw` | 162bdb05-1d05-46f8-b3cd-55a86ef65b8e | `fes2022_regional_breakdown_distributed_generation.csv` | 2022-07-15T13:35:02.326000 | 2026-10-08T10:56:54.720227+00:00 | 4701143 | 01da6b2f2853ae4f |
| `dgl21.csv` | `fes_regional_dg_lt_1mw` | e05c34ec-1a0b-496c-b098-e35d4f998dc9 | `fes2021_regional_breakdown_sub1mw_generation.csv` | 2021-07-06T10:54:05.003828 | 2026-10-08T10:57:04.504161+00:00 | 7005723 | d0559cf6f133b41c |
| `dgl22.csv` | `fes_regional_dg_lt_1mw` | 8cd8a80a-9c36-436c-9014-8666c77e95d7 | `fes2022_regional_breakdown_sub1mw_generation.csv` | 2022-07-15T13:35:59.358715 | 2026-10-08T10:57:07.008367+00:00 | 8733877 | d0d2ee1c046706d3 |
| `dsr21.csv` | `fes_regional_dsr` | 180d9908-7c4a-4db7-b6f1-92067209854c | `fes2021_regional_breakdown_demand_side_response.csv` | 2021-07-06T11:01:06.905207 | 2026-10-08T10:57:15.972865+00:00 | 940085 | 62807bb363fb2894 |
| `gsp21.csv` | `fes_regional_gsp_info` | 41fb4ca1-7b59-4fce-b480-b46682f346c9 | `fes2021_regional_breakdown_gsp_info.csv` | 2021-07-06T10:53:01.441148 | 2026-10-08T10:57:24.776324+00:00 | 15326 | b41263e86781a333 |
| `gsp22.csv` | `fes_regional_gsp_info` | 000d08b9-12d9-4396-95f8-6b3677664836 | `fes2022_regional_breakdown_gsp_info.csv` | 2022-07-15T13:31:25.009443 | 2026-10-08T10:57:27.213496+00:00 | 15316 | 6a93974d9352b7db |
| `stg23.csv` | `fes_regional_storage_gt_1mw` | 0465e5a3-ff3a-4e95-ac66-f59afd54cfe3 | `fes2023_regional_breakdown_dxstorage_gt1mw.csv` | 2023-07-07T13:36:43.988472 | 2026-10-08T10:57:36.424151+00:00 | 530184 | 3803f35087e8f7ba |
| `stg24.csv` | `fes_regional_storage_gt_1mw` | eba5fd0a-ea22-4c00-84c2-35260e328736 | `fes2024_regional_breakdown_dxstorage_gt1mw.csv` | 2024-07-15T06:45:04.345465 | 2026-10-08T10:57:39.027979+00:00 | 414317 | 1f5a5f6525a2f087 |
| `stgpre21.csv` | `fes_regional_storage_gt_1mw_pre2023` | b954c63f-c108-4e71-9b43-b249d0d92a1b | `fes2021_regional_breakdown_dxstorage_gt1mw.csv` | 2021-07-06T10:50:48.532360 | 2026-10-08T10:57:42.611783+00:00 | 437570 | f693b7337c840a22 |
| `stgpre22.csv` | `fes_regional_storage_gt_1mw_pre2023` | ae5e3faf-b264-478e-82a5-fe0eb3101bba | `fes2022_regional_breakdown_dxstorage_gt1mw.csv` | 2022-07-15T13:33:26.428024 | 2026-10-08T10:57:45.166372+00:00 | 538315 | 16c4f73c16e12085 |
| `stl23.csv` | `fes_regional_storage_lt_1mw` | 86f90e3f-2a48-4bfc-80a9-9ff65c353d6e | `fes2023_regional_breakdown_dxstorage_sub1mw.csv` | 2023-07-07T13:37:11.264211 | 2026-10-08T10:57:49.488080+00:00 | 1324371 | 2ea666297fbca32c |
| `stl24.csv` | `fes_regional_storage_lt_1mw` | 0a1fce9e-8711-4017-bdef-867c0ab040a1 | `fes2024_regional_breakdown_dxstorage_sub1mw.csv` | 2024-07-15T06:45:51.492269 | 2026-10-08T10:57:52.168193+00:00 | 1468158 | 3d564432cb3f568d |
| `stlpre21.csv` | `fes_regional_storage_lt_1mw_pre2023` | f0db3dc8-4e8f-40e0-9def-58e0bca11e47 | `fes2021_regional_breakdown_dxstorage_sub1mw.csv` | 2021-07-06T10:48:22.393456 | 2026-10-08T10:57:55.801307+00:00 | 1225360 | 746f53d8c5512fba |
| `stlpre22.csv` | `fes_regional_storage_lt_1mw_pre2023` | 08f1f7f3-0448-4e80-aeb6-0ef082d86e8f | `fes2022_regional_breakdown_dxstorage_sub1mw.csv` | 2022-07-15T13:36:47.954918 | 2026-10-08T10:57:58.726143+00:00 | 1367212 | a02fe3495e1bb7cc |
