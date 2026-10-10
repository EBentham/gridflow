# K-SCN-1c fixtures

Cut from the 2026-10-08 swept bronze under `C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/`
(read-only, a scratch script that is not committed). Each fixture is a slice of whole vendor rows:
`m20`-`m25` (main, one per edition and header epoch): the header and about nine rows with every row
unit (`MW`, `Number`, `GWh`, `MW availability` where the body has it), blank projection cells, the
blank `Baseline (2019)` cells of 2020, zero values, a non-blank `Comment`, a `Share of GSP` value
(2024, 2025) and a second scenario / pathway; `m23` also keeps three of the real body's all-blank
tail rows (4551 in the original). `m20c`, `m21c` and `m22c` hold the named full-body key collisions
(2020 `CT, Gen_BB015, Camblesforth, NPg Yorkshire, MW` at table rows 11449 / 11702; 2021 `Central
Forecast, Gen_BB001, Ratcliffe, East Midlands, MW` at rows 7 / 8; 2022 `Leading the Way, Gen_BB001,
Direct(NGET), NGET, MW` at rows 3 / 4 / 10) plus two clean rows. `d20` (the `" MW"` spelling and
three of the four all-blank rows), `d22` (both unnamed columns, `" Metres squared "`,
`"% customers "`, `"Number of "`), `d24` (blank `Units` and `Template`) and `l24` (the whole 19-row
licence-area body, five `N/A` cells in each Elexon column) cover the reference families. A BOM stays
where the original had one. `git` normalises line endings, so
`tests/unit/test_neso_scn1c_records.py::body` restores the bronze CRLF.

Package `future-energy-scenario-fes-building-block-data`, package id
`30df2649-99cf-4f84-9128-6c58fc1ea72a`; `url_type` is `upload` for every capture.

| Fixture | Family | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---:|---|
| `m20.csv` | `fes_building_blocks_main` | bff7061d-fbd3-4d8a-a95b-876affc2033d | `fes2020_building_blocks.csv` | 2020-09-07T13:38:22.491166 | 2026-10-08T10:53:18.158336+00:00 | 11553187 | 4902afc9b2356398 |
| `m20c.csv` | `fes_building_blocks_main` | bff7061d-fbd3-4d8a-a95b-876affc2033d | `fes2020_building_blocks.csv` | 2020-09-07T13:38:22.491166 | 2026-10-08T10:53:18.158336+00:00 | 11553187 | 4902afc9b2356398 |
| `m21.csv` | `fes_building_blocks_main` | 5f93098e-1d52-44bf-a375-d3edfb89f8a5 | `fes-2021-building-blocks-version-008.csv` | 2022-02-17T10:22:57.199905 | 2026-10-08T10:53:21.060308+00:00 | 11567428 | 847ddfb06f0d8e6f |
| `m21c.csv` | `fes_building_blocks_main` | 5f93098e-1d52-44bf-a375-d3edfb89f8a5 | `fes-2021-building-blocks-version-008.csv` | 2022-02-17T10:22:57.199905 | 2026-10-08T10:53:21.060308+00:00 | 11567428 | 847ddfb06f0d8e6f |
| `m22.csv` | `fes_building_blocks_main` | 36fd3aa9-6e42-418f-b1bb-a31bbfcf2008 | `fes-2022-building-blocks-version-4.0.csv` | 2022-09-23T16:06:07.625574 | 2026-10-08T10:53:24.135967+00:00 | 12640835 | e7b4ed31fc4836d8 |
| `m22c.csv` | `fes_building_blocks_main` | 36fd3aa9-6e42-418f-b1bb-a31bbfcf2008 | `fes-2022-building-blocks-version-4.0.csv` | 2022-09-23T16:06:07.625574 | 2026-10-08T10:53:24.135967+00:00 | 12640835 | e7b4ed31fc4836d8 |
| `m23.csv` | `fes_building_blocks_main` | 8d57568c-2534-4682-ab1d-72fcf2d14998 | `fes-2023-building-blocks-version-1.1.csv` | 2023-07-21T15:52:22.011143 | 2026-10-08T10:53:27.019988+00:00 | 11421609 | 8585248fb4d6312c |
| `m24.csv` | `fes_building_blocks_main` | be1f002b-bd8b-4f8d-9e1d-72d680336e26 | `fes-2024-building-blocks-version-1.1.csv` | 2024-08-02T10:37:06.008284 | 2026-10-08T10:53:30.058267+00:00 | 13349288 | 57b6164151b39a51 |
| `m25.csv` | `fes_building_blocks_main` | 73f69d8f-e9cb-4a2d-8538-baf03b5eadef | `fes2025_bb1_v006.csv` | 2025-12-10T16:53:30.336039 | 2026-10-08T10:53:34.208849+00:00 | 11530820 | 1d582ecd0a9cccde |
| `d20.csv` | `fes_building_blocks_block_definitions` | 9fb5211f-9689-4d6f-a73e-cf28f03c5885 | `building-block-definitions.csv` | 2020-08-13T12:40:08.469499 | 2026-10-08T10:52:56.827482+00:00 | 8353 | e582e54460517f92 |
| `d22.csv` | `fes_building_blocks_block_definitions` | e5ab7ecb-0ab1-4fe7-833c-1fe905b086f8 | `building-block-definitions-2022.csv` | 2026-09-03T13:45:29.162340 | 2026-10-08T10:53:02.747471+00:00 | 12205 | 14f8b9f42962e05b |
| `d24.csv` | `fes_building_blocks_block_definitions` | 778a505c-1972-463b-b4bf-53e33c4d3470 | `building-block-definitions-2024.csv` | 2024-07-15T06:35:12.667550 | 2026-10-08T10:53:07.837772+00:00 | 7523 | 7b3252b3236390d8 |
| `l24.csv` | `fes_building_blocks_block_licence_area` | 0ec29244-277e-41d1-bca7-17f12b71eb60 | `building-block-licence-area-name-mapping-2024.csv` | 2024-07-15T06:35:22.821993 | 2026-10-08T10:53:14.274565+00:00 | 1436 | 0583f3c909db4c97 |
