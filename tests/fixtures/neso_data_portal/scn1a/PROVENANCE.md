# K-SCN-1a fixtures

Cut from the 2026-10-08 swept bronze under `C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/`
(read-only). The CSV fixtures are slices (header, two geographies, a few building blocks, years
2025/2034/2035/2036/2050, the first zero and a scientific-notation value where the body has one);
git normalises their line endings, so `tests/unit/test_neso_scn1a_records.py::body` restores the
bronze CRLF. The workbook is a byte copy of the real body.

| Fixture | Family | Package id | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---|---:|---|
| `d_gsp.csv` | `tresp_demand_pathways_gsp` | e5e8eb8e-9fbd-4355-b4ad-9bbf00569d15 | d616d330-4829-4f8a-b74b-00a57b41bc20 | `tresp_pathways_demand_published.csv` | 2026-01-30T08:24:47.341529 | 2026-10-08T11:39:50.139049+00:00 | 11189109 | 2ced652c1e2f87a1 |
| `d_lae.csv` | `tresp_demand_pathways_la_england` | e5e8eb8e-9fbd-4355-b4ad-9bbf00569d15 | c3a261a4-94e9-4d75-b0de-d3805b0b5ae8 | `la_england_pathways_demand_published.csv` | 2026-01-30T08:21:33.560888 | 2026-10-08T11:39:54.204836+00:00 | 13370607 | 67944da06c948b6d |
| `d_las.csv` | `tresp_demand_pathways_la_scotland` | e5e8eb8e-9fbd-4355-b4ad-9bbf00569d15 | 67d699eb-c5e9-42cd-a57a-74fcc52bcea9 | `la_scotland_pathways_demand_published.csv` | 2026-01-30T08:19:49.400933 | 2026-10-08T11:39:57.658066+00:00 | 1488029 | 69e1c581ee06587c |
| `d_law.csv` | `tresp_demand_pathways_la_wales` | e5e8eb8e-9fbd-4355-b4ad-9bbf00569d15 | ccb3dfc8-ab4a-4f32-b944-8eced2e87bff | `la_wales_pathways_demand_published.csv` | 2026-01-30T08:18:41.157016 | 2026-10-08T11:40:02.562200+00:00 | 989626 | 8c76c5985b39d053 |
| `d_rr.csv` | `tresp_demand_pathways_resp_region` | e5e8eb8e-9fbd-4355-b4ad-9bbf00569d15 | cb1f1b31-1fde-40ab-86c9-34a3e1bdb665 | `tresp_pathways_demand_by_resp_region_published.csv` | 2026-01-30T08:23:31.655328 | 2026-10-08T11:40:06.710260+00:00 | 489762 | e8b3f7ffd49690b6 |
| `g_gsp.csv` | `tresp_generation_pathways_gsp` | 7f30dbc0-5d71-412a-b4f3-7e852ff7dfa7 | 970b7a25-6086-4b07-b109-b01ae43ae78c | `tresp_pathways_generation_storage_published.csv` | 2026-01-30T08:36:50.063067 | 2026-10-08T11:40:10.436470+00:00 | 19194150 | 1a9be88f54d17295 |
| `g_lae.csv` | `tresp_generation_pathways_la_england` | 7f30dbc0-5d71-412a-b4f3-7e852ff7dfa7 | d20aec73-01df-486d-a1aa-390c9cab2976 | `la_england_pathways_generation_storage_published.csv` | 2026-01-30T08:31:42.342845 | 2026-10-08T11:40:14.933216+00:00 | 23829011 | 560664536531b38a |
| `g_las.csv` | `tresp_generation_pathways_la_scotland` | 7f30dbc0-5d71-412a-b4f3-7e852ff7dfa7 | 38379002-1fcf-461a-beb1-2b15b555f994 | `la_scotland_pathways_generation_storage_published.csv` | 2026-01-30T08:28:56.090445 | 2026-10-08T11:40:18.223960+00:00 | 2654428 | 42fd39d67bc1c9d8 |
| `g_law.csv` | `tresp_generation_pathways_la_wales` | 7f30dbc0-5d71-412a-b4f3-7e852ff7dfa7 | 62275682-ed2c-44e8-aede-8404b19f3d67 | `la_wales_pathways_generation_storage_published.csv` | 2026-01-30T08:27:52.691429 | 2026-10-08T11:40:21.606872+00:00 | 1766450 | d62bc1fe619499a8 |
| `g_rr.csv` | `tresp_generation_pathways_resp_region` | 7f30dbc0-5d71-412a-b4f3-7e852ff7dfa7 | f295b8b5-8022-433d-b07f-b75b729fb199 | `tresp_pathways_generation_storage_by_resp_region_published.csv` | 2026-01-30T08:34:24.152349 | 2026-10-08T11:40:26.441105+00:00 | 862604 | 67c25b77cfc96137 |
| `tresp_lists.xlsx` | `tresp_demand_pathways_files` (sheets `tRESP Building Blocks`, `tRESP GSP Areas with Names`, `tRESP BB list - old`) | e5e8eb8e-9fbd-4355-b4ad-9bbf00569d15 | 11c36b60-eee5-45e9-b0da-6240e6e60d1c | `lists-of-tresp-pathways-building-blocks-and-of-tresp-gsp-areas-with-names.xlsx` | 2026-01-30T08:15:29.726307 | 2026-10-08T11:39:46.157281+00:00 | 41878 | fe3dde0f91c11acd6a3daf7f8eb0255dabb32c0203108ccbe7e2d7acb96d902e (full) |

`url_type` is `upload` for every capture.
