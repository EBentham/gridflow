# Container fixtures — provenance (ADR-037 P-15)

Byte copies of four real NESO Data Portal bronze bodies, copied out of
`C:/gridflow-data/bronze/neso_data_portal` on 2026-10-08 (read-only; bronze untouched).
`test_neso_containers.py` re-hashes every file against the SHA-256 below. Tests rebuild each
capture with `write_capture` using the sidecar values listed here.

| Fixture | SHA-256 | Bytes | Source body (under `bronze/neso_data_portal/`) |
|---|---|---|---|
| `cmp381_ii.xlsx` | `034f96d0ffb418ed0cd3bb31527e199c2a1086547b13d3e7aefc1beb4e196bc0` | 656880 | `current_bsuos_files/2026/10/08/raw_20261008T091602Z_88dcf101-1358-4d78-934f-527484d8789d_034f96d0.xlsx` |
| `tr129.xlsm` | `464bb287cf0867a370b8f76170205b7b51b049be9409ac9d81980ba90e780ad2` | 142726 | `ffr_post_tender_reports_files/2026/10/08/raw_20261008T110154Z_5a1f039a-c6c8-43a2-9ae4-069a0a0b32b2_464bb287.xlsm` |
| `result_summary.zip` | `54d0b3cb9894dbf81e9fa4af1c1caf63eafeb08193938a000cf368712b9f6587` | 1641 | `ffr_phase2_auction_files/2026/10/08/raw_20261008T105833Z_3928f192-97a5-4b4f-9234-0dcc9ad071a0_54d0b3cb.zip` |
| `tnuos_gen_zones.zip` | `18fa9cffdb7c4db1e1eedd09676edd1d82087ac7ae9a3b32ccc383686101c8b2` | 15399 | `gis_gen_charging_zones_files/2026/10/08/raw_20261008T110401Z_102b90d8-db66-42d1-a305-1108b3384e62_18fa9cff.zip` |

## Sidecar values

| Fixture | resource_id | resource_name | resource_filename | ckan_last_modified | ckan_format | package | package_id |
|---|---|---|---|---|---|---|---|
| `cmp381_ii.xlsx` | `88dcf101-1358-4d78-934f-527484d8789d` | `CMP381 II BSUoS Data` | `cmp381-current_ii_bsuos_110422.xlsx` | `2022-04-11T16:08:00.098987` | `XLSX` | `current-balancing-services-use-of-system-bsuos-data` | `d6a4bf54-c63f-4014-a716-49fd3878ca52` |
| `tr129.xlsm` | `5a1f039a-c6c8-43a2-9ae4-069a0a0b32b2` | `Post Tender Report TR129 September 2020` | `post-tender-report-tr129-september-2020-ext.xlsm` | `2020-10-21T07:51:24.169039` | `XLSX` | `firm-frequency-response-post-tender-reports` | `1162a519-b66d-48f6-96ae-1a69ff17951f` |
| `result_summary.zip` | `3928f192-97a5-4b4f-9234-0dcc9ad071a0` | `ResultSummary 2019-11-22 to 2019-11-29` | `resultsummary-2019-11-22-to-2019-11-29.zip` | `2021-03-31T09:22:26.071477` | `ZIP` | `phase-2-ffr-auction-results-summary` | `2d649d03-fb37-46a2-ae82-9e651438b559` |
| `tnuos_gen_zones.zip` | `102b90d8-db66-42d1-a305-1108b3384e62` | `GB Generation Charging Zones with ESRI Shape File` | `tnuosgenzones.zip` | `2022-05-16T15:06:19.632967` | `ZIP` | `gis-boundaries-for-gb-generation-charging-zones` | `f72029e7-3056-4021-9493-01c58a667d7a` |
