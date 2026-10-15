# K-FILE fixtures

`gsp_lookup.csv` is cut from the 2026-10-08 swept bronze
`C:/gridflow-data/bronze/neso_data_portal/gis_grid_supply_points/2026/10/08/raw_20261008T110407Z_bbe2cc72-a6c6-46e6-8f4e-48b879467368_e6048a53.csv`
(read-only; a scratch script, not committed, that splits the body on its CRLF record terminators
and keeps each kept record's physical bytes). It is the body's header (UTF-8 BOM and CRLF kept)
plus 13 whole vendor records, byte for byte; no record is edited. `.gitattributes` (`*.csv -text`)
keeps the CRLF terminators and the BOM through git.

| Fixture | Family | Resource id | Vendor filename | `ckan_last_modified` | `written_at` | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---:|---|
| `gsp_lookup.csv` | `gis_grid_supply_points` | bbe2cc72-a6c6-46e6-8f4e-48b879467368 "GSP - Gnode - Direct Connect - Region Lookup 20181031" | `gsp_gnode_directconnect_region_lookup.csv` | 2020-05-05T07:54:07.800829 | 2026-10-08T11:04:07.596378+00:00 | 34324 | e6048a53 |

Package `gis-boundaries-for-gb-grid-supply-points` (id `2810092e-d4b2-472f-b955-d8bea01f9ec0`), capture
`url_type: upload`, CKAN format `CSV`. The real body has 380 data rows, 19 columns, 381 CRLF lines.

## What the slice carries

Record numbers are the body's data-record positions (header = 0); a CSV line number is the record
number plus one.

| Record | What it proves |
|---:|---|
| 188 | `Clyde’s Mill` (U+2019 kept byte-equal), a `has_pv=1` row |
| 203 | `has_pv=0` with every Gnode / GSP / region / PES cell populated |
| 289 | empty GSP, Direct Connect, region and PES cells (`gsp_id`, `gsp_name`, `gsp_lat`, `gsp_lon`, `dc_*`, `region_*`, `pes_*` all null), `has_pv=0` |
| 301, 302, 303 | `gsp_id=289` ×3 (a repeated non-key id), `has_pv=1` |
| 342, 343 | `dc_id=12` ×2 (a populated, repeated Direct Connect id) |
| 353, 354 | `region_id=16` ×2 (repeated non-key id) |
| 372 | the `MELK` row: `ng_id=387`, `ggd_id=372`, `gsp_name=MELK_1`, `region_id=18` (`ng_id` and `ggd_id` differ) |
| 373 | `MELK` Gnode with empty GSP / region / PES cells and a Direct Connect |
| 377 | one of the 4 rows with empty Gnode cells |

## Field-dictionary evidence per column (field-info `20261008T114228Z/gis_grid_supply_points.json`)

| Column | Vendor meaning, paraphrased | Vendor unit | dtype | Evidence |
|---|---|---|---|---|
| `ng_id` | dataset-local identifier used when deriving regions | N/A | int64 | FI:14–22 |
| `ggd_id` | identity of a lookup entity linking GSP, Gnode and licence-area information | N/A | int64 | FI:26–34 |
| `gnode_id` | dataset-local Gnode identifier | N/A | int64 | FI:38–46 |
| `gnode_name` | NESO's preferred Gnode name | N/A | string | FI:50–58 |
| `gnode_lat`, `gnode_lon` | Gnode latitude / longitude | degrees (WGS 1984) | float64 | FI:62–82 |
| `gsp_id` | dataset-local GSP identifier | N/A | int64 | FI:86–94 |
| `gsp_name` | GSP name following Elexon settlement naming | N/A | string | FI:98–106 |
| `gsp_lat`, `gsp_lon` | GSP latitude / longitude | degrees | float64 | FI:110–130 |
| `dc_id` | dataset-local Direct Connect identifier | empty unit field | int64 | FI:134–142 |
| `dc_name` | preferred Direct Connect name | empty unit field | string | FI:146–154 |
| `dc_lat`, `dc_lon` | Direct Connect latitude / longitude | degrees | float64 | FI:158–178 |
| `region_id` | identifier of a boundary record | N/A | int64 | FI:182–190 |
| `region_name` | boundary name, usually based on a GSP name | N/A | string | FI:194–202 |
| `has_pv` | vendor assessment of possible embedded solar-PV hosting (vendor type boolean, kept as `0` / `1`) | N/A | int64 | FI:206–214 |
| `pes_id` | PES / licence-area / GSP-group identifier | N/A | int64 | FI:218–226 |
| `pes_name` | industry character code for that PES / GSP group | N/A | string | FI:230–238 |

The vendor describes the numeric ids as arbitrary and inconsistent between versions; they are never
treated as stable across GSP Regions versions.

## Measured key table (full real body, 380 rows; K-FILE-FACTS §2.3)

| Candidate | Distinct tuples | Duplicate excess | Duplicate rows | Rows with a null key component |
|---|---:|---:|---:|---:|
| `ng_id` | 380 | 0 | 0 | 0 |
| `ggd_id` | 380 | 0 | 0 | 0 |
| `gnode_id` | 361 | 19 | 31 | 4 |
| `gsp_id` | 339 | 41 | 61 | 20 |
| `dc_id` | 18 | 362 | 365 | 361 |
| `region_id` | 326 | 54 | 85 | 20 |
| `(gnode_id, gsp_id, dc_id)` | 380 | 0 | 0 | 380 |
| Full vendor row | 380 | 0 | 0 | — |

## Non-holding TODOs (never a hold)

- **VENDOR-UNIQUENESS** — `ggd_id` uniqueness is measured (380 / 380), vendor-described as the
  lookup-entity identity (FI:26–34) but not vendor-guaranteed.
- **GSP-ID-JOIN** — TODO: direct GSP-ID-to-feature equality is unavailable in this 2018 GeoJSON. A vendor-issued feature property or mapping identifying GSPs would settle it.
- **CHANGELOG-GAP** — TODO: a vendor changelog for the 20181031 → 20220314 revisions would settle their detailed changes.
