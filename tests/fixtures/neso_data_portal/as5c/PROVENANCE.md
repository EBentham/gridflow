# AS-5c fixtures — provenance

Cut from the 2026-10-08 swept bronze under `C:/gridflow-data/bronze/neso_data_portal/` (read-only;
bronze untouched) by scratch scripts that are not committed. Tests rebuild each capture with
`write_capture`; nothing reads `C:/gridflow-data`.

## Lineage cell maps (FFR post-tender workbooks)

Each `lineage_<tr>.json` is a cell map of one real workbook's tender sheet, rebuilt into a small
workbook by `tests/unit/_container_support.lineage_workbook`. It keeps, verbatim at their own
refs, every parsed cell of sheet rows 1..h+4 (the notices, the three composite-header rows and the
two example rows) and every merge within those rows; then the tender rows `data_first`,
`data_first + 1` and `data_last` at rows h+5, h+6, h+7; row h+8 empty; and the first footer row
(`data_last + 2`) at h+9. `source_rows` maps each kept source row to its fixture row. The block of
a lineage fixture is its production block with `data_last = h + 7`.

Cell kinds: `s` a string (shared or inline), `n` a number (its stored `<v>` text), `fs` a formula
string with its cached text, `e` an error, `f` a formula without a cached value. Formula bodies
are written as `RC`: the reader reads cached values only.

The maps keep cell kinds, stored values and merges, **not number formats or styles**: the
composite path reads no style part (dates stay raw stored text, ADR-037 amendment 1), so no output
can depend on a style (K-11).

| Fixture | Tender | resource_id | Sheet | h | Columns | Source rows kept | Source body (under `bronze/neso_data_portal/`) | SHA-256 |
|---|---|---|---|---:|---|---|---|---|
| `lineage_tr122.json` | TR122 | `bf1bbc9a-ace7-4ec0-a0d5-aa6313fade21` | `TR122` | 2 | A:AP | 1-8, 165, 167 | `ffr_post_tender_reports_files/2026/10/08/raw_20261008T110050Z_bf1bbc9a-ace7-4ec0-a0d5-aa6313fade21_1079591e.xlsx` | `1079591e55c1b575b94c398fea0988d427044afa9a669d924eadfd154ae87b48` |
| `lineage_tr112.json` | TR112 | `dbaa2bb4-ab77-4297-90f2-a7f27e96f7fb` | `Sheet1` | 3 | A:AP | 1-9, 98, 100 | `ffr_post_tender_reports_files/2026/10/08/raw_20261008T110120Z_dbaa2bb4-ab77-4297-90f2-a7f27e96f7fb_5b43701c.xlsx` | `5b43701c87d9f7d7128dd494cd57cf1b8f258127d45d8572d4f751e5cd8afbd9` |
| `lineage_tr126.json` | TR126 | `9137187c-8626-4d28-a538-3dd9eb395644` | `TR126` | 4 | A:AP | 1-10, 190, 192 | `ffr_post_tender_reports_files/2026/10/08/raw_20261008T110143Z_9137187c-8626-4d28-a538-3dd9eb395644_03f01d41.xlsx` | `03f01d41c7e7cd233e68f32abd6cbd443adfb4972265c455eb7807b338fa1a8d` |
| `lineage_tr108.json` | TR108 | `31854ad2-0e98-48ab-8893-dc9b9d473091` | `Sheet1` | 6 | A:AP | 1-12, 432, 434 | `ffr_post_tender_reports_files/2026/10/08/raw_20261008T110131Z_31854ad2-0e98-48ab-8893-dc9b9d473091_492cb1c1.xlsx` | `492cb1c180795d6e971b5b9e4d12bbd11b8484f36d89229305078a83b81ae72b` |
| `lineage_tr144.json` | TR144 | `6cd10acf-2524-4c3f-8509-f657eeae73e5` | `TR144` | 2 | A:AQ | 1-8, 246, 248 | `ffr_post_tender_reports_files/2026/10/08/raw_20261008T110239Z_6cd10acf-2524-4c3f-8509-f657eeae73e5_273d4e40.xlsm` | `273d4e406090a914fadf2d465ac102089c2b370ee9cef36894f8546832574b5f` |

TR144's Mon-Fri From heading is the damaged `L` (header variant H22); its example rows carry
`#REF!` error cells (kept as `e`).

## Container output pin

`container_pin.json` is the I-6 golden of `tests/unit/test_neso_as5c_container_pin.py`: per
existing container record, the row count, schema and SHA-256 of its composed output over the
committed fixtures (`containers/`, `constraint_mgmt/x1.xlsx`, `scn1a/tresp_lists.xlsx`), computed
on master `493f9a6` before any AS-5c source change.
