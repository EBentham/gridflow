# Provenance: FFR post-tender CSV reports and Phase-2 FFR auction fixtures (v0.22-K-AS-5a)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header record and the original bytes of each kept record; only the record selection
is ours. FFR cells hold embedded newlines, so the cut was made on CSV record boundaries (quote parity),
never on raw lines, and no body was loaded whole into a test. `.gitattributes` marks the fixtures
`-text`, so git stores the bytes unchanged: every body ends records with CRLF; the FFR bodies carry a
UTF-8 BOM and lone LFs inside quoted cells (header cells included); the phase-2 bodies carry neither
BOM nor lone LFs. Nothing is repaired or re-decoded. "sha8" is the first eight hex digits of the
original body's SHA-256 (the suffix of the bronze file name). "Original rows" counts the data records
of the original body (wholly blank rows included). "Records kept" are 1-based record numbers of the
original body (1 is the header). All eleven families are upload resources (`url_type: upload`, a
vendor file name each); the sidecar facts the tests write (package slug and id, resource name, vendor
file name, `ckan_last_modified`, `written_at`) are the originals', in
`test_neso_as5a_records.py::CAPTURES`.

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Records kept |
|---|---|---|---|---|---:|---|
| `r144.csv` | `ffr_post_tender_reports_csv` | `ceefff8c-7f61-41dd-b567-42774ca487bc` | `tr144-post-tender-reports_v11.csv` | `5d9de820` | 240 | 2 (slash dates), 37 (lone-space 10s cell), 91 (embedded-newline post code), 203 (Excel serials), 236 (`-` nomination fee) |
| `r139.csv` | `ffr_post_tender_reports_csv_ext` | `9347c45d-bffd-40ee-aa1e-593b41bde9a4` | `tr139-post-tender-reports_v1-1.csv` | `3a3f9799` | 97 | 2, 26-27 (prose 30s cells); F42 header |
| `r142.csv` | `ffr_post_tender_reports_ext` | `1882b277-6349-4513-9939-b39e1992620b` | `tr142-post-tender-reports_v1-1.csv` | `d2068bd6` | 164 | 2, 129 (`-` nomination fee) |
| `r146.csv` | `ffr_post_tender_reports_csv_ext` | `66e127dc-5e95-49b5-bc55-915269d09e1a` | `tr146-post-tender-reports_v11-csv-corrected-tender-ref.csv` | `60e06799` | 196 | 56 (the one `1` in the 10s column) |
| `r147.csv` | `ffr_post_tender_reports_csv_ext` | `c99081fb-0211-4a1a-9eed-53497f64a6ea` | `tr147-post-tender-reports-ext.csv` | `7c9c8fd2` | 212 | 2, 77 (`1,366.61` availability fee) |
| `r148.csv` | `ffr_post_tender_reports_csv_ext` | `fc994d78-f7d0-4a29-9ea7-31121931d04c` | `tr148-post-tender-reports_v11ext.csv` | `4dfd0883` | 212 | 2, 205 (wholly blank row) |
| `r163.csv` | `ffr_post_tender_reports_csv_ext` | `fbb1ce86-b2f7-405d-94ac-6cef6f14cceb` | `tr163-post-tender-reports_v11ext.csv` | `0f21c976` | 194 | 2, 190 (wholly blank row), 195 (`162.198`) |
| `r164.csv` | `ffr_post_tender_reports_csv_ext` | `2bfa2ab3-7f4e-44d7-a9ab-7342c4b9cd34` | `tr164-post-tender-reports_v11ext.csv` | `3afe4e88` | 194 | 195 (`162.198`) |
| `bw.csv` | `ffr_phase2_auction_phase_block_order` | `edfb5b8f-4415-45e6-b378-57020c41e96b` | `ngeso_fra-weekly_2021-11-26_blockorders.csv` | `ba34edab` | 379 | 2-3 (`NA` block code PRM, negative volume) |
| `bm9.csv` | `ffr_phase2_auction_phase_blockorders` | `979329c3-5587-4b08-a900-5520b58b2d8f` | `phase-2-ffr-auction-blockorders-masterdata-2019-2020.csv` | `01e68080` | 12,446 | 2-3 (blank Bid Validity), 3492 and 3494 (Bid Validity text); `1E+14` ids |
| `bm0.csv` | `ffr_phase2_auction_phase_blockorders` | `c074d827-ad03-4b11-b22e-8bbc96be3bab` | `phase-2-ffr-auction-blockorders-masterdata-2020-2021.csv` | `e9b8f15a` | 50,145 | 2 |
| `bm1.csv` | `ffr_phase2_auction_phase_blockorders` | `89467af0-a577-4bba-9955-f8c380a78923` | `phase-2-ffr-auction-blockorders-masterdata-2021-2022.csv` | `74f11546` | 23,071 | 2 |
| `lw.csv` | `ffr_phase2_auction_phase_linear_orders` | `5cce4cb6-c270-488f-96c7-9f5cd7476898` | `ngeso_fra-weekly_2021-11-26_linearorders.csv` | `f5bd5c2b` | 84 | 2, 26-27 (`NA` curve points) |
| `lm9.csv` | `ffr_phase2_auction_phase_linearorders` | `47ceb316-e284-4e14-bb67-76067a8f9562` | `phase-2-ffr-auction-linearorders-masterdata-2019-2020.csv` | `3226f37f` | 1,008 | 2-3 |
| `lm1.csv` | `ffr_phase2_auction_phase_linearorders` | `eb394ce7-20c6-4434-a263-470b2896fd7b` | `phase-2-ffr-auction-linearorders-masterdata-2021-2022.csv` | `11333c03` | 2,940 | 2, 2882 (`NA` curve points) |
| `uw.csv` | `ffr_phase2_auction_phase_results_unit` | `87ee8fc4-11ae-4575-b734-99548aab2d98` | `ngeso_fra-weekly_2021-11-26_resultsbyunit.csv` | `733fdcb8` | 201 | 2-3 (keys shared with um1) |
| `um9.csv` | `ffr_phase2_auction_phase_resultsbyunit` | `15d7fa42-1c8d-4a79-86f8-890cf9228794` | `phase-2-ffr-auction-resultsbyunit-masterdata-2019-2020.csv` | `327be93c` | 9,985 | 2 |
| `um0.csv` | `ffr_phase2_auction_phase_resultsbyunit` | `52faffc9-e804-44d8-b222-dd0821d79e7d` | `phase-2-ffr-auction-resultsbyunit-masterdata-2020-2021.csv` | `ab717cd3` | 39,765 | 2 |
| `um1.csv` | `ffr_phase2_auction_phase_resultsbyunit` | `feafb7ab-7db6-406e-a46f-bbcf6ed92ba1` | `phase-2-ffr-auction-resultsbyunit-masterdata-2021-2022.csv` | `a09a489f` | 19,256 | 19057-19058 (keys shared with uw), 12124 (Cancelled prose) |
| `sw.csv` | `ffr_phase2_auction_phase_results_sum` | `a9659701-f0d4-40d2-91f1-31f4bac0619d` | `ngeso_fra-weekly_2021-11-26_resultsummary.csv` | `3ecc1931` | 84 | 2-3 (keys shared with sm1) |
| `sm9.csv` | `ffr_phase2_auction_phase_resultsum` | `8b5fa222-f39f-4eea-9475-728ea8365ff3` | `phase-2-ffr-auction-resultsummary-masterdata-2019-2020.csv` | `266ee54d` | 1,428 | 2 |
| `sm0.csv` | `ffr_phase2_auction_phase_resultsum` | `279572a9-21c9-4e1e-a7c8-7aed233f8936` | `phase-2-ffr-auction-resultsummary-masterdata-2020-2021.csv` | `711263fd` | 4,284 | 2 |
| `sm1.csv` | `ffr_phase2_auction_phase_resultsum` | `920105e5-a5e3-4bed-83ce-ab6901cc1b84` | `phase-2-ffr-auction-resultsummary-masterdata-2021-2022.csv` | `7d27fd33` | 2,940 | 2858-2859 (keys shared with sw) |

## Epoch headers

The FFR headers are read from each body's first record the way the production reader returns it (BOM
removed, edge whitespace stripped, inner double spaces, `
` inside a header cell and the `£` signs
kept). F43 (TR144 and every later resource, TR142, TR143) has 43 columns; F42 (TR139) is F43 without
`Post code`. Phase-2: B24 (block orders, weekly and masters), L22 (linear orders), U10 (results by unit)
and S7 (results summary) are one header each across the weekly file and the three masters.

## Clock, unit and quoting evidence

- FFR dates and window clocks: `Start Date` / `End Date` mix calendar dates (`31/12/2021`) and Excel
  serial integers (`44562`) inside one body (TR144 and six other admitted resources); the recurring
  `From` / `To` labels are clock labels (`300`, `2300`). Both are raw strings (TODO: FFR date and clock
  export semantics, non-holding: the raw strings publish no semantic). Durations are hours,
  availability fee £/h, nomination fee £/h and window initiation fee £/window (`-` cells in 15 / 16
  resources, so `string`), response columns MW. The four load-point columns publish only `-`.
- `Automatic Response Energy Deliverable by 10s (MW)` publishes a single-space cell in TR144 / TR145
  and a `1` in TR146 (RULINGS 773); the 30s column publishes TR139 prose. Both are `string`.
- Embedded newlines in `Post code` / `Comments` are kept (TR144 `S33 6
ST10 3EQ`).
- Phase-2: EFA is the weekly ordinal 1..42 of four-hour blocks (`int64`, never composed with a date);
  `EFA Date` is `%Y-%m-%d` in the two weekly results files and `%d/%m/%Y` in all six result masters;
  delivery and entry times are naive with an undocumented export clock (TODO: PHASE2-EXPORT-CLOCK,
  non-holding) and stay raw strings; volumes MW, order prices "£/MW as published" (hour basis TODO:
  PHASE2-PRICE-DICTIONARY-CORRECTION, non-holding for the weekly block, weekly linear and master linear
  records), by-unit clearing price £/MW/h; `MAR` meaning undocumented (string); `3P`..`4V` carry
  literal `NA` in 36 rows of the weekly linear file and of the 2021-2022 master (TODO:
  LINEAR-NA-SEMANTICS, string columns); `Bid Validity` is blank except three master rows; the masters'
  `OrderID` / `OrderPeriodID` / `TradeID` read `1E+14` / `1.00E+14` in every row. The vendor writes no
  quoted-empty cells in these bodies: a blank is null.

## Hold questions and resource-HOLD reasons (verbatim in the registry)

- `ffr_phase2_auction_phase_blockorders` (held): TODO: PHASE2-MASTER-ORDER-ID.
- `ffr_phase2_auction_phase_resultsum` (held): TODO: PHASE2-SUMMARY-PRICE-UNIT.
- TR147 `c99081fb-0211-4a1a-9eed-53497f64a6ea` (resource HOLD): TODO: FFR-TR147-THOUSANDS-SEPARATOR.
- TR154 `7e48c1ce-b4cf-43fa-afe9-4e6730474944` (resource HOLD): TODO: FFR-TR154-ORPHAN-ROWS.

The `r147.csv` cut proves the strict cast fails on `1,366.61` (the HOLD's reason); TR154 has no
fixture (its reason is the ten orphan rows, FACTS l.219, l.259).

## Weekly / master overlap

The weekly results-by-unit file's 201 keys and the weekly summary's 84 keys all occur in the 2021-2022
masters, whose CKAN last-modified (2021-11-26T11:20:28 and 11:20:55) is later than the weekly files'
(11:17:15 and 11:17:51): `key_latest` resolves each shared key to the master row, an as-of between the
two instants serves the weekly row. `uw` / `um1` and `sw` / `sm1` are cut to share two keys each.
