# Provenance: Demand Flexibility Service and STOR day-ahead buy-curve fixtures (v0.22-K-DFS-a)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header record and the original bytes of each kept record; only the record
selection is ours, made by streaming lines (no body was loaded whole in a test). None of these bodies
has an embedded newline or a BOM, so a line is a record; every body ends records with CRLF.
`.gitattributes` marks the fixtures `-text`, so git stores the bytes unchanged. "sha8" is the first
eight hex digits of the original body's SHA-256 (the suffix of the bronze file name). "Original rows"
counts the data records of the original body. "Records kept" are 1-based record numbers of the
original body (1 is the header). The sidecar facts the tests write (package slug and id, resource
name, vendor file name, `ckan_last_modified`, `written_at`, `url_type` `upload`) are the originals',
in `test_neso_dfs_a_records.py::CAPTURES`.

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Records kept |
|---|---|---|---|---|---:|---|
| `qa.csv` | `demand_flexibility_service_requirement` (Archive 2025/2026) | `f5605e2b-b677-424c-8df7-d0ce4ee03cef` | `dfs-service-requirement.csv` | `c79b1515` | 1,132 | 2-3 (2026-03-27, quoted participant list, zero GAP), 156 (2025-12-31, winter) |
| `qc.csv` | `demand_flexibility_service_requirement` (current) | `3635fd80-49d7-4d02-964d-cc8c08d50302` | `dfs-service-requirement.csv` | `136ad305` | 1,238 | 2 (Event 143, 2026-10-08), 246-247 (Event 118 Live, 2026-09-10 19:00 and 19:30), 254-255 (Event 117 Test, same windows) |
| `na.csv` | `demand_flexibility_industry_notification` (Archive) | `3d042450-b556-450f-a4b5-cdb075cdf9c1` | `dfs-industry-notification.csv` | `b746d964` | 191 | 2, 177-178 (08/01/2025 `Requirement Published` 10:00 and `Revised Requirement Published` 10:15) |
| `ba.csv` | `demand_flexibility_utilisation_report` (Archive) | `cc36fff5-5f6f-4fde-8932-c935d982ecd8` | `dfs-utilisation-report.csv` | `133d0160` | 9,950 | 2-3, 3876-3877 (GRIDBE-02, `Total` 0 beside `DFS Volume MW` 4.4) |
| `bd.csv` | `demand_flexibility_utilisation_report` (Archive) | `cc36fff5-5f6f-4fde-8932-c935d982ecd8` | `dfs-utilisation-report.csv` | `133d0160` | 9,950 | 9387-9388 (AXLE-01, 2025-01-05 16:00, whole row repeated) |
| `bc.csv` | `demand_flexibility_utilisation_report` (current) | `3ebf77d7-05df-466e-a023-dc45a90efeea` | `dfs-utilisation-report.csv` | `eb38fea5` | 14,663 | 12 (Accepted), 3488 (Accepted), 3496 (Rejected, same participant and window as 3488, procured MW 2) |
| `sa.csv` | `demand_flexibility_utilisation_summary` (Archive) | `25698259-0b66-42f0-ac59-ef0df5245812` | `dfs-utilisation-report-summary.csv` | `4fdd2f73` | 1,130 | 3 (settlement present), 682-683 (2025-06-05, blank settlement) |
| `sc.csv` | `demand_flexibility_utilisation_summary` (current) | `705e573c-ddac-4675-b410-82916b35c4fe` | `dfs-utilisation-report-summary.csv` | `42c337a5` | 1,230 | 2 (Event 142, blank settlement), 568-569 (Event 75, `-0.03` MW / `-2.65` GBP, then positive) |
| `st.csv` | `stor_da_buy_curve` | `8abc71ba-4484-4683-be68-5bc241b6ae7a` | `stor-da-auction-buy-curve-report.csv` | `d240b649` | 5,475 | 2-4 (2021-04-01, 24 hour, zero price and zero MW vertices), 638-640 (2021-10-30, 25 hour), 1079-1081 (2022-03-26, 23 hour) |

`bd.csv` and `ba.csv` are two cuts of the same archive report body: the whole body fails the duplicate
guard (4 repeated whole rows: AXLE-01 and AXLE-03 at 2025-01-05 16:00 and 16:30; the 9,950 rows have
9,946 distinct keys), `bd.csv` holds the first repeated pair and `ba.csv` only non-repeated rows.

## Epoch headers

Headers are read from each body's first record the way the production reader returns it (BOM and edge
whitespace stripped): Q8 (8) and Q15 (15) for the requirement, N6 (6) for the notification, B25 (25)
and B15 (15) for the report, S9 (9) and S14 (14) for the summary, C9 (9) for the buy curve. The C9
seventh-to-ninth names are `MW`, `Availability Price (£/MWh)` (one UTF-8 `£` character; the
profiler proposal's `Â£` is mojibake) and `Buy Curve: Buy Curve ID`. Silver names are the vendor name
lower-cased with each run of non-`[a-z0-9]` characters as one `_`, except `From` / `From_Local` ->
`window_from`, `To` / `To_Local` -> `window_to`, `From_UTC` -> `window_from_utc`, `To_UTC` ->
`window_to_utc`, `DFS Submission Time_Local` -> `dfs_submission_time`, and the STOR `buy_curve_step_id`,
`availability_price`, `buy_curve_id`.

## Clock, unit and sign evidence

- Every DFS window clock label (`From`, `To`, `From_Local`, `To_Local`, `From_UTC`, `To_UTC`) and the
  notification issue time are raw strings: the engine has no date + clock-label recipe (RULINGS
  572 / 588) and the archive clock is disputed (TODO: ARCHIVE-CLOCK-CONSISTENCY: the archive
  requirement dictionary labels From/To GMT, the archive report and summary dictionaries local time).
  No delivery window falls on a clock-change Sunday and there is no settlement-period column.
- The one typed DFS instant is the current `DFS Submission Time_Local` (`%d/%m/%Y %H:%M`,
  `Europe/London`, `ambiguous: raise`): NESO DFS participation guidance (document 346011, Appendices 1
  and 6) states UK local time; all 142 distinct values are neither ambiguous nor nonexistent. It is
  the auction-close deadline, never an issue time. `timestamp_utc` is the `date_sp1` anchor of the
  service date (UK-local midnight), not a window start.
- Zeros stay zero: the archive and current GAP is `0` in all 1,132 archive and 1,232 current rows; a
  report `Total` of `0` next to a nonzero `DFS Volume MW` is published (the regional columns are the
  tendered split, TODO: REGIONAL-RECONCILIATION, non-holding). Blank settlement (archive 2, current
  512 per column) is null; 42 negative settled volumes and 45 negative settled costs stay signed
  (TODO: SIGNED-SETTLEMENT-SEMANTICS, non-holding). `DFS Procured MW` is positive on Rejected rows
  (3,890 of the 14,663 current rows): accepted procurement is gated by `status`.
- STOR `Effective From Date` / `Effective To Date` carry a literal `Z` equal to the documented 05:00
  Europe/London service day on every row (5,445 x 24 h, 15 x 23 h, 15 x 25 h); `availability_price` is
  £/MWh (never the sibling results' £/MW/h); the 1,825 zero `mw` and 1,825 zero price cells are curve
  vertices. The vendor-hash TXT (`stor_da_buy_curve_files`, resource
  `1eacef3e-c7e5-4ce2-a1e3-c667f39c0a17`) stays `DOC` (algorithm and preimage undocumented, TODO:
  HASH-ALGORITHM / PREIMAGE, docs only). The curve joins `stor_da_auction_results` on `Buy Curve ID`
  only in docs (1,818 curve ids join 121,152 result rows; 328 result rows have a blank id and 7 curves
  have no results), no silver join. TODO: MISSING-CURVE-DATE (2022-08-17 absent; never manufactured).

## Hold question (verbatim in the registry)

`demand_flexibility_service_requirement`, unit `v0.22-K-DFS-a`: TODO: REQUIREMENT-ISSUE-AVAILABILITY
(see `QUESTION` in `test_neso_dfs_a_records.py`).

## Ledger entries appended last (ADR-040, verbatim in `_reconcile_adjudications.json`)

1. `ffr_post_tender_reports_workbook`, `overlap`, ruling 798: TR127 July 2020 workbook `68529290` and
   its Amended edition `16773f53` (available 2020-07-16 and 2020-07-30) publish the same 175 tender
   refs; the amended edition changes `status` and `rejection_codes` on 9 rows (Rejected / 1.2 to
   Accepted / NA); both editions are served and never deduplicated.
2. `ffr_post_tender_reports_workbook`, `overlap`, ruling 798: TR136 April 2021 EXT workbook `7912d65a`
   and its Amended edition `a85e6149` (available 2021-04-20 and 2021-05-07), the same 132 tender refs,
   9 rows change (Rejected / 2 to Accepted / NA).
3. `demand_flexibility_utilisation_report`, `failed` `DuplicateEntityKeyError`, ruling 804: the
   Archive 2025/2026 upload `cc36fff5` repeats four whole rows (AXLE-01 and AXLE-03, 2025-01-05 16:00
   and 16:30, all Rejected); no key is lossless, the capture stays unloaded and is never deduplicated
   (TODO: ARCHIVE-REPORT-IDENTITY).

## Other non-holding TODOs

EVENT-ID-NAMESPACE (Event IDs join requirement, report and summary only inside the current
resource; never a global Event-ID-only join), ENDPOINT-CLOSURE (ends kept literal),
NOTIFICATION-ISSUE-RETENTION, NOTIFICATION-EMPTY-LIFECYCLE (`empty_allowed` stays `false`).
