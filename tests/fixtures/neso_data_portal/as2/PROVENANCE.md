# Provenance: ancillary-services batch 2 fixtures (v0.22-K-AS-2)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line (a BOM stays, CRLF stays); only
the line selection is ours. `.gitattributes` marks the fixtures `-text`, so git stores the bytes
unchanged (the notification body `n1` carries a lone LF inside a quoted `Description` cell, which an
end-of-line rewrite would corrupt). The "sha8" is the first eight hex digits of the original body's
SHA-256, the suffix of the bronze file name. "Lines" are 1-based line numbers of the original body
(1 is the header). Nothing is repaired or re-decoded. Every resource is an `upload` with exactly one
captured body. BOM on `n1`, `n2`, `n3`, `n5`, `c1`, `f1` and `w1`; none on `n4`, `n6`, `s1`, `a1`,
`a2`.

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---:|---|
| `n1.csv` | `as_industry_notifications_balancing` | `13f8f6b1-690b-4695-993e-dbc26f9bbe54` | `balancing-reserve-important-industry-notifications.csv` | `5e93ae28` | 13 | the whole body (a `Description` with an embedded newline and a trailing newline; commas and quotes) |
| `n2.csv` | `as_industry_notifications_dc_dm_dr` | `5d6c5cf8-7f18-43d9-ab8c-ba3f88352f30` | `dc-dm-dr-important-industry-notifications.csv` | `619f429b` | 28 | the whole body (five `Service` values, combined labels) |
| `n3.csv` | `as_industry_notifications_quick_reserve` | `a72085a8-e167-406d-8504-ed72d7e142af` | `qr-important-industry-notifications.csv` | `5a7c2ece` | 13 | the whole body (no `Service` column) |
| `n4.csv` | `as_industry_notifications_sffr_important` | `9b51636d-fca5-4d00-946b-4e19770f1904` | `sffr-important-industry-notifications.csv` | `437edca5` | 1 | the whole body (`01/03/2024 10:00`, `Example note`) |
| `n5.csv` | `as_industry_notifications_slow_reserve` | `11a90fe4-27c1-46b5-9619-016f2752413d` | `sr-important-industry-notifications.csv` | `68b9895a` | 4 | the whole body |
| `n6.csv` | `as_industry_notifications_stor_important` | `1dbebd85-6b42-4d8f-8542-16a83d17dc14` | `stor-important-industry-notifications.csv` | `5e5108a0` | 9 | the whole body (GMT and BST issue labels) |
| `c1.csv` | `contract_transfer_obligation` | `dbd7f746-07c9-4804-8ab2-9bec2d7ef0ec` | `contract-transfers.csv` | `39631781` | 12 | the whole body (7 distinct rows, 5 duplicates, `auctionUnit` blank throughout, a trailing U+00A0 on one receiving unit) |
| `s1.csv` | `slow_reserve_requirement_fc` | `ccde1a9e-1885-4271-84a3-bb064194bce9` | `slow-reserve-requirement-forecast.csv` | `77cd4e25` | 1,874 | 2-3 (windows 1-2 of 2026-09-23), 1538-1587 (the 50 windows of the clock-change day 2026-10-25: `5`/`5x` both start `25/10/2026 01:00`, `6`/`6x` both `01:30`) |
| `a1.csv` | `sffr_auction_results_main` | `76bc5d39-c39f-490b-984e-58eb8ff296ef` | `static-firm-frequency-response-auction-results_76bc5d39-c39f-490b-984e-58eb8ff296ef_patch.csv` | `d5058622` | 229,631 | 2-4 (accepted), 24-25 (two `Rejected` rows with `Accepted Volume` 0.0), 28917 (blank `Technology Type` and `Location`) |
| `a2.csv` | `sffr_auction_results_buy_orders` | `f9f4b295-6de6-4379-9211-b0b2117a0c48` | `static-firm-frequency-response-buy-order_f9f4b295-6de6-4379-9211-b0b2117a0c48_patch.csv` | `8a6f9f95` | 7,722 | 2-7 (2023-04-01 EFA 1-6) |
| `f1.csv` | `sffr_requirement` | `bf236101-7167-4351-9fa0-c0b2e7c85747` | `sffr-data.csv` | `151d3950` | 39 | 2-6 (2026-09-23 .. 27, BOM) |
| `w1.csv` | `stor_windows` | `90d4a1a4-d311-4a22-9138-84cb7ead2599` | `stor_committed_windows.csv` | `6e5c1515` | 1,826 | 2-3 (season `15.1`, BOM), 1756-1757 (season `19.6`) |

The sidecar facts the tests write (package slug and id, resource name, vendor file name,
`ckan_last_modified`, the `written_at` capture time) are the originals', in
`test_neso_as2_records.py::CAPTURES` and `PACKAGES`.

Record notes that have no field in the frozen record (kept here and in the test docstring):

- `contract_transfer_obligation` has **no record** (RULINGS 538: even every source column together
  fails the engine's duplicate guard on its 12 rows, 5 duplicate excess). The eligibility report has
  no field for a no-record reason, so the two open questions are recorded here and in
  `test_contract_transfer_obligation_has_no_record`. TODO: CONTRACT-TRANSFER-IDENTITY (settle with a
  vendor transfer identifier or a revision carrying distinguishing columns; no broader composite of
  the existing columns can). TODO: CONTRACT-TRANSFER-PERIOD (the date-only `deliveryStart` /
  `deliveryEnd` labels: clock, endpoint closure and omitted-time meaning).
- Industry notifications carry no `issue_time`: every newest issue label converts to an instant after
  the upload's last-modified (+163 s to +1,652 s), so it is the event time, not an evidenced
  availability instant. Effective-to closure is undocumented (endpoints kept as given).
- `sffr_auction_results_buy_orders` is held: the dictionary labels the band columns MW but describes
  their cells as prices (the `200` entry has no unit). The column names `c_25` ... `c_250` are the
  profiler's `silver_name` rule for a numeric header.
