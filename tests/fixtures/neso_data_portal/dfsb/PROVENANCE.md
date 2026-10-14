# Provenance: frozen Demand Flexibility Service packages (v0.22-K-DFS-b)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header record (BOM and physical trailing spaces included) and the original bytes of
each kept record; only the record selection is ours, made by streaming lines and checking each line
against the `csv.reader` record of the same position (no body has an embedded newline, so a line is a
record). Every body ends records with CRLF. `.gitattributes` marks the fixtures `-text`, so git stores
the bytes unchanged. "sha8" is the first eight hex digits of the original body's SHA-256 (the suffix of
the bronze file name). "Original rows" counts the data records of the original body. "Records kept" are
1-based record numbers of the original body (1 is the header). The sidecar facts the tests write
(package slug and id, resource name, vendor file name, `ckan_last_modified`, `written_at`, `url_type`
`upload`) are the originals', in `test_neso_dfs_b_records.py::CAPTURES`.

Each family has exactly one captured resource, so there is no `resource_id` in any key and no
`latest_partition`.

| Fixture | Family | Package | Resource id | Vendor file name | sha8 | Original rows | Records kept |
|---|---|---|---|---|---|---:|---|
| `n23.csv` | `dfs_industry_notification` | `demand-flexibility-service` | `94b4d470-a54e-41d7-ae6e-be158db9b9d4` | `dfs-industry-notification.csv` | `7a8cf000` | 30 | 2-3 (21/03/2024 `Requirement Published` and a trailing-space `Anticipated Requirement Notice` status, Test), 28-29 (Live) |
| `q23.csv` | `dfs_service_requirements` | `demand-flexibility-service` | `7914dd99-fe1c-41ba-9989-5784531c58bb` | `dfs-service-requirements.csv` | `b6411b04` | 32 | 2-3 (Test, zero GAP, quoted participant list), 25-26 (Live, 01/12/2023) |
| `b23.csv` | `dfs_utilisation_report` | `demand-flexibility-service` | `ed7019b0-32b7-425c-a2fb-5ba9e32733fb` | `dfs-utilisation-report.csv` | `23c27ea9` | 1,220 | 2-3 (`within-day 2`), 56 (Rejected), 139 (`within-day 1`), 631 (`day-ahead` Test), 933 (Live) |
| `s23.csv` | `dfs_utilisation_report_sum` | `demand-flexibility-service` | `71a9cb04-8935-4e3a-bdd6-17f6fff0a7c6` | `dfs-utilisation-report-summary.csv` | `01be5836` | 32 | 2-4 |
| `ql.csv` | `dfs_live_events_service_requirement` | `demand-flexibility-service-live-events` | `663f3f82-fec8-4c9a-a837-df5db8690a6f` | `dfs-service-requirement-live230120231430.csv` | `93fcb6a5` | 5 | 2-6 (whole body) |
| `nl.csv` | `dfs_live_events_service_update_industry` | `demand-flexibility-service-live-events` | `a59a9d56-af4b-4044-b0c8-6379ce1213f8` | `service-update-industry-notifications-live070320231430.csv` | `c6e0aaa4` | 7 | 2-8 (whole body; three `Type` cells end in a space) |
| `bl.csv` | `dfs_live_events_utilisation_report` | `demand-flexibility-service-live-events` | `4e87244e-2479-4e6e-84c2-698dffaca41f` | `dfs-participant-utilisation-report-live2501231600.csv` | `94191b4f` | 170 | 2-3 (Accepted, D0 present), 101-102 (Rejected, zero price, all D0 cells blank) |
| `sl.csv` | `dfs_live_events_utilisation_report_sum` | `demand-flexibility-service-live-events` | `011f1a15-f63f-4a95-b1eb-453e5ffcd176` | `utilisation-report-summary-live2501231600-fs.csv` | `9d016003` | 5 | 2-6 (whole body) |
| `qt.csv` | `dfs_test_events_service_requirement` | `demand-flexibility-service-test-events` | `000066dc-aa75-4bec-a5a2-9c935b1873d2` | `dfs-service-requirement-test270320231430.csv` | `98f2e3c1` | 40 | 2-3 (2023-03-28, after last_modified), 40-41 (2022-11-15) |
| `nt.csv` | `dfs_test_events_service_update_industry` | `demand-flexibility-service-test-events` | `a2dd7992-32d5-4723-b0f5-273caaecbde1` | `service-update-industry-notifications-test270320231430.csv` | `e96c5fed` | 40 | 2 (2023-03-27), 21 (2023-01-29, padded status), 22-23 (2023-01-22: a quoted-comma status with a padded `Type`, and the anticipated notice), 41 (2022-11-14) |
| `bt.csv` | `dfs_test_events_utilisation_report` | `demand-flexibility-service-test-events` | `fc466b76-6823-4dd9-8b4a-aab464e0e77b` | `dfs-participant-utilisation-report-test280320231300.csv` | `f70ca0e4` | 739 | 64-65 (CUB-02 Rejected, D0 blank), 213 (SMS-01 21/02/2023, `Other` -0.093121335, `D0 Other` -0.093121337), 416-417 (SMS-01 19/01/2023, `Other` -0.271 / -0.277) |
| `st.csv` | `dfs_test_events_utilisation_report_sum` | `demand-flexibility-service-test-events` | `e4804faf-92be-43d0-a1ec-3e52a50f9666` | `utilisation-report-summary-test280320231300-fs.csv` | `0a6afc5f` | 40 | 2-3 (28/03/2023), 24-25 (17/01/2023 17:30 and 18:00) |

## Epoch headers

Headers are read from each body's first record the way the production reader returns it (BOM and edge
whitespace stripped; the two physical trailing spaces of `DFS Procured ` and `Settled Cost ` in the
Live / Test summaries are in the fixture bytes and removed by the reader): N-23 6, Q-23 8, B-23 26 (one
quoted cell `North Wales, Merseyside and Cheshire`), S-23 9, Q-L / Q-T 4, N-L / N-T 4, B-L / B-T 40,
S-L / S-T 10 columns. Silver names are the vendor name lower-cased with each run of non-`[a-z0-9]`
characters as one `_`, except `From` / `From GMT` -> `window_from`, `To` / `To GMT` -> `window_to` and
the Live / Test notification `Time` -> `time_label`.

## Clock, unit, hold and sign evidence

- Every window clock label (`From`, `To`, `From GMT`, `To GMT`) and the 2023/24 notification issue time
  are raw `HH:MM` strings: the engine has no date + clock-label recipe (RULINGS 572 / 588). The
  Live / Test notification `Time` is a GMT schedule label (`time_label`, key). `timestamp_utc` is the
  `date_sp1` anchor of the delivery / posting date (UK-local midnight), never a window start. No
  delivery window falls on a clock-change day; there is no settlement-period column.
- Units: `*_mw` and the Live / Test `dfs_volume`, `dfs_required`, `dfs_procured`, regional and `d0_*`
  volume columns are MW; `price` and `utilisation_price_gbp_per_mwh` are GBP per MWh; the cost columns
  are GBP. The Live / Test requirement `DFS Required MW` has a blank unit cell and is MW by the
  vendor description (FACTS l.525).
- Zeros stay zero (GAP zeros, zero bids, zero regional values, the zero Live price); blanks stay null
  (only the `D0 ...` report columns are blank); negative `Other` / `D0 Other` stay signed.
- Test events are real provider deliveries (onboarding / regular tests), not dummy data. The 2022/23
  Live and Test packages are separate operational series with equal headers and zero shared delivery
  dates: separate records, never unioned. None of these packages has an Event ID; rows join only on
  date + window (+ unit for bids) within a package.
- Held (REQUIREMENT-ISSUE-AVAILABILITY): `dfs_service_requirements` (29 of 32 window starts on or
  before last_modified 2024-03-14) and `dfs_test_events_service_requirement` (38 of 40 on or before
  2023-03-27). The Live requirement is eligible: all 5 starts (2023-01-23 / 24) are after its
  last_modified 2023-01-23T14:59:08 (the forward-target exception, RULINGS 529).
- Non-holding TODOs: NOTIFICATION-ISSUE-RETENTION (2023/24, Live, Test notifications),
  REGIONAL-RECONCILIATION (2023/24: regional sums match Total in 1,133 of 1,220; Live 130 of 170),
  D0-RECONCILIATION (Live summary: D-1 reconciles to the report, D0 and settlement do not),
  NEGATIVE-REGIONAL-VALUES (Test report `Other` / `D0 Other`, 4 each), SUMMARY-RECONCILIATION (Test
  summary, published values kept, never recalculated), ENDPOINT-CLOSURE (FACTS l.787; NESO "will no
  longer be updated", FACTS l.346).
