# Provenance: ancillary-services batch 1 fixtures (v0.22-K-AS-1)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line (a BOM stays); only the line
selection is ours. The five bodies `d1`, `d2`, `m1`, `r1` and `q1` carry CRLF and `m1` / `r1` also a
BOM; the BR bodies (`b1`-`b4`) and the long-term body (`l1`) are LF with no BOM. `git` normalises the
committed line endings, so the tests rebuild CRLF for the five CRLF uploads only. The "sha8" is the
first eight hex digits of the original body's SHA-256, the suffix of the bronze file name. "Lines"
are 1-based line numbers of the original body (1 is the header). Nothing is repaired or re-decoded.
Every resource is an `upload` with exactly one captured body.

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---:|---|
| `b1.csv` | `br_auction_requirement_fc_da` | `019f6fba-4f17-4056-9ef8-31df44ff2e30` | `3a27b093f23b46e4ab29472e6385cc41_balancing-reserve-day-ahead-auction-requirements-forecast.csv` | `55978d13` | 48 | 2-4 (windows 1-3 of 2026-10-09) |
| `b2.csv` | `br_auction_requirement_fc_da_archive` | `b9ce03d0-c189-44ef-92e4-c02e91feb65d` | `f96b7ba699ca462285e7e3ca92fdfb8a_balancing-reserve-day-ahead-auction-requirement-forecast-archiv.csv` | `8ff327b8` | 46,414 | 2-3, 626-627 (zero requirement pair, 2024-02-28 windows 1-2), 6768 and 6813 (delivery 2024-07-05: window 46 produced 2024-06-20, window 1 produced 2024-07-03) |
| `b3.csv` | `br_auction_requirement_fc_medium_term` | `fb2e0eea-b1c0-4ba3-a817-0561182a6e26` | `510b11f90bf7456b8d6293ff4fd62acb_balancing-reserve-requirements-medium-term-forecast-archive.csv` | `a29eb9b5` | 46,414 | 2-3, 626-627 (zero requirement pair) |
| `b4.csv` | `br_auction_requirement_fc_medium_term` | `a9470515-c1de-4c4b-9597-2d78bf24c29f` | `64375eba1e78491b83ebd7917cd0c5b3_balancing-reserve-requirements-medium-term-forecast.csv` | `69d332df` | 672 | 2-4 (the current resource, production date 2026-09-24) |
| `d1.csv` | `dc_forecast_4d` | `1b85a3f3-80f0-49cf-9b0e-49648fa0cae6` | `f9bf31f0d49f45baa72649f0a8d2e254_dcrequirements_1b85a3f3-80f0-49cf-9b0e-49648fa0cae6_patch.csv` | `8c90bf7e` | 8 | 2-9 (the whole body; quoted cells, slash dates `08/10/2026`-`11/10/2026`) |
| `d2.csv` | `dc_forecast_4d_history` | `d5c3b48a-a0a9-4d57-a02a-ac0af09a6298` | `b4634e50af5e4d3bb7f610422776a819_dcrequirementshistorical1_d5c3b48a-a0a9-4d57-a02a-ac0af09a6298_.csv` | `42e2b455` | 12,932 | 2, 2002 (all-zero EFA row), 12475-12482 (`04/08/2026` then ISO `2026-08-06` creation labels; target `06/08/2026` DC-L under both), 12931-12933 (creation `2026-10-08`, after the upload's last-modified date) |
| `m1.csv` | `dm_requirements` | `2aae8747-776d-4fe5-af9c-adcf38f1af8a` | `dm-requirements.csv` | `a1deb27a` | 78 | 2-5 (BOM; `DML` / `DMH`, 2026-09-23 and 24) |
| `r1.csv` | `dr_requirements` | `d6c576b9-91d5-4c48-bf6d-300c7d7aa6ad` | `dr-requirements.csv` | `c58dfc4d` | 78 | 2-5 (BOM; `DRL` / `DRH`) |
| `l1.csv` | `dc_dm_dr_long_term_fc` | `5162c4d1-6d72-497b-a2ad-3a2449bc8952` | `dx_future_requirement.csv` | `7150db73` | 4,212 | 2-4 (DC-H), 3512-3520 (`DR_L`, EFA 1, all nine intervals of April, one `1.0` and eight `0.0`), 3566 (`DR_L` for May) |
| `q1.csv` | `qr_auction_requirement_fc` | `f012de08-b258-408b-bc41-f885e183f97f` | `qr.csv` | `d3ce986f` | 1,874 | 2-3, 1541-1546 (clock-change day 2026-10-25 windows 4, 5, 6, 5x, 6x, 7: `5`/`5x` both start `25/10/2026 01:00`, `6`/`6x` both `01:30`) |

The sidecar facts the tests write (package slug and id, resource name, vendor file name,
`ckan_last_modified`, the `written_at` capture time) are the originals', in
`test_neso_as1_records.py::CAPTURES` and `PACKAGES`.

Record notes that have no field in the frozen record (kept here and in the test docstring):

- `br_auction_requirement_fc_medium_term` units are MW per the shared Balancing Reserve requirement
  definition (K-AS-1-FACTS section 4.3: the medium-term field dictionary leaves units blank; the
  evidence transfer is NESO's BR requirements text and the EAC capacity definition, not magnitudes).
- `dm_requirements`: TODO: the vendor `Service` dictionary text describes DR, not DM; NESO to correct
  it. The DM codes `DML` / `DMH` are kept, never relabelled DR.
- `dc_dm_dr_long_term_fc`: TODO: authoritative mapping for the literal `DR_L` (the dictionary lists
  `DR-L`); preserved byte-equal.
- BR / QR end labels: TODO: endpoint precision and inclusivity (the dictionary examples carry seconds
  the bodies omit); kept as the raw vendor strings.
