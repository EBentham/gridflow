# Provenance: ASDP, ODFM and STOR day-ahead fixtures (v0.22-K-AS-5b)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header record and the original bytes of each kept record; only the record
selection is ours, made by streaming lines (no body was loaded whole, the 21 MB STOR body included).
None of these bodies has an embedded newline, so a line is a record. `.gitattributes` marks the
fixtures `-text`, so git stores the bytes unchanged: the ASDP datastore dumps end records with LF, the
ODFM and STOR uploads with CRLF; no body has a BOM. "sha8" is the first eight hex digits of the
original body's SHA-256 (the suffix of the bronze file name). "Original rows" counts the data records
of the original body. "Records kept" are 1-based record numbers of the original body (1 is the
header). The sidecar facts the tests write (package slug and id, resource name, vendor file name,
`ckan_last_modified`, `written_at`, `url_type`) are the originals', in
`test_neso_as5b_records.py::CAPTURES`.

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Records kept |
|---|---|---|---|---|---:|---|
| `i1.csv` | `asdp_instructions` | `6f037ebe-110d-4099-ab82-0ac3185e3f18` | datastore dump (resource id) | `0de9a797` | 9 | whole body; D8 header, the `2020-06-25 01:49:00Z` space-separated label |
| `i2.csv` | `asdp_instructions` | `90ba1625-d99b-4ec6-8356-ad69af4e6b20` | datastore dump | `06922544` | 1,125 | 2 (blank party / asset / tendered), 74-75 (`Tendered`), 930 (blank `MW`, `IndicativePrice` and dispatch stamp); D11 header |
| `i3.csv` | `asdp_instructions` | `9d03620d-e526-4c2e-8917-0d990efe71c9` | datastore dump | `bb0beedf` | 578 | 2, 406 (`RDP_NEGATIVE`, MW 7) |
| `i4.csv` | `asdp_instructions` | `f352265b-01c4-4f45-b0e4-bef889b74d23` | datastore dump | `e9095f35` | 837 | 2, 702 (`Non-Tendered`) |
| `w1.csv` | `asdp_window_prices` | `91749b29-d98a-4565-b126-bd021253b3af` | datastore dump | `a94dcf61` | 25,933 | 2, 3, 6 (EntryID `51279` published twice, different `CreatedTime`) |
| `o1.csv` | `odfm_market_information_instructed` | `f0a52698-5f80-4fb7-ad64-ede99015195f` | `odfm-instructed-volume-for-05-07-2020.csv` | `1c1bed8d` | 49 | 2-3 (V1 header with 46 empty trailing names; period 46 / 47 against Time 23:00 / 23:30) |
| `o2.csv` | `odfm_market_information_instructed` | `5c507a16-d2f3-44df-b1bb-98acc0d74e1c` | `odfm-instructed-volume-for-05-07-2020-v2.csv` | `0bb7cae7` | 49 | 2-5 (04/07/2020 periods 47 and 48, 05/07/2020 periods 1 and 2) |
| `l1.csv` | `odfm_market_information_load_factors` | `e6146148-c862-49f6-9c7f-52dc2b3e7086` | `odfm-loadfactors-2020-2021.csv` | `2cc1c8c5` | 240 | 2 (Solar `0`, Other `1`), 14-15 (Solar nonzero) |
| `s1.csv` | `stor_da_auction_results` | `844a1a65-0a3c-4704-b327-d1685d0c397b` | `stor-da-auction-results.csv` | `eb962f87` | 121,480 | 2 (24 hour), 44 (rejected, blank contracted MW and clearing price), 264 (23 hour), 2068 (rejected, minimum acceptable MW 0), 12814 (25 hour), 31176 (blank buy curve), 95618 (blank unit id) |

## Epoch headers

Headers are read from each body's first record the way the production reader returns it (edge
whitespace stripped): D8 (8 columns) and D11 (D8 plus `PartyID`, `AssetID`, `Tendered`) for the 70
ASDP instruction resources (16 and 54 bodies), P8 for both window-price resources, V2 (`Settlement
Period, ODFM Instructed MW, Time, Date`; the body writes `ODFM Instructed MW `, `Time ` and `Date `
with edge spaces), O43 (`Datetime` plus 14 regions x Wind, Solar, Other) for the load factors and T16
for STOR. The V1 header is those four fields plus 46 empty names, which `HeaderEpoch` cannot freeze.

## Clock, unit and sign evidence

- ASDP: `MW` is MW "dispatched" (3-100, 109 blanks), `IndicativePrice` £/MWh (4 zeros kept, 109
  blanks). The four stamps are raw strings: the early bodies write `2020-06-25 01:49:00Z`, the later
  ones naive ISO `2021-11-01T12:11:00` (TODO: ASDP-EXPORT-CLOCK, non-holding). `RDP_NEGATIVE` rows
  publish a positive MW next to the service code (TODO: ASDP-SIGN-CONVENTION, non-holding). Dispatch
  and cease stamps are send times of separate publication stages (TODO: ASDP-ISSUE-IMMUTABILITY,
  non-holding). `Tendered` publishes `Tendered`, `Non-tendered`, `Non-Tendered` and blank. The
  service migrated to OBP from 2025-10-10 with a different grain and id system: `entry_id` is never
  matched to the AS-3b OBP ids.
- Window prices: the key `(resource_id, created_time, asset_id)` is unique over 39,242 rows;
  `(EntryID, SPStartDateTime)` and `(EntryID, CreatedTime)` fail in the 2025 resource.
- ODFM: a 2020 temporary service, no longer live. V2 settlement periods agree with the Time labels
  on all 49 rows; V1 is one period behind on all 49 and 36 of its volumes differ. Load factors:
  240 rows, 42 numeric columns, no blanks, clock and scale undocumented. Results (42,895 rows in two
  bodies) repeat complete rows (9 and 170 exact duplicates) and `Unique ID` collides, so no key passes
  the duplicate guard: no record (RULINGS 538).
- STOR: `Service Delivery From` / `To` carry a literal `Z` equal to the documented 05:00
  Europe/London service day on every row (120,803 24-hour, 364 23-hour, 313 25-hour). Blanks:
  `Buy Curve ID` 328, `Unit ID` 55, `Contracted MW` and `Market Clearing Price` 58,171 each (rejected
  rows), `Minimum Acceptable MW` 243, `Tendered Availability Price` 3; 80 minimum-acceptable cells
  are `0`. Prices are £/MW/h.

## Hold questions and the resource-HOLD reason (verbatim in the registry)

- `asdp_window_prices` (held): TODO: ASDP-WINDOW-PRICE-UNIT.
- `odfm_market_information_load_factors` (held): TODO: ODFM-LOAD-FACTOR-CLOCK-SCALE.
- ODFM V1 `f0a52698-5f80-4fb7-ad64-ede99015195f` (resource HOLD): TODO: ODFM-INSTRUCTED-V1.

## Non-holding TODOs (docstring / this file only)

TODO: ASDP-EXPORT-CLOCK, ASDP-SIGN-CONVENTION, ASDP-ISSUE-IMMUTABILITY, ODFM-2020-FINAL-TERM (no
clipping to an assumed service window), ODFM-JOIN-CONTRACT (the three ODFM CSV families are never
joined), ODFM-RESULTS-ROW-ID, the CP1252 decoding candidate for the ODFM results and the mixed
report-period `Date` labels.
