# Provenance: BSAD aggregates, forward contracts and legacy Dynamic Containment fixtures (v0.22-K-AS-4a)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line; only the line selection is ours.
Bodies were read by streaming lines, never loaded whole (the block-orders body is 155 MB, the
by-unit results 45 MB). `.gitattributes` marks the fixtures `-text`, so git stores the bytes unchanged
(CRLF terminators on the upload bodies `a01`, `a03`, `a07`, `f02`, `f05`, `c01`; a lone LF on the
datastore bodies; a UTF-8 BOM on `a03` and `f02` only). Nothing is repaired or re-decoded: `a01` is
the original cp1252-style bytes and is **not** valid UTF-8. The "sha8" is the first eight hex digits of
the original body's SHA-256, the suffix of the bronze file name. "Lines" are 1-based line numbers of
the original body (1 is the header). `datastore` resources have no vendor file name (the sidecar
carries the resource id). The fixtures are named by the FACTS capture alias (`a03` is capture A03 of
`K-AS-4-FACTS.md`).

| Fixture | Family | Type | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---|---:|---|
| `a01.csv` | `aggregated_bsad` | upload | `d39fcf5e-32f9-4e44-a69d-b622effc165d` | `bs_netbsd_01-04-2013_31-03-2014.csv` | `27a4c4d6` | 17,538 | 1-3 (header holds the standalone `0xA3`) |
| `a03.csv` | `aggregated_bsad` | upload | `15950c77-8f7d-4d12-8f29-25eaab6a5155` | `bs_netbsd_01-04-2016_31-03-2017.csv` | `5b0b163d` | 17,525 | 1-3, 176 (BPA `0.26`), 17521 (31/03/2017 P48), 17522-17523 (wholly blank `,,,,,,,,,`) |
| `a07.csv` | `aggregated_bsad` | upload | `ec526741-f629-4620-9e31-d14da373392e` | `data.csv` | `ddc7639c` | 17,531 | 1, 4178-4181 (27/06/2020 P1 and P2, each twice) |
| `a10.csv` | `aggregated_bsad` | datastore | `36070f3e-3d73-4776-8fb3-da9e76d4bf82` | (none) | `0cdf624d` | 17,568 | 1, 10130-10131, 10178-10179 (2023-10-29 P1, P2, P49, P50), 17524, 17526, 17569 (2024-03-31 P1, P3, P46) |
| `f02.csv` | `bsad_forward_contracts` | upload | `16fe508d-27da-4282-b8af-1fa519b6f480` | `bs_forwardcontract_01-04-2017_31-03-2018.csv` | `bbcd85d1` | 17,525 | 1-3, 17521, 17522-17523 (wholly blank `,,,,,,,`) |
| `f05.csv` | `bsad_forward_contracts` | upload | `5d0f2119-1ba6-4d7d-8db5-ea4e23720aa9` | `data.csv` | `070a68c3` | 17,536 | 1, 7010-7011 (25/08/2020 P1: BSA 235 / SSA 170.5, then all zero) |
| `f09.csv` | `bsad_forward_contracts` | datastore | `93f9bdde-1fd9-4619-99e2-d1c5e38239e1` | (none) | `513e851f` | 17,520 | 1-3, 13578 (2025-01-08 P39, `BSA` = `-1`) |
| `c01.csv` | `dynamic_containment_masterdata` | upload | `0b8dbc3c-e05e-44a4-b855-7dd1aa079c68` | `dynamic_containment_masterdata.csv` | `00789d3d` | 10,388 | 1-2 (`N/A`), 4 (blank Total Cost), 5 (`Withdraw`), 78 (`NA` related entity), 170 (25 h), 2195 (padded agent), 3270 (23 h, `Withdraw`) |
| `c02.csv` | `dynamic_containment_dc_dr_dm_results_sum` | datastore | `888e5029-f786-41d2-bc15-cbfd1d285e96` | (none) | `d094e8d0` | 23,424 | 1-3 |
| `c03.csv` | `dynamic_containment_dc_dr_dm_results` | datastore | `ddc4afde-d2bd-424d-891c-56ad49c13d1a` | (none) | `05db5673` | 397,662 | 1-3 (literal `NA` technology type, quoted-empty Location and Cancelled) |
| `c04.csv` | `dynamic_containment_dc_dr_dm_linear` | datastore | `26aefbcc-fce0-403f-80e1-4a26af3fe84b` | (none) | `51a66faa` | 23,154 | 1-3 (the 14 + 64 column header) |
| `c05.csv` | `dynamic_containment_dc_dr_dm_block` | datastore | `6fd8e042-be27-4c67-ad59-5acdd2a7b0fd` | (none) | `6b634bc4` | 728,794 | 1-3 (`-1` volumes, `NA` block code PRM), 114147 (fractional MAR `0.45`, volume `-44`) |

The sidecar facts the tests write (package slug and id, resource name, vendor file name, `url_type`,
`ckan_last_modified`, the `written_at` capture time) are the originals', in
`test_neso_as4a_records.py::CAPTURES`. The datastore captures `a10` and `f09` (the 2023-24 onward
resources) carry an empty `ckan_last_modified`.

## Epoch headers

The frozen headers are read from each body's first line the way the production reader returns it (BOM
removed, edge whitespace stripped, inner spacing and `(£)` / `(MWh)` suffixes kept) and compared with
every other loading body of the family: A03-A07 share A-H1 (`Settlement Period`, suffixed value
names), A08-A13 share A-H2 (`SettlementPeriod`, bare names), F02-F05 share F-H0 and F06-F11 F-H1.
A01 / A02's legacy header A-H0 (EBCA, EBVA, ESCA, ESVA, SBVA, SSVA, BPA, SPA) is **not** frozen: those
bodies are not valid UTF-8 and fail before header matching.

## Clock, unit and quoting evidence

- BSAD dates: uploads `%d/%m/%Y`, datastore bodies `%Y-%m-%d` (one scalar format per epoch); the
  settlement period starts are elapsed half hours from UK-local midnight (the 50-period autumn day and
  46-period spring day are valid, `settlement_period` bounded `1..50`).
- BSAD units (vendor dictionaries): EBCA, ESCA, BCA, SCA GBP; EBVA, ESVA, SBVA, SSVA, BSA, BVA, SSA,
  SVA MWh; BPA, SPA GBP/MWh. Settlement period is an ordinal. All value columns `float64`.
- DC master: the resource dictionary states `Delivery Start UTC` / `Delivery End UTC` are UTC, the
  only evidence for `zone: "UTC"`. Volumes MW, duration hours, Availability Fee "GBP as published"
  (no rate derived), Total Cost GBP, Technology Type categorical despite a MW unit cell.
- DC results: Cleared Volume MW; by-unit Clearing Price GBP/MW/h (its own dictionary); delivery
  endpoints naive and kept as strings. Block orders: Price and ClearingPrice "GBP/MW as published",
  MAR dimensionless, negative volume is the sell convention. Order entry time is bid submission.
- Quoting: the vendor writes the empty `Location`, `Cancelled` (by-unit results) and `Bid Validity`
  (block orders) cells as a quoted empty string `""`, which the CSV reader keeps as `""` exactly as
  published (the K-TRD `Notes` precedent); an unquoted blank cell is null. The record maps no token.

## Hold questions (verbatim in the registry)

- `dynamic_containment_dc_dr_dm_results_sum`: DC-SUMMARY-PRICE-UNIT (the dictionary describes Clearing
  Price as a price per MW per hour but gives its unit as GBP/MW; the by-unit table states GBP/MW/h).
- `dynamic_containment_dc_dr_dm_linear`: DC-LINEAR-PRICE-UNITS (the nP price points carry conflicting
  unit declarations).

## Non-holding TODOs

- AGGREGATED-BSAD-2015-16: the vendor catalogue lists no 2015-16 aggregated resource; A02 covers only
  2014-04-01 to 2014-06-02 despite its FY label.
- FORWARD-BSAD-SIGN-CONVENTION and FORWARD-BSA-MINUS-ONE: no sign semantic is published; the one `-1`
  in BSA (F09, 2025-01-08 P39) is kept as `-1.0`.
- DISAGGREGATED-BSAD-ACTION-ID (no stable action identity: every body D01-D14 repeats identical
  complete rows, so the family has no record), DISAGGREGATED-BSAD-COST-SIGN, the D08 encoding, and the
  D-H2 date-format map for a future record (D10 `%Y-%m-%dT%H:%M:%S`).
- DC-MASTER-ENDPOINT-CLOSURE, DC-REJECTION-CODE-5 (code 5 kept as text), DC-RESULT-EXPORT-CLOCK,
  DC-ORDER-EXPORT-CLOCK, DC-BLOCK-PRICE-HOUR-BASIS (block prices stay "GBP/MW as published", never
  equated with the results table's GBP/MW/h).
- Legacy DC results are auction results, not requirement forecasts: never join or sum them with the AS-1
  forecast families (561 date labels overlap the forecast history).

## Full-data facts that no fixture can carry

A scratch transform (data root `C:/gridflow-wt/_scratch/e1`, a copy of the seven families' bronze)
writes 166,944 aggregated, 149,424 forward, 10,388 master, 23,424 summary, 397,662 results, 23,154
linear and 728,794 block rows (1,499,790 in all) with zero exclusions; the five adjudicated
captures fail exactly as the ledger says (A01 and A02 `UnicodeDecodeError`, A07, F01 and F05
`DuplicateEntityKeyError`). The reader's removal of 20 wholly blank rows (A03-A06) and 15 (F02-F04)
is not an exclusion.
