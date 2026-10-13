# Provenance: EAC-BR archive results and EAC-BR mock fixtures (v0.22-K-AS-4b)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line; only the line selection is ours.
Bodies were read by streaming lines, never loaded whole (the by-unit archive is 97 MB). All seven
bodies end every line with a lone LF and carry no BOM; `.gitattributes` marks the fixtures `-text`, so
git stores the bytes unchanged. The "sha8" is the first eight hex digits of the original body's
SHA-256, the suffix of the bronze file name. "Lines" are 1-based line numbers of the original body (1
is the header). `datastore` resources have no vendor file name (the sidecar carries the resource id).
Nothing is repaired or re-decoded. The fixtures are named by the FACTS capture alias (`b01` is capture
B01 of `K-AS-4-FACTS.md`, `m03` is M03).

| Fixture | Family | Package | Type | Resource id | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---|---:|---|
| `b01.csv` | `eac_br_results_by_unit` | `eac-br-auction-results` | datastore | `5d8e47be-e262-4398-89b0-6f93f636faf6` | `abc44247` | 621,111 | 1-3, 12 (quoted-empty `postCode`), 5113 (quoted-empty `technologyType` and `postCode`), 17664 (BST-season date 2024-04-01) |
| `b02.csv` | `eac_br_results_buy_orders` | `eac-br-auction-results` | datastore | `8a549d29-52cd-4016-a04e-c01a3e31bd3a` | `188f3ebd` | 98,592 | 1-3 (fractional `acceptanceRatio`, blank and coded `reasonRejected`) |
| `b03.csv` | `eac_br_results_summary` | `eac-br-auction-results` | datastore | `1b3f2ee1-74a0-4939-a5a3-f01f19e663e4` | `dcca5345` | 57,216 | 1-3 (auction 152 / product PBR at two `deliveryStart` values) |
| `m01.csv` | `eac_br_mock_results_by_unit` | `eac-br-mock-auction-results` | datastore | `7f5a3d87-a3fb-4c5b-88f0-ecba2401834d` | `a853c006` | 7,074 | 1-3 (unquoted blank `technologyType` and `postCode`), 6916 (June 2024) |
| `m02.csv` | `eac_br_mock_results_buy_orders` | `eac-br-mock-auction-results` | datastore | `e3ad918b-f10b-49c7-93ba-b7e81fe6feea` | `dfe605e1` | 3,696 | 1-3, 98, 963 (blank `joinedFamily` on 2-3 and 98, populated `joinedFamily` `1` on 963) |
| `m03.csv` | `eac_br_mock_results_sell_orders` | `eac-br-mock-auction-results` | datastore | `b73e34ca-28fc-4fb2-9275-d5f546d6f33a` | `81cd30d3` | 23,263 | 1-3, 11, 12, 3257, 3893 (blank and populated `loopedBasketID`, `PARENT` / `CHILD` / `SUBSTITUTABLECHILD`, `productID` `11`, blank and coded `reasonRejected`) |
| `m04.csv` | `eac_br_mock_results_summary` | `eac-br-mock-auction-results` | datastore | `a285483f-d830-4ae2-8b6d-98c05f6d21f3` | `0fffe098` | 1,632 | 1-3 |

The BR sell orders (`eac_br_results_sell_orders`, resource `3f53c38b-d287-41af-b4c8-769abb6e707a`)
have **no bronze body**: the 2026-10-08 sweep aborted its datastore dump at the 536,870,912 B cap.
`LIVE-PROBE.md` P1 (2026-10-13) recorded the live row total (3,124,058), the 20-field header and an
estimated body of 561,174,421 B. **`m03.csv` is mock bytes**: it is used only because its first line is
byte-equal to the P1 header, so the live BR sell record can be exercised on real vendor bytes with that
header. It is not BR live evidence, and the sell-order tests that load it under the live BR record
say so.

Evidence for the freeze (K-AS-4-FACTS sections 7 and 8, LIVE-PROBE P1):

- Clock: neither the BR archive package nor its mock package states the clock of the naive
  `deliveryStart`, `deliveryEnd` and `orderEntryTime` cells; the live EAC package's UTC declaration does
  not transfer. Every BR and mock time column is therefore a raw `string`, temporal `none`.
- Units from the resource dictionaries: `quantity`, `executedQuantity`, `clearedVolume` MW; `price`,
  `priceLimit`, `clearingPrice` GBP/MW/h; `acceptanceRatio` a dimensionless proportion.
- B01 writes 6,492 quoted-empty `""` cells (in `technologyType` and `postCode`); the CSV reader keeps
  a quoted empty string as the empty string exactly as published (the K-TRD / K-AS-4a precedent), while
  an unquoted blank (every mock body) is null.

Hold questions (verbatim in the registry):

- `TODO: EAC-BR-MOCK-TEST-DATA` (package `eac-br-mock-auction-results`, held): the mock tables are
  outcomes of mock runs, not live procurement.
- `TODO: BR-MOCK-EXPORT-CLOCK` (the four mock families, held): the mock export clock is undocumented
  and the captured dates (February-September 2024) span GMT and BST.

Non-holding TODOs (no field in the frozen record):

- `TODO: BR-ARCHIVE-EXPORT-CLOCK`: the clock of the BR archive time cells is undocumented; they stay
  raw strings, and any page built from these tables must say the clock is unknown. Endpoint
  inclusivity is also undocumented.
- `TODO: BR-MOCK-NOTICE-SCOPE`: the mock rows after the notice's February window are kept, never
  clipped.
- `TODO: POST-CAPTURE-BR-SELL`: the real capture must pass the FACTS 7.4 checks (cap, header, strict
  UTF-8, parser faithfulness, key uniqueness, casts, time lexemes) before the sell orders are
  transformed.
- `TODO: BR-SELL-ARCHIVE-OVERLAP`: the overlap of the BR sell orders with the EAC archive sell
  orders is measured after the capture.
- BR and the EAC archive are measured disjoint on auction ids (596 against 1,071, none in common) and on
  entity keys for the three captured tables; they stay separate families.
