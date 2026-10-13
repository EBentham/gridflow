# Provenance: mock EAC results, ORPS utilisation and OBP fixtures (v0.22-K-AS-3b)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line (CRLF terminators on `r1`-`r4`, LF
on the rest; a UTF-8 BOM on `r1`, `r3` and `r4` only); only the line selection is ours. Bodies were
read by streaming lines, never loaded whole. `.gitattributes` marks the fixtures `-text`, so git
stores the bytes unchanged. The "sha8" is the first eight hex digits of the original body's SHA-256,
the suffix of the bronze file name. "Lines" are 1-based line numbers of the original body (1 is the
header). Nothing is repaired or re-decoded. `datastore` resources have no vendor file name (the
sidecar carries the resource id) and an empty `ckan_last_modified`. The fixtures are named by the
FACTS capture alias (`m1` is capture M1, `r1` is R1, ... of `K-AS-3-FACTS.md`).

| Fixture | Family | Package | Type | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---|---|---:|---|
| `m1.csv` | `eac_mock_results_by_unit` | `eac-mock-auction-results` | datastore | `955cf89f-232e-4b9c-8414-6351080f49fa` | (none) | `14a76598` | 13,073 | 1-4 |
| `m2.csv` | `eac_mock_results_buy_orders` | `eac-mock-auction-results` | datastore | `700a73ff-38e9-45ff-a674-085bcbfc9faf` | (none) | `87022505` | 16,938 | 1-4 |
| `m3.csv` | `eac_mock_results_summary` | `eac-mock-auction-results` | datastore | `454216d8-61f3-40b6-b787-4dff035ce3c3` | (none) | `264795fe` | 7,128 | 1-4 |
| `m4.csv` | `eac_mock_results_sell_orders` | `eac-mock-auction-results` | datastore | `3b18345a-1933-44d7-abd9-062a2e695ccd` | (none) | `d5d8f6e0` | 30,869 | 1-4 |
| `r1.csv` | `orps_utilisation_reactive` | `obligatory-reactive-power-service-orps-utilisation` | upload | `a61c6c26-62ec-41e1-ae25-ed95f4562274` | `reactive-utilisation-data-apr-2020-dec-2024.csv` | `b9e76522` | 243 | 1-2, 38 (unit `KILW_01Z`, blank first monthly cell) |
| `r2.csv` | `orps_utilisation_reactive` | same | upload | `528b03d2-4008-4b65-a732-a001cd835a3b` | `reactive-utilisation-data.csv` | `e6a485d3` | 19,130 | 1-3 (the held long table's header; used only to prove it matches no epoch and is not read) |
| `r3.csv` | `orps_utilisation_historic_reactive` | same | upload | `3ee65326-3729-421d-b667-b2164501e4a5` | `reactive-historical-data-apr-2013-to-mar-2020.csv` | `af98c88f` | 295 | 1, 3 (comma numeral `1,120`, unit `KLYNA01Z`), 14 (padded dash ` -   `, unit `CORB_01Z`), 181 and 277 (the two `SPLN,SPLN_01Z,D,M` rows) |
| `r4.csv` | `orps_utilisation_reactive_default` | same | upload | `b00df5b1-4179-44e3-b556-28a836ad619f` | `reactive-default-payment-rate-oct-2026.csv` | `2830307a` | 228 | 1-3 |
| `p1.csv` | `obp_physical_notifications` | `obp-non-bm-physical-notifications` | upload | `dfe1d229-e973-43c6-b5eb-c28ad59d7136` | `pn_20261008_27_nbm.csv` | `8f6a94d9` | 495,086 | 1-2, 6 (60-second segment), 244324 (BST-season `Time From` `2026-07-01T05:00:00`) |
| `i1.csv` | `obp_reserve_instructions` | `obp-non-bm-reserve-instructions` | upload | `dad63c1b-8a8e-47de-9922-7880384ef104` | `instr_20261002064426_qr_sr_nbm.csv` | `87489f2e` | 422 | 1, 16-19 (instruction `3192267`: START, VP, STOP, VP) |
| `a1.csv` | `obp_reserve_avail_price` | `obp-reserve-availability-utilisation-price` | upload | `6bfe7df0-60aa-462d-94cd-44ac0a4abb2c` | `av_20261008_27_qr_sr_nbm.csv` | `70723223` | 150,309 | 1-3 (row 3 `Pair ID` `-1`), 11783 (price `10000.0`), 11784 (price `-10000.0`) |

The sidecar facts the tests write (package slug and id, resource name, vendor file name, `url_type`,
`ckan_last_modified`, the `written_at` capture time) are the originals', in
`test_neso_as3b_records.py::CAPTURES`.

Full-data facts that no fixture can carry (a scratch transform of the ten recorded families from copies
of the swept bronze bodies, RULINGS 711, wrote **714,591 rows with zero exclusions**: 13,073 +
16,938 + 7,128 + 30,869 mock; 295 + 243 + 228 ORPS; 495,086 + 422 + 150,309 OBP). The held long table
R2 (19,130 rows) is not in silver; its capture beside R1 neither fails nor writes.

## Mock EAC (package `eac-mock-auction-results`, id `8a6f23d7-8489-49ac-9b0a-566fb53d32f8`)

- Test data. NESO describes the resources as outcomes of mock auction runs. The package is held
  (`TODO: EAC-MOCK-TEST-DATA`) so the rows stay unpublished until a test/mock label with separate
  provenance exists, and every populated mock family is additionally held on the clock
  (`TODO: EAC-MOCK-CLOCK`): the mock package and its dictionaries do not state the clock of the naive
  `deliveryStart` / `deliveryEnd` / `orderEntryTime` cells, the live package's UTC declaration does not
  extend to it, and every captured date (delivery labels `2026-02-23T23:00:00` ... `2026-03-17T22:30:00`)
  falls in GMT. All time cells therefore stay raw strings and the temporal recipe is `none`.
- Mock auction ids 1-22 are reused for different dates; the mock and live summary keys do not
  intersect. Never join mock to live on auction id.
- Units from the mock resources' own dictionaries: volumes MW, prices GBP/MW/h, matching ratio a
  dimensionless fraction.
- Header-only daily mock resources (no bronze; LIVE-PROBE P1, `2026-10-13T03:20:30Z`: HTTP 200, a
  single header line, `datastore_search` total 0, last modified `2026-06-04`): by_unit
  `6a051ca2-715b-4966-9c16-58e52254f10b`, buy_orders `f994f9bb-a2f3-48ff-a776-0b343806d978`, summary
  `f67a53d7-d46d-4550-8947-0c60328b2347`, sell_orders `c4d0341d-806d-4e53-9ae8-2c54df740ba3`.
  `empty_allowed: true`, no record. The probe's sell header ends in the singular
  `minimumConsecutiveWindow`; the populated table says `minimumConsecutiveWindows`, so the populated
  header is not frozen for the empty resource.

## ORPS (package `obligatory-reactive-power-service-orps-utilisation`, id `7e142b03-8650-4f46-8420-7ce1e84e1e5b`)

- Utilisation unit MVArh per balancing mechanism unit per month (the package text). The historical
  dictionary labels the columns `MVAr`; the conflict is recorded, not resolved. The historic table
  covers `Oct-2013` ... `Mar-2020` (not April 2013 as the title says); reactive R1 `Apr-20` ...
  `Dec-24`; the long table R2 `Apr-20` ... `Aug-26`. Leading means absorption, lagging
  generation/export; `D` is ORPS/default procurement and `M` the Enhanced Reactive Power Service
  market.
- Historic R3 (held `ORPS-HIST-DASH` / `ORPS-HIST-IDENTITY`): 20,950 comma-bearing value cells, 11,418
  blanks, 32 padded dashes. Every value column is a string; no trim, token map or lexical-number
  mechanism. The identity `SPLN, SPLN_01Z, D, M` repeats (rows 181 and 277) with 234 different values;
  the entity key is all 238 silver columns, a duplicate guard and not a vendor identity.
- Reactive R1 (held `ORPS-REACTIVE-ARCHIVE`): `id` is opaque (vendor role open), 171 monthly
  LEAD / LAG / TOTAL float64 columns, 2,895 blanks. The long table R2 restates every R1 unit/month
  (13,851 keys shared) with 241 different values and 2,895 R1 blanks published as 0, and repeats the
  complete row `SKWF,SAKNW-1,D,SC,May-25,0,0,0`: no key passes the duplicate guard (RULINGS 538), so R2
  is a resource-level `HOLD`, not a dedup, ordinal or ADR-040 adjudication.
- Default R4 (held `ORPS-PRICE-INPUT-UNITS`): `Year` / `Month` are reserved Hive names, so the silver
  names are `year_vendor` / `month_vendor`. `X = 1` and `X = 0.2` are GBP/MVArh rates; FRPIm is a
  dimensionless RPI index level and Im a dimensionless indexation factor; HPIm, PAPIm and PPIm carry
  only the dictionary's "GBP" and the guidance does not state their denominator.
- Workbook X1 (`d91e4fd2-1f27-4d0b-8473-b4b19af7f3dc`) and its 33 children stay `HOLD`
  (`ORPS-WORKBOOK-LAYOUT`); the two PDFs stay `DOC`. No workbook family exists.

## OBP (three eligible packages)

- Packages `obp-non-bm-physical-notifications` (`73edf630-fe9e-45a6-a2be-fa0d31a8ec1d`),
  `obp-non-bm-reserve-instructions` (`0ba217bb-846b-470e-9717-ff152d97d009`),
  `obp-reserve-availability-utilisation-price` (`769ec53a-9940-4df3-a867-6fa62d483097`). The vendor
  dictionaries state `Time From`, `Time To`, `Point Datetime` and `Issue Datetime` are UTC; that
  statement is the only evidence for `zone: "UTC"` (no `zone_evidence`).
- They are gate-closure declaration / instruction event tables, not forecasts. The CKAN last-modified
  vintage is conservative for every row (no row is published earlier than true availability); 150 PN
  and 72 availability window starts after the last-modified instant are declared positions.
- PN segments are not all half-hours (down to 60 seconds); every endpoint is kept. Levels MW.
- Instruction `Issue Datetime` is blank on 259 rows (168 VP, 88 SPP, 3 STOP): a nullable data column,
  never an issue recipe, never propagated. `Target MW` is power relative to PN, `Target Price`
  indicative GBP/MWh, `Instruction Point ID` the vendor-documented unique reporting key.
- `Pair ID` `-1` is a negative-service band (valid); `Utilisation Price` +-10000 GBP/MWh are kept, not
  sentinels.

Record notes that have no field in the frozen record (kept here and in the test docstrings; none is a
hold):

- TODO: OBP-INSTRUCTION-IMMUTABILITY (no repeated same-resource capture proves whether reported
  points or statuses are amended or removed; whole-capture selection with the conservative vintage).
- TODO: cross-capture identity of the PN and availability rows (no vendor guarantee found; whole
  capture, no `key_latest`).
- TODO: INTERVAL-CLOSURE (no vendor statement whether the OBP windows are closed or open at their
  end; the endpoints are kept as given).
- Each OBP body spans about the 12 months before its last-modified instant, so under whole-capture
  selection older history lives only in older captures.
