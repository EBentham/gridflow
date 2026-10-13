# Provenance: EAC live auction results fixtures (v0.22-K-AS-3a)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line (CRLF terminators on `l2`,
`l3`, `l7`, `l12` and `l32`, LF on the rest; no BOM on any body); only the line selection is ours. Bodies were read by streaming lines, never loaded whole (the
sell bodies are 100-440 MB). `.gitattributes` marks the fixtures `-text`, so git stores the bytes
unchanged. The "sha8" is the first eight hex digits of the original body's SHA-256, the suffix of the
bronze file name. "Lines" are 1-based line numbers of the original body (1 is the header). Nothing is
repaired or re-decoded. `datastore` resources have no vendor file name (the sidecar carries the
resource id).

The package `eac-auction-results` (id `291e3c28-75f2-4a8f-b5f5-008bebaac368`) declares that its
datetime values are UTC; that declaration is the only evidence for the `zone: "UTC"` columns
(deliveryStart, deliveryEnd, orderEntryTime). It declares the daily resources hold the latest auction
only. The fixtures are named by the FACTS capture alias (`l1` is capture L1 of `K-AS-3-FACTS.md`).

| Fixture | Family | Type | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---|---:|---|
| `l1.csv` | `eac_results_by_unit` | datastore | `a63ab354-7e68-44c2-ad96-c6f920c30e85` | (none) | `4220afd5` | 1,444,630 | 1-4, 30 (BST-season `deliveryStart` `2026-03-31T22:00:00`), 70 (blank `technologyType` and `postCode`) |
| `l2.csv` | `eac_results_by_unit_fy` | upload | `7505ec16-e1e7-432e-8abd-a554e03e4f02` | `neso-response-reserve-results-by-unit-fy2023-archive.csv` | `d1dc758d` | 161,252 | 1-4 |
| `l3.csv` | `eac_results_by_unit_fy` | upload | `2fc9dd7e-5274-40e3-abf4-6d31b30f21c6` | `neso-response-reserve-results-by-unit-fy2024-archive.csv` | `7072d473` | 563,041 | 1-4 |
| `l5.csv` | `eac_results_daily_by_unit` | upload | `07f0dd8b-4471-4e2e-9ed6-d21b0ce1f6f5` | `neso-response-reserve-daily-results-by-unit.csv` | `545a88bc` | 7,925 | 1-4 |
| `l6.csv` | `eac_results_buy_orders` | datastore | `1cf68f59-8eb8-4f1d-bccf-11b5a47b24e5` | (none) | `b19b3c03` | 139,306 | 1-4, 61819 (auction `2758` order `2405348`, `acceptanceRatio` `1.049999949856768`) |
| `l7.csv` | `eac_results_buy_orders_fy` | upload | `7a2d64a5-3233-4ee2-92fd-240db03717f1` | `neso-response-reserve-buy-orders-fy2023-archive.csv` | `462a9d28` | 27,900 | 1-3, 1576 (`clearingPrice` `-1.0`) |
| `l10.csv` | `eac_results_daily_buy_orders` | upload | `e638cf3d-bed1-42ae-a984-8fe3c556febf` | `neso-response-reserve-daily-buy-orders.csv` | `0f509b0e` | 720 | 1-4 |
| `l11.csv` | `eac_results_summary` | datastore | `596f29ac-0387-4ba4-a6d3-95c243140707` | (none) | `ad2b0004` | 61,884 | 1-3 (auction `2661` product `DRH` at two `deliveryStart` values) |
| `l12.csv` | `eac_results_summary_fy` | upload | `be5c6b0d-a335-4859-93f2-389585b4e9a1` | `neso-response-reserve-results-summary-fy2023-archive.csv` | `2b5fd006` | 5,400 | 1-4 |
| `l15.csv` | `eac_results_daily_summary` | upload | `3c51a666-1c33-450e-a6eb-c9b4a0c91584` | `neso-response-reserve-daily-results-summary.csv` | `2a238a9f` | 324 | 1-4 |
| `l16.csv` | `eac_results_sell_orders` | datastore | `13b511df-d6ec-4143-afb1-0ecc6fd19810` | (none) | `1e19443b` | 588,221 | 1-4 (H-O22, `T` separator) |
| `l32.csv` | `eac_results_sell_orders` | upload | `5997f602-24f1-4f44-aa36-c7b898229d60` | `neso-response-reserve-sell-orders-202502-archive.csv` | `3cb5cc35` | 828,362 | 1-4 (H-O20: no `flexibleGroupID`, `minimumConsecutiveWindows`) |
| `l49.csv` | `eac_results_sell_orders` | upload | `fc52a1fa-6ce5-4c35-a661-be1b49c1c48f` | `neso-response-reserve-sell-orders-202607-archive.csv` | `6a582732` | 2,291,298 | 1-4 (H-O22, space separator in every time cell) |
| `l52.csv` | `eac_results_daily_sell_orders` | upload | `8ba48f26-d73e-4094-a90d-ba075eb739c1` | `neso-response-reserve-daily-sell-orders.csv` | `ce6183fc` | 73,098 | 1-4 |

The sidecar facts the tests write (package slug and id, resource name, vendor file name,
`url_type`, `ckan_last_modified`, the `written_at` capture time) are the originals', in
`test_neso_as3a_records.py::CAPTURES`.

Full-data facts that no fixture can carry, measured by the research (FACTS g1-g6) and re-checked
here by reading only the first line of each captured body:

- All 36 `eac_results_sell_orders` bodies have a first line equal to H-O22 (31 bodies) or H-O20
  (5: L29-L33, archives `202411`-`202503`); every other captured body of the package has exactly
  its family header. No body carries a BOM.
- Archive `202607` (L49) alone writes its delivery and submission cells with a space separator
  (`2026-06-30 22:00:00`); the other 35 sell bodies use `T`. One scalar `format` cannot read both and
  `formats_by_filename` is `date`-only (ADR-039), so every sell time column is a **raw string** in
  both epochs and the sell temporal recipe is `none` (`timestamp_utc` is the capture time). The
  values are documented UTC; a typed sell clock waits for a per-filename datetime format.

Record notes that have no field in the frozen record (kept here and in the test docstrings; none is
a hold):

- Current, FY and daily families stay separate records. Measured keys are disjoint between current
  and FY; each daily family repeats the latest-auction slice of its current family. A consumer must
  never sum a daily family with its current counterpart.
- TODO: EAC-BUY-ACCEPTANCE-RANGE (the current buy-order row auction `2758` / order `2405348` has
  `acceptanceRatio` `1.049999949856768`; the dictionary gives a dimensionless fraction with no stated
  upper bound; the value is kept, never clamped).
- TODO: EAC-ORDER-REVISION-IDENTITY (whether an order id is reissued when an order is revised is
  unmeasured; no `key_latest` is used).
- TODO: INTERVAL-CLOSURE (no vendor statement whether the delivery interval is closed or open at its
  end; the endpoints are kept as given).
- TODO: the original publication instants of the auction results are unknown (settled only by a
  resource's publication history or successive captures); `issue: none` everywhere and the vintage is
  the CKAN last-modified (upload-only families) or the capture time (families holding a datastore
  resource).
- Units: volumes MW; prices GBP/MW/h (not the SFFR GBP/MWh); `acceptanceRatio` a dimensionless
  fraction. The vendor's price columns keep negative values and zeros as they are; the research found
  no `NA` token, no `-1` sentinel, no thousands separator, no `%` and no padded numeric in any body.
