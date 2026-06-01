# Remediation Backlog — Vendor-Truth Audit (v-next candidate milestone)

**Source:** `vendor-truth-audit-2026-05-31` · 88 findings across 13 units (see [`findings/`](findings/) + [`findings/_ALL-FINDINGS.json`](findings/_ALL-FINDINGS.json)).
**Intended consumer:** GSD `research → plan → execute → code-review → ship`. Feed via `gsd-new-milestone` or `gsd-ingest-docs`.

## Core Value
Every silver row that reaches disk is schema-valid (or its failure is surfaced), and every vault/doc statement that the `gridflow-front-end` renders is true against the official vendor docs and the live code. Live-API correctness is already verified healthy (see [SUMMARY.md](SUMMARY.md)); this milestone closes the *write-path guarantees* and *documentation truth* gaps.

Only **upheld / well-evidenced** findings become requirements below. 3 refuted findings and 2 intentional-by-design items are listed at the end as **no-action**.

---

## P0 — Correctness / silent-bug class (HIGH)

- [ ] **VTA-SCHEMA-01** (HIGH, project-wide) — *Enforce Pydantic schemas in the silver write path.*
  Today `BaseSilverTransformer.run()` writes silver without `model_validate`; some transformers validate only `df.row(0)` and discard it; `day_ahead_prices` references no schema at all. Documented constraints never execute on live data.
  **Acceptance:** `run()` (or each `transform`) validates the **full** frame; validation failures are **logged, counted, and surfaced** as `completed_with_warnings` (per the CLAUDE.md hard rule — never silently dropped); `day_ahead_prices` applies `EntsoeDayAheadPrice`; policy is uniform across all vendors; a test proves a bad non-first row is caught.
  **Findings:** ELEXA-03, ELEXB-01, ENLG-01, OM-01, ENPRICE-01, GIE-04.

- [ ] **VTA-ELEXON-MKTDEPTH-01** (HIGH) — *Fix `market_depth` field mapping.*
  Maps non-existent `totalAdjustmentSell/BuyVolume`; drops real `pricedAcceptedOffers/BidsVolume`.
  **Acceptance:** drop the two phantom fields (and their schema fields); add `priced_accepted_offers_volume_mwh` / `priced_accepted_bids_volume_mwh` from the live keys; add a real `market_depth` fixture so the contract test guards the mapping; a live fetch confirms the silver columns.
  **Findings:** ELEXA-02.

- [ ] **VTA-GIE-DTRS-01** (HIGH, **live-verified**) — *Stop labelling ALSI `dtrs` as a percentage.*
  ALSI silver maps vendor `dtrs` → the percent-named/percent-documented `dtrs_pct_full`. **Live-verified** (LIVE-API-REPORT §L5): live ALSI returns `dtrs` (Belgium 724.1, Italy 889.8, Spain 2132.3 — a send-out reference capacity in GWh/d, **not** a %) and `dtmi`=`{lng,gwh}`, and has **no `full` field** (the `full` field exists only in a stale fixture).
  **Acceptance:** rename `dtrs` to an honest send-out-capacity column (units confirmed against **official ALSI docs** first — do NOT assume "Day Tank Recirculation Storage"); capture `dtmi.{lng,gwh}`; if a %-full metric is wanted, **derive** it (`gas_in_storage / dtmi.gwh × 100`), do NOT map a non-existent `full`; update schema + `lng.md` + `docs/endpoints/gie.md`; refresh the stale ALSI fixture; live ALSI no longer yields a >100 "percentage".
  **Findings:** GIE-01 (live-verified TF) + GIE-02 (fabricated definition) + GIE-04 (unclamped).

- [ ] **VTA-ENTSOE-PSR-01** (HIGH) — *Correct ENTSO-E PSR code labels + set a code/label policy.*
  `wind_solar_forecast` code annotations mislabel renewable PSR codes vs the official list.
  **Acceptance:** all PSR annotations/docstrings (and any vault PSR labels) match official **B16 Solar / B18 Wind Offshore / B19 Wind Onshore**; decide raw-code vs mapped-label representation and document the code→meaning map in the vault (none exists in `src/`). Cross-check the `vendor-docs/entsoe-codes.md` cache.
  **Findings:** ENLG-02, F-ENTSOE-PSR-RAW.

- [ ] **VTA-ENTSOG-TARIFF-01** (HIGH) — *Fix `tariffs` / `tariff_simulations` date-window `read_bronze`.*
  Date-window handling in `read_bronze` for these monthly datasets is wrong.
  **Acceptance:** `read_bronze` selects the correct partition(s) for date-window datasets; a live ENTSO-G `tariffs` ingest+transform yields the expected non-empty silver; covered by a test.
  **Findings:** ENGREF-01.

## P1 — Vault/doc accuracy (MEDIUM) — drives `gridflow-front-end` correctness

- [ ] **VTA-VAULT-ARCHIVE-01** (MED) — Archive stale vault page `entsoe/datasets/commercial_schedules_net_positions.md` (removed per ADR-019); sweep for other ADR-superseded pages. **Findings:** PF-001.
- [ ] **VTA-VAULT-ENUM-01** (MED) — Update vault enum-handling claims to ADR-022 behaviour: `replace_strict` no longer raises on unknown codes; an `'unmapped'` value is possible in `direction`/`reserve_type` and surfaces `completed_with_warnings`. Pages: `imbalance_prices`, `activated_balancing_prices`, `imbalance_volume`. **Findings:** ENPRICE-02.
- [ ] **VTA-DOCS-INTERNAL-01** (MED) — Fix internal docs: `docs/endpoints/entsoe.md` (`area_code` is the raw EIC mRID, not a human label; `cross_border_flows` is A11 not A88); refresh the stale dataset list in `docs/CLI_CHEAT_SHEET.md`. **Findings:** ENPRICE-03, PF-003.
- [ ] **VTA-GIE-DOCS-01** (MED) — Remove the unsourced "Day Tank Recirculation Storage" definition in `docs/endpoints/gie.md`; correct `lng.md` source-field column; update stale ALSI `total/pageSize` pagination wording (code uses `last_page`). **Findings:** GIE-02, GIE-03, GIE-06.
- [ ] **VTA-ENTSOG-UNITLABEL-01** (MED) — Resolve the contradictory `unit` column (`"kWh/d"`) alongside the converted `flow_gwh_per_day` (GWh/day) in ENTSO-G silver. **Findings:** F-ENTSOG-UNITLABEL.
- [ ] **VTA-NESO-COVERAGE-01** (MED) — Reconcile NESO coverage: config has 37 datasets, vault has 33 pages; add missing pages or document intentional omissions. **Findings:** S-NESO MEDIUMs.
- [ ] **VTA-MISC-MED-01** (MED) — Remaining per-unit MEDIUM vault/code/doc drifts not separately enumerated here. **Findings:** see `findings/{S-ENTSOE-OUTAGES,S-ENTSOE-BALANCING,S-ENTSOG-OPDATA,S-ENTSOG-CMP,S-OPENMETEO,S-ELEXON-B}.md`.
- [ ] **VTA-FRONTEND-SYNC-01** (MED, **prerequisite**) — Confirm how `gridflow-front-end` sources vault content. It holds its **own vendored `vault/` directory**; the P2 vault fixes only reach the live site if that copy is re-synced from canonical `quant-vault` (or rebuilt). **Acceptance:** the frontend's content source + sync mechanism is documented, and every VTA-VAULT-* / VTA-*-DOCS fix has a defined propagation path to the rendered site (resync, submodule, or build step). **Do this before/with the vault edits** so the user's end goal (correct frontend) is actually achieved.

## P2 — Low / informational cleanup (batch)

- [ ] **VTA-CLEANUP-01** (LOW/INFO) — Currency-label asymmetry (only `day_ahead_prices` carries `currency`; imbalance/activated-balancing store GBP into `price_eur_mwh` unlabeled — ENPRICE-04); imbalance direction-encoding join note; assorted doc nits. **Findings:** the 28 LOW + 24 INFO items in `findings/_ALL-FINDINGS.json` (filter `severity in {LOW,INFO}`).

---

## No-action (recorded so remediation does not re-raise)

**Refuted by adversarial verify** (claim wrong/overstated against the primary source):
- ELEXA-01 (DISBSAD `storFlag` drop), ENLG-03 (vault PSR labels), OM-02 (Open-Meteo schema field names).

**Resolved as intentional & documented** (not bugs):
- **F-ENTSOE-GB-ABSENT** — GB is queried but empty by design on ENTSO-E (ack 999, post-Brexit; GB via Elexon). `10Y1001A1001A59C` = Ireland (SEM).
- **PF-002 `activated_balancing_qty`** — deliberately deferred (GB rejects A83); vault documents it accurately.

## Traceability

| Requirement | Severity | Key findings | Suggested first step |
|---|---|---|---|
| VTA-SCHEMA-01 | HIGH | ELEXA-03, ELEXB-01, ENLG-01, OM-01, ENPRICE-01, GIE-04 | research: full-frame validation pattern (perf vs per-row) |
| VTA-ELEXON-MKTDEPTH-01 | HIGH | ELEXA-02 | live-fetch market-depth, capture fixture |
| VTA-GIE-DTRS-01 | HIGH | GIE-01, GIE-02, GIE-04 | obtain official ALSI field defs for dtrs/dtmi |
| VTA-ENTSOE-PSR-01 | HIGH | ENLG-02 | adopt `vendor-docs/entsoe-codes.md` as the label source |
| VTA-ENTSOG-TARIFF-01 | HIGH | ENGREF-01 | reproduce empty tariffs locally, fix read_bronze |
| VTA-VAULT-* / VTA-DOCS-* / VTA-GIE-DOCS-01 | MED | PF-001, ENPRICE-02/03, PF-003, GIE-02/03/06 | vault/doc edits (no code) |
| VTA-ENTSOG-UNITLABEL-01 / VTA-NESO-COVERAGE-01 / VTA-MISC-MED-01 | MED | F-ENTSOG-UNITLABEL, S-NESO, per-unit | per-unit |
| VTA-CLEANUP-01 | LOW/INFO | _ALL-FINDINGS.json | batch |

## Out of scope / sequencing notes
- **ENTSO-G CMP** (`cmp_*`) findings are low-confidence/read-only — another agent is actively reworking CMP (`fix/entsog-cmp-schema-drift`, ADR-021). **Do not action S-ENTSOG-CMP until that branch merges**, then re-audit.
- Gold-layer correctness and new-dataset coverage were **not** in scope (P1 live + P2 doc-truth only).
- Live-API correctness needs no remediation (verified healthy).
