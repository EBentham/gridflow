# Vendor-Truth Audit — Executive Summary

**Run:** `vendor-truth-audit-2026-05-31` · autonomous, started 2026-05-31, completed overnight into 2026-06-01.
**Scope:** reconcile **official vendor docs ⟷ gridflow code ⟷ Obsidian vault** (`quant-vault/30-vendors/`, which feeds `gridflow-front-end`), prioritising **live-API correctness**. Code snapshot `ca652c3` (master). **No code was modified** — this is an audit; fixes are the downstream GSD job.

> **Run-completeness note (read first):** this run depended on a self-paced loop while the user slept; the user later lifted throttling, so the remaining work was run as two concurrent Workflows. **All planned ticks completed** — T0 (scaffold+P1 smoke), T1 (P1 deep probes), T2 (cache+GIE+prices), T3 (11-unit batch), TF (this synthesis). See [STATE.json](STATE.json) `ticks[]`. Sparse artifacts would have meant an early loop death; that did not happen.

---

## Priority 1 — Live API correctness: **HEALTHY** ✅

Verified by running the **real** `gridflow ingest`+`transform` against live vendor APIs into an isolated temp lake (full "when I run it" fidelity, zero blast radius). Evidence: [LIVE-API-REPORT.md](LIVE-API-REPORT.md).

- **All 6 vendors reachable; both API keys valid** (ENTSO-E `securityToken`, GIE `x-key` — no 401s).
- **No silent duplication** — every shared-endpoint family that returns data is correctly differentiated (ENTSO-E `businessType`/`processType`: A25 B09/B07, A26 B08/A29, A65 A16/A01/A31/A32/A33; all 8 sampled ENTSO-G `/operationalData` indicators distinct). This was the user's top fear; it is not happening.
- **Renewable/forecast completeness confirmed** — solar GTI + all irradiance components, wind at every hub height (10/80/100/120/180 m on forecast; correctly only 10/100 m on ERA5 archive), Elexon Solar/Wind PSR types. No all-null leakage.
- **Unit conversions correct** — ENTSO-G kWh/d→GWh/day verified numerically.
- **Two flagged P1 items resolved as intentional & documented:** GB *is* queried on ENTSO-E but returns empty by design (ack 999, post-Brexit; GB sourced from Elexon); `activated_balancing_qty` is deliberately deferred. (`10Y1001A1001A59C` = Ireland/SEM.)

**Bottom line:** when you run the pipeline, the data that returns is correct and complete. The real issues are in the *write-path guarantees* and *documentation accuracy* (below), not in corrupted live data.

---

## Priority 2 — Vault/code vs vendor docs: **88 findings** across 13 units

Severity: **HIGH 11** (8 upheld by adversarial verify, 3 refuted), **MEDIUM 25**, **LOW 28**, **INFO 24**. Per-unit detail in [`findings/<unit>.md`](findings/); machine-readable in [`findings/_ALL-FINDINGS.json`](findings/_ALL-FINDINGS.json). Prioritised remediation in [REMEDIATION-BACKLOG.md](REMEDIATION-BACKLOG.md).

### The headline finding (project-wide, silent-bug class)
**Pydantic schemas are declared but never enforced in the silver write path.** `BaseSilverTransformer.run()` goes `transform → _write_silver` with no `model_validate`. Confirmed independently for **Elexon, ENTSO-E, Open-Meteo, and GIE** (findings ELEXA-03, ELEXB-01, ENLG-01, OM-01, ENPRICE-01, GIE-04). Some transformers validate only `df.row(0)` and discard it; `day_ahead_prices` references no schema at all. The documented constraints (`settlement_period ≤ 50`, price clamps, `run_type` regex, `0..1` LOLP, GIE `pct_full` clamp) **do not run on live data**, and the vault advertises them as enforced. This directly violates the CLAUDE.md hard rule *"Pydantic validation failures are logged and surfaced — never silently dropped."* One fix (validate the full frame in `run()`, surfacing failures) closes it across all vendors.

### Other confirmed (UPHELD) data/correctness bugs
- **Elexon `market_depth`** maps two fields the API doesn't return (`totalAdjustmentSell/BuyVolume`) and **drops two it does** (`pricedAcceptedOffers/BidsVolume`) — ELEXA-02.
- **GIE ALSI `dtrs_pct_full`** mislabels the vendor `dtrs` field (a send-out reference capacity, GWh/d — live values 724.1/889.8/2132.3, all >100) as a percentage — GIE-01, **live-verified** ([LIVE-API-REPORT.md](LIVE-API-REPORT.md) §L5). *Correction:* live ALSI has no `full` field (that was a stale fixture); the fix is to relabel `dtrs` and derive any %-full from `gas_in_storage / dtmi.gwh`, not to map `full`.
- **ENTSO-E `wind_solar_forecast`** PSR code annotations are wrong/swapped vs the official list (B16 Solar, **B18 Wind Offshore, B19 Wind Onshore**) — ENLG-02.
- **ENTSO-G `tariffs`/`tariff_simulations`** date-window handling in `read_bronze` — ENGREF-01.

### Vault/doc accuracy (the frontend-correctness driver)
Many MEDIUM/LOW: stale vault page `commercial_schedules_net_positions` (removed per ADR-019 → archive); vault pages claiming `replace_strict` *raises* on unknown enum codes when ADR-022 changed it to an `'unmapped'` sentinel; internal `docs/endpoints/entsoe.md` claiming `area_code` is a human label (it's the raw EIC mRID) and `cross_border_flows` as A88 (code uses A11); GIE `docs/endpoints/gie.md` carrying an unsourced "Day Tank Recirculation Storage" definition; `CLI_CHEAT_SHEET.md` stale dataset list; NESO config (37) vs vault (33) coverage gap.

### Adversarial verify earned its keep
3 HIGH findings were **REFUTED** by the verify pass and should NOT be actioned as stated: ELEXA-01 (DISBSAD `storFlag`), ENLG-03 (vault PSR labels), OM-02 (Open-Meteo field names). They are recorded so the remediation phase doesn't re-raise them.

---

## Methodology (for reproducibility)
- **Family collapse:** ~165 datasets → ~20 audit units (shared-endpoint families). Probes: `probes/run_probe.py` (smoke+nulls), `probes/diff_probe.py` (shared-differentiator marker diffs). Renderer: `render_findings.py`.
- **Workflows:** 1 P1 fan-out equivalent + 3 P2 Workflows (T2 cache+GIE+prices; the 11-unit batch; a prices rerun after the first prices agent stalled on the absent cache). ~23 audit agents total + adversarial verify on HIGH.
- **Authority:** official docs > fixtures > code; vault treated as under-audit. ADRs/config-comments read first to avoid false-positives (intentional removals classified as `vault-stale`, not `missing-coverage`).

## Handoff
- **Next step:** run the GSD `research → plan → execute → code-review → ship` flow over [REMEDIATION-BACKLOG.md](REMEDIATION-BACKLOG.md). Start with **VTA-SCHEMA-01** (project-wide, highest leverage).
- **Git:** this run made **no commits** (another agent shares the repo via a separate worktree). To preserve the audit: `git checkout -b audit/vendor-truth-2026-05-31 && git add .planning/audit/2026-05-31-vendor-truth-audit && git commit -m "docs(audit): vendor-truth audit findings + remediation backlog"`.
- **Note:** ENTSO-G **CMP** datasets were audited read-only/light — another agent is actively changing them (`fix/entsog-cmp-schema-drift`, ADR-021). Reconcile S-ENTSOG-CMP findings after that branch merges.
- **Frontend propagation (verify):** `gridflow-front-end/` contains its **own vendored `vault/` copy** (not confirmed to read `quant-vault` live). So vault remediation (the MEDIUM doc-truth items) must also reach the frontend — either by re-syncing `gridflow-front-end/vault/` from canonical `quant-vault`, or via whatever build step it uses. **Confirm the sync mechanism before assuming a vault fix updates the site.** (Frontend internals were not otherwise in scope.)
