# gridflow Vendor-Truth Audit — Deliverables (2026-05-31)

Versioned copies of the audit's shareable deliverables. The **full working artifacts are local-only** under `.planning/audit/2026-05-31-vendor-truth-audit/` — that path is git-ignored by repo convention (`.gitignore`: GSD / AI planning state). Local-only artifacts include:

- `findings/<unit>.md` — 13 per-unit discrepancy reports (each finding with file:line evidence + recommended fix)
- `findings/_ALL-FINDINGS.json` — machine-readable index of all 88 findings (severity, class, evidence, fix, verify verdict)
- `probes/` + `render_findings.py` + `STATE.json` — reproducible probe tooling + run state

Links inside the documents below that point to `findings/…`, `STATE.json`, or `probes/…` refer to that **local** directory. (They can be force-committed on request — `git add -f` — if you want the detail versioned too.)

## Contents
- [SUMMARY.md](SUMMARY.md) — executive summary. **P1 (live-API correctness) verified healthy**; **P2: 88 findings** (HIGH 11 → 8 upheld + 3 refuted, MED 25, LOW 28, INFO 24). Headline: **VTA-SCHEMA-01** (Pydantic schemas never enforced in the silver write path — project-wide).
- [REMEDIATION-BACKLOG.md](REMEDIATION-BACKLOG.md) — GSD-ready prioritized requirements (`VTA-*` IDs, severity, acceptance criteria, traceability).
- [LIVE-API-REPORT.md](LIVE-API-REPORT.md) — Priority-1 live-API verification evidence (connectivity, silent-duplication checks, forecast completeness, GIE-01 live verification).

## Method (one line)
Real `gridflow ingest`+`transform` against live vendor APIs into an isolated temp lake (P1) + tri-source `vendor-doc ⟷ vault ⟷ code` audits of ~20 family-units via Workflows with adversarial verification (P2). Authority: official docs > fixtures > code; vault under audit.

## Next step
Run GSD `research → plan → execute → code-review → ship` over `REMEDIATION-BACKLOG.md`, starting with **VTA-SCHEMA-01** (highest leverage — one fix closes the validation gap across all vendors). Note `VTA-FRONTEND-SYNC-01`: confirm how `gridflow-front-end` consumes the vault before assuming vault fixes reach the site.
