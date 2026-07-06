# gridflow

## What this is

Local-first Python data pipeline for UK/EU power and gas market data.
Medallion architecture: **bronze** (immutable raw API bytes) → **silver**
(validated Polars/Parquet) → **gold** (modelling-ready DuckDB views + Parquet).
No cloud. DuckDB + Parquet on local filesystem.

## Session-start protocol

Read before doing anything else:

1. The vault-context block injected at session start (head of the vault's
   `00-active/now.md` — current focus and milestone position). If absent, read
   `.planning/STATE.md`.
2. `.planning/STATE.md` + the active phase dir under `.planning/phases/` when
   working inside a milestone.
3. `docs/DECISION_LOG/` before touching a design boundary — check for a prior ADR.

## Ecosystem map

| Repo / store | Path | Role |
|---|---|---|
| gridflow (this repo) | `C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow` | Data pipeline: bronze → silver → gold |
| gridflow_models | `C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow_models` | GB power-market models; reads gridflow silver/gold via editable install |
| gridflow-front-end | `C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow-front-end` | Recruiter-facing static docs site (GitHub Pages) |
| quant-vault | `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault` | Obsidian vault: vendor reference (`30-vendors/`), domain theory, cross-repo contracts, current focus (`00-active/now.md`) |

Source-of-truth order: gridflow code → live API responses → vault `30-vendors/`
pages → front-end rendered pages. The front-end `vault/` mirror is generated —
refresh it with the `propagate-vault-mirror` skill, never by hand.

## Commands

```bash
uv run pytest -x -q                  # default test run
uv run pytest -m "not live"          # skip live-API tests
uv run ruff check src/ tests/
uv run mypy src/gridflow/
uv run gridflow init
uv run gridflow ingest <source> <dataset> --last 24h
uv run gridflow transform <source> <dataset> --last 24h
uv run gridflow pipeline <source> --all --last 24h
uv run gridflow status
uv run gridflow quality --all
```

If `uv` breaks, run the venv directly: `./.venv/Scripts/python.exe -m pytest ...`.
uv network ops (lock/sync) need `--system-certs` on this machine (Avast TLS).

## Hard rules

<!-- Python style (no-bare-except, no-comments-restating-code) and the tests-before-done gate are inherited from ~/.claude/CLAUDE.md; ruff enforces the style rules. -->

**Data correctness (silent-bug class):**
- All timestamps tz-aware UTC. `TimestampMixin` rejects naive input. Convert at
  the silver-transformer boundary, never at query time.
- Settlement period range is `1..50`, not `1..48`. DST days: 46 spring, 50 autumn.
- Never dedup settlement data on `(date, period)` alone — include `run_type`.
  Point-in-time queries: `WHERE published_at <= :as_of`.
- Gas day is a `date`. Don't synthesise a UTC timestamp without the 06:00 UTC offset.
- Pydantic validation failures are logged and surfaced — never silently dropped.
- BM unit IDs and EIC codes stored as-is, no normalisation.
- Parameterised SQL only. No f-string queries with API or user data.

**Code conventions:**
- No pandas. Polars only. `.to_pandas()` only at presentation boundaries.
- No manual path construction. Use `PathBuilder` from `storage/paths.py`.
- No `Path.rename()` for atomic writes on Windows. Use `os.replace()` —
  already encapsulated in `storage/parquet.write_parquet`.
- Bronze is immutable. Never edit bronze in place.

**Process:**
- Never commit to `main`. Feature branches only.
- Conventional commits: `feat:`, `fix:`, `test:`, `refactor:`, `chore:`, `docs:`.
- No live API ingestion (`gridflow ingest` against a real key) without explicit
  user confirmation.
- No new top-level packages under `src/gridflow/` without discussion.
- No cloud dependencies. Local-only is a hard constraint.
- Do not invent rate limits, endpoints, or schemas. If it's not documented,
  write a `TODO:` and stop.

## Verification hooks (mechanical, local-only)

`.claude/` is gitignored; these run on this machine only:

- `guard.py` (PreToolUse Bash) — blocks commit-on-master, force-push,
  `reset --hard`, and live `gridflow ingest` without `GRIDFLOW_ALLOW_INGEST=1`;
  hard-halts bronze writes and out-of-repo `rm -rf`.
- `format.py` (PostToolUse) — ruff format + autofix on every edited `.py`.
- `test_gate.py` (Stop) — a turn cannot end with a red fast suite
  (`-m "not live and not slow"`). Bypass: `GRIDFLOW_NO_TEST_GATE=1`.
- `vault_context.py` (SessionStart) / `vault_nudge.py` (Stop) — inject vault
  focus at start; remind once daily if commits landed without a vault note.

If a turn refuses to end or a command is vetoed, this is why — fix the cause,
don't fight the hook.

## Key pointers

| Need | Location |
|---|---|
| Per-decision rationale (ADRs) | `docs/DECISION_LOG/` |
| Connector pattern reference | `CONTRIBUTING.md` |
| Endpoint catalogs (per vendor) | `docs/*_endpoint_catalog.yaml`, `docs/endpoints/` |
| Milestone position / open items | `.planning/STATE.md` (git-ignored, local) |
| Shipped history | `CHANGELOG.md` (public) · `.planning/MILESTONES.md` (canonical) |
| 2026-07 review remediation plan | `.planning/review-2026-06/ROADMAP.md` (P0–P3) |
| Vendor truth (docs layer) | vault `30-vendors/<vendor>/datasets/` |

## Adding a new data source

Order: spec → config → schema → connector → transformer → wire imports → tests → run.
See `CONTRIBUTING.md` for the connector pattern reference.

Definition of done also includes the docs layer: vault dataset page (via the
`gridflow-dataset-spec` skill) and front-end mirror propagation (via the
`propagate-vault-mirror` skill).
