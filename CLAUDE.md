# gridflow

## What this is

Local-first Python data pipeline for UK/EU power and gas market data.
Medallion architecture: **bronze** (immutable raw API bytes) → **silver**
(validated Polars/Parquet) → **gold** (modelling-ready DuckDB views + Parquet).
No cloud. DuckDB + Parquet on local filesystem.

## Session-start protocol

Read before doing anything else:

1. `quant-vault/00-active/now.md` — current focus and active GSD workstream
2. `quant-vault/10-projects/energy-pipeline/README.md` — project status
3. `quant-vault/30-vendors/<vendor>/README.md` — before any vendor work

Access via the `obsidian-vault` MCP server. Don't ask the user to paste context.

## GSD workflow

Tasks run via GSD workstreams. Check `now.md` for the active workstream and
phase before starting. Use `/gsd-plan-phase` before implementation, not after.
Specs land in `quant-vault/10-projects/energy-pipeline/specs/<slug>.md` —
write the spec, wait for approval, then implement.

## Codebase navigation

This repo has a graphify knowledge graph at `graphify-out/`.

- Architecture and cross-module questions → `graphify query "<question>"` or
  `graphify path "<A>" "<B>"` — do not grep
- After modifying source files → `graphify update .` to keep the graph current
- `graphify-out/GRAPH_REPORT.md` for god-node and community structure overview

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

## Hard rules — check these reflexively, not on lookup

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
- No bare `except:`. Catch the narrowest exception that makes sense.
- No manual path construction. Use `PathBuilder` from `storage/paths.py`.
- No `Path.rename()` for atomic writes on Windows. Use `os.replace()` —
  already encapsulated in `storage/parquet.write_parquet`.
- No comments that restate what the code does. WHY only, when non-obvious.
- Bronze is immutable. Never edit bronze in place.

**Process:**
- Never commit to `main`. Feature branches only.
- Conventional commits: `feat:`, `fix:`, `test:`, `refactor:`, `chore:`, `docs:`.
- `uv run pytest -x -q` must pass before declaring a task complete.
- No live API ingestion (`gridflow ingest` against a real key) without explicit
  user confirmation.
- No new top-level packages under `src/gridflow/` without discussion.
- No cloud dependencies. Local-only is a hard constraint.
- Do not invent rate limits, endpoints, or schemas. If it's not in the vault,
  write a `TODO:` and stop.

## Adding a new data source

Full implementation sequence in SKILL.md pointer table and
`quant-vault/40-techniques/async-connector-pattern.md`.
Order: spec → config → schema → connector → transformer → wire imports → tests → run.
