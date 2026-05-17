# gridflow

## What this is

Local-first Python data pipeline for UK/EU power and gas market data.
Medallion architecture: **bronze** (immutable raw API bytes) → **silver**
(validated Polars/Parquet) → **gold** (modelling-ready DuckDB views + Parquet).
No cloud. DuckDB + Parquet on local filesystem.

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

## Hard rules

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
- Do not invent rate limits, endpoints, or schemas. If it's not documented,
  write a `TODO:` and stop.

## Adding a new data source

Order: spec → config → schema → connector → transformer → wire imports → tests → run.
See `CONTRIBUTING.md` for the connector pattern reference.
