---
slug: entsoe-401-auth-failure
status: resolved
trigger: "The ENTSOE pipeline does not work. Running `gridflow backfill entsoe --all --start 2026-04-15 --end 2026-04-25` produces 401 errors."
created: 2026-05-02
updated: 2026-05-02
---

## Symptoms

- **Command:** `gridflow backfill entsoe --all --start 2026-04-15 --end 2026-04-25`
- **Error:** `Client error '401 ' for url 'https://web-api.tp.entsoe.eu/api?documentType=A44&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202604150000&periodEnd=202604220000'`
- **History:** New setup — pipeline has never successfully run
- **API token:** User reports token is set in environment
- **Tests:** Live tests do not pass; unit tests likely mock HTTP and hide the real failure

## Current Focus

hypothesis: "API token not injected into requests because env var name mismatch prevents load_settings() from reading the key"
test: "Runtime repro confirmed all three resolution paths return empty"
expecting: "securityToken absent from request URL — confirmed by 401 error and repro"
next_action: "Apply fix: add dotenv.load_dotenv() call in load_settings() before PipelineSettings is instantiated"
reasoning_checkpoint: "pydantic-settings with env_prefix=GRIDFLOW_ reads GRIDFLOW_ENTSOE_API_KEY, but .env defines ENTSOE_API_KEY (no prefix). pydantic-settings does NOT push .env values into os.environ — it loads them into the model instance only. So pipeline.entsoe_api_key is always empty, key_map injection never fires, and the get_source_config fallback (which reads os.environ directly) also fails. The connector's guard `if self.config.api_key:` then skips adding securityToken to the query params."

## Evidence

- timestamp: 2026-05-02T00:00:00Z
  observation: "401 URL has no securityToken query param — confirms token was never attached"
  file: "symptom"

- timestamp: 2026-05-02T00:00:00Z
  observation: "client.py:190 — `if self.config.api_key: query_params['securityToken'] = self.config.api_key` — token is only added if api_key is non-empty"
  file: "src/gridflow/connectors/entsoe/client.py"

- timestamp: 2026-05-02T00:00:00Z
  observation: "settings.py:47 — PipelineSettings has env_prefix='GRIDFLOW_', so it reads GRIDFLOW_ENTSOE_API_KEY from env, not ENTSOE_API_KEY"
  file: "src/gridflow/config/settings.py"

- timestamp: 2026-05-02T00:00:00Z
  observation: ".env file defines ENTSOE_API_KEY (without GRIDFLOW_ prefix) — this is what .env.example documents and what api_key_env in sources.yaml references"
  file: ".env / .env.example / config/sources.yaml"

- timestamp: 2026-05-02T00:00:00Z
  observation: "Runtime repro: pipeline.entsoe_api_key set? False | os.environ has ENTSOE_API_KEY? False | get_source_config api_key set? False — all three resolution paths return empty"
  file: "runtime"

- timestamp: 2026-05-02T00:00:00Z
  observation: "pydantic-settings v2 with env_file='.env' loads values into the model instance only — does NOT push them into os.environ. So even the get_source_config fallback (os.environ.get('ENTSOE_API_KEY')) finds nothing."
  file: "pydantic-settings behavior"

## Eliminated

- Wrong parameter name: securityToken is correct per ENTSO-E spec and is used in client.py
- Token value being wrong: token is never even sent — eliminated
- Missing registration: connector registers correctly

## Resolution

root_cause: "PipelineSettings uses env_prefix='GRIDFLOW_' so it reads GRIDFLOW_ENTSOE_API_KEY, but the .env file defines ENTSOE_API_KEY (no prefix). pydantic-settings does not push .env values into os.environ, so the get_source_config fallback also fails. Result: api_key is always empty, securityToken is never added to requests, and ENTSO-E returns 401."
fix: "Add `from dotenv import load_dotenv; load_dotenv()` at the top of load_settings() in src/gridflow/config/settings.py, before PipelineSettings is instantiated. This loads .env values into os.environ, which allows both the pydantic-settings prefix lookup (if user adds GRIDFLOW_ prefix) AND the get_source_config fallback (os.environ.get('ENTSOE_API_KEY')) to work. Alternatively, rename PipelineSettings fields to use validation_alias matching the unprefixed names."
verification: "uv run python -c 'from gridflow.config.settings import load_settings; s=load_settings(); print(bool(s.get_source_config(\"entsoe\").api_key))' should print True"
files_changed: "src/gridflow/config/settings.py (add load_dotenv call in load_settings), pyproject.toml (add python-dotenv>=1.0 core dep)"
