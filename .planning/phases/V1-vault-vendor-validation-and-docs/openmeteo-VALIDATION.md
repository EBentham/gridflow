---
phase: V1
plan: V1-PLAN-F-openmeteo
vendor: open-meteo
validated_on: 2026-05-08
total_datasets: 2
pass: 2
fail: 0
empty: 0
---

# Open-Meteo — V1 live-validation report

Live-validation of the two active Open-Meteo datasets configured under
`open_meteo:` in `config/sources.yaml`. All calls were issued via
`curl --ssl-no-revoke` (Avast TLS quirk on the development workstation
— see `V1-CONTEXT.md`).

## Pre-flight

| Check | Command | Result |
|---|---|---|
| Carbon-intensity smoke | `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity` | `200` |
| Open-Meteo forecast smoke | `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.1&hourly=temperature_2m"` | `200` |
| Open-Meteo archive smoke | `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" "https://archive-api.open-meteo.com/v1/archive?latitude=51.5&longitude=-0.1&start_date=2025-05-01&end_date=2025-05-07&hourly=temperature_2m"` | `200` |

## Per-dataset results

| Dataset | Status | HTTP | Bytes | `hourly.time` length | Expected | Cause / notes |
|---|---|---|---|---|---|---|
| `historical` | PASS | 200 | 5966 | 168 | 168 (7 × 24) | London 51.5 / -0.1, 2025-05-01 → 2025-05-07. First time `2025-05-01T00:00`, last `2025-05-07T23:00`. Host: `archive-api.open-meteo.com`. |
| `forecast`   | PASS | 200 | 1973 | 48  | 48 (2 × 24)  | London 51.5 / -0.1, `forecast_days=2`. First time `2026-05-08T00:00`, last `2026-05-09T23:00`. Host: `api.open-meteo.com`. |

### Curl captures (verbatim)

```bash
# historical → /tmp/openmeteo-historical.json
curl --ssl-no-revoke -fsS \
  "https://archive-api.open-meteo.com/v1/archive?latitude=51.5&longitude=-0.1&start_date=2025-05-01&end_date=2025-05-07&hourly=temperature_2m,wind_speed_10m,shortwave_radiation" \
  -o .tmp/openmeteo-historical.json \
  -w "HTTP %{http_code} | %{size_download}B\n"
# → HTTP 200 | 5966B

# forecast → /tmp/openmeteo-forecast.json
curl --ssl-no-revoke -fsS \
  "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.1&hourly=temperature_2m,wind_speed_10m,shortwave_radiation&forecast_days=2" \
  -o .tmp/openmeteo-forecast.json \
  -w "HTTP %{http_code} | %{size_download}B\n"
# → HTTP 200 | 1973B
```

### First-200-byte response samples

**historical** (`archive-api.open-meteo.com`):
```json
{"latitude":51.5,"longitude":-0.125,"generationtime_ms":0.1051,"utc_offset_seconds":0,"timezone":"GMT","timezone_abbreviation":"GMT","elevation":18.0,"hourly_units":{"time":"iso8601","temperature_2m":"°C",...
```

**forecast** (`api.open-meteo.com`):
```json
{"latitude":51.5,"longitude":-0.10000038,"generationtime_ms":0.0717,"utc_offset_seconds":0,"timezone":"GMT","timezone_abbreviation":"GMT","elevation":23.0,"hourly_units":{"time":"iso8601",...
```

## Notable findings

### 1. Two-host configuration override — verified **OK**

`config/sources.yaml` declares only one `base_url` for the
`open_meteo` source:

```yaml
open_meteo:
  base_url: "https://api.open-meteo.com/v1"
  ...
  datasets:
    historical:
      endpoint: "archive"
    forecast:
      endpoint: "forecast"
```

This `base_url` is correct for `forecast` but **wrong** for
`historical`, which lives at `archive-api.open-meteo.com`. The
connector handles this by **bypassing the config `base_url` entirely**
inside `connectors/openmeteo/client.py:_fetch_location`:

```python
if dataset == "historical":
    url = f"{ARCHIVE_BASE_URL}/archive"      # ARCHIVE_BASE_URL = https://archive-api.open-meteo.com/v1
else:
    url = f"{FORECAST_BASE_URL}/forecast"    # FORECAST_BASE_URL = https://api.open-meteo.com/v1
```

Both `ARCHIVE_BASE_URL` and `FORECAST_BASE_URL` are constants in
`connectors/openmeteo/endpoints.py`. The override is present and
correct as of 2026-05-08. **No production bug.** Live evidence: the
historical call against the archive host returned HTTP 200 with the
expected 168-row payload, and the forecast call against the main host
returned HTTP 200 with the expected 48-row payload.

Caveat for future maintainers: anyone editing the connector to "honour
the config `base_url` consistently" without first moving the archive
host into config would silently break `historical`. The dataset-name
conditional should stay until the config schema can express
per-dataset hosts (or `endpoints.py` can express it cleanly).

Recorded in:
- `quant-vault/30-vendors/open-meteo/datasets/historical.md` →
  `## Implementation delta`.
- `quant-vault/30-vendors/open-meteo/datasets/forecast.md` →
  `## Implementation delta`.

### 2. Naming inconsistency — documented, **no action**

The vendor is referenced under three different identifiers across the
project:

| Context | Identifier |
|---|---|
| Obsidian vault folder | `open-meteo` (dash) |
| Python package (`connectors/`, `silver/`) | `openmeteo` (no separator) |
| Config / registry key | `open_meteo` (underscore) |

Each is forced by its host (filesystem, Python identifier rules,
project YAML convention). Renaming any of them would touch hundreds of
references across config, imports, registry, fixtures, and vault
links. **Documented in `quant-vault/30-vendors/open-meteo/README.md`
under `## Naming`** and cross-linked from both dataset pages'
`## Known issues and gotchas` sections. **No code or config changes**
are made or required by V1.

### 3. Other observations (logged, not in V1 scope)

- **No Pydantic schema** for Open-Meteo. There is no
  `src/gridflow/schemas/openmeteo.py`, unlike elexon / entsoe / entsog.
  Recorded in both dataset pages'`## Implementation delta`. Schema
  hardening is a backlog candidate, not a V1 fix.
- **Forecast model unpinned**, so the silver `forecast` dataset values
  can shift silently between ECMWF / GFS / ICON. Documented in the
  forecast page's `## Known issues and gotchas`. Out of V1 scope.
- **`forecast_run_at` not captured.** The silver forecast schema has
  no forecast-vintage column, so older forecasts are overwritten on
  re-ingest. Cross-vintage / lead-time analysis is impossible without
  a schema extension. Out of V1 scope, recorded in the forecast page.
- **Hard-coded `LOCATIONS`** (7 UK cities) in `endpoints.py`, not in
  config. Adding a city is a code change today. Recorded.

## Template heading note

The `gridflow-dataset-spec` template specifies `## Known issues and
gotchas`; the V1-PLAN-F acceptance check informally restated it as
`## Known gotchas`. The template's verbatim wording is authoritative
per `V1-CONTEXT.md`, so both dataset pages use `## Known issues and
gotchas`. Substring grep for `gotcha` matches both pages; an exact
`## Known gotchas` literal match would not — flagging here so the
orchestrator does not flag a false-positive on the heading check.

## Verification checklist

| Check | Result |
|---|---|
| 2 dataset pages exist at `quant-vault/30-vendors/open-meteo/datasets/` | yes (`historical.md`, `forecast.md`) |
| Both dataset pages contain `archive-api.open-meteo.com` literal | yes |
| Both dataset pages contain `## Known issues and gotchas` heading | yes |
| Both dataset pages mention naming inconsistency | yes |
| `endpoints.md` lists both datasets with correct hosts | yes |
| `README.md` has zero `TODO` occurrences | yes |
| `README.md` contains a `## Naming` heading | yes |
| `total_datasets: 2`, two rows in per-dataset table | yes |
| `## Notable findings` heading present | yes |
