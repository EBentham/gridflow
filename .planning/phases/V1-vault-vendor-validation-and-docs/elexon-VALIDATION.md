---
phase: V1
vendor: elexon
validated: 2026-05-08
total_datasets: 33
---

# Elexon - V1 Validation Report

Live validation pass executed 2026-05-08 against the Elexon BMRS Insights API.
All requests used `curl --ssl-no-revoke` to bypass the Avast Windows-cert-store
TLS quirk on the workstation; throttling was 0.6s between calls (vendor limit
2 req/s). The empty `ELEXON_API_KEY` in `.env` was retained intentionally - every
public BMRS endpoint returned 200 without an `apikey` header.

## Summary

| Status | Count |
|--------|-------|
| PASS   | 33    |
| EMPTY  | 0     |
| FAIL   | 0     |

Of the 33 PASSes, 26 passed on the first attempt with the canonical test
parameters (3-hour `publishDateTimeFrom/To` window or settlement date 2026-05-06).
7 datasets required a retry - see "Per-dataset results" below for the cause
column and the "Curl evidence" section for the original 4xx/empty response
bodies.

## Per-dataset results

| Dataset | Status | HTTP | Bytes | Time (s) | Rows | Cause / Notes | Vault page |
|---------|--------|------|-------|----------|------|----------------|------------|
| `system_prices` | PASS | 200 | 42446 | 0.067352 | 48 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/system_prices.md) |
| `market_depth` | PASS | 200 | 16105 | 0.245038 | 48 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/market_depth.md) |
| `boal` | PASS | 200 | 810534 | 0.126281 | 2027 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/boal.md) |
| `disbsad` | PASS | 200 | 1417 | 0.096647 | 7 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/disbsad.md) |
| `mid` | PASS | 200 | 2250 | 0.051572 | 14 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/mid.md) |
| `netbsad` | PASS | 200 | 2705 | 0.180133 | 7 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/netbsad.md) |
| `pn` | PASS | 200 | 558016 | 0.086471 | 2580 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/pn.md) |
| `freq` | PASS | 200 | 454437 | 0.094349 | 5761 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/freq.md) |
| `fuelhh` | PASS | 200 | 25271 | 0.053058 | 140 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/fuelhh.md) |
| `fuelinst` | PASS | 200 | 135020 | 0.068499 | 740 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/fuelinst.md) |
| `imbalngc` | PASS | 200 | 1021491 | 0.108704 | 5670 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/imbalngc.md) |
| `ndf` | PASS | 200 | 54115 | 0.101181 | 315 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/ndf.md) |
| `ndfd` | PASS | 200 | 1297 | 0.057 | 13 | PASS with 1-day window 2026-04-01..2026-04-02 — initial 3-hour window 2026-05-06T00:00..03:00Z had no publications | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/ndfd.md) |
| `melngc` | PASS | 200 | 998002 | 0.175014 | 5670 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/melngc.md) |
| `fou2t14d` | PASS | 200 | 259806 | 0.681896 | 988 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/fou2t14d.md) |
| `uou2t14d` | PASS | 200 | 4696834 | 0.516214 | 26832 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/uou2t14d.md) |
| `windfor` | PASS | 200 | 65418 | 0.094 | 584 | PASS with 1-day window 2026-05-05..2026-05-06 — initial 3-hour window had no publishes | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/windfor.md) |
| `temp` | PASS | 200 | 116 | 1.104 | 1 | PASS with 1-day window 2026-04-01..2026-04-02 — TEMP publishes daily | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/temp.md) |
| `agpt` | PASS | 200 | 18681 | 0.065329 | 66 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/agpt.md) |
| `agws` | PASS | 200 | 5134 | 0.074725 | 18 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/agws.md) |
| `atl` | PASS | 200 | 1379 | 1.085707 | 6 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/atl.md) |
| `indo` | PASS | 200 | 1109 | 0.059158 | 7 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/indo.md) |
| `itsdo` | PASS | 200 | 1116 | 0.045466 | 7 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/itsdo.md) |
| `indod` | PASS | 200 | 113 | 1.10 | 1 | PASS with 1-day window 2026-04-01..2026-04-02 — INDOD is daily total | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/indod.md) |
| `nonbm` | PASS | 200 | 169 | 0.083900 | 1 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/nonbm.md) |
| `inddem` | PASS | 200 | 995731 | 0.148544 | 5670 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/inddem.md) |
| `indgen` | PASS | 200 | 1015041 | 0.109110 | 5670 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/indgen.md) |
| `tsdf` | PASS | 200 | 980778 | 0.161644 | 5670 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/tsdf.md) |
| `tsdfd` | PASS | 200 | 1310 | 0.079 | 13 | PASS with 1-day window 2026-04-01..2026-04-02 — 2-14 day forecast publishes once-per-day | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/tsdfd.md) |
| `lolpdrm` | PASS | 200 | 82404 | 0.119494 | 309 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/lolpdrm.md) |
| `remit` | PASS | 200 | 188576 | 0.318 | 144 | PASS — vendor enforces max-1-day range; original 7-day query was rejected with HTTP 400 | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/remit.md) |
| `soso` | PASS | 200 | 952813 | 0.104 | 2304 | PASS — vendor enforces max-1-day range; original 7-day query was rejected with HTTP 400 | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/soso.md) |
| `bmunits_reference` | PASS | 200 | 2050942 | 0.372659 | 2960 | n/a | [page](C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/bmunits_reference.md) |


## Curl evidence

PASS rows omit detailed evidence to keep the report compact. The 7 datasets
that needed a retry are documented below with their original (failing/empty)
attempt and the successful retry.

### `ndfd` - first-attempt EMPTY, retry PASS

**Original (failing/empty) request:**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/NDFD?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-06T03:00Z&format=json" \
  -o ".tmp/elexon-ndfd-initial.json"
```

- HTTP: `200`
- Bytes: `11`
- Cause: 200 with empty `data: []` for 3-hour window 2026-05-06T00:00..03:00Z

Response body:

```json
{"data":[]}
```

**Remediation:** Retried 1-day window 2026-04-01..2026-04-02 returned 13 rows (PASS).

### `windfor` - first-attempt EMPTY, retry PASS

**Original (failing/empty) request:**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/WINDFOR?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-06T03:00Z&format=json" \
  -o ".tmp/elexon-windfor-initial.json"
```

- HTTP: `200`
- Bytes: `11`
- Cause: Same 3-hour-window-no-publishes pattern as NDFD

Response body:

```json
{"data":[]}
```

**Remediation:** Retried 1-day window 2026-05-05..2026-05-06 returned 584 rows (PASS).

### `temp` - first-attempt EMPTY, retry PASS

**Original (failing/empty) request:**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/TEMP?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-06T03:00Z&format=json" \
  -o ".tmp/elexon-temp-initial.json"
```

- HTTP: `200`
- Bytes: `11`
- Cause: Daily publication; 3-hour window had no publishes

Response body:

```json
{"data":[]}
```

**Remediation:** Retried 1-day window 2026-04-01..2026-04-02 returned 1 row (PASS).

### `indod` - first-attempt EMPTY, retry PASS

**Original (failing/empty) request:**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/INDOD?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-06T03:00Z&format=json" \
  -o ".tmp/elexon-indod-initial.json"
```

- HTTP: `200`
- Bytes: `11`
- Cause: Daily aggregate; 3-hour window had no publishes

Response body:

```json
{"data":[]}
```

**Remediation:** Retried 1-day window 2026-04-01..2026-04-02 returned 1 row (PASS).

### `tsdfd` - first-attempt EMPTY, retry PASS

**Original (failing/empty) request:**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/TSDFD?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-06T03:00Z&format=json" \
  -o ".tmp/elexon-tsdfd-initial.json"
```

- HTTP: `200`
- Bytes: `11`
- Cause: Daily 2-14 day forecast; 3-hour window had no publishes

Response body:

```json
{"data":[]}
```

**Remediation:** Retried 1-day window 2026-04-01..2026-04-02 returned 13 rows (PASS).

### `remit` - first-attempt FAIL, retry PASS

**Original (failing/empty) request:**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/REMIT?publishDateTimeFrom=2026-04-01T00:00Z&publishDateTimeTo=2026-04-08T00:00Z&format=json" \
  -o ".tmp/elexon-remit-initial.json"
```

- HTTP: `400`
- Bytes: `307`
- Cause: Vendor enforces max-1-day query window

Response body:

```json
{"type":"https://tools.ietf.org/html/rfc9110#section-15.5.1","title":"One or more validation errors occurred.","status":400,"errors":{"":["The date range between PublishDateTimeFrom and PublishDateTimeTo inclusive must not exceed 1 day"]}}
```

**Remediation:** Retried 1-day window 2026-05-06..2026-05-07 returned 144 rows (PASS).

### `soso` - first-attempt FAIL, retry PASS

**Original (failing/empty) request:**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/SOSO?publishDateTimeFrom=2026-04-01T00:00Z&publishDateTimeTo=2026-04-08T00:00Z&format=json" \
  -o ".tmp/elexon-soso-initial.json"
```

- HTTP: `400`
- Bytes: `307`
- Cause: Vendor enforces max-1-day query window (same as REMIT)

Response body:

```json
{"type":"https://tools.ietf.org/html/rfc9110#section-15.5.1","title":"One or more validation errors occurred.","status":400,"errors":{"":["The date range between PublishDateTimeFrom and PublishDateTimeTo inclusive must not exceed 1 day"]}}
```

**Remediation:** Retried 1-day window 2026-05-06..2026-05-07 returned 2304 rows (PASS).

## Implementation deltas (cross-cutting)

Doc-vs-code conflicts found during validation. These affect more than one
dataset or the wider connector contract; per-dataset deltas are captured in
each dataset page's `## Implementation delta` section.

### 1. `freq` parameter-name mismatch (real bug, single-dataset but high-impact)

**Severity:** medium - silently corrupts ingest output.

Official Swagger declares the `/datasets/FREQ` query parameters as:

- `measurementDateTimeFrom`
- `measurementDateTimeTo`

The connector's `ENDPOINTS["freq"]` uses the default `PUBLISH_DATETIME`
ParamStyle, which sends:

- `publishDateTimeFrom`
- `publishDateTimeTo`

Live test 2026-05-08 querying `?publishDateTimeFrom=2024-01-01T00:00Z&publishDateTimeTo=2024-01-01T03:00Z`
returned **5761 rows of *current* (2026-05-08) data** rather than the requested
January 2024 window. Querying with the correct `measurementDateTimeFrom/To`
parameter names returned **721 rows of correctly-windowed January 2024 data**.

**Implication:** the connector's frequency-data ingest currently captures the
latest ~5761 samples regardless of the requested window. Bronze files produced
by `gridflow ingest elexon freq --last 24h` (or any other windowed call) likely
contain the wrong rows.

**Fix recommendation (out of V1 scope):** override `from_param` and `to_param`
on the `ElexonEndpoint("freq", ...)` definition:

```python
"freq": ElexonEndpoint(
    path="/datasets/FREQ",
    description="System Frequency",
    param_style=ParamStyle.PUBLISH_DATETIME,
    from_param="measurementDateTimeFrom",
    to_param="measurementDateTimeTo",
)
```

### 2. `BOAL` -> `BOALF` rename (vault stale)

**Severity:** documentation only.

Pre-V1 vault `endpoints.md` listed `boal` as `/datasets/BOAL`. The vendor
renamed BOAL -> BOALF; only `BOALF` is served. The connector code
(`endpoints.py`) already uses `BOALF` and is correct. Vault `endpoints.md` and
`datasets/boal.md` now reflect the correct path.

### 3. REMIT and SOSO max-1-day query window (vendor-enforced, undocumented)

**Severity:** ingest-time bug if backfilling.

Querying `/datasets/REMIT` or `/datasets/SOSO` with a range > 1 day returns:

```json
{"errors":{"":["The date range between PublishDateTimeFrom and PublishDateTimeTo inclusive must not exceed 1 day"]}}
```

with HTTP 400. This limit is **not** in the Swagger spec and is not declared in
the developer portal. The connector currently uses the default `max_chunk_hours
= 24` - at the boundary. If a backfill window crosses a DST transition or
otherwise expands by even a few hours, the connector will receive HTTP 400.

**Fix recommendation (out of V1 scope):** set `max_chunk_hours = 23` on the
`remit` and `soso` ElexonEndpoint definitions.

### 4. Daily-publish datasets and narrow-window queries

**Severity:** test-time false-EMPTY only.

5 datasets (`ndfd`, `windfor`, `temp`, `indod`, `tsdfd`) publish only a handful
of rows per day at fixed times. A 3-hour window between publishes returns HTTP
200 with `{"data":[]}`. This is correct vendor behaviour - not a bug - but the
V1 plan's default 3-hour test window caused these datasets to fail the
"rows >= 1" PASS criterion on first attempt. **Recommendation:** any future
automated validation harness should use a 24-hour window for forecast/daily
datasets.

### 5. NDF and NDFD share a silver transformer

**Severity:** none - by-design.

`gridflow.silver.elexon.demand_forecast.DemandForecastTransformer` is
registered for both `ndf` and `ndfd`. The two datasets are distinguished by the
derived `forecast_type` field (`day_ahead` vs `2_14_day`). The transformer also
fills `settlement_period = 1` for NDFD records that lack a settlement period
in the API. Both behaviours are documented in the dataset pages.

### 6. `nonbm` parameter-style ambiguity

**Severity:** low.

Swagger declares `/datasets/NONBM` parameters as `from`/`to`. The connector
uses the default `PUBLISH_DATETIME` style (sends `publishDateTimeFrom/To`).
Live test returned 1 row with the connector's default - the API may accept
either name (or silently ignore). Worth a targeted A/B test in a future
code-fix phase.

### 7. Many datasets lack a Pydantic schema in `schemas/elexon.py`

**Severity:** low - by-design but worth surfacing.

Of 33 datasets, only 12 have a dedicated Pydantic class in `schemas/elexon.py`:
`ElexonSystemPrice`, `ElexonGenerationByFuel`, `ElexonFuelHH`, `ElexonBOAL`,
`ElexonBOD`, `ElexonMID`, `ElexonFrequency`, `ElexonDemandForecast`,
`ElexonWindForecast`, `ElexonPN`, `ElexonDISBSAD`, `ElexonBMUnit`. The
remaining 21 datasets rely on the silver transformer to enforce the output
column shape inline. This is consistent with the project's "schema where it
adds value" pattern but means contract validation is per-transformer rather
than centralised. Each dataset page lists this in `## Implementation delta`.

## Recommendations

Prioritised follow-up backlog items the executor would create:

1. **HIGH: Fix `freq` connector param names.** Override `from_param` /
   `to_param` on `ENDPOINTS["freq"]` to `measurementDateTimeFrom` /
   `measurementDateTimeTo`. Without this, FREQ ingest is silently capturing
   the wrong rows.
2. **MEDIUM: Cap `remit` and `soso` `max_chunk_hours = 23`.** Avoid the
   vendor 400 at backfill window boundaries.
3. **LOW: Verify `nonbm` accepts `publishDateTimeFrom/To`** (current default)
   vs `from/to` (docs).
4. **LOW: Decide whether to add Pydantic schemas** for the 21 datasets that
   rely on transformer-only validation. This is policy not a bug; a separate
   consistency phase.
5. **LOW: `system_prices` and `market_depth` `path` strings** in
   `endpoints.py` are missing the `{settlementDate}` placeholder. This is
   cosmetic - the connector uses `_fetch_date_path()` which appends the date -
   but flagging makes the registry shape match the docs.

## Metadata

- **Tools used:** `curl --ssl-no-revoke -fsS` (TLS-revocation bypass for Avast
  on this workstation), Python 3.13 stdlib for response shape analysis. No
  `httpx` calls.
- **Throttle:** `sleep 0.6` between Elexon calls.
- **Total live HTTP calls:** ~50 (33 first-attempt + 7 retries + ~10
  diagnostic calls including the FREQ param-name verification).
- **Pre-flight smoke test (carbonintensity.org.uk/intensity):** PASSED.
- **Pre-flight Elexon smoke test (FUELHH 2026-05-06):** PASSED with 137891 bytes / 740 rows.

---

## V2 re-validation (2026-05-09)

**Fix commit:** `fix(V2-A): elexon freq sends measurementDateTimeFrom/To not publishDateTimeFrom/To`
(SHA recorded by V2-PLAN-F aggregate close-out).

| Dataset | V1 status | V2 status | Window respected? |
|---------|-----------|-----------|-------------------|
| `freq` | PASS (5761 rows of *latest*, **wrong window**) | PASS (241 rows in 1h window, correct dates) | YES |

### `freq` re-validation evidence

**A/B comparison (proves V1 bug + V2 fix):**

```bash
# Wrong-name (current default before V2-A fix): API silently ignores
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?publishDateTimeFrom=2024-01-01T00:00Z&publishDateTimeTo=2024-01-01T03:00Z&format=json"
# → HTTP 200, 5761 rows, all dated 2026-05-08/09 (NOT 2024-01-01)

# Correct-name (Swagger): API honours the window
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?measurementDateTimeFrom=2024-01-01T00:00Z&measurementDateTimeTo=2024-01-01T03:00Z&format=json"
# → HTTP 200, 721 rows, all dated 2024-01-01
```

**V2 narrow-window confirmation (post-fix):**

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?measurementDateTimeFrom=2026-05-09T00:00Z&measurementDateTimeTo=2026-05-09T01:00Z&format=json"
```

- HTTP: `200`
- Bytes: `19028`
- Rows: `241`
- All `measurementTime` values within `[2026-05-09T00:00Z, 2026-05-09T01:00Z]`: **yes** (241 / 241)
- Time: `0.093 s`

**Regression tests:**

- `tests/unit/test_elexon_endpoints.py::TestBuildParamsPublishDatetime::test_freq_uses_measurement_datetime_param_names` — RED before fix, GREEN after.
- `tests/unit/test_elexon_endpoints.py::TestBuildParamsPublishDatetime::test_freq_endpoint_overrides_param_names` — asserts the explicit override on the dataclass.
- `tests/endpoints/test_endpoint_urls.py::TestElexonPublishDatetimeParams` — parametrize updated to carry per-dataset `from_param`/`to_param`; freq row now expects `measurementDateTime*`.
- Full fast suite: `1026 passed, 253 deselected` (`uv run pytest -m "not live and not slow" -x -q`).

**Out-of-V2-scope follow-up (queued for V2-PLAN-F backlog):**

Existing bronze files for `freq` were captured with the wrong param names — they hold "latest 5761 samples" instead of the requested window. Historical re-ingest is required to get correct windowed data on disk.
