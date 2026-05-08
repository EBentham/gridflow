---
phase: V1
plan: V1-PLAN-D-gie
vendor: gie
validated_on: 2026-05-08
total_datasets: 7
pass: 7
fail: 0
empty: 0
---

# GIE AGSI+ — V1 live-validation report

Live-validation of the seven active GIE AGSI datasets configured under
`gie_agsi:` in `config/sources.yaml`. ALSI LNG (`gie_alsi.lng`) is
explicitly out of V1 scope; the connector-still-loads check is recorded
below. All calls were issued via `curl --ssl-no-revoke` (Avast TLS
quirk on the development workstation — see `V1-CONTEXT.md`). Throttle
~1 req/s.

## Pre-flight

| Check | Command | Result |
|---|---|---|
| Carbon-intensity smoke | `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity` | `200` |
| `.env` `GIE_API_KEY` present | `grep -E "^GIE_API_KEY=" .env`, length check | `32 chars` (non-empty) |
| AGSI about smoke | `curl --ssl-no-revoke -fsS -H "x-key: $KEY" "https://agsi.gie.eu/api/about" -o .tmp/gie-about-smoke.json` | `HTTP 200, 558378B` |
| Both connectors load with `gie_alsi` config present | Python import + `get_connector('gie_agsi'/'gie_alsi', ...)` | OK — `AgsiConnector`, `AlsiConnector` instantiate cleanly |

## Per-dataset results

| Dataset | Status | HTTP | Bytes | Top-level shape | `total` | `last_page` | Cause / notes |
|---|---|---|---|---|---|---|---|
| `storage_reports` | PASS | 200 | 2841 | `{last_page, total, dataset, gas_day, data}` | 6 | 1 | `country=GB&from=2026-05-01&to=2026-05-07`. Body has 6 GB rows but values are `-` placeholders post-Brexit; structurally valid. DE substitution returns numeric values. |
| `storage` | PASS | 200 | 519 | `{last_page, total, dataset, gas_day, data}` | 1 | 1 | `country=GB&date=2026-05-06`. 1 GB row with `-` placeholders; DE substitution returns numeric values. |
| `about_summary` | PASS | 200 | 558378 | `{"SSO": {"Europe": {<Country>: [...]}}}` | n/a | n/a | `/api/about` no params. Live shape differs from fixture envelope `{"data": {...}}` — handled by recursive parser. |
| `about_listing` | PASS | 200 | 53040 | top-level **list** of 71 company objects | n/a | n/a | `/api/about?show=listing`. Live is a list; fixture wraps in `{"data": [...]}`. Connector handles both. |
| `news` | PASS | 200 | 2629579 | `{"data": [...]}` | absent | absent | `/api/news` no params. 299 records, no `last_page` / `total` in live response (catalog declares them). Single-page in practice. |
| `news_item` | PASS (with caveat) | 200 | 2629579 | `{"data": [...]}` | absent | absent | `/api/news?turl=1713470` returned **byte-identical** body to `/api/news` — the `turl` filter is silently ignored upstream. Connector's `_is_news_item_detail` shape filter discards the listing-shape, so silver yields 0 rows. HTTP-wise PASS; logged in dataset Implementation delta. |
| `unavailability` | PASS | 200 | 13727 (DE) / 148 (GB) | `{last_page, total, dataset, data}` | 30 (DE) / 0 (GB) | 7 (DE) / 1 (GB) | `country=DE&start=2026-04-01&end=2026-05-07` returns 30 rows (page 1 of 7). GB returns empty (no UK storage outages reported on AGSI — by design). |

### Curl captures (verbatim)

All commands executed from worktree CWD. `$KEY` substitutes
`GIE_API_KEY` from `.env` (32 chars).

```bash
# 1. storage_reports → .tmp/gie-storage_reports.json
curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api?country=GB&from=2026-05-01&to=2026-05-07" \
  -o .tmp/gie-storage_reports.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 2841B | 0.21s
sleep 1.0

# 2. storage → .tmp/gie-storage.json
curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api?country=GB&date=2026-05-06" \
  -o .tmp/gie-storage.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 519B | 0.17s
sleep 1.0

# 3. about_summary → .tmp/gie-about_summary.json
curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api/about" \
  -o .tmp/gie-about_summary.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 558378B | 0.78s
sleep 1.0

# 4. about_listing → .tmp/gie-about_listing.json
curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api/about?show=listing" \
  -o .tmp/gie-about_listing.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 53040B | 0.38s
sleep 1.0

# 5. news → .tmp/gie-news.json
curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api/news" \
  -o .tmp/gie-news.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 2629579B | 0.66s
sleep 1.0

# 6. news_item → .tmp/gie-news_item.json (turl=1713470 from news[0].url)
curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api/news?turl=1713470" \
  -o .tmp/gie-news_item.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 2629579B | 0.60s   (byte-identical to news listing)
sleep 1.0

# 7. unavailability → .tmp/gie-unavailability.json (GB) and -DE-startend.json (DE)
curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api/unavailability?country=GB&start=2026-04-01&end=2026-05-07" \
  -o .tmp/gie-unavailability.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 148B | 0.15s   (empty for GB)

curl --ssl-no-revoke -fsS -H "x-key: $KEY" \
  "https://agsi.gie.eu/api/unavailability?country=DE&start=2026-04-01&end=2026-05-07" \
  -o .tmp/gie-unavailability-DE-startend.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
# → HTTP 200 | 13727B | 0.17s   (30 rows, last_page=7)
```

Total live calls: 8 (7 active + 1 DE substitution for unavailability).
Total throttle sleeps: 7 × ~1.0s.

### First-200-byte response samples

**storage_reports** (`country=GB&from=2026-05-01&to=2026-05-07`):
```json
{"last_page":1,"total":6,"dataset":"<a href=\"\/historical\/GB\">United Kingdom (Pre-Brexit)<\/a>","gas_day":"2026-05-06","data":[{"name":"United Kingdom (Pre-Brexit)","code":"GB","url":"GB","updatedAt":"2026-05-08 17:36:56","gasDayStart":"2026-05-06","gasDayEnd":"2026-05-07","gasInStorage":"-",...
```

**about_summary** (`/api/about`):
```json
{"SSO":{"Europe":{"Austria":[{"image":"iVBORw0KGgoAAAANSUhEUgAAAIwAAAAzCAIAAADKPnfJAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5...
```

**about_listing** (`/api/about?show=listing`):
```json
[{"name":"GSA LLC","short_name":"GSA","type":"SSO","eic":"25X-GSALLC-----E","country":"AT","url":"https://agsi.gie.eu/api?country=AT&company=25X-GSALLC-----E","facilities":[...
```

**unavailability** (`country=DE&start=2026-04-01&end=2026-05-07`, page 1):
```json
{"last_page":7,"total":30,"dataset":"<a href=\"\/unavailability\/gantt\/DE\">Germany<\/a>","data":[{"published":"2026-05-08 10:40:07","country":{"name":"Germany","code":"DE"},"company":{"name":"EWE Gasspeicher","eic":"21X0000000011756"},...
```

## Registry-vs-docs URL deltas

Per Task 3, every URL was built by reading
`src/gridflow/connectors/gie/endpoints.py::ENDPOINTS` rather than
hardcoding paths. The cross-check against the official GIE API
(`https://agsi.gie.eu/api`) and `docs/gie_agsi_endpoint_catalog.yaml`
yielded:

| Dataset | Registry path | Catalog path | Live URL hit | Δ |
|---|---|---|---|---|
| `storage_reports` | `/api` | `/api` | `/api?country=...&from=...&to=...` | none |
| `storage` | `/api` | (not in catalog as separate dataset, equivalent to `storage_reports`) | `/api?country=...&date=...` | code adds an alias dataset for the country-only scope; functionally same endpoint |
| `about_summary` | `/api/about` | `/api/about` | `/api/about` | none |
| `about_listing` | `/api/about` (with `default_params={"show":"listing"}`) | `/api/about?show=listing` | `/api/about?show=listing` | none — equivalent representations |
| `news` | `/api/news` | `/api/news` | `/api/news` | none |
| `news_item` | `/api/news` (with `default_params={"turl":"{id}"}`) | `/api/news?turl={id}` | `/api/news?turl=1713470` | path same; **upstream filter silently ignored** — see Notable findings |
| `unavailability` | `/api/unavailability` | `/api/unavailability` | `/api/unavailability?country=...&start=...&end=...` | none |

No path-level discrepancies. The two behavioural deltas
(`news_item` `turl` ignored upstream; `news` pagination metadata
absent on live) are upstream API quirks, not connector bugs.

## Notable findings

### 1. ALSI exclusion confirmed — `gie_alsi` connector still loads

Per V1 scope, ALSI LNG (`gie_alsi.lng`) is **deferred**. The plan's
acceptance criterion is that the connector still loads when
`gie_alsi` is present in `config/sources.yaml`.

Verified 2026-05-08 by Python import:

```
gie_agsi datasets: ['storage_reports', 'storage', 'about_summary', 'about_listing', 'news', 'news_item', 'unavailability']
gie_alsi datasets: ['lng']
agsi connector: AgsiConnector
alsi connector: AlsiConnector
OK - both load with gie_alsi config present
```

`gie_alsi.lng` was not exercised. **ALSI is deferred** to a follow-up
phase (backlog item: "Extend E2E coverage to GIE ALSI LNG").

### 2. `news_item` `turl` filter silently ignored upstream

`/api/news?turl=1713470` returned the **byte-identical** body to
`/api/news` (HTTP 200, 2629579 bytes). The `turl` query param has no
effect on the live AGSI response as of 2026-05-08. The connector's
`_is_news_item_detail` shape filter (in
`src/gridflow/connectors/gie/client.py`) detects the listing shape and
discards it with a warning, so silver yield is 0 rows. **No connector
bug.** Logged in
`quant-vault/30-vendors/gie/datasets/news_item.md` →
`## Implementation delta`.

### 3. `news` pagination metadata absent on live response

Catalog YAML declares
`pagination.authoritative_total_pages: last_page` and
`per_page_count: total` for `/api/news`, but the live response
(2026-05-08) contains neither key — only `data` (299 records). The
connector's `_last_page` falls through to default 1, so it does not
loop pages. Effectively single-page in practice; no broken behaviour.
Logged in `quant-vault/30-vendors/gie/datasets/news.md` →
`## Implementation delta`.

### 4. `/api/unavailability` v007 documentation ambiguity

Per `docs/gie_agsi_endpoint_catalog.yaml`, the
`GIE_API_documentation_v007.pdf` cross-check leaves it unclear whether
`/api/unavailability` is part of the AGSI API surface or a separate
portal-only feature. The endpoint is **live-served and returns
well-formed JSON** with `last_page`/`total`/`data` envelope. We treat
it as active per this validation pass. Logged in
`quant-vault/30-vendors/gie/datasets/unavailability.md` →
`## Implementation delta`.

### 5. GB Pre-Brexit `-` placeholders in storage / storage_reports

`country=GB` returns `name: "United Kingdom (Pre-Brexit)"` with `-`
string placeholders for all numeric fields after Brexit. The silver
`_safe_float` helper converts `"-"` to `None`, so the data lands as
nulls. Modelling notes call out that GB Pre-Brexit rows are
effectively unusable post-2020. **No connector bug.** Documented in
both `storage.md` and `storage_reports.md` `## Known issues and
gotchas`.

### 6. Live shapes vs fixtures — fixtures stale (NOT regenerated in V1)

Several fixtures under `tests/fixtures/gie/` predate the current live
API shape:

- `agsi_about_summary_response.json` — flat `{"data": {...}}` envelope;
  live is `{"SSO": {"Europe": ...}}` nested tree.
- `agsi_listing_response.json` — `{"data": [...]}` envelope; live is
  top-level list.
- `agsi_unavailability_response.json` — uses `unavailableCapacity`,
  `eventStart`/`eventEnd`, flat `country`/`company`/`facility` strings;
  live uses `volume`, `start`/`end`, nested
  `{name, code}`/`{name, eic}` dicts.

Connector parsers are flexible enough to handle both shapes via
recursive walkers (`_about_summary_companies`, `_listing_rows`) and
dynamic typing (`AgsiJsonTransformer`). **Fixture regeneration is out
of V1 scope** per `V1-CONTEXT.md` (silver tests depend on fixtures —
needs separate change).

### 7. `gie_alsi` legacy `_fetch_country` uses `till=` not `to=`

The legacy code path (`connectors/gie/client.py::_fetch_country`,
used only by `gie_alsi`) builds query params with
`{"country": ..., "from": ..., "till": ...}`. The current AGSI live
API expects `to=`, not `till=`. Since `gie_alsi.lng` is deferred and
that code path is not exercised in V1, this is not a live bug — but
it will need to be fixed when ALSI is reactivated. Logged here, not
fixed (V1 is documentation + validation only).

## ALSI is deferred

ALSI LNG (`gie_alsi.lng`) is **deferred** from V1 active scope per
project decision (pre-V1, recorded in `V1-CONTEXT.md`). The
connector-still-loads check passed (see Pre-flight + Notable finding 1).
ALSI validation, dataset page, and any `_fetch_country` legacy fixes
are tracked as a backlog item: "Extend E2E coverage to GIE ALSI LNG."

## Verification checklist

| Check | Result |
|---|---|
| 7 dataset pages exist at `quant-vault/30-vendors/gie/datasets/` | yes (storage_reports, storage, about_summary, about_listing, news, news_item, unavailability) |
| Every page contains substring `x-key` | yes |
| Every page contains substring `last_page` or `pagination` | yes |
| `unavailability.md` contains literal `v007` in Implementation delta | yes |
| `endpoints.md` lists 7 active datasets with links | yes |
| `endpoints.md` contains `ALSI` and `deferred` | yes |
| `README.md` has zero `TODO` occurrences | yes (verified `grep -c TODO → 0`) |
| `README.md` contains `## Last validation` heading | yes |
| `total_datasets: 7`, 7 rows in per-dataset table | yes |
| `gie-VALIDATION.md` contains literal phrase `ALSI is deferred` | yes |
| Throttle ≥7s total sleep | yes (7 × ~1s explicit sleeps) |
| `news_item` chosen ID recorded | yes (`turl=1713470`) |
