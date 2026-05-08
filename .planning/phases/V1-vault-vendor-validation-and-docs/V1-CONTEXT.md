# Phase V1: Vault Vendor Validation And Docs - Context

**Gathered:** 2026-05-08
**Status:** Ready for planning
**Source:** User scoping conversation (2026-05-08) + advisor review

<domain>
## Phase Boundary

V1 is a **documentation + live-validation** phase. It does not modify any
production source code (connector, transformer, schema, or CLI files).

In scope:
- Read every active gridflow endpoint definition from `config/sources.yaml` and
  `src/gridflow/connectors/<vendor>/endpoints.py`.
- Read every official vendor API documentation reference at the URLs in
  `~/.claude/skills/gridflow-dataset-spec/references/vendor-doc-urls.md`.
- Hit each active endpoint at least once with reasonable parameters and
  classify it as PASS / FAIL / EMPTY.
- Populate `quant-vault/30-vendors/<vendor>/datasets/<dataset_key>.md` for
  every active dataset using the `gridflow-dataset-spec` skill template.
- Refresh `quant-vault/30-vendors/<vendor>/endpoints.md` and `README.md`.
- Write a per-vendor `<vendor>-VALIDATION.md` report inside the V1 phase
  directory, with PASS/FAIL/EMPTY status, cause, curl evidence, and a link to
  the dataset page.

Out of scope:
- Modifying connector / silver / schema source code. If a doc/code conflict is
  found, record it in the dataset page's `## Implementation delta` section and
  in `<vendor>-VALIDATION.md`. Do not edit Python source.
- Writing new tests, Pydantic schemas, or CLI commands.
- `gie_alsi` (LNG) — explicitly deferred from active scope (project decision,
  pre-V1).
- `connectors/ngeso/` — directory contains only `__init__.py`. Flag in the
  V1 summary, but do not document it.
- Renaming the `connectors/openmeteo/` package or the `30-vendors/open-meteo/`
  vault folder. Document the naming inconsistency (vault `open-meteo`, code
  `openmeteo`, config `open_meteo`) in the Open-Meteo VALIDATION report only.

</domain>

<decisions>
## Implementation Decisions

### Phase shape (locked)

- **Single phase V1 with nine parallel plans**, executing in one wave.
  Chosen 2026-05-08 over six separate phases to avoid 6× the GSD scaffolding
  for uniform documentation work. ENTSOE is split into four family-batch
  plans because 48 endpoints in one Task would exceed reasonable agent
  runtime and context budget (advisor recommendation).
- Plan IDs:
  - `V1-PLAN-A-elexon` — Elexon (33 datasets)
  - `V1-PLAN-B1-entsoe-load-prices` — ENTSOE load + prices + imbalance (≈11)
  - `V1-PLAN-B2-entsoe-generation-outages` — ENTSOE generation + outages (≈13)
  - `V1-PLAN-B3-entsoe-transmission-capacity` — ENTSOE transmission +
    capacity allocation (≈18)
  - `V1-PLAN-B4-entsoe-balancing` — ENTSOE balancing (≈8) — paired with B3
    rate-limit-wise but logically separate
  - `V1-PLAN-C-entsog` — ENTSOG (33 datasets)
  - `V1-PLAN-D-gie` — GIE AGSI (7 endpoints)
  - `V1-PLAN-E-neso` — NESO (33 datasets, refresh in place)
  - `V1-PLAN-F-openmeteo` — Open-Meteo (2 datasets)

### Vault absolute path (locked — prevents silent wrong-directory writes)

The Obsidian vault is **outside** the gridflow git tree:

```
VAULT_ROOT = C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault
VENDOR_ROOT = C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors
```

Every plan's tasks MUST use the absolute `VENDOR_ROOT` path when writing
dataset pages, README updates, or `endpoints.md`. The worktree CWD is
`C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.claude\worktrees\lucid-mccarthy-9ed3e0`
— a relative path like `quant-vault/...` from there points at a
non-existent worktree-local directory. Agents that resolve relative paths
will silently write to the wrong place.

**Per-vendor write paths (locked):**
- Elexon → `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\`
- ENTSOE → `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\`
- ENTSOG → `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\`
- GIE → `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\`
- NESO → `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\`
- Open-Meteo → `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\`
  (note dash in vault path — keep as-is; do not rename)

Per-vendor `VALIDATION.md` reports stay inside the gridflow worktree at
`.planning/phases/V1-vault-vendor-validation-and-docs/<vendor>-VALIDATION.md`.

### Endpoint scope (locked)

- **Active pipeline datasets only** — defined as datasets present in
  `config/sources.yaml` under each vendor block. This excludes deferred,
  excluded, or planned-but-unimplemented endpoints from existing
  `endpoints.md` and `docs/<vendor>_endpoint_catalog.yaml` files.
- Per-vendor counts (locked from `config/sources.yaml` 2026-05-08):
  - Elexon: 33 datasets
  - ENTSOE: 48 endpoints
  - ENTSOG: 33 datasets
  - GIE AGSI: 7 endpoints (gie_alsi deferred separately)
  - NESO: 33 datasets (existing 33 vault pages — validate in place)
  - Open-Meteo: 2 datasets (`archive`, `forecast`)
  - **Total: 156 datasets**

### NESO handling (locked)

- **Validate + refresh in place.** Read each existing
  `30-vendors/neso/datasets/<key>.md` page, tick it against the live API and
  source code, fix drift, leave correct content alone. Do **not** rewrite
  pages whose content already matches reality.

### Live API access (locked)

- API keys for Elexon, ENTSOE, GIE are present in
  `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env` (main repo).
- The worktree at
  `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.claude/worktrees/lucid-mccarthy-9ed3e0`
  does **not** have `.env` (gitignored, not propagated to worktrees).
- **Pre-flight task in every plan:** copy `.env` from the main repo to the
  worktree before any live call. Do not commit the copy. Do not modify the
  main repo's `.env`.
- ENTSOG, NESO, Open-Meteo are public — no key needed.

### Authority hierarchy (locked, from gridflow-dataset-spec skill)

When official docs, fixtures, and code disagree:
1. **Highest:** Official vendor API documentation
2. Next: `tests/fixtures/<vendor>/` (evidence of real response shape)
3. Lowest: Codebase (`connectors/`, `silver/`, `schemas/`)

Conflicts are logged in the dataset page's `## Implementation delta` and in
the per-vendor `VALIDATION.md`. **Never silently adopt the code's version.**

### Per-vendor deliverables (locked)

For each vendor, the plan produces all four:
1. **Per-dataset pages** in `quant-vault/30-vendors/<vendor>/datasets/<key>.md`
   following the `gridflow-dataset-spec` skill template verbatim.
2. **Updated `endpoints.md`** quick-summary table with every active dataset
   key, path, parameter style, and one-line description.
3. **Updated `README.md`** with auth method, rate limit, status URL, and
   known-gotchas all confirmed (no remaining `TODO`).
4. **`<vendor>-VALIDATION.md`** in the phase directory
   (`.planning/phases/V1-vault-vendor-validation-and-docs/`) recording PASS /
   FAIL / EMPTY per endpoint, cause, raw curl command, HTTP status, and any
   `## Implementation delta` items.

### Validation pass criteria (grep-checkable)

Each endpoint's status is decided by these concrete checks:

- **PASS:** Live GET returns 2xx with at least one row matching the schema /
  fixture, AND the request shape in code matches the official-docs request
  shape (criterion is vendor-specific — see below).
- **FAIL:** Live GET returns 4xx/5xx unexpectedly, **or** request shape in
  code differs materially from docs (deprecated path / param, wrong base,
  wrong required param), **or** Pydantic schema rejects the response.
- **EMPTY:** Live GET returns 2xx but with zero rows. The plan must
  investigate cause (deprecated path, wrong date window, requires filter,
  known-empty by design, etc.) and document it in `<vendor>-VALIDATION.md`.

**Vendor-specific "request shape matches docs" criterion (locked):**

- **Elexon, ENTSOG, GIE, NESO, Open-Meteo (path-based):** URL path in
  `connectors/<vendor>/endpoints.py` matches the official-docs path
  character-for-character.
- **ENTSOE (single-endpoint, parameter-based):** ENTSOE has one base
  endpoint `/api?<params>`. Validation criterion is **the
  (documentType, processType, businessType, area-param-name) tuple matches
  the API guide PDF for that data type**. Path equivalence is meaningless —
  every dataset uses the same `/api` path. Record the tuple in each ENTSOE
  dataset page.
- **Open-Meteo two-host gotcha:** forecast lives at
  `https://api.open-meteo.com/v1/forecast` and historical lives at
  `https://archive-api.open-meteo.com/v1/archive` — different host. The
  `config/sources.yaml` `base_url` for `open_meteo` is
  `https://api.open-meteo.com/v1` and the `historical` dataset path is
  `archive`. The Open-Meteo plan MUST verify the connector overrides the
  host for the archive endpoint, OR record this as a `## Implementation
  delta` (and likely a FAIL) if it does not.

### Rate-limit awareness (locked)

- **Elexon:** 2 req/s — use `asyncio.sleep(0.5)` between live calls.
- **GIE AGSI:** 60 calls/min (1/s) — use `asyncio.sleep(1)` between live
  calls.
- **ENTSOE:** Vendor-published limit not documented. Treat as 1 req/s
  default. Backoff on 429.
- **ENTSOG:** Public, no rate limit specified. Default 1 req/s.
- **NESO:** Public, no rate limit specified. Default 5 req/s (config uses 10
  but be polite).
- **Open-Meteo:** Public, free tier soft limit ~10000 req/day. 1 req/s for
  validation is fine (we only have 2 endpoints).

### Pre-known config oddities to investigate (locked)

- **NESO duplicate:** `intensity_current` and `carbon_intensity` both map
  to path `/intensity` in `config/sources.yaml`. The NESO plan MUST
  determine whether one is an alias for the other or whether they
  legitimately produce different silver outputs. Record finding in the
  NESO `VALIDATION.md` and either create two pages with cross-links or one
  page with the alias noted.
- **Open-Meteo two-host:** see Validation criteria block above. The
  Open-Meteo plan MUST verify the connector handles two hosts.
- **`gie_alsi`:** present in `config/sources.yaml` but excluded from V1
  scope. The GIE plan should verify the connector still loads without
  error and note that ALSI is deferred — do not document its `lng` dataset.
- **`connectors/ngeso/`:** empty package (only `__init__.py`). Mention in
  the V1 phase summary; not in any plan.

### Live API call mechanism (locked — Avast TLS quirk)

**Use `curl --ssl-no-revoke` for every live validation call. Do not use
Python `httpx` directly.**

Background: this Windows machine runs Avast antivirus which intercepts
TLS connections and substitutes its own root cert. Avast's root is in the
Windows cert store but **not** in Python's certifi bundle, so:

- `curl https://...` (default) fails with `CRYPT_E_NO_REVOCATION_CHECK` —
  Windows cert store is reachable but revocation servers are blocked.
- `curl --ssl-no-revoke https://...` succeeds (200 OK against
  carbonintensity, entsog, etc.).
- `python -c "import httpx; httpx.get(...)"` fails with
  `CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate`.

This is a per-machine workstation quirk, not a gridflow or vendor problem.
For the V1 documentation phase agents only validate **URL shape, response
shape, and field semantics**, not the gridflow connector's SSL stack —
`curl --ssl-no-revoke` is the correct tool.

### Pre-flight smoke test (mandatory in every plan)

Before any vendor-specific live call, each plan must run:

```
curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity
```

Expected output: `200`. If this fails, halt the plan and write the failure
to `<vendor>-VALIDATION.md` — every subsequent live call will share the
same root cause.

### Live-call request/response capture pattern

Each plan's tasks should capture each live call's evidence with the
following pattern (so the dataset page's "Working curl example" and the
VALIDATION.md evidence both come from one canonical record):

```
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://api.example.com/<path>?<params>" \
  -o "/tmp/<vendor>-<dataset>.json" \
  -w "HTTP %{http_code} | %{size_download} bytes | %{time_total}s\n" \
  2> "/tmp/<vendor>-<dataset>.err"
```

Record:
- The exact curl command (paste verbatim into the dataset page).
- HTTP status, bytes, time.
- First ~200 bytes of the response body (paste into "Bronze sample" section
  of the dataset page).
- For ENTSOE (XML): use `--header "Accept: application/xml"` and capture
  the document type / process type tuple from the response root element.

### Claude's Discretion

- Choice of "reasonable parameters" for each live call — pick recent dates
  with known good data (typically yesterday or 7 days ago for daily / weekly
  datasets, last hour for half-hourly), valid GB or major-EU areas for
  ENTSOE, and a single representative connection point for ENTSOG.
- Fixture selection — agents may use existing files in `tests/fixtures/` as
  ground-truth response shape if a live call returns empty.
- Internal organisation of `endpoints.md` (group by parameter style, by
  document type, or by data family) — pick whatever matches the existing
  vendor's `endpoints.md` style.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before writing dataset pages.**

### Skill (mandatory)

- `~/.claude/skills/gridflow-dataset-spec/SKILL.md` — defines the dataset
  page template, authority hierarchy, and step protocol. Use the template
  in `references/spec-template.md` exactly. No section additions, removals,
  or reorderings.
- `~/.claude/skills/gridflow-dataset-spec/references/vendor-doc-urls.md` —
  authoritative vendor docs URL list.
- `~/.claude/skills/gridflow-dataset-spec/references/spec-template.md` —
  page template.

### Project rules

- `CLAUDE.md` (root) — gridflow hard rules. Settlement period 1..50 (not
  1..48). Settlement run dedup includes `run_type`. Gas day with 06:00 UTC
  offset. BM unit IDs not normalised. No pandas. Polars only.
- `.planning/STATE.md` — project decisions and history (read-only).

### Source files (read for cross-reference, do not modify)

- `config/sources.yaml` — registered endpoint, schedule, max_query_days.
- `src/gridflow/connectors/<vendor>/endpoints.py` — endpoint path, param
  style, param names.
- `src/gridflow/connectors/<vendor>/client.py` — request building, auth.
- `src/gridflow/silver/<vendor>/<dataset>.py` — silver field mapping.
- `src/gridflow/schemas/<vendor>.py` — Pydantic schema, types, constraints.
- `tests/fixtures/<vendor>/` — real response shapes.

### Per-vendor existing vault state (starting point)

- `quant-vault/30-vendors/elexon/` — README + endpoints.md, **empty
  datasets/**.
- `quant-vault/30-vendors/entsoe/` — README + endpoints.md, **empty
  datasets/**.
- `quant-vault/30-vendors/entsog/` — README + endpoints.md, **empty
  datasets/**.
- `quant-vault/30-vendors/gie/` — README + endpoints.md, **empty datasets/**.
- `quant-vault/30-vendors/neso/` — README + endpoints.md + **33 dataset
  pages** (validate-and-refresh, do not rewrite if accurate).
- `quant-vault/30-vendors/open-meteo/` — README + endpoints.md, **empty
  datasets/**.

</canonical_refs>

<specifics>
## Specific Ideas

### Per-vendor active dataset list (locked from config/sources.yaml)

**Elexon (33):** system_prices, boal, disbsad, freq, fuelhh, fuelinst,
imbalngc, mid, netbsad, ndf, ndfd, pn, melngc, fou2t14d, uou2t14d, windfor,
temp, agpt, agws, atl, indo, itsdo, indod, nonbm, inddem, indgen, tsdf,
tsdfd, lolpdrm, remit, soso, market_depth, bmunits_reference

**ENTSOE (48):** day_ahead_prices, actual_load, load_forecast,
actual_generation, wind_solar_forecast, cross_border_flows,
outages_generation, outages_consumption, outages_transmission,
outages_offshore_grid, outages_production, installed_capacity,
installed_capacity_units, generation_forecast, actual_generation_units,
water_reservoirs, generation_units_master_data, load_forecast_weekly,
load_forecast_monthly, load_forecast_yearly, forecast_margin,
net_transfer_capacity, dc_link_intraday_transfer_limits, commercial_schedules,
commercial_schedules_net_positions, redispatching_cross_border,
redispatching_internal, countertrading, congestion_management_costs,
offered_transfer_capacity_continuous, offered_transfer_capacity_implicit,
offered_transfer_capacity_explicit, auction_revenue, transfer_capacity_use,
total_nominated_capacity, total_capacity_allocated, congestion_income,
net_positions, imbalance_prices, imbalance_volume, activated_balancing_prices,
contracted_reserves, current_balancing_state, balancing_energy_bids,
aggregated_balancing_energy_bids, procured_balancing_capacity,
cross_zonal_balancing_capacity, balancing_financial_expenses_income

**ENTSOG (33):** physical_flows, nominations, allocations, renominations,
firm_available, firm_booked, firm_technical, interruptible_available,
interruptible_booked, interruptible_total, gcv, wobbe_index, methane_content,
hydrogen_content, oxygen_content, available_through_oversubscription,
available_through_surrender, available_through_uioli_long_term,
available_through_uioli_short_term, cmp_unsuccessful_requests,
cmp_unavailable_firm_capacity, cmp_auction_premiums, interruptions,
aggregated_physical_flows, tariffs, tariff_simulations, urgent_market_messages,
connection_points, operators, balancing_zones, operator_point_directions,
interconnections, aggregate_interconnections

**GIE AGSI (7):** storage_reports, storage, about_summary, about_listing,
news, news_item, unavailability

**NESO (33):** intensity_current, intensity_today, intensity_date,
intensity_period, intensity_factors, intensity_at, intensity_fw24h,
intensity_fw48h, intensity_pt24h, carbon_intensity, intensity_stats,
intensity_stats_block, generation_current, generation_pt24h, generation,
regional_current, regional_england, regional_scotland, regional_wales,
regional_postcode, regional_regionid, regional_intensity_fw24h,
regional_intensity_fw24h_postcode, regional_intensity_fw24h_regionid,
regional_intensity_fw48h, regional_intensity_fw48h_postcode,
regional_intensity_fw48h_regionid, regional_intensity_pt24h,
regional_intensity_pt24h_postcode, regional_intensity_pt24h_regionid,
regional_intensity, regional_intensity_postcode, regional_intensity_regionid

**Open-Meteo (2):** historical (path `archive`), forecast (path `forecast`)

### Reasonable test parameters (default per vendor)

- Settlement-date style (Elexon): `settlementDate=2026-05-06` (two days ago).
- Publish-datetime style (Elexon): `publishDateTimeFrom=2026-05-06T00:00Z`,
  `publishDateTimeTo=2026-05-07T00:00Z`.
- ENTSOE day-ahead prices: `in_Domain=10YGB----------A`,
  `out_Domain=10YGB----------A`,
  `periodStart=202605060000`, `periodEnd=202605070000`.
- ENTSOG: pick a single connection point from
  `tests/fixtures/entsog/operator_point_directions.json` if present, default
  to British Gas (`pointKey=BRENG-X`, `operatorKey=21X-GB-A-A0A0A-Z`,
  `directionKey=entry`) if no fixture.
- GIE AGSI: `country=GB&from=2026-05-01&to=2026-05-07` for storage; default
  query for about_summary.
- NESO: dates `from=2026-05-06T00:00Z`, `to=2026-05-06T23:30Z`; postcode
  `RG41` for postcode-based; regionid `13` (London) for regionid-based.
- Open-Meteo: `latitude=51.51&longitude=-0.1&start_date=2025-05-01&end_date=2025-05-07&hourly=temperature_2m`
  for archive; `latitude=51.51&longitude=-0.1&hourly=temperature_2m` for
  forecast.

### Per-vendor doc URL anchors

| Vendor | Primary doc URL |
|--------|-----------------|
| Elexon | https://developer.elexon.co.uk/api-details#api=prod-insol-insights-api (Swagger: https://bmrs.elexon.co.uk/api-documentation) |
| ENTSOE | https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.pdf |
| ENTSOG | https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_API%20-%20v2.1.pdf |
| GIE AGSI | https://agsi.gie.eu/api |
| NESO | https://carbon-intensity.github.io/api-docs |
| Open-Meteo | https://open-meteo.com/en/docs/historical-weather-api , https://open-meteo.com/en/docs |

</specifics>

<deferred>
## Deferred Ideas

- ALSI LNG validation and docs — `gie_alsi` excluded. Already a backlog item
  (`Extend E2E coverage to GIE ALSI LNG`).
- `connectors/ngeso/` — empty placeholder. Triage in a follow-up phase
  (delete or implement); not in V1 scope.
- Scheduled live endpoint monitoring — already a backlog item. V1 is one
  point-in-time validation pass; ongoing monitoring is out of scope.
- Vault renames (`open-meteo` → `openmeteo` to match code) — leave naming
  as-is and document the inconsistency. Renaming is a separate backlog item.
- Fixture regeneration — if a live call shows that a fixture is stale, log
  it in `## Implementation delta` but do **not** regenerate fixtures in V1
  (silver tests depend on them; needs a separate change).
- New silver schemas / Pydantic field additions — log doc-vs-code field
  deltas only. Code changes are out of V1 scope.

</deferred>

---

*Phase: V1-vault-vendor-validation-and-docs*
*Context gathered: 2026-05-08 via direct user scoping (no /gsd-discuss-phase)*
