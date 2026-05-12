---
phase: V1
vendor: entsog
plan_id: V1-PLAN-C-entsog
validated_on: 2026-05-08
total_datasets: 33
pass: 29
empty: 4
fail: 0
---

# ENTSOG — Live validation report

## Summary

| Status | Count | Datasets |
|---|---|---|
| PASS  | 29 | 19 operationalData (minus 3 content empties) + 3 CMP + 1 aggregated + 3 tariffs/UMM + 6 reference + 1 = see table |
| EMPTY | 4  | `methane_content`, `hydrogen_content`, `oxygen_content`, `interruptions` |
| FAIL  | 0  | — |

**Total: 33 active datasets.** No code paths were broken. All four EMPTY
results reflect known data sparsity for the captured pointDirection on
2026-05-06 (or, for `interruptions`, sparsity in the recent window) —
not vendor or connector defects. Widening confirms the indicator name
and route shape are correct.

## Pre-flight

| Check | Command | Result |
|---|---|---|
| Carbon Intensity smoke test | `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity` | `200` |
| ENTSOG operators reachable | `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" "https://transparency.entsog.eu/api/v1/balancingZones?limit=5"` | `200` |
| ENTSOG operationalData reachable | `curl --ssl-no-revoke -sS ... /operationalData?...&indicator=Physical%20Flow` | `200` |

Captured pointDirection tuple from `/operatorPointDirections` (offset=0,
limit=2000, total=1517 records): **`UK-TSO-0001` + `ITP-00005` + `exit`**
(Bacton IUK exit, `hasData=true`). Aggregate-zone tuple:
**`UK---------UK-TSO-0001entryLNG Terminals`**.

Note: a single transient ENTSOG error during pre-flight (504 Gateway
Timeout, then connection-refused on retry) cleared in ~5 seconds. No
later call hit a 5xx.

## Per-dataset validation

Every operational call used `from=2026-05-06&to=2026-05-06&timeZone=UCT&periodType=day&forceDownload=true&limit=1000`.
All requests at 1 req/s throttle.

### Operational data (`/operationalData`, 19 datasets)

| Dataset | Indicator (exact-case) | HTTP | Records | Status | Notes |
|---|---|---|---|---|---|
| `physical_flows` | `Physical Flow` | 200 | 1 | PASS | total=2 (entry+exit at point); record shows `flowStatus=Provisional` |
| `nominations` | `Nomination` | 200 | 1 | PASS | total=2 |
| `allocations` | `Allocation` | 200 | 1 | PASS | total=1 |
| `renominations` | `Renomination` | 200 | 1 | PASS | total=2 |
| `firm_available` | `Firm Available` | 200 | 1 | PASS | |
| `firm_booked` | `Firm Booked` | 200 | 1 | PASS | |
| `firm_technical` | `Firm Technical` | 200 | 1 | PASS | |
| `interruptible_available` | `Interruptible Available` | 200 | 1 | PASS | |
| `interruptible_booked` | `Interruptible Booked` | 200 | 1 | PASS | |
| `interruptible_total` | `Interruptible Total` | 200 | 1 | PASS | |
| `gcv` | `GCV` | 200 | 1 | PASS | |
| `wobbe_index` | `Wobbe Index` | 200 | 1 | PASS | |
| `methane_content` | `Methane Content` | 404 | 0 | EMPTY | Widening to 30 days + dropping `pointDirection` returns 649 KB of rows → indicator name correct, simply not published for ITP-00005 exit |
| `hydrogen_content` | `Hydrogen Content` | 404 | 0 | EMPTY | Same as methane_content; widened call returns 964 KB |
| `oxygen_content` | `Oxygen Content` | 404 | 0 | EMPTY | Same; widened call returns 863 KB |
| `available_through_oversubscription` | `Available through Oversubscription` | 200 | 2 | PASS | |
| `available_through_surrender` | `Available through Surrender` | 200 | 2 | PASS | |
| `available_through_uioli_long_term` | `Available through UIOLI long-term` | 200 | 2 | PASS | |
| `available_through_uioli_short_term` | `Available through UIOLI short-term` | 200 | 1 | PASS | |

**Indicator-name verification.** Every indicator above appears verbatim
in the returned `meta.fields` echo for the 16 PASS calls, and the 3 EMPTY
content indicators returned data when re-queried with a wider window —
confirming the connector's `OPERATIONAL_INDICATORS` map matches the
vendor's accepted strings exactly.

### Capacity Market Platform (`/cmp*`, 3 datasets)

| Dataset | Path | HTTP | Records | Status | Notes |
|---|---|---|---|---|---|
| `cmp_unsuccessful_requests` | `/cmpUnsuccessfulRequests` | 200 | 248 | PASS | `directionKey` capitalised (`Exit`) |
| `cmp_unavailable_firm_capacity` | `/cmpUnavailables` | 200 | 269 | PASS | |
| `cmp_auction_premiums` | `/cmpAuctions` | 200 | 214 | PASS | `isCAMRelevant` (uppercase CAM) |

### Other operational (`/interruptions`, `/aggregatedData`, 2 datasets)

| Dataset | Path | HTTP | Records | Status | Notes |
|---|---|---|---|---|---|
| `interruptions` | `/interruptions` | 404 | 0 | EMPTY | Widened to full year 2024 still 404; widened to 2022-2023 returns 100 records → endpoint works, but our test window (2026-05-06) has no interruption events. Connector's `requires_dates=True` is correct; downstream tooling needs to use a wider window. |
| `aggregated_physical_flows` | `/aggregatedData` | 200 | 1 | PASS | Used aggregate-zone form `UK---------UK-TSO-0001entryLNG Terminals`; record shows `pointsNames="Isle of Grain\|Milford Haven"` and `flowStatus="Provisionnal"` (vendor typo, preserved as-is) |

### Tariffs and bulletins (3 datasets)

| Dataset | Path | HTTP | Records | Status | Notes |
|---|---|---|---|---|---|
| `tariffs` | `/tariffsFulls` | 200 | 1000 | PASS | `countryKey=UK` filter; 2.4 MB response |
| `tariff_simulations` | `/tariffsSimulations` | 200 | 1000 | PASS | 1.4 MB |
| `urgent_market_messages` | `/urgentMarketMessages` | 200 | 100 | PASS | No date params accepted; uses `limit`/`offset` only |

### Reference data (6 datasets)

All reference calls used `limit=100&offset=0` (small page to demonstrate pagination shape; `limit=-1` would return full inventory).

| Dataset | Path | HTTP | Records | Status | Notes |
|---|---|---|---|---|---|
| `connection_points` | `/connectionPoints` | 200 | 100 | PASS | |
| `operators` | `/operators` | 200 | 100 | PASS | `hasData=1` filter applied |
| `balancing_zones` | `/balancingZones` | 200 | 48 | PASS | total=48 (full inventory) |
| `operator_point_directions` | `/operatorPointDirections` | 200 | 96 | PASS | `hasData=1`; full inventory total=1517 (verified separately) |
| `interconnections` | `/interconnections` | 200 | 100 | PASS | `fromCountryKey=UK` |
| `aggregate_interconnections` | `/aggregateInterconnections` | 200 | 27 | PASS | `countryKey=UK`; total=27 |

## Curl evidence

Captured response bodies live at
`.tmp/entsog/<dataset>.json` inside the worktree (gitignored). One
representative call for each family:

### `/operationalData` (physical_flows)

```bash
curl --ssl-no-revoke -sS \
  "https://transparency.entsog.eu/api/v1/operationalData\
?from=2026-05-06&to=2026-05-06&timeZone=UCT\
&indicator=Physical%20Flow&periodType=day\
&pointDirection=UK-TSO-0001ITP-00005exit\
&forceDownload=true&limit=1000" \
  -o .tmp/entsog/physical_flows.json \
  -w "%{http_code}|%{size_download}|%{time_total}\n"
# → 200|1971|0.156885
```

### Empty (404) example (methane_content)

```bash
curl --ssl-no-revoke -sS \
  "https://transparency.entsog.eu/api/v1/operationalData\
?from=2026-05-06&to=2026-05-06&timeZone=UCT\
&indicator=Methane%20Content&periodType=day\
&pointDirection=UK-TSO-0001ITP-00005exit\
&forceDownload=true&limit=1000" \
  -o .tmp/entsog/methane_content.json \
  -w "%{http_code}|%{size_download}|%{time_total}\n"
# → 404|29|0.121120
# body: {"message":"No result found"}
```

### Widened methane_content (proves indicator name correct)

```bash
curl --ssl-no-revoke -sS \
  "https://transparency.entsog.eu/api/v1/operationalData\
?from=2026-04-08&to=2026-05-08&timeZone=UCT\
&indicator=Methane%20Content&periodType=day\
&forceDownload=true&limit=1000" \
  -o .tmp/entsog/methane_content_widened.json \
  -w "%{http_code}|%{size_download}\n"
# → 200|649552
```

### CMP example

```bash
curl --ssl-no-revoke -sS \
  "https://transparency.entsog.eu/api/v1/cmpUnavailables\
?from=2026-05-06&to=2026-05-06&timeZone=UCT&periodType=day\
&forceDownload=true&limit=1000" \
  -o .tmp/entsog/cmp_unavailable_firm_capacity.json \
  -w "%{http_code}|%{size_download}|%{time_total}\n"
# → 200|287906|0.225073
```

### Reference example

```bash
curl --ssl-no-revoke -sS \
  "https://transparency.entsog.eu/api/v1/operatorPointDirections\
?limit=100&offset=0&hasData=1" \
  -o .tmp/entsog/operator_point_directions.json \
  -w "%{http_code}|%{size_download}|%{time_total}\n"
# → 200|277151|0.150407
```

## Cross-cutting Implementation deltas

These apply to multiple dataset pages and are recorded once here.

1. **Vendor empty convention is HTTP 404, not HTTP 200 + `[]`.** ENTSOG
   returns `HTTP 404` with body `{"message":"No result found"}` when an
   indicator/window/point combination produces no rows. The connector's
   `@RETRY_POLICY` retries on `httpx.HTTPStatusError`, which would retry
   404 up to 5 times before reraising — wasteful but not incorrect.
   Optional follow-up: short-circuit 404 in `EntsogConnector._request`
   so empty windows do not consume retry budget. **Not in V1 scope** —
   logged as a backlog candidate.

2. **`timeZone=UCT` accepted; `meta.timezone=CET` echoed.** Despite
   passing `timeZone=UCT`, the response `meta.timezone` always echoes
   `CET`, and `periodFrom` / `periodTo` carry `+02:00` (CEST) or
   `+01:00` (CET) offsets. `parse_entsog_datetime` converts to UTC, so
   downstream silver/gold see correct UTC instants. The vendor's
   `timeZone` parameter appears to control window interpretation, not
   response formatting.

3. **`directionKey` casing varies across families.** Lowercase
   (`entry`/`exit`) in `/operationalData`, `/interruptions`,
   `/aggregatedData`. Capitalised (`Exit`) in
   `/cmpUnsuccessfulRequests`. Generic silver normalises to lowercase
   snake_case but does NOT case-normalise the value — downstream code
   that filters on direction must compare case-insensitively.

4. **`isCAMRelevant` (uppercase CAM) vs `isCamRelevant` across
   endpoints.** Same logical field, two casings. The generic silver
   transformer (`silver/entsog/generic.py:_normalise_column_names`)
   coalesces both into a single snake_case `is_cam_relevant` column via
   `pl.coalesce`. Same pattern for `isCMPRelevant`/`isCmpRelevant`.

5. **Datetime placeholders tolerated.** Live `cmp_unsuccessful_requests`
   records show `auctionFrom: "N/A"` and `auctionTo: "N/A"`. The silver
   transformer's `parse_entsog_datetime` returns `None` for placeholder
   values (`""`, `"-"`, `"N/A"`, `"null"`), producing null Polars
   datetime cells rather than raising.

6. **Synthetic / stale fixture.**
   `tests/fixtures/entsog/physical_flows_response.json` carries
   placeholder `pointKey: "IUK"` and `operatorKey: "OP-IUK"` — these do
   not exist in live data (real keys are `ITP-00005` /
   `UK-TSO-0001`). Fixture regeneration is **deferred** per V1 policy
   (silver tests depend on the placeholder shape).

7. **`physical_flows` deliberately omits `pointDirection`** in
   `endpoints.py:122`. This produces a full-system snapshot in one
   call (the dataset's documented shape is "all interconnection points
   in one response"). Validated by setting `point_directions=None` for
   `physical_flows` only; all other operationalData datasets pass
   `DEFAULT_POINT_DIRECTIONS`.

8. **`flowStatus: "Provisionnal"` (vendor typo)** appears in
   `aggregated_physical_flows`. Preserved as-is in bronze.

9. **`/aggregatedData` capitalisation in the API manual PDF** is
   inconsistent (`/AggregatedData` in one example, `/aggregatedData` in
   the connector). Live API accepts the connector's spelling
   (`/aggregatedData`) — we did not test the alternative. No action.

10. **Reference endpoints echo `total` in `meta`.** `balancing_zones`
    total=48, `operator_point_directions` total=1517 (full inventory),
    `aggregate_interconnections` total=27. Connector's `limit=-1` plus
    `requires_dates=False` for reference fetches is correct.

## Files written

- 33 dataset pages: `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\datasets\<key>.md`
- Updated `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\endpoints.md`
- Updated `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\README.md` (zero TODO)
- This file.

## Blockers

None.

---

## V2 re-validation (2026-05-09)

**Fix commit:** `8c8da2d` — `fix(V2-E): ENTSOG short-circuits 404+empty body; ngeso deleted`.

### V2-FIX-07 — `@RETRY_POLICY` 404 short-circuit

V1 entsog-VALIDATION Findings §1 confirmed ENTSOG returns HTTP 404
with body `{"message":"No result found"}` as the documented empty
convention. The pre-V2 connector decorated `_request` with
`@RETRY_POLICY` (5 attempts on `httpx.HTTPStatusError`) and would
retry that response 5 times before reraising — wasted budget for an
expected vendor outcome.

V2-FIX-07: `EntsogConnector._request` now inspects 404 responses; if
the body matches the empty convention, it returns the response
immediately without retries. Other 404s (genuine errors with a
different body) preserve the existing `raise_for_status()` + retry
path.

**Regression tests** in
`tests/integration/test_entsog_mocked_e2e.py::TestV2ENTSOG404ShortCircuit`:

- `test_404_no_result_found_short_circuits_no_retry` — RED before
  fix (5 requests captured); GREEN after (1 request,
  `response.http_status == 404`).
- `test_genuine_404_preserves_retry` — GREEN before AND after;
  counts > 1 requests on a non-empty 404 body and confirms
  `HTTPStatusError` is raised. Locks the no-regression contract for
  genuine errors.

### V2-TRIAGE-01 — `connectors/ngeso/` deleted

V1 close-out flagged the empty placeholder package
(`src/gridflow/connectors/ngeso/__init__.py` only). Verified
pre-deletion: 0 imports of `gridflow.connectors.ngeso` across
`src/`, `tests/`, `config/`. Deleted via `git rm` + `rmdir`.

NGESO operational endpoints are out of current gridflow scope; if
they become in-scope post-v0.10, a fresh package can be created
without the now-stale placeholder ambiguity.

### Vault changes

- `30-vendors/entsog/README.md` "Known gotchas" — 404 short-circuit
  bullet expanded with V2 commit reference, mechanism, and
  pre-V2-vs-post-V2 contrast. `updated: 2026-05-09`.
