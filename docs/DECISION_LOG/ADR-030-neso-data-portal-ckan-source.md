# ADR-030 - NESO Data Portal (CKAN) as a gridflow source

**Status:** Proposed
**Date:** 2026-08-19
**Phase:** `neso-data-portal`, `.planning/phases/neso-data-portal/PLAN.md` (revision 14)
**Cross-references:** ADR-023 (connector per-unit failure contract — the definitive-absent
guard this source applies), ADR-024 (silver schema manifest — designated date columns),
ADR-025 (temporal vintage and revision capture — the `available_at` contract this source's
`APPEND_ONLY` + `VINTAGE_PER_BRONZE_FILE` combination implements), ADR-027 (watermark
advance safety), ADR-028 (bronze vouching; `BronzeVouchReason` gains a member here, and
the `VINTAGE_PER_BRONZE_FILE` branch is brought up to its accounting contract), ADR-029
(bronze is never deleted, repaired or written to), `connectors/neso_data_portal/client.py`,
`silver/csv_bronze.py`, `silver/base.py`, `pipeline/runner.py`

> Provenance: written in stage B1c of the `neso-data-portal` phase, after B1a (connector,
> CSV reader, guards, config, wiring) and B1b (vault snapshot materializer) landed and
> before B2/B3a (the three transformers). `status: proposed` — this is a **decision
> record, not a completion record**, and every decision below carries its implementation
> status at time of writing so the claim is checkable rather than blanket.

## Context

NESO (National Energy System Operator) publishes GB power-system datasets through a CKAN
instance at `https://api.neso.energy`. Three packages are taken in this phase:
`daily-wind-availability`, `historic-generation-mix` and
`embedded-wind-and-solar-forecasts`.

This is gridflow's first **CKAN** source, its first **CSV-bodied bronze** source, and its
first source whose resources are **whole-file snapshots with no server-side date filter**.
Each of those three firsts drove decisions that a JSON, date-windowed API source would not
have needed.

The evidence base is a Stage-A live capture stored under
`.planning/phases/neso-data-portal/_probe/` (28 files, captured 2026-08-16). It is cited
inline below. **It is corroboration, not a vendor contract** — the distinction is made
explicitly wherever the capture is load-bearing, because a probe that agrees with a guess
is not a specification.

---

## Decision

Each item is tagged **[implemented: B1a]**, **[implemented: B1b]**, **[implemented: B1c]**
or **[decided, scheduled: B2/B3a]**.

### Identity and scope — D-01, D-03, D-04, D-05

**D-01 — a new source key, `neso_data_portal`.** **[implemented: B1a]**
Packages `gridflow.connectors.neso_data_portal`, `gridflow.silver.neso_data_portal`,
`gridflow.schemas.neso_data_portal`. The pre-existing `neso` source is a different API and
is not touched.

**D-03 — CKAN identity is recorded verbatim from the probe.** **[implemented: B1a]**
From `_probe/package_search_p*.json` and the per-package
`_probe/show_<package>.json` captures, all three target resources are
`url_type: upload`, `format: CSV`, `datastore_active: true`:

| dataset key | package slug | `resources[].name` (exact) | current filename |
|---|---|---|---|
| `daily_wind_availability` | `daily-wind-availability` | `Daily Wind Availability` | `windavailability.csv` |
| `historic_generation_mix` | `historic-generation-mix` | `Historic GB Generation Mix` | `df_fuel_ckan.csv` |
| `embedded_wind_solar_forecast` | `embedded-wind-and-solar-forecasts` | `Embedded Solar and Wind Forecast` | `202608161825_embedded_forecast.csv` |

Resource UUIDs are captured as fetch-time provenance and are **never** used as selectors —
CKAN re-creates resources, and a UUID selector would silently stop matching.

**D-04 — selection is an exact `resources[].name` string match.** **[implemented: B1a]**
Zero matches or more than one raises `NesoResourceSelectionError`, naming the dataset, the
expected name and every name the package actually returned. No fuzzy match, no
`"Archive"`-substring fallback, no `last_modified` tie-break. The packages carry Archive
resources with near-identical names (`_probe/show_embedded-wind-and-solar-forecasts.json`
lists eight of them), so a loose matcher would silently take a 2019 archive instead of the
live file.

**D-05 — the datastore API is not used.** **[implemented: B1a]**
`/datastore/dump/<id>`, `datastore_search` and `datastore_search_sql` appear on no code
path. All three D-03 resources are `url_type: upload`, so none needs them, and NESO's
guidance rate-limits the datastore family at 2 req/min — a class whose applicability to
`/datastore/dump` is unstated (TODO-01).

### The fetch primitive — D-39

**D-39 — one owner for every byte on the wire.** **[implemented: B1a]**

`NesoDataPortalConnector._send(request, *, stream=False) -> httpx.Response` is the **only**
site in the package that performs network I/O. It is worth recording at length, because it
replaced a composition of five separately-written decisions that had stopped holding.

**Why one primitive rather than a fourth patch.** The superseded design mandated
`client.stream(...)` in one decision and permitted network I/O only through
`_send(request)` in another. Those cannot both be true: `AsyncClient.stream` is
`build_request` + `send(stream=True)` + `aclose()` in a `finally` (read in the pinned httpx
0.28.1), so it bypasses the throttle and hands back a **closed** response. Three further
responsibilities were unowned by any decision: which checks sit inside the retry boundary,
who closes a response nobody read, and which byte count `Content-Length` is being compared
against. One owner settles all four.

**Why `client.stream` cannot coexist with the primitive.** It is not a style preference.
`stream` performs its own send, so a throttle placed in `_send` never runs for it, and the
per-send pacing invariant (D-07) becomes false without any code that looks wrong.
`client.stream` and an `aiter_bytes()` byte count are both explicitly prohibited, and two
cheap textual assertions pin that prohibition.

**Why status classification sits inside the retry boundary.** `_send` is decorated with
`RETRY_POLICY` and calls `raise_for_status` within it — the repo's existing idiom
(`entsoe/client.py`, and the same in elexon, gie and openmeteo). A returned 5xx therefore
enters the retry policy, and because the throttle is *also* inside `_send`, every retry
attempt is throttled like any other send. The gate is `is_success`, deliberately not
`is_error`: a `304`, or a `3xx` carrying no `Location`, is neither an error nor a redirect,
and an `is_error` gate would return it as though it were a body, surfacing later as a
bewildering parse error.

**Why the transfer is forced to identity coding.** httpx's default request carries
`accept-encoding: gzip, deflate`. Under any content coding, `Content-Length` describes the
**encoded** representation while `aiter_bytes()` yields **decoded** bytes — comparing those
two would classify every compressed CSV as truncated. Rather than reconcile two counters,
the coding is removed from the path: the request asks for `identity`; a final response
carrying any other `Content-Encoding` raises `NesoUnexpectedEncodingError` naming the
value (we asked, and we do not guess what the vendor did instead); and the body is read
with `aiter_raw()`, so the bytes counted, capped and written to bronze are the same bytes
`Content-Length` describes. One counter, one meaning. Adding the request header cannot
break the presigned signature: the R2 URL carries `X-Amz-SignedHeaders=host`
(`_probe/sample_historic-generation-mix.headers`), so `Host` is the only signed header.

**Why an interrupted body read is deliberately not retried.** The body is consumed *after*
`_send` returns, so a mid-stream `httpx.RemoteProtocolError` (translated to
`NesoTruncatedBodyError`) or `httpx.ReadTimeout` falls outside `_send`'s retry policy. Both
fail the fetch. A retry there would re-enter at the file host on a presigned URL that may
already be spent, and would need its own attempt budget nested inside `_send`'s five.
Nothing partial can reach bronze either way, and the next run re-resolves the URL from
scratch. The asymmetry is stated rather than hidden — CKAN action calls read their body
inside `_send` and so do get transport retries on the body; the streamed file leg gets them
on the header phase only. Recorded as TODO-11.

**Target validation lives inside the primitive.** `_send` validates `request.url` before it
does anything else — not the caller, not the hop loop, not `_download_resource`. There is
no second call site to forget. This is the structural answer to a defect that recurred
twice in review under two different descriptions (incomplete redirect validation, then an
unvalidated initial vendor-supplied `resources[].url`); both were the same defect —
validation applied at *remembered* call sites — and a third remembered site would have been
the same bug with a longer fuse.

**How the single-primitive invariant is proven.** Not by a syntactic gate. An earlier AST
gate drew a blocker in two consecutive review passes in **opposite** directions (too broad
to let the suite pass; too narrow to catch a module-level `httpx.get` or a second client),
which is the signal that syntactic enumeration is the wrong mechanism. Instead `_send`
stamps each request with a fresh, single-use token in `httpx.Request.extensions` — a
per-request dict httpx hands to the transport and never serialises onto the wire — and the
test observer **consumes** each token it sees, so a replayed or copied token fails on its
second appearance exactly as an absent one fails on its first. respx patches httpx's
transport globally, so the proof is a property over *observed traffic*: every request
reaching the transport on an exercised path carries the current nonce. A bypass is caught
by its effect, not its spelling. Stated narrowly, because a decaying completeness claim is
worse than a modest one: this guards against an implementer reaching the network without
validating; it is **not** an authentication token against an adversary who would forge
attestation, and it cannot see a bypass on a path no test exercises.

There are exactly **five** request kinds, all of them `_send` calls: `package_show` on the
ingest path; the initial vendor-supplied `resources[].url`; each resolved redirect hop;
each `package_search` page in `discover_catalog`; and the single `package_list` call that
reconciles the paginated name-set. Pagination advances by `rows`/`start` parameters
constructed against `base_url` — **no URL taken from a response body is ever fetched
anywhere in this source.**

### Redirect policy — D-08

**D-08 — manual, one validated hop at a time.** **[implemented: B1a]**
The client is built with `follow_redirects=False`, and `_send` passes it explicitly too, so
a client-level default cannot silently re-enable it. Two functions, deliberately split:

- `_resolve_redirect_target(response)` — **resolution only**, because it is the only step
  that needs the response: `response.request.url.join(response.headers["Location"])`, so a
  relative `Location` resolves against the host that sent it and an absolute one passes
  through unchanged;
- `_assert_safe_target(url)` — **the policy**, applied by `_send` to every outbound
  request: scheme must be `https`; userinfo must be empty (a `user:pass@host` URL would
  have httpx attach Basic credentials to that host); the host must resolve, via
  `loop.getaddrinfo`, to addresses that are **every one** `is_global`. **Every**, not
  "any" — a DNS answer mixing a public and a private address passes an any-check while
  httpx may connect to the private one.

**Why manual rather than `follow_redirects=True` plus a final-URL check** (the superseded
design): validating the *final* URL happens after the request has been sent, so it prevents
nothing, and an `https://` loopback or RFC-1918 target would pass it. Manual hops are also
what make the per-send throttle invariant achievable — an automatic-redirect client issues
two network sends inside one httpx call and one throttle.

The validated target is sent **verbatim**, never re-encoded or re-ordered: the R2 target is
presigned and any query normalisation invalidates the signature. No credentials are
attached (the source is keyless) and no cookie crosses the host boundary — the 302 sets
three cookies (`token`, `token-fresh`, `ckan`;
`_probe/sample_historic-generation-mix.headers`) which httpx stores domain-scoped to
`api.neso.energy`, so the file-host request carries no `Cookie` header.

**Residual, stated not papered — TOCTOU.** Resolve-then-connect leaves a window in which
DNS could answer differently for the connection than for the check (rebinding). Because
`_assert_safe_target` sits inside the retry-decorated body, each attempt re-resolves, which
**narrows but does not close** it. Closing it fully requires pinning the connection to the
validated address; that is disproportionate here — the redirect chain originates from
NESO's own API and gridflow is a local-only pipeline with no ambient network authority to
be confused. Recorded as an accepted residual, not claimed as mitigated.

### Admission — D-36

**D-36 — nothing enters bronze until the response is proven good, to the exact extent it
can be.** **[implemented: B1a]** Four rungs, in order: status (inside `_send`, within the
retry boundary); transport completeness (identity guard, running cap, premature-termination
translation, and — when a `Content-Length` was declared — exact equality against the
accumulated raw byte count, never inferred from a successful parse); **parse admission**
(the CSV reader is run over the downloaded bytes with this dataset's `expected_columns`);
only then is the `RawResponse` built.

Rung 3 exists because D-10 stamps `content_type="text/csv"` from CKAN metadata rather than
from the response header. That is correct for the `.bin` problem below, but it means a JSON
error envelope, an HTML interstitial or a binary body would be labelled `.csv` and written
to **immutable** bronze, where it is not recoverable by re-running. The parse is not
duplicated work — it is the same call silver makes later, run once at the boundary as an
admission check, and its result is discarded. It deliberately sits **outside** the retry
boundary: header drift is a vendor change, not a transient fault, and retrying it five
times would be five pointless 62 MB downloads.

**The narrow scope of what this proves, stated because a decaying guarantee is worse than a
modest one (FM-15).** A CSV truncated at a row boundary is still well-formed CSV with the
right header and a non-zero row count; it passes rung 3. Rung 2 catches it **only** when
the vendor sent a `Content-Length` the body then failed to match. Two residuals stand:

- a chunked response with no `Content-Length` cannot be completeness-checked at the
  transport layer at all;
- a body truncated **upstream of the transport** — NESO publishing a short file — is
  indistinguishable from a legitimately short one. Detecting it would need a
  vendor-published row count or checksum. **NESO publishes neither, and this phase does not
  invent one**: no row-count floor, no size-delta heuristic, no "it shrank so it must be
  broken" rule. A fabricated completeness semantic is the silent-data-bug class dressed as
  a safeguard. TODO-10.

### Bronze stamping — D-10, D-13

**D-10 — `content_type` comes from CKAN's `format`, never from the HTTP header.**
**[implemented: B1a]** The presigned R2 response carries
`Content-Type: application/octet-stream` (verified in `_probe/sample_*.headers`), which
`bronze/writer.py` maps to `.bin` — and a `.bin` body is invisible to the `raw_*.csv` glob
the transformers use, so silver would read zero rows from a bronze tree that is not empty.
A CKAN `format` other than `CSV` raises. D-36's rung 3 is what makes this stamp safe.

**D-13 — the bronze partition is the UTC date of the resolved fetch window's END.**
**[implemented: B1a]** `run_transform` resolves its target dates as
`date_range(start_dt.date(), end_dt.date())`, end-inclusive. Deriving the bronze partition
from the same `end` makes the two legs agree **by construction**, with no dependence on
when the fetch happens to finish.

Two rejected alternatives, both recorded:

- **NESO's publication date** — a silent-zero-row hazard. The read branch reads the *exact*
  date partition only, with no covering fallback. A resource last republished N days ago
  would land bronze N days in the past, outside every default transform window: ingest
  reports success, transform emits one generic "No bronze data", zero rows — with N
  governed by a vendor cadence this phase refuses to guess (TODO-03).
- **`fetched_at.date()`** — the same hazard through a narrower door, and a **midnight-
  rollover hazard** specifically. `fetched_at` is stamped at `RawResponse` construction,
  i.e. *after* the download. A `--last 24h` run started at 23:58 UTC whose 62 MB download
  finishes at 00:01 stamps the partition on day N+1 while the transform leg, working from
  the window resolved at 23:58, only looks at day N. Ingest succeeds, transform finds
  nothing. Not hypothetical for a multi-minute download on a nightly schedule.

A third option — threading the actually-written bronze partition through to the transform
leg — was rejected as a shared-layer change (`run_ingest`/`run_transform` signatures,
affecting all six sources) for a problem fully solvable inside this connector.

**Two residuals**, the same shape: the partition follows the *ingest* window, so a transform
over a different window misses it. (1) **Split workflow** — a `gridflow transform … --last
24h` run two days after the ingest reads the exact partition for *its* window, finds
nothing, and emits the generic warning with zero rows. (2) **Stale-but-admitted window** —
D-34 admits an ingest whose `end` is up to 48 h old; bronze lands on that older date, and an
immediately-following default `--last 24h` transform may not reach back that far. Narrower
than (1) but the same mechanism, and it is the price of D-34's tolerance being loose enough
not to reject legitimate "yesterday to today" windows. `gridflow pipeline` is immune to both
because it runs ingest and transform from one resolved window. This is a property of the
repo's exact-partition read path — shared with `elexon/system_prices` — not something this
source introduces.

**Honesty note:** `end.date()` is a *partition key derived from the request window*, not a
claim about the data's own calendar dates. The rows' own time axis is `event_time` and the
vendor's publication instant is `published_at`; neither is affected.

### Timezone reading — D-15

**D-15 — CKAN's naive `resources[].last_modified` and the embedded-forecast filename token
are read as UTC.** **[implemented: B1a]** CKAN emits them without an offset. The reading is
corroborated by three independent observations, each with its file and its exact values:

1. **Embedded forecast.** CKAN `last_modified = 2026-08-16T18:25:03.877001`
   (`_probe/show_embedded-wind-and-solar-forecasts.json`, resource `Embedded Solar and Wind
   Forecast`) against the R2 response header
   `Last-Modified: Sun, 16 Aug 2026 18:25:04 GMT`
   (`_probe/sample_embedded-forecast-current.headers`) — RFC-7231 GMT by definition, and
   **0.1 s apart**. A BST reading would be exactly 3600 s off.
2. **Historic generation mix.** CKAN `last_modified = 2026-08-16T18:21:07.098698`
   (`_probe/show_historic-generation-mix.json`) against
   `Last-Modified: Sun, 16 Aug 2026 18:21:38 GMT`
   (`_probe/sample_historic-generation-mix.headers`) — **31 s apart**, same non-offset.
3. **The filename token.** `202608161825_embedded_forecast.csv` equals that resource's
   `last_modified` to the minute. 18:25 **UTC** falls inside settlement period **39** of
   2026-08-16, which is exactly the first row the file carries
   (`_probe/sample_embedded-forecast-current.csv`, first data row:
   `2026-08-16T00:00:00,18:30,2026-08-16T00:00:00,39,468,6417,1323,23301`). A BST reading
   (17:25 UTC) lands in SP37 and would predict a first row two periods earlier than
   observed.

**This is corroboration pinned by a live test, not a vendor contract.** NESO documents
neither. A `@pytest.mark.live` re-check detects a change; it does not make the reading
authoritative. What would make it one is recorded as TODO-02: NESO support confirmation, or
an observation across a DST boundary — a filename token and `last_modified` captured either
side of the October transition would separate UTC from Europe/London definitively.

### Window admission and backfill — D-34, D-35

**D-34 — four checks at the top of `fetch()`, before any network I/O.**
**[implemented: B1a]** Well-formed (both endpoints tz-aware UTC, `end >= start`); not in
the future (beyond a 5-minute clock-skew tolerance); not more than 48 h stale; and not
wider than 7 days. Each raises before a byte leaves the process.

The 7-day bound is not invented: it is `PipelineSettings.max_incremental_lookback_hours`
(168 h), the widest window `run_ingest` itself can ever resolve, asserted against the
field's **declared default** rather than a constructed settings object — instantiating
`PipelineSettings` reads env vars and `.env`, which would make the assertion pass or fail on
a local environment rather than on the repo's declared ceiling.

A one-day bound (the configured `max_query_days`) was rejected on evidence: every
post-first `--incremental` run resolves a span of roughly four days, so a one-day refusal
would false-refuse an ordinary legitimate command, recurringly — and `max_query_days` is
dead config that no code in the repo reads, so citing it would lend the number authority it
does not have. Where a span exceeds the per-dataset `max_query_days`, the request is
**honoured with one loud WARNING** naming the requested span, the configured maximum and
the snapshot semantics — what was honoured is said out loud rather than reinterpreted in
silence.

**D-35 — `SNAPSHOT_ONLY`, a declared capability enforced generically.**
**[implemented: B1a]** `BaseConnector` gains `SNAPSHOT_ONLY: ClassVar[bool] = False`; the
NESO connector sets it `True`; `pipeline/runner.assert_backfillable(source)` resolves the
connector **class** from the registry and raises `BackfillUnsupportedError` when the class
declares it. Both backfill entry points call it as their first statement.

**Why a capability property rather than a tighter recency tolerance — the structural
answer.** D-34's recency check is a *proxy* for "this is a backfill", and the proxy leaks:
`--start 2026-08-14 --end 2026-08-16 --chunk-days 1` yields chunk ends 45.5 h and 21.5 h
stale, both inside tolerance, both admitted — two duplicate 62 MB downloads and a reported
success. **No value of the constant fixes this**, because a recent backfill window is
genuinely indistinguishable from a live one by recency. The capability check is decided by
*what the source is*, not by what the window looks like, so it holds for every window shape
and every chunk size. D-34 and D-35 are therefore a split of responsibilities, not
belt-and-braces: D-34 refuses windows this source cannot honestly serve; D-35 refuses
backfill as a mode.

`assert_backfillable` bootstraps the connector registry as its own first statement. This is
not defensive padding: in a fresh CLI process the registry is empty until well after
dataset resolution, and a helper that resolves a class from an empty registry cannot refuse
anything — it would fail to find NESO *and* fail to find the five legitimate sources.
`import_connectors()` is idempotent, so the existing call becomes a no-op.

### Catalogue discovery and the vault snapshot — D-17

**D-17 — `discover_catalog()` returns payloads *and* per-request evidence.**
**[implemented: B1a; consumed by the materializer in B1b]** For each HTTP call it carries
the normalized request params, start/end timings, HTTP status, the headers that matter
(`date`, `content-type`, `etag`, `last-modified`) and the body sha256. Returning bare
payloads would leave the snapshot contract with no source for `provenance.json` and force
placeholders — the thing a provenance file exists to prevent.

It performs the pagination reconciliation **permanently**: page `package_search` until
`count` is covered, reject duplicate package names across pages, then compare the name-set
against `package_list`; any mismatch raises `CkanPaginationMismatch` naming both totals and
up to 10 example names. It is deliberately **not** on the per-dataset ingest path — that
path calls `package_show`, never `package_search`, so there would be nothing to reconcile,
and it would cost 4+ extra CKAN requests per run against a 1 req/s budget.

### ADR-024 / ADR-025 compliance — D-21, D-24

**D-21 — all three transformers set `VINTAGE_PER_BRONZE_FILE = True` **and**
`APPEND_ONLY = True`, and each gets a `LATEST_VIEW_SPECS` entry.**
**[decided, scheduled: B2/B3a]** Uniform, no exceptions. Every capture is a whole-file
snapshot, so successive captures land under different partitions and **coexist** in the
base DuckDB view. Under atomic-replace there would be no selection surface at all:
`daily_wind_availability` would return up to 13 forecast vintages per BMU-day and
`historic_generation_mix` would return K full copies of 2009-present.
`historic-generation-mix`'s own CKAN `notes` field states *"The data is subject to change
due to a data cleansing process"* — ADR-025's triggering condition verbatim.
`VINTAGE_PER_BRONZE_FILE` is additionally what makes re-transform idempotent: the
run-suffix comes from the scalar `available_at`, derived deterministically from the bronze
sidecar, so a second transform of the same bronze produces the same path and replaces the
first. On the plain read branch that scalar would be `datetime.now(UTC)`, minting a new
Parquet file on every re-transform.

**D-24 — the per-dataset silver contract** (columns, `event_time` source,
`ENTITY_KEY_COLUMNS`, `LATEST_VIEW_SPECS.key_columns`, ADR-024 designated date columns).
**[decided, scheduled: B2/B3a]** Two consequences of it are worth recording here because
they are correctness rules, not table entries: `daily_wind_availability` has no natural
instant and must emit `timestamp_utc = settlement_period_to_utc(availability_date, 1)`
rather than fall into the midnight-UTC-of-target-date fallback, which — since the partition
is the D-13 window-end date — would stamp every row with the ingest window's end rather
than its own availability date; and `embedded_wind_solar_forecast` must **not** emit a
`timestamp_utc` column at all, because the base class prefers it over the settlement pair
and the pair branch is the one that calls the DST-fold-safe helper. `TIME_GMT` is carried
unparsed as `time_gmt_raw` — its start-vs-end convention is undocumented by NESO and no
code path may depend on it.

**D-30 — `historic_generation_mix` is captured whole, every fetch, unconditionally.**
**[decided, scheduled: B3a]** Rejected: `datastore_search` incremental (barred by D-05 and
the 2 req/min class, and it would change bronze from vendor bytes to a re-serialised JSON
page set); byte-range resumption (the file is republished whole, so ranges are not stable);
post-download hash dedup (republished with appended rows, so bytes differ on nearly every
real refresh and the dedup would almost never fire). Cost, priced rather than hidden:
62 MB bronze plus 10–20 MB Parquet per capture, ≈22 GB/yr daily, ≈3.2 GB/yr weekly,
≈0.7 GB/yr monthly; ~309K rows per capture, so a year of weekly captures puts ~16M rows in
the base view. That is the reason the **`_latest` view is the default consumer surface** for
all three datasets — the base view is the vintage archive, not the query target. The
republish cadence is unknown and is not invented (TODO-03); the dataset ships with
`schedule: "daily"` for config uniformity but is not wired into any automated schedule.

### Settlement-period validity — D-27

**D-27 — a settlement period that does not exist on its settlement date is declared invalid
by the schema AND excluded by the transformer.** **[decided, scheduled: B3a]**
Both, for reasons of mechanism rather than belt-and-braces.

The shared `SettlementPeriodMixin` declares `settlement_period: int` with **no `Field`
constraint and no validator** — measured against the installed code,
`settlement_period=0`, `-5` and `999` are all accepted. `settlement_period_to_utc` then
simply steps 30 minutes from SP1 and bound-checks nothing, so the error is two-sided and
both sides are silent: SP49 on an ordinary day (or SP47 on a 46-period spring day) lands in
the *next* settlement day, and SP0 lands in the *previous* one — measured,
`settlement_period_to_utc(2026-08-16, 0)` returns `2026-08-15 22:30Z`, half an hour before
that day's SP1. The correct bound is therefore both-sided and DST-derived:
`1 <= settlement_period <= settlement_periods_in_day(settlement_date)`.

**One predicate, two callers.** The constraint is a single module-level callable that both
the schema validator and the transformer filter call; neither restates the comparison. Two
hand-written copies of a two-sided bound is how one of them ends up upper-only again.

**Why not the validator alone:** `_validate_against_schema` documents itself as *never
raising and never dropping a row* — invalid rows are still written and the count is the
only signal. A validator-only design would therefore count the bad row **and still write
it**, with the wrong-day `event_time` computed immediately afterwards. Counting a silent
data error is not preventing it. **Why not the exclusion alone:** validation runs on
`transform()`'s output, so a transformer that filters first leaves the schema silent about
the constraint, and a later refactor dropping the filter would restore the hazard with
nothing to catch it. The validator is the durable statement; the filter is the enforcement.

**Scope: NESO-local.** Constraining the shared mixin would change validation for every
settlement-based schema in the repo and needs its own blast-radius review — filed as
TODO-12, not smuggled into this phase.

Whether NESO ever emits such a row is unknown and is not guessed: TODO-09.

### Exclusion accounting — D-40, D-41, D-42

**D-40 — `last_excluded_row_count`: an excluded row is a counted, surfaced row.**
**[shared plumbing implemented: B1c; the transformer half decided, scheduled: B3a]**
The exclusion happens inside `transform()` and the validator runs on `transform()`'s
*output*, so an excluded row is **never seen by the thing that counts**. Without a counter,
a run can emit a WARNING per excluded row and still report plain `success` — loud in the
log, invisible in the run status. `BaseSilverTransformer` therefore gains
`last_excluded_row_count`, reset in `run()` and accumulated with `+=` (never `=`: on the
`VINTAGE_PER_BRONZE_FILE` branch `transform()` runs once per bronze file against one
reset, so an assignment would discard every earlier file's exclusions), and `run_transform`
folds it into the total it already surfaces.

**The conflation it accepts, stated here rather than left to be discovered.**
`rows_invalid` now carries **two dispositions**: rows that failed validation and were still
**written** (the base class's documented fail-soft) and rows a transformer declared invalid
and **excluded**. The discriminator is the per-row WARNING. One number is deliberate — the
operator's question is "did this run produce anything the contract calls wrong", and that
question has one answer. Widening `last_validation_failure_count` in place was rejected
because it would make an existing shared contract quietly false for one source.

**D-41 — every exclusion, at every granularity, reaches the run status; an all-excluded run
is a failure, not a success with zero rows.** **[implemented: B1c]**
If this source's code declines to persist something a vendor supplied — a row, a body, a
vintage — that decision must be counted, must reach the warning path, and must become a
hard failure when *everything* examined was declined. A log line is not accounting: nobody
reads a WARNING on a scheduled run, and the run's own status is what a scheduler,
`gridflow status` and the exit code report.

The mechanism reuses the accounting the repo already has. `last_unvouched_bronze` /
`last_unvouched_total_exclusion` already carry file-level exclusions with a `(path, reason)`
association, already survive deduplication across dates, and are already folded into a
warning rung and an all-excluded failure rung — they were simply never populated by the
`VINTAGE_PER_BRONZE_FILE` branch, which had **no file-level accounting whatsoever**.
`BronzeVouchReason` gains `UNUSABLE_PROVENANCE` for the second skip (the sidecar vouched
and the timestamp key was fine; what failed was one level further in), rather than reusing
`NO_TIMESTAMP_KEY`, which would be a false record.

**The total-exclusion predicate is over FILES declined at those two skips, and deliberately
not over "was any frame processed".** The looser wording mis-fires on the boundary case
D-40 exists for: one body whose sidecar vouches, whose provenance is fine, and every row of
which is D-27-excluded. No frame is processed — yet nothing about that file was excluded at
the file level; it was read, **consumed**, and its rows were accounted. Because the
total-exclusion rung is checked *before* the warnings rung, the looser predicate would
report `failed` for a case that must report `completed_with_warnings`, and the wrong answer
would win silently.

**D-42 — the exit inventory is derived from the control-flow graph, not recalled; and the
empty-frame exit is classified by the base class, not by transformer convention.**
**[implemented: B1c]**

**The derivation method is itself the decision, and is recorded as such.** Three successive
"exhaustive" exclusion inventories were each found incomplete — row level, then file level,
then frame level plus a lost-on-exception publication path. The inventories were not
careless; they were **recalled**, and a recalled inventory is incomplete by construction.
The fix is to derive the list from something finite and checkable: the control-flow graph
of one function. The plan's table walks **every** `return`, `continue`, early exit and raise
in `silver/base.py` between entering the per-file loop and writing silver, from the source,
stating each one's accounting disposition — including the ordinary successful exit, because
a table of exits that omits the ordinary one is not a complete table. A reader can check it
against the same lines; that is the point of it.

**The classification.** An empty `transform()` output has two entirely different meanings
that must not share a status: *accounted* (the transformer declined rows and counted them —
correct outcome, warnings) and *unaccounted* (the frame emptied for a reason nothing
counted). `_process_frame` snapshots `last_excluded_row_count` and `last_unmapped_count`
immediately before `transform()` and compares immediately after; those two are the only
accounting a transformer can move from inside `transform()`. Neither moved means
unaccounted. **"Moved" is `!=`, never `>`** — `last_excluded_row_count` accumulates, but
`last_unmapped_count` is *assigned* by the transformers that use it, so `>` would misread
an assignment landing on the previous file's value. (Even `!=` cannot see the same value
assigned twice in succession; that is unreachable on the gated branch today and is carried
as a second item under TODO-13.)

**This is a mechanism, not a request.** D-40 alone depended on each transformer voluntarily
incrementing before returning empty — a convention `system_prices` never agreed to and 60+
transformers were never told about. Under D-42 a transformer that forgets is *detected*.
Same principle as putting target validation inside `_send`: the guarantee lives where it
cannot be skipped.

**The gate is load-bearing.** The increment is conditioned on `VINTAGE_PER_BRONZE_FILE`
because `_process_frame` has **three** call sites (vintage, lockstep, plain), enumerated by
grepping the symbol rather than by reasoning about which branch "the" transformer uses. An
ungated increment would fire for every transformer in the repo and flip runs across all six
sources off `success` — contradicting this phase's own claim that `elexon/system_prices` is
the single place it reaches outside its own source. It is placed at the empty-from-
`transform()` exit specifically, not around `_process_frame` as a whole, so that exit stays
distinguishable from the window-filter empty, where an existing counter already drives a
hard failure; counting both would double-charge one outcome.

**The publication rule.** Several exits leave the per-file loop by exception, and publishing
the accounting *after* the loop meant a raise on file N discarded the exclusions recorded
for files 1..N-1 — the run failed while reporting zero exclusions, at exactly the moment the
record matters most. The loop is therefore wrapped in `try: … finally:` and the `finally`
publishes everything it accumulated. On the exception path the predicates are computed over
what was examined **before** the failure, which is honest: they describe the run that
actually happened, not the one that was planned.

**And the consumer half**, which the producer half alone does not deliver: `run_transform`'s
normal accumulation sits *inside* the per-date loop, so when `run()` raises, the failing
date's counters are never folded. Its exception handler now folds `last_unmapped_count`,
`last_validation_failure_count + last_excluded_row_count` and
`last_unaccounted_empty_frames` alongside the orphan set it already folded, and the failed
result carries `rows_skipped`, `rows_unmapped` and `rows_invalid`. No double-count: the
raising date never reached the in-loop accumulation, and earlier dates did not leave their
values on the instance for a second pass, because `run()` resets every counter on entry.

**Exposure.** The frame count is deliberately **not** folded into `rows_skipped` or
`rows_invalid` — those are row counts and this is a frame count, and inflating one with the
other is the overloading this layer already rejects for excluded files. It gets no
`DatasetResult` field either, so **one renderer**, `_describe_unaccounted_frames`, emits
the exact token `unaccounted_empty_frames=<N>` and is appended to the warned message **and**
to every failed rung's `error_message`. A value that is computed and folded but never shown
is not accounted for; that was the defect on the failed path, where the fold landed and
nothing displayed it. **Accepted residual:** a structured consumer reading the dataclass
still cannot distinguish an unaccounted empty frame from the other warning causes without
reading the message. Adding a field is a shared-model change across six sources for a
discriminator no consumer asks for yet, and the status — which is what schedulers and exit
codes read — is correct either way.

---

## Consequences

### This phase changes the reported status of an existing dataset, `elexon/system_prices`

**This is the single place the phase reaches outside its own source, and it is recorded
here so a reader of the decision log finds it without reading a phase plan.**

`elexon/system_prices` is the only other `VINTAGE_PER_BRONZE_FILE` transformer in the repo
(grep-verified). D-41 and D-42 alter its reported status in **three** ways:

| Scenario | Before | After |
|---|---|---|
| A bronze body is skipped (no sidecar, unreadable sidecar, no usable timestamp key, or a body that reads as empty) | silent `success` | `completed_with_warnings`, with the file counted in `bronze_unvouched` |
| **Every** candidate body for a date is skipped | `success` with zero rows | **`failed`** |
| `transform()` returns empty for an uncounted reason — which it does **today** on a missing required column, with only a `logger.error` | `success` with zero rows | `completed_with_warnings`, or **`failed`** when no body for the date produced a frame |

**Two holes are closed, both named:** the `VINTAGE_PER_BRONZE_FILE` branch's two skips,
which had no counter at all (one logged and continued; the other did not even log), and the
empty-`transform()` exit in `_process_frame`, which returned `None` and reached
`run_transform` as `success`.

**What does not change**: rows written, vintages assigned and silver filenames are
untouched, on this source and on every other. What moves is the reported status, its
message, and the structured accounting fields around them: skipped bodies now appear in
`bronze_unvouched`, and exception-path failures for **every** source can carry non-zero
`rows_skipped` / `rows_unmapped` / `rows_invalid` (the widening §"Consumer contract"
describes above). Consumers of `DatasetResult` should review against that section, not
this summary line. The full fast suite passes unchanged and **no existing test assertion was edited** —
a standing tripwire for this unit required stopping and re-presenting if any existing test
pinned `success` on a skipped-body or empty-body scenario, since a test pinning the silent
behaviour would be evidence about intent this phase may not overrule unilaterally. None
did.

### The rest of the repo is untouched by construction, not by inspection

- `BRONZE_BODY_GLOB` defaults to the literal it replaced, and its sole inheritor is
  enumerated; both branches (JSON and CSV) are pinned by tests asserting written silver
  **filenames**, because the ClassVar's real reach is the filename, not the read.
- `last_excluded_row_count` and `last_unaccounted_empty_frames` default to `0`, so all 60+
  existing transformers contribute zero to totals they already contributed to.
- D-42's increment is gated on `VINTAGE_PER_BRONZE_FILE`; a non-vintage transformer whose
  `transform()` empties for an uncounted reason keeps master's exact status.
- `SNAPSHOT_ONLY` defaults to `False`, so no existing connector's backfill behaviour moves.
- The throttle and the redirect validator are **copied into this connector, not hoisted**
  into `BaseConnector` — hoisting either would change request pacing or redirect handling
  for all six existing sources. That hoist is filed as TODO-07.

### Operational consequences

- **No backfill.** gridflow's vintage history for these datasets starts at first capture.
  A `gridflow backfill neso_data_portal …` is refused before date resolution, with zero
  requests.
- **Prefer `gridflow pipeline`.** It resolves one window for ingest and transform and is
  immune to both D-13 residuals; a split ingest/transform workflow must pass `--start/--end`
  covering the ingest window's end date.
- **Read `_latest`, not the base view.** For all three datasets the base view is the vintage
  archive; `silver_neso_data_portal_<dataset>_latest` is the consumer default (D-30).
- **One WARNING per multi-day `fetch()`.** Every post-first `--incremental` invocation
  (~4-day spans) logs the D-34 reinterpretation notice. Accepted volume: it is a statement
  about what the call actually did, placed where the operator running the command is
  looking.

---

## Open items

Carried from the phase plan; none is a blocker for what shipped.

- **TODO-01** — `/datastore/dump/<resource_id>`'s rate-limit class is unstated by NESO
  (guidance names only `datastore_search`/`datastore_search_sql` under 2 req/min). Not on
  this phase's path (all three resources are `url_type: upload`). Required before any
  package whose current resource is `url_type: datastore` is taken.
- **TODO-02** — D-15's UTC reading is corroborated, not documented. Resolution: NESO
  support confirmation, or an observation across the October DST transition.
- **TODO-03** — `historic-generation-mix`'s republish cadence is unknown and is **not
  invented**. Resolution: record `ckan_last_modified` per poll for two weeks. D-30's cost
  tables are parameterised on cadence, not on a number.
- **TODO-04** — deferred conditional fetch (skip the download when `ckan_last_modified`
  matches the newest existing bronze sidecar). Deferred because it makes the connector read
  bronze, and introduces an empty-`fetch()` path that must be reconciled with watermark
  evidence, run status and the "No bronze data" warning. Triggers, any one: TODO-03 yields a
  cadence; this dataset's bronze exceeds 5 GB; or the silver table exceeds ~20M rows.
- **TODO-09** — does NESO ever publish an out-of-calendar settlement period on a DST day?
  Unknown, and not guessed. D-27 excludes and announces such rows, so the answer arrives as
  a WARNING count rather than as silent corruption. If they turn out to be routine rather
  than exceptional, the rule needs revisiting with NESO's semantics in hand, not ours.
- **TODO-10** — FM-15's semantic-truncation residual would be closable if NESO published a
  row count, checksum or manifest per resource. None exists. Until one does, no detector is
  invented.
- **TODO-11** — D-39's retry asymmetry (CKAN action calls retry transport faults on the
  body; the streamed file leg retries the header phase only). Trigger to revisit: an
  observed `NesoTruncatedBodyError` rate that a single daily retry would have absorbed —
  real evidence, not a hypothetical flaky link. The fix, if it comes, is a retry around the
  whole download leg with the redirector re-resolved, never a retry of a half-consumed
  stream.

Two further items belong to the shared layer rather than to this source and are filed
separately: **TODO-12** (`SettlementPeriodMixin` declares no bound on `settlement_period`,
for every settlement-based schema in the repo) and **TODO-13** (D-42's detection is gated on
`VINTAGE_PER_BRONZE_FILE`, so the same empty-`transform()` hole stays open on the lockstep
and plain branches for the other five sources).
