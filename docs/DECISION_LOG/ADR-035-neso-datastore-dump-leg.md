# ADR-035 — NESO datastore dump leg, datastore lane, dump vintage, content-hash dedup

**Status:** proposed
**Date:** 2026-10-08
**Phase:** v0.22 unit D (datastore leg)
**Amends:** ADR-033 (the `deferred` datastore member becomes a captured one; the usable rule
widens for dumps), ADR-034 (V-8 and `capture_context` for dump families).
**Cross-references:** ADR-023 (definitive-absent), ADR-030 (NESO source, D-39, TODO-01),
ADR-033 (registry, upload leg, pacer), ADR-034 (generic silver, vintage recipes).

## Context

Unit A's registry seeds 152 `url_type: datastore` resources in 43 families (all CSV, all
tabular SILVER); 8 families hold both uploads and dumps. Until this unit a datastore member
was yielded `deferred` and nothing was sent. A dump has no file `last_modified` that dates
its rows (decision 9), so neither A's metadata skip nor B's `ckan_last_modified` recipe
applies to it.

## Decision

**P-1 — the datastore lane.** `_send`, `_throttle_request` and `_ckan_action` take a
`lane` (default `Lane.CKAN`). The lane is a parameter of the retried send primitive, so
every retry attempt and every redirect hop is admitted on the caller's lane. Only the dump
download and `datastore_fields` pass `Lane.DATASTORE` (A's 30 s lane, 2 req/min).
**I-1:** on the CKAN path every call expression is master's — `_send(request, target)`,
`_send(request, target, stream=True)`, `_throttle_request()` — so test doubles with the
pre-D signatures keep working. `pacer.py` is unchanged.

**P-2 / P-3 — the dump target (D-39).** The dump URL is `base_url` +
`/datastore/dump/<id>`, built only from an id the registry seeds as a `datastore` resource
of the family being captured, canonical lowercase UUID, live format CSV. It is **never** read
from `resources[].url`. A NESO-recreated dump under a new UUID fails loud
(`NesoDatastoreMemberError`) until a registry commit seeds it. A redirect is followed only to
the `base_url` origin; an off-origin `Location` raises `NesoDumpRedirectError` before that
hop is sent. 206, truncation, oversize, an identity-coding violation and more than three
hops are refused as on the upload leg.

**P-4 — admission.** A's `_admit_member_body` with `declared_format="CSV"`; markup and JSON
envelopes are refused, and a PK body (which CSV would admit as a zip) is refused because a
dump is CSV text. A header-only dump is captured with the empty marker only in an
`empty_allowed` family.

**P-5 — provenance.** D-12's keys, with `ckan_last_modified` as the CKAN string or `""`
(never the string `"None"`), `resource_filename` = the resource id, A's member keys
(`capture_family`, `url_type: "datastore"`, `empty_capture`, `declared_content_length`), and
two evidence keys for the open questions below: `ckan_metadata_modified` and
`response_last_modified`. `request_url` is the built dump URL.

**P-6 — the usable rule for dumps.** A sidecar with `url_type: datastore` is usable on the
identity form (D-12 keys other than `ckan_last_modified` non-empty) whatever
`require_provenance` says, so coverage counts a dump capture as `captured` and it can be a
dedup basis. Every other sidecar keeps A's rule. `Capture.url_type` is added (defaulted, last).
A dump capture is never the skip basis for an upload.

**P-7 — frozen cadence and its check stamp (decision 11).** A dump member of a `frozen`
family is **not due** iff a verified basis exists (P-8) and the latest of the basis capture's
`written_at` and an honoured check stamp's `verified_at` — ignoring any dated after
`end.date()` — falls inside 7 `end`-dates. Every other refresh class is due every run. A
byte-identical frozen check writes no bronze, so it is recorded as a **check stamp** at
`state/neso_data_portal/dump_checks/<family>/<rid>.json`
(`{body_sha256, resource_id, verified_at}`, written atomically). A stamp is written only
after a verified identical observation; it is honoured only for the same resource and the
same basis hash with a tz-aware time. **Stamps are disposable**: deleting one, or any fault
in one, only makes the member due sooner. No CKAN field enters the rule.

**P-8 — content-hash dedup, fail-open.** A due dump is downloaded and suppressed iff a basis
exists and its body's SHA-256 equals the newest capture's. A basis exists only if the newest
usable capture is present, no unusable sidecar in the family directory names the resource,
no unusable sidecar names no resource at all, and a streamed re-hash of the basis body on
disk equals its recorded hash. **Cost:** every dump member with a candidate basis re-hashes
that body once per run, whether it is then skipped, suppressed or captured. Equality is with
the newest capture only, so a body equal to an older capture (P → E → P) is written.
Suppression yields `unchanged`; the runner already treats that as no publish and no frontier
advance.

**P-10 — dump vintage.** A family holding a datastore resource takes
`vintage: capture_fallback` and nothing else: V-8 refuses `issue_time_evidenced` at load,
and `capture_context` refuses a datastore sidecar under any other vintage at run time (a
registry lagging a live `url_type` change). Every dump row therefore has `published_at`
null and `available_at` = the capture's `written_at`. **Mixed families (C-7):** the 8
families holding uploads and dumps take `capture_fallback` for their uploads too — honest,
but later than the upload's `last_modified`. `embedded_wind_solar_forecast_archive` (7
uploads + the 2026 dump) can take `issue_time_evidenced` only after a registry commit splits
the dump into its own family. EF splits the dump into its own owner: ADR-038.

**P-11 — field-info mode.** `python -m gridflow.connectors.neso_data_portal.catalog_snapshot
--field-info [--snapshot DIR] [--family KEY ...]` verifies the snapshot, then for every
registry family with a `datastore_active` resource in it (276 on the snapshot of record)
calls `datastore_search` with `limit=0` on the datastore lane, on the resource with the
maximum `(metadata_modified, id)`. A response carrying rows or a non-zero `limit` is refused
for that family; the evidence file is built from an allowlist (`_links` dropped) and still
passes the row-sample guard. Files go to `<out>/field-info/<run id>/`, `sha256sums.txt`
last; no manifest is advanced. The default session is bound, so a separate field-info
process cannot overlap an ingest and its first datastore request follows the lock by 30 s.

## The five unknowns (research record, class 3)

| Unknown | Status | Default kept |
|---|---|---|
| (a) rate class of `/datastore/dump` (ADR-030 TODO-01) | NESO publishes 2 req/min for the Datastore API but does not classify the dump route | 2 req/min (the datastore lane) |
| (b) dump vs upload bytes (`_id`, order, quoting, encoding) | UNMEASURED (connection failed) | no dump-specific header epochs |
| (c) redirect vs stream; `Content-Length`, `Last-Modified` | UNMEASURED | both shapes accepted, same-origin redirects only; headers recorded as observed |
| (d) publication recipe for the 152 dumps | no evidence that any field dates row availability | `capture_fallback` for every dump |
| (e) reliable change signal | none established (`hash` empty, `cache_last_updated` null) | scheduled downloads + content-hash dedup |

Enabling a metadata change signal or another vintage for dumps is a later unit with its own
evidence and test; the P-5 evidence keys accumulate the paired observations it needs.

## Consequences and residuals

- **FM-11, accepted residual (decision 11 latency).** A frozen dump that changes inside its
  week is captured on the first due run, at most 7 `end`-dates after its last verification
  (plus up to 48 h of `end` lag, D-34). Its `available_at` is that capture time, so no as-of
  read sees it early.
- **Re-hash I/O.** P-8 (iv) reads the newest capture of every dump member once per run.
- **Mixed-family vintage** as stated under P-10.
- **Unseeded dump ids** fail loud per member until a registry commit; A4 coverage reports
  them missing meanwhile.
- **Same-origin hops only.** If NESO moves dump delivery to another host, every dump fails
  loud (`NesoDumpRedirectError`) until a decision widens P-3.

**Forward pointer:** ADR-036 freezes the first records (the six pilot families) and adds
the offline profiler that proposes the rest.
