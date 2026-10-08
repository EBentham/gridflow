# ADR-033 — NESO Data Portal registry, upload leg, run-scoped pacing, bounded ingest

**Status:** proposed
**Date:** 2026-10-07
**Phase:** v0.22 unit A (registry and upload leg)
**Amends:** ADR-030 D-04 (as D-04′ below); closes ADR-030 TODO-04.
**Cross-references:** ADR-023 (definitive-absent), ADR-027 (watermark advance), ADR-030
(D-03, D-07, D-11, D-12, D-23, D-39).

## Context

ADR-030 onboarded three NESO Data Portal datasets, each one hand-written `CkanDataset`
selecting exactly one CKAN resource. The portal's snapshot of record
(20261006T195819Z, sha256 `baace556…`) holds **131 packages and 1,385 resources**. v0.22
captures all of them. Four things in ADR-030's shape do not scale to that:

- one dataset = one resource (D-04) cannot express an archive series of eight files;
- bronze names were `raw_<ts>_<sha8>`, so two resources with identical bytes fetched in the
  same second collide, and `BronzeWriter.write()` publishes with `os.replace`, which
  silently replaces an existing file;
- pacing lived on the connector instance and reset at every dataset handoff, so a sweep of
  hundreds of datasets would not honour NESO's 1 req/s guidance;
- `fetch()` returns the whole family as a list, so memory grows with the family size.

## Decision

**D-04′ — a gridflow dataset is a *family* of N CKAN resources of one package.** Membership
is stated once, per resource, in the registry. Selection is the exact `(name, FORMAT)` pair
as stored (no strip, no case-fold, no normalisation), or an optional anchored `name_regex`
for a format the family already holds. Zero live members, or a listed member matched by two
live resources, is a selection failure (ADR-023). A listed member the vendor no longer
serves is reported `absent`; a live resource no family claims is logged once per process as
unassigned and not fetched. The three legacy keys keep their bespoke transformers, their
in-code header contracts and `_select_resource` unchanged.

**Registry.** `connectors/neso_data_portal/registry/<package>.json`, one file per package,
shipped as package data; seeded once from the snapshot of record and the ratified dataset
matrix by `scripts/seed_neso_registry.py` (310 families: 275 tabular, 35 files). Every
resource carries exactly one disposition: SILVER (its own tabular family), DOC, GIS, or HOLD
(XLSX/XLSM/ZIP and the 39 CSV-declared ZIP bodies of `system-frequency-data`, all for
X-R; the two HOLD(N) packages are held at package level). `endpoints.FAMILIES`,
`endpoints.DATASETS` (the legacy view) and the `config/sources.yaml` datasets block are
generated from it; `python -m gridflow.connectors.neso_data_portal.registry yaml --check`
pins the agreement.

**Key freeze (I-F).** A key with bronze cannot be renamed or removed. At runtime every bind
refuses before its first send when a bronze directory is not a registry key (directory names
only — never the ledger, so a sweep's own new keys never block it). In CI,
`_frozen_keys.json` (seeded with the three legacy keys) must be honoured by the registry.
Ledger lag is a coverage gap (`unfrozen`), never a refusal; the seat appends keys after a
sweep.

**Admission (P-7).** A member body is admitted by its leading bytes, per CKAN format
(PK, PDF, PNG, OLE2, SQLite, GeoJSON, text). A CSV text body that is markup or a JSON
envelope (led by `<` or `{`, or parsing as a JSON array), or a TXT body that is markup, is
refused; a `[`-led CSV that is not JSON is admitted; encoding is not checked. The extension
comes from the redirector filename when its signature class allows it, else the format's
default, never `.bin`. Zero bytes are refused; a header-only CSV is captured, marked
`empty_capture: true`, only in families that allow empty captures (registers), and refused
everywhere else. The header is the first CSV record, found by a byte scan of CSV's record grammar: an
unquoted CR, LF or CRLF ends it, and a `"` at a field start opens a quoted field whose
terminators and doubled `""` stay in the header. The scan has no parse step that can fail,
so no body counts as non-empty for want of a parse; what follows the header is header-only
when it holds nothing but ASCII whitespace, `,` and `"`. Residual: an unterminated opening quote makes the whole body one record, so it
reads as header-only. `record_count` stays `None`.

**Publication (P-9).** `BronzeWriter.publish_capture` names a capture
`raw_<ts>_<resource_id>_<sha8>.<ext>` and publishes each file through a unique temp, fsync
and `os.link`, which refuses an existing target atomically (`BronzeCollisionError`); there is
no fallback to `os.replace`. The sidecar is published last and is the commit marker: a body
without one is an orphan. `write()` and `_extension()` are unchanged, so no other source's
bronze moves. The three legacy keys now publish through it too, so their new captures gain
the resource id in the filename and four additive sidecar keys (`capture_family`,
`url_type`, `empty_capture`, `declared_content_length`); their request sequence, body bytes,
D-12 values and admission are unchanged.

**Usable capture and conditional fetch (P-10; closes TODO-04).** A sidecar is a usable
capture iff its `written_at` is tz-aware, it has exactly one sibling body of the recorded
size, `provenance_for` (the D-23 rule, called not copied) accepts it, and registry identity
holds: its directory is a family whose package is the provenance package and whose selector
accepts the recorded `(resource_name, ckan_format)`. Per REVIEW-PLAN-3 M1 the resource UUID
is capture identity (newest-capture lookup, coverage) but is **not** required to be a seeded
id: NESO may recreate a resource under a new UUID (ADR-030 D-03) and selection follows the
name. An upload member whose newest usable capture carries the live `last_modified` string is
skipped as `unchanged`; anything unprovable is fetched. A run whose every member is unchanged
is `success` with `members_unchanged = n`, logs that the frontier is unchanged, and does
**not** advance the watermark (no evidence, ADR-027).

**Coverage (P-11).** `python -m gridflow.connectors.neso_data_portal.coverage --snapshot …`
classifies every snapshot resource as `captured`, `adjudicated` (`_adjudications.json`, each
row citing a ruling), `unusable` or `missing`, and reports unregistered and unfrozen keys,
orphans, temps and (with `--verify-sha`) hash failures. The expected set comes from the
snapshot, never from bronze. Exit 0 clean, 1 gap, 2 usage.

**Pacing (P-12, invariant I-P).** One process-wide `RunPacer` with a CKAN lane
(`1 / rate_limit_per_second`) and a datastore lane (30 s, applied by unit D). Admission is a
check-and-set under a `threading.Lock` on the monotonic clock, read after any wait; nothing is
persisted and admission does no file I/O. A bound process holds an OS lock on
`<data_dir>/state/neso_data_portal/pacer.lock` from before its first send until it exits;
the next process can lock only after that, and its first admission per lane is at least one
interval after its own acquisition. Both production entry points bind: `run_ingest` and the
catalogue snapshot tool. A non-positive rate is refused. Every non-zero interval is widened
by the clock's resolution, because two readings one interval apart on Windows'
15.6 ms monotonic clock can be up to one tick less apart in real time.

**Why the gap is enforced in code from the lock, not by persisted instants (C-8).** An
instant written before a send can precede that send by an unbounded amount, and one written
after it is lost on a crash. The lock's acquisition follows the previous holder's last send
by construction, so no clock value has to cross the process boundary.

**Bounded ingest (P-5, A9).** A connector that implements `MemberCaptureConnector`
(`bind_data_dir`, `iter_members`) takes `run_ingest`'s member branch: members are consumed
one at a time and each capture is published before the next download, so peak memory is one
body whatever the family size. Every other connector's path (`fetch()` + `write()`) is
untouched. `skipped = failed + deferred + absent`; every member failing with none captured or
unchanged fails the dataset.

## Consequences and residuals

- **C-7, residual.** `gridflow pipeline neso_data_portal --all` exits 1 at transform until
  unit B, because only the three legacy keys have transformers. The capture sweep uses
  `gridflow ingest neso_data_portal --all`; the legacy datasets keep per-dataset `pipeline`.
  C-7 closed by ADR-034 P-13.
- **FM-9, accepted residual: fail-open skip.** If NESO changes a file's bytes without moving
  its `last_modified`, the skip keeps the older capture. TODO: vendor confirmation, or an
  observed equal-`last_modified` republication with a different SHA, would retire this. The
  SHA is recorded on every capture, and D's content-hash rule is the precedent if evidence
  turns.
- **FM-14, accepted residual.** Admission is measured at the transport: no await sits
  between admission and `client.send`, but httpx's connection setup inside `send` precedes
  the request bytes. Anchoring at the response instead would hold the lane for a round trip
  on every request.
- **FM-15, accepted residual.** An unbound connector (direct construction in a test or a
  notebook) takes no lock and is paced within its process only.
- **Hard links required.** Publication assumes an NTFS or POSIX volume; elsewhere `os.link`
  raises and publication fails loud.
- **C-3, measured.** 39 `system-frequency-data` resources declare CKAN `CSV` but serve ZIP
  archives; they are captured as `.zip` and held for X-R.
- The datastore lane and the dump leg: ADR-035.
