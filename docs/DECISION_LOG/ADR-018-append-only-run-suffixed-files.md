# ADR-018 — Append-only uses run-timestamp filenames, not row-level merging

Status: Accepted (F7, 2026-05-07)

## Context

Append-only silver storage for revision-publishing datasets (REMIT,
FOU2T14D) needs a file layout that preserves every run's output. Two
implementations were considered. The first reads the existing partition
file, merges new rows with old based on a business key, and writes the
merged result atomically. The second writes each run to a unique file
within the partition directory and lets the read path apply
`QUALIFY`-style deduplication when the caller asks for the latest
revision available at `as_of`.

## Decision

Use the second approach. Filenames follow the pattern
`<dataset>_<YYYYMMDD>_run<ISO-8601 available_at>.parquet`, with `:` and
`+` replaced by `-` for filesystem safety. The suffix is derived from
`available_at`, not from `datetime.now()`, so that `--reingest` (which
reconstructs `available_at` from bronze sidecars) is idempotent: two
re-ingest passes over the same date produce the same filename and the
second cleanly overwrites the first via `os.replace()`. Live runs
generate distinct timestamps and therefore distinct files.

## Consequences

- No read-merge-write race condition; each run's output is a
  self-contained Parquet file inspectable in isolation.
- No per-dataset merge-key configuration is needed in the writer; merge
  semantics live in the read path where they belong (see ADR-019).
- Re-ingest is idempotent without special-casing.
- Read paths that want a single latest-revision view must apply a
  `QUALIFY` clause; this complexity already lives in the data layer
  introduced in F3.
- Partition directories accumulate files over time. A future compaction
  job can roll old append-only files into a single per-month archive
  without changing the writer.
