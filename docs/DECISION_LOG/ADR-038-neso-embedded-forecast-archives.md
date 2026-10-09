# ADR-038 — NESO embedded wind and solar forecast archives

**Status:** proposed
**Date:** 2026-10-09
**Phase:** v0.22 unit EF (embedded archives)
**Amends:** ADR-035 P-10 (the archive family's dump is split into its own owner).
**Cross-references:** ADR-033 (registry, upload leg), ADR-034 (generic engine, clocks,
`_latest`), ADR-035 (dump leg, `capture_fallback`), ADR-036 (records, `zone_evidence`),
ADR-037 (sibling-fed owners).

## Context

The package `embedded-wind-and-solar-forecasts` holds the live forecast (resource
`db6c038f-…`, served by the bespoke `embedded_wind_solar_forecast` transformer) and an
archive family, `embedded_wind_solar_forecast_archive`, holding seven yearly CSV uploads
(2019–2025) and the 2026 datastore dump. Before EF the archive family had no record: its
bronze was captured, nothing reached silver.

Measured on the 2026-10-08 bronze (read-only):

- Every captured body (2019–2024 uploads, 2026 dump) shares one nine-column header, **H9**:
  `DATE_GMT,TIME_GMT,SETTLEMENT_DATE,SETTLEMENT_PERIOD,EMBEDDED_WIND_FORECAST,EMBEDDED_WIND_CAPACITY,EMBEDDED_SOLAR_FORECAST,EMBEDDED_SOLAR_CAPACITY,Forecast_Datetime`.
  The 2025 upload (645,655,728 B, above the old 512 MiB cap, never captured) begins with
  **H10** = H9 + `,source_file` (the seat's 8,192-byte ranged probe, RULINGS 514).
- The uploads write every datetime with a trailing `Z`; the dump writes none.
- From `Forecast_Datetime` `2026-06-12T11:54:02` the dump changes shape (the forecast-system
  migration): `TIME_GMT` drops its seconds, and on three issues (1,887 rows)
  `SETTLEMENT_DATE` carries the period's time rather than midnight; its date part is still
  the settlement date.
- `(SETTLEMENT_DATE, SETTLEMENT_PERIOD, Forecast_Datetime)` is unique in every body; a
  target alone repeats about 300 times.
- The 2019 body is truncated by the vendor: the declared `Content-Length` equals the
  260,472,832 B received, the last row ends mid-value (`2019-12-2`), and issues stop at
  2019-12-21T03:12.

## Decision

**P-1 — two owners; the archive key stays the bronze home.** One record on the archive
family cannot exist: V-8 refuses `ckan_last_modified` for any family whose resources include
a datastore one, and one epoch cannot type both the uploads' `Z` datetimes and the dump's
naive ones (an epoch is chosen by exact header alone). So `embedded_wind_solar_forecast_archive`
stays recordless (reconcile lists it as skipped `ingest-only`), and two sibling-fed owners
(ADR-037 P-10) carry the records, each with `siblings: [embedded_wind_solar_forecast_archive]`
and no resources of their own:

| Owner | Resources (all keep `family: embedded_wind_solar_forecast_archive`) | Vintage |
|---|---|---|
| `embedded_forecast_archive_upload` | 2020–2025 uploads | `ckan_last_modified` |
| `embedded_forecast_archive_dump` | the 2026 datastore dump | `capture_fallback` |

The keys are short for MAX_PATH margin (243 characters worst case under `C:/gridflow-data`,
against 254 for `embedded_wind_solar_forecast_archive_upload`). The archive is therefore
served by **two relations**; a consumer that wants the whole archive unions them. Bronze does
not move (registry keys are frozen once bronze exists).

**P-2 — the records.** Both: `reader: csv`, `temporal: sp_pair (settlement_date,
settlement_period)`, `entity_key: [settlement_date, settlement_period, issue_time]`,
`latest: key_latest`, issue recipe `data_column` on `forecast_datetime`. Columns, in header
order: `date_gmt_raw` and `time_gmt_raw` (strings, unparsed: their forms change in the
migration), `settlement_date` (date), `settlement_period` (int64, 1..50), the four MW
columns (float64, nullable: the dump carries 48 vendor nulls in solar, kept), and
`forecast_datetime` (datetime, `Europe/London`, `ambiguous: earliest`, P-3). Formats differ:

| | Upload record | Dump record |
|---|---|---|
| `SETTLEMENT_DATE` | `%Y-%m-%dT00:00:00Z` (strict midnight) | `%Y-%m-%dT%H:%M:%S` (keeps the date of the migration rows) |
| `Forecast_Datetime` | `%Y-%m-%dT%H:%M:%SZ` (a literal `Z`, not `%z`) | `%Y-%m-%dT%H:%M:%S` |

The upload record carries two epochs: H9 (2020–2024) and H10 (2025), the same nine column
specs plus `source_file`. `source_file` lands as a nullable string, stored as-is: an epoch's
columns must align 1:1 with its header, so excluding it is not expressible without engine
change; it is typed null on every H9 row and kept out of `entity_key` (the key is unique
without it, and a key column must not be null). The dump record has H9 only: a dump that
grows `source_file` fails loud with `HeaderEpochError`.

**Clocks.** Upload rows get `published_at` = `available_at` = the archive's CKAN
`last_modified` (ADR-034 P-5): **as-of reads before an archive's publication see none of its
rows.** Dump rows get `published_at` null and `available_at` = the capture's `written_at`
(ADR-035 P-10); the dump's sidecar `last_modified` is metadata time and is never used.
`_latest` keeps one row per `(settlement_date, settlement_period)`, ordered by `issue_time`
then `available_at`.

**P-3 — `Forecast_Datetime` is UK local time, not UTC.** The evidence, measured on the
uploads and the dump:

- **Spring gap.** On 2023-03-26, 2024-03-31 and 2026-03-29 there is no 01:xx issue (the
  hourly `:12` issues run 00:12, 02:12, 03:12), though 01:12 UTC is a real instant.
- **Fold.** On 2023-10-29 and 2024-10-27 there is a single 01:12 issue, and its first target
  ends 00:30 UTC: the BST occurrence.
- **Seasonal step.** The median of (`Forecast_Datetime` read as UTC − first target end) is
  −18 minutes in winter and +42 in summer; read as London time the step collapses to a
  constant −18.

So the column is typed `Europe/London`, fold `earliest`, with that evidence as its
`zone_evidence`. Rejected: **UTC as labelled** (every BST `issue_time` an hour wrong), and
**string with issue recipe `none`** (the target-only key repeats about 300 times and would
raise `DuplicateEntityKeyError` on the first capture). NESO does not document the zone, so
both owners are `eligibility: held` under unit `E-SEM` with the question recorded on each
record. Rows after the dump's 2026-06-12 migration have crossed no DST transition yet; their
zone is unmeasured (residual R-2).

**P-4 — the 2019 upload is held.** Resource `bc4d1093` is `HOLD(E-SEM)`, reason: vendor
truncation; a strict cast fails the whole capture (D-41) and no null token or exclusion
rule can type `2019-12-2`. It is re-disposed only after NESO re-uploads (a new
`last_modified`). Its bronze is retained. With this, every CSV resource in the package is
transformed or held: live (bespoke), 2020–2024 (upload owner, H9), 2025 (upload owner, H10,
after the seat's recapture), 2026 dump (dump owner), 2019 (`HOLD`).

**P-5 — the archive cap.** `embedded_wind_solar_forecast_archive.max_download_bytes` rises
from 536,870,912 to **805,306,368** (768 MiB), 1.247× the 2025 body; the dump (379 MB over
294 days) stays under it at year end. The connector reads the cap off the capturing family,
so the owners' caps are inert. Ingest holds one body; typing peaks at about 6.6× the body
(3.0–3.4 GB measured on the 2020–2024 bodies), about 5.3 GB at the cap, inside 16 GB.
1 GiB was rejected: it would admit about 7 GB transforms for no known body. No other
family's cap moves.

**P-6 — 16b answered: no repoint.** The live forecast is resource `db6c038f-…` under the
legacy key, the single exact-name member of its selector. `_ISSUE_TOKEN_PATTERN` matches
the current and newer live filenames (`202610061925_…`, `202610080725_embedded_forecast.csv`)
and not `embedded_archive_2025.csv`. The filename token equals CKAN `last_modified` (UTC) to
the minute, which corroborates D-15. The bespoke transformer, `_bronze.py`, its schema and
selector are untouched. The bespoke reads only its own exact bronze partition (its
`BRONZE_SIBLING_DATASETS` is empty), so no archive body reaches it.

**P-7 — no combined live + archive `_latest`.** The dump holds every target of both live
captures, so ROADMAP unknown (b) expected `_latest` across the keys to be "a sibling read".
The engine refutes that: a generic owner transforms a sibling capture only when the
capture's single disposition names it, `vintage` is one value per record, and V-8 forbids
`ckan_last_modified` beside a datastore resource, so no one record can carry the uploads'
bound clock and the dump's. A combined owner would also have to reroute the live resource's
`SILVER` disposition away from the bespoke key, changing the bespoke family's input. EF
therefore ships three relations (live, upload archive, dump archive) and no combined
surface; any combined consumer view is the seat's to route.

## Migration facts (evidence only)

For 16b's class-3 migration instant: from `2026-06-12T11:54:02` the dump's `TIME_GMT` is
`HH:MM` (1,780,828 rows), three issues carry a non-midnight `SETTLEMENT_DATE` (1,887 rows,
each equal to the settlement date + (SP−1)×30 min), and the median issue lead moves from
about −18 to −34..−37 minutes. 2,753 issues and 1,781,476 rows follow the instant.

## Failure modes

- A body whose header is neither of its record's epochs (a third upload header, a dump that
  grows `source_file`) fails with `HeaderEpochError`: a failure record and a reconcile
  `failed` gap, fixed by a registry commit.
- A registry lagging a live `url_type` flip: a datastore sidecar under the upload owner
  raises `CaptureContextError`; an upload sidecar under the dump owner is dated by its
  capture time (later, so conservative).
- A transform killed mid-capture leaves no output and no completion (atomic write, then
  completion); reconcile reports `missing` and the drain re-runs it.
- The same `(target, issue)` in two captures (year-boundary overlap, repeated dump captures)
  keeps both rows; `_latest` resolves by `available_at`, then the capture tie-break.
- The full 2025 body is measured only by the seat's recapture: a repeated header line, a
  `(target, issue)` repeated across its sub-files or a new value shape fails the capture
  (strict cast or `DuplicateEntityKeyError`), never silently.

## Consequences and residuals

- **R-1.** The 2019 archive's 2,775,615 bronze rows are not in silver (P-4).
- **R-2.** If post-migration dump values are in fact UTC, their `issue_time` is an hour late
  on BST rows. The output is held; `_latest` order within a target is unchanged (a uniform
  shift), and the as-of barrier is `available_at`, not `issue_time`. A 01:xx value at the
  2027-03-28 gap fails the capture loudly (non-existent local time), which surfaces the
  question.
- **R-3.** On the post-migration fall-back night (2026-10-25) a dump that writes the same
  local `Forecast_Datetime` twice for one target fails with `DuplicateEntityKeyError` (the
  old system wrote one occurrence).
- **R-4.** The archive is two relations; reconcile lists the recordless archive key as
  skipped, its captures are reached through the owners' `siblings`.
- E-SEM receives both owners' zone questions and the 2019 reason.
