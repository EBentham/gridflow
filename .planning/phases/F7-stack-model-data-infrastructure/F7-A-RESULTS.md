# F7 Workstream A — Results

Date: 2026-05-07
Branch: `feat/f7-stack-and-bitemporal`
Repository: `gridflow`

## Task Status

| Task | Description | Status |
|---|---|---|
| A1 | RED tests for bitemporal columns and append-only on stack datasets | done |
| A2 | GREEN: extend `BaseSilverTransformer` with `APPEND_ONLY` flag and run-suffixed filenames | done |
| A3 | GREEN: REMIT preserves revisions, `DATASET_VERSION` bumped to 2.0.0 | done |
| A4 | Set `DATASET_VERSION` (and `APPEND_ONLY` where applicable) on the remaining stack transformers | done |
| A5 | Re-ingest the four stack datasets from existing bronze | **deferred** — bronze tree absent locally |

## Test Results

Final run: `uv run pytest -q`

```
1025 passed, 253 skipped, 1 warning in 56.21s
```

The 253 skipped tests are pre-existing live-API tests (`-m live`) that
remain opt-in. No tests fail. The 11 tests added by F7 Workstream A all
pass:

```
tests/property/test_bitemporal_stack_datasets.py    ......
tests/integration/test_append_only_writes.py        ...
tests/unit/test_remit_revision_preservation.py      ...
```

## Lint and Type-Check (F7-touched surface)

```
uv run ruff check src/gridflow/silver/base.py \
  src/gridflow/silver/elexon/{remit,bmunits,fou2t14d}.py \
  src/gridflow/silver/entsoe/installed_capacity_units.py \
  src/gridflow/silver/entsog/generic.py \
  src/gridflow/silver/neso/carbon_intensity.py \
  tests/property/test_bitemporal_stack_datasets.py \
  tests/integration/test_append_only_writes.py \
  tests/unit/test_remit_revision_preservation.py
# All checks passed!
```

```
uv run mypy <same files>
# Success: no issues found in 7 source files
```

The wider `src/gridflow/` mypy run reports 51 pre-existing errors in 22
files that pre-date F7 (notably `connectors/gie/client.py`,
`observability.py`, `cli.py`, `utils/logging.py`). None are in files
F7 modified. Those are out of scope per the executor's deviation rules
and are recorded as known baseline.

## A5 — Re-ingest Deferral

**Status:** Deferred to the user. Re-ingest commands have **not** been
executed.

**Reason:** The bronze tree on this machine does not contain any data for
the four F7 stack datasets. Verification:

```
data/bronze/elexon/remit                          → directory missing
data/bronze/elexon/fou2t14d                       → directory missing
data/bronze/elexon/bmunits_reference              → directory missing
data/bronze/entsoe/installed_capacity_units       → directory missing
```

The only bronze data present locally is for `elexon/fuelhh` (an unrelated
dataset).

**Per executor instructions:** "If bronze does NOT exist for any dataset,
do NOT attempt re-ingest for it — instead, document the skip with a note
that the user must run the re-ingest commands once bronze is present."

**Required user action when bronze becomes available:**

```bash
uv run gridflow transform elexon remit --reingest --start 2022-01-01 --end 2026-05-04
uv run gridflow transform elexon bmunits_reference --reingest --start 2022-01-01 --end 2026-05-04
uv run gridflow transform elexon fou2t14d --reingest --start 2022-01-01 --end 2026-05-04
uv run gridflow transform entsoe installed_capacity_units --reingest --start 2022-01-01 --end 2026-05-04
```

After re-ingest, capture row counts and append a follow-up section to
this file. For REMIT specifically, the row-count delta against the pre-F7
silver represents the historical revisions previously discarded by the
removed `df.unique(subset=["mrid"], keep="last")` line.

## REMIT Row-Count Delta

Cannot be measured locally — no pre-F7 REMIT silver exists either. The
delta is to be captured by the user during the deferred re-ingest above.

## Deviations from Plan

1. **Lint follow-up commit (`chore(F7)`).** A1 RED tests included
   unused `datetime` imports and a `pytest` import that ruff TC002
   wants gated behind `TYPE_CHECKING`. These were minor oversights in
   the test files just authored; fixed in `8cf3a76 chore(F7): lint
   cleanup in F7 test files`. No semantic change.
2. **`_write_silver` signature change beyond the four stack datasets.**
   A2 needed `available_at` inside `_write_silver` to derive the
   append-only filename suffix. The cleanest implementation changes the
   base signature; three pre-existing override sites (`bmunits.py`,
   `entsog/generic.py`, `neso/carbon_intensity.py`) were updated to
   accept the new keyword. Their behaviour is unchanged.

No checkpoints were hit. No architectural Rule 4 decisions arose.

## Commits

```
2d2fecd test(F7): RED tests for bitemporal columns and append-only on stack datasets
6a0e828 feat(F7): add APPEND_ONLY flag and run-suffixed filenames to BaseSilverTransformer
cf8ff1a feat(F7): preserve REMIT revisions in silver, bump DATASET_VERSION to 2.0.0
091bbef feat(F7): set DATASET_VERSION and APPEND_ONLY on remaining stack transformers
8cf3a76 chore(F7): lint cleanup in F7 test files
```

(plus a `docs(F7)` commit installing this RESULTS file alongside.)

## Architectural Decision Records Added

- `docs/DECISION_LOG/ADR-017-append-only-class-attribute.md`
- `docs/DECISION_LOG/ADR-018-append-only-run-suffixed-files.md`

ADR-019 (`QUALIFY` partition columns are per-dataset configuration) and
ADR-020 (`ManualCommodityPriceSource` is an adapter in
`gridflow_models`) belong to the `gridflow_models` repository and are
out of scope for Workstream A.
