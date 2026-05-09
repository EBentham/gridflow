# ADR-019 — ENTSOE A09 commercial_schedules registry dedup

Status: Accepted (V2, 2026-05-09)

## Context

V1 live validation 2026-05-08 confirmed that the dataset keys
`commercial_schedules` and `commercial_schedules_net_positions` use an
identical `EntsoeDocType("A09", None, ..., zone_pair,
optional_params=("contract_MarketAgreement.Type",))` and return the
identical XML payload (5296 bytes, 2 TimeSeries) for the identical
GB→FR request. The dataset key distinction is silver-transformer label
only; no semantic difference at bronze.

Both keys are wired through:
- `connectors/entsoe/endpoints.py` `ENDPOINTS` dict (one entry per
  key)
- `silver/entsoe/h6_market.py` (one transformer subclass per key,
  both extending `_H6QuantityTransformer` with no overrides)
- `config/sources.yaml` (one block per key)
- Inventory tests that enumerate active datasets

The redundancy was an oversight from H6's bulk `_H6QuantityTransformer`
roll-out — the `_net_positions` variant was registered with an
intent to derive a true signed `net_position_mw` column later, but
that derivation was never implemented. As a result, both silver
tables today contain the same per-direction TimeSeries rows; neither
is a "net" view.

## Decision

**Drop `commercial_schedules_net_positions`.**

- Remove its entry from `connectors/entsoe/endpoints.py::ENDPOINTS`.
- Remove its block from `config/sources.yaml`.
- Remove `CommercialSchedulesNetPositionsTransformer` from
  `silver/entsoe/h6_market.py` (class deletion + `_TRANSFORMERS`
  list entry).
- Remove the dataset key from any `tests/` parametrize list that
  enumerates active datasets.

Existing silver Parquet at
`silver/entsoe/commercial_schedules_net_positions/...` is left in
place as a historical artefact — no scheduled gold consumer reads
it; deleting it would be an unrelated data-side cleanup.

## Alternatives considered

**Option B — derive net positions.** Keep both keys and rewrite
`CommercialSchedulesNetPositionsTransformer` so it pairs the two
zone-pair directions (GB→FR and FR→GB) per period and emits a real
signed `net_position_mw = imports - exports`. This would be the
"correct" net-positions silver table and would justify keeping the
dataset registered.

Rejected for V2 because:
1. No current downstream gold consumer needs net positions.
2. The pairing logic is non-trivial (must match periods, handle
   missing-direction periods, decide sign convention) and would be a
   net-new transformer family rather than a one-line rename.
3. Without a consumer to specify the exact semantics
   (signed-from-which-side, periods with one-sided missing, etc.),
   shipping a half-spec would be technical debt either way.

Recorded as a backlog item (`docs/DECISION_LOG/`-adjacent: appears
in `.planning/ROADMAP.md` Backlog after V2-PLAN-F close-out) for
when a real net-positions consumer materialises.

## Consequences

- Future `gridflow ingest entsoe commercial_schedules_net_positions`
  fails with "unknown dataset" — acceptable; no scheduled job uses
  it.
- Existing silver Parquet remains queryable but is frozen at the
  last V1 ingest.
- A future signed-net-positions dataset can be added cleanly under a
  more semantically accurate key (e.g. `commercial_net_position`) in
  a fresh phase if a consumer materialises.
- Inventory tests' active-dataset count drops by 1 (ENTSOE goes
  from 48 → 47 active).
- V1 `commercial_schedules.md` cross-link to
  `commercial_schedules_net_positions.md` becomes a deprecation
  notice; the deprecated page stays for historical reference.

## Rollback

Re-add the key to `ENDPOINTS`, `config/sources.yaml`, and
`_TRANSFORMERS` from this commit's git diff. The
`CommercialSchedulesNetPositionsTransformer` class is restored
verbatim (it had no overrides). Inventory tests' parametrize lists
restore the dataset.
