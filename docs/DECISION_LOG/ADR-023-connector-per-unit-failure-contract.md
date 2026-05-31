# ADR-023 — Connector per-unit failure contract: surface-vs-swallow after retries

Status: Accepted (contract); remediation (M3 / L4 / L5) DEFERRED post-v1.5

> The **contract below is Accepted** — the uniform per-unit failure rule
> (classify post-retry failures as definitive-absent vs transient, propagate the
> transient case) is the decided gridflow connector policy. Its **remediation
> (review findings M3 + L4 + L5) is explicitly DEFERRED to a post-v1.5 gridflow
> connector-failure follow-up and is NOT a v1.5 close-blocker** — no connector
> code has been changed yet under this ADR. The two blocking High findings
> (H1 realised-join, H2 unmapped-enum) are tracked separately (gridflow_models
> ADR-051 and this repo's ADR-022). The one genuinely open design choice
> (fail-the-run vs structured dropped-unit marker) is recorded below for the
> implementing phase.
>
> (Renumbered from a colliding ADR-022 draft to ADR-023 in F32; gridflow's
> unmapped-enum-code policy keeps ADR-022.)

## Context

The v1.5 "Connector Completeness" milestone set out to close the
**silent-incomplete-bronze** class: a connector that iterates a list of
sub-units (settlement-period pages, weather locations, GIE member countries)
and, on a post-retry transient, writes a short bronze layer that is recorded as
success and is indistinguishable from a genuinely-absent unit. That defect class
is tracked in
`gridflow_models/.planning/issues/code-review-2026-05/13-connector-incomplete-fetch.md`
(issue-13, findings 183–186).

The milestone fixed the pattern for two connectors but left the third
inconsistent, and shipped two documentation artifacts that assert failure
guarantees the code does not have. The v1.5 pre-close review surfaced three
related findings on this one contract:

### M3 — GIE legacy `_fetch_legacy_country_dataset` still swallows a per-country failure

`src/gridflow/connectors/gie/client.py:99-106` wraps each country's fetch in a
bare `except Exception as exc:  # noqa: BLE001` that logs at WARNING and
continues the loop:

```python
for country in countries:
    try:
        country_responses = await self._fetch_country(...)
        responses.extend(country_responses)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch %s/%s for country %s: %s", ...)
```

`_request` is `@RETRY_POLICY`-decorated with `reraise=True`, so a country that
keeps 5xx-ing after the 5-attempt policy is exhausted re-raises into this bare
except, is logged-and-skipped, and `fetch()` returns the surviving countries as
a clean success. Per the CLI ingest path (`src/gridflow/cli.py`), bronze is
written from whatever `fetch()` returns and the run is marked complete — so an
ALSI/`lng` country that transiently 500s is silently dropped from bronze with no
error. This is the structurally identical anti-pattern that commit `b3796da`
**removed** for Open-Meteo. `gie_alsi`/`lng` routes through this legacy path
(`source_name != "gie_agsi"`), and no downstream completeness check exists
(`src/gridflow/quality/checks.py` has no country-completeness check), so a
dropped country is invisible.

### L4 — `_last_page` does NOT fail closed on a bare-array / null JSON body

Commit `99662ce` reused the AGSI `_last_page` helper as the legacy paginator's
terminator and added a comment at `src/gridflow/connectors/gie/client.py:356-358`
asserting it "fails closed to 1 page on a malformed or `last_page`-less body."
That claim is **false for a valid-JSON non-dict body**. The helper
(`gie/client.py:413-421`) catches only `JSONDecodeError` and `(TypeError,
ValueError)`, not the `AttributeError` raised by `payload.get(...)` when
`payload` is a list or `None`. Verified empirically (2026-05-31):

| body | `_last_page` result |
|---|---|
| `b''` | `1` |
| `b'{}'` | `1` |
| `b'{"last_page": null}'` | `1` |
| `b'null'` | **raises `AttributeError`** |
| `b'[]'` | **raises `AttributeError`** |
| `b'[{"x":1}]'` | **raises `AttributeError`** |

So the dict-with-null case is fine; only a bare-array or `null` top-level body
trips it. On the AGSI `_fetch_paginated` path a bare-array body fails the run;
on the legacy `_fetch_country` path the `AttributeError` is **caught by the M3
swallow** → the country is logged-but-dropped. `99662ce` also removed the old
`except Exception: break` that had previously kept page 1, so the legacy path is
strictly worse on a non-dict body than before the commit. The common live
envelope is a dict, so this is a defensive/edge path — but the shipped comment
and the commit message both assert a safety property the code lacks.

### L5 — Open-Meteo `_request` docstring still claims the warning-only swallow is "unchanged"

Commit `b3796da` removed the per-location `except Exception: logger.warning`
swallow in `fetch()` so a post-retry location failure now propagates — the
`fetch()` body (`src/gridflow/connectors/openmeteo/client.py:69-78`) now calls
`_fetch_location` bare, with a comment stating the failure "propagates to the
caller." But the `_request` docstring in the *same file* (`:122-127`) still says:

> "The per-location failure policy after retries are exhausted is unchanged (the
> caller's warning-only swallow); only transient recovery is added here."

This is stale and directly contradicts the swallow removal in the same commit;
it will mislead a reader about the connector's failure contract.

### Scope honesty (why this warrants follow-up but is not a v1.5 close-blocker)

- **L5 is the genuinely v1.5-introduced defect**: `b3796da` removed the swallow
  but left its own docstring describing the old behaviour.
- **M3 and L4 are pre-existing but newly-relied-upon**: the GIE legacy swallow
  predates v1.5 (`is_v15_scope: false`), and `_last_page` predates v1.5 — but
  `99662ce` newly *relies* on `_last_page` as the legacy terminator and ships a
  comment documenting a fail-closed guarantee it does not have.
- None of these is a v1.5 **close-blocker**. The two blocking High findings (H1
  F30 dead-code currency contract; H2 unmapped `flow_direction`) are tracked
  separately. These three are the same silent-incomplete-bronze *class*, now
  left inconsistent across connectors, and warrant a coherent follow-up so the
  next connector author does not re-introduce the swallow by copying GIE.

### Interaction note

M3 and L4 **compound on the same legacy path**: a bare-array/null body raises
`AttributeError` (L4), which is caught by the M3 swallow and becomes a
logged-but-dropped country. They are distinct defects with distinct fixes but
must be fixed together — fixing only M3 (narrowing the except to 404/empty)
would let the L4 `AttributeError` propagate and fail the run on an
envelope-shape edge; fixing only L4 leaves the country-level 5xx swallow intact.

## Decision (proposed — pending sign-off)

Adopt **one uniform per-unit failure contract** across every list-iterating
connector, keyed on the Elexon `_fetch_date_period` reference pattern (issue-13
finding 183):

> After the shared `@RETRY_POLICY` is exhausted, a per-unit fetch failure is
> classified by *what the failure is*, not swallowed by a blanket `except`:
> - **Definitive-absent** — HTTP 404, or HTTP 200 with an empty payload →
>   tolerate: skip that unit, continue the loop. A legitimately empty/absent
>   unit is not an error.
> - **Transient/server** — post-retry 5xx or timeout (anything that escaped the
>   5-attempt policy) → **propagate** (fail-loud). A partial bronze is never
>   recorded as a complete success.

Crucially, "uniform rule" means applying this **distinction** uniformly — it
does **not** mean "remove the `except` everywhere." How the rule lands depends on
whether a unit can be *legitimately* absent:

1. **Open-Meteo** iterates a *fixed* capacity-weighted location list — no
   location is ever legitimately absent. The distinction therefore collapses to
   "propagate everything," which is exactly what `b3796da` did by removing the
   swallow. **No further connector-behaviour change required**; only the stale
   docstring (L5) needs aligning to the shipped fail-loud behaviour.

2. **GIE legacy `_fetch_legacy_country_dataset`** iterates *member countries*
   where an empty window / 404 is legitimate on a blind-iterated list. The fix
   is therefore to **narrow** the `except` at `gie/client.py:99-106` to tolerate
   only 404 / empty-window, and let a post-retry 5xx/timeout propagate (matching
   `b3796da` and the AGSI path). **Do NOT blanket-remove the `except`** — that
   would regress legitimate empty windows on the member-country list. As an
   alternative to propagation, emit an explicit structured "country dropped"
   marker the orchestrator can fail on (see open question below).

3. **`_last_page` must genuinely fail closed (L4)**: guard the helper so a
   valid-JSON non-dict body returns 1 rather than raising — either
   `if not isinstance(payload, dict): return 1` or by catching `AttributeError`
   alongside the existing `(TypeError, ValueError)`. Then **correct the
   `:356-358` comment** so it matches the actual guarantee (it currently claims
   fail-closed on "a malformed or `last_page`-less body," which is only true for
   dict bodies). M3 and L4 must land in the same change.

4. **Documentation alignment (L5 + the L4 comment)**: every connector docstring
   and inline comment that describes the per-unit failure policy must state the
   chosen behaviour (post-retry 5xx propagates; 404/empty tolerated). Treat the
   docstring/comment as part of the contract, not commentary.

### Is an F32 follow-up phase warranted?

**Yes — recommend folding M3 + L4 + L5 into the F32 remediation phase as
ride-along Medium/Low items under this single connector-failure-contract**, per
the review's F32 scope sketch (which already places them there). They are *not*
close-blockers (H1/H2 are), so they do not gate the v1.5 milestone close; but
they are the same defect class and are cheapest to fix coherently as one unit
(M3 + L4 in one change on the GIE legacy path, L5 a one-line docstring edit).

### One open question for the reviewer (genuinely undecided)

For the **post-retry transient** case the recommendation above is
**propagate/fail-loud** (matching `b3796da` and the AGSI path). The one
genuinely open design choice the human must settle is *how* the failure
surfaces:

- **(a) Fail the run** — let the exception propagate out of `fetch()` so the CLI
  marks the date/run FAILED. Simple; consistent with Open-Meteo today.
- **(b) Structured "unit dropped" marker** — return the surviving units plus an
  explicit dropped-unit record the orchestrator inspects and can choose to fail
  on (or down-grade to a partial status). More work; lets a single transient
  country-drop be distinguished from a whole-run failure.

This mirrors the same partial-vs-failed-status question raised for the
unmapped-enum-code ADR; resolving both together (consistent CLI status taxonomy)
is preferable to deciding them in isolation.

## Consequences

**If accepted (proposed remediation lands in F32):**

- **Positive:** one connector failure contract across Elexon / Open-Meteo / GIE;
  the next connector author has a single rule to copy instead of three
  divergent precedents. A post-retry 5xx on any per-unit list can no longer be
  recorded as a complete bronze write. `_last_page` genuinely fails closed as
  its comment promises. Docstrings stop describing behaviour the code abandoned.
- **Negative / cost:** GIE legacy needs the 404-vs-5xx classification wired
  (httpx exception-type inspection, not a bare `except`), plus a regression test
  feeding a persistent country-level 5xx that asserts the run does **not** report
  complete, and a separate test feeding a legitimate empty/404 window that
  asserts the loop continues. L4 needs a unit test over the non-dict-body table
  above. Small, well-scoped; no schema or migration impact.
- **Reversal cost:** Low. All changes are connector-local; the downstream
  bitemporal invariants (`TrainingSet` leakage barrier, `available_at <= as_of`,
  the `QUALIFY ROW_NUMBER() ... ORDER BY available_at DESC` revision dedup) are
  untouched — this is a completeness/trust defect upstream of, and invisible to,
  those guards, and the fix must keep parameterised requests, atomic
  `os.replace` writes, tz-aware UTC, and settlement period `1..50`.

**If deferred (not folded into F32):** the GIE legacy path keeps silently
dropping a transiently-5xx-ing country and a bare-array body keeps becoming a
logged-but-dropped country; the shipped `_last_page` comment and Open-Meteo
docstring continue to assert guarantees the code lacks, so the next reader
trusts a contract that does not hold. This must then be recorded as an explicit
tracked deferral (STATE.md + issue-13 status), not left implicit.

## References

- v1.5 pre-close review synthesis (lives in the **gridflow_models** repo — this
  ADR is in **gridflow**; cross-repo by design):
  `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow_models/.planning/reviews/2026-05-31-v1.5-pre-close/REVIEW.md`
  — findings M3, L4, L5; ADR topic #3 and the F32 scope sketch.
- Source issue (also in **gridflow_models**):
  `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow_models/.planning/issues/code-review-2026-05/13-connector-incomplete-fetch.md`
  (issue-13, findings 183–186; `status: ready-for-agent` at time of writing).
- Reference pattern: Elexon `_fetch_date_period` 404-vs-transient distinction —
  `src/gridflow/connectors/elexon/client.py` (issue-13 finding 183, fixed
  `df41592`).
- Open-Meteo swallow removal (the precedent this ADR generalises):
  commit `b3796da`; current state `src/gridflow/connectors/openmeteo/client.py:69-78`
  (fail-loud) vs. the stale docstring `:122-127` (L5).
- GIE legacy swallow (M3): `src/gridflow/connectors/gie/client.py:99-106`.
- `_last_page` helper + inaccurate comment (L4): `src/gridflow/connectors/gie/client.py:413-421`
  (helper) and `:356-358` (the comment added in `99662ce`).
- Related ADR topics in the same review (separate decisions): gridflow_models
  ADR-051 (ENTSO-E realised-join contract, H1+M2+L1); gridflow ADR-022
  unmapped-enum-code policy + CLI partial-vs-failed status (H2) — the latter
  shares this ADR's open question on run-status taxonomy.
