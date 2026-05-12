---
phase: V2
plan_id: V2-PLAN-E-entsog-and-ngeso
slug: entsog-404-shortcut-and-ngeso-triage
status: draft
milestone: v0.10
wave: 2
severity: LOW
depends_on:
  - V2-PLAN-A-elexon-freq-fix
  - V2-PLAN-B-neso-region-period-fields
autonomous: false  # ngeso decision is a user-input moment if directory non-trivial
files_modified:
  - src/gridflow/connectors/entsog/client.py
  - tests/integration/test_entsog_e2e.py
  - src/gridflow/connectors/ngeso/__init__.py  # if delete path
  - docs/DECISION_LOG/ADR-020-ngeso-placeholder.md  # if keep path
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\README.md  # mention 404 short-circuit
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsog-VALIDATION.md
requirements:
  - V2-FIX-07  # ENTSOG @RETRY_POLICY 404 short-circuit
  - V2-TRIAGE-01  # ngeso placeholder
---

# V2 Plan E — ENTSOG `@RETRY_POLICY` 404 Short-circuit + ngeso Triage

## Goal

1. **ENTSOG retry budget** (V2-FIX-07). The vendor's documented empty
   convention is `HTTP 404 + body {"message":"No result found"}`. The
   current `@RETRY_POLICY` retries on `httpx.HTTPStatusError`, which
   means the connector retries 404 up to N times before reraising —
   wasted budget for an expected response. Short-circuit 404+empty so
   ENTSOG returns an empty bronze immediately without retries.

2. **`connectors/ngeso/` placeholder** (V2-TRIAGE-01). The directory
   contains only `__init__.py`. V1's close-out flagged it for triage.
   Decide: delete (default) OR keep with ADR-020 explaining why.

## must_haves (goal-backward verification)

1. `EntsogConnector._request` (or its retry wrapper) treats `httpx.HTTPStatusError`
   with `response.status_code == 404` AND `response.json().get("message") ==
   "No result found"` as an empty-bronze success — no retries, no exception.
2. The retry budget for non-404 5xx errors is unchanged.
3. A respx-mocked test simulates 404+empty-message and asserts the
   connector emits exactly **one** request (not N).
4. A respx-mocked test simulates 404+different-message (e.g. plain
   404 from an actual error) and asserts the connector still retries
   (then reraises) — preserves error visibility for non-empty 404s.
5. `connectors/ngeso/` is either:
   - **Default — deleted.** `git rm src/gridflow/connectors/ngeso/__init__.py`,
     remove parent dir, and add `## Deleted directories` line under
     `.planning/STATE.md` `Decisions` block.
   - **Keep — ADR-020 written.** `docs/DECISION_LOG/ADR-020-ngeso-placeholder.md`
     explains the long-term intent and what the directory is reserved
     for.
6. `entsog-VALIDATION.md` has a `## V2 re-validation` section with the
   404 short-circuit confirmation.
7. Vault `30-vendors/entsog/README.md` "Known gotchas" mentions the
   404 short-circuit behaviour.
8. `uv run pytest -m "not live and not slow" -x -q` passes.

## Tasks

### Task 1 — Pre-flight (no key needed for ENTSOG)

<read_first>
- .planning/phases/V1-vault-vendor-validation-and-docs/V1-CONTEXT.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-CONTEXT.md
</read_first>

<action>
1. `[ -f .env ] || cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env` (consistency)
2. `mkdir -p .tmp`
3. Smoke + ENTSOG health:
   ```bash
   curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity
   curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://transparency.entsog.eu/api/v1/aggregateInterconnections
   ```
   Both `200`.
</action>

<acceptance_criteria>
- Both smoke tests `200`.
</acceptance_criteria>

### Task 2 — Capture a 404+empty-message fixture

<read_first>
- src/gridflow/connectors/entsog/client.py (line 77 area)
- src/gridflow/utils/retry.py (read `RETRY_POLICY` definition)
- .planning/phases/V1-vault-vendor-validation-and-docs/entsog-VALIDATION.md §"Findings §1"
</read_first>

<action>
Capture a real 404+empty-message live response for fixture material:

```bash
curl --ssl-no-revoke -sS -H "Accept: application/json" \
  "https://transparency.entsog.eu/api/v1/operationalData?from=2026-05-06&to=2026-05-06&indicator=Methane%20Content&pointDirection=ITP-00005exit" \
  -o .tmp/entsog-404-empty.json -w "HTTP %{http_code} | %{size_download}B\n"
```

Expected: HTTP 404, body `{"message":"No result found"}`, ~29 bytes.

Save the body verbatim to
`tests/fixtures/entsog/empty_404_response.json`.
</action>

<acceptance_criteria>
- `.tmp/entsog-404-empty.json` exists with HTTP 404 and the expected
  message body.
- `tests/fixtures/entsog/empty_404_response.json` is the trimmed
  fixture (just the body — `{"message":"No result found"}`).
</acceptance_criteria>

### Task 3 — Apply the 404 short-circuit fix

<read_first>
- src/gridflow/connectors/entsog/client.py (full file, especially
  the `_request` method around line 77 and the `@RETRY_POLICY`
  decorator on it)
- src/gridflow/utils/retry.py (to understand which exception types
  RETRY_POLICY catches)
</read_first>

<action>
Two safe places to apply the short-circuit:

**Option A — wrap inside the `@RETRY_POLICY`-decorated function.**
Catch `HTTPStatusError` for 404 inside `_request`, parse the body, and
if the body matches the empty-message convention return a synthetic
"empty" response (e.g. raise a typed `EntsogEmptyResponse` that the
caller treats as empty-bronze). This is the more surgical fix.

**Option B — short-circuit before raise_for_status().**
Replace `resp.raise_for_status()` with a custom raise: if `resp.status_code
== 404` and `resp.json().get("message") == "No result found"`, return
the response (with empty body) instead of raising. The caller's bronze
writer treats an empty body as empty-bronze (verify).

**Default: Option B** (minimal blast radius). Edit
`src/gridflow/connectors/entsog/client.py` `_request`:

```python
@RETRY_POLICY
async def _request(
    self, path: str, params: dict[str, Any]
) -> httpx.Response:
    """Rate-limited, retried HTTP GET request.

    Vendor empty convention: HTTP 404 + body {"message":"No result found"}.
    Short-circuit that to "empty success" so RETRY_POLICY does not
    waste budget.
    """
    if self._client is None:
        raise RuntimeError(
            "Connector not initialized. Use 'async with' context manager."
        )
    if self._semaphore is None:
        raise RuntimeError(
            "Semaphore not initialized. Use 'async with' context manager."
        )

    async with self._semaphore:
        resp = await self._client.get(path, params=params)
        if resp.status_code == 404:
            try:
                body = resp.json()
            except (ValueError, json.JSONDecodeError):
                body = {}
            if body.get("message") == "No result found":
                # Documented vendor empty convention; do not retry.
                return resp
        resp.raise_for_status()
        return resp
```

Add the necessary `import json` at the top if not present.

Document the short-circuit at the top of the function and in the
caller comment chain — the caller's bronze writer must treat
`status_code == 404` as "empty bronze, not failure". Verify the
caller does this; if not, update the caller too (likely
`EntsogConnector.fetch` in the same file).
</action>

<acceptance_criteria>
- `EntsogConnector._request` returns the 404 response when the body
  matches `{"message": "No result found"}`, without raising.
- For any other 404 body (or any non-404 status) the existing
  raise_for_status() behaviour holds.
- The caller (`EntsogConnector.fetch` or equivalent) writes an empty
  bronze for the short-circuited 404 — verify by inspection.
- `uv run mypy --strict src/gridflow/connectors/entsog/client.py`
  exits `0`.
</acceptance_criteria>

### Task 4 — Add regression tests

<read_first>
- tests/integration/test_entsog_e2e.py
- tests/fixtures/entsog/empty_404_response.json
</read_first>

<action>
Add two respx-mocked tests:

```python
import json
import httpx
import pytest
import respx

from gridflow.connectors.entsog.client import EntsogConnector


@respx.mock
async def test_404_no_result_found_short_circuits_no_retry():
    """V2-FIX-07: vendor empty convention 404+'No result found' must
    not consume retry budget."""
    route = respx.get(...).mock(
        return_value=httpx.Response(
            404, json={"message": "No result found"}
        )
    )
    async with EntsogConnector(...) as conn:
        result = await conn.fetch(...)
    assert route.call_count == 1, (
        "expected exactly 1 request; got "
        f"{route.call_count} — RETRY_POLICY did not short-circuit"
    )
    assert result == [] or result is None  # empty-bronze convention


@respx.mock
async def test_genuine_404_still_retries():
    """A non-empty-message 404 (real error) preserves the retry path."""
    route = respx.get(...).mock(
        return_value=httpx.Response(
            404, json={"message": "Endpoint not found"}
        )
    )
    async with EntsogConnector(...) as conn:
        with pytest.raises(httpx.HTTPStatusError):
            await conn.fetch(...)
    assert route.call_count > 1, (
        "expected multiple requests for genuine 404; got "
        f"{route.call_count}"
    )
```

Fill in the actual respx URL pattern by reading the existing tests'
URL pattern style.
</action>

<acceptance_criteria>
- Both tests exist in `tests/integration/test_entsog_e2e.py`.
- Before the Task 3 fix the first test FAILS (call_count > 1).
- After the Task 3 fix both tests PASS.
- `uv run pytest -m "not live and not slow" -x -q -k "404 or no_result_found" tests/`
  passes.
</acceptance_criteria>

### Task 5 — `connectors/ngeso/` triage

<read_first>
- src/gridflow/connectors/ngeso/__init__.py
- `grep -rn "ngeso" src/gridflow/` to see if anything imports from it
- `grep -rn "ngeso" config/sources.yaml`
</read_first>

<action>
1. Confirm the directory contains only `__init__.py` (no other files,
   no module-level imports).
2. Confirm nothing imports `gridflow.connectors.ngeso` (grep — should
   return only the directory's own `__init__.py`).
3. Confirm `config/sources.yaml` has no `ngeso:` block.

**Default — delete:**
```bash
rm "src/gridflow/connectors/ngeso/__init__.py"
rmdir "src/gridflow/connectors/ngeso"
```

Update `.planning/STATE.md` `Decisions` block:
- "V2 deleted empty `connectors/ngeso/` placeholder. NESO carbon-intensity
  data is served via the active `connectors/neso/` module; ngeso was a
  legacy directory placeholder with no implementation."

**Alternative — keep + ADR:**
Write `docs/DECISION_LOG/ADR-020-ngeso-placeholder.md`:

```markdown
# ADR-020: connectors/ngeso/ placeholder retained

**Status:** Accepted
**Date:** 2026-05-09
**Phase:** V2

## Context

`src/gridflow/connectors/ngeso/__init__.py` is the only file in an
otherwise empty package. V1's close-out flagged it for triage.

## Decision

Retain. The directory is reserved for future National Grid ESO (NGESO)
operational endpoints — distinct from NESO Carbon Intensity API.
NGESO publishes a separate set of operational data (e.g. balancing
mechanism summaries, reactive power, demand control) that may become
in-scope post-v0.10.

## Consequences

- The empty package costs nothing at import time.
- Future contributors importing `gridflow.connectors.ngeso` get a no-op
  package; they must add real code before any ingest call works.

## Rollback

`git rm -rf src/gridflow/connectors/ngeso/`.
```

**Choose default unless live grep reveals dependency.**
</action>

<acceptance_criteria>
- The chosen path is applied; no other files touched.
- If delete: `ls src/gridflow/connectors/` does not include `ngeso/`.
- If keep: ADR-020 exists.
- `uv run pytest -m "not live and not slow" -x -q` still passes.
</acceptance_criteria>

### Task 6 — Vault README update

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\README.md
</read_first>

<action>
1. Find the "Known gotchas" or "Implementation notes" section in
   `entsog/README.md`.
2. Add a bullet:
   - `2026-05-09 — V2: connector now short-circuits HTTP 404 + body
     `{"message":"No result found"}` as the vendor's documented empty
     convention. Retry budget is preserved for genuine 5xx errors.`
3. Bump `last_verified: 2026-05-09`.

Do NOT touch any per-dataset page (the 404 short-circuit is a connector
detail, not a per-endpoint behaviour).
</action>

<acceptance_criteria>
- `entsog/README.md` has the new bullet and `last_verified: 2026-05-09`.
- Per-dataset pages untouched.
</acceptance_criteria>

### Task 7 — Re-validation evidence

<action>
Append to V1's `entsog-VALIDATION.md` under `## V2 re-validation`:

```markdown
## V2 re-validation (2026-05-09)

**Fix commit:** <SHA>

ENTSOG connector short-circuits HTTP 404 + body
`{"message":"No result found"}` as the vendor's documented empty
convention. Verified via respx-mocked tests that the call_count for
this case is exactly 1 (no retries). Genuine non-empty 404s preserve
the existing retry behaviour.

`connectors/ngeso/`: <deleted | retained per ADR-020>.
```
</action>

<acceptance_criteria>
- `entsog-VALIDATION.md` has the new V2 section.
- V1 tables untouched.
</acceptance_criteria>

### Task 8 — Commit

<action>
Stage and commit:

```
git add src/gridflow/connectors/entsog/client.py
git add tests/integration/test_entsog_e2e.py
git add tests/fixtures/entsog/empty_404_response.json
# If delete path:
git rm src/gridflow/connectors/ngeso/__init__.py
# If keep path:
git add docs/DECISION_LOG/ADR-020-ngeso-placeholder.md
git add .planning/phases/V1-vault-vendor-validation-and-docs/entsog-VALIDATION.md
git add .planning/STATE.md  # ngeso decision note
```

Commit message:

```
fix(V2-E): ENTSOG short-circuits 404+empty body; <ngeso deleted | retained>

ENTSOG vendor convention is HTTP 404 with body
{"message":"No result found"} for empty datasets. The previous
@RETRY_POLICY-decorated _request retried 404 up to N times before
reraising — wasted budget for an expected response.

Now: 404 + that exact body returns the response immediately
(empty-bronze success). Other 404s (genuine errors) preserve the
existing retry path. Two respx-mocked regression tests prove both
branches.

ngeso: <deleted empty placeholder | ADR-020 retains for future NGESO
operational endpoints>.

Closes V2-FIX-07, V2-TRIAGE-01.
```
</action>

<acceptance_criteria>
- `git log --oneline -1` shows commit prefix `fix(V2-E):`.
- `git status` clean modulo `.tmp/` and out-of-tree vault.
</acceptance_criteria>

## Risks / known-unknowns

- **Bronze-write contract.** Confirm the connector's bronze writer
  treats an empty-body 404 response as "empty bronze" (skip-write or
  zero-byte file with metadata). If it currently throws, Task 3
  needs to update the caller too. Read `EntsogConnector.fetch` to
  verify.
- **Other 404 messages.** ENTSOG may emit 404 with different body
  shapes for genuine errors (endpoint not found, malformed param). The
  Task 4 second test guards against this — keep it strict.
- **`json` import.** If `client.py` does not currently import `json`,
  add it. Avoid `httpx.Response.json()` directly throwing on a body
  that's not strict JSON; the `try`/`except` in Task 3 covers this.

## Verification

```bash
uv run pytest -m "not live and not slow" -x -q
uv run mypy --strict src/gridflow/connectors/entsog/client.py
uv run ruff check src/gridflow/
```

All three must exit `0`.
