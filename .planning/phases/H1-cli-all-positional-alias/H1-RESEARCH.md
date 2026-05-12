# Phase H1: Fix CLI `all` Positional Alias - Research

**Researched:** 2026-05-02
**Domain:** Typer CLI argument resolution, Python unit testing
**Confidence:** HIGH

## Summary

Phase H1 is a surgical one-function fix. The entire change lives in `_resolve_datasets`
inside `src/gridflow/cli.py`. The function currently has three branches:

1. `if all_flag:` — expand to all registered datasets for the source
2. `elif dataset:` — return `[dataset]`
3. `else:` — raise `typer.BadParameter`

The missing case is: `dataset == "all"`. When a user types `gridflow pipeline entsoe all
--last 24h`, typer sees `all` as the positional `dataset` argument (not the `--all` flag),
so `all_flag` is `False` and `dataset` is `"all"`. The fix is to treat this as equivalent
to `all_flag=True` inside `_resolve_datasets`.

Four commands call `_resolve_datasets`: `ingest`, `transform`, `pipeline`, and
`backfill`. The `pipeline` command calls `ingest` and `transform` directly with the
`dataset` value it received, so once `_resolve_datasets` is fixed, all four commands are
covered through the single helper — no per-command changes are needed.

`export_csv` also calls `_resolve_datasets` and would benefit from the same fix for
consistency, but CLI-01/CLI-02 do not explicitly cover it.

**Primary recommendation:** Add `or dataset == "all"` (case-insensitive) to the
`if all_flag:` guard in `_resolve_datasets`, then write unit tests that call
`_resolve_datasets` directly.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CLI-01 | User can run `gridflow pipeline entsoe all --last 24h` and it processes all ENTSO-E datasets (positional `all` treated as `--all`) | Fix the `if all_flag:` branch in `_resolve_datasets` to also match `dataset.lower() == "all"`; `pipeline` delegates to `ingest`/`transform` which each call `_resolve_datasets` independently, so the fix propagates automatically |
| CLI-02 | Same `all` positional alias works for `gridflow ingest` and `gridflow transform` subcommands | Covered by the same `_resolve_datasets` fix — both `ingest` (line 36) and `transform` (line 98) call the helper directly |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Argument aliasing (`all` -> `--all`) | CLI helper (`_resolve_datasets`) | — | All commands funnel through this single function; fix here propagates everywhere |
| Dataset enumeration | Config layer (`SourceConfig.datasets`) | — | `_resolve_datasets` reads from settings; no change needed there |
| Command routing | Typer app layer | — | No change; typer already passes positional args correctly |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| typer | project-pinned | CLI framework | Already in use; `CliRunner` from `typer.testing` for test isolation |
| pytest | project-pinned | Test runner | Project standard (`pytest -x -q`) |

No new dependencies required for this phase.

**Installation:** none needed.

## Architecture Patterns

### Recommended Change Location

```
src/gridflow/cli.py
└── _resolve_datasets()   <- single-line condition change here
```

```
tests/unit/
└── test_cli_resolve_datasets.py   <- new file: unit tests for the helper
```

### Pattern 1: Positional Alias in `_resolve_datasets`

**What:** Treat `dataset == "all"` (case-insensitive) the same as `all_flag=True`.

**When to use:** User passes `all` as the positional dataset argument on any command.

**Current code (lines 620-626 of cli.py):** [VERIFIED: codebase read]
```python
def _resolve_datasets(
    source: str,
    dataset: str | None,
    all_flag: bool,
    settings: object,
) -> list[str]:
    from gridflow.config.settings import GridflowConfig
    if not isinstance(settings, GridflowConfig):
        raise TypeError("Expected GridflowConfig")

    if all_flag:
        source_config = settings.get_source_config(source)
        return list(source_config.datasets.keys())
    if dataset:
        return [dataset]
    raise typer.BadParameter("Specify a dataset name or use --all")
```

**Fixed version:**
```python
    if all_flag or (dataset is not None and dataset.lower() == "all"):
        source_config = settings.get_source_config(source)
        return list(source_config.datasets.keys())
    if dataset:
        return [dataset]
    raise typer.BadParameter("Specify a dataset name or use --all")
```

**Source:** [VERIFIED: cli.py lines 609-626, direct codebase read]

### Pattern 2: Testing `_resolve_datasets` Directly

The function is a pure function (aside from the `isinstance` check and the
`settings.get_source_config()` call). It can be tested by constructing a minimal
`GridflowConfig` object — the project's `conftest.py` already provides a
`sample_config` fixture that shows the exact construction pattern.

**Note on the `sample_config` fixture:** It has only one Elexon dataset
(`system_prices`). The alias test still proves the logic, but the assertion is over a
single-element list. Consider adding a two-dataset fixture in Wave 0 to give stronger
evidence that `all` truly expands multiple datasets (not just the one).

**Example unit test shape:** [VERIFIED: conftest.py + cli.py codebase read]
```python
from __future__ import annotations

import pytest

from gridflow.cli import _resolve_datasets


def test_all_positional_treated_as_flag(sample_config):
    result = _resolve_datasets("elexon", "all", False, sample_config)
    assert result == list(sample_config.get_source_config("elexon").datasets.keys())


def test_all_positional_case_insensitive(sample_config):
    result = _resolve_datasets("elexon", "ALL", False, sample_config)
    assert result == list(sample_config.get_source_config("elexon").datasets.keys())


def test_all_flag_still_works(sample_config):
    result = _resolve_datasets("elexon", None, True, sample_config)
    assert result == list(sample_config.get_source_config("elexon").datasets.keys())


def test_specific_dataset_unchanged(sample_config):
    result = _resolve_datasets("elexon", "system_prices", False, sample_config)
    assert result == ["system_prices"]


def test_no_dataset_raises(sample_config):
    # _resolve_datasets is called as a plain function — typer.BadParameter propagates
    # directly as an exception (no Click context to convert it to sys.exit).
    import typer
    with pytest.raises(typer.BadParameter):
        _resolve_datasets("elexon", None, False, sample_config)
```

### Pattern 3: Typer `CliRunner` Smoke Test (optional)

`typer.testing.CliRunner` lets you invoke the full CLI without subprocess overhead.
However, for this phase the `ingest` and `transform` commands make network calls and
require DuckDB init, so a direct unit test of `_resolve_datasets` is sufficient and
preferred. A `CliRunner` smoke test is optional, not required.

### Anti-Patterns to Avoid

- **Patching per-command:** Do not add `if dataset == "all"` inside each of `ingest`,
  `transform`, `pipeline`, `backfill` separately. The helper centralises this logic;
  fix it once there.
- **Mutating the `dataset` parameter before calling `_resolve_datasets`:** Doing
  `dataset = None if dataset == "all" else dataset` + `all_flag = True` in each
  command body would work but duplicates logic four times.
- **Forgetting `backfill`:** `backfill` calls `_resolve_datasets` at line 208; since
  the fix is in the helper, backfill is covered automatically.
- **`pipeline` passes `dataset` through to `ingest`/`transform`:** When `pipeline`
  resolves datasets, it calls `ingest(dataset=dataset, ...)` and
  `transform(dataset=dataset, ...)` with the original string `"all"`. Each of those
  functions then calls `_resolve_datasets` again with `dataset="all"` and
  `all_datasets=False`. The fix in `_resolve_datasets` handles this correctly —
  no special handling needed in `pipeline`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead |
|---------|-------------|-------------|
| CLI invocation in tests | Subprocess + shell | `typer.testing.CliRunner` |
| Config construction in tests | Hardcoded dicts | `sample_config` fixture from conftest.py |

## Common Pitfalls

### Pitfall 1: `pipeline` Double-Resolves Datasets

**What goes wrong:** `pipeline` calls `_resolve_datasets` to get a list, but then passes
the original `dataset` string (not the resolved list) to `ingest` and `transform`. Each
sub-call resolves again.

**Why it happens:** The `pipeline` command passes its raw arguments through to
`ingest`/`transform` rather than the resolved list. This is intentional — each
sub-command is responsible for its own resolution.

**How to avoid:** The fix in `_resolve_datasets` handles both the top-level call in
`pipeline` and the delegated calls to `ingest`/`transform`. No change needed in the
`pipeline` command body. Verify by tracing the call path: `pipeline` ->
`_resolve_datasets("entsoe", "all", False, ...)` -> returns all datasets (after fix);
then `ingest(dataset="all", all_datasets=False, ...)` ->
`_resolve_datasets("entsoe", "all", False, ...)` -> also returns all datasets.
Both resolve correctly.

**Warning signs:** If only `pipeline` works but `ingest entsoe all` still fails,
the fix is not in the helper.

### Pitfall 2: Case Sensitivity

**What goes wrong:** User types `ALL` or `All`; the alias fails.

**Why it happens:** String comparison is case-sensitive by default.

**How to avoid:** Use `dataset.lower() == "all"` in the condition.

### Pitfall 3: `typer.BadParameter` Exception Scope in Tests

**What goes wrong:** Test author uses `pytest.raises(SystemExit)` for the error path,
but the test fails because `typer.BadParameter` propagates as-is.

**Why it happens:** `typer.BadParameter` (which is `click.UsageError`) is only
converted to `SystemExit(2)` when Click's command runner is in the call stack. When
`_resolve_datasets` is called directly as a plain function (no Click context), the
exception propagates normally.

**How to avoid:**
- Direct helper tests: `pytest.raises(typer.BadParameter)` — exception propagates normally.
- CliRunner tests: check `result.exit_code == 2` — Click swallows the exception.

**Warning signs:** Test catches `Exception` but not `typer.BadParameter` specifically —
indicates the wrong exception type was expected.

## Code Examples

### Minimal `GridflowConfig` for Unit Tests

The existing `sample_config` fixture in `tests/conftest.py` already constructs a
`GridflowConfig` with one Elexon dataset (`system_prices`). The `_resolve_datasets`
unit test file can reuse this fixture via standard pytest fixture injection.
[VERIFIED: tests/conftest.py lines 29-57]

## State of the Art

| Old Approach | Current Approach | Impact |
|--------------|------------------|--------|
| Positional `all` raises `BadParameter` | Positional `all` treated as `--all` | Fixes UX confusion documented in STATE.md |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `export_csv` uses `_resolve_datasets` and would get the fix for free | Summary | None — `export_csv` is outside CLI-01/CLI-02 scope; fix is correct regardless |

**Risk assessment:** A1 is verified by reading cli.py line 267. The fix applies to all
callers of `_resolve_datasets` regardless.

## Open Questions

None. The fix is fully determined by reading cli.py.

## Environment Availability

Step 2.6: SKIPPED (no external dependencies — code-only change).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (project standard) |
| Config file | none detected (use pytest directly) |
| Quick run command | `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` |
| Full suite command | `uv run pytest -x -q` |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CLI-01 | `gridflow pipeline entsoe all --last 24h` routes to all datasets | unit | `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` | Wave 0 |
| CLI-02 | Same alias on `ingest` and `transform` subcommands | unit | `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` | Wave 0 |

### Sampling Rate
- **Per task commit:** `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q`
- **Per wave merge:** `uv run pytest -x -q`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/unit/test_cli_resolve_datasets.py` — covers CLI-01, CLI-02

## Security Domain

Not applicable — this change adds no authentication, session management, external I/O,
or data validation logic. It is a pure argument normalisation fix in a local CLI helper.

## Sources

### Primary (HIGH confidence)
- `src/gridflow/cli.py` lines 609-626 — `_resolve_datasets` implementation, all callers identified [VERIFIED: codebase read]
- `tests/conftest.py` lines 29-57 — `sample_config` fixture pattern [VERIFIED: codebase read]
- `.planning/REQUIREMENTS.md` — CLI-01, CLI-02 requirement text [VERIFIED: codebase read]
- `.planning/STATE.md` — Decision: treat `all` positional as `--all` [VERIFIED: codebase read]

### Secondary (MEDIUM confidence)
- typer docs: `typer.testing.CliRunner` for CLI test isolation [ASSUMED]
- click/typer exception handling: `BadParameter` propagates directly when no Click context is present [ASSUMED — easily verified by running `pytest.raises(typer.BadParameter)` in Wave 0]

## Metadata

**Confidence breakdown:**
- Fix location: HIGH — confirmed by direct codebase read
- Fix approach: HIGH — single condition change in a well-isolated helper
- Test approach: HIGH — project test patterns confirmed from conftest.py
- `BadParameter` exception scope: MEDIUM — training knowledge; verify in Wave 0 by running the test

**Research date:** 2026-05-02
**Valid until:** Until cli.py is refactored (stable)
