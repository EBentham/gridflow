# ADR-001 — Project root resolution via git-common-dir

**Status:** Accepted
**Date:** 2026-05-10
**Phase:** Companion to gridflow_models F11 / WBH-04

## Context

Running gridflow CLI commands from a Jupyter kernel whose CWD is
`notebooks/` caused `data_dir` (default `Path("./data")`) to resolve to
`notebooks/data/`, not `<gridflow_root>/data/`. This silently bifurcated
the user's data store: `gridflow init` would write the bronze parquet
tree to one location, the next `gridflow run` (from a different CWD)
would read a different empty tree, and the user would observe
`rows_in: 0, rows_out: 0` runs with no actionable error. This was the
dominant first-run friction during the live walkthrough on 2026-05-10
(F10-UAT, gridflow_models F11 §I-03).

The analogous bug in the gridflow_models companion package was fixed
under D-WORKTREE-PATH (commit `3e9b7e8`) using a `_project_root()`
helper plus a `@model_validator(mode="after")` pattern. The pattern is:

1. `@cache` a function that resolves `Path` → the parent of
   `git rev-parse --git-common-dir`. This is **worktree-safe** because
   `--git-common-dir` returns the canonical `.git` directory regardless
   of whether the process runs from the main checkout or a linked
   worktree under `.claude/worktrees/`.
2. Walk-up-for-`pyproject.toml` if `git` fails (sdist install, CI
   minimal container).
3. `Path.cwd()` only as last resort.
4. A pydantic `@model_validator(mode="after")` that anchors any
   relative path field on `_project_root()` rather than CWD.

This ADR ports that pattern verbatim into gridflow.

## Decision

Add `_project_root()` and `PipelineSettings._resolve_paths` to
`src/gridflow/config/settings.py`. The validator anchors `data_dir`,
`duckdb_path`, and `log_dir` against `_project_root()` when the field
value is relative; absolute paths pass through unchanged.

Implementation (verbatim port from
`gridflow_models/src/gridflow_models/config/settings.py:25-103`):

```python
@cache
def _project_root() -> Path:
    here = Path(__file__).resolve().parent
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            cwd=here,
            capture_output=True,
            text=True,
            check=True,
            timeout=2,
        )
        return Path(result.stdout.strip()).parent
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        for parent in [here, *here.parents]:
            if (parent / "pyproject.toml").exists():
                return parent
        return Path.cwd()
```

```python
@model_validator(mode="after")
def _resolve_paths(self) -> PipelineSettings:
    if not self.data_dir.is_absolute():
        object.__setattr__(self, "data_dir", (_project_root() / self.data_dir).resolve())
    if not self.duckdb_path.is_absolute():
        object.__setattr__(self, "duckdb_path", (_project_root() / self.duckdb_path).resolve())
    if not self.log_dir.is_absolute():
        object.__setattr__(self, "log_dir", (_project_root() / self.log_dir).resolve())
    return self
```

## Consequences

**Accepted:**

- **Worktree-safe.** `git rev-parse --git-common-dir` returns the canonical
  `.git` even from a linked worktree, so the project root resolves to the
  main checkout regardless of whether the process runs from the canonical
  tree or a `.claude/worktrees/<branch>/` linked worktree.
- **CWD-independent.** Running `gridflow init`, `gridflow run`, or any
  other CLI command from any subdirectory still writes to
  `<gridflow_root>/data/`. The "wrote to notebooks/data/" silent-bug
  class is eliminated.
- **Backward-compatible.** Absolute paths in `data_dir` /
  `GRIDFLOW_DATA_DIR` etc pass through unchanged — the validator only
  rewrites relative paths via `is_absolute()`.
- **Tested.** Four integration tests in
  `tests/integration/test_data_dir_resolution.py` cover: project_root
  sanity (lands on a directory containing pyproject.toml), foreign-CWD
  resolution (relative paths anchor on project root, not the foreign CWD),
  absolute-path pass-through, and end-to-end `load_settings()` with a
  patched config dir.

**Tradeoff:**

- One subprocess (`git rev-parse`) per process startup, cached via
  `@cache`. The 2-second `timeout=` parameter on `subprocess.run` plus
  `try/except (SubprocessError, FileNotFoundError, OSError)` handles the
  git-unavailable case gracefully (CI minimal containers, sdist installs)
  by falling through to the pyproject-walk-up.

## Alternatives considered

- **`Path("./data")` (status quo).** Rejected: broken under non-root
  CWD. This is the bug being fixed.
- **`Path(__file__).parent.parent.parent.parent` (walk-up only).**
  Rejected: brittle to package restructuring, doesn't gracefully handle
  worktrees with the same robustness as `--git-common-dir`, and
  `_find_config_dir` already uses this exact technique with a documented
  failure mode.
- **Environment-variable-only.** Rejected: user-hostile — requires manual
  `GRIDFLOW_DATA_DIR=/abs/path` setup per kernel, per shell, per test
  invocation. The validator runs even when no env var is set.
- **Search-up by sentinel file other than `pyproject.toml`** (e.g. a
  `.gridflow-root` marker). Rejected: extra concept without benefit;
  pyproject.toml is the canonical project marker for Python tooling.

## References

- Source pattern: gridflow_models `src/gridflow_models/config/settings.py`
  (lines 25-103) — D-WORKTREE-PATH, commit `3e9b7e8`.
- gridflow_models F11-G plan
  (`.planning/phases/F11-workbench-hardening/F11-G-gridflow-project-root-port-PLAN.md`).
- gridflow_models F11-RESEARCH.md §15.1 (verbatim port pattern).
- gridflow_models F11-CONTEXT.md §I-03 (the bug walkthrough).
- gridflow_models F10-UAT.md (origin of D-WORKTREE-PATH).
- This commit's tests:
  `tests/integration/test_data_dir_resolution.py`.
