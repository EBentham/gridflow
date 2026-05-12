# ADR-002 — GridflowClient.reopen_readonly() public method

**Status:** Accepted
**Date:** 2026-05-10
**Phase:** Companion to gridflow_models F11 / WBH-02 / WBH-04

## Context

gridflow_models hit a DuckDB connection conflict
(`Can't open a connection to same database file with a different
configuration than existing connections`) when `setup_notebook()`
(read-only handle on `gridflow.duckdb`) and
`control.refresh.run_pipeline()` (read-write handle on the same file)
coexisted in the same Jupyter kernel. The pipeline cell hung in a
tenacity retry loop. See gridflow_models F11-CONTEXT.md §I-01.

Per gridflow_models D-F11-02 (locked decision in F11-CONTEXT.md), the
broker design is **eager-rebind wrapped behind a context manager**
(Shape A wrapped in Shape C from gridflow_models F11-RESEARCH.md §3).
gridflow_models maintains a `weakref.WeakSet[GridflowClient]` in
`gridflow_models.control.refresh`; `writeable_pipeline_session(db_path)`
is the single context manager every write-mode entry point opens before
touching the file. Inside the context, every read-only handle on the
target db_path is closed; on exit, every handle that was closed is
reopened read-only.

Without a public reopen hook on `GridflowClient`, the broker would have
to mutate `client._con` and read `client._db_path` directly — a
package-boundary violation that makes gridflow's private attributes a
de-facto-public API surface.

## Decision

Add two public methods to `GridflowClient`:

1. `close() -> None` (refactored to be **idempotent**). Safe to call
   repeatedly; second and subsequent calls are no-ops. Sets `self._con`
   to `None` after closing so the closed state is observable.
2. `reopen_readonly() -> None` (**new**). Closes any existing
   connection, then opens a fresh read-only handle on the same
   `_db_path`. Idempotent: each invocation produces a working
   read-only handle regardless of prior state.

Both methods are pure no-data-touching state transitions on the
client's own internal connection — they do not affect any other
GridflowClient instance, do not write to disk, and do not change the
read-only contract of the class.

Implementation (in `src/gridflow/serving/client.py`):

```python
def close(self) -> None:
    """Close the underlying DuckDB connection. Idempotent."""
    if self._con is not None:
        self._con.close()
        self._con = None

def reopen_readonly(self) -> None:
    """Reopen the read-only DuckDB handle on the same db_path."""
    if self._con is not None:
        self._con.close()
    self._con = duckdb.connect(str(self._db_path), read_only=True)
```

The annotation of `_con` widens to
`duckdb.DuckDBPyConnection | None` to reflect the closed state. A
private `_require_con()` helper at every read-path call site
(`query()`, `get_tables()`) raises a clear `RuntimeError("connection
is closed; call reopen_readonly() before issuing queries")` instead of
the bare `AttributeError: 'NoneType' object has no attribute 'sql'`
the mypy-naive code would otherwise produce.

## Consequences

**Accepted:**

- The gridflow_models DuckDB broker (F11-D
  `writeable_pipeline_session`) calls `client.close()` before the
  write phase and `client.reopen_readonly()` after, with no access
  to private attributes. The package boundary stays clean.
- The user's bound `client` variable in the notebook continues to work
  after `run_pipeline()` returns — the same instance is reopened, so
  any subsequent `client.get_system_prices(...)` succeeds.
- The pattern is reusable: any future "exclusive write phase"
  operation in gridflow_models (or any other downstream consumer) can
  use the same context manager without mutating private state.
- The existing context-manager protocol (`with GridflowClient(...) as
  c:`) keeps working — `__exit__` calls `close()`, which is now
  idempotent rather than throwing if the connection was already
  closed manually.
- Issuing a query after `close()` raises a one-line actionable
  `RuntimeError` rather than an opaque `AttributeError`.

**Tradeoff:**

- `_con` annotation widens from `DuckDBPyConnection` to
  `DuckDBPyConnection | None`. Existing call sites that assume `_con
  is not None` continue to work because the only `None`-state is after
  explicit `close()` — `__init__` always opens a connection. The
  `_require_con()` helper makes the assumption explicit at every
  read-path site, satisfying mypy `--strict`.
- One extra round-trip for the broker (close + reopen) per write
  phase. Acceptable: the write phase is the slow operation by orders
  of magnitude (parquet write, duckdb compaction).

## Alternatives considered

- **Lazy-client wrapper** (Shape B in gridflow_models F11-RESEARCH.md
  §3). Rejected: would require gridflow_models to wrap GridflowClient
  in a proxy object, breaking the F10 4-handle contract that
  `setup_notebook()["client"]` is a real `GridflowClient` instance
  with all its public methods directly accessible.
- **Broker reaches into private attributes** (`client._con`,
  `client._db_path`). Rejected as a layering violation. Once the
  broker depends on the private surface, every gridflow refactor of
  the client risks silently breaking gridflow_models.
- **Make `_con` itself a `@property` with auto-reopen on access.**
  Rejected: hides the close-then-reopen lifecycle from the broker,
  which actively wants to see "closed" as a temporary state. Implicit
  reopen on access also makes the conflict-with-write-handle window
  unobservable.
- **Add a separate `reopen()` method that takes a `read_only=True`
  argument.** Rejected for v1: the broker's only use case is
  read-only re-open. Adding the parameter now would invite future
  call sites that re-open with `read_only=False`, defeating the
  purpose of the read-only client. If a write-capable client is ever
  needed, a separate class is the right home (it has a different
  failure-mode contract).

## References

- gridflow_models F11-RESEARCH.md §3 (broker shape rationale).
- gridflow_models F11-CONTEXT.md "Locked decisions" §D-F11-02.
- gridflow_models F11-G plan
  (`.planning/phases/F11-workbench-hardening/F11-G-gridflow-project-root-port-PLAN.md`).
- gridflow_models F11-D plan (the consumer of this method, via
  `writeable_pipeline_session`).
- This commit's tests:
  `tests/unit/serving/test_client_reopen_readonly.py` (7 unit tests).
