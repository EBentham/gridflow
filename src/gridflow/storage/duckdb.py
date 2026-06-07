"""DuckDB catalogue management and view registration."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def _is_strict_mode() -> bool:
    """True when broken view registration should raise instead of debug-log.

    F15-D / PBI-05: gates _try_create_view and _register_gold_views exception
    handling. Activated automatically by pytest (PYTEST_CURRENT_TEST) or
    explicitly via GRIDFLOW_ENV=dev/test.
    """
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return True
    return os.environ.get("GRIDFLOW_ENV", "").strip().lower() in {"dev", "test"}


def _is_transient_lock_error(exc: Exception) -> bool:
    """True only for transient file-lock/contention errors worth retrying.

    The documented motivation for retrying is a cloud sync tool (Google Drive
    File Stream) holding the file between chunks, which surfaces as a DuckDB
    lock/IO contention error. A genuinely broken or missing path (e.g. a
    read_only open of a non-existent DB) is non-transient and must fail fast
    rather than sleep through the full exponential backoff (~255s) before
    surfacing the real cause.
    """
    if not isinstance(exc, duckdb.IOException):
        return False
    msg = str(exc).lower()
    lock_markers = (
        "lock",  # "Conflicting lock", "Could not set lock"
        "being used by another",
        "resource temporarily unavailable",
    )
    return any(marker in msg for marker in lock_markers)


def get_connection(
    db_path: Path | str,
    read_only: bool = False,
    retries: int = 8,
    base_delay: float = 1.0,
) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating the file if necessary.

    Retries ONLY on transient file-lock errors (e.g. cloud sync tools such as
    Google Drive File Stream holding the file between chunks). A non-transient
    error (broken path, missing read-only DB file) is re-raised immediately so
    the real DuckDB cause surfaces in well under the full backoff.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return duckdb.connect(str(db_path), read_only=read_only)
        except Exception as exc:  # noqa: BLE001
            if not _is_transient_lock_error(exc):
                # Non-transient: do not blind-retry. Surface the real cause now.
                raise
            last_exc = exc
            wait = base_delay * (2**attempt)
            logger.warning(
                "DuckDB connection attempt %d/%d failed on a lock (%s); retrying in %.1fs",
                attempt + 1,
                retries,
                exc,
                wait,
            )
            time.sleep(wait)
    raise RuntimeError(
        f"Could not open DuckDB at {db_path} after {retries} lock-retry attempts"
    ) from last_exc


def init_catalogue(db_path: Path, data_dir: Path) -> None:
    """Initialise the DuckDB catalogue with views and metadata tables.

    Creates views pointing to Parquet files and metadata tables for
    pipeline tracking.
    """
    con = get_connection(db_path)

    # Create metadata tables
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          VARCHAR PRIMARY KEY,
            source          VARCHAR NOT NULL,
            dataset         VARCHAR NOT NULL,
            operation       VARCHAR NOT NULL,
            started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
            completed_at    TIMESTAMP WITH TIME ZONE,
            status          VARCHAR NOT NULL,
            rows_in         INTEGER DEFAULT 0,
            rows_out        INTEGER DEFAULT 0,
            rows_skipped    INTEGER DEFAULT 0,
            duration_seconds FLOAT,
            error_message   VARCHAR,
            parameters      VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_watermarks (
            source      VARCHAR NOT NULL,
            dataset     VARCHAR NOT NULL,
            last_end    TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL,
            PRIMARY KEY (source, dataset)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS quality_reports (
            run_id          VARCHAR,
            id              INTEGER,
            run_date        TIMESTAMP WITH TIME ZONE,
            check_name      VARCHAR,
            dataset         VARCHAR,
            source          VARCHAR,
            passed          BOOLEAN,
            metric          DOUBLE,
            detail          VARCHAR
        )
    """)
    # Reconcile a legacy quality_reports created before run_id existed:
    # CREATE TABLE IF NOT EXISTS no-ops on an old 8-column table, which would
    # then break the reporter's explicit-column run_id INSERT. Idempotent.
    con.execute("ALTER TABLE quality_reports ADD COLUMN IF NOT EXISTS run_id VARCHAR")

    # Register views for silver and gold Parquet files
    _register_views(con, data_dir)

    # Register SQL-defined gold cross-source views
    _register_gold_views(con)

    con.close()
    logger.info(f"DuckDB catalogue initialised at {db_path}")


def _register_views(con: duckdb.DuckDBPyConnection, data_dir: Path) -> None:
    """Register DuckDB views pointing to Parquet files on disk."""
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"

    # Silver views — source-qualified (C1-4): silver_{source}_{dataset}. Two
    # sources sharing a dataset directory name (e.g. a future bare ``forecast``)
    # would otherwise collapse to one view under CREATE OR REPLACE in
    # nondeterministic iterdir() order, silently shadowing one source's data.
    if silver_dir.exists():
        # Track which source(s) own each dataset NAME so the alias pass can tell
        # single-source names (safe to alias) from collision names (must NOT).
        dataset_sources: dict[str, set[str]] = {}
        for source_dir in silver_dir.iterdir():
            if not source_dir.is_dir():
                continue
            for dataset_dir in source_dir.iterdir():
                if not dataset_dir.is_dir():
                    continue
                dataset_sources.setdefault(dataset_dir.name, set()).add(source_dir.name)
                view_name = f"silver_{source_dir.name}_{dataset_dir.name}"
                pattern = str(dataset_dir / "**" / "*.parquet").replace("\\", "/")
                _try_create_view(con, view_name, pattern)

        _register_silver_aliases(con, dataset_sources)

    # Gold views
    if gold_dir.exists():
        for dataset_dir in gold_dir.iterdir():
            if not dataset_dir.is_dir():
                continue
            view_name = f"gold_{dataset_dir.name}"
            pattern = str(dataset_dir / "**" / "*.parquet").replace("\\", "/")
            _try_create_view(con, view_name, pattern)


def _register_silver_aliases(
    con: duckdb.DuckDBPyConnection, dataset_sources: dict[str, set[str]]
) -> None:
    """Register DEPRECATED backward-compat aliases for renamed silver views (C1-4).

    The silver views were renamed to the source-qualified scheme
    ``silver_{source}_{dataset}`` (C1-4 / CH-ARCH-03). Each alias registered here
    is a deprecation shim ``silver_{dataset} -> silver_{source}_{dataset}`` so an
    out-of-repo caller (e.g. ``gridflow_models``) querying the OLD single-token
    name survives the rename. These shims are DEPRECATED: new code must use the
    qualified name, and the aliases may be dropped in a future cleanup.

    An alias is auto-generated for every dataset NAME owned by exactly ONE source
    (looked up in ``dataset_sources``). A name owned by MORE than one source is a
    collision case — exactly what the source-qualified scheme exists to
    disambiguate — so it is SKIPPED (logged at DEBUG): a single-token alias would
    arbitrarily shadow one source's data and reintroduce the foot-gun the rename
    removed.

    An alias is only created when its qualified target view actually exists in
    the live catalogue: tests (and partial pipelines) init over a tmpdir holding
    only a subset of datasets, so a dataset dir whose parquet has not been
    written yet has no qualified view, and aliasing an absent target raises a
    DuckDB binder error — which under pytest / GRIDFLOW_ENV strict mode (F15-D)
    would propagate and break catalogue init. Gating on the live catalogue keeps
    the shim loud-failure-free.

    Args:
        con: Open DuckDB connection whose silver views have already been
            registered by the caller.
        dataset_sources: Map of each silver dataset NAME to the set of source
            names that own a ``silver/<source>/<dataset>`` directory, built by
            :func:`_register_views` while registering the qualified views.
    """
    existing = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    for dataset, sources in dataset_sources.items():
        if len(sources) > 1:
            # Collision name: a single-token alias would silently shadow one
            # source. The qualified ``silver_{source}_{dataset}`` views remain
            # the only way to reach these — by design (C1-4).
            logger.debug(
                "Skipping ambiguous silver alias silver_%s: owned by %d sources (%s)",
                dataset,
                len(sources),
                ", ".join(sorted(sources)),
            )
            continue
        (source,) = tuple(sources)
        alias = f"silver_{dataset}"
        target = f"silver_{source}_{dataset}"
        if target not in existing:
            continue
        if alias in existing:
            # The single-token alias collides with an already-registered
            # qualified view (only possible if a future dataset name equals
            # ``{source}_{dataset2}``). Never CREATE OR REPLACE over it — that is
            # the silent-shadow this whole mechanism exists to prevent (C1-4).
            logger.debug("Skipping silver alias %s: name already taken by a qualified view", alias)
            continue
        con.execute(
            f"CREATE OR REPLACE VIEW {_quote_identifier(alias)} "
            f"AS SELECT * FROM {_quote_identifier(target)}"
        )
        # WHY logged: these single-token aliases are deprecation shims; a future
        # cleanup can drop them once downstream migrates to the qualified name.
        logger.info("Registered DEPRECATED backward-compat silver alias: %s -> %s", alias, target)


def _quote_identifier(name: str) -> str:
    """Quote a DuckDB identifier, doubling any embedded double-quote.

    View names are filesystem-derived (dataset directory names) and cannot be
    bound as SQL parameters — DDL identifiers are not parameterisable. Quoting
    (rather than raw interpolation) makes a space/hyphen/quote in a directory
    name safe and closes the local injection surface.
    """
    return '"' + name.replace('"', '""') + '"'


def _quote_string_literal(value: str) -> str:
    """Quote a DuckDB string literal, doubling any embedded single-quote.

    The parquet glob is interpolated into a string literal; an apostrophe in a
    user path (e.g. C:/Users/O'Brien/...) would otherwise close the literal
    early and produce malformed DDL.
    """
    return "'" + value.replace("'", "''") + "'"


def _is_benign_absent_parquet(exc: Exception) -> bool:
    """True for the deliberate F15-D swallow case: parquet not yet written.

    DuckDB raises an IOException whose message says no files matched the glob.
    That is benign (the directory exists but is empty / not yet populated) and
    is swallowed in production. A binder/parser/catalog error is a deterministic
    DDL bug and must NOT be treated as benign.
    """
    if not isinstance(exc, duckdb.IOException):
        return False
    msg = str(exc).lower()
    return "no files found" in msg or "no files that match" in msg


def _try_create_view(con: duckdb.DuckDBPyConnection, view_name: str, pattern: str) -> None:
    """Create a view over the Parquet files, if any have been written.

    The view name is quoted as an identifier and the glob is escaped as a
    string literal (DDL cannot bind parameters). A genuinely-malformed DDL or
    binder failure is surfaced loudly (raise under strict mode, WARNING+ in
    production); only the benign "parquet not yet written" case is swallowed at
    DEBUG, preserving the F15-D / PBI-05 production-swallow contract.
    """
    try:
        con.execute(
            f"CREATE OR REPLACE VIEW {_quote_identifier(view_name)} AS "
            f"SELECT * FROM read_parquet({_quote_string_literal(pattern)}, "
            f"hive_partitioning=true, union_by_name=true)"
        )
        logger.info(f"Registered view: {view_name}")
    except Exception as e:
        if _is_strict_mode():
            raise
        if _is_benign_absent_parquet(e):
            logger.debug(f"Could not create view {view_name} (no parquet yet): {e}")
        else:
            # Deterministic DDL/binder error — make the silent-corruption case
            # visible rather than letting a later SELECT fail opaquely.
            logger.warning(
                "View registration failed for %s (not absent-data): %s",
                view_name,
                e,
            )


def _register_gold_views(con: duckdb.DuckDBPyConnection) -> None:
    """Execute SQL files from the gold/views directory to create cross-source views."""
    views_dir = Path(__file__).parent.parent / "gold" / "views"
    if not views_dir.exists():
        return

    for sql_file in sorted(views_dir.glob("*.sql")):
        try:
            sql = sql_file.read_text()
            con.execute(sql)
            logger.info("Registered gold view from: %s", sql_file.name)
        except Exception as exc:
            if _is_strict_mode():
                raise
            logger.debug("Could not register gold view %s: %s", sql_file.name, exc)


def refresh_views(db_path: Path, data_dir: Path) -> None:
    """Re-register all views from the current filesystem state."""
    con = get_connection(db_path)
    _register_views(con, data_dir)
    _register_gold_views(con)
    con.close()
