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
            last_end    TIMESTAMP WITH TIME ZONE NOT NULL,
            updated_at  TIMESTAMP WITH TIME ZONE NOT NULL,
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

    # Silver views
    if silver_dir.exists():
        for source_dir in silver_dir.iterdir():
            if not source_dir.is_dir():
                continue
            for dataset_dir in source_dir.iterdir():
                if not dataset_dir.is_dir():
                    continue
                view_name = f"silver_{dataset_dir.name}"
                pattern = str(dataset_dir / "**" / "*.parquet").replace("\\", "/")
                _try_create_view(con, view_name, pattern)

    # Gold views
    if gold_dir.exists():
        for dataset_dir in gold_dir.iterdir():
            if not dataset_dir.is_dir():
                continue
            view_name = f"gold_{dataset_dir.name}"
            pattern = str(dataset_dir / "**" / "*.parquet").replace("\\", "/")
            _try_create_view(con, view_name, pattern)


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
