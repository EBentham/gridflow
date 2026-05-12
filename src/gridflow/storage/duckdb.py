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


def get_connection(
    db_path: Path | str,
    read_only: bool = False,
    retries: int = 8,
    base_delay: float = 1.0,
) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating the file if necessary.

    Retries on transient file-lock errors (e.g. cloud sync tools such as
    Google Drive File Stream holding the file between chunks).
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return duckdb.connect(str(db_path), read_only=read_only)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = base_delay * (2 ** attempt)
            logger.warning(
                "DuckDB connection attempt %d/%d failed (%s); retrying in %.1fs",
                attempt + 1,
                retries,
                exc,
                wait,
            )
            time.sleep(wait)
    raise RuntimeError(
        f"Could not open DuckDB at {db_path} after {retries} attempts"
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


def _try_create_view(
    con: duckdb.DuckDBPyConnection, view_name: str, pattern: str
) -> None:
    """Create a view if the Parquet files exist."""
    try:
        con.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true, union_by_name=true)"
        )
        logger.info(f"Registered view: {view_name}")
    except Exception as e:
        if _is_strict_mode():
            raise
        logger.debug(f"Could not create view {view_name}: {e}")


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
