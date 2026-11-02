"""DuckDB catalogue management and view registration."""

from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from gridflow.silver.latest_views import LATEST_VIEW_SPECS, latest_view_sql
from gridflow.storage.parquet import sweep_orphan_temp_files
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Sequence

    from gridflow.silver.latest_views import LatestViewSpec
    from gridflow.silver.owned_relations import RegisteredRelationsTransformer

logger = logging.getLogger(__name__)
_SAFE_PARQUET_GLOB = "**/[!.]*.parquet"
# Views whose Parquet exists but would not register in the current
# _register_views pass, so a gold view missing one is broken, not waiting.
_failed_over_parquet: set[str] = set()


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
    paths = PathBuilder(data_dir)
    silver_root = paths.silver_root()
    gold_root = paths.gold_root()
    sweep_orphan_temp_files(silver_root, gold_root)
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
    _register_views(con, silver_root, gold_root)

    # Register SQL-defined gold cross-source views
    _register_gold_views(con)

    con.close()
    logger.info(f"DuckDB catalogue initialised at {db_path}")


def _register_views(con: duckdb.DuckDBPyConnection, silver_root: Path, gold_root: Path) -> None:
    """Register DuckDB views pointing to Parquet files on disk."""
    # ADR-034 P-11: generated families register through the silver bootstrap,
    # which `gridflow init` would otherwise never import (E9).
    from gridflow.silver.schema_manifest import _ensure_silver_transformers_registered

    _failed_over_parquet.clear()
    _ensure_silver_transformers_registered()
    owners = _registered_relation_owners()

    # Silver views — source-qualified (C1-4): silver_{source}_{dataset}. Two
    # sources sharing a dataset directory name (e.g. a future bare ``forecast``)
    # would otherwise collapse to one view under CREATE OR REPLACE in
    # nondeterministic iterdir() order, silently shadowing one source's data.
    latest_bases: list[tuple[str, LatestViewSpec | None]] = []
    if silver_root.exists():
        for source_dir in silver_root.iterdir():
            if not source_dir.is_dir():
                continue
            for dataset_dir in source_dir.iterdir():
                if not dataset_dir.is_dir():
                    continue
                if (source_dir.name, dataset_dir.name) in owners:
                    # The owned-relations pass below registers this dataset.
                    continue
                view_name = f"silver_{source_dir.name}_{dataset_dir.name}"
                pattern = str(dataset_dir / _SAFE_PARQUET_GLOB).replace("\\", "/")
                spec = LATEST_VIEW_SPECS.get((source_dir.name, dataset_dir.name))
                if not _try_create_view(con, view_name, pattern):
                    # The failed CREATE OR REPLACE leaves an earlier init's view in
                    # place, bound to a glob that no longer matches (a pruned
                    # dataset keeps its directory); its _latest would then abort
                    # init on the dead glob.
                    for name in (f"{view_name}_latest", view_name):
                        con.execute(f"DROP VIEW IF EXISTS {_quote_identifier(name)}")
                elif spec is not None:
                    latest_bases.append((view_name, spec))

    if owners:
        latest_bases += _register_owned_relations(con, silver_root.parent, owners)

    _register_latest_views(con, latest_bases)

    _drop_retired_silver_aliases(con, silver_root)

    # Gold views
    if gold_root.exists():
        for dataset_dir in gold_root.iterdir():
            if not dataset_dir.is_dir():
                continue
            view_name = f"gold_{dataset_dir.name}"
            pattern = str(dataset_dir / _SAFE_PARQUET_GLOB).replace("\\", "/")
            _try_create_view(con, view_name, pattern)


def _registered_relation_owners() -> dict[tuple[str, str], type[RegisteredRelationsTransformer]]:
    """Every registered class whose relations exist by registration (P-11's set G)."""
    from gridflow.silver.owned_relations import RegisteredRelationsTransformer
    from gridflow.silver.registry import get_transformer_class, list_transformers

    out: dict[tuple[str, str], type[RegisteredRelationsTransformer]] = {}
    for key in list_transformers():
        cls = get_transformer_class(*key)
        if cls is not None and issubclass(cls, RegisteredRelationsTransformer):
            out[key] = cls
    return out


def _typed_empty_view(
    con: duckdb.DuckDBPyConnection, view_name: str, columns: Sequence[tuple[str, str]]
) -> None:
    """``CREATE OR REPLACE VIEW`` of zero rows with the given ``(name, type)`` columns.

    Names are record identifiers or engine constants, quoted; types come from
    a fixed map. No data value enters the DDL.
    """
    select = ", ".join(
        f"CAST(NULL AS {sql_type}) AS {_quote_identifier(name)}" for name, sql_type in columns
    )
    con.execute(
        f"CREATE OR REPLACE VIEW {_quote_identifier(view_name)} AS SELECT {select} WHERE FALSE"
    )
    logger.info("Registered typed-empty view: %s", view_name)


def _has_parquet(directory: Path) -> bool:
    """Whether ``directory`` holds at least one non-hidden Parquet file (E23)."""
    return directory.is_dir() and any(directory.rglob("[!.]*.parquet"))


def _register_view_or_empty(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    directory: Path,
    columns: Sequence[tuple[str, str]],
) -> None:
    # The file test exists only so _try_create_view never gets a glob that
    # matches nothing (E23); it never decides whether the relation exists.
    if _has_parquet(directory):
        _try_create_view(con, view_name, str(directory / _SAFE_PARQUET_GLOB).replace("\\", "/"))
    else:
        _typed_empty_view(con, view_name, columns)


def _register_owned_relations(
    con: duckdb.DuckDBPyConnection,
    data_dir: Path,
    owners: dict[tuple[str, str], type[RegisteredRelationsTransformer]],
) -> list[tuple[str, LatestViewSpec | None]]:
    """Register every owner's relations, unconditionally (ADR-034 I-1).

    Support relations first (a ``_latest`` may read them), once per name; then
    per owner its base view; :func:`_register_latest_views` adds the ``_latest``
    views afterwards. Each base or support view is
    over its Parquet when any exists, else typed-empty. Relation EXISTENCE has
    one input, registration; what the relations return is the selection over
    whatever silver and the ledger hold, and any disagreement between the two
    is reconcile's to report, never the catalogue's.

    Returns:
        Each owner's base view name with its ``_latest`` spec, ``None`` when it
        has none, in registration order.
    """
    paths = PathBuilder(data_dir)
    seen: set[str] = set()
    for key in sorted(owners):
        for support in owners[key].support_relations(data_dir):
            if support.name not in seen:
                seen.add(support.name)
                _register_view_or_empty(con, support.name, support.directory, support.columns)

    bases: list[tuple[str, LatestViewSpec | None]] = []
    for source, dataset in sorted(owners):
        cls = owners[(source, dataset)]
        view_name = f"silver_{source}_{dataset}"
        _register_view_or_empty(
            con, view_name, paths.silver_dir(source, dataset), cls.output_columns()
        )
        bases.append((view_name, LATEST_VIEW_SPECS.get((source, dataset))))
    return bases


def _register_latest_views(
    con: duckdb.DuckDBPyConnection, bases: Sequence[tuple[str, LatestViewSpec | None]]
) -> None:
    """Create or drop each base view's ``_latest``, in order, from one column snapshot.

    One unfiltered ``information_schema.columns`` read replaces a filtered read
    per view: each filtered read enumerated the whole catalogue, so init time
    grew with the view count. Every base and support view a ``_latest`` reads
    is registered before this runs. A base view that is absent, or has no spec,
    gets no ``_latest``.
    """
    columns: dict[str, set[str]] = {}
    for table_name, column_name in con.execute(
        "SELECT c.table_name, c.column_name FROM information_schema.columns c "
        "JOIN information_schema.views v "
        "ON v.table_schema = c.table_schema AND v.table_name = c.table_name "
        "WHERE c.table_schema = 'main'"
    ).fetchall():
        columns.setdefault(table_name, set()).add(column_name)
    for view_name, spec in bases:
        latest_name = f"{view_name}_latest"
        available = columns.get(view_name)
        sql = None
        if spec is not None and available is not None:
            # Order/rank terms adapt to the view's actual columns (live-feed
            # system_prices has no run_type; legacy files lack available_at).
            sql = latest_view_sql(view_name, latest_name, spec, available_columns=available)
        if sql is not None:
            con.execute(sql)
        else:
            # A previously-registered projection that can no longer be built
            # must not linger as a binder-error trap.
            con.execute(f"DROP VIEW IF EXISTS {_quote_identifier(latest_name)}")


def _drop_retired_silver_aliases(con: duckdb.DuckDBPyConnection, silver_root: Path) -> None:
    """Drop the single-token ``silver_{dataset}`` views earlier inits registered.

    They were deprecation shims for the source-qualified rename (C1-4) and are
    retired. ``CREATE OR REPLACE`` never removes a view, so a catalogue built
    before the retirement keeps them until this drops them. They were made from
    the silver dirs, so a dir with no registered transformer counts too. A name
    that is also a qualified view is never dropped.
    """
    from gridflow.silver.registry import list_transformers

    pairs = set(list_transformers())
    if silver_root.exists():
        pairs |= {
            (source_dir.name, dataset_dir.name)
            for source_dir in silver_root.iterdir()
            if source_dir.is_dir()
            for dataset_dir in source_dir.iterdir()
            if dataset_dir.is_dir()
        }
    qualified = {f"silver_{source}_{dataset}" for source, dataset in pairs}
    for name in sorted({f"silver_{dataset}" for _source, dataset in pairs} - qualified):
        con.execute(f"DROP VIEW IF EXISTS {_quote_identifier(name)}")


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


def _try_create_view(con: duckdb.DuckDBPyConnection, view_name: str, pattern: str) -> bool:
    """Create a view over the Parquet files, if any have been written.

    The view name is quoted as an identifier and the glob is escaped as a
    string literal (DDL cannot bind parameters). A genuinely-malformed DDL or
    binder failure is surfaced loudly (raise under strict mode, WARNING+ in
    production); only the benign "parquet not yet written" case is swallowed at
    DEBUG, preserving the F15-D / PBI-05 production-swallow contract.

    Returns:
        Whether the view was created; ``False`` only for a swallowed failure.
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
            _failed_over_parquet.add(view_name)
            logger.warning(
                "View registration failed for %s (not absent-data): %s",
                view_name,
                e,
            )
        return False
    return True


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
            awaited = _awaited_silver(con, exc)
            if awaited is not None:
                logger.info(
                    "Gold view %s registers after %s/%s is backfilled", sql_file.name, *awaited
                )
            else:
                logger.warning("Gold view registration failed for %s: %s", sql_file.name, exc)


_MISSING_TABLE_RE = re.compile(r'Table with name "?(\w+)"? does not exist')


def _awaited_silver(con: duckdb.DuckDBPyConnection, exc: Exception) -> tuple[str, str] | None:
    """The ``(source, dataset)`` a gold view failed for only because it has no silver yet.

    A fresh data root fails every gold view this way, which is expected, not a
    fault. A misspelt relation raises the same CatalogException, so the missing
    name must be a view a backfill registers (``silver_<source>_<dataset>`` or its
    ``_latest``), and that dataset's base view must be absent without having
    failed over existing Parquet: a ``_latest`` missing beside its base, or a
    base whose files would not read, is a fault, not a wait.
    """
    if not isinstance(exc, duckdb.CatalogException):
        return None
    match = _MISSING_TABLE_RE.search(str(exc))
    if match is None:
        return None
    from gridflow.silver.registry import list_transformers
    from gridflow.silver.schema_manifest import _ensure_silver_transformers_registered

    _ensure_silver_transformers_registered()
    inputs = {f"silver_{s}_{d}": (s, d) for s, d in list_transformers()}
    inputs |= {f"silver_{s}_{d}_latest": (s, d) for s, d in LATEST_VIEW_SPECS}
    awaited = inputs.get(match.group(1))
    if awaited is None:
        return None
    base = f"silver_{awaited[0]}_{awaited[1]}"
    if base in _failed_over_parquet:
        return None
    base_registered = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
        [base],
    ).fetchone()
    return None if base_registered else awaited


def refresh_views(db_path: Path, data_dir: Path) -> None:
    """Re-register all views from the current filesystem state."""
    paths = PathBuilder(data_dir)
    silver_root = paths.silver_root()
    gold_root = paths.gold_root()
    sweep_orphan_temp_files(silver_root, gold_root)
    con = get_connection(db_path)
    _register_views(con, silver_root, gold_root)
    _register_gold_views(con)
    con.close()
