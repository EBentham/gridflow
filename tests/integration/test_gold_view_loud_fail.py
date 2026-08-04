"""Integration tests for F15-D: gold-view loud-fail under strict mode.

PBI-05: _try_create_view and _register_gold_views must raise under pytest/dev
instead of silently swallowing view-creation errors.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import duckdb
import pytest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.integration
def test_register_gold_views_raises_on_broken_sql_under_strict_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F15-D / PBI-05: _register_gold_views raises when GRIDFLOW_ENV=dev.

    The real gold SQL files reference silver views (silver_gie_agsi_storage,
    etc.) that do not exist in a fresh in-memory connection.  DuckDB fails at CREATE VIEW
    time (not lazily).  Pre-F15-D: exception is swallowed; pytest.raises sees
    no raise → FAILS RED.  Post-F15-D: exception propagates → PASSES GREEN.
    """
    from gridflow.storage.duckdb import _register_gold_views

    monkeypatch.setenv("GRIDFLOW_ENV", "dev")

    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(duckdb.Error):
            _register_gold_views(con)
    finally:
        con.close()


@pytest.mark.integration
def test_try_create_view_raises_under_pytest(tmp_path: Path) -> None:
    """F15-D / PBI-05: _try_create_view raises when PYTEST_CURRENT_TEST is set.

    pytest auto-sets PYTEST_CURRENT_TEST for every test, so strict mode is on
    during all test runs.  Pre-F15-D: exception swallowed → FAILS RED.
    Post-F15-D: exception propagates → PASSES GREEN.
    """
    from gridflow.storage.duckdb import _try_create_view

    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(duckdb.Error):
            _try_create_view(
                con,
                "silver_x",
                str(tmp_path / "does" / "not" / "exist" / "*.parquet"),
            )
    finally:
        con.close()


@pytest.mark.integration
def test_production_mode_swallows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """F15-D / PBI-05: legacy swallow behaviour preserved in production mode.

    When neither PYTEST_CURRENT_TEST nor GRIDFLOW_ENV=dev/test is set,
    _try_create_view must NOT raise — logs debug and continues.  Passes both
    before and after F15-D because the production swallow path is unchanged.
    """
    from gridflow.storage.duckdb import _try_create_view

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    con = duckdb.connect(":memory:")
    try:
        _try_create_view(
            con,
            "silver_x",
            str(tmp_path / "missing" / "*.parquet"),
        )
    finally:
        con.close()


@pytest.mark.integration
def test_register_gold_views_debug_logs_benign_absent_in_production_mode(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B1.1 / F-14 / R3-c: benign-absent gold view registration logs DEBUG.

    With neither PYTEST_CURRENT_TEST nor GRIDFLOW_ENV=dev/test set, and no
    silver views registered, every gold SQL file fails to bind because its
    referenced silver relation does not exist yet -- the benign "not yet
    transformed" case (storage/duckdb.py:369-390). _register_gold_views must
    swallow (not raise) and log at DEBUG, mirroring _try_create_view's
    benign-absent convention (storage/duckdb.py:354-382), not WARNING.
    """
    from gridflow.storage.duckdb import _register_gold_views

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    con = duckdb.connect(":memory:")
    try:
        with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
            _register_gold_views(con)
    finally:
        con.close()

    debug_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG
    ]
    warning_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert any("uk_imbalance_context.sql" in msg for msg in debug_messages), (
        f"expected a DEBUG log mentioning uk_imbalance_context.sql, got: {debug_messages}"
    )
    assert not warning_messages, (
        f"expected no WARNING logs for the benign-absent case, got: {warning_messages}"
    )


@pytest.mark.integration
def test_register_gold_views_warns_on_misspelled_relation_in_production_mode(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol diff pass 1 major: a misspelled relation name must WARN, not DEBUG.

    A gold SQL file referencing a name that LOOKS like a silver relation but
    isn't one gridflow actually registers (a typo) raises a CatalogException
    with the identical "does not exist" message shape as the genuine
    benign-absent case. _is_benign_absent_gold_view must classify by
    membership in the known-relation set, not by message shape alone --
    otherwise this typo is silently logged at DEBUG and the gold view stays
    silently absent in production.
    """
    from gridflow.storage import duckdb as duckdb_module

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    fake_module_dir = tmp_path / "storage"
    fake_module_dir.mkdir()
    monkeypatch.setattr(duckdb_module, "__file__", str(fake_module_dir / "duckdb.py"))

    views_dir = tmp_path / "gold" / "views"
    views_dir.mkdir(parents=True)
    (views_dir / "misspelled_relation.sql").write_text(
        "CREATE OR REPLACE VIEW gold_misspelled AS SELECT * FROM silver_elexon_sysprices"
    )

    con = duckdb.connect(":memory:")
    try:
        with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
            duckdb_module._register_gold_views(con)
    finally:
        con.close()

    warning_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    debug_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG
    ]
    assert any("misspelled_relation.sql" in msg for msg in warning_messages), (
        f"expected a WARNING mentioning misspelled_relation.sql, got: {warning_messages}"
    )
    assert not any("misspelled_relation.sql" in msg for msg in debug_messages), (
        f"expected no DEBUG log for the misspelled-relation fixture, got: {debug_messages}"
    )


@pytest.mark.integration
def test_register_gold_views_warns_on_real_error_in_production_mode(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B1.1 / F-14 / R3-c: a genuine DDL/binder error still WARNs.

    A gold SQL file referencing an EXISTING silver table but a bogus column
    is a deterministic DDL/binder bug, not a "not yet transformed" absence
    (the CatalogException "does not exist" case _is_benign_absent_gold_view
    matches). _register_gold_views must swallow it (strict-mode raise path
    untouched) but log at WARNING so the failure stays visible in
    production, distinguishing it from the benign-absent DEBUG case.
    """
    from gridflow.storage import duckdb as duckdb_module

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    fake_module_dir = tmp_path / "storage"
    fake_module_dir.mkdir()
    monkeypatch.setattr(duckdb_module, "__file__", str(fake_module_dir / "duckdb.py"))

    views_dir = tmp_path / "gold" / "views"
    views_dir.mkdir(parents=True)
    (views_dir / "broken_real_error.sql").write_text(
        "CREATE OR REPLACE VIEW gold_broken AS SELECT bogus_col FROM silver_present"
    )

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_present (a INTEGER)")
    try:
        with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
            duckdb_module._register_gold_views(con)
    finally:
        con.close()

    warning_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    debug_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG
    ]
    assert any("broken_real_error.sql" in msg for msg in warning_messages), (
        f"expected a WARNING mentioning broken_real_error.sql, got: {warning_messages}"
    )
    assert not any("broken_real_error.sql" in msg for msg in debug_messages), (
        f"expected no DEBUG log for the real-error fixture, got: {debug_messages}"
    )


@pytest.mark.integration
def test_register_gold_views_warns_on_missing_gold_serving_alias_in_production_mode(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol diff pass 2 major 1: a missing GOLD relation must WARN, not DEBUG.

    Before the fix, ``_expected_silver_relation_names`` was built from the
    UNFILTERED manifest, which includes gold-layer serving aliases
    (``gold_eu_gas_storage``, ``gold_uk_imbalance_context``). A gold SQL file
    referencing one of those names as if it were an upstream relation would
    then classify as benign-absent DEBUG -- re-opening the silent-swallow
    for exactly the names the membership test exists to police. The expected
    set must be filtered to silver-layer relations (``relation_kind`` of
    ``"silver"``/``"serving_alias"``) only, so a missing gold-layer object
    always falls to WARNING.
    """
    from gridflow.storage import duckdb as duckdb_module

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    fake_module_dir = tmp_path / "storage"
    fake_module_dir.mkdir()
    monkeypatch.setattr(duckdb_module, "__file__", str(fake_module_dir / "duckdb.py"))

    views_dir = tmp_path / "gold" / "views"
    views_dir.mkdir(parents=True)
    (views_dir / "downstream_of_gold.sql").write_text(
        "CREATE OR REPLACE VIEW gold_downstream AS SELECT * FROM gold_uk_imbalance_context"
    )

    con = duckdb.connect(":memory:")
    try:
        with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
            duckdb_module._register_gold_views(con)
    finally:
        con.close()

    warning_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    debug_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG
    ]
    assert any("downstream_of_gold.sql" in msg for msg in warning_messages), (
        f"expected a WARNING mentioning downstream_of_gold.sql, got: {warning_messages}"
    )
    assert not any("downstream_of_gold.sql" in msg for msg in debug_messages), (
        f"expected no DEBUG log for a missing gold-layer relation, got: {debug_messages}"
    )


@pytest.mark.integration
def test_register_gold_views_fails_closed_when_expected_set_build_raises(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol diff pass 2 major 2: an expected-set build failure must not escape.

    ``_expected_silver_relation_names`` is built INSIDE
    ``_register_gold_views``' exception handler, and its manifest builder
    (``get_silver_schema_manifest``) has documented ``ValueError`` paths (an
    incomplete transformer/date/spec registration). Before the fix, a build
    failure there would propagate out of ``_register_gold_views`` and turn
    production's logging-only posture into an init/refresh hard failure.
    The build must fail closed: registration completes without the
    exception escaping, classifying the triggering error as non-benign
    (WARNING), and the build failure itself is logged at WARNING once.
    """
    from gridflow.storage import duckdb as duckdb_module

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    def _boom() -> frozenset[str]:
        raise ValueError("No designated date column registered for fake/dataset")

    monkeypatch.setattr(duckdb_module, "_expected_silver_relation_names", _boom)

    fake_module_dir = tmp_path / "storage"
    fake_module_dir.mkdir()
    monkeypatch.setattr(duckdb_module, "__file__", str(fake_module_dir / "duckdb.py"))

    views_dir = tmp_path / "gold" / "views"
    views_dir.mkdir(parents=True)
    (views_dir / "uk_imbalance_context.sql").write_text(
        "CREATE OR REPLACE VIEW gold_uk_imbalance_context AS "
        "SELECT * FROM silver_elexon_system_prices_latest"
    )

    con = duckdb.connect(":memory:")
    try:
        with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
            duckdb_module._register_gold_views(con)
    finally:
        con.close()

    warning_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert any(
        "expected silver relation names" in msg and "No designated date column" in msg
        for msg in warning_messages
    ), f"expected a WARNING logging the expected-set build failure, got: {warning_messages}"
    assert any("uk_imbalance_context.sql" in msg for msg in warning_messages), (
        f"expected a WARNING mentioning uk_imbalance_context.sql, got: {warning_messages}"
    )
