"""T1-r: the CAS write (``advance_watermark``) and its write-outcome contract.

Covers D-20's compare-and-set arms under a REAL concurrent second in-process
DuckDB connection (``con.cursor()``, per the probed §3.7 FACT-3a), the D-20.1
monotonic no-op precondition, the D-20.5/D-20.9 total-function guards, the
D-20.6 failure path (never raises, never double-warns above DEBUG), the D-24
seed/admin-arm classification, and the T1-q AST pin: no module under
``src/gridflow/`` other than ``observability.py`` itself may call
``update_watermark`` -- production advancement goes exclusively through
``advance_watermark`` (D-21/D-22, no exceptions after the rev-6 unification).
"""

from __future__ import annotations

import ast
import logging
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from gridflow.observability import (
    WatermarkOutcome,
    WatermarkRead,
    WatermarkWrite,
    advance_watermark,
    read_watermark,
    update_watermark,
)
from gridflow.storage.duckdb import init_catalogue

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "gridflow"


@pytest.fixture
def con(tmp_path, monkeypatch: pytest.MonkeyPatch) -> duckdb.DuckDBPyConnection:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    db_path = tmp_path / "gridflow.duckdb"
    data_dir = tmp_path / "data"
    init_catalogue(db_path, data_dir)
    connection = duckdb.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()


PAST = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
NEWER = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Happy paths
# --------------------------------------------------------------------------- #


def test_arm_a_undisturbed_advances(con: duckdb.DuckDBPyConnection) -> None:
    """Arm A (present, undisturbed): CAS succeeds, value updated."""
    update_watermark(con, "elexon", "fuelhh", PAST)
    expected = read_watermark(con, "elexon", "fuelhh")

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.ADVANCED
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


def test_arm_b_undisturbed_creates_row(con: duckdb.DuckDBPyConnection) -> None:
    """Arm B (absent, undisturbed): CAS inserts, row created."""
    expected = read_watermark(con, "elexon", "fuelhh")
    assert expected.status == "absent"

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.ADVANCED
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


# --------------------------------------------------------------------------- #
# T1-n / T1-o / T1-p (#27 / #28 / #29): concurrent interloper via con.cursor()
# --------------------------------------------------------------------------- #


def test_arm_a_row_modified_since_snapshot_is_cas_mismatch(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """T1-n (#27): a second connection writes a different value -> CAS_MISMATCH."""
    update_watermark(con, "elexon", "fuelhh", PAST)
    expected = read_watermark(con, "elexon", "fuelhh")

    interloper_value = datetime(2026, 3, 1, tzinfo=UTC)
    interloper = con.cursor()
    interloper.execute(
        "UPDATE pipeline_watermarks SET last_end = ? WHERE source = ? AND dataset = ?",
        [interloper_value.replace(tzinfo=None), "elexon", "fuelhh"],
    )

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.CAS_MISMATCH
    assert result.observed == interloper_value
    assert read_watermark(con, "elexon", "fuelhh").value == interloper_value


def test_arm_b_row_appeared_since_snapshot_is_cas_mismatch(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """T1-o (#28, the P2-1 orphan scenario): a second connection inserts first."""
    expected = read_watermark(con, "elexon", "fuelhh")
    assert expected.status == "absent"

    interloper_value = datetime(2026, 3, 1, tzinfo=UTC)
    interloper = con.cursor()
    interloper.execute(
        "INSERT INTO pipeline_watermarks (source, dataset, last_end, updated_at) "
        "VALUES (?, ?, ?, ?)",
        [
            "elexon",
            "fuelhh",
            interloper_value.replace(tzinfo=None),
            interloper_value.replace(tzinfo=None),
        ],
    )

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.CAS_MISMATCH
    # Revision 4 would have overwritten the interloper's frontier; the CAS must not.
    assert read_watermark(con, "elexon", "fuelhh").value == interloper_value


def test_arm_a_row_deleted_since_snapshot_is_cas_mismatch_no_recreate(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """T1-p (#29): a second connection deletes the row -> CAS_MISMATCH, no re-create."""
    update_watermark(con, "elexon", "fuelhh", PAST)
    expected = read_watermark(con, "elexon", "fuelhh")

    interloper = con.cursor()
    interloper.execute(
        "DELETE FROM pipeline_watermarks WHERE source = ? AND dataset = ?",
        ["elexon", "fuelhh"],
    )

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.CAS_MISMATCH
    assert read_watermark(con, "elexon", "fuelhh").status == "absent"


# --------------------------------------------------------------------------- #
# T2-r (#10 / D-20.1): monotonic no-op precondition
# --------------------------------------------------------------------------- #


def test_last_end_at_or_before_expected_is_no_op(con: duckdb.DuckDBPyConnection) -> None:
    """A ``last_end <= expected.value`` write is a NO_OP: nothing written, no rewind."""
    update_watermark(con, "elexon", "fuelhh", NEWER)
    expected = read_watermark(con, "elexon", "fuelhh")

    result = advance_watermark(con, "elexon", "fuelhh", PAST, expected=expected)

    assert result.outcome == WatermarkOutcome.NO_OP
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


def test_last_end_equal_to_expected_is_no_op(con: duckdb.DuckDBPyConnection) -> None:
    """Exact equality is also a NO_OP (the D-20.1 precondition is ``<=``)."""
    update_watermark(con, "elexon", "fuelhh", NEWER)
    expected = read_watermark(con, "elexon", "fuelhh")

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.NO_OP
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


# --------------------------------------------------------------------------- #
# D-20.5 / D-20.9 total-function guards
# --------------------------------------------------------------------------- #


def test_unreadable_expected_is_cas_mismatch_no_write(con: duckdb.DuckDBPyConnection) -> None:
    """D-20.5: an ``unreadable`` snapshot is unreachable in practice but total."""
    expected = WatermarkRead(status="unreadable", value=None, error="boom")

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.CAS_MISMATCH
    assert read_watermark(con, "elexon", "fuelhh").status == "absent"


def test_t1t_present_status_with_none_value_is_cas_mismatch(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """T1-t (#40): a hand-built contract-violating snapshot refuses, never raises."""
    expected = WatermarkRead(status="present", value=None)

    result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.CAS_MISMATCH
    assert read_watermark(con, "elexon", "fuelhh").status == "absent"


# --------------------------------------------------------------------------- #
# D-20.6 / #38 / D-26: write failure never raises, never double-warns
# --------------------------------------------------------------------------- #


def test_write_failure_returns_write_failed_and_logs_only_debug(
    con: duckdb.DuckDBPyConnection, caplog: pytest.LogCaptureFixture
) -> None:
    """A write exception -> WRITE_FAILED, redacted detail, nothing above DEBUG."""
    update_watermark(con, "elexon", "fuelhh", PAST)
    expected = read_watermark(con, "elexon", "fuelhh")
    con.close()  # force the write attempt to raise

    with caplog.at_level(logging.DEBUG, logger="gridflow.observability"):
        result = advance_watermark(con, "elexon", "fuelhh", NEWER, expected=expected)

    assert result.outcome == WatermarkOutcome.WRITE_FAILED
    assert result.error is not None
    above_debug = [r for r in caplog.records if r.levelno > logging.DEBUG]
    assert above_debug == [], f"advance_watermark must log nothing above DEBUG: {above_debug}"


# --------------------------------------------------------------------------- #
# T1-s (#39): the seed/admin arm's D-24 classification
# --------------------------------------------------------------------------- #


def test_update_watermark_prior_later_than_bind_is_no_op(con: duckdb.DuckDBPyConnection) -> None:
    update_watermark(con, "elexon", "fuelhh", NEWER)

    result = update_watermark(con, "elexon", "fuelhh", PAST)

    assert result.outcome == WatermarkOutcome.NO_OP
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


def test_update_watermark_prior_equal_to_bind_is_no_op(con: duckdb.DuckDBPyConnection) -> None:
    update_watermark(con, "elexon", "fuelhh", NEWER)

    result = update_watermark(con, "elexon", "fuelhh", NEWER)

    assert result.outcome == WatermarkOutcome.NO_OP
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


def test_update_watermark_prior_earlier_than_bind_is_advanced(
    con: duckdb.DuckDBPyConnection,
) -> None:
    update_watermark(con, "elexon", "fuelhh", PAST)

    result = update_watermark(con, "elexon", "fuelhh", NEWER)

    assert result.outcome == WatermarkOutcome.ADVANCED
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


def test_update_watermark_row_absent_is_advanced(con: duckdb.DuckDBPyConnection) -> None:
    result = update_watermark(con, "elexon", "fuelhh", NEWER)

    assert result.outcome == WatermarkOutcome.ADVANCED
    assert read_watermark(con, "elexon", "fuelhh").value == NEWER


# --------------------------------------------------------------------------- #
# T1-q (#32): the AST anti-drift pin
# --------------------------------------------------------------------------- #


def _calls_update_watermark(tree: ast.AST) -> bool:
    """True if the AST contains a call whose callee resolves to ``update_watermark``."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id == "update_watermark":
            return True
        if isinstance(func, ast.Attribute) and func.attr == "update_watermark":
            return True
    return False


def test_no_production_module_calls_update_watermark_except_observability() -> None:
    """T1-q: production advancement must go through ``advance_watermark`` (D-21/D-22).

    No exceptions, no allow-list entries -- the rev-6 unification removed the
    one production call (the explicit path) rev 5 had retained, so the pin
    holds with zero carve-outs.
    """
    offending: list[str] = []
    for path in SRC_ROOT.rglob("*.py"):
        if path.name == "observability.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if _calls_update_watermark(tree):
            offending.append(str(path.relative_to(SRC_ROOT)))

    assert offending == [], f"production modules calling update_watermark: {offending}"


def test_watermark_write_is_frozen_dataclass() -> None:
    """Sanity: WatermarkWrite is immutable (matches WatermarkRead's contract)."""
    write = WatermarkWrite(outcome=WatermarkOutcome.ADVANCED)
    with pytest.raises(Exception):  # noqa: B017, PT011 -- frozen dataclass raises FrozenInstanceError
        write.outcome = WatermarkOutcome.NO_OP  # type: ignore[misc]
