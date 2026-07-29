"""F-09: the incremental fetch window can never exceed
``max_incremental_lookback_hours``, however long the frontier has been frozen.

Covers, per R2-C-PLAN.md Task 1:
- T1-a: the resolved span is always <= ``max_incremental_lookback_hours``,
  proven by a staleness sweep (48h/120h/240h).
- T1-c: the settings validator's three clauses, every named boundary.
- T1-d: ``ingest_runs_since`` -- normal count, and fail-closed to ``None``.
- T1-e/T1-k: frontier absent -- permitted when not clamped, denied by clause 4
  when a directly-supplied ``default_start`` clamps (validator-independent).
- T1-f: a future ``end_dt`` -- near (+1h) and far (+30d) -- is denied
  (``FUTURE_END``); ``end_dt == now`` (the CLI shape) is unaffected.
- T1-g: a frontier ahead of ``end_dt`` is still permitted by the window
  (the NO_OP at the write layer is D-20.1, covered in test_watermark_cas.py).
- T1-h: the resolver tolerates an ``ingest_runs_since`` failure.
- T1-i: an unreadable read denies and populates a REDACTED ``frontier_error``.
- T1-j: exactly ONE ``read_watermark`` call per resolution (D-10.3).
- T1-m: a negative ``overlap`` passed directly denies, independent of the
  config validator.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import pytest
from pydantic import ValidationError

from gridflow.config.settings import PipelineSettings
from gridflow.observability import (
    PipelineRunTracker,
    WatermarkRead,
    ingest_runs_since,
    update_watermark,
)
from gridflow.pipeline.runner import IncrementalWindow, WindowReason, resolve_incremental_window
from gridflow.storage.duckdb import init_catalogue


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


OVERLAP = timedelta(hours=72)
MAX_LOOKBACK = timedelta(hours=168)


# --------------------------------------------------------------------------- #
# T1-a: the staleness sweep -- the acceptance criterion, directly.
# --------------------------------------------------------------------------- #


def test_staleness_48h_not_clamped_healthy(con: duckdb.DuckDBPyConnection) -> None:
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    frontier = now - timedelta(hours=48)
    update_watermark(con, "elexon", "fuelhh", frontier)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )

    assert window.clamped is False
    assert window.advance_permitted is True
    assert window.reason == WindowReason.HEALTHY
    assert now - window.start <= MAX_LOOKBACK


def test_staleness_120h_clamped_permitted_no_gap(con: duckdb.DuckDBPyConnection) -> None:
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    frontier = now - timedelta(hours=120)
    update_watermark(con, "elexon", "fuelhh", frontier)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )

    assert window.clamped is True
    assert window.advance_permitted is True
    assert window.reason == WindowReason.CLAMPED_NO_GAP
    assert now - window.start <= MAX_LOOKBACK


def test_staleness_240h_clamped_denied_gap_exactly_72h(con: duckdb.DuckDBPyConnection) -> None:
    """The 30-day-stale case (T1-a's original RED scenario), generalised: a
    240h-stale frontier clamps AND denies, with a gap of exactly 72h."""
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    frontier = now - timedelta(hours=240)
    update_watermark(con, "elexon", "fuelhh", frontier)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )

    assert window.clamped is True
    assert window.advance_permitted is False
    assert window.reason == WindowReason.UNFETCHED_GAP
    assert now - window.start <= MAX_LOOKBACK
    assert window.gap_start == frontier
    assert window.gap_end == window.start
    assert window.gap_end - window.gap_start == timedelta(hours=72)


def test_span_is_bounded_by_max_incremental_lookback_hours_thirty_day_stall(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """The original T1-a scenario: a 30-day-stale watermark resolves <= 168h."""
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    watermark = now - timedelta(days=30)
    update_watermark(con, "elexon", "fuelhh", watermark)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )
    span = now - window.start

    assert span <= MAX_LOOKBACK, f"span {span} must be clamped to <= {MAX_LOOKBACK}"
    assert window.advance_permitted is False, "a 30-day stall is a TRUE GAP, not a quiet clamp"


# --------------------------------------------------------------------------- #
# T1-e / T1-k: frontier absent.
# --------------------------------------------------------------------------- #


def test_frontier_absent_not_clamped_permitted_healthy(con: duckdb.DuckDBPyConnection) -> None:
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    default_start = now - timedelta(hours=24)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", default_start, OVERLAP, now, MAX_LOOKBACK
    )

    assert window.clamped is False
    assert window.advance_permitted is True
    assert window.reason == WindowReason.HEALTHY
    assert window.start == default_start
    assert window.snapshot is not None
    assert window.snapshot.status == "absent"


def test_clamp_with_absent_frontier_denied_by_clause_4(con: duckdb.DuckDBPyConnection) -> None:
    """T1-k (#20): forbidden by validator clause 2 in real config, but the
    resolver itself is total and structurally denies via clause 4 for a
    directly-supplied `default_start` that clamps despite an absent frontier."""
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    default_start = now - timedelta(days=300)  # would violate D-11/clause 2 if validated

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", default_start, OVERLAP, now, MAX_LOOKBACK
    )

    assert window.clamped is True
    assert window.advance_permitted is False


# --------------------------------------------------------------------------- #
# T1-f: the right bound (D-14.2) -- future `end_dt`.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("delta", [timedelta(hours=1), timedelta(days=30)])
def test_future_end_dt_denied_near_and_far(
    con: duckdb.DuckDBPyConnection, delta: timedelta
) -> None:
    real_now = datetime.now(UTC)
    end_dt = real_now + delta

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", real_now - timedelta(hours=24), OVERLAP, end_dt, MAX_LOOKBACK
    )

    assert window.advance_permitted is False
    assert window.reason == WindowReason.FUTURE_END


def test_end_dt_equal_to_now_is_the_cli_shape_and_unaffected(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """§3.8: whenever incremental=True reaches run_ingest from the CLI,
    end_dt == now -- confirm this ordinary shape is never denied as FUTURE_END."""
    now = datetime.now(UTC)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )

    assert window.reason != WindowReason.FUTURE_END
    assert window.advance_permitted is True


# --------------------------------------------------------------------------- #
# T1-g: frontier ahead of end_dt -- permitted by clause 3 (NO_OP is D-20.1,
# tested at the write layer in test_watermark_cas.py).
# --------------------------------------------------------------------------- #


def test_frontier_ahead_of_end_dt_still_permitted(con: duckdb.DuckDBPyConnection) -> None:
    end_dt = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    frontier = end_dt + timedelta(hours=2)
    update_watermark(con, "elexon", "fuelhh", frontier)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", end_dt - timedelta(hours=24), OVERLAP, end_dt, MAX_LOOKBACK
    )

    assert window.advance_permitted is True
    assert window.reason == WindowReason.HEALTHY


# --------------------------------------------------------------------------- #
# T1-d / T1-h: ingest_runs_since -- counts, and fails closed to None.
# --------------------------------------------------------------------------- #


def test_ingest_runs_since_counts_runs_after_anchor(con: duckdb.DuckDBPyConnection) -> None:
    anchor = datetime.now(UTC) - timedelta(hours=1)
    PipelineRunTracker(con, "elexon", "fuelhh", "ingest")
    PipelineRunTracker(con, "elexon", "fuelhh", "ingest")

    count = ingest_runs_since(con, "elexon", "fuelhh", anchor)

    assert count == 2


def test_ingest_runs_since_returns_none_on_failure(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DROP TABLE pipeline_runs")

    result = ingest_runs_since(con, "elexon", "fuelhh", datetime.now(UTC))

    assert result is None


def test_resolver_tolerates_ingest_runs_since_failure(con: duckdb.DuckDBPyConnection) -> None:
    """T1-h (#12): a diagnostic-only failure never affects the advance decision."""
    now = datetime.now(UTC)
    watermark = now - timedelta(hours=10)
    update_watermark(con, "elexon", "fuelhh", watermark)
    con.execute("DROP TABLE pipeline_runs")

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )

    assert window.runs_since is None
    assert window.advance_permitted is True


# --------------------------------------------------------------------------- #
# T1-i: an unreadable frontier denies and redacts the error.
# --------------------------------------------------------------------------- #


def test_unreadable_frontier_denies_and_redacts_error(
    con: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _broken_read(con: Any, source: str, dataset: str) -> WatermarkRead:
        return WatermarkRead(
            status="unreadable",
            value=None,
            error="db unreachable: securityToken=SECRET123 boom",
        )

    monkeypatch.setattr("gridflow.observability.read_watermark", _broken_read)
    now = datetime.now(UTC)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )

    assert window.advance_permitted is False
    assert window.reason == WindowReason.FRONTIER_UNREADABLE
    assert window.frontier_error is not None
    assert "SECRET123" not in window.frontier_error
    assert window.frontier is None


# --------------------------------------------------------------------------- #
# T1-j (#18): exactly ONE read_watermark call per resolution (D-10.3).
# --------------------------------------------------------------------------- #


def test_resolver_reads_watermark_exactly_once(
    con: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    from gridflow.observability import read_watermark as real_read_watermark

    calls: list[tuple[str, str]] = []

    def _spy(con: Any, source: str, dataset: str) -> WatermarkRead:
        calls.append((source, dataset))
        return real_read_watermark(con, source, dataset)

    monkeypatch.setattr("gridflow.observability.read_watermark", _spy)
    now = datetime.now(UTC)
    update_watermark(con, "elexon", "fuelhh", now - timedelta(hours=10))

    resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), OVERLAP, now, MAX_LOOKBACK
    )

    assert calls == [("elexon", "fuelhh")]


# --------------------------------------------------------------------------- #
# T1-m (#23): negative overlap denies structurally, independent of the
# config validator (which is bypassed here by calling the resolver directly).
# --------------------------------------------------------------------------- #


def test_negative_overlap_denied_independent_of_validator(
    con: duckdb.DuckDBPyConnection,
) -> None:
    now = datetime.now(UTC)
    watermark = now - timedelta(hours=10)
    update_watermark(con, "elexon", "fuelhh", watermark)

    window = resolve_incremental_window(
        con, "elexon", "fuelhh", now - timedelta(hours=24), timedelta(hours=-1), now, MAX_LOOKBACK
    )

    assert window.advance_permitted is False
    assert window.reason == WindowReason.UNFETCHED_GAP


# --------------------------------------------------------------------------- #
# not_applicable -- the explicit/backfill path builds a window too (D-12/D-16).
# --------------------------------------------------------------------------- #


def test_not_applicable_permits_and_carries_no_snapshot() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)

    window = IncrementalWindow.not_applicable(start)

    assert window.start == start
    assert window.advance_permitted is True
    assert window.snapshot is None
    assert window.reason == WindowReason.NOT_INCREMENTAL


# --------------------------------------------------------------------------- #
# T1-c (R-7): the settings validator's three clauses, every named boundary.
# --------------------------------------------------------------------------- #


def _settings(**overrides: int) -> PipelineSettings:
    base: dict[str, int] = {
        "default_lookback_hours": 24,
        "incremental_overlap_hours": 72,
        "max_incremental_lookback_hours": 168,
    }
    base.update(overrides)
    return PipelineSettings(**base)  # type: ignore[arg-type]


def test_clause1_overlap_equal_to_max_lookback_is_ok() -> None:
    _settings(incremental_overlap_hours=168, max_incremental_lookback_hours=168)


def test_clause1_overlap_one_over_max_lookback_raises() -> None:
    with pytest.raises(ValidationError, match="incremental_overlap_hours"):
        _settings(incremental_overlap_hours=169, max_incremental_lookback_hours=168)


def test_clause2_default_lookback_equal_to_max_lookback_is_ok() -> None:
    _settings(default_lookback_hours=168, max_incremental_lookback_hours=168)


def test_clause2_default_lookback_one_over_max_lookback_raises() -> None:
    with pytest.raises(ValidationError, match="default_lookback_hours"):
        _settings(default_lookback_hours=169, max_incremental_lookback_hours=168)


def test_clause3_overlap_negative_one_raises() -> None:
    with pytest.raises(ValidationError, match="incremental_overlap_hours"):
        _settings(incremental_overlap_hours=-1)


def test_clause3_overlap_zero_is_ok() -> None:
    _settings(incremental_overlap_hours=0)


def test_clause3_overlap_one_is_ok() -> None:
    _settings(incremental_overlap_hours=1)


def test_clause3_default_lookback_zero_raises() -> None:
    with pytest.raises(ValidationError, match="default_lookback_hours"):
        _settings(default_lookback_hours=0)


def test_clause3_default_lookback_one_is_ok() -> None:
    _settings(default_lookback_hours=1)


def test_clause3_max_lookback_zero_raises() -> None:
    """max_incremental_lookback_hours=0 always fails validation -- via clause 2
    (default_lookback_hours <= max_lookback) whenever default_lookback_hours >= 1,
    or via clause 3's own floor otherwise; either way the config is rejected."""
    with pytest.raises(ValidationError):
        _settings(max_incremental_lookback_hours=0)


def test_clause3_max_lookback_one_is_ok() -> None:
    _settings(
        max_incremental_lookback_hours=1, default_lookback_hours=1, incremental_overlap_hours=0
    )
