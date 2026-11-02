"""``runner.run_backfill``: each chunk is one half-open ``[start, end)`` window.

The chunk is handed to ingest and transform unchanged, so a final chunk that
ends mid-day still transforms its last day, and an incomplete ingest is never
followed by a transform of the partial bronze it left behind.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from gridflow.pipeline import runner
from gridflow.pipeline.runner import DatasetResult
from gridflow.utils.time import window_dates

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.config.settings import GridflowConfig

D = datetime(2024, 1, 15, tzinfo=UTC)
ONE = timedelta(days=1)


@dataclass
class Calls:
    """Each stage's recorded windows; ``ingest_status`` sets a chunk's ingest outcome."""

    ingest: list[tuple[datetime, datetime]] = field(default_factory=list)
    transform: list[tuple[datetime, datetime]] = field(default_factory=list)
    ingest_status: dict[datetime, str] = field(default_factory=dict)


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> Calls:
    """Stub the context, views and both stages; record each stage's window."""
    recorded = Calls()

    @contextlib.contextmanager
    def _fake_context(_settings: object) -> Iterator[object]:
        yield object()

    def _stage(name: str) -> Any:
        def run(*args: Any, **_kwargs: Any) -> list[DatasetResult]:
            _ctx, source, datasets, start, end = args
            getattr(recorded, name).append((start, end))
            status = recorded.ingest_status.get(start, "success") if name == "ingest" else "success"
            error = "boom" if status == "failed" else None
            return [DatasetResult(source, datasets[0], name, status, error=error)]

        return run

    monkeypatch.setattr(runner, "build_context", _fake_context)
    monkeypatch.setattr(runner, "refresh_views", lambda *a, **k: None)
    monkeypatch.setattr(runner, "run_ingest", _stage("ingest"))
    monkeypatch.setattr(runner, "run_transform", _stage("transform"))
    return recorded


def test_a_chunk_ending_mid_day_transforms_its_last_day(
    sample_config: GridflowConfig, calls: Calls
) -> None:
    """The old ``chunk_end - 1 day`` handed transform ``[D, D-1 12:00)``: no dates."""
    runner.run_backfill(sample_config, "elexon", ["system_prices"], D, D + timedelta(hours=12))

    assert calls.transform == calls.ingest == [(D, D + timedelta(hours=12))]
    assert window_dates(*calls.transform[0]) == [D.date()]


def test_each_chunk_transforms_exactly_the_days_it_ingested(
    sample_config: GridflowConfig, calls: Calls
) -> None:
    runner.run_backfill(sample_config, "elexon", ["system_prices"], D, D + 3 * ONE, chunk_days=2)

    assert calls.transform == calls.ingest
    assert [window_dates(*chunk) for chunk in calls.transform] == [
        [date(2024, 1, 15), date(2024, 1, 16)],
        [date(2024, 1, 17)],
    ]


def test_a_failed_ingest_skips_that_chunks_transform(
    sample_config: GridflowConfig, calls: Calls
) -> None:
    """A transform of partial bronze writes silver, and a caller that counts
    silver rows as "done" would then never retry the chunk's ingest."""
    calls.ingest_status[D] = "failed"

    report = runner.run_backfill(sample_config, "elexon", ["system_prices"], D, D + 2 * ONE)

    assert len(calls.ingest) == 2
    assert calls.transform == [(D + ONE, D + 2 * ONE)]
    assert [(r.operation, r.status) for r in report.results] == [
        ("ingest", "failed"),
        ("ingest", "success"),
        ("transform", "success"),
    ]


def test_a_partial_ingest_skips_the_transform_and_fails_the_run(
    sample_config: GridflowConfig, calls: Calls
) -> None:
    """On the backfill path ``completed_with_warnings`` means units were skipped,
    so the chunk is as incomplete as a failed one, yet on its own it would let
    the run end ok with no silver for the chunk."""
    calls.ingest_status[D] = "completed_with_warnings"

    report = runner.run_backfill(sample_config, "elexon", ["system_prices"], D, D + ONE)

    assert calls.transform == []
    assert not report.ok
    assert [(r.operation, r.status) for r in report.results] == [
        ("ingest", "completed_with_warnings"),
        ("transform", "failed"),
    ]
    assert "skipped units" in (report.results[1].error or "")


def test_on_chunk_reports_each_chunk_once(sample_config: GridflowConfig, calls: Calls) -> None:
    seen: list[tuple[str, int, datetime, datetime]] = []

    runner.run_backfill(
        sample_config,
        "elexon",
        ["system_prices", "pn"],
        D,
        D + 2 * ONE,
        on_chunk=lambda *args: seen.append(args),
    )

    assert seen == [
        ("system_prices", 1, D, D + ONE),
        ("system_prices", 2, D + ONE, D + 2 * ONE),
        ("pn", 1, D, D + ONE),
        ("pn", 2, D + ONE, D + 2 * ONE),
    ]


@pytest.mark.usefixtures("calls")
def test_chunk_days_below_one_is_refused(
    sample_config: GridflowConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``chunk_days=0`` never advanced the loop. ``run_ingest`` raises here so
    the old code fails fast instead of hanging the suite."""

    def _ingest(*_args: object, **_kwargs: object) -> list[DatasetResult]:
        raise RuntimeError("the chunk loop was entered")

    monkeypatch.setattr(runner, "run_ingest", _ingest)

    with pytest.raises(ValueError, match="chunk_days must be >= 1"):
        runner.run_backfill(sample_config, "elexon", ["system_prices"], D, D + ONE, chunk_days=0)


@pytest.mark.parametrize(
    ("source", "dataset"),
    [("open_meteo", "historical_demand"), ("open_meteo", "forecast_wind"), ("gie_alsi", "lng")],
)
def test_chunks_past_the_look_back_are_refused(
    sample_config: GridflowConfig, calls: Calls, source: str, dataset: str
) -> None:
    """Detects an Open-Meteo or ALSI chunk wider than the 35 days a date looks
    back for its request being fetched: its later days would never reach silver."""
    with pytest.raises(ValueError, match="at most 35 days, not 36"):
        runner.run_backfill(sample_config, source, [dataset], D, D + 40 * ONE, chunk_days=36)
    assert calls.ingest == []

    runner.run_backfill(sample_config, source, [dataset], D, D + 40 * ONE, chunk_days=35)
    assert calls.ingest == [(D, D + 35 * ONE), (D + 35 * ONE, D + 40 * ONE)]
