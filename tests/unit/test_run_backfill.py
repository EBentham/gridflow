"""``runner.run_backfill``: each chunk is one half-open ``[start, end)`` window.

The chunk is handed to ingest and transform unchanged, so a final chunk that
ends mid-day still transforms its last day, and a failed ingest is never
followed by a transform of the partial bronze it left behind.
"""

from __future__ import annotations

import contextlib
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


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[tuple[datetime, datetime]]]:
    """Stub the context, views and both stages; record each stage's window."""
    recorded: dict[str, list[tuple[datetime, datetime]]] = {"ingest": [], "transform": []}

    @contextlib.contextmanager
    def _fake_context(_settings: object) -> Iterator[object]:
        yield object()

    def _ingest(
        _ctx: object,
        source: str,
        datasets: list[str],
        start: datetime,
        end: datetime,
        **_kwargs: Any,
    ) -> list[DatasetResult]:
        recorded["ingest"].append((start, end))
        return [DatasetResult(source, datasets[0], "ingest", "success")]

    def _transform(
        _ctx: object,
        source: str,
        datasets: list[str],
        start: datetime,
        end: datetime,
        **_kwargs: Any,
    ) -> list[DatasetResult]:
        recorded["transform"].append((start, end))
        return [DatasetResult(source, datasets[0], "transform", "success")]

    monkeypatch.setattr(runner, "build_context", _fake_context)
    monkeypatch.setattr(runner, "refresh_views", lambda *a, **k: None)
    monkeypatch.setattr(runner, "run_ingest", _ingest)
    monkeypatch.setattr(runner, "run_transform", _transform)
    return recorded


def test_a_chunk_ending_mid_day_transforms_its_last_day(
    sample_config: GridflowConfig, calls: dict[str, list[tuple[datetime, datetime]]]
) -> None:
    """The old ``chunk_end - 1 day`` handed transform ``[D, D-1 12:00)``: no dates."""
    runner.run_backfill(sample_config, "elexon", ["system_prices"], D, D + timedelta(hours=12))

    assert calls["transform"] == calls["ingest"] == [(D, D + timedelta(hours=12))]
    assert window_dates(*calls["transform"][0]) == [D.date()]


def test_each_chunk_transforms_exactly_the_days_it_ingested(
    sample_config: GridflowConfig, calls: dict[str, list[tuple[datetime, datetime]]]
) -> None:
    runner.run_backfill(
        sample_config, "elexon", ["system_prices"], D, D + timedelta(days=3), chunk_days=2
    )

    assert calls["transform"] == calls["ingest"]
    assert [window_dates(*chunk) for chunk in calls["transform"]] == [
        [date(2024, 1, 15), date(2024, 1, 16)],
        [date(2024, 1, 17)],
    ]


def test_a_failed_ingest_skips_that_chunks_transform(
    sample_config: GridflowConfig,
    calls: dict[str, list[tuple[datetime, datetime]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transform of partial bronze writes silver, and a caller that counts
    silver rows as "done" would then never retry the chunk's ingest."""

    def _ingest(
        _ctx: object,
        source: str,
        datasets: list[str],
        start: datetime,
        end: datetime,
        **_kwargs: Any,
    ) -> list[DatasetResult]:
        calls["ingest"].append((start, end))
        if start == D:
            return [DatasetResult(source, datasets[0], "ingest", "failed", error="boom")]
        return [DatasetResult(source, datasets[0], "ingest", "success")]

    monkeypatch.setattr(runner, "run_ingest", _ingest)

    report = runner.run_backfill(
        sample_config, "elexon", ["system_prices"], D, D + timedelta(days=2)
    )

    assert len(calls["ingest"]) == 2
    assert calls["transform"] == [(D + timedelta(days=1), D + timedelta(days=2))]
    assert [(r.operation, r.status) for r in report.results] == [
        ("ingest", "failed"),
        ("ingest", "success"),
        ("transform", "success"),
    ]


def test_a_partial_ingest_skips_the_transform_and_fails_the_run(
    sample_config: GridflowConfig,
    calls: dict[str, list[tuple[datetime, datetime]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On the backfill path ``completed_with_warnings`` means units were skipped,
    so the chunk is as incomplete as a failed one, yet on its own it would let
    the run end ok with no silver for the chunk."""

    def _ingest(
        _ctx: object,
        source: str,
        datasets: list[str],
        start: datetime,
        end: datetime,
        **_kwargs: Any,
    ) -> list[DatasetResult]:
        calls["ingest"].append((start, end))
        return [
            DatasetResult(source, datasets[0], "ingest", "completed_with_warnings", rows_skipped=1)
        ]

    monkeypatch.setattr(runner, "run_ingest", _ingest)

    report = runner.run_backfill(
        sample_config, "elexon", ["system_prices"], D, D + timedelta(days=1)
    )

    assert calls["transform"] == []
    assert not report.ok
    assert [(r.operation, r.status) for r in report.results] == [
        ("ingest", "completed_with_warnings"),
        ("transform", "failed"),
    ]
    assert "skipped units" in (report.results[1].error or "")


def test_on_chunk_reports_each_chunk_once(
    sample_config: GridflowConfig, calls: dict[str, list[tuple[datetime, datetime]]]
) -> None:
    seen: list[tuple[str, int, datetime, datetime]] = []

    runner.run_backfill(
        sample_config,
        "elexon",
        ["system_prices", "pn"],
        D,
        D + timedelta(days=2),
        on_chunk=lambda *args: seen.append(args),
    )

    one = timedelta(days=1)
    assert seen == [
        ("system_prices", 1, D, D + one),
        ("system_prices", 2, D + one, D + 2 * one),
        ("pn", 1, D, D + one),
        ("pn", 2, D + one, D + 2 * one),
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
        runner.run_backfill(
            sample_config, "elexon", ["system_prices"], D, D + timedelta(days=1), chunk_days=0
        )
