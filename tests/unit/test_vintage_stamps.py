"""Vendor-stamp, fallback-accounting, and transform-window contracts for L."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from pydantic import ValidationError

from gridflow.pipeline import runner
from gridflow.schemas.elexon import ElexonSystemPrice
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.schema_manifest import get_silver_schema_manifest
from gridflow.storage.paths import PathBuilder
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from pathlib import Path


def _system_rows(
    day: date,
    periods: list[int],
    publications: list[object] | None,
) -> pl.DataFrame:
    data: dict[str, list[object]] = {
        "settlementDate": [day.isoformat()] * len(periods),
        "settlementPeriod": periods,
        "systemSellPrice": [50.0 + period for period in periods],
        "systemBuyPrice": [60.0 + period for period in periods],
        "netImbalanceVolume": [100.0] * len(periods),
    }
    if publications is not None:
        data["createdDateTime"] = publications
    return pl.DataFrame(data)


def _schema_row(published_at: datetime | None) -> dict[str, object]:
    return {
        "settlement_date": date(2021, 1, 15),
        "settlement_period": 1,
        "timestamp_utc": datetime(2021, 1, 15, tzinfo=UTC),
        "system_sell_price": 50.0,
        "system_buy_price": 60.0,
        "net_imbalance_volume": 100.0,
        "published_at": published_at,
    }


def test_l_t01_vendor_stamp_wins_is_utc_and_persists(tmp_path: Path) -> None:
    day = date(2021, 1, 15)
    published = datetime(2021, 1, 15, 0, 52, tzinfo=UTC)
    capture = datetime(2026, 9, 6, tzinfo=UTC)
    partition = PathBuilder(tmp_path).bronze_date_dir("elexon", "system_prices", day)
    partition.mkdir(parents=True)
    body = partition / "raw_capture.json"
    body.write_text(
        json.dumps({"data": _system_rows(day, [1], [published.isoformat()]).to_dicts()}),
        encoding="utf-8",
    )
    body.with_suffix(".meta.json").write_text(
        json.dumps({"written_at": capture.isoformat()}), encoding="utf-8"
    )

    transformer = SystemPriceTransformer(tmp_path)
    assert transformer.run(day, run_id="l-t01", reingest=True) == 1
    files = list(PathBuilder(tmp_path).silver_dir("elexon", "system_prices").rglob("*.parquet"))
    assert len(files) == 1
    frame = pl.read_parquet(files[0])
    assert frame.schema["published_at"] == pl.Datetime("us", "UTC")
    assert frame["published_at"].to_list() == [published]
    assert frame["available_at"].to_list() == [published]
    assert frame["vintage_policy"].to_list() == ["vendor"]
    assert frame["dataset_version"].to_list() == ["2.0.0"]
    assert frame["system_sell_price"].to_list() == [51.0]
    assert frame["system_buy_price"].to_list() == [61.0]
    assert transformer.last_publication_fallback_count == 0
    assert (frame["published_at"] >= frame["event_time"]).all()


def test_l_t01_publication_normalization_boundaries_and_bad_values(tmp_path: Path) -> None:
    transformer = SystemPriceTransformer(tmp_path)
    day = date(2021, 1, 15)
    earlier_than_policy = "2021-01-15T01:05:00+01:00"
    later_than_policy = "2021-01-15T02:00:00Z"
    frame = transformer.transform(
        _system_rows(day, [1, 2], [earlier_than_policy, later_than_policy])
    )
    stamped = transformer._add_bitemporal_columns(
        frame, day, "l-t01", datetime(2026, 1, 1, tzinfo=UTC)
    )
    assert stamped["published_at"].to_list() == [
        datetime(2021, 1, 15, 0, 5, tzinfo=UTC),
        datetime(2021, 1, 15, 2, tzinfo=UTC),
    ]
    assert stamped["available_at"].to_list() == stamped["published_at"].to_list()
    assert stamped["vintage_policy"].to_list() == ["vendor", "vendor"]
    assert (stamped["published_at"] >= stamped["event_time"]).all()

    for bad in ("2021-01-15T00:52:00", "not-a-timestamp"):
        with pytest.raises((ValueError, pl.exceptions.ComputeError)):
            transformer.transform(_system_rows(day, [1], [bad]))

    for dst_day, period in ((date(2025, 3, 30), 46), (date(2025, 10, 26), 50)):
        event = settlement_period_to_utc(dst_day, period)
        result = transformer.transform(
            _system_rows(dst_day, [period], [(event + timedelta(minutes=5)).isoformat()])
        )
        assert result["published_at"].to_list() == [event + timedelta(minutes=5)]


def test_l_t01_schema_requires_utc_publication() -> None:
    accepted = ElexonSystemPrice(**_schema_row(datetime(2021, 1, 15, tzinfo=ZoneInfo("UTC"))))
    assert accepted.published_at is not None
    for invalid in (
        datetime(2021, 1, 15),
        datetime(2021, 1, 15, tzinfo=ZoneInfo("Europe/London")),
    ):
        # Use a summer instant for the named zone so its offset is non-zero.
        value = invalid.replace(month=7) if invalid.tzinfo is not None else invalid
        with pytest.raises(ValidationError, match="published_at"):
            ElexonSystemPrice(**_schema_row(value))


def test_l_t02_publication_fallback_accumulates_and_run_resets(tmp_path: Path) -> None:
    transformer = SystemPriceTransformer(tmp_path)
    day = date(2021, 1, 15)
    absent = transformer.transform(_system_rows(day, [1], None))
    mixed = transformer.transform(_system_rows(day, [2, 3], [None, "2021-01-15T02:00:00Z"]))
    assert absent["published_at"].null_count() == 1
    assert mixed["published_at"].null_count() == 1
    assert transformer.last_publication_fallback_count == 2

    transformer.last_publication_fallback_count = 99
    assert transformer.run(day + timedelta(days=1), run_id="empty") == 0
    assert transformer.last_publication_fallback_count == 0


class _WindowProbe(BaseSilverTransformer):
    source = "test"
    dataset = "window"
    PARTITION_DATE_COLUMN: ClassVar[str | None] = "settlement_date"
    PARTITION_SOURCE_OFFSETS: ClassVar[tuple[int, ...]] = (1, -1, 0)

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df

    def run(self, target_date: date, run_id: str | None = None, reingest: bool = False) -> int:
        self.last_publication_fallback_count = 1
        return 1


def _run_probe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    start: datetime,
    end: datetime,
    probe: BaseSilverTransformer,
) -> runner.DatasetResult:
    tracker = SimpleNamespace(
        run_id="probe",
        complete=lambda **_kwargs: None,
        complete_with_warnings=lambda **_kwargs: None,
        fail=lambda _message: None,
    )
    monkeypatch.setattr("gridflow.silver.registry.get_transformer", lambda *_args: probe)
    monkeypatch.setattr("gridflow.observability.PipelineRunTracker", lambda *_args: tracker)
    settings = SimpleNamespace(pipeline=SimpleNamespace(data_dir=tmp_path, write_silver_csv=False))
    return runner.run_transform(
        runner.PipelineContext(con=None, settings=settings),
        "test",
        ["window"],
        start,
        end,
    )[0]


def test_l_t02_runner_surfaces_retained_fallback_without_skipped_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    instant = datetime(2026, 9, 7, tzinfo=UTC)
    result = _run_probe(monkeypatch, tmp_path, instant, instant, _WindowProbe(tmp_path))
    assert result.status == "completed_with_warnings"
    assert result.rows_publication_fallback == 1
    assert result.rows_invalid == result.rows_skipped == 0


def test_l_t02_partial_failure_preserves_aggregated_fallbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = _WindowProbe(tmp_path)
    calls = 0

    def partial_run(target_date: date, run_id: str | None = None, reingest: bool = False) -> int:
        nonlocal calls
        calls += 1
        probe.last_publication_fallback_count = calls
        if calls == 2:
            raise RuntimeError("partial publication probe")
        return 1

    probe.run = partial_run  # type: ignore[method-assign]
    start = datetime(2026, 9, 6, tzinfo=UTC)
    result = _run_probe(monkeypatch, tmp_path, start, start + timedelta(days=1), probe)
    assert result.status == "failed"
    assert result.rows_publication_fallback == 3
    assert result.rows_skipped == result.rows_invalid == 0


def test_l_t02_cli_displays_fallback_and_window_counts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from gridflow.cli import _echo_transform_results

    result = runner.DatasetResult(
        "elexon",
        "system_prices",
        "transform",
        "completed_with_warnings",
        rows_out=2,
        rows_publication_fallback=2,
        partition_retouch_warnings=1,
    )
    _echo_transform_results("elexon", [result])
    output = capsys.readouterr().out
    assert "2 publication fallback" in output
    assert "1 re-touch window warning" in output


@pytest.mark.parametrize(
    "start,end,offsets,declaring,expected",
    [
        # Explicit expected heuristic case: bounded historical single-date windows warn by design.
        ("2026-09-07T00:00Z", "2026-09-07T00:00Z", (1, -1, 0), True, 1),
        ("2026-09-06T00:00Z", "2026-09-07T00:00Z", (1, -1, 0), True, 0),
        ("2026-09-06T23:30Z", "2026-09-07T00:30Z", (1,), True, 0),
        ("2026-09-06T00:00Z", "2026-09-07T00:00Z", (2, 0), True, 1),
        ("2026-09-07T00:00Z", "2026-09-07T00:00Z", (-1, 0), True, 0),
        ("2026-09-07T00:00Z", "2026-09-07T00:00Z", (2,), False, 0),
    ],
)
def test_l_t05_positive_offset_window_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    start: str,
    end: str,
    offsets: tuple[int, ...],
    declaring: bool,
    expected: int,
) -> None:
    probe = _WindowProbe(tmp_path)
    probe.PARTITION_SOURCE_OFFSETS = offsets
    probe.PARTITION_DATE_COLUMN = "settlement_date" if declaring else None
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    result = _run_probe(monkeypatch, tmp_path, start_dt, end_dt, probe)
    assert result.partition_retouch_warnings == expected
    assert result.rows_skipped == 0
    messages = [
        record.message for record in caplog.records if "re-touch heuristic" in record.message
    ]
    assert len(messages) == expected
    if expected:
        assert "Historical single-date windows warn by design" in messages[0]


def test_l_t03_mid_policy_literals_boundaries_and_dst(tmp_path: Path) -> None:
    transformer = MIDTransformer(tmp_path)
    policy = transformer.VINTAGE_POLICY
    assert policy is not None
    assert policy.name == "elexon-mid/vp-2026-09b"
    assert policy.lag == timedelta(minutes=35)
    assert policy.dated == date(2026, 9, 7)
    assert policy.applies_before == datetime(2026, 8, 1, tzinfo=UTC)
    assert transformer.DATASET_VERSION == "1.1.0"
    before = policy.applies_before - timedelta(microseconds=1)
    capture = before + policy.lag
    events = [before, policy.applies_before, before, before]
    captures = [
        capture + timedelta(microseconds=1),
        capture,
        capture,
        capture - timedelta(microseconds=1),
    ]
    result = transformer._add_bitemporal_columns(
        pl.DataFrame({"timestamp_utc": events, "capture": captures}),
        before.date(),
        "l-t03",
        capture,
        vintage_column="capture",
    )
    assert result["vintage_policy"].to_list() == [
        policy.name,
        "ingest-clock",
        "ingest-clock",
        "ingest-clock",
    ]

    for dst_day, period in ((date(2025, 3, 30), 46), (date(2025, 10, 26), 50)):
        raw = pl.DataFrame(
            {
                "settlementDate": [dst_day.isoformat()],
                "settlementPeriod": [period],
                "dataProvider": ["APXMIDP"],
                "price": [50.0],
                "volume": [100.0],
            }
        )
        transformed = transformer.transform(raw)
        event = settlement_period_to_utc(dst_day, period)
        assert transformed["timestamp_utc"].to_list() == [event]
        stamped = transformer._add_bitemporal_columns(
            transformed, dst_day, "l-t03", datetime(2026, 9, 7, tzinfo=UTC)
        )
        assert stamped["available_at"].to_list() == [event + timedelta(minutes=35)]


def test_l_t04_manifest_exposes_system_price_publication() -> None:
    entries = [
        entry
        for entry in get_silver_schema_manifest()
        if entry.source == "elexon" and entry.dataset == "system_prices"
    ]
    assert {entry.relation_kind for entry in entries} == {"silver", "serving_alias"}
    assert all("published_at" in (entry.columns or ()) for entry in entries)
