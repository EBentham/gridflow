"""Exercise real backfill transforms and capture preservation using temporary data."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl
import pytest

from gridflow.pipeline import runner
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.storage.paths import PathBuilder
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.config.settings import GridflowConfig

pytestmark = pytest.mark.integration


def _write_fixture(
    root: Path,
    dataset: str,
    day: date,
    capture: datetime,
    *,
    price: float = 50.0,
    period: int = 1,
) -> None:
    """Create a new mock response and sidecar without accessing external data."""
    partition = PathBuilder(root).bronze_date_dir("elexon", dataset, day)
    partition.mkdir(parents=True, exist_ok=True)
    record: dict[str, object] = {
        "settlementDate": day.isoformat(),
        "settlementPeriod": period,
    }
    if dataset == "mid":
        record.update(dataProvider="APXMIDP", price=price, volume=100.0)
    else:
        record.update(systemSellPrice=price, systemBuyPrice=price, netImbalanceVolume=100.0)
    stem = f"raw_{capture.strftime('%Y%m%dT%H%M%S')}"
    (partition / f"{stem}.json").write_text(json.dumps({"data": [record]}), encoding="utf-8")
    (partition / f"{stem}.meta.json").write_text(
        json.dumps(
            {
                "source": "elexon",
                "dataset": dataset,
                "written_at": capture.isoformat(),
                "data_date": day.isoformat(),
            }
        ),
        encoding="utf-8",
    )


@pytest.mark.parametrize("dataset", ["mid", "system_prices"])
def test_run_backfill_reconstructs_availability(
    sample_config: GridflowConfig, monkeypatch: pytest.MonkeyPatch, dataset: str
) -> None:
    """Run real backfill transforms and silver views against mocked ingest."""
    calls: list[datetime] = []
    # Cross-source gold inputs are absent from this temporary Elexon-only catalogue.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    def ingest_fixture(
        ctx: runner.PipelineContext,
        source: str,
        datasets: list[str],
        start_dt: datetime,
        end_dt: datetime,
        *,
        incremental: bool,
        write_watermark: bool,
    ) -> list[runner.DatasetResult]:
        assert source == "elexon"
        assert datasets == [dataset]
        assert not incremental and not write_watermark
        assert end_dt - start_dt == timedelta(days=1)
        calls.append(start_dt)
        _write_fixture(
            ctx.settings.pipeline.data_dir,
            dataset,
            start_dt.date(),
            datetime(2026, 9, 6, tzinfo=UTC),
        )
        return [
            runner.DatasetResult(
                source=source, dataset=dataset, operation="ingest", status="success"
            )
        ]

    monkeypatch.setattr(runner, "run_ingest", ingest_fixture)
    start = datetime(2021, 1, 15, tzinfo=UTC)
    report = runner.run_backfill(
        sample_config, "elexon", [dataset], start, start + timedelta(days=2)
    )
    assert calls == [start, start + timedelta(days=1)]
    transforms = [item for item in report.results if item.operation == "transform"]
    assert len(transforms) == 2
    assert all(item.status in {"success", "completed_with_warnings"} for item in transforms)
    assert sum(item.rows_out for item in transforms) == 2
    frame = pl.read_parquet(
        PathBuilder(sample_config.pipeline.data_dir).silver_glob_pattern("elexon", dataset)
    ).sort("event_time")
    lag = timedelta(minutes=60 if dataset == "mid" else 90)
    assert frame["available_at"].to_list() == [event + lag for event in calls]
    assert frame["vintage_policy"].to_list() == [f"elexon-{dataset}/vp-2026-09"] * 2


@pytest.mark.parametrize("reingest", [False, True])
@pytest.mark.parametrize("before_cutover", [False, True])
def test_system_prices_preserves_capture_files_and_live_pit(
    tmp_path: Path, reingest: bool, before_cutover: bool
) -> None:
    day = date(2026, 7, 30) if before_cutover else date(2026, 7, 31)
    # July settlement dates are BST: period 3 starts at the UTC cutover.
    period = 1 if before_cutover else 3
    captures = [datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 9, 1, tzinfo=UTC)]
    for capture, price in zip(captures, [50.0, 75.0], strict=True):
        _write_fixture(tmp_path, "system_prices", day, capture, price=price, period=period)
    transformer = SystemPriceTransformer(tmp_path)
    assert transformer.run(day, run_id="test", reingest=reingest) == 2
    paths = PathBuilder(tmp_path)
    files = sorted(paths.silver_dir("elexon", "system_prices").rglob("*.parquet"))
    assert len(files) == 2
    original_names = [path.name for path in files]
    frame = pl.read_parquet(files).sort("system_sell_price")
    assert frame["run_type"].null_count() == 2
    if before_cutover:
        event = settlement_period_to_utc(day, period)
        assert frame["available_at"].to_list() == [event + timedelta(minutes=90)] * 2
        assert frame["vintage_policy"].to_list() == ["elexon-system_prices/vp-2026-09"] * 2
    else:
        assert frame["available_at"].to_list() == captures
        assert frame["vintage_policy"].to_list() == ["ingest-clock"] * 2
        visible = frame.filter(pl.col("available_at") <= datetime(2026, 8, 15, tzinfo=UTC))
        assert visible["system_sell_price"].to_list() == [50.0]
    transformer.run(day, run_id="rerun", reingest=reingest)
    assert (
        sorted(path.name for path in paths.silver_dir("elexon", "system_prices").rglob("*.parquet"))
        == original_names
    )


@pytest.mark.parametrize("reingest", [False, True])
def test_mid_modes_reconstruct_old_events(tmp_path: Path, reingest: bool) -> None:
    day = date(2021, 1, 15)
    _write_fixture(tmp_path, "mid", day, datetime(2026, 9, 6, tzinfo=UTC))
    assert MIDTransformer(tmp_path).run(day, run_id="test", reingest=reingest) == 1
    frame = pl.read_parquet(PathBuilder(tmp_path).silver_file("elexon", "mid", day))
    assert frame["available_at"].to_list() == [datetime(2021, 1, 15, 1, tzinfo=UTC)]
    assert frame["vintage_policy"].to_list() == ["elexon-mid/vp-2026-09"]
