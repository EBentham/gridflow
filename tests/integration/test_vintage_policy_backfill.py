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
    published: datetime | None = None,
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
        if published is not None:
            record["createdDateTime"] = published.isoformat()
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
    lag = timedelta(minutes=35 if dataset == "mid" else 90)
    assert frame["available_at"].to_list() == [event + lag for event in calls]
    policy_name = (
        "elexon-mid/vp-2026-09b" if dataset == "mid" else "elexon-system_prices/vp-2026-09"
    )
    assert frame["vintage_policy"].to_list() == [policy_name] * 2


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
    assert transformer.last_publication_fallback_count == 2
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
    assert transformer.last_publication_fallback_count == 2
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
    assert frame["available_at"].to_list() == [datetime(2021, 1, 15, 0, 35, tzinfo=UTC)]
    assert frame["vintage_policy"].to_list() == ["elexon-mid/vp-2026-09b"]


def test_l_t01_l_t04_vendor_stamp_persists_and_serving_handles_legacy_union(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from gridflow.serving.client import GridflowClient
    from gridflow.storage.duckdb import init_catalogue

    day = date(2021, 1, 15)
    event = settlement_period_to_utc(day, 1)
    published = event + timedelta(minutes=52)
    capture = datetime(2026, 9, 6, tzinfo=UTC)
    _write_fixture(
        tmp_path,
        "system_prices",
        day,
        capture,
        published=published,
    )
    transformer = SystemPriceTransformer(tmp_path)
    assert transformer.run(day, run_id="l-serving", reingest=True) == 1
    paths = PathBuilder(tmp_path)
    files = list(paths.silver_dir("elexon", "system_prices").rglob("*.parquet"))
    assert len(files) == 1
    new_frame = pl.read_parquet(files[0])
    legacy = new_frame.drop("published_at").with_columns(
        pl.lit(published - timedelta(minutes=1))
        .cast(pl.Datetime("us", "UTC"))
        .alias("available_at"),
        pl.lit(49.0).alias("system_sell_price"),
    )
    legacy.write_parquet(files[0].parent / "legacy.parquet")

    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda _con: None)
    db_path = tmp_path / "catalogue.duckdb"
    init_catalogue(db_path, tmp_path)
    client = GridflowClient(db_path=db_path)
    try:
        served = client.get_system_prices(day, day)
        # Production visibility uses available_at as the primary barrier. With
        # zero fallback rows, published_at is an expected coincident cross-check.
        con = client._require_con()
        at_stamp = con.execute(
            "SELECT count(*) FROM silver_elexon_system_prices "
            "WHERE available_at <= ? AND settlement_date = ?",
            [published, day],
        ).fetchone()
        just_before = con.execute(
            "SELECT count(*) FROM silver_elexon_system_prices "
            "WHERE available_at <= ? AND settlement_date = ?",
            [published - timedelta(microseconds=1), day],
        ).fetchone()
    finally:
        client.close()

    assert served["published_at"].to_list() == [published]
    assert served["available_at"].to_list() == [published]
    assert served["vintage_policy"].to_list() == ["vendor"]
    assert at_stamp is not None and at_stamp[0] == 2
    assert just_before is not None and just_before[0] == 1
