"""P1.1 acceptance: a historical ``as_of`` fetch returns rows for covered spans.

ROADMAP `.planning/review-2026-06/ROADMAP.md:38`. Pre-coalesce (before ADR-025 §3),
``available_at`` was always the recent ingest clock, so the models-side barrier
``available_at <= as_of`` excluded every row for any historical ``as_of`` and the
fetch returned an empty frame (RESEARCH §7.1). This proves the coalesce fixes that
end-to-end, using FABRICATED PAST vintages — the real on-disk windfor carries only
2026-07 vintages and so cannot demonstrate a historical cut.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import polars as pl

from gridflow.silver.elexon.wind_forecast import WindForecastTransformer
from gridflow.storage.parquet import read_parquet

if TYPE_CHECKING:
    from pathlib import Path

TARGET_DATE = date(2024, 1, 15)
RUN_ID = "test-run-id"
VINTAGE_0800 = datetime(2024, 1, 14, 8, 0, tzinfo=UTC)
VINTAGE_1200 = datetime(2024, 1, 14, 12, 0, tzinfo=UTC)


def _write_windfor_bronze(data_dir: Path, records: list[dict]) -> None:
    bronze_dir = (
        data_dir
        / "bronze"
        / "elexon"
        / "windfor"
        / str(TARGET_DATE.year)
        / f"{TARGET_DATE.month:02d}"
        / f"{TARGET_DATE.day:02d}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps({"data": records}))


def test_historical_as_of_fetch_returns_rows(tmp_data_dir: Path) -> None:
    # Two forecast issues for the SAME delivery period, both published in the PAST.
    _write_windfor_bronze(
        tmp_data_dir,
        [
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "initialForecast": 4500.0,
                "latestForecast": 4400.0,
                "publishDateTime": "2024-01-14T08:00:00Z",
            },
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "initialForecast": 4500.0,
                "latestForecast": 4300.0,
                "publishDateTime": "2024-01-14T12:00:00Z",
            },
        ],
    )

    WindForecastTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    df = read_parquet(tmp_data_dir / "silver" / "elexon" / "windfor" / "**" / "*.parquet")

    # Both vintages survive (dedup key includes published_at), and available_at now
    # carries the publication vintage — a genuine spread, not the ingest collapse.
    assert df.height == 2
    assert df["available_at"].null_count() == 0
    assert set(df["available_at"].to_list()) == {VINTAGE_0800, VINTAGE_1200}
    assert df["available_at"].to_list() == df["published_at"].to_list()

    # The models point-in-time barrier, applied here as a Polars filter:
    # a historical as_of BETWEEN the two issues must return the earlier one only.
    as_of_between = datetime(2024, 1, 14, 10, 0, tzinfo=UTC)
    visible = df.filter(pl.col("available_at") <= as_of_between)
    assert visible.height > 0, "historical as_of fetch must return rows (P1.1 acceptance)"
    assert set(visible["available_at"].to_list()) == {VINTAGE_0800}

    # Before ANY publication, nothing is knowable — the leakage barrier holds.
    before_any = datetime(2024, 1, 13, tzinfo=UTC)
    assert df.filter(pl.col("available_at") <= before_any).is_empty()
