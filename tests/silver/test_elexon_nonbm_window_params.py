"""NONBM bronze fetched with window parameters Elexon ignores never reaches silver.

``/datasets/NONBM`` filters on ``from``/``to``. Sent ``publishDateTimeFrom``/
``publishDateTimeTo`` instead, it ignores the window and returns its latest
record (1 Apr 2026 SP22) for every request, which silver then copied into each
August partition (backlog 12b; live A/B on the 1 Aug window, 2026-10-16).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.silver.elexon.nonbm import NONBMTransformer
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Callable

AUG_1 = date(2026, 8, 1)
WINDOW_START = datetime(2026, 8, 1, tzinfo=UTC)
WINDOW_END = datetime(2026, 8, 2, tzinfo=UTC)

# The exact record every publishDateTimeFrom/To request returned (bronze, 2026-08-16).
STALE_LATEST_RECORD: dict[str, object] = {
    "dataset": "NONBM",
    "publishTime": "2026-04-01T10:04:00Z",
    "startTime": "2026-04-01T09:30:00Z",
    "settlementDate": "2026-04-01",
    "settlementPeriod": 22,
    "generation": 0,
}


def _in_window_record(period: int) -> dict[str, object]:
    return {
        "dataset": "NONBM",
        "publishTime": "2026-08-01T10:04:00Z",
        "startTime": "2026-08-01T09:30:00Z",
        "settlementDate": "2026-08-01",
        "settlementPeriod": period,
        "generation": 12.5,
    }


def _write_body(
    tmp_path: Path,
    records: list[dict[str, object]],
    *,
    from_param: str,
    to_param: str,
) -> None:
    """Write one bronze raw+sidecar pair shaped like the real Elexon connector."""
    response = RawResponse(
        body=json.dumps({"data": records}).encode(),
        content_type="application/json",
        source="elexon",
        dataset="nonbm",
        request_url="https://data.elexon.co.uk/bmrs/api/v1/datasets/NONBM",
        request_params={
            from_param: WINDOW_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
            to_param: WINDOW_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "page": 1,
        },
        api_version="v1",
        page=1,
        total_pages=1,
        http_status=200,
        data_date=AUG_1,
    )
    BronzeWriter(tmp_path).write(response)


def test_body_fetched_with_ignored_params_is_excluded_and_counted(tmp_path: Path) -> None:
    _write_body(
        tmp_path,
        [STALE_LATEST_RECORD],
        from_param="publishDateTimeFrom",
        to_param="publishDateTimeTo",
    )

    transformer = NONBMTransformer(tmp_path)
    rows = transformer.run(AUG_1, run_id="nonbm-ignored")

    assert rows == 0
    assert transformer.last_excluded_row_count == 1
    assert not PathBuilder(tmp_path).silver_file("elexon", "nonbm", AUG_1).exists()


def test_from_to_body_survives_beside_an_ignored_params_body(tmp_path: Path) -> None:
    # After a corrected re-ingest the old bodies stay in bronze (immutable)
    # beside the new ones; only the from/to body's rows may reach silver.
    _write_body(
        tmp_path,
        [STALE_LATEST_RECORD],
        from_param="publishDateTimeFrom",
        to_param="publishDateTimeTo",
    )
    _write_body(
        tmp_path,
        [_in_window_record(23), _in_window_record(24)],
        from_param="from",
        to_param="to",
    )

    transformer = NONBMTransformer(tmp_path)
    rows = transformer.run(AUG_1, run_id="nonbm-mixed")

    assert rows == 2
    assert transformer.last_excluded_row_count == 1
    silver = pl.read_parquet(PathBuilder(tmp_path).silver_file("elexon", "nonbm", AUG_1))
    assert silver["settlement_date"].unique().to_list() == [AUG_1]
    assert sorted(silver["settlement_period"].to_list()) == [23, 24]
    assert silver["generation_mw"].to_list() == [12.5, 12.5]


def _sidecars(tmp_path: Path) -> list[Path]:
    partition = PathBuilder(tmp_path).bronze_date_dir("elexon", "nonbm", AUG_1)
    return sorted(partition.glob("raw_*.meta.json"))


def _strip_window(sidecar: Path) -> None:
    meta = json.loads(sidecar.read_text(encoding="utf-8"))
    meta["request_params"] = {"page": 1}
    sidecar.write_text(json.dumps(meta), encoding="utf-8")


@pytest.mark.parametrize(
    ("damage", "reason"),
    [
        (Path.unlink, "sidecar cannot be read"),
        (lambda p: p.write_bytes(b"\xff\xfe{not utf-8"), "sidecar cannot be read"),
        (lambda p: p.write_text("{truncated", encoding="utf-8"), "sidecar cannot be read"),
        (_strip_window, "no from/to window"),
    ],
    ids=["missing", "not-utf8", "bad-json", "no-window"],
)
def test_body_without_proof_of_a_from_to_window_is_excluded(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    damage: Callable[[Path], object],
    reason: str,
) -> None:
    # Fail closed: the stale latest record looks like valid data, so a body
    # whose sidecar cannot show a from/to fetch must not reach silver.
    _write_body(tmp_path, [STALE_LATEST_RECORD], from_param="from", to_param="to")
    (sidecar,) = _sidecars(tmp_path)
    damage(sidecar)

    transformer = NONBMTransformer(tmp_path)
    with caplog.at_level(logging.WARNING, logger="gridflow.silver.elexon.nonbm"):
        rows = transformer.run(AUG_1, run_id="nonbm-unproven")

    assert rows == 0
    assert transformer.last_excluded_row_count == 1
    assert reason in caplog.text
    assert not PathBuilder(tmp_path).silver_file("elexon", "nonbm", AUG_1).exists()
