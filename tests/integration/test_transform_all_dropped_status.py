"""FIX 2 follow-up (Sol re-review, 2026-07-26): a 100%-out-of-window
event-window drop must surface as a HARD FAILURE through
``runner.run_transform()``, not merely as a log line.

Before this fix, ``_apply_event_window_filter``'s ``all_dropped`` branch
logged ``ERROR`` and returned an empty frame; ``_process_frame`` skipped the
write (already correct -- no silver written); but ``run_transform`` only
ever inspected ``last_unmapped_count``/``last_validation_failure_count`` to
decide between ``"success"``/``"completed_with_warnings"``, never the
partition-filter counters -- so a wholly out-of-window transform (e.g. a
horizon dataset mistakenly opted into ``EVENT_WINDOW_FILTER``) was reported
``status="success"`` with the command exiting 0, exactly as if nothing had
gone wrong.

Deliberately END-TO-END: drives the real ``pipeline.runner.run_transform``
against a real (tmp) DuckDB connection, mirroring
``test_bitemporal_run_id.py::test_script_silver_step_threads_run_id_and_reingest``
-- a unit-level assertion on ``transformer.last_partition_filter_all_dropped_count``
would not prove the STATUS actually changes.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import gridflow.silver.entsoe  # noqa: F401 -- registers every entsoe transformer
from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.endpoints import ENTSOE_DT_FORMAT
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

TARGET_DATE = date(2024, 1, 16)
WINDOW_START = datetime(2024, 1, 16, tzinfo=UTC)
WINDOW_END = datetime(2024, 1, 17, tzinfo=UTC)


def _points_xml(period_start: datetime, hours: int) -> bytes:
    """Minimal A44 Publication_MarketDocument with ``hours`` sequential PT60M points."""
    period_end = period_start + (hours * timedelta(hours=1))
    start_s = period_start.strftime("%Y-%m-%dT%H:%MZ")
    end_s = period_end.strftime("%Y-%m-%dT%H:%MZ")
    points = "".join(
        f"<Point><position>{i}</position><price.amount>{40.0 + i}</price.amount></Point>"
        for i in range(1, hours + 1)
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <mRID>d3d-{start_s}</mRID>
  <revisionNumber>1</revisionNumber>
  <type>A44</type>
  <createdDateTime>{start_s}</createdDateTime>
  <period.timeInterval>
    <start>{start_s}</start>
    <end>{end_s}</end>
  </period.timeInterval>
  <TimeSeries>
    <mRID>1</mRID>
    <businessType>A62</businessType>
    <in_Domain.mRID codingScheme="A01">10YGB----------A</in_Domain.mRID>
    <out_Domain.mRID codingScheme="A01">10YGB----------A</out_Domain.mRID>
    <currency_Unit.name>EUR</currency_Unit.name>
    <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
    <curveType>A01</curveType>
    <Period>
      <timeInterval>
        <start>{start_s}</start>
        <end>{end_s}</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      {points}
    </Period>
  </TimeSeries>
</Publication_MarketDocument>""".encode()


def _write_wholly_out_of_window_chunk(tmp_path: Path) -> None:
    """Seed day_ahead_prices bronze for TARGET_DATE whose own recorded
    [periodStart, periodEnd) window is WINDOW_START/WINDOW_END, but every
    point in the body sits many days BELOW window_start -- a 100% drop under
    exclude_out_of_window, the exact misclassification signature D-5's
    all_dropped exists to surface (e.g. a horizon/annual dataset wrongly
    opted into EVENT_WINDOW_FILTER, or a vendor response that over-spans on
    one side entirely)."""
    own_start = datetime(2024, 1, 10, 0, tzinfo=UTC)
    response = RawResponse(
        body=_points_xml(own_start, 24),
        content_type="text/xml",
        source="entsoe",
        dataset="day_ahead_prices",
        request_url="https://web-api.tp.entsoe.eu/api",
        request_params={
            "periodStart": WINDOW_START.strftime(ENTSOE_DT_FORMAT),
            "periodEnd": WINDOW_END.strftime(ENTSOE_DT_FORMAT),
        },
        page=1,
        total_pages=1,
        http_status=200,
        data_date=TARGET_DATE,
    )
    BronzeWriter(tmp_path).write(response)


def _isolated_env(tmp_path: Path, monkeypatch) -> Path:
    data_dir = tmp_path / "data"
    db_path = tmp_path / "catalogue" / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return db_path


def test_wholly_out_of_window_transform_surfaces_as_failed_not_success(
    tmp_path: Path, monkeypatch
) -> None:
    """RED against pre-fix code: status was 'success', rows_out == 0, exit 0
    -- indistinguishable from a genuinely clean (empty) date. GREEN after the
    fix: status == 'failed', pipeline_runs.status == 'failed', no silver
    file written, and DatasetResult.ok is False."""
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage.duckdb import get_connection, init_catalogue

    db_path = _isolated_env(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    _write_wholly_out_of_window_chunk(data_dir)

    settings = load_settings()
    pipeline_runner.import_transformers()
    init_catalogue(db_path, data_dir)
    con = get_connection(db_path)
    try:
        start_dt = datetime(TARGET_DATE.year, TARGET_DATE.month, TARGET_DATE.day, tzinfo=UTC)
        ctx = pipeline_runner.PipelineContext(con=con, settings=settings)
        results = pipeline_runner.run_transform(
            ctx, "entsoe", ["day_ahead_prices"], start_dt, start_dt
        )
    finally:
        con.close()

    assert len(results) == 1
    result = results[0]
    assert result.status == "failed", (
        f"a 100%-out-of-window transform must hard-fail, not report "
        f"'{result.status}' -- silent success on a misclassified opt-in "
        f"leaves the operator with no signal at all: {result.error}"
    )
    assert result.ok is False
    assert result.rows_out == 0
    assert result.error is not None
    assert "day_ahead_prices" in result.error

    con = get_connection(db_path, read_only=True)
    try:
        rows = con.execute(
            "SELECT status FROM pipeline_runs WHERE source = 'entsoe' "
            "AND dataset = 'day_ahead_prices' AND operation = 'transform'"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("failed",)]

    silver_dir = PathBuilder(data_dir).silver_dir("entsoe", "day_ahead_prices")
    written = list(silver_dir.rglob("*.parquet")) if silver_dir.exists() else []
    assert written == [], f"no silver should be written for the 100%-dropped date: {written}"
