"""ENTSO-E event-window filter — classification + D-3d durability tests (R2-A Task 4).

Covers A-6 (classification completeness), the transform-backed replacement
for the ineffective rev-2 ``_event_time_column`` assertion, and A-16's D-3d
per-instant lower-bound durability proof (the S4-1 mixed-ownership
regression, demonstrated RED against both an unconditional-trim and a
per-date ``frozenset[date]`` implementation via the git history this plan
records, not re-derived here).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import gridflow.silver.entsoe  # noqa: F401 -- registers every entsoe transformer
from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.endpoints import ENTSOE_DT_FORMAT
from gridflow.silver.entsoe._event_window import EVENT_WINDOW_FILTER_EXEMPT
from gridflow.silver.entsoe.actual_generation import ActualGenerationTransformer
from gridflow.silver.entsoe.actual_generation_units import ActualGenerationUnitsTransformer
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer
from gridflow.silver.entsoe.generation_forecast import GenerationForecastTransformer
from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.entsoe.wind_solar_forecast import WindSolarForecastTransformer
from gridflow.silver.registry import get_transformer_class, list_transformers

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"

OPTED_IN_DATASETS = (
    "day_ahead_prices",
    "actual_load",
    "load_forecast",
    "actual_generation",
    "actual_generation_units",
    "wind_solar_forecast",
    "generation_forecast",
)

HORIZON_EXEMPT_DATASETS = (
    "load_forecast_weekly",
    "load_forecast_monthly",
    "load_forecast_yearly",
    "forecast_margin",
    "installed_capacity",
    "installed_capacity_units",
    "water_reservoirs",
    "outages_generation",
    "outages_consumption",
    "outages_transmission",
    "outages_offshore_grid",
    "outages_production",
    "generation_units_master_data",
)


# ---------------------------------------------------------------------------
# A-6 — classification completeness
# ---------------------------------------------------------------------------


class TestClassificationCompleteness:
    def test_every_entsoe_transformer_is_classified(self) -> None:
        registered = list_transformers("entsoe")
        assert registered, "entsoe transformers must be registered before this test runs"
        for source, dataset in registered:
            cls = get_transformer_class(source, dataset)
            assert cls is not None
            opted_in = cls.EVENT_WINDOW_FILTER
            exempt = dataset in EVENT_WINDOW_FILTER_EXEMPT
            assert opted_in or exempt, f"{dataset} is neither opted in nor exempt with a reason"
            assert not (opted_in and exempt), f"{dataset} is both opted in and exempt"

    def test_opted_in_set_matches_the_plan(self) -> None:
        for source, dataset in list_transformers("entsoe"):
            cls = get_transformer_class(source, dataset)
            assert cls is not None
            assert (dataset in OPTED_IN_DATASETS) == cls.EVENT_WINDOW_FILTER, (
                f"{dataset}: EVENT_WINDOW_FILTER does not match the plan's 7-dataset opt-in set"
            )


class TestHorizonDatasetsNeverFiltered:
    def test_horizon_datasets_are_never_filtered(self) -> None:
        for dataset in HORIZON_EXEMPT_DATASETS:
            cls = get_transformer_class("entsoe", dataset)
            assert cls is not None, f"{dataset} must be registered"
            assert cls.EVENT_WINDOW_FILTER is False
            assert dataset in EVENT_WINDOW_FILTER_EXEMPT


class TestOptedInTransformersProduceFilterableTimestamp:
    """Transform-backed replacement for the ineffective rev-2
    ``_event_time_column`` assertion (``base.py``'s ``_event_time_column``
    returns the literal ``"timestamp_utc"`` unconditionally, overridden
    nowhere) -- actually runs ``read_bronze()`` + ``transform()`` against a
    real fixture and asserts ``timestamp_utc`` is present and populated.
    """

    @pytest.mark.parametrize(
        ("dataset", "cls", "fixture"),
        [
            ("day_ahead_prices", DayAheadPricesTransformer, "day_ahead_prices_gb.xml"),
            ("actual_load", ActualLoadTransformer, "actual_load_gb.xml"),
            ("load_forecast", LoadForecastTransformer, "load_forecast_gb.xml"),
            ("actual_generation", ActualGenerationTransformer, "actual_generation_gb.xml"),
            (
                "actual_generation_units",
                ActualGenerationUnitsTransformer,
                "actual_generation_units_gb.xml",
            ),
            ("wind_solar_forecast", WindSolarForecastTransformer, "wind_solar_forecast_gb.xml"),
            ("generation_forecast", GenerationForecastTransformer, "generation_forecast_gb.xml"),
        ],
    )
    def test_opted_in_transformers_produce_a_filterable_timestamp_utc(
        self, tmp_path: Path, dataset: str, cls: type, fixture: str
    ) -> None:
        target_date = date(2024, 1, 15)
        partition_dir = tmp_path / "bronze" / "entsoe" / dataset / "2024" / "01" / "15"
        partition_dir.mkdir(parents=True)
        (partition_dir / "raw_test.xml").write_bytes((FIXTURES / fixture).read_bytes())

        transformer = cls(tmp_path)
        raw_df = transformer.read_bronze(target_date)
        assert not raw_df.is_empty(), f"{dataset} fixture produced no raw rows"
        clean_df = transformer.transform(raw_df)
        assert "timestamp_utc" in clean_df.columns
        assert not clean_df.is_empty()


# ---------------------------------------------------------------------------
# A-16 — D-3d ENTSO-E lower-bound trim, per-instant ownership
# ---------------------------------------------------------------------------


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


def _write_entsoe_chunk(
    tmp_path: Path,
    partition_date: date,
    body: bytes,
    period_start: datetime,
    period_end: datetime,
    *,
    page: int = 1,
    total_pages: int = 1,
    suffix: str = "a",
) -> None:
    response = RawResponse(
        body=body,
        content_type="text/xml",
        source="entsoe",
        dataset="day_ahead_prices",
        request_url="https://web-api.tp.entsoe.eu/api",
        request_params={
            "periodStart": period_start.strftime(ENTSOE_DT_FORMAT),
            "periodEnd": period_end.strftime(ENTSOE_DT_FORMAT),
        },
        page=page,
        total_pages=total_pages,
        http_status=200,
        data_date=partition_date,
    )
    BronzeWriter(tmp_path).write(response)


class TestD3dLowerBoundDurabilityProof:
    def test_entsoe_lower_bound_trim_requires_a_page_complete_predecessor(
        self, tmp_path: Path
    ) -> None:
        """Predecessor present, single-page (durable) -> the below-window
        over-span row is trimmed."""
        target_date = date(2024, 1, 16)
        window_start = datetime(2024, 1, 16, tzinfo=UTC)
        window_end = datetime(2024, 1, 17, tzinfo=UTC)

        # Own partition: 25 points, starting 1h before window.start (over-span).
        own_start = datetime(2024, 1, 15, 23, tzinfo=UTC)
        _write_entsoe_chunk(
            tmp_path, target_date, _points_xml(own_start, 25), window_start, window_end
        )

        # Predecessor (2024-01-15): its OWN durable, single-page chunk.
        predecessor_start = datetime(2024, 1, 15, tzinfo=UTC)
        _write_entsoe_chunk(
            tmp_path,
            date(2024, 1, 15),
            _points_xml(predecessor_start, 24),
            predecessor_start,
            window_start,
        )

        transformer = DayAheadPricesTransformer(tmp_path)
        rows = transformer.run(target_date, run_id="d3d-complete")
        assert rows == 24, "the below-window row must be trimmed by the proven predecessor"
        assert transformer.last_partition_filter_dropped_count == 1
        assert transformer.last_partition_filter_boundary_retained_count == 0

    def test_entsoe_lower_bound_rows_retained_when_predecessor_is_incomplete(
        self, tmp_path: Path
    ) -> None:
        """S3-1-equivalent regression for ENTSO-E: predecessor declares
        total_pages=2, only page 1 present -> the below-window row is
        RETAINED, not dropped."""
        target_date = date(2024, 1, 16)
        window_start = datetime(2024, 1, 16, tzinfo=UTC)
        window_end = datetime(2024, 1, 17, tzinfo=UTC)

        own_start = datetime(2024, 1, 15, 23, tzinfo=UTC)
        _write_entsoe_chunk(
            tmp_path, target_date, _points_xml(own_start, 25), window_start, window_end
        )

        predecessor_start = datetime(2024, 1, 15, tzinfo=UTC)
        _write_entsoe_chunk(
            tmp_path,
            date(2024, 1, 15),
            _points_xml(predecessor_start, 24),
            predecessor_start,
            window_start,
            total_pages=2,
            page=1,
        )

        transformer = DayAheadPricesTransformer(tmp_path)
        rows = transformer.run(target_date, run_id="d3d-incomplete")
        assert rows == 25, "the below-window row must be RETAINED when the predecessor is torn"
        assert transformer.last_partition_filter_dropped_count == 0
        assert transformer.last_partition_filter_boundary_retained_count == 1

    def test_entsoe_mixed_ownership_within_one_utc_date_trims_only_the_proven_row(
        self, tmp_path: Path
    ) -> None:
        """S4-1 regression: the predecessor partition for date D holds a
        CLAMPED chunk covering only [D T06:00, D+1 00:00) (as
        ``utils/time.py``'s ``day_subwindows`` produces at a range edge).
        Two below-bound rows on date D: one at D T20:00 (inside the clamped
        chunk) and one at D T03:00 (outside it). Only the first is trimmed;
        the second is retained with NO_COVERING_CHUNK -- never collapsed to
        a per-date verdict.
        """
        target_date = date(2024, 1, 17)
        window_start = datetime(2024, 1, 17, tzinfo=UTC)
        window_end = datetime(2024, 1, 18, tzinfo=UTC)

        predecessor_date = date(2024, 1, 16)
        # The predecessor's OWN clamped sub-window: [D 06:00, D+1 00:00).
        clamped_start = datetime(2024, 1, 16, 6, tzinfo=UTC)
        clamped_hours = 18  # 06:00 -> next-day 00:00
        _write_entsoe_chunk(
            tmp_path,
            predecessor_date,
            _points_xml(clamped_start, clamped_hours),
            clamped_start,
            window_start,
        )

        # Current partition's own raw body: two below-window points (one
        # inside the predecessor's clamped chunk, one outside it) plus the
        # 24 in-window hours -- built via explicit per-point timestamps
        # (_mixed_points_xml) since the two extra points are not
        # contiguous with the 24-hour block.
        inside_instant = datetime(2024, 1, 16, 20, tzinfo=UTC)  # inside [06:00, 24:00)
        outside_instant = datetime(2024, 1, 16, 3, tzinfo=UTC)  # outside the clamped chunk
        body = _mixed_points_xml(
            [inside_instant, outside_instant]
            + [window_start + timedelta(hours=h) for h in range(24)]
        )
        _write_entsoe_chunk(tmp_path, target_date, body, window_start, window_end)

        transformer = DayAheadPricesTransformer(tmp_path)
        rows = transformer.run(target_date, run_id="d3d-mixed")
        assert rows == 25, "only the proven (inside-chunk) row is trimmed; the other is retained"
        assert transformer.last_partition_filter_dropped_count == 1
        assert transformer.last_partition_filter_boundary_retained_count == 1


def _mixed_points_xml(timestamps: list[datetime]) -> bytes:
    """Build an A44 document whose Points carry EXPLICIT, non-contiguous
    instants via one ``Period`` per point (each with its own 1h
    ``timeInterval`` and a single ``position=1`` Point) -- avoids relying on
    ``start + (position-1)*resolution`` arithmetic for non-sequential
    instants."""
    periods = []
    for ts in timestamps:
        start_s = ts.strftime("%Y-%m-%dT%H:%MZ")
        end_s = (ts + __import__("datetime").timedelta(hours=1)).strftime("%Y-%m-%dT%H:%MZ")
        periods.append(
            f"""<Period>
      <timeInterval><start>{start_s}</start><end>{end_s}</end></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><price.amount>50.0</price.amount></Point>
    </Period>"""
        )
    periods_xml = "\n    ".join(periods)
    doc_end_s = (timestamps[-1] + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%MZ")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <mRID>d3d-mixed</mRID>
  <revisionNumber>1</revisionNumber>
  <type>A44</type>
  <createdDateTime>{timestamps[0].strftime("%Y-%m-%dT%H:%MZ")}</createdDateTime>
  <period.timeInterval>
    <start>{timestamps[0].strftime("%Y-%m-%dT%H:%MZ")}</start>
    <end>{doc_end_s}</end>
  </period.timeInterval>
  <TimeSeries>
    <mRID>1</mRID>
    <businessType>A62</businessType>
    <in_Domain.mRID codingScheme="A01">10YGB----------A</in_Domain.mRID>
    <out_Domain.mRID codingScheme="A01">10YGB----------A</out_Domain.mRID>
    <currency_Unit.name>EUR</currency_Unit.name>
    <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
    <curveType>A01</curveType>
    {periods_xml}
  </TimeSeries>
</Publication_MarketDocument>""".encode()
