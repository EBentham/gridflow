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
# HALF_OPEN interval semantics -- unconditional out-of-scope exclusion
# (Sol ruling, 2026-07-26, amending the R2-A plan's original D-3d).
#
# ENTSO-E's request is [periodStart, periodEnd); the vendor may return rows
# beyond it (measured CET/CEST delivery-day over-span, S1.4). Those rows
# were never requested by THIS partition at all, so there is no ownership
# question and no neighbour-durability proof to make -- they are always
# excluded, with or without any neighbour bronze existing. This supersedes
# the per-instant NEIGHBOUR-durability-gated lower bound this file
# originally pinned for ENTSO-E; that mechanism remains correct and
# untouched for Elexon's CLOSED-interval boundary (D-3b,
# tests/silver/test_elexon_publication_window.py), and the underlying
# per-instant-not-per-date primitive (the S4-1 regression) stays pinned at
# the primitive level, unaffected by this ruling, in
# tests/unit/test_partition_window_filter.py::TestNeighbourOwns::
# test_lower_bound_ownership_is_resolved_per_instant_not_per_date.
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


class TestHalfOpenIntervalUnconditionalExclusion:
    def test_out_of_window_rows_excluded_with_no_neighbour_bronze_at_all(
        self, tmp_path: Path
    ) -> None:
        """No predecessor and no successor bronze exists ANYWHERE -- the
        over-span rows are still excluded, proving no neighbour proof is
        attempted or needed under HALF_OPEN semantics (contrast with
        Elexon's CLOSED-interval boundary, which RETAINS in this exact
        no-neighbour situation, D-3b)."""
        target_date = date(2024, 1, 16)
        window_start = datetime(2024, 1, 16, tzinfo=UTC)
        window_end = datetime(2024, 1, 17, tzinfo=UTC)

        # 26 points: 1h below window.start, 24 in-window, 1h at/after window.end.
        own_start = datetime(2024, 1, 15, 23, tzinfo=UTC)
        _write_entsoe_chunk(
            tmp_path, target_date, _points_xml(own_start, 26), window_start, window_end
        )
        # Deliberately NO predecessor (2024-01-15) and NO successor
        # (2024-01-17) bronze partition exists anywhere.

        transformer = DayAheadPricesTransformer(tmp_path)
        rows = transformer.run(target_date, run_id="half-open-no-neighbours")
        assert rows == 24, "both out-of-scope rows must be excluded without any neighbour proof"
        assert transformer.last_partition_filter_dropped_count == 2
        assert transformer.last_partition_filter_boundary_retained_count == 0, (
            "HALF_OPEN semantics never retains an out-of-scope row"
        )

    def test_out_of_window_exclusion_is_counted_and_logged_never_silent(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        target_date = date(2024, 1, 16)
        window_start = datetime(2024, 1, 16, tzinfo=UTC)
        window_end = datetime(2024, 1, 17, tzinfo=UTC)

        own_start = datetime(2024, 1, 15, 22, tzinfo=UTC)
        _write_entsoe_chunk(
            tmp_path, target_date, _points_xml(own_start, 28), window_start, window_end
        )

        transformer = DayAheadPricesTransformer(tmp_path)
        with caplog.at_level("WARNING"):
            rows = transformer.run(target_date, run_id="half-open-logged")

        assert rows == 24
        assert transformer.last_partition_filter_dropped_count == 4
        assert any("out-of-scope" in message for message in caplog.messages)

    def test_in_window_rows_are_unaffected(self, tmp_path: Path) -> None:
        """A request whose response does not over-span at all is a no-op."""
        target_date = date(2024, 1, 16)
        window_start = datetime(2024, 1, 16, tzinfo=UTC)
        window_end = datetime(2024, 1, 17, tzinfo=UTC)

        _write_entsoe_chunk(
            tmp_path, target_date, _points_xml(window_start, 24), window_start, window_end
        )

        transformer = DayAheadPricesTransformer(tmp_path)
        rows = transformer.run(target_date, run_id="half-open-noop")
        assert rows == 24
        assert transformer.last_partition_filter_dropped_count == 0
