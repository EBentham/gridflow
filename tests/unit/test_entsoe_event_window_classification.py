"""ENTSO-E event-window filter — classification + D-3d durability tests (R2-A Task 4).

Covers A-6 (classification completeness), the transform-backed replacement
for the ineffective rev-2 ``_event_time_column`` assertion, and A-16's D-3d
per-instant lower-bound durability proof (the S4-1 mixed-ownership
regression, demonstrated RED against both an unconditional-trim and a
per-date ``frozenset[date]`` implementation via the git history this plan
records, not re-derived here).
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import gridflow.silver.entsoe  # noqa: F401 -- registers every entsoe transformer
from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.endpoints import ENTSOE_DT_FORMAT
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsoe._event_window import (
    EVENT_WINDOW_CLASSIFICATION,
    EVENT_WINDOW_FILTER_EXEMPT,
    R2A_CARRIED_DATASETS,
    Classification,
)
from gridflow.silver.entsoe.actual_generation import ActualGenerationTransformer
from gridflow.silver.entsoe.actual_generation_units import ActualGenerationUnitsTransformer
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer
from gridflow.silver.entsoe.generation_forecast import GenerationForecastTransformer
from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.entsoe.wind_solar_forecast import WindSolarForecastTransformer
from gridflow.silver.registry import get_transformer_class, list_transformers


class TestClassificationMap:
    """B4 (N-9) EVENT_WINDOW_CLASSIFICATION map invariants I-1..I-9.

    I-7 is reserved to the family mechanics tests
    (``test_entsoe_event_window_family_mechanics.py``, T4/T5) and the final
    gate -- not part of this class.
    """

    def test_i1_filter_safe_iff_opted_in_and_not_exempt(self) -> None:
        """I-1: classification == FILTER_SAFE <=> EVENT_WINDOW_FILTER is
        True <=> dataset absent from EVENT_WINDOW_FILTER_EXEMPT."""
        for dataset, entry in EVENT_WINDOW_CLASSIFICATION.items():
            cls = get_transformer_class("entsoe", dataset)
            assert cls is not None, f"{dataset} must be registered"
            opted_in = cls.EVENT_WINDOW_FILTER
            exempt_present = dataset in EVENT_WINDOW_FILTER_EXEMPT
            is_filter_safe = entry.classification is Classification.FILTER_SAFE
            assert is_filter_safe == opted_in, (
                f"I-1 [{dataset}]: classification FILTER_SAFE={is_filter_safe} "
                f"but EVENT_WINDOW_FILTER={opted_in}"
            )
            assert is_filter_safe == (not exempt_present), (
                f"I-1 [{dataset}]: classification FILTER_SAFE={is_filter_safe} "
                f"but exempt-dict presence={exempt_present}"
            )

    def test_i2_exempt_or_unknown_iff_not_opted_in_and_exempt_present(self) -> None:
        """I-2: classification in (EXEMPT, UNKNOWN) <=> EVENT_WINDOW_FILTER
        is False <=> dataset present in EVENT_WINDOW_FILTER_EXEMPT with a
        non-empty reason."""
        for dataset, entry in EVENT_WINDOW_CLASSIFICATION.items():
            cls = get_transformer_class("entsoe", dataset)
            assert cls is not None
            opted_in = cls.EVENT_WINDOW_FILTER
            reason = EVENT_WINDOW_FILTER_EXEMPT.get(dataset, "")
            is_exempt_or_unknown = entry.classification in (
                Classification.EXEMPT,
                Classification.UNKNOWN,
            )
            assert is_exempt_or_unknown == (not opted_in), (
                f"I-2 [{dataset}]: EXEMPT/UNKNOWN={is_exempt_or_unknown} "
                f"but EVENT_WINDOW_FILTER={opted_in}"
            )
            assert is_exempt_or_unknown == bool(reason), (
                f"I-2 [{dataset}]: EXEMPT/UNKNOWN={is_exempt_or_unknown} "
                f"but exempt-dict reason={reason!r}"
            )

    def test_i3_total_coverage(self) -> None:
        """I-3: set(EVENT_WINDOW_CLASSIFICATION) == set(registered entsoe
        datasets) -- no orphan entry either way."""
        registered = {dataset for _, dataset in list_transformers("entsoe")}
        assert set(EVENT_WINDOW_CLASSIFICATION) == registered

    def test_i4_every_entry_has_a_resolvable_citation(self) -> None:
        """I-4: non-empty doc_type/evidence/family/transformer; every
        UNKNOWN entry's exemption reason starts with 'TODO:'; every entry
        with an empty probes tuple names a vault page path in evidence,
        except R2A_CARRIED_DATASETS members (I-9's pointer carve-out)."""
        for dataset, entry in EVENT_WINDOW_CLASSIFICATION.items():
            assert entry.doc_type, f"I-4 [{dataset}]: empty doc_type"
            assert entry.evidence, f"I-4 [{dataset}]: empty evidence"
            assert entry.family, f"I-4 [{dataset}]: empty family"
            assert entry.transformer, f"I-4 [{dataset}]: empty transformer"
            if entry.classification is Classification.UNKNOWN:
                reason = EVENT_WINDOW_FILTER_EXEMPT.get(dataset, "")
                assert reason.startswith("TODO:"), (
                    f"I-4 [{dataset}]: UNKNOWN reason does not start with 'TODO:': {reason!r}"
                )
            if not entry.probes and dataset not in R2A_CARRIED_DATASETS:
                assert "30-vendors/" in entry.evidence, (
                    f"I-4 [{dataset}]: empty probes but no vault page path in evidence"
                )

    def test_i5_end_state_figures_match_the_plan(self) -> None:
        """I-5: every figure in Sec 5/5a holds when measured from a live
        import, transcribed from the plan (the only place these figures are
        stated)."""
        registered = {dataset for _, dataset in list_transformers("entsoe")}
        classification_counts = Counter(
            e.classification for e in EVENT_WINDOW_CLASSIFICATION.values()
        )
        assert classification_counts[Classification.FILTER_SAFE] == 26
        assert classification_counts[Classification.EXEMPT] == 17
        assert classification_counts[Classification.UNKNOWN] == 5
        true_flag_count = 0
        for dataset in registered:
            cls = get_transformer_class("entsoe", dataset)
            assert cls is not None
            if cls.EVENT_WINDOW_FILTER:
                true_flag_count += 1
        assert true_flag_count == 26
        assert len(EVENT_WINDOW_FILTER_EXEMPT) == 22
        todo_count = sum(
            1 for reason in EVENT_WINDOW_FILTER_EXEMPT.values() if reason.startswith("TODO:")
        )
        assert todo_count == 5
        assert len(R2A_CARRIED_DATASETS) == 20

    def test_i6_true_flag_is_never_inherited(self) -> None:
        """I-6: for every registered ENTSO-E transformer,
        EVENT_WINDOW_FILTER is True implies 'EVENT_WINDOW_FILTER' in
        cls.__dict__ -- a True flag is always locally declared, never
        inherited (M-6). Already true on master (C-14)."""
        for source, dataset in list_transformers("entsoe"):
            cls = get_transformer_class(source, dataset)
            assert cls is not None
            if cls.EVENT_WINDOW_FILTER is True:
                assert "EVENT_WINDOW_FILTER" in cls.__dict__, (
                    f"I-6 [{dataset}]: EVENT_WINDOW_FILTER=True but not locally "
                    f"declared on {cls.__name__} -- inherited True (M-6)"
                )

    def test_i8_family_and_transformer_match_the_live_class_graph(self) -> None:
        """I-8: entry.transformer == get_transformer_class(...).__name__,
        and entry.family == cls.__bases__[0].__name__ unless that immediate
        base is BaseSilverTransformer, in which case entry.family ==
        'own'."""
        for dataset, entry in EVENT_WINDOW_CLASSIFICATION.items():
            cls = get_transformer_class("entsoe", dataset)
            assert cls is not None
            assert entry.transformer == cls.__name__, (
                f"I-8 [{dataset}]: transformer={entry.transformer!r} != {cls.__name__!r}"
            )
            base = cls.__bases__[0]
            expected_family = "own" if base is BaseSilverTransformer else base.__name__
            assert entry.family == expected_family, (
                f"I-8 [{dataset}]: family={entry.family!r} != {expected_family!r}"
            )

    def test_i9_r2a_pointer_rule(self) -> None:
        """I-9: dataset in R2A_CARRIED_DATASETS <=> limitation == '', and
        every member also has an empty probes tuple and a non-empty pointer
        evidence. R2A_CARRIED_DATASETS must equal the registered ENTSO-E
        datasets minus the B4 research population (_B1_POPULATION,
        transcribed independently above, never re-derived from the map
        itself), checked against a live registry walk."""
        registered = {dataset for _, dataset in list_transformers("entsoe")}
        expected_r2a = registered - set(_B1_POPULATION)
        assert expected_r2a == R2A_CARRIED_DATASETS
        assert len(R2A_CARRIED_DATASETS) == 20
        for dataset, entry in EVENT_WINDOW_CLASSIFICATION.items():
            is_member = dataset in R2A_CARRIED_DATASETS
            assert is_member == (entry.limitation == ""), (
                f"I-9 [{dataset}]: R2A member={is_member} but limitation={entry.limitation!r}"
            )
            if is_member:
                assert entry.probes == (), f"I-9 [{dataset}]: R2A member has non-empty probes"
                assert entry.evidence, f"I-9 [{dataset}]: R2A member has empty evidence"
            else:
                assert entry.limitation, f"I-9 [{dataset}]: non-member has empty limitation"


#: The 28-dataset B4 research population (R3-RESEARCH.md Sec 1.1/1.3),
#: transcribed independently of EVENT_WINDOW_CLASSIFICATION so I-9's check
#: below is not circular (it must not derive its own expected answer from
#: the map under test).
_B1_POPULATION = (
    "cross_border_flows",
    "commercial_schedules",
    "total_nominated_capacity",
    "net_transfer_capacity",
    "net_positions",
    "procured_balancing_capacity",
    "balancing_energy_bids",
    "dc_link_intraday_transfer_limits",
    "redispatching_cross_border",
    "redispatching_internal",
    "countertrading",
    "offered_transfer_capacity_continuous",
    "offered_transfer_capacity_implicit",
    "offered_transfer_capacity_explicit",
    "transfer_capacity_use",
    "total_capacity_allocated",
    "activated_balancing_prices",
    "imbalance_prices",
    "imbalance_volume",
    "congestion_management_costs",
    "balancing_financial_expenses_income",
    "auction_revenue",
    "congestion_income",
    "contracted_reserves",
    "current_balancing_state",
    "aggregated_balancing_energy_bids",
    "cross_zonal_balancing_capacity",
    "activated_balancing_qty",
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"

#: B4 (N-9): the R2-A original 7 plus the 19 evidence-classified FILTER_SAFE
#: datasets (R3-RESEARCH.md Sec 1.1) -- 26 total, per
#: EVENT_WINDOW_CLASSIFICATION / plan Sec 5.
OPTED_IN_DATASETS = (
    "day_ahead_prices",
    "actual_load",
    "load_forecast",
    "actual_generation",
    "actual_generation_units",
    "wind_solar_forecast",
    "generation_forecast",
    "cross_border_flows",
    "commercial_schedules",
    "total_nominated_capacity",
    "net_transfer_capacity",
    "net_positions",
    "procured_balancing_capacity",
    "balancing_energy_bids",
    "dc_link_intraday_transfer_limits",
    "redispatching_cross_border",
    "redispatching_internal",
    "countertrading",
    "offered_transfer_capacity_continuous",
    "offered_transfer_capacity_implicit",
    "offered_transfer_capacity_explicit",
    "transfer_capacity_use",
    "total_capacity_allocated",
    "activated_balancing_prices",
    "imbalance_prices",
    "imbalance_volume",
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
                f"{dataset}: EVENT_WINDOW_FILTER does not match B4's 26-dataset "
                "opt-in set (R2-A's 7 plus B4's 19 evidence-classified FILTER_SAFE)"
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
