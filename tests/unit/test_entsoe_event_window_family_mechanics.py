"""ENTSO-E event-window filter -- family mechanics tests (B4 T4/T5, I-7).

Pins the *filter's* boundary behaviour (HALF_OPEN semantics,
:func:`exclude_out_of_window`) for every newly-classified family (Sec 5,
Sec 9): the two shared families from T4 -- h6-quantity via
:class:`NetPositionsTransformer`, and h8-balancing via
:class:`ProcuredBalancingCapacityTransformer` and
:class:`BalancingEnergyBidsTransformer` for the distinct output shape --
plus the five single-class families added in T5 (``cross_border_flows``,
``net_transfer_capacity``, ``imbalance_prices``, ``imbalance_volume``,
``activated_balancing_prices``). These tests exercise both request
boundaries in the mechanics layer; they cannot and do not upgrade the
*vendor* evidence recorded in
``silver/entsoe/_event_window.py::EVENT_WINDOW_CLASSIFICATION`` (Sec 9 step
5) -- some opted-in families rest on an untested boundary or a sample never
interval-compared, and this file's job is only to pin the filter mechanic,
not the vendor's real-world behaviour.

**Harness note (T4, C-12's trap).** ``_write_entsoe_partition`` below takes
``source``/``dataset``/``body`` as explicit parameters -- never hardcoded --
so the written sidecar's identity always matches the transformer under
test. A hardcoded ``dataset="day_ahead_prices"`` (or any other family's
name) would write the raw body to a DIFFERENT bronze partition than the one
the transformer under test reads from (``BronzeWriter`` derives its write
path from ``response.source``/``response.dataset``, not the caller's
intent), silently producing an empty read or an unresolved window (M-4) --
not a filter-mechanics failure at all. If a test unexpectedly shows
``dropped_count == 0``, ``last_partition_filter_unresolved_count`` is
checked FIRST in every scenario below for exactly this reason.

**Partition isolation (Sec 9 step 0).** ``partition_request_window`` unions
every sidecar's window across ONE partition (C-15), and ``BronzeWriter``
names files from a second-resolution ``fetched_at`` plus a body hash
(C-16). Every scenario below therefore gets its own subdirectory of
``tmp_path`` as an independent data root -- never two scenarios sharing one
partition.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import gridflow.silver.entsoe  # noqa: F401 -- registers every entsoe transformer
from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.endpoints import ENTSOE_DT_FORMAT
from gridflow.silver.entsoe.activated_balancing_prices import ActivatedBalancingPricesTransformer
from gridflow.silver.entsoe.cross_border_flows import CrossBorderFlowsTransformer
from gridflow.silver.entsoe.h6_market import NetPositionsTransformer
from gridflow.silver.entsoe.h8_balancing import (
    AggregatedBalancingEnergyBidsTransformer,
    BalancingEnergyBidsTransformer,
    ProcuredBalancingCapacityTransformer,
)
from gridflow.silver.entsoe.imbalance_prices import ImbalancePricesTransformer
from gridflow.silver.entsoe.imbalance_volume import ImbalanceVolumeTransformer
from gridflow.silver.entsoe.net_transfer_capacity import NetTransferCapacityTransformer

if TYPE_CHECKING:
    from gridflow.silver.base import BaseSilverTransformer

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"

#: The target partition date every fixture's fixed ``<start>2024-01-15...``
#: timestamps fall on (C-11/C-17's fixtures are all dated 2024-01-15).
TARGET_DATE = date(2024, 1, 15)

#: (family_id, transformer class, source, dataset, fixture filename) --
#: T4: the two shared families (h6-quantity, h8-balancing, including its
#: distinct-output-shape sibling). T5: the five single-class families.
FAMILY_CASES = [
    pytest.param(
        "h6_quantity",
        NetPositionsTransformer,
        "entsoe",
        "net_positions",
        "h6_market_quantity_gb_fr.xml",
        id="h6_quantity",
    ),
    pytest.param(
        "h8_balancing_capacity",
        ProcuredBalancingCapacityTransformer,
        "entsoe",
        "procured_balancing_capacity",
        "procured_balancing_capacity_gb.xml",
        id="h8_balancing_capacity",
    ),
    pytest.param(
        # N-21: this case now runs on the real ReserveBid_MarketDocument /
        # Bid_TimeSeries envelope (D-4b reshaped balancing_energy_bids_gb.xml
        # to the live A37 shape, F-1) rather than the fictional
        # Balancing_MarketDocument / bare-TimeSeries shape it used before.
        # The filter operates on the transformed frame, not the XML, so what
        # is proven about the filter mechanic here is unchanged -- the proof
        # now rests on a shape ENTSO-E actually returns (Sec 3b row 4).
        "h8_balancing_bids",
        BalancingEnergyBidsTransformer,
        "entsoe",
        "balancing_energy_bids",
        "balancing_energy_bids_gb.xml",
        id="h8_balancing_bids",
    ),
    pytest.param(
        "cross_border_flows",
        CrossBorderFlowsTransformer,
        "entsoe",
        "cross_border_flows",
        "cross_border_flows_gb_fr.xml",
        id="cross_border_flows",
    ),
    pytest.param(
        "net_transfer_capacity",
        NetTransferCapacityTransformer,
        "entsoe",
        "net_transfer_capacity",
        "net_transfer_capacity_gb_fr.xml",
        id="net_transfer_capacity",
    ),
    pytest.param(
        "imbalance_prices",
        ImbalancePricesTransformer,
        "entsoe",
        "imbalance_prices",
        "imbalance_prices_gb.xml",
        id="imbalance_prices",
    ),
    pytest.param(
        "imbalance_volume",
        ImbalanceVolumeTransformer,
        "entsoe",
        "imbalance_volume",
        "imbalance_volume_gb.xml",
        id="imbalance_volume",
    ),
    pytest.param(
        "activated_balancing_prices",
        ActivatedBalancingPricesTransformer,
        "entsoe",
        "activated_balancing_prices",
        "activated_balancing_prices_gb.xml",
        id="activated_balancing_prices",
    ),
]


def _write_entsoe_partition(
    data_dir: Path,
    *,
    source: str,
    dataset: str,
    partition_date: date,
    body: bytes,
    period_start: datetime,
    period_end: datetime,
) -> None:
    """Write one bronze raw body + sidecar, parameterised by source/dataset
    (T4 harness note) -- never hardcoded, so the sidecar's recorded
    identity always matches the transformer under test."""
    response = RawResponse(
        body=body,
        content_type="text/xml",
        source=source,
        dataset=dataset,
        request_url="https://web-api.tp.entsoe.eu/api",
        request_params={
            "periodStart": period_start.strftime(ENTSOE_DT_FORMAT),
            "periodEnd": period_end.strftime(ENTSOE_DT_FORMAT),
        },
        data_date=partition_date,
    )
    BronzeWriter(data_dir).write(response)


#: A window wide enough to cover every fixture's rows regardless of family
#: (all fixtures are dated 2024-01-15). The learning pass never consults the
#: event-window filter (``transform()`` doesn't call it), so the exact bounds
#: are immaterial -- only that the sidecar is written through the real
#: ``BronzeWriter`` harness (T4 harness note), not a hand-built path.
_LEARN_WINDOW_START = datetime(2020, 1, 1, tzinfo=UTC)
_LEARN_WINDOW_END = datetime(2030, 1, 1, tzinfo=UTC)


def _learn_instants(
    tmp_path: Path,
    cls: type[BaseSilverTransformer],
    source: str,
    dataset: str,
    fixture_name: str,
) -> list[tuple[datetime, int]]:
    """Sec 9 step 1: run ``read_bronze()`` + ``transform()`` against the raw
    fixture (no filter is consulted by ``transform()`` itself) to learn the
    fixture's real ``timestamp_utc`` instants and how many rows share each
    one -- never guessed from the filename. Uses its own isolated partition
    (step 0), written through the same parameterised ``BronzeWriter`` harness
    (``_write_entsoe_partition``) the scenario steps use -- no hand-built
    ``bronze/<source>/<dataset>/...`` path (plan Sec 9)."""
    root = tmp_path / "learn"
    _write_entsoe_partition(
        root,
        source=source,
        dataset=dataset,
        partition_date=TARGET_DATE,
        body=(FIXTURES / fixture_name).read_bytes(),
        period_start=_LEARN_WINDOW_START,
        period_end=_LEARN_WINDOW_END,
    )

    transformer = cls(root)
    raw_df = transformer.read_bronze(TARGET_DATE)
    assert not raw_df.is_empty(), f"{dataset} fixture {fixture_name} produced no raw rows"
    clean_df = transformer.transform(raw_df)
    assert not clean_df.is_empty(), f"{dataset} fixture {fixture_name} produced no clean rows"

    timestamps = sorted(clean_df["timestamp_utc"].to_list())
    counts: dict[datetime, int] = {}
    for ts in timestamps:
        counts[ts] = counts.get(ts, 0) + 1
    instants = sorted(counts.items())
    assert len(instants) >= 2, (
        f"{dataset}: fixture {fixture_name} yields fewer than 2 distinct instants "
        "-- cannot express a partial boundary drop (Sec 9 fallback threshold)"
    )
    return instants


class TestEventWindowFamilyMechanics:
    """I-7: each newly-classified family (Sec 5) drops the rows at/after
    ``window.end`` and below ``window.start``, keeps the rows at
    ``window.end - resolution`` and at ``window.start``, and reports the
    drop through ``last_partition_filter_dropped_count`` (HALF_OPEN,
    Sec 9)."""

    @pytest.mark.parametrize(
        ("family_id", "cls", "source", "dataset", "fixture_name"), FAMILY_CASES
    )
    def test_in_window_control(
        self,
        tmp_path: Path,
        family_id: str,
        cls: type[BaseSilverTransformer],
        source: str,
        dataset: str,
        fixture_name: str,
    ) -> None:
        """Sec 9 step 2: a window covering every row is a no-op -- proves
        the filter is not indiscriminate."""
        instants = _learn_instants(tmp_path, cls, source, dataset, fixture_name)
        total_rows = sum(count for _, count in instants)
        first_ts = instants[0][0]
        last_ts, _last_count = instants[-1]
        resolution = instants[1][0] - instants[0][0]

        root = tmp_path / "s_in_window"
        window_start = first_ts
        window_end = last_ts + resolution
        _write_entsoe_partition(
            root,
            source=source,
            dataset=dataset,
            partition_date=TARGET_DATE,
            body=(FIXTURES / fixture_name).read_bytes(),
            period_start=window_start,
            period_end=window_end,
        )
        transformer = cls(root)
        rows = transformer.run(TARGET_DATE, run_id=f"{family_id}-in-window")
        assert transformer.last_partition_filter_unresolved_count == 0, (
            f"{family_id}: window unresolved (M-4) -- check the sidecar identity first"
        )
        assert rows == total_rows
        assert transformer.last_partition_filter_dropped_count == 0

    @pytest.mark.parametrize(
        ("family_id", "cls", "source", "dataset", "fixture_name"), FAMILY_CASES
    )
    def test_boundary_row_dropped_at_window_end(
        self,
        tmp_path: Path,
        family_id: str,
        cls: type[BaseSilverTransformer],
        source: str,
        dataset: str,
        fixture_name: str,
    ) -> None:
        """Sec 9 step 3: ``window.end`` exactly at the last instant's
        timestamp -> every row at that instant is dropped (HALF_OPEN:
        ``>= end`` is out)."""
        instants = _learn_instants(tmp_path, cls, source, dataset, fixture_name)
        total_rows = sum(count for _, count in instants)
        first_ts = instants[0][0]
        last_ts, last_count = instants[-1]

        root = tmp_path / "s_boundary_out"
        window_start = first_ts
        window_end = last_ts
        _write_entsoe_partition(
            root,
            source=source,
            dataset=dataset,
            partition_date=TARGET_DATE,
            body=(FIXTURES / fixture_name).read_bytes(),
            period_start=window_start,
            period_end=window_end,
        )
        transformer = cls(root)
        rows = transformer.run(TARGET_DATE, run_id=f"{family_id}-boundary-out")
        assert transformer.last_partition_filter_unresolved_count == 0, (
            f"{family_id}: window unresolved (M-4) -- check the sidecar identity first"
        )
        assert rows == total_rows - last_count
        assert transformer.last_partition_filter_dropped_count == last_count

    @pytest.mark.parametrize(
        ("family_id", "cls", "source", "dataset", "fixture_name"), FAMILY_CASES
    )
    def test_boundary_row_kept_when_window_extends_one_step(
        self,
        tmp_path: Path,
        family_id: str,
        cls: type[BaseSilverTransformer],
        source: str,
        dataset: str,
        fixture_name: str,
    ) -> None:
        """Sec 9 step 4: ``window.end`` one resolution step later -> the
        same last-instant rows are kept, dropped_count == 0."""
        instants = _learn_instants(tmp_path, cls, source, dataset, fixture_name)
        total_rows = sum(count for _, count in instants)
        first_ts = instants[0][0]
        last_ts, _last_count = instants[-1]
        resolution = instants[1][0] - instants[0][0]

        root = tmp_path / "s_boundary_in"
        window_start = first_ts
        window_end = last_ts + resolution
        _write_entsoe_partition(
            root,
            source=source,
            dataset=dataset,
            partition_date=TARGET_DATE,
            body=(FIXTURES / fixture_name).read_bytes(),
            period_start=window_start,
            period_end=window_end,
        )
        transformer = cls(root)
        rows = transformer.run(TARGET_DATE, run_id=f"{family_id}-boundary-in")
        assert transformer.last_partition_filter_unresolved_count == 0, (
            f"{family_id}: window unresolved (M-4) -- check the sidecar identity first"
        )
        assert rows == total_rows
        assert transformer.last_partition_filter_dropped_count == 0

    @pytest.mark.parametrize(
        ("family_id", "cls", "source", "dataset", "fixture_name"), FAMILY_CASES
    )
    def test_below_start_row_dropped(
        self,
        tmp_path: Path,
        family_id: str,
        cls: type[BaseSilverTransformer],
        source: str,
        dataset: str,
        fixture_name: str,
    ) -> None:
        """Sec 9 step 5: ``window.start`` one resolution step after the
        first instant -> the rows at the first instant are dropped, counted
        from the learning read."""
        instants = _learn_instants(tmp_path, cls, source, dataset, fixture_name)
        total_rows = sum(count for _, count in instants)
        first_ts, first_count = instants[0]
        last_ts, _last_count = instants[-1]
        resolution = instants[1][0] - instants[0][0]

        root = tmp_path / "s_below_start"
        window_start = first_ts + resolution
        window_end = last_ts + resolution
        _write_entsoe_partition(
            root,
            source=source,
            dataset=dataset,
            partition_date=TARGET_DATE,
            body=(FIXTURES / fixture_name).read_bytes(),
            period_start=window_start,
            period_end=window_end,
        )
        transformer = cls(root)
        rows = transformer.run(TARGET_DATE, run_id=f"{family_id}-below-start")
        assert transformer.last_partition_filter_unresolved_count == 0, (
            f"{family_id}: window unresolved (M-4) -- check the sidecar identity first"
        )
        assert rows == total_rows - first_count
        assert transformer.last_partition_filter_dropped_count == first_count

    @pytest.mark.parametrize(
        ("family_id", "cls", "source", "dataset", "fixture_name"), FAMILY_CASES
    )
    def test_warning_logged_on_drop(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        family_id: str,
        cls: type[BaseSilverTransformer],
        source: str,
        dataset: str,
        fixture_name: str,
    ) -> None:
        """Sec 9 step 6: M-2's visibility mechanism -- the WARNING is
        emitted with the dropped count, never a silent drop (the
        ``caplog`` pattern at ``test_entsoe_event_window_classification.py``'s
        HALF_OPEN tests)."""
        instants = _learn_instants(tmp_path, cls, source, dataset, fixture_name)
        first_ts = instants[0][0]
        last_ts, last_count = instants[-1]

        root = tmp_path / "s_warning"
        window_start = first_ts
        window_end = last_ts
        _write_entsoe_partition(
            root,
            source=source,
            dataset=dataset,
            partition_date=TARGET_DATE,
            body=(FIXTURES / fixture_name).read_bytes(),
            period_start=window_start,
            period_end=window_end,
        )
        transformer = cls(root)
        with caplog.at_level("WARNING"):
            transformer.run(TARGET_DATE, run_id=f"{family_id}-warning")
        assert transformer.last_partition_filter_dropped_count == last_count
        assert any(
            "out-of-scope" in message and str(last_count) in message for message in caplog.messages
        ), (
            f"{family_id}: no out-of-scope WARNING logged carrying the measured "
            f"dropped-row count ({last_count}) for a non-zero drop (M-2)"
        )


class TestAggregatedBalancingEnergyBidsNeverFilters:
    """Explicit M-6/C-5 regression: ``AggregatedBalancingEnergyBidsTransformer``
    subclasses ``BalancingEnergyBidsTransformer`` (now ``EVENT_WINDOW_FILTER
    = True``) and would silently inherit that opt-in without its own
    explicit override."""

    def test_flag_is_false_and_locally_declared(self) -> None:
        assert AggregatedBalancingEnergyBidsTransformer.EVENT_WINDOW_FILTER is False
        assert "EVENT_WINDOW_FILTER" in AggregatedBalancingEnergyBidsTransformer.__dict__, (
            "the False must be declared on the subclass's own body, not merely true by default"
        )

    def test_parent_is_true_so_the_override_is_load_bearing(self) -> None:
        """Without the explicit False, this class would inherit True from
        its parent -- confirms the override is not vacuous."""
        assert BalancingEnergyBidsTransformer.EVENT_WINDOW_FILTER is True
