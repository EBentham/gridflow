"""Elexon publication-window filter — end-to-end wiring tests (R2-A Task 2).

Covers F-04's adjacent-partition boundary duplication (A-1), the anti-loss
truths (A-2), D-7's fail-loud/counted-warning contract (A-3), the S3-1
page-completeness regression (A-12), and G-1/G-2's scope classification
(A-11).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import polars as pl
import pytest

import gridflow.silver.elexon  # noqa: F401 -- registers every elexon transformer
from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.silver.elexon._publication_window import (
    PUBLICATION_WINDOW_EXEMPT,
    publication_window_params,
)
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.indo import INDOTransformer
from gridflow.silver.elexon.remit import REMITTransformer
from gridflow.silver.registry import list_transformers
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

DATASET_ENDPOINT_PATH = {
    "indo": "/datasets/INDO",
    "fuelhh": "/datasets/FUELHH",
    "remit": "/datasets/REMIT",
}


def _write_chunk(
    tmp_path: Path,
    dataset: str,
    start: datetime,
    end: datetime,
    records: list[dict[str, object]],
    *,
    page: int = 1,
    total_pages: int = 1,
    from_param: str = "publishDateTimeFrom",
    to_param: str = "publishDateTimeTo",
) -> None:
    """Write one bronze raw+sidecar pair shaped like the real Elexon connector."""
    body = json.dumps({"data": records}).encode()
    response = RawResponse(
        body=body,
        content_type="application/json",
        source="elexon",
        dataset=dataset,
        request_url=f"https://data.elexon.co.uk/bmrs/api/v1{DATASET_ENDPOINT_PATH[dataset]}",
        request_params={
            from_param: start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            to_param: end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "page": page,
        },
        api_version="v1",
        page=page,
        total_pages=total_pages,
        http_status=200,
        data_date=start.date(),
    )
    BronzeWriter(tmp_path).write(response)


def _write_chunk_missing_page(
    tmp_path: Path,
    dataset: str,
    start: datetime,
    end: datetime,
    *,
    total_pages: int,
    present_page: int = 1,
) -> None:
    """Write only ONE page of a multi-page chunk (the S3-1 torn-ingest case)."""
    records = [
        {
            "settlementDate": start.date().isoformat(),
            "settlementPeriod": 1,
            "publishTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "demand": 100.0,
        }
    ]
    _write_chunk(tmp_path, dataset, start, end, records, page=present_page, total_pages=total_pages)


def _indo_record(settlement_date: date, period: int, publish_time: datetime) -> dict[str, object]:
    return {
        "settlementDate": settlement_date.isoformat(),
        "settlementPeriod": period,
        "publishTime": publish_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "demand": 100.0 + period,
    }


def _fuelhh_record(
    settlement_date: date, period: int, fuel_type: str, publish_time: datetime
) -> dict[str, object]:
    return {
        "settlementDate": settlement_date.isoformat(),
        "settlementPeriod": period,
        "fuelType": fuel_type,
        "generation": 50.0,
        "publishTime": publish_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# A-1 — adjacent partitions do not duplicate the boundary publication (F-04)
# ---------------------------------------------------------------------------


class TestAdjacentPartitionBoundaryDeduplication:
    def test_adjacent_partitions_do_not_duplicate_boundary_publication(
        self, tmp_path: Path
    ) -> None:
        day = date(2026, 7, 11)
        next_day = date(2026, 7, 12)
        boundary_instant = datetime(2026, 7, 12, tzinfo=UTC)

        _write_chunk(
            tmp_path,
            "indo",
            datetime(2026, 7, 11, tzinfo=UTC),
            boundary_instant,
            [
                _indo_record(day, 1, datetime(2026, 7, 11, tzinfo=UTC)),
                _indo_record(next_day, 2, boundary_instant),  # the shared boundary row
            ],
        )
        _write_chunk(
            tmp_path,
            "indo",
            boundary_instant,
            datetime(2026, 7, 13, tzinfo=UTC),
            [
                _indo_record(next_day, 2, boundary_instant),  # duplicated in both partitions
                _indo_record(next_day, 3, datetime(2026, 7, 12, 0, 30, tzinfo=UTC)),
            ],
        )

        transformer = INDOTransformer(tmp_path)
        rows_day = transformer.run(day, run_id="a1")
        rows_next_day = transformer.run(next_day, run_id="a1")

        assert rows_day == 1, "the boundary row must be trimmed from the predecessor"
        assert rows_next_day == 2

        paths = PathBuilder(tmp_path)
        combined = pl.concat(
            [
                pl.read_parquet(paths.silver_file("elexon", "indo", day)),
                pl.read_parquet(paths.silver_file("elexon", "indo", next_day)),
            ]
        )
        key = combined.select(["settlement_date", "settlement_period"])
        assert key.n_unique() == len(combined), "boundary key duplicated across partitions"

    def test_fuelhh_variant(self, tmp_path: Path) -> None:
        day = date(2026, 7, 11)
        next_day = date(2026, 7, 12)
        boundary_instant = datetime(2026, 7, 12, tzinfo=UTC)

        _write_chunk(
            tmp_path,
            "fuelhh",
            datetime(2026, 7, 11, tzinfo=UTC),
            boundary_instant,
            [
                _fuelhh_record(day, 1, "WIND", datetime(2026, 7, 11, tzinfo=UTC)),
                _fuelhh_record(next_day, 2, "WIND", boundary_instant),
                _fuelhh_record(next_day, 2, "GAS", boundary_instant),
            ],
        )
        _write_chunk(
            tmp_path,
            "fuelhh",
            boundary_instant,
            datetime(2026, 7, 13, tzinfo=UTC),
            [
                _fuelhh_record(next_day, 2, "WIND", boundary_instant),
                _fuelhh_record(next_day, 2, "GAS", boundary_instant),
                _fuelhh_record(next_day, 3, "WIND", datetime(2026, 7, 12, 0, 30, tzinfo=UTC)),
            ],
        )

        transformer = FuelHHTransformer(tmp_path)
        rows_day = transformer.run(day, run_id="a1")
        rows_next_day = transformer.run(next_day, run_id="a1")

        assert rows_day == 1
        assert rows_next_day == 3

        paths = PathBuilder(tmp_path)
        combined = pl.concat(
            [
                pl.read_parquet(paths.silver_file("elexon", "fuelhh", day)),
                pl.read_parquet(paths.silver_file("elexon", "fuelhh", next_day)),
            ]
        )
        key = combined.select(["settlement_date", "settlement_period", "fuel_type"])
        assert key.n_unique() == len(combined)


# ---------------------------------------------------------------------------
# A-2 — anti-loss
# ---------------------------------------------------------------------------


class TestAntiLoss:
    def test_next_settlement_days_sp1_survives_without_a_successor_partition(
        self, tmp_path: Path
    ) -> None:
        """The trailing boundary (no successor partition at all) is retained,
        not silently dropped — D-3b's neighbour-ownership gate defaults to
        unproven when there IS no neighbour."""
        day = date(2026, 7, 11)
        boundary_instant = datetime(2026, 7, 12, tzinfo=UTC)

        _write_chunk(
            tmp_path,
            "indo",
            datetime(2026, 7, 11, tzinfo=UTC),
            boundary_instant,
            [
                _indo_record(day, 1, datetime(2026, 7, 11, tzinfo=UTC)),
                _indo_record(date(2026, 7, 12), 2, boundary_instant),
            ],
        )
        # No successor (2026-07-12) bronze partition exists at all.

        transformer = INDOTransformer(tmp_path)
        rows = transformer.run(day, run_id="a2")
        assert rows == 2, "unproven boundary must be RETAINED, not dropped"
        assert transformer.last_partition_filter_boundary_retained_count == 1

    def test_autumn_dst_50_period_day_is_untouched(self, tmp_path: Path) -> None:
        """A 50-settlement-period autumn DST day is filtered on published_at,
        not settlement_date/period, so DST period counts are unaffected."""
        day = date(2026, 10, 25)  # 2026's UK autumn DST transition
        start = datetime(2026, 10, 25, tzinfo=UTC)
        end = datetime(2026, 10, 26, tzinfo=UTC)
        records = [_indo_record(day, period, start) for period in range(1, 51)]
        _write_chunk(tmp_path, "indo", start, end, records)

        transformer = INDOTransformer(tmp_path)
        rows = transformer.run(day, run_id="a2-dst")
        assert rows == 50


# ---------------------------------------------------------------------------
# A-3 — fail-loud, with attribution
# ---------------------------------------------------------------------------


class TestFailLoudAndAttribution:
    def test_absent_declared_column_is_fail_loud(self, tmp_path: Path) -> None:
        class _DropsPublishedAt(INDOTransformer):
            def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
                return super().transform(raw_df).drop("published_at")

        start = datetime(2026, 7, 11, tzinfo=UTC)
        end = datetime(2026, 7, 12, tzinfo=UTC)
        _write_chunk(tmp_path, "indo", start, end, [_indo_record(date(2026, 7, 11), 1, start)])

        transformer = _DropsPublishedAt(tmp_path)
        with pytest.raises(ValueError, match="published_at.*absent"):
            transformer.run(date(2026, 7, 11), run_id="a3")

    def test_unparseable_dimension_value_is_kept_and_counted(self, tmp_path: Path) -> None:
        start = datetime(2026, 7, 11, tzinfo=UTC)
        end = datetime(2026, 7, 12, tzinfo=UTC)
        record_without_publish_time = {
            "settlementDate": "2026-07-11",
            "settlementPeriod": 5,
            "demand": 42.0,
        }
        _write_chunk(
            tmp_path,
            "indo",
            start,
            end,
            [_indo_record(date(2026, 7, 11), 1, start), record_without_publish_time],
        )

        transformer = INDOTransformer(tmp_path)
        rows = transformer.run(date(2026, 7, 11), run_id="a3-null")
        assert rows == 2, "the unclassifiable (null) row is kept, not dropped"
        assert transformer.last_partition_filter_unclassified_count == 1

    def test_in_scope_unresolvable_partition_is_counted_and_filtering_disabled(
        self, tmp_path: Path
    ) -> None:
        """D-7e: an orphan raw body disables the whole partition's filter --
        counted into last_partition_filter_unresolved_count, WARNING, but the
        rows are still written (never silently dropped)."""
        partition_dir = PathBuilder(tmp_path).bronze_date_dir("elexon", "indo", date(2026, 7, 11))
        partition_dir.mkdir(parents=True, exist_ok=True)
        (partition_dir / "raw_20260711T000000Z_orphan01.json").write_text(
            json.dumps(
                {"data": [_indo_record(date(2026, 7, 11), 1, datetime(2026, 7, 11, tzinfo=UTC))]}
            )
        )
        # No sidecar written for this raw body at all.

        transformer = INDOTransformer(tmp_path)
        rows = transformer.run(date(2026, 7, 11), run_id="a3-orphan")
        assert rows == 1
        assert transformer.last_partition_filter_unresolved_count == 1


# ---------------------------------------------------------------------------
# A-12 — neighbour gate (S-1 + S3-1 + S4-2)
# ---------------------------------------------------------------------------


class TestNeighbourGate:
    def test_boundary_row_dropped_only_when_successor_chunk_is_page_complete(
        self, tmp_path: Path
    ) -> None:
        day = date(2026, 7, 11)
        boundary_instant = datetime(2026, 7, 12, tzinfo=UTC)
        _write_chunk(
            tmp_path,
            "indo",
            datetime(2026, 7, 11, tzinfo=UTC),
            boundary_instant,
            [
                _indo_record(day, 1, datetime(2026, 7, 11, tzinfo=UTC)),
                _indo_record(day, 2, boundary_instant),
            ],
        )
        _write_chunk(
            tmp_path,
            "indo",
            boundary_instant,
            datetime(2026, 7, 13, tzinfo=UTC),
            [_indo_record(date(2026, 7, 12), 2, boundary_instant)],
            page=1,
            total_pages=1,
        )

        transformer = INDOTransformer(tmp_path)
        rows = transformer.run(day, run_id="a12-complete")
        assert rows == 1, "only the boundary row is trimmed; the non-boundary row survives"
        assert transformer.last_partition_filter_dropped_count == 1
        assert transformer.last_partition_filter_boundary_retained_count == 0

    def test_boundary_row_retained_when_successor_chunk_is_missing_a_page(
        self, tmp_path: Path
    ) -> None:
        """S3-1 regression: successor declares total_pages=2, only page 1
        present -> the boundary row is RETAINED, boundary_retained == 1,
        WARNING naming INCOMPLETE_PAGE_SET. Demonstrated RED (below, via
        git-stash) against a rev-4 implementation that proved ownership by
        window-existence alone."""
        day = date(2026, 7, 11)
        boundary_instant = datetime(2026, 7, 12, tzinfo=UTC)
        _write_chunk(
            tmp_path,
            "indo",
            datetime(2026, 7, 11, tzinfo=UTC),
            boundary_instant,
            [_indo_record(day, 2, boundary_instant)],
        )
        _write_chunk_missing_page(
            tmp_path,
            "indo",
            boundary_instant,
            datetime(2026, 7, 13, tzinfo=UTC),
            total_pages=2,
            present_page=1,
        )

        transformer = INDOTransformer(tmp_path)
        rows = transformer.run(day, run_id="a12-incomplete")
        assert rows == 1, "the boundary row must be RETAINED, not dropped"
        assert transformer.last_partition_filter_dropped_count == 0
        assert transformer.last_partition_filter_boundary_retained_count == 1

    def test_orphan_body_in_a_non_covering_chunk_of_the_successor_still_drops(
        self, tmp_path: Path
    ) -> None:
        day = date(2026, 7, 11)
        boundary_instant = datetime(2026, 7, 12, tzinfo=UTC)
        _write_chunk(
            tmp_path,
            "indo",
            datetime(2026, 7, 11, tzinfo=UTC),
            boundary_instant,
            [
                _indo_record(day, 1, datetime(2026, 7, 11, tzinfo=UTC)),
                _indo_record(day, 2, boundary_instant),
            ],
        )
        # Successor's covering chunk (the whole day) is durable...
        _write_chunk(
            tmp_path,
            "indo",
            boundary_instant,
            datetime(2026, 7, 13, tzinfo=UTC),
            [_indo_record(date(2026, 7, 12), 2, boundary_instant)],
        )
        # ...but an unrelated orphan body sits in the SAME successor
        # partition directory (a different, non-covering chunk in practice
        # would carry its own distinct window; here we simulate the minimal
        # "body with no sidecar at all" shape).
        successor_dir = PathBuilder(tmp_path).bronze_date_dir("elexon", "indo", date(2026, 7, 12))
        (successor_dir / "raw_20260712T235900Z_orphanbb.json").write_text(json.dumps({"data": []}))

        transformer = INDOTransformer(tmp_path)
        rows = transformer.run(day, run_id="a12-orphan-noncovering")
        assert rows == 1, "an orphan body outside the covering chunk must not block ownership"


# ---------------------------------------------------------------------------
# D-1b — remit filters on timestamp_utc, not published_at (S-2)
# ---------------------------------------------------------------------------


class TestRemitColumnOverride:
    def test_remit_filters_on_timestamp_utc(self, tmp_path: Path) -> None:
        # remit chunks at 23h and drifts off midnight alignment over
        # successive chunks (endpoints.py's max_chunk_hours=23) -- phase this
        # first chunk to land exactly on a midnight boundary so the two
        # chunks fall into genuinely distinct bronze date partitions.
        start = datetime(2026, 7, 11, 1, tzinfo=UTC)
        end = datetime(2026, 7, 12, tzinfo=UTC)
        boundary = end
        early_record = {
            "mrid": "MSG-EARLY",
            "revisionNumber": 1,
            "publishTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "messageType": "Production unavailability",
            "eventStartTime": "2026-07-11T06:00:00Z",
            "eventEndTime": "2026-07-11T12:00:00Z",
        }
        boundary_record = {
            "mrid": "MSG-BOUNDARY",
            "revisionNumber": 1,
            "publishTime": boundary.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "messageType": "Production unavailability",
            "eventStartTime": "2026-07-12T00:00:00Z",
            "eventEndTime": "2026-07-13T00:00:00Z",
        }
        _write_chunk(tmp_path, "remit", start, end, [early_record, boundary_record])
        successor_end = datetime(2026, 7, 12, 22, tzinfo=UTC)
        _write_chunk(tmp_path, "remit", boundary, successor_end, [boundary_record])

        transformer = REMITTransformer(tmp_path)
        rows = transformer.run(date(2026, 7, 11), run_id="remit-override")
        assert rows == 1, "the boundary row is proven durable in the successor and trimmed"
        assert transformer.last_partition_filter_dropped_count == 1


# ---------------------------------------------------------------------------
# G-1 / G-2 preconditions (A-11)
# ---------------------------------------------------------------------------


class TestScopeClassification:
    def test_every_elexon_dataset_is_filtered_or_exempt_with_a_reason(self) -> None:
        """``bod`` is excluded -- see the identical exclusion + rationale in
        ``test_elexon_exact_partition_read.py`` (a pre-existing cross-test
        registry-pollution artifact, out of scope for R2-A)."""
        registered = list_transformers("elexon")
        assert registered, "elexon transformers must be registered before this test runs"
        for _source, dataset in registered:
            if dataset == "bod":
                continue
            in_scope = publication_window_params(dataset) is not None
            exempt = dataset in PUBLICATION_WINDOW_EXEMPT
            assert in_scope or exempt, f"{dataset} is neither filtered nor exempt with a reason"
            assert not (in_scope and exempt), f"{dataset} is both in scope and exempt"

    def test_exempt_dataset_is_never_filtered(self, tmp_path: Path) -> None:
        """fuelinst is N-12 exempt: out of scope even though it is
        PUBLISH_DATETIME with default param names."""
        assert publication_window_params("fuelinst") is None
        assert "fuelinst" in PUBLICATION_WINDOW_EXEMPT
