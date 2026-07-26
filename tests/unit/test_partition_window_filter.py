"""Unit tests for the sidecar request-window filter primitive (R2-A Task 1).

Covers D-7(a) sidecar validation, D-7(e) all-or-nothing partition pairing,
D-3b's chunk-scoped durability proof, D-3d's per-instant lower-bound
ownership, D-3e's reason propagation, D-5's drop-all refusal, and (Sol
ruling, 2026-07-26) ``exclude_out_of_window``'s HALF_OPEN interval
semantics -- unconditional exclusion, no neighbour proof at all.
"""

from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import polars as pl

from gridflow.silver.partition_window import (
    OwnershipVerdict,
    RequestWindow,
    WindowReason,
    covering_chunk_is_durable,
    exclude_out_of_window,
    filter_frame_to_window,
    neighbour_owns,
    partition_request_window,
    request_window_from_sidecar,
)

if TYPE_CHECKING:
    from pathlib import Path


def _write_raw(
    partition_dir: Path,
    name: str,
    *,
    source: str = "elexon",
    dataset: str = "indo",
    request_params: dict[str, object] | None = None,
    page: object = 1,
    total_pages: object = 1,
    with_body: bool = True,
    with_sidecar: bool = True,
    data_date: str = "2026-07-11",
) -> None:
    """Write a raw body + sidecar pair (or one half) into ``partition_dir``."""
    partition_dir.mkdir(parents=True, exist_ok=True)
    if with_body:
        (partition_dir / f"{name}.json").write_text("{}")
    if with_sidecar:
        meta = {
            "source": source,
            "dataset": dataset,
            "data_date": data_date,
            "request_params": request_params if request_params is not None else {},
            "page": page,
            "total_pages": total_pages,
        }
        (partition_dir / f"{name}.meta.json").write_text(json.dumps(meta))


def _window_params(start: str, end: str) -> dict[str, str]:
    return {"publishDateTimeFrom": start, "publishDateTimeTo": end}


# ---------------------------------------------------------------------------
# request_window_from_sidecar (D-7a)
# ---------------------------------------------------------------------------


class TestRequestWindowFromSidecar:
    def test_valid_sidecar_resolves_window(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-11T00:00:00Z", "2026-07-12T00:00:00Z"),
        )
        window, reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert reason == WindowReason.OK
        assert window == RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )

    def test_missing_sidecar_is_no_sidecar(self, tmp_path: Path) -> None:
        window, reason = request_window_from_sidecar(
            tmp_path / "missing.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.NO_SIDECAR

    def test_corrupt_json_is_no_sidecar(self, tmp_path: Path) -> None:
        meta_path = tmp_path / "raw_1.meta.json"
        meta_path.write_text("{not json")
        window, reason = request_window_from_sidecar(
            meta_path,
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.NO_SIDECAR

    def test_identity_mismatch(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            dataset="fuelhh",
            request_params=_window_params("2026-07-11T00:00:00Z", "2026-07-12T00:00:00Z"),
        )
        window, reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.IDENTITY_MISMATCH

    def test_no_request_params(self, tmp_path: Path) -> None:
        _write_raw(tmp_path, "raw_1", request_params={})
        window, reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.NO_REQUEST_PARAMS

    def test_missing_param(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path, "raw_1", request_params={"publishDateTimeFrom": "2026-07-11T00:00:00Z"}
        )
        window, reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.MISSING_PARAM

    def test_unparseable_bound(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params={
                "publishDateTimeFrom": "not-a-date",
                "publishDateTimeTo": "2026-07-12T00:00:00Z",
            },
        )
        window, reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.UNPARSEABLE_BOUND

    def test_non_positive_range(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-11T00:00:00Z"),
        )
        window, reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.NON_POSITIVE_RANGE

    def test_entsoe_compact_format_parses(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            source="entsoe",
            dataset="day_ahead_prices",
            request_params={"periodStart": "202401150000", "periodEnd": "202401160000"},
        )
        window, reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "periodStart",
            "periodEnd",
            expect_source="entsoe",
            expect_dataset="day_ahead_prices",
        )
        assert reason == WindowReason.OK
        assert window is not None
        assert window.start == datetime(2024, 1, 15, tzinfo=UTC)
        assert window.end == datetime(2024, 1, 16, tzinfo=UTC)


# ---------------------------------------------------------------------------
# partition_request_window (D-7e: all-or-nothing pairing)
# ---------------------------------------------------------------------------


class TestPartitionRequestWindow:
    def test_single_valid_member_resolves(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-11T00:00:00Z", "2026-07-12T00:00:00Z"),
        )
        window, reason = partition_request_window(
            tmp_path,
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert reason == WindowReason.OK
        assert window is not None

    def test_no_raw_bodies_is_no_sidecar(self, tmp_path: Path) -> None:
        window, reason = partition_request_window(
            tmp_path,
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.NO_SIDECAR

    def test_orphan_raw_body_disables_filtering_for_the_whole_partition(
        self, tmp_path: Path
    ) -> None:
        """A-14: one orphan body (no sidecar) disables the ENTIRE partition,
        even though a second, fully-valid member is present."""
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-11T00:00:00Z", "2026-07-11T12:00:00Z"),
        )
        _write_raw(tmp_path, "raw_2", with_sidecar=False)

        window, reason = partition_request_window(
            tmp_path,
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.ORPHAN_BODY

    def test_orphan_raw_body_red_against_a_subset_resolution_implementation(
        self, tmp_path: Path
    ) -> None:
        """A-14's RED demonstration: a rev-3-style implementation that resolved
        the window from ANY subset of valid sidecars (ignoring unpaired bodies
        entirely) would have returned a window here instead of ORPHAN_BODY.
        This test pins the all-or-nothing behaviour directly; the subset
        alternative is shown by asserting the valid member alone WOULD resolve
        (i.e. a subset-based reader had material to work with and chose wrongly
        to use it)."""
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-11T00:00:00Z", "2026-07-11T12:00:00Z"),
        )
        _write_raw(tmp_path, "raw_2", with_sidecar=False)

        # The all-or-nothing reader refuses.
        window, reason = partition_request_window(
            tmp_path,
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert window is None
        assert reason == WindowReason.ORPHAN_BODY

        # A subset-resolution implementation would have found raw_1's sidecar
        # alone sufficient to resolve a (wrong, silently narrowed) window.
        subset_window, subset_reason = request_window_from_sidecar(
            tmp_path / "raw_1.meta.json",
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert subset_reason == WindowReason.OK
        assert subset_window is not None

    def test_window_spans_min_start_max_end_across_members(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-11T00:00:00Z", "2026-07-11T12:00:00Z"),
        )
        _write_raw(
            tmp_path,
            "raw_2",
            request_params=_window_params("2026-07-11T12:00:00Z", "2026-07-12T00:00:00Z"),
        )
        window, reason = partition_request_window(
            tmp_path,
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert reason == WindowReason.OK
        assert window == RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )


# ---------------------------------------------------------------------------
# covering_chunk_is_durable / neighbour_owns (D-3b, S3-1)
# ---------------------------------------------------------------------------


class TestCoveringChunkIsDurable:
    def test_single_page_covering_chunk_proves_durability(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-13T00:00:00Z"),
            page=1,
            total_pages=1,
        )
        verdict = covering_chunk_is_durable(
            tmp_path,
            datetime(2026, 7, 12, 6, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(True, WindowReason.OK)

    def test_missing_page_in_the_covering_chunk_fails_the_proof(self, tmp_path: Path) -> None:
        """S3-1 regression: total_pages=2 declared, only page 1 present."""
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-13T00:00:00Z"),
            page=1,
            total_pages=2,
        )
        verdict = covering_chunk_is_durable(
            tmp_path,
            datetime(2026, 7, 12, 6, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(False, WindowReason.INCOMPLETE_PAGE_SET)

    def test_unpaired_page_in_the_covering_chunk_fails_the_proof(self, tmp_path: Path) -> None:
        """A sidecar in the covering group with no body on disk fails pairing."""
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-13T00:00:00Z"),
            page=1,
            total_pages=1,
            with_body=False,
        )
        verdict = covering_chunk_is_durable(
            tmp_path,
            datetime(2026, 7, 12, 6, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(False, WindowReason.ORPHAN_BODY)

    def test_orphan_body_in_a_NON_covering_chunk_does_not_block_ownership(  # noqa: N802
        self, tmp_path: Path
    ) -> None:
        """The test that stops a future partition-wide 'simplification': an
        orphan body sitting in a DIFFERENT window group must not block the
        covering group's own (fully durable) verdict."""
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-13T00:00:00Z"),
            page=1,
            total_pages=1,
        )
        # An unrelated orphan body (no sidecar at all) elsewhere in the
        # partition -- e.g. a crash mid-write of a later, non-covering chunk.
        _write_raw(tmp_path, "raw_orphan", with_sidecar=False)

        verdict = covering_chunk_is_durable(
            tmp_path,
            datetime(2026, 7, 12, 6, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(True, WindowReason.OK)

    def test_absent_total_pages_on_legacy_sidecar_fails_the_proof(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-13T00:00:00Z"),
            page=None,
            total_pages=None,
        )
        verdict = covering_chunk_is_durable(
            tmp_path,
            datetime(2026, 7, 12, 6, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(False, WindowReason.INCOMPLETE_PAGE_SET)

    def test_no_covering_chunk_when_partition_absent(self, tmp_path: Path) -> None:
        verdict = covering_chunk_is_durable(
            tmp_path / "does-not-exist",
            datetime(2026, 7, 12, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(False, WindowReason.NO_COVERING_CHUNK)

    def test_no_covering_chunk_when_instant_outside_every_group(self, tmp_path: Path) -> None:
        _write_raw(
            tmp_path,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-13T00:00:00Z"),
        )
        verdict = covering_chunk_is_durable(
            tmp_path,
            datetime(2026, 7, 14, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(False, WindowReason.NO_COVERING_CHUNK)

    def test_ownership_verdict_carries_the_failing_reason(self, tmp_path: Path) -> None:
        """S4-2: the verdict is never a bare boolean."""
        verdict = covering_chunk_is_durable(
            tmp_path,
            datetime(2026, 7, 12, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict.owned is False
        assert isinstance(verdict.reason, WindowReason)
        assert verdict.reason == WindowReason.NO_COVERING_CHUNK


class TestNeighbourOwns:
    def test_resolves_the_partition_for_the_instants_own_date(self, tmp_path: Path) -> None:
        successor = tmp_path / "2026" / "07" / "12"
        _write_raw(
            successor,
            "raw_1",
            request_params=_window_params("2026-07-12T00:00:00Z", "2026-07-13T00:00:00Z"),
        )
        verdict = neighbour_owns(
            tmp_path,
            datetime(2026, 7, 12, 0, 0, tzinfo=UTC),
            "publishDateTimeFrom",
            "publishDateTimeTo",
            expect_source="elexon",
            expect_dataset="indo",
        )
        assert verdict == OwnershipVerdict(True, WindowReason.OK)

    def test_lower_bound_ownership_is_resolved_per_instant_not_per_date(
        self, tmp_path: Path
    ) -> None:
        """D-3d / S4-1: a predecessor chunk clamped at [D T06:00, D+1 00:00)
        (as ``day_subwindows`` produces at a range edge) owns an instant
        inside it but NOT an earlier instant on the same UTC date."""
        predecessor = tmp_path / "2026" / "07" / "11"
        _write_raw(
            predecessor,
            "raw_1",
            source="entsoe",
            dataset="day_ahead_prices",
            request_params={"periodStart": "202607110600", "periodEnd": "202607120000"},
        )

        inside = datetime(2026, 7, 11, 20, tzinfo=UTC)
        outside = datetime(2026, 7, 11, 3, tzinfo=UTC)

        verdict_inside = neighbour_owns(
            tmp_path,
            inside,
            "periodStart",
            "periodEnd",
            expect_source="entsoe",
            expect_dataset="day_ahead_prices",
        )
        verdict_outside = neighbour_owns(
            tmp_path,
            outside,
            "periodStart",
            "periodEnd",
            expect_source="entsoe",
            expect_dataset="day_ahead_prices",
        )

        assert verdict_inside == OwnershipVerdict(True, WindowReason.OK)
        assert verdict_outside == OwnershipVerdict(False, WindowReason.NO_COVERING_CHUNK)
        # The two instants share a UTC calendar date but resolve differently --
        # the exact invariant a frozenset[date] implementation would collapse.
        assert inside.date() == outside.date()


# ---------------------------------------------------------------------------
# filter_frame_to_window (D-3, D-3d, D-3e, D-5)
# ---------------------------------------------------------------------------


class TestFilterFrameToWindow:
    def _frame(self, values: list[datetime | None]) -> pl.DataFrame:
        return pl.DataFrame({"published_at": pl.Series(values, dtype=pl.Datetime("us", "UTC"))})

    def test_absent_column_is_a_noop(self) -> None:
        df = pl.DataFrame({"other": [1, 2, 3]})
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(True, WindowReason.OK),
        )
        assert result.frame.equals(df)
        assert result.dropped == 0
        assert result.refused is False

    def test_boundary_row_dropped_when_upper_bound_owned(self) -> None:
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )
        df = self._frame(
            [
                datetime(2026, 7, 11, 12, tzinfo=UTC),
                datetime(2026, 7, 12, tzinfo=UTC),  # at window.end
            ]
        )
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(True, WindowReason.OK),
        )
        assert result.dropped == 1
        assert len(result.frame) == 1
        assert result.boundary_retained == 0
        assert result.retained_reasons == ()

    def test_boundary_row_retained_when_upper_bound_unproven(self) -> None:
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )
        df = self._frame([datetime(2026, 7, 12, tzinfo=UTC)])
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(False, WindowReason.INCOMPLETE_PAGE_SET),
        )
        assert result.dropped == 0
        assert len(result.frame) == 1
        assert result.boundary_retained == 1
        assert result.retained_reasons == ((WindowReason.INCOMPLETE_PAGE_SET, 1),)

    def test_below_window_rows_kept_and_counted_when_lower_bound_none(self) -> None:
        """D-3: Elexon never enforces the lower bound at all -- kept, counted
        into below_window, but NOT attributed as a proof failure."""
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )
        df = self._frame([datetime(2026, 7, 10, 23, tzinfo=UTC)])
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(False, WindowReason.NOT_RESOLVED),
            lower_bound_ownership=None,
        )
        assert result.dropped == 0
        assert result.below_window == 1
        assert result.boundary_retained == 0
        assert result.retained_reasons == ()

    def test_below_window_row_dropped_when_owned(self) -> None:
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        instant = datetime(2026, 7, 10, 23, tzinfo=UTC)
        df = self._frame([instant, datetime(2026, 7, 11, 12, tzinfo=UTC)])
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(False, WindowReason.NOT_RESOLVED),
            lower_bound_ownership={instant: OwnershipVerdict(True, WindowReason.OK)},
        )
        assert result.dropped == 1
        assert len(result.frame) == 1
        assert result.below_window == 1
        assert result.boundary_retained == 0

    def test_unresolved_instant_defaults_to_retained_with_NOT_RESOLVED(self) -> None:  # noqa: N802
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        instant = datetime(2026, 7, 10, 23, tzinfo=UTC)
        df = self._frame([instant])
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(False, WindowReason.NOT_RESOLVED),
            lower_bound_ownership={},  # instant absent from the mapping
        )
        assert result.dropped == 0
        assert result.boundary_retained == 1
        assert result.retained_reasons == ((WindowReason.NOT_RESOLVED, 1),)

    def test_retained_reasons_attribute_every_unenforced_bound(self) -> None:
        """A-3: every boundary_retained event is attributed by reason, not
        just counted."""
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        below_a = datetime(2026, 7, 10, 20, tzinfo=UTC)
        below_b = datetime(2026, 7, 10, 22, tzinfo=UTC)
        df = self._frame([below_a, below_b, datetime(2026, 7, 12, tzinfo=UTC)])
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(False, WindowReason.INCOMPLETE_PAGE_SET),
            lower_bound_ownership={
                below_a: OwnershipVerdict(False, WindowReason.NO_COVERING_CHUNK),
                below_b: OwnershipVerdict(False, WindowReason.ORPHAN_BODY),
            },
        )
        assert result.boundary_retained == 3
        assert dict(result.retained_reasons) == {
            WindowReason.INCOMPLETE_PAGE_SET: 1,
            WindowReason.NO_COVERING_CHUNK: 1,
            WindowReason.ORPHAN_BODY: 1,
        }

    def test_unclassified_null_rows_are_kept_and_counted(self) -> None:
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )
        df = self._frame([None, datetime(2026, 7, 11, 12, tzinfo=UTC)])
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(True, WindowReason.OK),
        )
        assert result.unclassified == 1
        assert len(result.frame) == 2  # the null row is kept, not dropped

    def test_drop_all_refusal_keeps_the_frame(self) -> None:
        """D-5: a filter may never empty an otherwise non-empty frame."""
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("publishDateTimeFrom", "publishDateTimeTo"),
        )
        df = self._frame([datetime(2026, 7, 12, tzinfo=UTC)])
        result = filter_frame_to_window(
            df,
            "published_at",
            window,
            upper_bound_ownership=OwnershipVerdict(True, WindowReason.OK),
        )
        assert result.refused is True
        assert result.dropped == 0
        assert len(result.frame) == 1


# ---------------------------------------------------------------------------
# exclude_out_of_window (HALF_OPEN interval semantics -- Sol ruling,
# 2026-07-26): unconditional exclusion, no ownership question, no neighbour
# proof at all. Contrast with TestFilterFrameToWindow above (CLOSED interval,
# Elexon), which stays untouched and unchanged by this ruling.
# ---------------------------------------------------------------------------


class TestExcludeOutOfWindow:
    def _frame(self, values: list[datetime | None]) -> pl.DataFrame:
        return pl.DataFrame({"timestamp_utc": pl.Series(values, dtype=pl.Datetime("us", "UTC"))})

    def test_absent_column_is_a_noop(self) -> None:
        df = pl.DataFrame({"other": [1, 2, 3]})
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        result = exclude_out_of_window(df, "timestamp_utc", window)
        assert result.frame.equals(df)
        assert result.dropped == 0
        assert result.refused is False

    def test_rows_outside_the_window_are_dropped_on_both_sides_unconditionally(self) -> None:
        """No OwnershipVerdict / neighbour proof is passed at all -- this is
        the whole point of HALF_OPEN semantics."""
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        df = self._frame(
            [
                datetime(2026, 7, 10, 23, tzinfo=UTC),  # below start
                datetime(2026, 7, 11, 12, tzinfo=UTC),  # in window
                datetime(2026, 7, 12, tzinfo=UTC),  # at/after end
            ]
        )
        result = exclude_out_of_window(df, "timestamp_utc", window)
        assert result.dropped == 2
        assert len(result.frame) == 1
        assert result.frame["timestamp_utc"].to_list() == [datetime(2026, 7, 11, 12, tzinfo=UTC)]
        assert result.boundary_retained == 0
        assert result.retained_reasons == ()

    def test_no_neighbour_bronze_needed_no_ownership_verdict_argument_exists(self) -> None:
        """The function signature itself proves the point: there is no
        ``upper_bound_ownership``/``lower_bound_ownership`` parameter to
        pass, unlike ``filter_frame_to_window``."""
        params = inspect.signature(exclude_out_of_window).parameters
        assert "upper_bound_ownership" not in params
        assert "lower_bound_ownership" not in params

    def test_unclassified_null_rows_are_kept_and_counted(self) -> None:
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        df = self._frame([None, datetime(2026, 7, 11, 12, tzinfo=UTC)])
        result = exclude_out_of_window(df, "timestamp_utc", window)
        assert result.unclassified == 1
        assert len(result.frame) == 2  # the null row is kept, not dropped

    def test_drop_all_is_performed_not_refused(self) -> None:
        """D-5's REFUSAL does NOT carry over to HALF_OPEN semantics: unlike
        filter_frame_to_window's CLOSED path, a 100%-out-of-window frame is
        still excluded, not kept -- the TRIM ruling is unconditional even at
        100%. ``all_dropped`` signals the case for ERROR-level logging
        instead of the usual per-row WARNING."""
        window = RequestWindow(
            start=datetime(2026, 7, 11, tzinfo=UTC),
            end=datetime(2026, 7, 12, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        df = self._frame([datetime(2026, 7, 12, tzinfo=UTC)])
        result = exclude_out_of_window(df, "timestamp_utc", window)
        assert result.refused is False
        assert result.all_dropped is True
        assert result.dropped == 1
        assert len(result.frame) == 0
