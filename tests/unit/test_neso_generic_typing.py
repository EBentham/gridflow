"""Per-child typing, exclusion and the capture-wide pass (ADR-034 P-4/P-5).

T-B1-4..6, T-B1-8, T-B1-14, T-B2-1 and T-B2-3 at the function level: each
test types synthetic all-``Utf8`` child tables through ``type_child`` and
``finish_capture`` exactly as the engine does, so a defect is pinned to the
pass that owns it. The engine-level halves (dataset status, counters, other
captures still written) live in ``test_neso_generic_engine.py``.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from _neso_registry_support import column, epoch, record, sp_columns

from gridflow.connectors.neso_data_portal.registry import SchemaRecord
from gridflow.silver.neso_data_portal.casting import (
    AllRowsExcludedError,
    DuplicateEntityKeyError,
    ExclusionTally,
    HeaderEpochError,
    IssueTimeError,
    finish_capture,
    record_columns,
    type_child,
)
from gridflow.silver.neso_data_portal.completion import CaptureContext
from gridflow.silver.neso_data_portal.readers import ChildTable

CAPTURED = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
LAST_MODIFIED = datetime(2026, 10, 7, 11, 55, tzinfo=UTC)


def _ctx(**overrides: Any) -> CaptureContext:
    values: dict[str, Any] = {
        "capture_id": "bronze/neso_data_portal/gen/2026/10/07/raw_x.csv",
        "partition_date": date(2026, 10, 7),
        "body": Path("raw_x.csv"),
        "sidecar": Path("raw_x.meta.json"),
        "capture_written_at": CAPTURED,
        "resource_id": "r",
        "resource_filename": "file.csv",
        "url_type": "upload",
        "body_sha256": "0" * 64,
        "empty_capture": False,
        "published_at": LAST_MODIFIED,
    }
    values.update(overrides)
    return CaptureContext(**values)


def _rec(**kwargs: Any) -> SchemaRecord:
    return SchemaRecord.model_validate(record(**kwargs))


def _table(header: list[str], rows: list[list[str | None]], child_id: str = "") -> ChildTable:
    frame = pl.DataFrame(
        {name: [row[i] for row in rows] for i, name in enumerate(header)},
        schema=dict.fromkeys(header, pl.Utf8),
    )
    return ChildTable(child_id=child_id, header=tuple(header), frame=frame)


def _run(rec: SchemaRecord, *tables: ChildTable, ctx: CaptureContext | None = None) -> pl.DataFrame:
    context = ctx or _ctx()
    typed = [type_child(table, rec, context) for table in tables]
    tally = ExclusionTally()
    for child in typed:
        tally.merge(child.tally)
    return finish_capture([child.frame for child in typed], tally, rec, context, "gen")


SP_HEADER = ["SettlementDate", "SettlementPeriod", "Unit", "Value"]


class TestTemporalRecipes:
    """T-B1-4: one synthetic family per temporal recipe."""

    def test_sp_pair_on_the_spring_day_keeps_sp46_and_excludes_sp47(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Detects a 48-period assumption on the 46-period spring-forward day."""
        rows = [["2026-03-29", "46", "A", "1"], ["2026-03-29", "47", "A", "2"]]
        with caplog.at_level(logging.WARNING):
            out = _run(_rec(), _table(SP_HEADER, rows))
        assert out["settlement_period"].to_list() == [46]
        assert out["timestamp_utc"].to_list() == [datetime(2026, 3, 29, 22, 30, tzinfo=UTC)]
        assert "settlement_period" in caplog.text

    def test_sp_pair_on_the_autumn_day_keeps_sp49_and_sp50(self) -> None:
        """Detects a 48-period cap on the 50-period autumn-back day."""
        rows = [["2026-10-25", "49", "A", "1"], ["2026-10-25", "50", "A", "2"]]
        out = _run(_rec(), _table(SP_HEADER, rows))
        assert out["timestamp_utc"].to_list() == [
            datetime(2026, 10, 25, 23, 0, tzinfo=UTC),
            datetime(2026, 10, 25, 23, 30, tzinfo=UTC),
        ]

    def test_utc_instant(self) -> None:
        cols = [
            column("At", "at", "datetime", nullable=False, format="%Y-%m-%dT%H:%M:%S%z"),
            column("V", "v", "float64"),
        ]
        rec = _rec(
            epochs=[epoch(cols)],
            temporal={"kind": "utc_instant", "column": "at"},
            entity_key=("at",),
        )
        out = _run(rec, _table(["At", "V"], [["2026-10-07T13:00:00+01:00", "1"]]))
        assert out["timestamp_utc"].to_list() == [datetime(2026, 10, 7, 12, tzinfo=UTC)]

    def test_date_sp1_and_month(self) -> None:
        """date_sp1 is SP1 of the GB day (23:00Z the day before in BST); month is
        SP1 of the month's first day."""
        cols = [column("Day", "trading_day", "date", nullable=False), column("V", "v", "float64")]
        rec = _rec(
            epochs=[epoch(cols)],
            temporal={"kind": "date_sp1", "date_column": "trading_day"},
            entity_key=("trading_day",),
        )
        out = _run(rec, _table(["Day", "V"], [["2026-08-16", "1"], ["2026-01-15", "2"]]))
        assert out["timestamp_utc"].to_list() == [
            datetime(2026, 8, 15, 23, tzinfo=UTC),
            datetime(2026, 1, 15, 0, tzinfo=UTC),
        ]
        month_cols = [
            column("Month", "month_start", "date", nullable=False, format="%Y-%m-%d"),
            column("V", "v", "float64"),
        ]
        rec = _rec(
            epochs=[epoch(month_cols)],
            temporal={"kind": "month", "date_column": "month_start"},
            entity_key=("month_start",),
        )
        out = _run(rec, _table(["Month", "V"], [["2026-08-20", "1"]]))
        assert out["timestamp_utc"].to_list() == [datetime(2026, 7, 31, 23, tzinfo=UTC)]

    def test_none_is_the_capture_instant(self) -> None:
        rec = _rec(
            epochs=[epoch([column("K", "k", nullable=False), column("V", "v")])],
            temporal={"kind": "none"},
            entity_key=("k",),
        )
        out = _run(rec, _table(["K", "V"], [["a", "1"]]))
        assert out["timestamp_utc"].to_list() == [CAPTURED]
        assert out.columns == list(record_columns(rec))


class TestLocalAmbiguity:
    """T-B1-5: Europe/London's repeated and missing hours."""

    @staticmethod
    def _local(ambiguous: str) -> SchemaRecord:
        cols = [
            column(
                "At",
                "at",
                "datetime",
                nullable=False,
                format="%Y-%m-%d %H:%M",
                zone="Europe/London",
                zone_evidence="vendor states UK local time",
                ambiguous=ambiguous,
            ),
            column("V", "v", "float64"),
        ]
        return _rec(
            epochs=[epoch(cols)],
            temporal={"kind": "local_instant", "column": "at"},
            entity_key=("at",),
        )

    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            ("earliest", datetime(2026, 10, 25, 0, 30, tzinfo=UTC)),
            ("latest", datetime(2026, 10, 25, 1, 30, tzinfo=UTC)),
        ],
    )
    def test_ambiguous_hour_resolves_by_the_declared_rule(
        self, rule: str, expected: datetime
    ) -> None:
        out = _run(self._local(rule), _table(["At", "V"], [["2026-10-25 01:30", "1"]]))
        assert out["timestamp_utc"].to_list() == [expected]

    def test_raise_fails_the_capture(self) -> None:
        with pytest.raises(pl.exceptions.ComputeError):
            _run(self._local("raise"), _table(["At", "V"], [["2026-10-25 01:30", "1"]]))

    def test_a_non_existent_hour_fails_the_capture(self) -> None:
        with pytest.raises(pl.exceptions.ComputeError):
            _run(self._local("earliest"), _table(["At", "V"], [["2026-03-29 01:30", "1"]]))


class TestHeaderEpochs:
    """T-B1-6: header-epoch switch."""

    @staticmethod
    def _two_epochs() -> SchemaRecord:
        first = sp_columns()[:3]
        second = [*sp_columns()]
        return _rec(epochs=[epoch(first), epoch(second)])

    def test_both_epochs_type_and_a_missing_column_is_a_typed_null(self) -> None:
        rec = self._two_epochs()
        old = _run(rec, _table(SP_HEADER[:3], [["2026-10-07", "1", "A"]]))
        new = _run(rec, _table(SP_HEADER, [["2026-10-07", "1", "A", "2.5"]]))
        assert old.schema == new.schema
        assert old["value"].to_list() == [None]
        assert new["value"].to_list() == [2.5]

    def test_a_third_header_fails(self) -> None:
        with pytest.raises(HeaderEpochError):
            _run(self._two_epochs(), _table(["SettlementDate", "Other"], [["2026-10-07", "1"]]))

    def test_two_children_of_different_epochs_type_once_each_and_concatenate(self) -> None:
        """Guards against a second typing pass: an already-typed frame could not
        be re-cast from its vendor header."""
        rec = self._two_epochs()
        out = _run(
            rec,
            _table(SP_HEADER[:3], [["2026-10-07", "1", "A"]], child_id="a"),
            _table(SP_HEADER, [["2026-10-07", "1", "B", "3"]], child_id="b"),
        )
        assert out["unit"].to_list() == ["A", "B"]
        assert out["value"].to_list() == [None, 3.0]


class TestNullTokensAndStrictCasts:
    """T-B1-7 (function level): a declared token is null; anything else fails."""

    def test_declared_token_becomes_a_kept_null(self) -> None:
        cols = [*sp_columns()[:3], column("Value", "value", "float64", null_tokens=["N/A"])]
        out = _run(_rec(epochs=[epoch(cols)]), _table(SP_HEADER, [["2026-10-07", "1", "A", "N/A"]]))
        assert out["value"].to_list() == [None]

    def test_undeclared_bad_value_fails(self) -> None:
        with pytest.raises(pl.exceptions.InvalidOperationError):
            _run(_rec(), _table(SP_HEADER, [["2026-10-07", "1", "A", "x"]]))


class TestExclusion:
    """T-B1-8 (function level): P-4 step 5 inside one sp_pair capture."""

    def test_each_rule_excludes_and_counts(self, caplog: pytest.LogCaptureFixture) -> None:
        cols = [*sp_columns()[:3], column("Value", "value", "float64", nullable=False, max=100)]
        rec = _rec(epochs=[epoch(cols)])
        rows = [
            ["2026-10-07", "1", "OK1", "1"],
            [None, "2", "NODATE", "1"],
            ["2026-10-07", None, "NOPERIOD", "1"],
            ["2026-10-07", "3", "NOVALUE", None],
            ["2026-10-07", "4", "BIG", "500"],
            ["2026-10-07", "5", "OK2", "2"],
        ]
        context = _ctx()
        typed = type_child(_table(SP_HEADER, rows), rec, context)
        assert typed.tally.counts == {"null": 3, "range": 1}
        assert len(typed.tally.samples) == 4
        with caplog.at_level(logging.WARNING):
            out = finish_capture([typed.frame], typed.tally, rec, context, "gen")
        assert out["unit"].to_list() == ["OK1", "OK2"]
        assert "excluded 4 row(s)" in caplog.text
        assert "BIG" in caplog.text

    def test_a_duplicate_key_fails_the_capture(self) -> None:
        rows = [["2026-10-07", "1", "A", "1"], ["2026-10-07", "1", "A", "2"]]
        with pytest.raises(DuplicateEntityKeyError):
            _run(_rec(), _table(SP_HEADER, rows))

    def test_every_row_excluded_fails_the_capture(self) -> None:
        with pytest.raises(AllRowsExcludedError):
            _run(_rec(), _table(SP_HEADER, [[None, "1", "A", "1"]]))


class TestEpochScopedExclusion:
    """T-B1-14 (I-2): a row is judged only by its own epoch's rules."""

    def test_rules_never_cross_epochs(self, caplog: pytest.LogCaptureFixture) -> None:
        epoch_a = [
            column("RowId", "row_id", "int64", nullable=False),
            column("Value", "value", "float64", nullable=True, max=100),
        ]
        epoch_b = [
            column("RowId", "row_id", "int64", nullable=False),
            column("Value", "value", "float64", nullable=False, max=50),
            column("Extra", "extra", nullable=False),
        ]
        rec = _rec(
            epochs=[epoch(epoch_a), epoch(epoch_b)],
            temporal={"kind": "none"},
            entity_key=("row_id",),
        )
        child_a = _table(["RowId", "Value"], [["1", None], ["2", "75"]], child_id="a")
        child_b = _table(
            ["RowId", "Value", "Extra"],
            [["3", "40", "x"], ["4", "60", "y"], ["5", "40", None]],
            child_id="b",
        )
        context = _ctx()
        typed = [type_child(child, rec, context) for child in (child_a, child_b)]
        tally = ExclusionTally()
        for child in typed:
            tally.merge(child.tally)
        with caplog.at_level(logging.WARNING):
            out = finish_capture([child.frame for child in typed], tally, rec, context, "gen")
        assert sorted(out["row_id"].to_list()) == [1, 2, 3]
        assert tally.counts == {"range": 1, "null": 1}
        assert tally.total == 2
        assert "row_id=4" in caplog.text and "row_id=5" in caplog.text


class TestIssueTime:
    """T-B2-1: a null issue time fails the capture, before exclusion."""

    @staticmethod
    def _issued(nullable: bool) -> SchemaRecord:
        cols = [
            *sp_columns(),
            column(
                "Issued",
                "issued",
                "datetime",
                nullable=nullable,
                format="%Y-%m-%dT%H:%M:%S",
                zone="UTC",
            ),
        ]
        return _rec(
            epochs=[epoch(cols, issue={"kind": "data_column", "column": "issued"})],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
        )

    @pytest.mark.parametrize("nullable", [True, False])
    def test_data_column_null_fails_even_when_the_column_is_non_nullable(
        self, nullable: bool
    ) -> None:
        """With ``nullable=False`` exclusion would silently drop the row; the
        failure proves P-4 step 4 runs before step 5."""
        rows = [["2026-10-07", "1", "A", "1", None]]
        with pytest.raises(IssueTimeError):
            _run(self._issued(nullable), _table([*SP_HEADER, "Issued"], rows))

    def test_filename_token_that_does_not_match_fails(self) -> None:
        issue = {"kind": "filename_token", "pattern": r"^(\d{12})_f\.csv$", "format": "%Y%m%d%H%M"}
        rec = _rec(
            epochs=[epoch(sp_columns(), issue=issue)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
        )
        table = _table(SP_HEADER, [["2026-10-07", "1", "A", "1"]])
        with pytest.raises(IssueTimeError):
            _run(rec, table, ctx=_ctx(resource_filename="nope.csv"))
        out = _run(rec, table, ctx=_ctx(resource_filename="202610070930_f.csv"))
        assert out["issue_time"].to_list() == [datetime(2026, 10, 7, 9, 30, tzinfo=UTC)]


class TestPublishedAt:
    """T-B2-3 (function level): ``published_at`` per vintage recipe."""

    def test_ckan_last_modified_is_the_capture_value(self) -> None:
        out = _run(_rec(), _table(SP_HEADER, [["2026-10-07", "1", "A", "1"]]))
        assert out["published_at"].to_list() == [LAST_MODIFIED]

    def test_capture_fallback_is_null(self) -> None:
        rec = _rec(vintage="capture_fallback")
        out = _run(
            rec, _table(SP_HEADER, [["2026-10-07", "1", "A", "1"]]), ctx=_ctx(published_at=None)
        )
        assert out["published_at"].to_list() == [None]
        assert out["published_at"].dtype == pl.Datetime("us", "UTC")

    def test_issue_time_evidenced_is_the_issue_time(self) -> None:
        issue = {"kind": "filename_token", "pattern": r"^(\d{12})_f\.csv$", "format": "%Y%m%d%H%M"}
        rec = _rec(
            epochs=[epoch(sp_columns(), issue=issue)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
            vintage="issue_time_evidenced",
            vintage_evidence="the token is the vendor issue instant",
        )
        out = _run(
            rec,
            _table(SP_HEADER, [["2026-10-07", "1", "A", "1"]]),
            ctx=_ctx(resource_filename="202610070930_f.csv", published_at=None),
        )
        assert out["published_at"].to_list() == [datetime(2026, 10, 7, 9, 30, tzinfo=UTC)]
