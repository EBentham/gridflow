"""Revision selection: tie-break, as-of and whole-capture (ADR-034 P-10; B3, B4).

Every selection is run through BOTH renderers — the DuckDB ``SELECT`` from
``latest_select_sql`` (bound with an ISO ``$as_of`` string, C-7) and the
Polars ``select_latest_vintage`` — and the two results must agree, so a
divergence between the catalogue and Polars readers is caught where it starts.
"""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta
from typing import Any

import duckdb
import polars as pl
import pytest

from gridflow.silver.latest_views import (
    _SETTLEMENT_RUN_RANK,
    LatestViewSpec,
    latest_select_sql,
    latest_view_sql,
    select_latest_vintage,
)

TS = pl.Datetime("us", "UTC")
TIE = ("capture_written_at", "bronze_capture_id")
KEY_SPEC = LatestViewSpec(
    key_columns=("settlement_date", "settlement_period", "unit"),
    order_columns=("issue_time", "available_at"),
    tiebreak_columns=TIE,
)
WHOLE_SPEC = LatestViewSpec(
    key_columns=(),
    mode="whole_capture",
    completion_relation="completion",
    completion_family="fam",
    tiebreak_columns=TIE,
)


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 7, hour, minute, tzinfo=UTC)


def _sql(
    frame: pl.DataFrame,
    spec: LatestViewSpec,
    as_of: datetime | None = None,
    completions: pl.DataFrame | None = None,
) -> pl.DataFrame:
    con = duckdb.connect(":memory:")
    try:
        con.register("base", frame.to_arrow())
        if completions is not None:
            con.register("completion", completions.to_arrow())
        select = latest_select_sql("base", spec, set(frame.columns), as_of_param=as_of is not None)
        assert select is not None
        params = {"as_of": as_of.isoformat()} if as_of is not None else None
        out = con.execute(select, params).pl() if params else con.execute(select).pl()
    finally:
        con.close()
    return _utc(out)


def _polars(
    frame: pl.DataFrame,
    spec: LatestViewSpec,
    as_of: datetime | None = None,
    completions: pl.DataFrame | None = None,
) -> pl.DataFrame:
    lazy = completions.lazy() if completions is not None else None
    return select_latest_vintage(frame.lazy(), spec, as_of, completions=lazy).collect()


def _utc(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col(name).dt.convert_time_zone("UTC").cast(TS)
        for name, dtype in frame.schema.items()
        if isinstance(dtype, pl.Datetime)
    )


def _both(frame: pl.DataFrame, spec: LatestViewSpec, **kwargs: Any) -> list[str]:
    """Run both renderers; assert they agree; return the winning capture ids."""
    sql = _sql(frame, spec, **kwargs)
    pol = _polars(frame, spec, **kwargs)
    sort = sorted(frame.columns)
    assert sql.select(sort).sort(sort).to_dicts() == pol.select(sort).sort(sort).to_dicts()
    return sorted(pol["bronze_capture_id"].to_list())


def _rows(*rows: dict[str, Any]) -> pl.DataFrame:
    schema = {
        "settlement_date": pl.Date,
        "settlement_period": pl.Int64,
        "unit": pl.Utf8,
        "value": pl.Float64,
        "issue_time": TS,
        "published_at": TS,
        "available_at": TS,
        "capture_written_at": TS,
        "bronze_capture_id": pl.Utf8,
    }
    base = {
        "settlement_date": date(2026, 10, 7),
        "settlement_period": 1,
        "unit": "U",
        "value": 0.0,
        "issue_time": None,
        "published_at": None,
    }
    return pl.DataFrame([{**base, **row} for row in rows], schema=schema)


class TestAsOfBeforeSelection:
    def test_t_b3_2_a_later_issue_not_yet_available_never_wins(self) -> None:
        """Detects as-of applied after selection: the 12:00 row has the later
        issue time, so selecting first would pick it and the as-of filter would
        then drop the key entirely, losing the 08:00 row that WAS available."""
        frame = _rows(
            {
                "issue_time": _t(7),
                "available_at": _t(8),
                "capture_written_at": _t(8),
                "bronze_capture_id": "c1",
            },
            {
                "issue_time": _t(9),
                "available_at": _t(12),
                "capture_written_at": _t(12),
                "bronze_capture_id": "c2",
            },
        )
        assert _both(frame, KEY_SPEC, as_of=_t(9)) == ["c1"]
        assert _both(frame, KEY_SPEC) == ["c2"]


class TestTieBreak:
    def test_t_b3_3_equal_available_at_resolves_by_capture_then_id(self) -> None:
        """Detects a tie resolved by scan order: shuffled input, one winner."""
        rows = [
            {"available_at": _t(8), "capture_written_at": _t(9), "bronze_capture_id": "c1"},
            {"available_at": _t(8), "capture_written_at": _t(10), "bronze_capture_id": "c0"},
            {"available_at": _t(8), "capture_written_at": _t(10), "bronze_capture_id": "c2"},
        ]
        for seed in range(6):
            random.Random(seed).shuffle(rows)
            assert _both(_rows(*rows), KEY_SPEC) == ["c2"]

    def test_t_b3_4_equal_published_at_with_different_bytes(self) -> None:
        """C-3 (accepted residual): a later capture repeating an earlier
        ``published_at`` inherits its availability; the tie-break still orders
        the two deterministically (by capture time, then id). The as-of
        exposure is named in ADR-034, not fixed here."""
        frame = _rows(
            {
                "published_at": _t(8),
                "available_at": _t(8),
                "capture_written_at": _t(8, 30),
                "bronze_capture_id": "c1",
                "value": 1.0,
            },
            {
                "published_at": _t(8),
                "available_at": _t(8),
                "capture_written_at": _t(12),
                "bronze_capture_id": "c2",
                "value": 2.0,
            },
        )
        assert _both(frame, KEY_SPEC) == ["c2"]
        assert _both(frame, KEY_SPEC, as_of=_t(9)) == ["c2"]

    def test_a_missing_tiebreak_column_skips_fail_closed(self) -> None:
        """Detects a silently dropped tie-break (the winner would follow scan order)."""
        frame = _rows(
            {"available_at": _t(8), "capture_written_at": _t(9), "bronze_capture_id": "c1"}
        ).drop("capture_written_at")
        assert latest_view_sql("base", "base_latest", KEY_SPEC, set(frame.columns)) is None

    def test_run_type_rank_precedes_the_tiebreak(self) -> None:
        """T-B1-9's selection half: available_at, then the run rank, then the
        run_type column itself for unmapped runs."""
        spec = LatestViewSpec(
            key_columns=("settlement_date", "settlement_period"),
            order_columns=("available_at",),
            rank_column="run_type",
            rank_map=_SETTLEMENT_RUN_RANK,
            tiebreak_columns=(*TIE, "run_type"),
        )
        rows = _rows(
            {"available_at": _t(8), "capture_written_at": _t(8), "bronze_capture_id": "c1"},
            {"available_at": _t(8), "capture_written_at": _t(8), "bronze_capture_id": "c1"},
        ).with_columns(pl.Series("run_type", ["R1", "SF"]), pl.Series("value", [1.0, 2.0]))
        assert _sql(rows, spec)["run_type"].to_list() == ["R1"]
        assert _polars(rows, spec)["run_type"].to_list() == ["R1"]
        unmapped = rows.with_columns(pl.Series("run_type", ["ZZ", "YY"]))
        assert _sql(unmapped, spec)["run_type"].to_list() == ["ZZ"]
        assert _polars(unmapped, spec)["run_type"].to_list() == ["ZZ"]


def _completions(*rows: dict[str, Any]) -> pl.DataFrame:
    schema = {
        "family": pl.Utf8,
        "bronze_capture_id": pl.Utf8,
        "outcome": pl.Utf8,
        "row_count": pl.Int64,
        "available_at": TS,
        "capture_written_at": TS,
    }
    return pl.DataFrame([{"family": "fam", **row} for row in rows], schema=schema)


class TestWholeCapture:
    """E4's shape: populated(2) at 08:00, valid-empty at 10:00, populated(1) at 12:00."""

    @staticmethod
    def _state() -> tuple[pl.DataFrame, pl.DataFrame]:
        base = _rows(
            {
                "unit": "A",
                "available_at": _t(8),
                "capture_written_at": _t(8),
                "bronze_capture_id": "c1",
            },
            {
                "unit": "B",
                "available_at": _t(8),
                "capture_written_at": _t(8),
                "bronze_capture_id": "c1",
            },
            {
                "unit": "A",
                "available_at": _t(12),
                "capture_written_at": _t(12),
                "bronze_capture_id": "c3",
            },
        )
        completions = _completions(
            {
                "bronze_capture_id": "c1",
                "outcome": "populated",
                "row_count": 2,
                "available_at": _t(8),
                "capture_written_at": _t(8),
            },
            {
                "bronze_capture_id": "c2",
                "outcome": "valid_empty",
                "row_count": 0,
                "available_at": _t(10),
                "capture_written_at": _t(10),
            },
            {
                "bronze_capture_id": "c3",
                "outcome": "populated",
                "row_count": 1,
                "available_at": _t(12),
                "capture_written_at": _t(12),
            },
        )
        return base, completions

    @pytest.mark.parametrize(
        ("as_of", "expected"),
        [(_t(9), ["c1", "c1"]), (_t(11), []), (_t(13), ["c3"]), (None, ["c3"])],
    )
    def test_newest_complete_capture_as_of(
        self, as_of: datetime | None, expected: list[str]
    ) -> None:
        base, completions = self._state()
        assert _both(base, WHOLE_SPEC, as_of=as_of, completions=completions) == expected

    def test_an_output_whose_count_disagrees_is_not_eligible(self) -> None:
        """c3's rows gone (or partial): c2, the newest still-complete capture, wins."""
        base, completions = self._state()
        assert (
            _both(
                base.filter(pl.col("bronze_capture_id") != "c3"),
                WHOLE_SPEC,
                completions=completions,
            )
            == []
        )

    def test_an_output_without_a_completion_is_never_eligible(self) -> None:
        base, completions = self._state()
        orphan = _rows(
            {
                "unit": "Z",
                "available_at": _t(14),
                "capture_written_at": _t(14),
                "bronze_capture_id": "c4",
            }
        )
        assert _both(pl.concat([base, orphan]), WHOLE_SPEC, completions=completions) == ["c3"]

    def test_polars_without_completions_raises(self) -> None:
        base, _completions_frame = self._state()
        with pytest.raises(ValueError, match="completion records"):
            select_latest_vintage(base.lazy(), WHOLE_SPEC)


class TestRendererParity:
    def test_t_b3_5_randomized_frames_agree_incl_nulls(self) -> None:
        """Detects any divergence between the SQL and Polars renderers, with
        null ``issue_time``/``published_at`` sorting last on both."""
        rng = random.Random(20261007)
        for _ in range(25):
            rows = []
            for i in range(rng.randint(1, 30)):
                available = _t(6) + timedelta(minutes=rng.randint(0, 600))
                rows.append(
                    {
                        "settlement_period": rng.randint(1, 3),
                        "unit": rng.choice("AB"),
                        "issue_time": rng.choice([None, _t(5), _t(7)]),
                        "published_at": rng.choice([None, available]),
                        "available_at": available,
                        "capture_written_at": available + timedelta(minutes=rng.randint(0, 5)),
                        "bronze_capture_id": f"c{i:03d}",
                    }
                )
            frame = _rows(*rows)
            _both(frame, KEY_SPEC)
            _both(frame, KEY_SPEC, as_of=_t(11))

    def test_no_registered_view_text_carries_the_parameter(self) -> None:
        sql = latest_view_sql("base", "base_latest", KEY_SPEC, set(_rows().columns))
        assert sql is not None and "$as_of" not in sql
        whole = latest_view_sql("base", "base_latest", WHOLE_SPEC, set(_rows().columns))
        assert whole is not None and "$as_of" not in whole
