"""Tests for latest-vintage selection (ADR-025 P0.3): SQL views + Polars mirror."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import duckdb
import polars as pl

from gridflow.quality.checks import check_duplicates
from gridflow.silver.latest_views import (
    LATEST_VIEW_SPECS,
    latest_view_sql,
    select_latest_vintage,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

_SP_SPEC = LATEST_VIEW_SPECS[("elexon", "system_prices")]

_SP_COLUMNS = {
    "settlement_date",
    "settlement_period",
    "system_sell_price",
    "run_type",
    "available_at",
}


def _sp_frame() -> pl.DataFrame:
    """Two keys probing both semantics: time beats rank; rank breaks time ties.

    Key (2024-01-15, 1): the II row is PUBLISHED LATER than the R1 row — it must
    win despite the lower run rank (available_at-primary, per ADR-025).
    Key (2024-01-15, 2): both rows share available_at — R1 outranks SF.
    """
    return pl.DataFrame(
        {
            "settlement_date": [date(2024, 1, 15)] * 4,
            "settlement_period": [1, 1, 2, 2],
            "system_sell_price": [44.0, 45.5, 10.0, 11.0],
            "run_type": ["R1", "II", "SF", "R1"],
            "available_at": [
                datetime(2024, 1, 15, 8, tzinfo=UTC),
                datetime(2024, 1, 15, 12, tzinfo=UTC),
                datetime(2024, 1, 15, 10, tzinfo=UTC),
                datetime(2024, 1, 15, 10, tzinfo=UTC),
            ],
        }
    )


_EXPECTED_WINNERS = {(date(2024, 1, 15), 1): 45.5, (date(2024, 1, 15), 2): 11.0}


class TestLatestViewSql:
    def test_renders_qualify_with_rank_tiebreak(self):
        sql = latest_view_sql("base", "base_latest", _SP_SPEC, _SP_COLUMNS)
        assert sql is not None
        assert '"available_at" DESC NULLS LAST' in sql
        assert 'CASE "run_type"' in sql
        assert sql.index("available_at") < sql.index("CASE"), "rank must be secondary"

    def test_rank_term_dropped_when_column_absent(self):
        # Live DISEBSP silver has no run_type column at all — the view must
        # still bind instead of failing catalogue registration.
        sql = latest_view_sql("base", "base_latest", _SP_SPEC, _SP_COLUMNS - {"run_type"})
        assert sql is not None
        assert "CASE" not in sql

    def test_missing_key_column_returns_none(self):
        assert latest_view_sql("base", "base_latest", _SP_SPEC, {"available_at"}) is None

    def test_no_order_column_returns_none(self):
        columns = {"settlement_date", "settlement_period"}
        assert latest_view_sql("base", "base_latest", _SP_SPEC, columns) is None


class TestSqlPolarsParity:
    def test_duckdb_view_and_polars_mirror_pick_identical_rows(self):
        df = _sp_frame()
        con = duckdb.connect(":memory:")
        try:
            con.register("base", df.to_arrow())
            sql = latest_view_sql("base", "base_latest", _SP_SPEC, set(df.columns))
            assert sql is not None
            con.execute(sql)
            # Project only non-timestamp columns: TIMESTAMPTZ read-back needs
            # pytz/ICU (absent in CI — see MEMORY gotcha).
            sql_rows = {
                (row[0], row[1]): row[2]
                for row in con.execute(
                    "SELECT settlement_date, settlement_period, system_sell_price FROM base_latest"
                ).fetchall()
            }
        finally:
            con.close()

        polars_result = select_latest_vintage(df.lazy(), _SP_SPEC).collect()
        polars_rows = {
            (row["settlement_date"], row["settlement_period"]): row["system_sell_price"]
            for row in polars_result.iter_rows(named=True)
        }

        assert sql_rows == _EXPECTED_WINNERS
        assert polars_rows == _EXPECTED_WINNERS

    def test_null_run_type_ranks_below_any_mapped_run(self):
        df = pl.DataFrame(
            {
                "settlement_date": [date(2024, 1, 15)] * 2,
                "settlement_period": [1, 1],
                "system_sell_price": [1.0, 2.0],
                "run_type": [None, "SF"],
                "available_at": [datetime(2024, 1, 15, 10, tzinfo=UTC)] * 2,
            }
        )
        result = select_latest_vintage(df.lazy(), _SP_SPEC).collect()
        assert result["system_sell_price"].to_list() == [2.0]


class TestSelectLatestVintage:
    def test_rank_helper_column_not_leaked(self):
        result = select_latest_vintage(_sp_frame().lazy(), _SP_SPEC).collect()
        assert "_vintage_rank" not in result.columns

    def test_missing_key_returns_frame_unchanged(self, caplog: pytest.LogCaptureFixture):
        df = pl.DataFrame({"other": [1, 2]})
        with caplog.at_level("WARNING"):
            result = select_latest_vintage(df.lazy(), _SP_SPEC).collect()
        assert result.height == 2
        assert "key column(s)" in caplog.text

    def test_remit_revision_number_breaks_available_at_tie(self):
        spec = LATEST_VIEW_SPECS[("elexon", "remit")]
        stamp = datetime(2024, 1, 15, 10, tzinfo=UTC)
        df = pl.DataFrame(
            {
                "mrid": ["m1", "m1", "m2"],
                "revision_number": [1, 2, 1],
                "available_at": [stamp, stamp, stamp],
            }
        )
        result = select_latest_vintage(df.lazy(), spec).collect().sort("mrid")
        assert result["mrid"].to_list() == ["m1", "m2"]
        assert result["revision_number"].to_list() == [2, 1]

    def test_fou2t14d_one_row_per_fuel_key(self):
        spec = LATEST_VIEW_SPECS[("elexon", "fou2t14d")]
        df = pl.DataFrame(
            {
                "settlement_date": [date(2024, 1, 15)] * 3,
                "settlement_period": [1, 1, 1],
                "fuel_type": ["WIND", "WIND", "CCGT"],
                "output_usable": [100, 120, 300],
                "available_at": [
                    datetime(2024, 1, 15, 8, tzinfo=UTC),
                    datetime(2024, 1, 15, 12, tzinfo=UTC),
                    datetime(2024, 1, 15, 8, tzinfo=UTC),
                ],
            }
        )
        result = select_latest_vintage(df.lazy(), spec).collect().sort("fuel_type")
        assert result["output_usable"].to_list() == [300, 120]


class TestQualityReadsLatestSurface:
    """The quality CLI's duplicate check must see one row per key (ADR-025)."""

    def test_duplicate_check_false_fails_on_raw_and_passes_on_latest(self):
        df = _sp_frame()
        raw = check_duplicates(
            df, ["settlement_date", "settlement_period"], source="elexon", dataset="system_prices"
        )
        latest = check_duplicates(
            select_latest_vintage(df.lazy(), _SP_SPEC).collect(),
            ["settlement_date", "settlement_period"],
            source="elexon",
            dataset="system_prices",
        )
        assert not raw.passed, "raw vintages must look duplicated — guards the fixture"
        assert latest.passed

    def test_quality_cli_runs_clean_on_two_vintage_silver(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from typer.testing import CliRunner

        from gridflow.cli import app

        partition = tmp_path / "data" / "silver" / "elexon" / "system_prices"
        partition = partition / "year=2024" / "month=01"
        partition.mkdir(parents=True)
        df = _sp_frame().with_columns(
            pl.col("available_at").cast(pl.Datetime("us", "UTC")),
            pl.lit(datetime(2024, 1, 15, 0, 30, tzinfo=UTC))
            .cast(pl.Datetime("us", "UTC"))
            .alias("timestamp_utc"),
        )
        for stamp in ("08", "12"):
            vintage = df.filter(pl.col("available_at").dt.hour() == int(stamp))
            vintage.write_parquet(partition / f"system_prices_20240115_run{stamp}.parquet")

        monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(tmp_path / "quality.duckdb"))
        monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))

        result = CliRunner().invoke(app, ["quality"])

        assert result.exit_code == 0, result.output
        assert "duplicate" not in result.output.lower(), (
            "duplicate check must not false-fail on coexisting vintages: " + result.output
        )
