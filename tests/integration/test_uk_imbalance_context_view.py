"""Behavioural tests for the gold_uk_imbalance_context cross-source view (issue 15).

Asserts the silver -> gold join behaviour and values, not just shape:
- the LEFT JOIN on delivery time does not fan out (N price rows -> N gold rows);
- a price row with no carbon match survives with null carbon intensity;
- the realised ``carbon_intensity_actual_*`` column is labelled (column
  comment) so a model author can tell it apart from the forecast.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from gridflow.storage.duckdb import _register_views

VIEW_SQL = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gridflow"
    / "gold"
    / "views"
    / "uk_imbalance_context.sql"
)


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _connection_with_view(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """In-memory connection with silver views + only the view under test.

    Avoids init_catalogue, which under strict mode (pytest) would also try to
    register the unrelated eu_gas_storage gold view whose
    silver_gie_agsi_storage table is absent in this fixture.
    """
    con = duckdb.connect(":memory:")
    _register_views(con, data_dir)
    con.execute(VIEW_SQL.read_text())
    return con


def _seed_silver(data_dir: Path) -> None:
    """Three price rows; carbon intensity for only two of the three timestamps."""
    t0 = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
    t1 = datetime(2024, 1, 15, 0, 30, tzinfo=UTC)
    t2 = datetime(2024, 1, 15, 1, 0, tzinfo=UTC)

    prices = pl.DataFrame(
        {
            "timestamp_utc": [t0, t1, t2],
            "settlement_date": [t0.date(), t0.date(), t0.date()],
            "settlement_period": [1, 2, 3],
            "system_sell_price": [45.5, 46.75, 48.0],
            "system_buy_price": [55.0, 56.25, 58.5],
            "net_imbalance_volume": [-120.5, 80.3, -45.0],
            "run_type": ["SF", "SF", "SF"],
        }
    )
    _write_parquet(
        prices,
        data_dir
        / "silver"
        / "elexon"
        / "system_prices"
        / "year=2024"
        / "month=01"
        / "system_prices_20240115.parquet",
    )

    # Carbon intensity for t0 and t1 only — t2 has no match (tests LEFT JOIN).
    carbon = pl.DataFrame(
        {
            "timestamp_utc": [t0, t1],
            "forecast_gco2_kwh": [200.0, 210.0],
            "actual_gco2_kwh": [195.0, 205.0],
            "intensity_index": ["moderate", "moderate"],
        }
    )
    _write_parquet(
        carbon,
        data_dir
        / "silver"
        / "neso"
        / "carbon_intensity"
        / "year=2024"
        / "month=01"
        / "carbon_intensity_20240115.parquet",
    )


def test_view_join_does_not_fan_out_and_left_join_nulls(tmp_path: Path) -> None:
    """N price rows -> N gold rows; the unmatched row keeps null carbon."""
    data_dir = tmp_path / "data"
    _seed_silver(data_dir)

    con = _connection_with_view(data_dir)
    try:
        n_prices = con.execute("SELECT count(*) FROM silver_elexon_system_prices").fetchone()[0]
        rows = con.execute(
            """
            SELECT settlement_period,
                   carbon_intensity_forecast_gco2_kwh,
                   carbon_intensity_actual_gco2_kwh
            FROM gold_uk_imbalance_context
            ORDER BY settlement_period
            """
        ).fetchall()
    finally:
        con.close()

    # No fan-out: gold row count equals price (left) row count.
    assert len(rows) == n_prices == 3

    # The third period (no carbon match) survives with null carbon intensity.
    sp3 = rows[2]
    assert sp3[0] == 3
    assert sp3[1] is None  # forecast
    assert sp3[2] is None  # actual

    # The matched rows carry the joined actual value.
    assert rows[0][2] == 195.0
    assert rows[1][2] == 205.0


def test_actual_column_is_labelled_as_realised(tmp_path: Path) -> None:
    """The realised actual column must carry a comment flagging it as a
    future-realised value not available at delivery time, so a model author
    does not silently pull it as a delivery-time feature.
    """
    data_dir = tmp_path / "data"
    _seed_silver(data_dir)

    con = _connection_with_view(data_dir)
    try:
        comment = con.execute(
            """
            SELECT comment
            FROM duckdb_columns()
            WHERE table_name = 'gold_uk_imbalance_context'
              AND column_name = 'carbon_intensity_actual_gco2_kwh'
            """
        ).fetchone()
    finally:
        con.close()

    assert comment is not None
    assert comment[0] is not None and comment[0].strip() != "", (
        "carbon_intensity_actual_gco2_kwh must carry a leakage-warning comment"
    )
    assert "realis" in comment[0].lower() or "actual" in comment[0].lower()
