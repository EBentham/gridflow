"""Behavioural tests for the gold_uk_imbalance_context cross-source view (issue 15).

Asserts the silver -> gold join behaviour and values, not just shape:
- the LEFT JOIN on delivery time does not fan out (N price rows -> N gold rows);
- a price row with no carbon match survives with null carbon intensity;
- the realised ``carbon_intensity_actual_*`` column is labelled (column
  comment) so a model author can tell it apart from the forecast.

R1-A (F-01, v0.18): two-vintage regression fixture + as-of semantics + the
parquet-derived P1.5 producer/view contract guard. See the module-level tests
below for the AC-1/AC-2/AC-4/AC-5 evidence.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import polars as pl

from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.storage.duckdb import _register_views
from gridflow.storage.paths import PathBuilder

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
    paths = PathBuilder(data_dir)
    _register_views(con, paths.silver_root(), paths.gold_root())
    con.execute(VIEW_SQL.read_text())
    return con


def _seed_silver(data_dir: Path) -> None:
    """Three price rows; carbon intensity for only two of the three timestamps.

    ``available_at`` is a single stamp across all three rows (not a vintage
    axis in this fixture) — it exists only so the gold view's ``_latest``
    dependency (interaction 3, R1-A) renders; each settlement_period here is a
    distinct key, so the collapse is a no-op for these three rows.
    """
    t0 = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
    t1 = datetime(2024, 1, 15, 0, 30, tzinfo=UTC)
    t2 = datetime(2024, 1, 15, 1, 0, tzinfo=UTC)
    single_vintage_stamp = datetime(2024, 1, 15, 12, tzinfo=UTC)

    prices = pl.DataFrame(
        {
            "timestamp_utc": [t0, t1, t2],
            "settlement_date": [t0.date(), t0.date(), t0.date()],
            "settlement_period": [1, 2, 3],
            "system_sell_price": [45.5, 46.75, 48.0],
            "system_buy_price": [55.0, 56.25, 58.5],
            "net_imbalance_volume": [-120.5, 80.3, -45.0],
            "price_derivation_code": ["A", "A", "A"],
            "available_at": [single_vintage_stamp, single_vintage_stamp, single_vintage_stamp],
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


def _seed_silver_two_vintages(data_dir: Path) -> None:
    """Two run-suffixed parquet vintages in one partition (AC-4).

    Mirrors the real on-disk shape produced by ``APPEND_ONLY`` +
    ``VINTAGE_PER_BRONZE_FILE`` (``silver/elexon/system_prices.py:31-32``) — two
    files, not two rows in one file — so the guard exercises the real
    ``read_parquet(..., union_by_name=true)`` glob rather than a synthetic
    single-frame shape.

    Vintage A (08:00Z) and vintage B (18:00Z) differ in BOTH ``available_at``
    and every price value, so "latest wins" is distinguishable from "first
    file wins" and from "either row passes". The 08:00Z/18:00Z gap leaves
    12:00Z available as a between-vintages ``as_of`` for the semantics test.
    ``run_type`` is omitted from both files — verified on-disk DISEBSP silver
    has no ``run_type`` column at all, so the rank tie-break is correctly
    inert here (``available_at`` is the primary axis, ADR-025 §2).
    """
    sp_date = date(2024, 1, 15)
    t0 = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
    t1 = datetime(2024, 1, 15, 0, 30, tzinfo=UTC)

    vintage_a = pl.DataFrame(
        {
            "timestamp_utc": [t0, t1],
            "settlement_date": [sp_date, sp_date],
            "settlement_period": [1, 2],
            "system_sell_price": [44.0, 10.0],
            "system_buy_price": [54.0, 20.0],
            "net_imbalance_volume": [-120.5, 80.3],
            "price_derivation_code": ["N", "N"],
            "available_at": [
                datetime(2024, 1, 15, 8, tzinfo=UTC),
                datetime(2024, 1, 15, 8, tzinfo=UTC),
            ],
        }
    )
    vintage_b = pl.DataFrame(
        {
            "timestamp_utc": [t0, t1],
            "settlement_date": [sp_date, sp_date],
            "settlement_period": [1, 2],
            "system_sell_price": [45.5, 11.0],
            "system_buy_price": [55.5, 21.0],
            "net_imbalance_volume": [-130.5, 90.3],
            "price_derivation_code": ["N", "N"],
            "available_at": [
                datetime(2024, 1, 15, 18, tzinfo=UTC),
                datetime(2024, 1, 15, 18, tzinfo=UTC),
            ],
        }
    )
    partition = data_dir / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01"
    _write_parquet(vintage_a, partition / "system_prices_20240115_runA.parquet")
    _write_parquet(vintage_b, partition / "system_prices_20240115_runB.parquet")

    # Carbon intensity for both price timestamps — the LEFT JOIN is not the
    # thing under test here.
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


def test_gold_view_collapses_vintages(tmp_path: Path) -> None:
    """AC-1 / AC-4: with two vintages on disk, gold returns exactly one row
    per (settlement_date, settlement_period) — the LATER available_at wins.

    Permanent positive control: ``silver_elexon_system_prices`` (the
    all-vintage base view) really does hold both vintages (4 rows) — this is
    what makes the guard able to fail. If a future change ever made the
    fixture single-vintage, this control would catch it (guards against the
    F-06 fou2t14d vacuous-acceptance failure mode).
    """
    data_dir = tmp_path / "data"
    _seed_silver_two_vintages(data_dir)

    con = _connection_with_view(data_dir)
    try:
        base_count = con.execute("SELECT count(*) FROM silver_elexon_system_prices").fetchone()[0]
        gold_count = con.execute("SELECT count(*) FROM gold_uk_imbalance_context").fetchone()[0]
        distinct_keys = con.execute(
            "SELECT count(DISTINCT (settlement_date, settlement_period)) "
            "FROM gold_uk_imbalance_context"
        ).fetchone()[0]
        rows = con.execute(
            "SELECT settlement_period, system_sell_price FROM gold_uk_imbalance_context "
            "ORDER BY settlement_period"
        ).fetchall()
    finally:
        con.close()

    # Positive control: the base view really holds both vintages (4 rows).
    # Without this, a fixture that silently stopped being multi-vintage could
    # make the gold_count==2 assertion pass vacuously.
    assert base_count == 4
    assert gold_count == 2
    assert distinct_keys == 2
    # Value assertion, not just shape: the LATER available_at (vintage B) wins
    # for BOTH periods — not merely "one arbitrary row survived".
    assert rows == [(1, 45.5), (2, 11.0)]


def test_available_at_is_a_failclosed_cutoff_not_time_travel(tmp_path: Path) -> None:
    """AC-2: per ADR-025:117-120 the ``_latest`` view is the "current best
    value as of now" projection — it does NOT implement point-in-time
    selection.

    This is a fail-closed CUTOFF, not historical point-in-time selection: an
    ``as_of`` between vintages yields NOTHING rather than the value that was
    current at that moment. The collapse happens first (only the winning
    vintage B survives into ``_latest``); the ``available_at <= as_of``
    filter is then applied to that single already-collapsed row, so an
    ``as_of`` that falls before vintage B's stamp cuts it away entirely —
    vintage A is not there to fall back to. Genuine historical PIT is
    ``available_at <= as_of`` then latest-of-survivors, evaluated against the
    ALL-vintage base view (``silver_elexon_system_prices``) or its deprecated
    ``silver_system_prices`` alias, both of which still return every vintage
    by design.
    """
    data_dir = tmp_path / "data"
    _seed_silver_two_vintages(data_dir)

    con = _connection_with_view(data_dir)
    try:
        columns = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE table_name = 'gold_uk_imbalance_context'"
            ).fetchall()
        }
        assert "available_at" in columns, (
            "available_at must be a projected column on the collapsed gold view"
        )

        after = con.execute(
            "SELECT count(*) FROM gold_uk_imbalance_context WHERE available_at <= ?",
            [datetime(2024, 1, 15, 23, tzinfo=UTC)],
        ).fetchone()[0]
        between = con.execute(
            "SELECT count(*) FROM gold_uk_imbalance_context WHERE available_at <= ?",
            [datetime(2024, 1, 15, 12, tzinfo=UTC)],
        ).fetchone()[0]
        before = con.execute(
            "SELECT count(*) FROM gold_uk_imbalance_context WHERE available_at <= ?",
            [datetime(2024, 1, 15, 0, tzinfo=UTC)],
        ).fetchone()[0]
    finally:
        con.close()

    assert after == 2, "as_of AFTER the winning vintage: the winner survives the cutoff"
    assert between == 0, (
        "as_of BETWEEN vintages: must be 0, NOT 2 and NOT the earlier vintage. "
        "The collapse happens first (only the later vintage survives into "
        "_latest); the cutoff filter then removes that survivor too, because "
        "this is a fail-closed cutoff, NOT historical point-in-time selection."
    )
    assert before == 0, "as_of BEFORE both vintages: nothing survives"


def test_view_referenced_columns_are_carried_by_written_silver(tmp_path: Path) -> None:
    """AC-5 (F-13): the P1.5 guard, taken against ACTUALLY WRITTEN silver.

    ``available_at`` (a referenced ``sp.*`` column after Task 2) is injected
    at the shared silver write boundary (``silver/base.py:367-408``,
    ``_add_bitemporal_columns``) and never by ``transform()`` itself — so the
    producer contract must be derived from a parquet the transformer's real
    ``run()`` path actually wrote, not from bare ``transform()`` output
    (SOL-1). The assertion is unconditional: no exclusion list, so it cannot
    drift from ``_add_bitemporal_columns``.

    Pre-change (before Task 2 adds ``sp.available_at`` to the SELECT list),
    the view references no write-boundary column, so this test PASSES before
    ANY src/ edit in this unit — its guard value is realised jointly by Task 2
    (which adds ``available_at`` to the referenced set) and Task 4
    (unconditional business-column emission, so a raw field's absence cannot
    silently drop a column the view SELECTs). See OQ-5 (superseded).
    """
    target_date = date(2024, 1, 15)
    bronze_dir = tmp_path / "bronze" / "elexon" / "system_prices" / "2024" / "01" / "15"
    bronze_dir.mkdir(parents=True)
    written_at = datetime(2024, 1, 15, 8, tzinfo=UTC)
    # Live-shaped bronze: priceDerivationCode present, settlementRunType absent
    # (verified on-disk DISEBSP shape).
    (bronze_dir / "raw_live.json").write_text(
        json.dumps(
            {
                "data": [
                    {
                        "settlementDate": target_date.isoformat(),
                        "settlementPeriod": 1,
                        "systemSellPrice": 44.0,
                        "systemBuyPrice": 54.0,
                        "netImbalanceVolume": -120.5,
                        "priceDerivationCode": "N",
                    }
                ]
            }
        )
    )
    (bronze_dir / "raw_live.meta.json").write_text(
        json.dumps({"written_at": written_at.isoformat()})
    )

    transformer = SystemPriceTransformer(tmp_path)
    assert transformer.run(target_date) == 1

    silver_dir = tmp_path / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01"
    [silver_path] = silver_dir.glob("system_prices_20240115_run*.parquet")
    written_columns = set(pl.read_parquet(silver_path).columns)

    sql = VIEW_SQL.read_text()
    select_match = re.search(
        r"\bSELECT\b(?P<select>.*?)\bFROM\b", sql, flags=re.IGNORECASE | re.DOTALL
    )
    assert select_match is not None, "could not find the SELECT list in uk_imbalance_context.sql"
    referenced_sp_columns = set(
        re.findall(r"\bsp\.([A-Za-z_][A-Za-z0-9_]*)\b", select_match.group("select"))
    )
    assert referenced_sp_columns, "no sp.* columns parsed — check the regex against the SQL"

    # Unconditional: no exclusion list, so this cannot drift from
    # _add_bitemporal_columns the way a hard-coded subtraction would.
    missing = referenced_sp_columns - written_columns
    assert not missing, (
        f"gold view references sp.{missing} but the written silver parquet does not carry it"
    )
