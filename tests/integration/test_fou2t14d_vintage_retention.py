"""End-to-end bronze -> silver -> _latest vintage-retention guard for fou2t14d.

R1-C (F-06, v0.18): the non-vacuous replacement for P0.3's fou2t14d
acceptance. ``tests/unit/test_duckdb_views.py::test_latest_views_for_remit_and_fou2t14d``
hand-writes silver parquet directly, bypassing ``transform()`` entirely -- it
proves the ``_latest`` view RENDERS and SELECTS correctly, but the F-06 defect
lives one layer below, in ``transform()``'s dedup key, so no amount of
hand-written parquet can catch it. This module starts at bronze and asserts at
three layers (run() return, written parquet, and both view renderers) so a
regression of the collapse fails loudly here instead of passing vacuously.

fou2t14d does NOT set ``VINTAGE_PER_BRONZE_FILE`` (unlike ``system_prices``,
see ``test_uk_imbalance_context_view.py``), so its real on-disk multiplicity
is two ROWS in one run-suffixed FILE, not two files -- the fixture below
reproduces that real shape rather than copying Cycle A's two-file pattern.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import polars as pl

from gridflow.silver.elexon.fou2t14d import FOU2T14DTransformer
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
from gridflow.storage.duckdb import _register_views
from gridflow.storage.paths import PathBuilder


def _seed_bronze_two_vintages(data_dir: Path) -> None:
    """Two bronze files in ONE fetch-day partition, real forecastDate-only shape.

    Both are matched by ``read_bronze:51``'s ``sorted(glob("raw_*.json"))``.
    Deliberately the forecastDate-only shape (no settlementPeriod) -- it is
    the real on-disk shape (100% of on-disk fou2t14d, X1-F05 footnote 1) and
    exercises ``optional_key_columns`` dropping settlement_period
    (``latest_views.py:73-79``).

    ``publishTime`` uses exactly ``%Y-%m-%dT%H:%M:%SZ`` -- ``fou2t14d.py:124``
    parses with ``strict=False``, so a ``.000Z`` or ``+00:00`` form would
    silently null ``published_at`` (verified), which would silently
    RE-COLLAPSE this fixture and make the test lie (R-1). This is the single
    most likely way this fixture rots.

    No ``.meta.json`` sidecars -- deliberately. ``run()`` reads sidecars only
    on the ``VINTAGE_PER_BRONZE_FILE`` branch or under ``--reingest``
    (``base.py:207-245``); this dataset takes neither.
    """
    bronze_dir = data_dir / "bronze" / "elexon" / "fou2t14d" / "2024" / "01" / "15"
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "raw_0600.json").write_text(
        json.dumps(
            {
                "data": [
                    {
                        "forecastDate": "2024-01-17",
                        "fuelType": "WIND",
                        "outputUsable": 1000.0,
                        "publishTime": "2024-01-15T06:00:00Z",
                    }
                ]
            }
        )
    )
    (bronze_dir / "raw_1800.json").write_text(
        json.dumps(
            {
                "data": [
                    {
                        "forecastDate": "2024-01-17",
                        "fuelType": "WIND",
                        "outputUsable": 1750.0,
                        "publishTime": "2024-01-15T18:00:00Z",
                    }
                ]
            }
        )
    )


def _connection_with_views(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """In-memory connection with silver views registered, no gold/init_catalogue.

    Does NOT call ``init_catalogue`` -- strict mode would try to register
    unrelated gold views whose dependency silver tables are absent under
    ``tmp_path``. ``gold_root`` will not exist here; ``_register_views``'s
    ``if gold_root.exists()`` guard makes that a no-op.
    """
    con = duckdb.connect(":memory:")
    paths = PathBuilder(data_dir)
    _register_views(con, paths.silver_root(), paths.gold_root())
    return con


def test_bronze_publications_both_reach_silver(tmp_path: Path) -> None:
    """AC-1/AC-2/AC-4: run() writes both bronze publications into one silver file.

    Pre-fix: run() returns 1 (the collapse) -- RED.
    """
    data_dir = tmp_path / "data"
    _seed_bronze_two_vintages(data_dir)

    transformer = FOU2T14DTransformer(data_dir)
    written = transformer.run(date(2024, 1, 15), run_id="R1-C-fixture")
    assert written == 2

    silver_dir = data_dir / "silver" / "elexon" / "fou2t14d" / "year=2024" / "month=01"
    [silver_path] = silver_dir.glob("fou2t14d_20240115_run*.parquet")
    result = pl.read_parquet(silver_path)

    assert result.height == 2
    # available_at is the row-wise coalesce(published_at, ingest_scalar)
    # (base.py:404-420) -- proving it, not now(), is this dataset's vintage
    # axis: each row's available_at equals its OWN published_at exactly, two
    # ROWS in one FILE (fou2t14d has no VINTAGE_PER_BRONZE_FILE, unlike
    # system_prices.py:31-32, which would instead write two files).
    result = result.sort("available_at")
    assert result["available_at"].to_list() == [
        datetime(2024, 1, 15, 6, tzinfo=UTC),
        datetime(2024, 1, 15, 18, tzinfo=UTC),
    ]
    assert result["available_at"].to_list() == result["published_at"].to_list()


def test_latest_view_collapses_genuine_multi_vintage_silver(tmp_path: Path) -> None:
    """AC-4/AC-5: silver_elexon_fou2t14d_latest returns exactly 1 row, the LATER
    publication, against genuinely multi-vintage silver written by the real
    pipeline (not hand-written parquet).

    Pre-fix: the base view's positive control itself fails (base count is 1,
    not 2, because the collapse already happened inside transform()) -- RED.
    """
    data_dir = tmp_path / "data"
    _seed_bronze_two_vintages(data_dir)
    FOU2T14DTransformer(data_dir).run(date(2024, 1, 15), run_id="R1-C-fixture")

    con = _connection_with_views(data_dir)
    try:
        # Permanent positive control, asserted FIRST: this is what makes the
        # _latest assertion below able to fail. Without it, a fixture that
        # silently regressed to single-vintage would pass the _latest
        # assertion vacuously -- exactly F-06's original defect.
        base_count = con.execute("SELECT count(*) FROM silver_elexon_fou2t14d").fetchone()[0]
        assert base_count == 2

        latest_count = con.execute("SELECT count(*) FROM silver_elexon_fou2t14d_latest").fetchone()[
            0
        ]
        assert latest_count == 1

        row = con.execute(
            "SELECT fuel_type, output_usable_mw, available_at FROM silver_elexon_fou2t14d_latest"
        ).fetchone()
        assert row == ("WIND", 1750.0, datetime(2024, 1, 15, 18, tzinfo=UTC))

        # A silently-dropped projection (duckdb.py:194-197) must not pass as a
        # zero-row result -- assert the view is actually registered.
        view_exists = con.execute(
            "SELECT 1 FROM information_schema.views "
            "WHERE table_schema = 'main' AND table_name = 'silver_elexon_fou2t14d_latest'"
        ).fetchone()
        assert view_exists is not None
    finally:
        con.close()


def test_sql_and_polars_latest_agree_on_the_same_multi_vintage_frame(tmp_path: Path) -> None:
    """AC-6: select_latest_vintage (Polars) and silver_elexon_fou2t14d_latest
    (SQL) pick the SAME winning row on the same genuinely-multi-vintage
    fixture.

    X1's "817/817 SQL/Polars parity" was measured over partially-collapsed
    on-disk data (cross-fetch-day vintages survived; intra-fetch-day ones did
    not), so agreement was never previously exercised on genuinely
    multi-vintage-within-file data -- exactly the shape this fix creates. The
    two renderers share one skip decision (_resolve_selection) and diverge
    only in their reaction to a skip (R1-A/F-18); this test pins the agreement
    on the SELECTION path itself.

    Pre-fix: passes trivially (one row in, one row out) -- this is NOT RED
    evidence, unlike the other two tests in this module. Reported honestly.
    """
    data_dir = tmp_path / "data"
    _seed_bronze_two_vintages(data_dir)
    FOU2T14DTransformer(data_dir).run(date(2024, 1, 15), run_id="R1-C-fixture")

    silver_dir = data_dir / "silver" / "elexon" / "fou2t14d" / "year=2024" / "month=01"
    [silver_path] = silver_dir.glob("fou2t14d_20240115_run*.parquet")

    polars_result = select_latest_vintage(
        pl.scan_parquet(silver_path), LATEST_VIEW_SPECS[("elexon", "fou2t14d")]
    ).collect()
    assert polars_result.height == 1
    polars_tuple = (
        polars_result["settlement_date"][0],
        polars_result["fuel_type"][0],
        polars_result["output_usable_mw"][0],
        polars_result["available_at"][0],
    )

    con = _connection_with_views(data_dir)
    try:
        sql_row = con.execute(
            "SELECT settlement_date, fuel_type, output_usable_mw, available_at "
            "FROM silver_elexon_fou2t14d_latest"
        ).fetchone()
    finally:
        con.close()

    assert sql_row is not None
    assert polars_tuple == tuple(sql_row)
