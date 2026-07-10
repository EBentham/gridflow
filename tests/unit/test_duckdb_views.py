"""Tests for DuckDB view registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from gridflow.storage.duckdb import (
    get_connection,
    init_catalogue,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_silver_view_reads_mixed_pre_and_post_f0_schemas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # F15-D: gold SQL views reference silver tables absent from test tmpdir.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    dataset_dir = data_dir / "silver" / "elexon" / "mixed" / "year=2024" / "month=01"

    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "old.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "available_at": ["2024-01-16T00:00:00Z"]}),
        dataset_dir / "new.parquet",
    )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT value, available_at IS NULL AS missing_available_at
            FROM silver_elexon_mixed
            ORDER BY value
            """
        ).fetchall()
    finally:
        con.close()

    assert rows == [(1, True), (2, False)]


def test_two_sources_sharing_a_dataset_name_get_distinct_views(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C1-7: source-qualified views never collide on a shared dataset name.

    Two sources both expose a ``dupe`` dataset. Under the old unqualified
    scheme both registered as ``silver_dupe`` and one silently shadowed the
    other (CREATE OR REPLACE in nondeterministic iterdir() order). With
    source-qualification each must surface as its own view carrying its own
    rows; this is RED on the un-renamed producer (one view, one source's rows).
    """
    # The real gold SQL references canonical silver views absent from this
    # dupe-only tmpdir; neutralise it so the test fails (or passes) only on the
    # silver-collision behaviour it targets.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"

    _write_parquet(
        pl.DataFrame({"src": ["elexon"], "value": [1]}),
        data_dir / "silver" / "elexon" / "dupe" / "year=2024" / "month=01" / "e.parquet",
    )
    _write_parquet(
        pl.DataFrame({"src": ["entsog"], "value": [2]}),
        data_dir / "silver" / "entsog" / "dupe" / "year=2024" / "month=01" / "g.parquet",
    )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        elexon_rows = con.execute(
            "SELECT src, value FROM silver_elexon_dupe ORDER BY value"
        ).fetchall()
        entsog_rows = con.execute(
            "SELECT src, value FROM silver_entsog_dupe ORDER BY value"
        ).fetchall()
        silver_views = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name LIKE 'silver_%'"
            ).fetchall()
        }
    finally:
        con.close()

    assert elexon_rows == [("elexon", 1)]
    assert entsog_rows == [("entsog", 2)]
    # A name owned by >1 source must get NO single-token backward-compat alias:
    # an ambiguous ``silver_dupe`` would arbitrarily shadow one source (C1-4).
    assert "silver_dupe" not in silver_views


def test_silver_view_count_matches_silver_dir_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C1-7 anti-regression: one qualified silver view per silver/<source>/<dataset>.

    The qualified-view count must equal the silver-dir count; the auto-generated
    backward-compat aliases are the extra views, one per dataset name owned by
    exactly ONE source. Both halves are recomputed from the dir structure so the
    guard stays correct as the alias scheme evolves. Reverting to the unqualified
    producer collapses the two ``dupe`` dirs into a single ``silver_dupe`` view,
    so the qualified count drops below the directory count and this guard fails.
    """
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"

    # Three distinct (source, dataset) silver dirs, two of which share the
    # dataset name ``dupe`` across different sources.
    silver_dirs = [
        ("elexon", "dupe"),
        ("entsog", "dupe"),
        ("elexon", "fuelhh"),
    ]
    for source, dataset in silver_dirs:
        _write_parquet(
            pl.DataFrame({"value": [1]}),
            data_dir / "silver" / source / dataset / "year=2024" / "month=01" / "x.parquet",
        )

    # Expected qualified views and single-token aliases, derived from the dirs:
    # a dataset name gets an alias iff exactly one source owns it (``dupe`` is
    # owned by two → no alias; ``fuelhh`` by one → ``silver_fuelhh``).
    expected_qualified = {f"silver_{source}_{dataset}" for source, dataset in silver_dirs}
    name_owners: dict[str, set[str]] = {}
    for source, dataset in silver_dirs:
        name_owners.setdefault(dataset, set()).add(source)
    expected_aliases = {
        f"silver_{dataset}" for dataset, owners in name_owners.items() if len(owners) == 1
    }

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        silver_views = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name LIKE 'silver_%'"
            ).fetchall()
        }
    finally:
        con.close()

    qualified = silver_views - expected_aliases
    assert len(qualified) == len(silver_dirs)
    assert qualified == expected_qualified
    # The aliases are exactly the single-source dataset names — no more, no less.
    assert silver_views == expected_qualified | expected_aliases
    assert expected_aliases == {"silver_fuelhh"}
    # The collision name gets no single-token alias.
    assert "silver_dupe" not in silver_views


def test_backward_compat_alias_resolves_to_qualified_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C1-7: each deprecation alias forwards to the correct qualified view.

    Seeds elexon/system_prices and asserts the legacy ``silver_system_prices``
    alias returns exactly the rows of ``silver_elexon_system_prices`` — the
    minimal check that the alias source->target mapping is wired correctly (a
    wrong mapping would otherwise pass the collision/count guards silently).
    Also asserts an alias whose target is absent is NOT created (no loud-fail).
    """
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    _write_parquet(
        pl.DataFrame({"value": [7]}),
        data_dir / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01" / "sp.parquet",
    )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        via_alias = con.execute("SELECT value FROM silver_system_prices").fetchall()
        via_qualified = con.execute("SELECT value FROM silver_elexon_system_prices").fetchall()
        present = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name LIKE 'silver_%'"
            ).fetchall()
        }
    finally:
        con.close()

    assert via_alias == via_qualified == [(7,)]
    # Aliases whose qualified target was not seeded must not be registered.
    assert "silver_storage" not in present
    assert "silver_fuelhh" not in present


def test_previously_unaliased_names_now_resolve_via_auto_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C1-4 cross-repo safety: ALL single-source old names get a working alias.

    The old hardcoded allow-list aliased only five names; a downstream repo
    querying any OTHER single-source old name (e.g. ``silver_day_ahead_prices``,
    ``silver_actual_load``) hit a ``BinderException`` after the rename. The
    auto-alias pass closes that gap: each previously-unaliased single-source name
    now resolves to its qualified view's rows.

    RED before the auto-alias change: ``silver_day_ahead_prices`` /
    ``silver_actual_load`` do not exist (the SELECT raises BinderException).
    GREEN after: each returns exactly its qualified view's rows.
    """
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    seeded = {
        ("entsoe", "day_ahead_prices"): 11,
        ("entsoe", "actual_load"): 22,
    }
    for (source, dataset), value in seeded.items():
        _write_parquet(
            pl.DataFrame({"value": [value]}),
            data_dir / "silver" / source / dataset / "year=2024" / "month=01" / "x.parquet",
        )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        # Each old single-token name resolves to exactly its qualified rows.
        for (source, dataset), value in seeded.items():
            via_alias = con.execute(f"SELECT value FROM silver_{dataset}").fetchall()
            via_qualified = con.execute(f"SELECT value FROM silver_{source}_{dataset}").fetchall()
            assert via_alias == via_qualified == [(value,)]
    finally:
        con.close()


def test_gold_view_reads_mixed_schemas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # F15-D: gold SQL views reference silver tables absent from test tmpdir.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    dataset_dir = data_dir / "gold" / "model_features" / "year=2024" / "month=01"

    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "old.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "feature_version": ["post-f0"]}),
        dataset_dir / "new.parquet",
    )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT value, feature_version IS NULL AS missing_feature_version
            FROM gold_model_features
            ORDER BY value
            """
        ).fetchall()
    finally:
        con.close()

    assert rows == [(1, True), (2, False)]


def test_latest_view_registered_for_append_only_datasets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ADR-025 P0.3: coexisting vintages resolve to one row per key via _latest."""
    from datetime import UTC, date, datetime

    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    partition = data_dir / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01"

    # Vintage A (08:00): SP1 R1 + SP2 SF. Vintage B (12:00 for SP1, 10:00 tie for
    # SP2): SP1 II published later must WIN despite lower rank (available_at-
    # primary); SP2 R1 wins its available_at tie on run rank.
    _write_parquet(
        pl.DataFrame(
            {
                "settlement_date": [date(2024, 1, 15)] * 2,
                "settlement_period": [1, 2],
                "system_sell_price": [44.0, 10.0],
                "run_type": ["R1", "SF"],
                "available_at": [
                    datetime(2024, 1, 15, 8, tzinfo=UTC),
                    datetime(2024, 1, 15, 10, tzinfo=UTC),
                ],
            }
        ),
        partition / "system_prices_20240115_runA.parquet",
    )
    _write_parquet(
        pl.DataFrame(
            {
                "settlement_date": [date(2024, 1, 15)] * 2,
                "settlement_period": [1, 2],
                "system_sell_price": [45.5, 11.0],
                "run_type": ["II", "R1"],
                "available_at": [
                    datetime(2024, 1, 15, 12, tzinfo=UTC),
                    datetime(2024, 1, 15, 10, tzinfo=UTC),
                ],
            }
        ),
        partition / "system_prices_20240115_runB.parquet",
    )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        base_count = con.execute("SELECT COUNT(*) FROM silver_elexon_system_prices").fetchone()[0]
        latest = con.execute(
            "SELECT settlement_period, system_sell_price "
            "FROM silver_elexon_system_prices_latest ORDER BY settlement_period"
        ).fetchall()
    finally:
        con.close()

    assert base_count == 4
    assert latest == [(1, 45.5), (2, 11.0)]


def test_latest_view_survives_missing_rank_column(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live DISEBSP silver has no run_type at all — registration must not fail."""
    from datetime import UTC, date, datetime

    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    partition = data_dir / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01"

    for hour, price, suffix in ((8, 44.0, "A"), (12, 45.5, "B")):
        _write_parquet(
            pl.DataFrame(
                {
                    "settlement_date": [date(2024, 1, 15)],
                    "settlement_period": [1],
                    "system_sell_price": [price],
                    "available_at": [datetime(2024, 1, 15, hour, tzinfo=UTC)],
                }
            ),
            partition / f"system_prices_20240115_run{suffix}.parquet",
        )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        latest = con.execute(
            "SELECT system_sell_price FROM silver_elexon_system_prices_latest"
        ).fetchall()
    finally:
        con.close()

    assert latest == [(45.5,)]


def test_latest_views_for_remit_and_fou2t14d(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The F7 APPEND_ONLY datasets finally get their read surface (R1-F02)."""
    from datetime import UTC, date, datetime

    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    stamp = datetime(2024, 1, 15, 10, tzinfo=UTC)

    remit_dir = data_dir / "silver" / "elexon" / "remit" / "year=2024" / "month=01"
    _write_parquet(
        pl.DataFrame({"mrid": ["m1"], "revision_number": [1], "available_at": [stamp]}),
        remit_dir / "remit_20240115_runA.parquet",
    )
    _write_parquet(
        pl.DataFrame({"mrid": ["m1"], "revision_number": [2], "available_at": [stamp]}),
        remit_dir / "remit_20240115_runB.parquet",
    )

    fou_dir = data_dir / "silver" / "elexon" / "fou2t14d" / "year=2024" / "month=01"
    for hour, usable, suffix in ((8, 100, "A"), (12, 120, "B")):
        _write_parquet(
            pl.DataFrame(
                {
                    "settlement_date": [date(2024, 1, 15)],
                    "settlement_period": [1],
                    "fuel_type": ["WIND"],
                    "output_usable": [usable],
                    "available_at": [datetime(2024, 1, 15, hour, tzinfo=UTC)],
                }
            ),
            fou_dir / f"fou2t14d_20240115_run{suffix}.parquet",
        )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        remit_rows = con.execute(
            "SELECT mrid, revision_number FROM silver_elexon_remit_latest"
        ).fetchall()
        fou_rows = con.execute(
            "SELECT fuel_type, output_usable FROM silver_elexon_fou2t14d_latest"
        ).fetchall()
    finally:
        con.close()

    assert remit_rows == [("m1", 2)]
    assert fou_rows == [("WIND", 120)]
