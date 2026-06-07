"""Tests for DuckDB view registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from gridflow.storage.duckdb import (
    _SILVER_VIEW_ALIASES,
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
    finally:
        con.close()

    assert elexon_rows == [("elexon", 1)]
    assert entsog_rows == [("entsog", 2)]


def test_silver_view_count_matches_silver_dir_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C1-7 anti-regression: one silver view per silver/<source>/<dataset> dir.

    Excludes the backward-compat alias allow-list (those are extra views over
    existing targets). Reverting to the unqualified producer collapses the two
    ``dupe`` dirs into a single ``silver_dupe`` view, so the count drops below
    the directory count and this guard fails.
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

    qualified = silver_views - set(_SILVER_VIEW_ALIASES)
    assert len(qualified) == len(silver_dirs)
    assert qualified == {"silver_elexon_dupe", "silver_entsog_dupe", "silver_elexon_fuelhh"}


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
