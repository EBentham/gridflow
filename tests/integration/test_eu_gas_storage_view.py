"""Positive end-to-end registration test for the gold_eu_gas_storage view.

Mirrors ``test_uk_imbalance_context_view.py`` but drives the REAL
``init_catalogue`` (i.e. ``_register_views`` + ``_register_gold_views``), so it
proves the source-qualified silver name baked into ``eu_gas_storage.sql``
(``FROM silver_gie_agsi_storage``) is correct end-to-end — a wrong qualified
name there would raise a BinderException at gold-view registration.

Under pytest, ``_is_strict_mode()`` is True, so ``_register_gold_views`` runs
EVERY ``gold/views/*.sql`` and RAISES on any binder error rather than debug-log.
``uk_imbalance_context.sql`` binds ``silver_elexon_system_prices`` and
``silver_neso_carbon_intensity``; if those silver views were absent the strict
registration would blow up at setup. So all three silver inputs are seeded to let
the full gold registration run clean — without monkeypatching the gold pass away,
which would defeat the "proves the qualified name end-to-end" guarantee.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from gridflow.storage.duckdb import get_connection, init_catalogue

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _seed_silver(data_dir: Path) -> None:
    """Seed all three silver inputs the real gold SQL views bind against.

    - ``gie_agsi/storage`` is the dataset under test (``gold_eu_gas_storage``).
    - ``elexon/system_prices`` + ``neso/carbon_intensity`` are required only so
      the co-registered ``uk_imbalance_context.sql`` binds cleanly under strict
      mode (they are otherwise irrelevant to this assertion).
    """
    storage = pl.DataFrame(
        {
            "gas_day": [date(2024, 1, 15), date(2024, 1, 15)],
            "country_code": ["DE", "FR"],
            "country_name": ["Germany", "France"],
            "gas_in_storage_gwh": [1000.0, 800.0],
            "withdrawal_gwh": [10.0, 8.0],
            "injection_gwh": [0.0, 0.0],
            "working_gas_volume_gwh": [2000.0, 1600.0],
            "storage_pct_full": [50.0, 50.0],
            "trend": [-0.5, -0.5],
            "data_provider": ["GIE", "GIE"],
            "ingested_at": ["2024-01-16T06:00:00Z", "2024-01-16T06:00:00Z"],
        }
    )
    _write_parquet(
        storage,
        data_dir
        / "silver"
        / "gie_agsi"
        / "storage"
        / "year=2024"
        / "month=01"
        / "storage_20240115.parquet",
    )

    # Minimal seeds so uk_imbalance_context.sql binds under strict mode.
    _write_parquet(
        pl.DataFrame(
            {
                "timestamp_utc": ["2024-01-15T00:00:00Z"],
                "settlement_date": [date(2024, 1, 15)],
                "settlement_period": [1],
                "system_sell_price": [45.5],
                "system_buy_price": [55.0],
                "net_imbalance_volume": [-120.5],
                "run_type": ["SF"],
            }
        ),
        data_dir
        / "silver"
        / "elexon"
        / "system_prices"
        / "year=2024"
        / "month=01"
        / "sp_20240115.parquet",
    )
    _write_parquet(
        pl.DataFrame(
            {
                "timestamp_utc": ["2024-01-15T00:00:00Z"],
                "forecast_gco2_kwh": [200.0],
                "actual_gco2_kwh": [195.0],
                "intensity_index": ["moderate"],
            }
        ),
        data_dir
        / "silver"
        / "neso"
        / "carbon_intensity"
        / "year=2024"
        / "month=01"
        / "ci_20240115.parquet",
    )


def test_gold_eu_gas_storage_registers_and_returns_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real catalogue registers gold_eu_gas_storage over the qualified view.

    Drives ``init_catalogue`` (no monkeypatch on the gold pass) and asserts the
    gold view both registers and returns the seeded rows — confirming
    ``eu_gas_storage.sql``'s ``FROM silver_gie_agsi_storage`` resolves to the
    real source-qualified silver view.
    """
    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    _seed_silver(data_dir)

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        registered = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = 'gold_eu_gas_storage'"
            ).fetchall()
        }
        rows = con.execute(
            "SELECT country_code, gas_in_storage_gwh FROM gold_eu_gas_storage ORDER BY country_code"
        ).fetchall()
    finally:
        con.close()

    assert registered == {"gold_eu_gas_storage"}
    assert rows == [("DE", 1000.0), ("FR", 800.0)]
