"""Data contract tests: verify silver output matches Pydantic schemas."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.schemas.elexon import ElexonSystemPrice
from gridflow.silver.elexon.system_prices import SystemPriceTransformer


class TestBronzeSilverContract:
    def test_silver_output_matches_schema(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """Silver transformer output must validate against Pydantic schema."""
        # Write bronze data
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)

        # Transform to silver
        transformer = SystemPriceTransformer(tmp_data_dir)
        transformer.run(date(2024, 1, 15))

        # Read silver output
        silver_path = (
            tmp_data_dir
            / "silver"
            / "elexon"
            / "system_prices"
            / "year=2024"
            / "month=01"
            / "system_prices_20240115.parquet"
        )
        df = pl.read_parquet(silver_path)

        # Validate every row against the schema
        errors = []
        for row in df.iter_rows(named=True):
            try:
                ElexonSystemPrice(**row)
            except Exception as e:
                errors.append(f"Row {row}: {e}")

        assert not errors, f"Schema validation failures:\n" + "\n".join(errors)

    def test_required_columns_present(self, tmp_data_dir: Path, sample_raw_response: RawResponse):
        """Silver output must contain all columns defined in the schema."""
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)

        transformer = SystemPriceTransformer(tmp_data_dir)
        transformer.run(date(2024, 1, 15))

        silver_path = (
            tmp_data_dir
            / "silver"
            / "elexon"
            / "system_prices"
            / "year=2024"
            / "month=01"
            / "system_prices_20240115.parquet"
        )
        df = pl.read_parquet(silver_path)

        required_cols = {
            "settlement_date",
            "settlement_period",
            "timestamp_utc",
            "system_sell_price",
            "system_buy_price",
            "net_imbalance_volume",
            "run_type",
            "data_provider",
        }
        actual_cols = set(df.columns)
        missing = required_cols - actual_cols
        assert not missing, f"Missing required columns: {missing}"
