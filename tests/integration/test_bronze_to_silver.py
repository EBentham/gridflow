"""Integration test: bronze ingestion to silver transformation."""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from gridflow.bronze.writer import BronzeWriter
from gridflow.silver.elexon.system_prices import SystemPriceTransformer

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse


class TestBronzeToSilver:
    def test_full_pipeline(self, tmp_data_dir: Path, sample_raw_response: RawResponse):
        """Test the complete bronze -> silver flow."""
        # Step 1: Write to bronze
        writer = BronzeWriter(tmp_data_dir)
        bronze_path = writer.write(sample_raw_response)
        assert bronze_path.exists()
        assert bronze_path.suffix == ".json"

        # Verify metadata sidecar
        meta_path = bronze_path.with_suffix("").with_suffix(".meta.json")
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["source"] == "elexon"
        assert meta["dataset"] == "system_prices"

        # Step 2: Transform to silver
        transformer = SystemPriceTransformer(tmp_data_dir)
        rows = transformer.run(date(2024, 1, 15))

        assert rows == 3  # 3 unique SPs, no collapse (ADR-025 APPEND_ONLY)

        # Step 3: Verify silver file exists — APPEND_ONLY writes one run-suffixed
        # file per bronze vintage (one bronze file here → exactly one).
        silver_dir = tmp_data_dir / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01"
        silver_files = sorted(silver_dir.glob("system_prices_20240115_run*.parquet"))
        assert len(silver_files) == 1

        # Step 4: Verify silver data
        df = pl.read_parquet(silver_files[0])
        assert len(df) == 3
        assert "timestamp_utc" in df.columns
        assert "system_sell_price" in df.columns
        assert "data_provider" in df.columns

    def test_idempotent_silver_write(self, tmp_data_dir: Path, sample_raw_response: RawResponse):
        """Running the transform twice produces the same result."""
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)

        transformer = SystemPriceTransformer(tmp_data_dir)

        rows1 = transformer.run(date(2024, 1, 15))
        rows2 = transformer.run(date(2024, 1, 15))

        assert rows1 == rows2

        # Same bronze sidecar → same available_at → same run-suffixed filename:
        # the second run overwrites the first, never stacks a duplicate vintage.
        silver_dir = tmp_data_dir / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01"
        silver_files = sorted(silver_dir.glob("system_prices_20240115_run*.parquet"))
        assert len(silver_files) == 1
        df = pl.read_parquet(silver_files[0])
        assert len(df) == 3

    def test_no_bronze_data(self, tmp_data_dir: Path):
        """Transform with no bronze data returns 0 rows."""
        transformer = SystemPriceTransformer(tmp_data_dir)
        rows = transformer.run(date(2024, 1, 15))
        assert rows == 0
