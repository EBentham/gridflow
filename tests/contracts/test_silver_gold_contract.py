"""Data contract tests: verify gold output is correctly built from silver."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder
from gridflow.silver.elexon.system_prices import SystemPriceTransformer


class TestSilverGoldContract:
    def test_gold_built_from_silver(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """Gold dataset should contain enriched silver data."""
        # Build silver first
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)

        transformer = SystemPriceTransformer(tmp_data_dir)
        transformer.run(date(2024, 1, 15))

        # Build gold
        builder = SystemMarginalPriceBuilder(tmp_data_dir)
        rows = builder.run(date(2024, 1, 15), date(2024, 1, 15))

        assert rows > 0

    def test_gold_has_derived_features(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """Gold dataset should contain derived features like spread."""
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)

        transformer = SystemPriceTransformer(tmp_data_dir)
        transformer.run(date(2024, 1, 15))

        builder = SystemMarginalPriceBuilder(tmp_data_dir)
        df = builder.build(date(2024, 1, 15), date(2024, 1, 15))

        assert "spread" in df.columns
        assert "abs_imbalance" in df.columns
