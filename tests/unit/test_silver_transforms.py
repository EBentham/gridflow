"""Unit tests for silver-layer transformers."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from gridflow.silver.elexon.system_prices import SystemPriceTransformer


class TestSystemPriceTransformer:
    def setup_method(self):
        """Fresh transformer for each test (data_dir doesn't matter for transform())."""
        self.transformer = SystemPriceTransformer.__new__(SystemPriceTransformer)
        self.transformer.data_dir = Path("/tmp/test")
        self.transformer.bronze_dir = Path("/tmp/test/bronze/elexon/system_prices")
        self.transformer.silver_dir = Path("/tmp/test/silver/elexon/system_prices")

    def _make_raw_df(self, records: list[dict]) -> pl.DataFrame:
        return pl.DataFrame(records)

    def test_transform_basic(self):
        """Basic transform with standard field names."""
        raw = self._make_raw_df([
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "systemSellPrice": 45.50,
                "systemBuyPrice": 55.00,
                "netImbalanceVolume": -120.5,
                "settlementRunType": "SF",
            },
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 2,
                "systemSellPrice": 46.75,
                "systemBuyPrice": 56.25,
                "netImbalanceVolume": 80.3,
                "settlementRunType": "SF",
            },
        ])
        result = self.transformer.transform(raw)

        assert len(result) == 2
        assert "timestamp_utc" in result.columns
        assert "system_sell_price" in result.columns
        assert "data_provider" in result.columns
        assert result["data_provider"][0] == "elexon"

    def test_run_type_resolution(self):
        """Later run types should supersede earlier ones."""
        raw = self._make_raw_df([
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "systemSellPrice": 44.00,
                "systemBuyPrice": 54.00,
                "netImbalanceVolume": -115.0,
                "settlementRunType": "II",
            },
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "systemSellPrice": 45.50,
                "systemBuyPrice": 55.00,
                "netImbalanceVolume": -120.5,
                "settlementRunType": "SF",
            },
        ])
        result = self.transformer.transform(raw)

        # Should keep SF (precedence 2) over II (precedence 1)
        assert len(result) == 1
        assert result["run_type"][0] == "SF"
        assert result["system_sell_price"][0] == 45.50

    def test_empty_input(self):
        """Empty DataFrame should return empty DataFrame."""
        raw = pl.DataFrame()
        result = self.transformer.transform(raw)
        assert result.is_empty()

    def test_missing_columns_returns_empty(self):
        """Missing required columns should return empty DataFrame."""
        raw = self._make_raw_df([{"foo": "bar"}])
        result = self.transformer.transform(raw)
        assert result.is_empty()

    def test_timestamp_utc_winter(self):
        """SP1 on a winter date should be 00:00 UTC."""
        raw = self._make_raw_df([
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "systemSellPrice": 45.50,
                "systemBuyPrice": 55.00,
                "netImbalanceVolume": 0.0,
                "settlementRunType": "SF",
            },
        ])
        result = self.transformer.transform(raw)
        ts = result["timestamp_utc"][0]
        # SP1 on winter day = 00:00 UTC
        assert ts.hour == 0
        assert ts.minute == 0
