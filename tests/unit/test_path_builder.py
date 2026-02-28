"""Unit tests for the path builder."""

from datetime import date
from pathlib import Path

from gridflow.storage.paths import PathBuilder


class TestPathBuilder:
    def setup_method(self):
        self.pb = PathBuilder(Path("/data"))

    def test_bronze_dir(self):
        result = self.pb.bronze_dir("elexon", "system_prices")
        assert result == Path("/data/bronze/elexon/system_prices")

    def test_bronze_date_dir(self):
        result = self.pb.bronze_date_dir("elexon", "system_prices", date(2024, 1, 15))
        assert result == Path("/data/bronze/elexon/system_prices/2024/01/15")

    def test_silver_dir(self):
        result = self.pb.silver_dir("elexon", "system_prices")
        assert result == Path("/data/silver/elexon/system_prices")

    def test_silver_partition_dir(self):
        result = self.pb.silver_partition_dir("elexon", "system_prices", date(2024, 1, 15))
        assert result == Path("/data/silver/elexon/system_prices/year=2024/month=01")

    def test_silver_file(self):
        result = self.pb.silver_file("elexon", "system_prices", date(2024, 1, 15))
        expected = Path(
            "/data/silver/elexon/system_prices/year=2024/month=01/system_prices_20240115.parquet"
        )
        assert result == expected

    def test_gold_dir(self):
        result = self.pb.gold_dir("system_marginal_price")
        assert result == Path("/data/gold/system_marginal_price")

    def test_gold_file(self):
        result = self.pb.gold_file("system_marginal_price", date(2024, 1, 15))
        expected = Path(
            "/data/gold/system_marginal_price/year=2024/system_marginal_price_20240115.parquet"
        )
        assert result == expected

    def test_duckdb_path(self):
        result = self.pb.duckdb_path()
        assert result == Path("/data/gridflow.duckdb")
