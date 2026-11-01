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

    def test_bronze_date_dir_honours_dataset_directory_override(self):
        result = self.pb.bronze_date_dir(
            "elexon",
            "system_prices",
            date(2024, 1, 15),
            dataset_dir=Path("/read-only/bronze/system_prices"),
        )
        assert result == Path("/read-only/bronze/system_prices/2024/01/15")

    def test_silver_dir(self):
        result = self.pb.silver_dir("elexon", "system_prices")
        assert result == Path("/data/silver/elexon/system_prices")

    def test_silver_root(self):
        assert self.pb.silver_root() == Path("/data/silver")

    def test_silver_partition_dir(self):
        result = self.pb.silver_partition_dir("elexon", "system_prices", date(2024, 1, 15))
        assert result == Path("/data/silver/elexon/system_prices/year=2024/month=01")

    def test_silver_file(self):
        result = self.pb.silver_file("elexon", "system_prices", date(2024, 1, 15))
        expected = Path(
            "/data/silver/elexon/system_prices/year=2024/month=01/system_prices_20240115.parquet"
        )
        assert result == expected

    def test_silver_file_honours_dataset_directory_override(self):
        result = self.pb.silver_file(
            "elexon",
            "system_prices",
            date(2024, 1, 15),
            dataset_dir=Path("/temporary/silver/system_prices"),
        )
        assert result == Path(
            "/temporary/silver/system_prices/year=2024/month=01/system_prices_20240115.parquet"
        )

    def test_gold_dir(self):
        result = self.pb.gold_dir("system_marginal_price")
        assert result == Path("/data/gold/system_marginal_price")

    def test_gold_root(self):
        assert self.pb.gold_root() == Path("/data/gold")
