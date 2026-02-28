"""Unit tests for Pydantic schema validation."""

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from gridflow.schemas.elexon import ElexonSystemPrice


class TestElexonSystemPrice:
    def test_valid_record(self):
        """Valid record should pass validation."""
        record = ElexonSystemPrice(
            settlement_date=date(2024, 1, 15),
            settlement_period=1,
            timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
            system_sell_price=45.50,
            system_buy_price=55.00,
            net_imbalance_volume=-120.5,
            run_type="SF",
        )
        assert record.settlement_period == 1
        assert record.system_sell_price == 45.50
        assert record.data_provider == "elexon"

    def test_invalid_settlement_period_too_high(self):
        """SP > 50 should fail."""
        with pytest.raises(ValidationError):
            ElexonSystemPrice(
                settlement_date=date(2024, 1, 15),
                settlement_period=51,
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
                system_sell_price=45.50,
                system_buy_price=55.00,
                net_imbalance_volume=0,
                run_type="SF",
            )

    def test_invalid_settlement_period_zero(self):
        """SP < 1 should fail."""
        with pytest.raises(ValidationError):
            ElexonSystemPrice(
                settlement_date=date(2024, 1, 15),
                settlement_period=0,
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
                system_sell_price=45.50,
                system_buy_price=55.00,
                net_imbalance_volume=0,
                run_type="SF",
            )

    def test_invalid_run_type(self):
        """Invalid run type should fail."""
        with pytest.raises(ValidationError):
            ElexonSystemPrice(
                settlement_date=date(2024, 1, 15),
                settlement_period=1,
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
                system_sell_price=45.50,
                system_buy_price=55.00,
                net_imbalance_volume=0,
                run_type="INVALID",
            )

    def test_naive_timestamp_rejected(self):
        """Naive (non-UTC) timestamps should be rejected."""
        with pytest.raises(ValidationError):
            ElexonSystemPrice(
                settlement_date=date(2024, 1, 15),
                settlement_period=1,
                timestamp_utc=datetime(2024, 1, 15, 0, 0),  # no tzinfo
                system_sell_price=45.50,
                system_buy_price=55.00,
                net_imbalance_volume=0,
                run_type="SF",
            )

    def test_price_out_of_range(self):
        """Prices outside [-500, 10000] should fail."""
        with pytest.raises(ValidationError):
            ElexonSystemPrice(
                settlement_date=date(2024, 1, 15),
                settlement_period=1,
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
                system_sell_price=15000.0,  # too high
                system_buy_price=55.00,
                net_imbalance_volume=0,
                run_type="SF",
            )

    def test_extra_fields_ignored(self):
        """Extra fields should be ignored (not raise errors)."""
        record = ElexonSystemPrice(
            settlement_date=date(2024, 1, 15),
            settlement_period=1,
            timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
            system_sell_price=45.50,
            system_buy_price=55.00,
            net_imbalance_volume=0,
            run_type="SF",
            extra_field="should be ignored",  # type: ignore[call-arg]
        )
        assert not hasattr(record, "extra_field")
