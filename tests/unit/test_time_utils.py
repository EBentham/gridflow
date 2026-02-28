"""Unit tests for settlement period / UTC conversion utilities."""

from datetime import date, datetime, timedelta, timezone

import pytest

from gridflow.utils.time import (
    date_range,
    parse_lookback,
    settlement_period_to_utc,
    utc_to_settlement_period,
)


class TestSettlementPeriodToUTC:
    def test_sp1_winter(self):
        """SP1 on a winter day starts at 00:00 UTC."""
        result = settlement_period_to_utc(date(2024, 1, 15), 1)
        assert result == datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)

    def test_sp1_summer(self):
        """SP1 on a summer day starts at 23:00 UTC the previous day (BST = UTC+1)."""
        result = settlement_period_to_utc(date(2024, 7, 15), 1)
        assert result == datetime(2024, 7, 14, 23, 0, tzinfo=timezone.utc)

    def test_sp48_winter(self):
        """SP48 on a winter day starts at 23:30 UTC."""
        result = settlement_period_to_utc(date(2024, 1, 15), 48)
        assert result == datetime(2024, 1, 15, 23, 30, tzinfo=timezone.utc)

    def test_sp2(self):
        """SP2 on a winter day starts at 00:30 UTC."""
        result = settlement_period_to_utc(date(2024, 1, 15), 2)
        assert result == datetime(2024, 1, 15, 0, 30, tzinfo=timezone.utc)

    def test_result_is_utc(self):
        """Result should always be UTC."""
        result = settlement_period_to_utc(date(2024, 6, 15), 25)
        assert result.tzinfo == timezone.utc


class TestUTCToSettlementPeriod:
    def test_midnight_winter(self):
        """00:00 UTC on a winter day is SP1."""
        ts = datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
        sd, sp = utc_to_settlement_period(ts)
        assert sd == date(2024, 1, 15)
        assert sp == 1

    def test_midnight_summer(self):
        """23:00 UTC on a summer day is SP1 of the next day."""
        ts = datetime(2024, 7, 14, 23, 0, tzinfo=timezone.utc)
        sd, sp = utc_to_settlement_period(ts)
        assert sd == date(2024, 7, 15)
        assert sp == 1


class TestRoundTrip:
    def test_roundtrip_winter(self):
        """UTC -> SP -> UTC roundtrip is lossless for winter."""
        original = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
        sd, sp = utc_to_settlement_period(original)
        recovered = settlement_period_to_utc(sd, sp)
        assert recovered == original

    def test_roundtrip_summer(self):
        """UTC -> SP -> UTC roundtrip is lossless for summer."""
        original = datetime(2024, 7, 15, 10, 0, tzinfo=timezone.utc)
        sd, sp = utc_to_settlement_period(original)
        recovered = settlement_period_to_utc(sd, sp)
        assert recovered == original


class TestParseLookback:
    def test_hours(self):
        assert parse_lookback("24h") == timedelta(hours=24)

    def test_days(self):
        assert parse_lookback("7d") == timedelta(days=7)

    def test_minutes(self):
        assert parse_lookback("30m") == timedelta(minutes=30)

    def test_invalid_unit(self):
        with pytest.raises(ValueError):
            parse_lookback("10x")


class TestDateRange:
    def test_single_day(self):
        result = date_range(date(2024, 1, 15), date(2024, 1, 15))
        assert result == [date(2024, 1, 15)]

    def test_multi_day(self):
        result = date_range(date(2024, 1, 15), date(2024, 1, 17))
        assert result == [
            date(2024, 1, 15),
            date(2024, 1, 16),
            date(2024, 1, 17),
        ]

    def test_empty_range(self):
        result = date_range(date(2024, 1, 17), date(2024, 1, 15))
        assert result == []
