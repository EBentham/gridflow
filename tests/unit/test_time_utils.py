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


class TestDSTSpringTransition:
    """2024-03-31: clocks go forward 01:00->02:00 BST. Short day = 46 SPs.

    The local hour 01:00-02:00 does not exist, so SP3 (01:00 local nominal)
    falls at 01:00 UTC and every later SP steps real 30-min UTC time from there.
    All expected instants below are hardcoded known-good UTC, NOT recomputed
    via zoneinfo, so a shared arithmetic error cannot mask the assertion.
    """

    DATE = date(2024, 3, 31)
    N_PERIODS = 46

    def test_all_periods_strictly_monotonic_and_unique(self):
        """Every one of the 46 SPs maps to a distinct, increasing UTC instant.

        FAILS on the buggy converter: SP5/SP6 collide with SP3/SP4 on 01:00Z/
        01:30Z, giving 44 unique values and a non-monotonic sequence.
        """
        instants = [
            settlement_period_to_utc(self.DATE, sp)
            for sp in range(1, self.N_PERIODS + 1)
        ]
        assert len(set(instants)) == self.N_PERIODS, "duplicate timestamp_utc across SPs"
        assert all(
            instants[i] < instants[i + 1] for i in range(len(instants) - 1)
        ), "timestamp_utc not strictly increasing across SPs"

    def test_consecutive_periods_are_30_min_of_real_time(self):
        """Each consecutive SP pair is exactly 1800s of real elapsed UTC time."""
        instants = [
            settlement_period_to_utc(self.DATE, sp)
            for sp in range(1, self.N_PERIODS + 1)
        ]
        gaps = {
            int((instants[i + 1] - instants[i]).total_seconds())
            for i in range(len(instants) - 1)
        }
        assert gaps == {1800}, f"non-uniform 30-min steps: {sorted(gaps)}"

    def test_anchor_instants(self):
        """Hardcoded known-good UTC anchors for the spring short day."""
        assert settlement_period_to_utc(self.DATE, 1) == datetime(
            2024, 3, 31, 0, 0, tzinfo=timezone.utc
        )
        # SP3 = the (non-existent) 01:00 local => 01:00 UTC. Buggy code agrees
        # here but then repeats it at SP5; the monotonic test catches that.
        assert settlement_period_to_utc(self.DATE, 3) == datetime(
            2024, 3, 31, 1, 0, tzinfo=timezone.utc
        )
        # SP46 is the last period of a 23h day -> 22:30 UTC.
        assert settlement_period_to_utc(self.DATE, 46) == datetime(
            2024, 3, 31, 22, 30, tzinfo=timezone.utc
        )


class TestDSTAutumnTransition:
    """2024-10-27: clocks go back 02:00->01:00 BST. Long day = 50 SPs.

    The local hour 01:00-02:00 occurs twice. SP5 is the *first* (BST) 01:00
    local = 01:00 UTC; SP7 is the *second* (GMT) 01:00 local = 02:00 UTC.
    The buggy forward converter skips the repeated real hour (SP5 jumps to
    02:00Z), so a plain monotonic+unique assertion would PASS on the bug --
    we assert the real-time gap and absolute anchors instead.
    """

    DATE = date(2024, 10, 27)
    N_PERIODS = 50

    def test_all_periods_strictly_monotonic_and_unique(self):
        """50 distinct increasing instants (regression guard, not the red test)."""
        instants = [
            settlement_period_to_utc(self.DATE, sp)
            for sp in range(1, self.N_PERIODS + 1)
        ]
        assert len(set(instants)) == self.N_PERIODS
        assert all(instants[i] < instants[i + 1] for i in range(len(instants) - 1))

    def test_no_skipped_or_aliased_real_half_hour(self):
        """Every consecutive SP pair is exactly 1800s -- no skipped UTC half-hour.

        FAILS on the buggy converter: SP4->SP5 is a 5400s jump (00:30Z->02:00Z),
        aliasing away the repeated 01:00-02:00 local hour.
        """
        instants = [
            settlement_period_to_utc(self.DATE, sp)
            for sp in range(1, self.N_PERIODS + 1)
        ]
        gaps = {
            int((instants[i + 1] - instants[i]).total_seconds())
            for i in range(len(instants) - 1)
        }
        assert gaps == {1800}, f"a real half-hour was skipped/aliased: {sorted(gaps)}"

    def test_anchor_instants(self):
        """Hardcoded known-good UTC anchors spanning the repeated hour.

        SP5 == 01:00Z FAILS on the buggy converter (it returns 02:00Z).
        """
        assert settlement_period_to_utc(self.DATE, 1) == datetime(
            2024, 10, 26, 23, 0, tzinfo=timezone.utc
        )
        # First (BST) occurrence of 01:00 local == 01:00 UTC.
        assert settlement_period_to_utc(self.DATE, 5) == datetime(
            2024, 10, 27, 1, 0, tzinfo=timezone.utc
        )
        # Second (GMT) occurrence of 01:00 local == 02:00 UTC.
        assert settlement_period_to_utc(self.DATE, 7) == datetime(
            2024, 10, 27, 2, 0, tzinfo=timezone.utc
        )
        # SP50 is the last period of a 25h day -> 23:30 UTC same date.
        assert settlement_period_to_utc(self.DATE, 50) == datetime(
            2024, 10, 27, 23, 30, tzinfo=timezone.utc
        )

    def test_inverse_disambiguates_repeated_hour(self):
        """00:00Z and 01:00Z on the autumn day map to DISTINCT periods.

        FAILS on the buggy inverse: both collapse to (2024-10-27, SP3).
        """
        sd0, sp0 = utc_to_settlement_period(
            datetime(2024, 10, 27, 0, 0, tzinfo=timezone.utc)
        )
        sd1, sp1 = utc_to_settlement_period(
            datetime(2024, 10, 27, 1, 0, tzinfo=timezone.utc)
        )
        assert (sd0, sp0) == (date(2024, 10, 27), 3)
        assert (sd1, sp1) == (date(2024, 10, 27), 5)
        assert sp0 != sp1


class TestDSTRoundTrip:
    """Forward o inverse must round-trip losslessly for EVERY SP on the
    transition days, not just non-DST dates. FAILS on the buggy converter,
    which breaks 2 round-trips on each transition day."""

    @pytest.mark.parametrize(
        "settlement_date,n_periods",
        [
            (date(2024, 3, 31), 46),   # spring short day
            (date(2024, 10, 27), 50),  # autumn long day
            (date(2024, 1, 15), 48),   # winter regression guard
            (date(2024, 7, 15), 48),   # summer regression guard
        ],
    )
    def test_forward_inverse_roundtrip_every_period(self, settlement_date, n_periods):
        for sp in range(1, n_periods + 1):
            ts = settlement_period_to_utc(settlement_date, sp)
            recovered_date, recovered_sp = utc_to_settlement_period(ts)
            assert (recovered_date, recovered_sp) == (settlement_date, sp), (
                f"SP{sp} on {settlement_date} round-tripped to "
                f"({recovered_date}, SP{recovered_sp})"
            )

    @pytest.mark.parametrize(
        "first_instant,n_periods",
        [
            (datetime(2024, 3, 31, 0, 0, tzinfo=timezone.utc), 46),
            (datetime(2024, 10, 26, 23, 0, tzinfo=timezone.utc), 50),
        ],
    )
    def test_inverse_forward_roundtrip_every_real_half_hour(
        self, first_instant, n_periods
    ):
        """UTC -> SP -> UTC is lossless for every real half-hour of the day."""
        for i in range(n_periods):
            ts = first_instant + timedelta(minutes=30 * i)
            sd, sp = utc_to_settlement_period(ts)
            assert settlement_period_to_utc(sd, sp) == ts, (
                f"UTC {ts.isoformat()} -> ({sd}, SP{sp}) did not round-trip"
            )


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
