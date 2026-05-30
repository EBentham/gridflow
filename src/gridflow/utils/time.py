"""UTC helpers and settlement period / datetime conversion utilities."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UK_TZ = ZoneInfo("Europe/London")


def _settlement_day_start_utc(settlement_date: date) -> datetime:
    """UTC instant of settlement period 1 (00:00 UK local) for a settlement date.

    Anchoring SP1 in UTC and stepping real time from it is what makes the
    converter DST-fold-safe. Building the local midnight then converting once
    pins the day start regardless of whether the day is 23h (spring), 24h, or
    25h (autumn) long.
    """
    local_midnight = datetime(
        settlement_date.year,
        settlement_date.month,
        settlement_date.day,
        tzinfo=UK_TZ,
    )
    return local_midnight.astimezone(timezone.utc)


def settlement_period_to_utc(settlement_date: date, period: int) -> datetime:
    """Convert Elexon settlement date + period to UTC datetime.

    Settlement period 1 starts at 00:00 UK local time on the settlement date.
    Each subsequent period is 30 minutes of *real elapsed time* from there, so
    the day spans 46 periods (23h) on the spring-forward day and 50 periods
    (25h) on the autumn-back day.

    The step is applied to SP1's UTC instant, not to a ``zoneinfo`` local
    datetime: adding a wall-clock ``timedelta`` to a tz-aware local datetime
    does not cross the DST fold, which previously collapsed two spring periods
    onto one UTC instant and skipped a real autumn half-hour.
    """
    sp1_utc = _settlement_day_start_utc(settlement_date)
    return sp1_utc + timedelta(minutes=30 * (period - 1))


def utc_to_settlement_period(ts: datetime) -> tuple[date, int]:
    """Convert UTC timestamp to (settlement_date, period).

    Exact inverse of :func:`settlement_period_to_utc`. The settlement date is
    the UK-local calendar date of ``ts``; the period is real elapsed half-hours
    from that day's SP1 UTC instant. Disambiguating by real elapsed time (not
    local wall clock) keeps the two occurrences of the repeated autumn
    01:00-02:00 local hour on distinct periods.

    Naive datetimes are coerced as system-local by ``astimezone`` (preserving
    the pre-existing contract); callers pass tz-aware UTC per the project rule.
    """
    settlement_date = ts.astimezone(UK_TZ).date()
    sp1_utc = _settlement_day_start_utc(settlement_date)
    period = int(round((ts - sp1_utc).total_seconds() / 1800)) + 1
    return settlement_date, period


def parse_lookback(lookback: str) -> timedelta:
    """Parse a lookback string like '24h' or '7d' into a timedelta."""
    value = int(lookback[:-1])
    unit = lookback[-1].lower()
    if unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    elif unit == "m":
        return timedelta(minutes=value)
    else:
        raise ValueError(f"Unknown lookback unit: {unit}. Use 'h', 'd', or 'm'.")


def date_range(start: date, end: date) -> list[date]:
    """Generate a list of dates from start to end (inclusive)."""
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates
