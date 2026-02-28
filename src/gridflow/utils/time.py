"""UTC helpers and settlement period / datetime conversion utilities."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UK_TZ = ZoneInfo("Europe/London")


def settlement_period_to_utc(settlement_date: date, period: int) -> datetime:
    """Convert Elexon settlement date + period to UTC datetime.

    Settlement period 1 starts at 00:00 UK local time on the settlement date.
    Each period is 30 minutes. During BST->GMT transitions, there are 50 periods;
    during GMT->BST transitions, there are 46 periods.
    """
    local_midnight = datetime(
        settlement_date.year,
        settlement_date.month,
        settlement_date.day,
        tzinfo=UK_TZ,
    )
    local_start = local_midnight + timedelta(minutes=30 * (period - 1))
    return local_start.astimezone(timezone.utc)


def utc_to_settlement_period(ts: datetime) -> tuple[date, int]:
    """Convert UTC timestamp to (settlement_date, period)."""
    local = ts.astimezone(UK_TZ)
    local_midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    delta_minutes = (local - local_midnight).total_seconds() / 60
    period = int(delta_minutes // 30) + 1
    return local.date(), period


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
