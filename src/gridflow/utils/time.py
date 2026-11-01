"""UTC helpers and settlement period / datetime conversion utilities."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

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
    return local_midnight.astimezone(UTC)


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


def settlement_period_to_utc_expr(
    settlement_date: str | pl.Expr, period: str | pl.Expr | int = 1
) -> pl.Expr:
    """Vector form of :func:`settlement_period_to_utc`, as a ``Datetime("us", "UTC")``.

    Column names or expressions are accepted. With no ``period`` it stamps the
    start of settlement period 1, 00:00 UK local, which is where every
    date-only Elexon row sits. UK local midnight is never inside a clock
    change, so the zone lookup cannot hit a gap or a fold, and the period
    offset is real elapsed time from that instant, as in the scalar form.

    With a ``period`` column, a null date or period raises ``ValueError`` when
    the frame is evaluated, as the scalar form does, rather than stamp a null
    instant that schema validation would only count. A null date with the
    default period gives a null instant.
    """
    if isinstance(settlement_date, str):
        settlement_date = pl.col(settlement_date)
    if isinstance(period, str):
        period = pl.col(period)
    if isinstance(period, pl.Expr):
        period = pl.struct(settlement_date.alias("date"), period.alias("period")).map_batches(
            _period_or_raise, return_dtype=pl.Int64
        )
    day_start = (
        settlement_date.cast(pl.Datetime("us"))
        .dt.replace_time_zone(UK_TZ.key)
        .dt.convert_time_zone("UTC")
    )
    return day_start + pl.duration(minutes=30 * (period - 1))


def _period_or_raise(pair: pl.Series) -> pl.Series:
    """The period half of a (date, period) struct column, refusing a null in either."""
    parts = pair.struct.unnest()
    nulls = parts.null_count().sum_horizontal().item()
    if nulls:
        raise ValueError(f"{nulls} null settlement date or period value(s): no instant to stamp")
    return parts.to_series(1).cast(pl.Int64)


def settlement_periods_in_day(settlement_date: date) -> int:
    """Return the number of settlement periods on ``settlement_date``.

    Derived from the same DST-fold-safe day-length machinery as
    :func:`settlement_period_to_utc` (:func:`_settlement_day_start_utc` of the
    next day minus this day, in half-hours) rather than a hardcoded 46/48/50
    lookup table, so a future DST rule change is picked up automatically.

    R2-B / F-22: this bounds the elexon connector's per-period REQUEST loop
    (``ElexonConnector._fetch_date_period``). The vendor's own
    ``settlementPeriod`` *parameter* validates statically against 1..50 for
    every date (measured 2026-07-27: an out-of-calendar period, e.g. SP 49 on
    a 48-period day, returns HTTP 200 with an empty ``data`` array — never a
    4xx), so this is a request-count reduction, not a crash fix. The
    project's schema-level ``1..50`` validator (``SettlementPeriodMixin``) is
    unrelated and unchanged.

    Args:
        settlement_date: The UK settlement date.

    Returns:
        46 on the spring-forward DST day, 50 on the autumn-back DST day, 48
        on every ordinary day.
    """
    half_hours = (
        _settlement_day_start_utc(settlement_date + timedelta(days=1))
        - _settlement_day_start_utc(settlement_date)
    ).total_seconds() / 1800
    return round(half_hours)


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
    try:
        value = int(lookback[:-1])
    except ValueError:
        raise ValueError(
            f"Malformed lookback {lookback!r}; expected e.g. '24h', '7d', '30m'."
        ) from None
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


def window_dates(start: datetime, end: datetime) -> list[date]:
    """UTC calendar dates touched by the half-open window ``[start, end)``.

    Every window inside gridflow is half-open, so an ``end`` at midnight
    excludes that date. A zero-width window (``start == end``) yields start's
    date, so a point request still covers one day.
    """
    if end < start:
        raise ValueError(f"window end {end!r} is before start {start!r}")
    last = (end - timedelta(microseconds=1)).date() if end > start else start.date()
    return date_range(start.date(), last)


def day_subwindows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Clamped calendar-day sub-windows covering ``[start, end)`` exactly.

    Used by the ENTSO-E, ENTSO-G and NESO Carbon Intensity connectors to chunk a
    multi-day fetch window into one request per covered UTC calendar day, so
    bronze ``data_date`` honours its documented contract (the calendar date the
    data refers to) instead of stamping every day's rows under the window's
    start date.

    Args:
        start: Window start. Must be tz-aware UTC.
        end: Window end (exclusive). Must be tz-aware UTC and ``>= start``.

    Returns:
        A list of ``(sub_start, sub_end)`` pairs, one per UTC calendar day
        touched by ``[start, end)``, clamped to the window bounds. An ``end``
        that falls exactly at midnight excludes that date (matching the
        exclusive end of every gridflow window).
        ``start == end`` returns an empty list; callers apply their own
        degenerate-window guard (a single legacy-shape request).

    Raises:
        ValueError: if either argument is naive, has a non-zero UTC offset, or
            if ``end < start``.
    """
    for name, value in (("start", start), ("end", end)):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"day_subwindows: {name} must be tz-aware, got naive {value!r}")
        if value.utcoffset() != timedelta(0):
            raise ValueError(f"day_subwindows: {name} must be UTC (zero offset), got {value!r}")
    if end < start:
        raise ValueError(f"day_subwindows: end {end!r} is before start {start!r}")
    if end == start:
        return []

    windows: list[tuple[datetime, datetime]] = []
    for day in date_range(start.date(), end.date()):
        day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        sub_start = max(start, day_start)
        sub_end = min(end, day_start + timedelta(days=1))
        if sub_start < sub_end:
            windows.append((sub_start, sub_end))
    return windows
