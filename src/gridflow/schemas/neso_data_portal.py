"""The NESO Open Data Portal settlement-period bound (D-27).

Distinct from ``gridflow.schemas.neso``, which is the Carbon Intensity API
(D-01). The portal's silver columns come from frozen registry records
(ADR-034), not Pydantic contracts; only the bound below is shared.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gridflow.utils.time import settlement_periods_in_day

if TYPE_CHECKING:
    from datetime import date


def is_valid_settlement_period(settlement_date: date, settlement_period: int) -> bool:
    """Does ``settlement_period`` exist on ``settlement_date``? (D-27)

    **One predicate.** The generic engine's ``sp_pair`` exclusion
    (``casting``) calls this rather than restating the comparison: a
    hand-written copy of a two-sided bound is how one ends up upper-only, which
    would let SP0 through to a wrong-day ``event_time``.

    The bound is **two-sided**, and both sides are load-bearing because
    ``settlement_period_to_utc`` bound-checks nothing:

    * **upper** — SP49 on an ordinary 48-period day, or SP47 on a 46-period
      spring day, lands in the *next* settlement day;
    * **lower** — SP0 lands in the *previous* one (measured:
      ``settlement_period_to_utc(2026-08-16, 0)`` is ``2026-08-15 22:30Z``,
      half an hour before that day's SP1), and a negative period walks further
      back still.

    The day's length comes from :func:`~gridflow.utils.time.settlement_periods_in_day`,
    which derives 46/48/50 from the DST machinery rather than a hardcoded table.

    **Scope: NESO-local, deliberately.** The shared
    :class:`~gridflow.schemas.common.SettlementPeriodMixin` declares no bound at
    all — not an insufficient one, none — and adding one there would change
    validation for every settlement-based schema in the repo. That needs its own
    blast-radius review (TODO-12) and is not smuggled in here.

    Args:
        settlement_date: The UK settlement date the period is stated against.
        settlement_period: The vendor's settlement period, unmodified.

    Returns:
        ``True`` when ``1 <= settlement_period <=
        settlement_periods_in_day(settlement_date)``.
    """
    return 1 <= settlement_period <= settlement_periods_in_day(settlement_date)
