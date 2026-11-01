"""The vector settlement-period conversion equals the scalar one, pair for pair.

1,100 consecutive settlement dates from 2023-01-01 (three spring and three
autumn clock changes) times periods 1..50 gives 55,000 pairs. Periods past a
day's own count are included on purpose: the scalar steps real time past the
day's end and the vector form must agree there too.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from gridflow.utils.time import settlement_period_to_utc, settlement_period_to_utc_expr

FIRST_DATE = date(2023, 1, 1)
N_DATES = 1_100
PERIODS = range(1, 51)


def test_vector_equals_scalar_over_55000_pairs() -> None:
    dates = [FIRST_DATE + timedelta(days=i) for i in range(N_DATES)]
    pairs = pl.DataFrame(
        {
            "settlement_date": [d for d in dates for _ in PERIODS],
            "settlement_period": [p for _ in dates for p in PERIODS],
        },
        schema={"settlement_date": pl.Date, "settlement_period": pl.Int32},
    )
    assert pairs.height == 55_000

    vector = pairs.select(
        settlement_period_to_utc_expr("settlement_date", "settlement_period")
    ).to_series()
    scalar = [
        settlement_period_to_utc(d, p)
        for d, p in pairs.select("settlement_date", "settlement_period").iter_rows()
    ]

    assert vector.dtype == pl.Datetime("us", "UTC")
    mismatches = [
        (row, got, want)
        for row, (got, want) in enumerate(zip(vector.to_list(), scalar, strict=True))
        if got != want
    ]
    assert not mismatches, mismatches[:5]


def test_date_only_form_equals_scalar_period_1() -> None:
    dates = [FIRST_DATE + timedelta(days=i) for i in range(N_DATES)]
    frame = pl.DataFrame({"d": dates}, schema={"d": pl.Date})
    vector = frame.select(settlement_period_to_utc_expr("d")).to_series().to_list()
    assert vector == [settlement_period_to_utc(d, 1) for d in dates]
