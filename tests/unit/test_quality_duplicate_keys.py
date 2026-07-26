"""F-16: the duplicate check gets the dataset's real entity key (R2-A Task 3).

The hardcoded ``(settlement_date, settlement_period)`` pair fed every
dataset's duplicate check regardless of its real grain, falsely flagging
every genuinely-distinct row of a finer-grained dataset (``fuelhh``'s
``fuel_type``) as a duplicate: 27,959 false positives alongside the 580 real
ones (measured, ``R2-A-PLAN.md`` S1.1).
"""

from __future__ import annotations

import logging

import polars as pl

from gridflow.cli import _entity_key_for
from gridflow.quality.checks import check_duplicates
from gridflow.silver.elexon.fuelhh import FuelHHTransformer


def _fuelhh_frame() -> pl.DataFrame:
    """Same (settlement_date, settlement_period) across 20 distinct fuel
    types -- a real quality-check frame's shape, not a duplicate."""
    return pl.DataFrame(
        {
            "settlement_date": ["2026-07-11"] * 20,
            "settlement_period": [1] * 20,
            "fuel_type": [f"FUEL_{i}" for i in range(20)],
        }
    )


class TestFuelhhFalsePositive:
    def test_fuelhh_distinct_fuel_types_are_not_duplicates(self) -> None:
        """RED against the pre-fix hardcoded pair: this exact frame reports
        20 duplicate 'keys' under (settlement_date, settlement_period) alone."""
        df = _fuelhh_frame()
        entity_key = _entity_key_for("elexon", "fuelhh", df.columns)
        assert entity_key == ["settlement_date", "settlement_period", "fuel_type"]

        result = check_duplicates(df, entity_key, source="elexon", dataset="fuelhh")
        assert result.passed is True
        assert result.metric == 0.0

        # The RED comparison, demonstrated inline: the OLD hardcoded pair
        # DOES flag this exact frame as one giant duplicate group.
        legacy_result = check_duplicates(
            df, ["settlement_date", "settlement_period"], source="elexon", dataset="fuelhh"
        )
        assert legacy_result.passed is False
        assert legacy_result.metric == 1.0  # one duplicate key combination, 20 rows

    def test_genuine_duplicate_on_the_full_key_is_still_reported(self) -> None:
        df = pl.concat([_fuelhh_frame(), _fuelhh_frame().head(1)])  # one real dupe
        entity_key = _entity_key_for("elexon", "fuelhh", df.columns)

        result = check_duplicates(df, entity_key, source="elexon", dataset="fuelhh")
        assert result.passed is False
        assert result.metric == 1.0


class TestEntityKeyFallback:
    def test_unregistered_dataset_falls_back_loudly(self, caplog) -> None:
        available = ["settlement_date", "settlement_period", "some_other_col"]
        with caplog.at_level(logging.WARNING):
            key = _entity_key_for("elexon", "totally_unregistered_dataset", available)

        assert key == ["settlement_date", "settlement_period"]
        assert any("No declared ENTITY_KEY_COLUMNS" in r.message for r in caplog.records)

    def test_non_elexon_dataset_without_legacy_columns_is_skipped(self) -> None:
        """Preserves today's behaviour exactly: a non-Elexon frame with
        neither a declared key nor the legacy pair is not checked at all
        (returns None), same as before this fix."""
        available = ["timestamp_utc", "area_code", "price_eur_mwh"]
        key = _entity_key_for("entsoe", "day_ahead_prices", available)
        assert key is None

    def test_optional_column_included_only_when_present(self) -> None:
        with_boundary = _entity_key_for(
            "elexon", "tsdf", ["settlement_date", "settlement_period", "boundary"]
        )
        without_boundary = _entity_key_for(
            "elexon", "tsdf", ["settlement_date", "settlement_period"]
        )
        assert with_boundary == ["settlement_date", "settlement_period", "boundary"]
        assert without_boundary == ["settlement_date", "settlement_period"]


def test_fuelhh_entity_key_columns_class_attribute() -> None:
    """Pins the hoisted class attribute directly against the transform()'s
    own dedup subset."""
    assert FuelHHTransformer.ENTITY_KEY_COLUMNS == (
        "settlement_date",
        "settlement_period",
        "fuel_type",
    )
