"""Data contract tests: verify gold output is correctly built from silver."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.gold.demand_features import DemandFeaturesBuilder
from gridflow.gold.merit_order import MeritOrderBuilder
from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder
from gridflow.silver.elexon.system_prices import SystemPriceTransformer


class TestSilverGoldContract:
    def test_gold_built_from_silver(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """Gold dataset should contain enriched silver data."""
        # Build silver first
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)

        transformer = SystemPriceTransformer(tmp_data_dir)
        transformer.run(date(2024, 1, 15))

        # Build gold
        builder = SystemMarginalPriceBuilder(tmp_data_dir)
        rows = builder.run(date(2024, 1, 15), date(2024, 1, 15))

        assert rows > 0

    def test_gold_has_derived_features(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """Gold dataset should contain derived features like spread."""
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)

        transformer = SystemPriceTransformer(tmp_data_dir)
        transformer.run(date(2024, 1, 15))

        builder = SystemMarginalPriceBuilder(tmp_data_dir)
        df = builder.build(date(2024, 1, 15), date(2024, 1, 15))

        assert "spread" in df.columns
        assert "abs_imbalance" in df.columns

    def test_spread_sign_convention(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """spread MUST equal buy - sell (not sell - buy).

        The sample fixture has buy > sell on every row, so a flipped
        convention would produce negative spreads. Asserting the value, not
        just column presence, catches a sign flip a refactor could introduce.
        """
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)
        SystemPriceTransformer(tmp_data_dir).run(date(2024, 1, 15))

        df = SystemMarginalPriceBuilder(tmp_data_dir).build(
            date(2024, 1, 15), date(2024, 1, 15)
        )

        expected = df["system_buy_price"] - df["system_sell_price"]
        assert df["spread"].to_list() == expected.to_list()
        # Fixture: buy strictly above sell -> every spread strictly positive.
        assert (df["spread"] > 0).all()

    def test_abs_imbalance_value(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """abs_imbalance MUST equal |net_imbalance_volume|, incl. negative NIV.

        The fixture's SP1 has NIV = -120.5; abs_imbalance must be +120.5.
        """
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)
        SystemPriceTransformer(tmp_data_dir).run(date(2024, 1, 15))

        df = SystemMarginalPriceBuilder(tmp_data_dir).build(
            date(2024, 1, 15), date(2024, 1, 15)
        )

        expected = df["net_imbalance_volume"].abs()
        assert df["abs_imbalance"].to_list() == expected.to_list()
        # All abs_imbalance values are non-negative even for negative NIV rows.
        assert (df["abs_imbalance"] >= 0).all()
        # At least one source NIV is negative -> proves abs() actually applied.
        assert (df["net_imbalance_volume"] < 0).any()

    def test_gold_row_count_matches_silver(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """The builder must not fan out: N silver rows -> N gold rows."""
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)
        silver_rows = SystemPriceTransformer(tmp_data_dir).run(date(2024, 1, 15))

        df = SystemMarginalPriceBuilder(tmp_data_dir).build(
            date(2024, 1, 15), date(2024, 1, 15)
        )
        assert len(df) == silver_rows


class TestGoldBuilderEmptyOutGuard:
    """Guard that distinguishes "nothing to build" from "builder is broken".

    A real builder fed non-empty silver must not silently emit zero gold rows;
    the intentional Phase-3 placeholders must stay empty until implemented, and
    the guard must flip to a failure the moment they start producing rows.
    """

    def test_real_builder_with_silver_emits_rows(
        self, tmp_data_dir: Path, sample_raw_response: RawResponse
    ):
        """SystemMarginalPriceBuilder fed non-empty silver must emit gold rows.

        Guards against a future regression turning a real build into a silent
        0-row no-write (today only a log.warning, never a test failure).
        """
        writer = BronzeWriter(tmp_data_dir)
        writer.write(sample_raw_response)
        silver_rows = SystemPriceTransformer(tmp_data_dir).run(date(2024, 1, 15))
        assert silver_rows > 0, "fixture must seed non-empty silver"

        gold_rows = SystemMarginalPriceBuilder(tmp_data_dir).run(
            date(2024, 1, 15), date(2024, 1, 15)
        )
        assert gold_rows > 0, (
            "real builder with non-empty silver must not silently emit 0 gold rows"
        )

    def test_empty_in_empty_out_stays_benign(self, tmp_data_dir: Path):
        """No silver in -> 0 gold rows, no error (the benign empty-in case)."""
        # No bronze / silver seeded for system_prices.
        gold_rows = SystemMarginalPriceBuilder(tmp_data_dir).run(
            date(2024, 1, 15), date(2024, 1, 15)
        )
        assert gold_rows == 0

    @pytest.mark.xfail(
        strict=True,
        reason="DemandFeaturesBuilder is an intentional Phase-3 placeholder; "
        "this xfail flips to a failure the moment it is implemented and emits "
        "rows, forcing a real value-level contract test to be written.",
    )
    def test_demand_features_placeholder_is_unimplemented(self, tmp_data_dir: Path):
        df = DemandFeaturesBuilder(tmp_data_dir).build(
            date(2024, 1, 15), date(2024, 1, 15)
        )
        # While a placeholder, build() returns empty -> this assertion fails ->
        # xfail. Once implemented to return rows, the assertion passes ->
        # xpass -> strict xfail turns the suite RED, demanding a real test.
        assert not df.is_empty()

    @pytest.mark.xfail(
        strict=True,
        reason="MeritOrderBuilder is an intentional Phase-3 placeholder; this "
        "xfail flips to a failure the moment it is implemented and emits rows.",
    )
    def test_merit_order_placeholder_is_unimplemented(self, tmp_data_dir: Path):
        df = MeritOrderBuilder(tmp_data_dir).build(
            date(2024, 1, 15), date(2024, 1, 15)
        )
        assert not df.is_empty()
