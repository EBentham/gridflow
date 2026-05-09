"""Unit tests for Pydantic schema validation."""

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from gridflow.schemas.elexon import (
    ElexonBMUnit,
    ElexonBOAL,
    ElexonBOD,
    ElexonDemandForecast,
    ElexonDISBSAD,
    ElexonFrequency,
    ElexonFuelHH,
    ElexonMID,
    ElexonPN,
    ElexonSystemPrice,
    ElexonWindForecast,
)


class TestElexonSystemPrice:
    def test_valid_record(self):
        """Valid record should pass validation."""
        record = ElexonSystemPrice(
            settlement_date=date(2024, 1, 15),
            settlement_period=1,
            timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
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
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
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
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
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
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
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
                timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
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
            timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
            system_sell_price=45.50,
            system_buy_price=55.00,
            net_imbalance_volume=0,
            run_type="SF",
            extra_field="should be ignored",  # type: ignore[call-arg]
        )
        assert not hasattr(record, "extra_field")

    def test_run_type_optional(self):
        """V2-FIX-04: /balancing/settlement/system-prices/{date} does not
        expose any field that maps to BSC run-type semantics. The schema
        must accept run_type=None (the default) so live silver rows from
        that endpoint validate cleanly."""
        record = ElexonSystemPrice(
            settlement_date=date(2026, 5, 6),
            settlement_period=1,
            timestamp_utc=datetime(2026, 5, 6, 0, 0, tzinfo=UTC),
            system_sell_price=96.79,
            system_buy_price=96.79,
            net_imbalance_volume=-37.99,
        )
        assert record.run_type is None

    def test_price_derivation_code_accepts_live_values(self):
        """V2-FIX-04: live API returns priceDerivationCode in {'N','P'}.
        These are unrelated to BSC run types — they describe how the
        SBP/SSP was derived for the period. Surfaced as a separate
        silver column with no regex constraint."""
        for code in ("N", "P"):
            record = ElexonSystemPrice(
                settlement_date=date(2026, 5, 6),
                settlement_period=1,
                timestamp_utc=datetime(2026, 5, 6, 0, 0, tzinfo=UTC),
                system_sell_price=96.79,
                system_buy_price=96.79,
                net_imbalance_volume=-37.99,
                price_derivation_code=code,
            )
            assert record.price_derivation_code == code

    def test_price_derivation_code_optional(self):
        """price_derivation_code defaults to None when the endpoint
        (or fixture) does not surface it."""
        record = ElexonSystemPrice(
            settlement_date=date(2024, 1, 15),
            settlement_period=1,
            timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
            system_sell_price=45.50,
            system_buy_price=55.00,
            net_imbalance_volume=0,
            run_type="SF",
        )
        assert record.price_derivation_code is None


_TS = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
_DT = date(2024, 1, 15)


class TestElexonFuelHH:
    def test_valid_record(self):
        r = ElexonFuelHH(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            fuel_type="CCGT", generation_mw=12500.0,
        )
        assert r.fuel_type == "CCGT"
        assert r.data_provider == "elexon"

    def test_invalid_settlement_period(self):
        with pytest.raises(ValidationError):
            ElexonFuelHH(
                settlement_date=_DT, settlement_period=0, timestamp_utc=_TS,
                fuel_type="CCGT", generation_mw=12500.0,
            )

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            ElexonFuelHH(
                settlement_date=_DT, settlement_period=1,
                timestamp_utc=datetime(2024, 1, 15, 0, 0),  # naive
                fuel_type="CCGT", generation_mw=12500.0,
            )


class TestElexonBOAL:
    def test_valid_record(self):
        r = ElexonBOAL(
            settlement_date=_DT, settlement_period=10, timestamp_utc=_TS,
            bm_unit_id="T_DRAXX-1",
        )
        assert r.bm_unit_id == "T_DRAXX-1"
        assert r.so_flag is False

    def test_optional_fields_default_false(self):
        r = ElexonBOAL(
            settlement_date=_DT, settlement_period=10, timestamp_utc=_TS,
            bm_unit_id="T_DRAXX-1",
        )
        assert r.deem_flag is False
        assert r.stor_flag is False
        assert r.rr_flag is False

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            ElexonBOAL(
                settlement_date=_DT, settlement_period=10,
                timestamp_utc=datetime(2024, 1, 15, 4, 30),
                bm_unit_id="T_DRAXX-1",
            )


class TestElexonBOD:
    def test_valid_record(self):
        r = ElexonBOD(
            settlement_date=_DT, settlement_period=10, timestamp_utc=_TS,
            bm_unit_id="T_DRAXX-1", bid_price=-50.0, offer_price=75.0,
        )
        assert r.bid_price == -50.0

    def test_optional_prices(self):
        r = ElexonBOD(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            bm_unit_id="T_DRAXX-1",
        )
        assert r.bid_price is None
        assert r.offer_price is None


class TestElexonMID:
    def test_valid_record(self):
        r = ElexonMID(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            data_provider_id="N2EXMIP", market_index_price=65.32,
        )
        assert r.data_provider_id == "N2EXMIP"

    def test_all_optional(self):
        # Only required fields are date, period, timestamp
        r = ElexonMID(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
        )
        assert r.market_index_price is None


class TestElexonFrequency:
    def test_valid_record(self):
        r = ElexonFrequency(timestamp_utc=_TS, frequency_hz=50.01)
        assert r.frequency_hz == 50.01
        assert r.data_provider == "elexon"

    def test_frequency_below_range(self):
        with pytest.raises(ValidationError):
            ElexonFrequency(timestamp_utc=_TS, frequency_hz=48.5)

    def test_frequency_above_range(self):
        with pytest.raises(ValidationError):
            ElexonFrequency(timestamp_utc=_TS, frequency_hz=51.5)

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            ElexonFrequency(timestamp_utc=datetime(2024, 1, 15, 0, 0), frequency_hz=50.0)


class TestElexonDemandForecast:
    def test_valid_ndf(self):
        r = ElexonDemandForecast(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            forecast_type="day_ahead", national_demand_mw=28500.0,
        )
        assert r.forecast_type == "day_ahead"

    def test_valid_ndfd(self):
        r = ElexonDemandForecast(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            forecast_type="2_14_day", national_demand_mw=29000.0,
        )
        assert r.forecast_type == "2_14_day"

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            ElexonDemandForecast(
                settlement_date=_DT, settlement_period=1,
                timestamp_utc=datetime(2024, 1, 15, 0, 0),
                forecast_type="day_ahead", national_demand_mw=28500.0,
            )


class TestElexonWindForecast:
    def test_valid_with_settlement(self):
        r = ElexonWindForecast(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            initial_forecast_mw=4500.0, latest_forecast_mw=4320.0,
        )
        assert r.initial_forecast_mw == 4500.0

    def test_valid_without_settlement(self):
        """settlement_date and period are optional in WindForecast."""
        r = ElexonWindForecast(timestamp_utc=_TS, latest_forecast_mw=4320.0)
        assert r.settlement_date is None
        assert r.settlement_period is None

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            ElexonWindForecast(timestamp_utc=datetime(2024, 1, 15, 0, 0))


class TestElexonPN:
    def test_valid_record(self):
        r = ElexonPN(
            settlement_date=_DT, settlement_period=10, timestamp_utc=_TS,
            bm_unit_id="T_DRAXX-1", level_from=380.0, level_to=400.0,
        )
        assert r.level_from == 380.0

    def test_optional_levels(self):
        r = ElexonPN(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            bm_unit_id="T_DRAXX-1",
        )
        assert r.level_from is None

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            ElexonPN(
                settlement_date=_DT, settlement_period=1,
                timestamp_utc=datetime(2024, 1, 15, 5, 0),
                bm_unit_id="T_DRAXX-1",
            )


class TestElexonDISBSAD:
    def test_valid_record(self):
        r = ElexonDISBSAD(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
            adjustment_action_id="DISBSAD-001", component="ENERGY",
            cost=1250.50, volume=15.0,
        )
        assert r.cost == 1250.50

    def test_optional_flags_default_false(self):
        r = ElexonDISBSAD(
            settlement_date=_DT, settlement_period=1, timestamp_utc=_TS,
        )
        assert r.so_flag is False
        assert r.stor_flag is False

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            ElexonDISBSAD(
                settlement_date=_DT, settlement_period=1,
                timestamp_utc=datetime(2024, 1, 15, 0, 0),
            )


class TestElexonBMUnit:
    def test_valid_record(self):
        r = ElexonBMUnit(
            bm_unit_id="T_DRAXX-1",
            bm_unit_name="Drax Power Station Unit 1",
            fuel_type="BIOMASS",
            registered_capacity_mw=645.0,
            company_name="Drax Power Ltd",
        )
        assert r.bm_unit_id == "T_DRAXX-1"
        assert r.data_provider == "elexon"

    def test_all_optional_except_id(self):
        r = ElexonBMUnit(bm_unit_id="T_TEST-1")
        assert r.fuel_type is None
        assert r.registered_capacity_mw is None

    def test_extra_fields_ignored(self):
        r = ElexonBMUnit(
            bm_unit_id="T_DRAXX-1",
            extra_field="ignored",  # type: ignore[call-arg]
        )
        assert not hasattr(r, "extra_field")
