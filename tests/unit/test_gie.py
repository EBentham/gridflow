"""Unit tests for GIE AGSI+/ALSI connector, schemas, and silver transformers."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from gridflow.connectors.gie.endpoints import AGSI_COUNTRIES, ALSI_COUNTRIES
from gridflow.schemas.gie import GasStorage, LNGTerminal
from gridflow.silver.gie.agsi import (
    AboutListingTransformer,
    AboutSummaryTransformer,
    GasStorageTransformer,
    NewsItemTransformer,
    NewsTransformer,
    UnavailabilityTransformer,
)
from gridflow.silver.gie.alsi import LNGTerminalTransformer
from gridflow.silver.registry import list_transformers

FIXTURES = Path(__file__).parent.parent / "fixtures" / "gie"


# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------


class TestGieEndpoints:
    def test_agsi_countries_not_empty(self):
        assert len(AGSI_COUNTRIES) > 0

    def test_alsi_countries_not_empty(self):
        assert len(ALSI_COUNTRIES) > 0

    def test_gb_in_agsi_countries(self):
        assert "GB" in AGSI_COUNTRIES

    def test_gb_in_alsi_countries(self):
        assert "GB" in ALSI_COUNTRIES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_transformer(cls):
    t = cls.__new__(cls)
    ds = cls.dataset
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path(f"/tmp/test/bronze/{cls.source}/{ds}")
    t.silver_dir = Path(f"/tmp/test/silver/{cls.source}/{ds}")
    return t


def _load_fixture_records(filename: str) -> list[dict]:
    payload = json.loads((FIXTURES / filename).read_text())
    return payload.get("data", [])


# ---------------------------------------------------------------------------
# GasStorageTransformer (AGSI)
# ---------------------------------------------------------------------------


class TestGasStorageTransformer:
    def setup_method(self):
        self.t = _make_transformer(GasStorageTransformer)

    def _make_raw_df(self) -> pl.DataFrame:
        records = _load_fixture_records("agsi_gb_response.json")
        return pl.DataFrame(records)

    def test_transform_basic(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "gas_day" in result.columns
        assert "gas_in_storage_gwh" in result.columns

    def test_gas_day_dtype(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["gas_day"].dtype == pl.Date

    def test_country_code_populated(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert "country_code" in result.columns
        assert result["country_code"][0] == "GB"

    def test_storage_pct_full(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw).sort("gas_day", descending=True)
        assert "storage_pct_full" in result.columns
        assert abs(result["storage_pct_full"][0] - 81.4) < 0.1

    def test_numeric_columns_are_float(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["gas_in_storage_gwh"].dtype == pl.Float64
        assert result["withdrawal_gwh"].dtype == pl.Float64

    def test_data_provider(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "gie_agsi"

    def test_three_records(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert len(result) == 3

    def test_dedup(self):
        raw = self._make_raw_df()
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 3  # deduplicated back to 3

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_column_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_sorted_by_gas_day(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        days = result["gas_day"].to_list()
        assert days == sorted(days)

    def test_storage_reports_transformer_registered(self):
        assert ("gie_agsi", "storage") in list_transformers("gie_agsi")
        assert ("gie_agsi", "storage_reports") in list_transformers("gie_agsi")

    def test_transform_preserves_live_storage_fields(self):
        raw = pl.DataFrame(
            [
                {
                    "name": "EU",
                    "code": "EU",
                    "url": "https://agsi.gie.eu/api?type=EU",
                    "updatedAt": "2026-05-01T10:30:00Z",
                    "gasDayStart": "2026-05-01T06:00:00Z",
                    "gasDayEnd": "2026-05-02T06:00:00Z",
                    "gasInStorage": "754.1",
                    "consumption": "501.2",
                    "consumptionFull": "34.5",
                    "injection": "12.3",
                    "withdrawal": "4.5",
                    "netWithdrawal": "-7.8",
                    "workingGasVolume": "1000.0",
                    "injectionCapacity": "55.1",
                    "withdrawalCapacity": "66.2",
                    "contractedCapacity": "77.3",
                    "availableCapacity": "88.4",
                    "coveredCapacity": "99.5",
                    "full": "75.4",
                    "trend": "1",
                    "status": "confirmed",
                    "info": {"service": "maintenance"},
                    "__request_type": "EU",
                }
            ]
        )

        result = self.t.transform(raw)

        assert len(result) == 1
        assert result["entity_level"].to_list() == ["aggregate_type"]
        assert result["entity_code"].to_list() == ["EU"]
        assert result["updated_at"].dtype == pl.Datetime("us", "UTC")
        assert result["gas_day_end"].dtype == pl.Datetime("us", "UTC")
        assert result["net_withdrawal_gwh"].to_list() == [-7.8]
        assert result["available_capacity_gwh_per_day"].to_list() == [88.4]
        assert result["status"].to_list() == ["confirmed"]
        assert "maintenance" in result["info"].to_list()[0]

    def test_transform_preserves_same_day_query_scopes(self):
        raw = pl.DataFrame(
            [
                {
                    "name": "EU",
                    "code": "EU",
                    "gasDayStart": "2026-05-01T06:00:00Z",
                    "gasInStorage": "100",
                    "__request_type": "EU",
                },
                {
                    "name": "Germany",
                    "code": "DE",
                    "gasDayStart": "2026-05-01T06:00:00Z",
                    "gasInStorage": "90",
                    "__request_country": "DE",
                },
                {
                    "name": "Alpha Storage",
                    "code": "21X-DEMO-ALPHA",
                    "gasDayStart": "2026-05-01T06:00:00Z",
                    "gasInStorage": "80",
                    "__request_country": "DE",
                    "__request_company": "21X-DEMO-ALPHA",
                },
                {
                    "name": "Alpha One",
                    "code": "21W-DEMO-ALPHA-1",
                    "gasDayStart": "2026-05-01T06:00:00Z",
                    "gasInStorage": "70",
                    "__request_country": "DE",
                    "__request_company": "21X-DEMO-ALPHA",
                    "__request_facility": "21W-DEMO-ALPHA-1",
                },
            ]
        )

        result = self.t.transform(raw)

        assert len(result) == 4
        assert set(result["entity_level"].to_list()) == {
            "aggregate_type",
            "country",
            "company",
            "facility",
        }


class TestAgsiReferenceTransformers:
    def test_about_listing_flattens_companies_and_facilities(self):
        transformer = _make_transformer(AboutListingTransformer)
        payload = json.loads((FIXTURES / "agsi_listing_response.json").read_text())
        raw = pl.DataFrame(transformer._records_from_payload(payload))

        result = transformer.transform(raw)

        assert len(result) == 7
        assert set(result["entity_level"].to_list()) == {"company", "facility"}
        assert "company_code" in result.columns
        assert result["data_provider"].unique().to_list() == ["gie_agsi"]

    def test_about_summary_transform_basic(self):
        transformer = _make_transformer(AboutSummaryTransformer)
        raw = pl.DataFrame(
            [
                {
                    "platform": "AGSI",
                    "dataset": "storage",
                    "updatedAt": "2026-05-01T10:00:00Z",
                    "totalCompanies": "3",
                }
            ]
        )

        result = transformer.transform(raw)

        assert len(result) == 1
        assert result["updated_at"].dtype == pl.Datetime("us", "UTC")
        assert result["total_companies"].to_list() == [3.0]

    def test_news_transform_preserves_nested_entities(self):
        transformer = _make_transformer(NewsTransformer)
        raw = pl.DataFrame(
            [
                {
                    "url": "demo-news",
                    "title": "Maintenance",
                    "summary": "Demo announcement",
                    "start_at": "2026-05-01T00:00:00Z",
                    "entities": [{"code": "DE"}],
                }
            ]
        )

        result = transformer.transform(raw)

        assert len(result) == 1
        assert result["start_at"].dtype == pl.Datetime("us", "UTC")
        assert "DE" in result["entities"].to_list()[0]

    def test_news_item_transform_accepts_detail_payload(self):
        transformer = _make_transformer(NewsItemTransformer)
        result = transformer.transform(
            pl.DataFrame(
                [
                    {
                        "turl": "demo-news",
                        "title": "Maintenance detail",
                        "details": "Detailed text",
                    }
                ]
            )
        )

        assert len(result) == 1
        assert result["turl"].to_list() == ["demo-news"]

    def test_unavailability_transform_basic(self):
        transformer = _make_transformer(UnavailabilityTransformer)
        result = transformer.transform(
            pl.DataFrame(
                [
                    {
                        "id": "unav-1",
                        "status": "planned",
                        "eventStart": "2026-05-01T06:00:00Z",
                        "eventEnd": "2026-05-02T06:00:00Z",
                        "unavailableCapacity": "12.5",
                    }
                ]
            )
        )

        assert len(result) == 1
        assert result["event_start"].dtype == pl.Datetime("us", "UTC")
        assert result["unavailable_capacity"].to_list() == [12.5]


# ---------------------------------------------------------------------------
# LNGTerminalTransformer (ALSI)
# ---------------------------------------------------------------------------


class TestLNGTerminalTransformer:
    def setup_method(self):
        self.t = _make_transformer(LNGTerminalTransformer)

    def _make_raw_df(self) -> pl.DataFrame:
        records = _load_fixture_records("alsi_gb_response.json")
        return pl.DataFrame(records)

    def test_transform_basic(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "gas_day" in result.columns
        assert "lng_in_storage_gwh" in result.columns

    def test_gas_day_dtype(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["gas_day"].dtype == pl.Date

    def test_country_code_populated(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["country_code"][0] == "GB"

    def test_data_provider(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert result["data_provider"][0] == "gie_alsi"

    def test_two_records(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        assert len(result) == 2

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_sorted_by_gas_day(self):
        raw = self._make_raw_df()
        result = self.t.transform(raw)
        days = result["gas_day"].to_list()
        assert days == sorted(days)

    def test_dtrs_carried_raw_not_relabelled_as_percentage(self):
        """Regression (VTA-GIE-DTRS-01): vendor ``dtrs`` is NOT a percentage.

        Live ALSI returns ``dtrs`` ~724-2132 (>100), so it must be carried raw under
        a neutral name and must never land in a ``*pct*``/``*full*`` column. The
        honest %-full is derived as ``lng_in_storage_gwh / dtmi_gwh * 100`` (0-100).
        Pre-fix the transformer mapped ``dtrs`` -> ``dtrs_pct_full``, surfacing 724.1
        under a percent-named column; this asserts that no longer happens.
        """
        result = self.t.transform(self._make_raw_df())

        # Honesty property: no column whose name implies a percentage carries >100.
        pct_like = [c for c in result.columns if "pct" in c or "full" in c]
        for col in pct_like:
            vals = [v for v in result[col].to_list() if v is not None]
            assert all(v <= 100.0 for v in vals), f"{col} holds a value >100: {vals}"

        # dtrs is carried raw under its own neutral name (not a percentage).
        assert "dtrs" in result.columns
        assert "dtrs_pct_full" not in result.columns
        row = result.filter(pl.col("gas_day") == date(2024, 1, 15))
        assert abs(row["dtrs"][0] - 724.1) < 1e-6

        # dtmi.{lng,gwh} captured as unconfirmed-unit columns.
        assert "dtmi_lng" in result.columns
        assert "dtmi_gwh" in result.columns
        assert abs(row["dtmi_gwh"][0] - 2132.3) < 1e-6
        assert abs(row["dtmi_lng"][0] - 1.5) < 1e-6

        # Honest derived %-full = lng_in_storage_gwh / dtmi_gwh * 100, clamped 0-100.
        assert "lng_pct_full" in result.columns
        expected = row["lng_in_storage_gwh"][0] / row["dtmi_gwh"][0] * 100
        assert abs(row["lng_pct_full"][0] - expected) < 1e-6
        all_pct = [v for v in result["lng_pct_full"].to_list() if v is not None]
        assert all(0.0 <= v <= 100.0 for v in all_pct)


# ---------------------------------------------------------------------------
# GasStorage schema
# ---------------------------------------------------------------------------


class TestGasStorageSchema:
    _DATE = date(2024, 1, 15)

    def test_valid_record(self):
        r = GasStorage(
            gas_day=self._DATE,
            country_code="GB",
            gas_in_storage_gwh=28500.5,
            storage_pct_full=81.4,
        )
        assert r.data_provider == "gie_agsi"
        assert r.country_code == "GB"

    def test_optional_fields_default_none(self):
        r = GasStorage(gas_day=self._DATE, country_code="GB")
        assert r.gas_in_storage_gwh is None
        assert r.withdrawal_gwh is None
        assert r.storage_pct_full is None

    def test_pct_full_clamped(self):
        r = GasStorage(gas_day=self._DATE, country_code="GB", storage_pct_full=110.0)
        assert r.storage_pct_full == 100.0

    def test_negative_pct_clamped(self):
        r = GasStorage(gas_day=self._DATE, country_code="GB", storage_pct_full=-5.0)
        assert r.storage_pct_full == 0.0


# ---------------------------------------------------------------------------
# LNGTerminal schema
# ---------------------------------------------------------------------------


class TestLNGTerminalSchema:
    _DATE = date(2024, 1, 15)

    def test_valid_record(self):
        r = LNGTerminal(
            gas_day=self._DATE,
            country_code="GB",
            lng_in_storage_gwh=5200.4,
            lng_pct_full=74.3,
            dtrs=724.1,
            dtmi_gwh=2132.3,
        )
        assert r.data_provider == "gie_alsi"
        assert r.country_code == "GB"
        # dtrs is carried raw (NOT a percentage) — never clamped to 0-100.
        assert r.dtrs == 724.1

    def test_optional_fields_default_none(self):
        r = LNGTerminal(gas_day=self._DATE, country_code="GB")
        assert r.lng_in_storage_gwh is None
        assert r.lng_pct_full is None
        assert r.dtrs is None
        assert r.dtmi_lng is None
        assert r.dtmi_gwh is None

    def test_pct_full_clamped_high(self):
        r = LNGTerminal(gas_day=self._DATE, country_code="GB", lng_pct_full=105.0)
        assert r.lng_pct_full == 100.0
