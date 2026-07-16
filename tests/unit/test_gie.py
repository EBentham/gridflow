"""Unit tests for GIE AGSI+/ALSI connector, schemas, and silver transformers."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from gridflow.connectors.gie.endpoints import AGSI_COUNTRIES, ALSI_COUNTRIES
from gridflow.schemas.gie import GasStorage, LNGTerminal
from gridflow.silver.base import BaseSilverTransformer, gas_day_event_time_expr
from gridflow.silver.gie.agsi import (
    AboutListingTransformer,
    AboutSummaryTransformer,
    GasStorageTransformer,
    NewsItemTransformer,
    NewsTransformer,
    StorageReportsTransformer,
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


def _write_bronze_payload(
    transformer: BaseSilverTransformer,
    target_date: date,
    payload: dict,
) -> None:
    bronze_dir = (
        transformer.bronze_dir
        / str(target_date.year)
        / f"{target_date.month:02d}"
        / f"{target_date.day:02d}"
    )
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload), encoding="utf-8")


def _seed_fixture(
    transformer: BaseSilverTransformer,
    target_date: date,
    filename: str,
) -> None:
    payload = json.loads((FIXTURES / filename).read_text(encoding="utf-8"))
    _write_bronze_payload(transformer, target_date, payload)


def _read_single_silver(transformer: BaseSilverTransformer) -> pl.DataFrame:
    paths = list(transformer.silver_dir.rglob("*.parquet"))
    assert len(paths) == 1
    return pl.read_parquet(paths[0])


def _assert_transformer_isolated(
    transformer: BaseSilverTransformer,
    tmp_data_dir: Path,
) -> None:
    root = tmp_data_dir.resolve()
    assert transformer.bronze_dir.resolve().is_relative_to(root)
    assert transformer.silver_dir.resolve().is_relative_to(root)


@pytest.mark.parametrize("gas_day", [date(2024, 1, 15), date(2024, 7, 15)])
def test_gas_day_event_time_expr_is_fixed_0600_utc(gas_day: date) -> None:
    result = pl.DataFrame({"gas_day": [gas_day]}).with_columns(gas_day_event_time_expr())

    assert result["event_time"].dtype == pl.Datetime("us", "UTC")
    assert result["event_time"].to_list() == [
        datetime(gas_day.year, gas_day.month, gas_day.day, 6, tzinfo=UTC)
    ]


@pytest.mark.parametrize(
    ("transformer_cls", "fixture_name", "target_date", "expected_rows"),
    [
        (GasStorageTransformer, "agsi_gb_response.json", date(2024, 1, 20), 3),
        (
            StorageReportsTransformer,
            "agsi_storage_reports_response.json",
            date(2026, 5, 2),
            4,
        ),
    ],
)
def test_gie_gas_day_run_persists_row_event_time_contract(
    tmp_data_dir: Path,
    transformer_cls: type[BaseSilverTransformer],
    fixture_name: str,
    target_date: date,
    expected_rows: int,
) -> None:
    transformer = transformer_cls(tmp_data_dir)
    _assert_transformer_isolated(transformer, tmp_data_dir)
    _seed_fixture(transformer, target_date, fixture_name)

    assert transformer.run(target_date, run_id="gie-gas-day-test") == expected_rows
    persisted = _read_single_silver(transformer)

    assert persisted["gas_day"].dtype == pl.Date
    assert persisted["gas_day"].null_count() == 0
    assert persisted["event_time"].dtype == pl.Datetime("us", "UTC")
    # AGSI storage's read path is unchanged by P0.8 (no per-record date
    # filter) — a seeded partition can legitimately carry rows for gas days
    # other than target_date.
    assert any(gas_day != target_date for gas_day in persisted["gas_day"].to_list())
    assert persisted["event_time"].to_list() == [
        datetime(gas_day.year, gas_day.month, gas_day.day, 6, tzinfo=UTC)
        for gas_day in persisted["gas_day"].to_list()
    ]
    assert set(persisted["dataset_version"].to_list()) == {"2.0.0"}


def test_alsi_lng_run_persists_only_target_gas_day(tmp_data_dir: Path) -> None:
    """P0.8: ALSI's silver-side gas-day filter — split out of the shared
    parametrized case above because, unlike AGSI storage, ALSI now keeps
    ONLY the rows matching ``target_date`` (fallback+filter, deviation
    note 1). ``alsi_gb_response.json`` carries two gas days (2024-01-15 and
    2024-01-14); seeding it at partition 2024-01-15 and running for that
    date keeps only the 2024-01-15 row.
    """
    transformer = LNGTerminalTransformer(tmp_data_dir)
    _assert_transformer_isolated(transformer, tmp_data_dir)
    target_date = date(2024, 1, 15)
    _seed_fixture(transformer, target_date, "alsi_gb_response.json")

    assert transformer.run(target_date, run_id="gie-gas-day-test") == 1
    persisted = _read_single_silver(transformer)

    assert persisted["gas_day"].dtype == pl.Date
    assert persisted["gas_day"].to_list() == [target_date]
    assert persisted["event_time"].to_list() == [datetime(2024, 1, 15, 6, tzinfo=UTC)]
    assert set(persisted["dataset_version"].to_list()) == {"2.0.0"}


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

    def test_run_excludes_malformed_gas_day(
        self,
        tmp_data_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        target_date = date(2024, 1, 20)
        transformer = GasStorageTransformer(tmp_data_dir)
        _assert_transformer_isolated(transformer, tmp_data_dir)
        _write_bronze_payload(
            transformer,
            target_date,
            {
                "data": [
                    {
                        "gasDayStart": "2024-01-15",
                        "code": "GB",
                        "countryCode": "GB",
                        "gasInStorage": "100",
                    },
                    {
                        "gasDayStart": "not-a-date",
                        "code": "FR",
                        "countryCode": "FR",
                        "gasInStorage": "200",
                    },
                ]
            },
        )

        assert transformer.run(target_date, run_id="agsi-malformed-test") == 1
        persisted = _read_single_silver(transformer)
        assert persisted["gas_day"].to_list() == [date(2024, 1, 15)]
        assert persisted["event_time"].to_list() == [datetime(2024, 1, 15, 6, tzinfo=UTC)]
        assert "Missing required gas day in GIE AGSI storage row" in caplog.text


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

    def test_about_listing_run_retains_target_date_midnight(
        self,
        tmp_data_dir: Path,
    ) -> None:
        target_date = date(2024, 1, 20)
        transformer = AboutListingTransformer(tmp_data_dir)
        _assert_transformer_isolated(transformer, tmp_data_dir)
        _seed_fixture(transformer, target_date, "agsi_listing_response.json")

        assert transformer.run(target_date, run_id="agsi-reference-test") == 7
        persisted = _read_single_silver(transformer)

        assert "gas_day" not in persisted.columns
        assert set(persisted["event_time"].to_list()) == {datetime(2024, 1, 20, tzinfo=UTC)}
        assert set(persisted["dataset_version"].to_list()) == {"1.0.0"}


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

    def test_run_drops_invalid_gas_days_with_bounded_raw_context(
        self,
        tmp_data_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # P0.8: the valid record's gasDayStart must equal the seeded target
        # date, since the new silver-side gas-day filter (read_bronze) now
        # drops any record whose gasDayStart parses successfully to a
        # DIFFERENT date than target_date — the None/"not-a-date" records
        # are unparseable, so the filter's fail-open hedge keeps them and
        # the transform's own invalid-date drop (asserted below) still
        # removes them.
        target_date = date(2024, 1, 20)
        transformer = LNGTerminalTransformer(tmp_data_dir)
        _assert_transformer_isolated(transformer, tmp_data_dir)
        _write_bronze_payload(
            transformer,
            target_date,
            {
                "data": [
                    {"gasDayStart": "2024-01-20", "code": "GB", "lngInventory": "100"},
                    {"gasDayStart": None, "code": "DE", "lngInventory": "200"},
                    {"gasDayStart": "not-a-date", "code": "FR", "lngInventory": "300"},
                ]
            },
        )

        assert transformer.run(target_date, run_id="alsi-malformed-test") == 1
        persisted = _read_single_silver(transformer)
        assert persisted["gas_day"].to_list() == [date(2024, 1, 20)]
        assert persisted["event_time"].to_list() == [datetime(2024, 1, 20, 6, tzinfo=UTC)]
        assert "Dropping 2 GIE ALSI row(s)" in caplog.text
        assert "not-a-date" in caplog.text
        assert "None" in caplog.text
        assert "DE" in caplog.text
        assert "FR" in caplog.text

    def test_run_with_all_invalid_gas_days_writes_nothing(
        self,
        tmp_data_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        target_date = date(2024, 1, 20)
        transformer = LNGTerminalTransformer(tmp_data_dir)
        _assert_transformer_isolated(transformer, tmp_data_dir)
        _write_bronze_payload(
            transformer,
            target_date,
            {
                "data": [
                    {"gasDayStart": None, "code": "DE"},
                    {"gasDayStart": "invalid-day", "code": "FR"},
                ]
            },
        )

        assert transformer.run(target_date, run_id="alsi-all-invalid-test") == 0
        assert "Dropping 2 GIE ALSI row(s)" in caplog.text
        assert "invalid-day" in caplog.text
        assert not list(transformer.silver_dir.rglob("*.parquet"))

    def test_run_exact_partition_filters_to_target_gas_day(self, tmp_data_dir: Path) -> None:
        """P0.8: an exact bronze partition holding 3 gas days -> run(D)
        persists only day-D rows."""
        target_date = date(2024, 1, 20)
        transformer = LNGTerminalTransformer(tmp_data_dir)
        _assert_transformer_isolated(transformer, tmp_data_dir)
        _write_bronze_payload(
            transformer,
            target_date,
            {
                "data": [
                    {"gasDayStart": "2024-01-18", "code": "GB", "lngInventory": "100"},
                    {"gasDayStart": "2024-01-19", "code": "GB", "lngInventory": "200"},
                    {"gasDayStart": "2024-01-20", "code": "GB", "lngInventory": "300"},
                ]
            },
        )

        assert transformer.run(target_date, run_id="alsi-p08-exact") == 1
        persisted = _read_single_silver(transformer)
        assert persisted["gas_day"].to_list() == [target_date]

    def test_run_covering_fallback_partition_filters_to_target_gas_day(
        self, tmp_data_dir: Path
    ) -> None:
        """P0.8: a window-start bronze partition at D-1 (resolved via the
        covering fallback — ALSI's connector is unchunked, deviation note 1)
        holding both D-1 and D gas days -> run(D) keeps only D's rows (the
        rolling-window walk the plan's deviation note describes)."""
        window_start = date(2024, 1, 19)
        target_date = date(2024, 1, 20)
        transformer = LNGTerminalTransformer(tmp_data_dir)
        _assert_transformer_isolated(transformer, tmp_data_dir)
        _write_bronze_payload(
            transformer,
            window_start,
            {
                "data": [
                    {"gasDayStart": "2024-01-19", "code": "GB", "lngInventory": "100"},
                    {"gasDayStart": "2024-01-20", "code": "GB", "lngInventory": "200"},
                ]
            },
        )

        assert transformer.run(target_date, run_id="alsi-p08-fallback") == 1
        persisted = _read_single_silver(transformer)
        assert persisted["gas_day"].to_list() == [target_date]


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
