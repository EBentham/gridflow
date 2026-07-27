"""Unit tests for ENTSO-G connector, schemas, and silver transformers."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

import gridflow.silver.entsog  # noqa: F401
from gridflow.config.settings import load_settings
from gridflow.connectors.entsog.endpoints import (
    DEFAULT_POINT_DIRECTIONS,
    ENDPOINTS,
    ENTSOG_ALL_RECORDS_LIMIT,
    ENTSOG_API_PATH,
    ENTSOG_TIMEZONE,
    ENTSOG_TIMEZONE_PARAM,
    OPERATIONAL_INDICATORS,
    PHYSICAL_FLOW_INDICATOR,
    build_params,
)
from gridflow.schemas.entsog import EntsogPhysicalFlow
from gridflow.silver.entsog.datetime import filter_records_to_target_date
from gridflow.silver.entsog.physical_flows import (
    PhysicalFlowsTransformer,
    _normalise_to_gwh_day,
)
from gridflow.silver.registry import list_transformers

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsog"
START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------


class TestEntsogEndpoints:
    def test_api_path_is_operational_data(self):
        assert ENTSOG_API_PATH == "/operationalData"

    def test_timezone_is_uct(self):
        assert ENTSOG_TIMEZONE == "UCT"
        assert ENTSOG_TIMEZONE_PARAM == "timeZone"

    def test_limit_is_minus_one(self):
        assert ENTSOG_ALL_RECORDS_LIMIT == -1

    def test_physical_flow_indicator(self):
        assert PHYSICAL_FLOW_INDICATOR == "Physical Flow"

    def test_active_inventory_matches_config_and_silver_registry(self):
        configured = set(load_settings().get_source_config("entsog").datasets)
        endpoint_datasets = set(ENDPOINTS)
        transformers = {dataset for _, dataset in list_transformers("entsog")}

        assert endpoint_datasets == configured
        assert endpoint_datasets <= transformers

    def test_operational_indicator_values_are_exact_case(self):
        assert OPERATIONAL_INDICATORS["nominations"] == "Nomination"
        assert OPERATIONAL_INDICATORS["methane_content"] == "Methane Content"
        assert OPERATIONAL_INDICATORS["hydrogen_content"] == "Hydrogen Content"
        assert OPERATIONAL_INDICATORS["oxygen_content"] == "Oxygen Content"

    def test_build_params_for_operational_dataset(self):
        endpoint = ENDPOINTS["physical_flows"]
        params = build_params(endpoint, start=START, end=END)

        assert params["from"] == "2024-01-15"
        assert params["to"] == "2024-01-16"
        assert params["indicator"] == "Physical Flow"
        assert params["periodType"] == "day"
        assert params["timeZone"] == "UCT"
        assert params["limit"] == -1
        assert "pointDirection" not in params

    def test_build_params_keeps_point_direction_for_other_operational_datasets(self):
        endpoint = ENDPOINTS["nominations"]
        params = build_params(endpoint, start=START, end=END)

        assert params["pointDirection"] == ",".join(DEFAULT_POINT_DIRECTIONS)

    def test_build_params_allows_live_limit_override(self):
        endpoint = ENDPOINTS["operators"]
        params = build_params(endpoint, start=START, end=END, limit=1)

        assert "from" not in params
        assert "to" not in params
        assert params["hasData"] == 1
        assert params["limit"] == 1


# ---------------------------------------------------------------------------
# Unit normalisation helper
# ---------------------------------------------------------------------------


class TestNormaliseToGwhDay:
    def test_kwh_d_to_gwh_d(self):
        # 1e6 kWh/d = 1 GWh/d
        assert abs(_normalise_to_gwh_day(1_000_000.0, "kWh/d") - 1.0) < 1e-9

    def test_kwh_h_to_gwh_d(self):
        # 1e6 kWh/h * 24 = 24 GWh/d
        assert abs(_normalise_to_gwh_day(1_000_000.0, "kWh/h") - 24.0) < 1e-9

    def test_gwh_d_is_identity_not_mis_scaled(self):
        """Issue 05 #3: GWh/d is already the target unit — it must pass through
        unchanged, NOT be divided by 1e6.

        FAILS on pre-fix code: the else-branch multiplied any non-kWh/h unit
        by 1e-6, turning 15_000 GWh/d into 0.015.
        """
        assert abs(_normalise_to_gwh_day(15_000.0, "GWh/d") - 15_000.0) < 1e-9

    def test_mwh_d_scaled_by_thousand_not_mis_scaled(self):
        """MWh/d -> GWh/d divides by 1e3 (1000 MWh/d = 1 GWh/d), not 1e6."""
        assert abs(_normalise_to_gwh_day(1_000.0, "MWh/d") - 1.0) < 1e-9

    def test_unknown_unit_rejected_not_assumed_kwh_d(self):
        """Issue 05 #3: an unrecognised unit must be rejected, not silently
        assumed to be kWh/d.

        FAILS on pre-fix code: 'unknown' fell through to the kWh/d branch.
        """
        with pytest.raises(ValueError):
            _normalise_to_gwh_day(1_000_000.0, "unknown")

    def test_zero_value(self):
        assert _normalise_to_gwh_day(0.0, "kWh/d") == 0.0

    def test_large_value(self):
        # 15 billion kWh/d = 15,000 GWh/d
        result = _normalise_to_gwh_day(15_000_000_000.0, "kWh/d")
        assert abs(result - 15_000.0) < 0.01


# ---------------------------------------------------------------------------
# filter_records_to_target_date (R2-B / F-05)
# ---------------------------------------------------------------------------


class TestFilterRecordsToTargetDate:
    def test_record_dated_to_target_is_kept(self):
        records = [{"periodFrom": "2026-04-17T05:00:00+02:00", "pointKey": "A"}]
        result = filter_records_to_target_date(
            records, date(2026, 4, 17), ("periodFrom",), source="entsog", dataset="physical_flows"
        )
        assert result == records

    def test_record_dated_to_another_day_is_dropped(self):
        records = [{"periodFrom": "2026-04-18T05:00:00+02:00", "pointKey": "A"}]
        result = filter_records_to_target_date(
            records, date(2026, 4, 17), ("periodFrom",), source="entsog", dataset="physical_flows"
        )
        assert result == []

    def test_undated_record_kept_with_exactly_one_bounded_warning(self, caplog):
        import logging

        records = [
            {"pointKey": "A"},
            {"pointKey": "B"},
            {"periodFrom": "2026-04-17T05:00:00+02:00", "pointKey": "C"},
        ]
        with caplog.at_level(logging.WARNING):
            result = filter_records_to_target_date(
                records,
                date(2026, 4, 17),
                ("periodFrom",),
                source="entsog",
                dataset="physical_flows",
            )

        assert result == records, "undated records must be KEPT (fail-open), not dropped"
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1, "exactly one bounded WARNING per call, not one per record"
        assert "2 record(s)" in warnings[0].getMessage()
        assert "entsog" in warnings[0].getMessage()
        assert "physical_flows" in warnings[0].getMessage()
        assert "2026-04-17" in warnings[0].getMessage()


class TestReferenceDatasetReingestVintage:
    """R2-B / Sol finding 1: availability reconstruction must follow the bronze
    files the transformer ACTUALLY reads.

    ``GenericEntsogJsonTransformer`` overrides ``_bronze_files`` — a reference
    dataset deliberately reads the newest bronze file anywhere under its dir,
    since it is fetched weekly and has no per-date partition. Availability
    reconstruction on ``--reingest`` went through ``_bronze_date_dirs`` instead,
    which R2-B made exact-only for ``entsog``. The two then disagreed: the rows
    were read from the older partition but stamped ``available_at = now()``,
    silently fabricating a vintage weeks off the recorded one and (via
    ``available_at <= :as_of``) dropping those rows out of historical
    point-in-time queries.
    """

    RECORDED = datetime(2026, 7, 1, 9, 15, tzinfo=UTC)

    def _build(self, tmp_path: Path):
        from gridflow.silver.entsog.generic import GenericEntsogJsonTransformer
        from gridflow.storage.paths import PathBuilder

        class _RefStub(GenericEntsogJsonTransformer):
            dataset = "operators"
            response_key = "operators"
            reference_dataset = True
            date_window_dataset = False

        day = PathBuilder(tmp_path).bronze_date_dir("entsog", "operators", date(2026, 7, 1))
        day.mkdir(parents=True, exist_ok=True)
        (day / "raw_0001.json").write_text(json.dumps({"operators": [{"operatorKey": "X"}]}))
        (day / "raw_0001.meta.json").write_text(
            json.dumps({"written_at": self.RECORDED.isoformat()})
        )
        return _RefStub(tmp_path)

    def test_reingest_vintage_matches_the_file_actually_read(self, tmp_path: Path) -> None:
        transformer = self._build(tmp_path)
        target = date(2026, 7, 2)  # no exact partition; the 07-01 file is what gets read

        assert [p.name for p in transformer._bronze_files(target)] == ["raw_0001.json"], (
            "precondition: the reference reader still resolves the older bronze file"
        )
        assert transformer._available_at_from_bronze(target) == self.RECORDED

    def test_never_borrows_a_sibling_file_s_timestamp(self, tmp_path: Path) -> None:
        """Sol pass-2 finding 1: the fallback must not reach a file that was not read.

        Same partition, two files: the reader takes the newest (``raw_1000``),
        whose sidecar is missing — the writer persists the body first, so a
        crash between the two leaves exactly this residue. Delegating the
        fallback to the base method scans the WHOLE partition and returns
        ``raw_0900``'s 09:15 stamp, marking 10:00 rows as available at 09:15.
        That is the leakage direction: an ``as_of=09:30`` query would surface
        data that did not yet exist. Falling forward to ``now()`` is the only
        conservative answer when the file actually read cannot vouch for itself.
        """
        transformer = self._build(tmp_path)
        day = transformer.bronze_dir / "2026" / "07" / "01"
        (day / "raw_0900.json").write_text(json.dumps({"operators": [{"operatorKey": "X"}]}))
        (day / "raw_0900.meta.json").write_text(
            json.dumps({"written_at": self.RECORDED.isoformat()})
        )
        (day / "raw_1000.json").write_text(json.dumps({"operators": [{"operatorKey": "Y"}]}))
        (day / "raw_0001.json").unlink()
        (day / "raw_0001.meta.json").unlink()

        target = date(2026, 7, 1)
        assert [p.name for p in transformer._bronze_files(target)] == ["raw_1000.json"], (
            "precondition: the reader takes the newest file, the one with no sidecar"
        )

        before = datetime.now(UTC)
        result = transformer._available_at_from_bronze(target)
        after = datetime.now(UTC)
        assert before <= result <= after, (
            "must be THIS call's now(), not the borrowed 09:15 and not merely "
            "some instant after it — a 09:15:01 stamp still leaks at as_of=09:30"
        )
        assert result > self.RECORDED, (
            "unvouched read -> conservative now(), never an earlier stamp"
        )

    def test_mixed_sidecars_across_multiple_read_files(self, tmp_path: Path) -> None:
        """Sol pass-3 finding 1: a NON-reference dataset reads every file in the
        partition, so one unvouched file among several must not be silently
        skipped in favour of a sibling's older stamp.

        ``raw_0900`` is complete at 09:15; ``raw_1000`` carries rows but lost its
        sidecar. Both are read. Taking ``max`` over only the stamps that exist
        yields 09:15 and marks the 10:00 rows available before they were written.
        """
        from gridflow.silver.entsog.generic import GenericEntsogJsonTransformer
        from gridflow.storage.paths import PathBuilder

        class _NonRefStub(GenericEntsogJsonTransformer):
            dataset = "operational_data"
            response_key = "operationalData"
            reference_dataset = False
            date_window_dataset = True

        target = date(2026, 7, 1)
        day = PathBuilder(tmp_path).bronze_date_dir("entsog", "operational_data", target)
        day.mkdir(parents=True, exist_ok=True)
        (day / "raw_0900.json").write_text(json.dumps({"operationalData": [{"id": "a"}]}))
        (day / "raw_0900.meta.json").write_text(
            json.dumps({"written_at": self.RECORDED.isoformat()})
        )
        (day / "raw_1000.json").write_text(json.dumps({"operationalData": [{"id": "b"}]}))

        transformer = _NonRefStub(tmp_path)
        assert [p.name for p in transformer._bronze_files(target)] == [
            "raw_0900.json",
            "raw_1000.json",
        ], "precondition: a non-reference dataset reads BOTH files"

        before = datetime.now(UTC)
        result = transformer._available_at_from_bronze(target)
        after = datetime.now(UTC)
        assert before <= result <= after, (
            "the unvouched raw_1000 must contribute now(), so the frame's vintage "
            "cannot be raw_0900's 09:15"
        )


# ---------------------------------------------------------------------------
# Helper to build transformer instances bypassing __init__
# ---------------------------------------------------------------------------


def _make_transformer() -> PhysicalFlowsTransformer:
    t = PhysicalFlowsTransformer.__new__(PhysicalFlowsTransformer)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/entsog/physical_flows")
    t.silver_dir = Path("/tmp/test/silver/entsog/physical_flows")
    return t


def _load_fixture_df() -> pl.DataFrame:
    payload = json.loads((FIXTURES / "physical_flows_response.json").read_text())
    records = payload.get("operationalData", [])
    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# PhysicalFlowsTransformer
# ---------------------------------------------------------------------------


class TestPhysicalFlowsTransformer:
    def setup_method(self):
        self.t = _make_transformer()

    def test_transform_basic(self):
        raw = pl.DataFrame(
            [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "GOOD",
                    "directionKey": "entry",
                    "value": 1_000_000.0,
                    "unit": "kWh/d",
                },
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "MISSING",
                    "directionKey": "entry",
                    "unit": "kWh/d",
                },
            ]
        )
        result = self.t.transform(raw)
        assert not result.is_empty()
        assert "timestamp_utc" in result.columns
        assert "point_key" in result.columns
        assert "flow_gwh_per_day" in result.columns
        missing = result.filter(pl.col("point_key") == "MISSING")
        assert len(missing) == 1
        assert missing["flow_gwh_per_day"].null_count() == 1
        assert missing.filter(pl.col("flow_gwh_per_day") == 0.0).is_empty()

    def test_filters_to_physical_flow_only(self):
        """Non-Physical-Flow records (e.g. 'Other Indicator') are excluded."""
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        # Fixture has 3 Physical Flow records + 1 'Other Indicator' (NORI)
        assert len(result) == 3

    def test_timestamp_dtype(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")

    def test_timestamp_value(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw).sort("timestamp_utc", "point_key")
        expected = datetime(2024, 1, 15, 6, 0, 0, tzinfo=UTC)
        assert result["timestamp_utc"][0] == expected

    def test_kwh_d_normalised_to_gwh_d(self):
        """15_000_000_000 kWh/d becomes 15,000 GWh/d for IUK entry."""
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        iuk_entry = result.filter(
            (pl.col("point_key") == "IUK") & (pl.col("direction_key") == "entry")
        )
        assert len(iuk_entry) == 1
        assert abs(iuk_entry["flow_gwh_per_day"][0] - 15_000.0) < 0.01

    def test_point_key_preserved(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        point_keys = set(result["point_key"].to_list())
        assert "IUK" in point_keys
        assert "BBL" in point_keys

    def test_direction_key_preserved(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        assert "direction_key" in result.columns
        directions = set(result["direction_key"].to_list())
        assert "entry" in directions
        assert "exit" in directions

    def test_data_provider(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        assert all(v == "entsog" for v in result["data_provider"].to_list())

    def test_unit_column_relabelled_to_converted_unit(self):
        """F-ENTSOG-UNITLABEL: flow_gwh_per_day is normalised to GWh/day, so the
        emitted ``unit`` must read 'GWh/d', not the raw vendor 'kWh/d'. A row whose
        value is in GWh/day while its own unit column still says kWh/d is internally
        contradictory and mis-scales any consumer that trusts the unit column by 1e6.
        """
        raw = _load_fixture_df()  # fixture records all carry unit='kWh/d'
        result = self.t.transform(raw)
        assert "unit" in result.columns
        assert set(result["unit"].to_list()) == {"GWh/d"}, (
            "unit must reflect the normalised value (GWh/d), not the raw API unit"
        )

    def test_dedup(self):
        raw = _load_fixture_df()
        doubled = pl.concat([raw, raw])
        result = self.t.transform(doubled)
        assert len(result) == 3

    def test_empty_input(self):
        assert self.t.transform(pl.DataFrame()).is_empty()

    def test_missing_required_columns_returns_empty(self):
        raw = pl.DataFrame([{"foo": "bar"}])
        assert self.t.transform(raw).is_empty()

    def test_sorted_output(self):
        raw = _load_fixture_df()
        result = self.t.transform(raw)
        ts_list = result["timestamp_utc"].to_list()
        assert ts_list == sorted(ts_list)

    def test_gwh_d_input_not_mis_scaled_in_transform(self):
        """Issue 05 #3 (transform level): a GWh/d-denominated flow must reach
        silver unchanged, not divided by 1e6."""
        raw = pl.DataFrame(
            [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "IUK",
                    "directionKey": "entry",
                    "value": 15_000.0,
                    "unit": "GWh/d",
                }
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1
        assert abs(result["flow_gwh_per_day"][0] - 15_000.0) < 0.01

    def test_genuine_zero_flow_is_preserved(self):
        raw = pl.DataFrame(
            [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "ZERO",
                    "directionKey": "entry",
                    "value": 0.0,
                    "unit": "GWh/d",
                }
            ]
        )

        result = self.t.transform(raw)

        assert len(result) == 1
        assert result["flow_gwh_per_day"][0] == 0.0
        assert result["flow_gwh_per_day"].null_count() == 0

    def test_non_finite_flow_values_are_dropped_with_diagnostics(self, caplog):
        import logging

        raw = pl.DataFrame(
            [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "GOOD",
                    "directionKey": "entry",
                    "value": "1000000",
                    "unit": "kWh/d",
                },
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "NOT_A_NUMBER",
                    "directionKey": "entry",
                    "value": "NaN",
                    "unit": "kWh/d",
                },
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "INFINITE",
                    "directionKey": "entry",
                    "value": "inf",
                    "unit": "kWh/d",
                },
            ]
        )

        with caplog.at_level(logging.WARNING):
            result = self.t.transform(raw)

        assert result["point_key"].to_list() == ["GOOD"]
        assert "unparseable value" in caplog.text
        assert "NOT_A_NUMBER" in caplog.text
        assert "NaN" in caplog.text
        assert "INFINITE" in caplog.text
        assert "inf" in caplog.text

    def test_unparseable_value_without_unit_is_dropped(self, caplog):
        import logging

        raw = pl.DataFrame(
            [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "GOOD",
                    "directionKey": "entry",
                    "value": "1000000",
                },
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "UNPARSEABLE",
                    "directionKey": "entry",
                    "value": "abc",
                },
            ]
        )

        with caplog.at_level(logging.WARNING):
            result = self.t.transform(raw)

        assert result["point_key"].to_list() == ["GOOD"]
        assert result["flow_gwh_per_day"][0] == 1.0
        assert "UNPARSEABLE" in caplog.text
        assert "abc" in caplog.text

    def test_gas_day_offset_converted_to_utc(self):
        """ENGOP-04 (VT4): a gas-day periodFrom bearing a real offset (winter CET
        +01:00) must convert to the equivalent UTC instant — the CLAUDE.md
        silent-bug class for gas-day -> UTC.

        06:00 at +01:00 is 05:00 UTC. A naive parse (dropping the offset) would
        wrongly emit 06:00 UTC; a double-applied offset would emit 04:00/07:00.
        parse_entsog_datetime() does `.astimezone(UTC)`, so the expected value is
        a single correct conversion.
        """
        raw = pl.DataFrame(
            [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00+01:00",
                    "pointKey": "IUK",
                    "directionKey": "entry",
                    "value": 15_000.0,
                    "unit": "GWh/d",
                }
            ]
        )
        result = self.t.transform(raw)
        assert len(result) == 1
        assert result["timestamp_utc"].dtype == pl.Datetime("us", "UTC")
        assert result["timestamp_utc"][0] == datetime(2024, 1, 15, 5, 0, tzinfo=UTC)

    def test_unknown_unit_row_dropped_not_mis_scaled(self, caplog):
        """Issue 05 #3 (transform level): a row with an unrecognised unit must
        NOT be emitted with a silently mis-scaled value. It is dropped with a
        logged count (CLAUDE.md: never silently dropped)."""
        import logging

        raw = pl.DataFrame(
            [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "GOOD",
                    "directionKey": "entry",
                    "value": 1_000_000.0,
                    "unit": "kWh/d",
                },
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "BAD",
                    "directionKey": "entry",
                    "value": 1_000_000.0,
                    "unit": "mystery-unit",
                },
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2024-01-15T06:00:00Z",
                    "pointKey": "UNPARSEABLE",
                    "directionKey": "entry",
                    "value": "abc",
                    "unit": "kWh/d",
                },
            ],
            schema_overrides={"value": pl.String},
        )
        with caplog.at_level(logging.WARNING):
            result = self.t.transform(raw)
        point_keys = set(result["point_key"].to_list())
        # The good row survives, correctly scaled; the unknown-unit row is gone.
        assert "GOOD" in point_keys
        assert "BAD" not in point_keys, (
            "unknown-unit row must be dropped, not emitted with a mis-scaled value"
        )
        assert "UNPARSEABLE" not in point_keys
        good = result.filter(pl.col("point_key") == "GOOD")
        assert abs(good["flow_gwh_per_day"][0] - 1.0) < 1e-9
        assert "mystery-unit" in caplog.text, "unknown unit drop must be logged, not silent"
        assert "unparseable value" in caplog.text

    def test_read_bronze_filters_records_to_target_date(self, tmp_path):
        target = date(2026, 4, 17)
        bronze_path = tmp_path / "2026" / "04" / "17"
        bronze_path.mkdir(parents=True)
        payload = {
            "operationalData": [
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2026-04-17T05:00:00+02:00",
                    "pointKey": "ITP-00005",
                },
                {
                    "indicator": "Physical Flow",
                    "periodFrom": "2026-04-18T05:00:00+02:00",
                    "pointKey": "ITP-00005",
                },
            ]
        }
        (bronze_path / "raw_20260417T000000Z_abcd1234.json").write_text(json.dumps(payload))
        self.t.bronze_dir = tmp_path

        result = self.t.read_bronze(target)

        assert len(result) == 1
        assert result["periodFrom"].to_list() == ["2026-04-17T05:00:00+02:00"]


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestEntsogPhysicalFlowSchema:
    _TS = datetime(2024, 1, 15, 6, 0, 0, tzinfo=UTC)

    def test_valid_record(self):
        r = EntsogPhysicalFlow(
            timestamp_utc=self._TS,
            point_key="IUK",
            point_label="Interconnector UK",
            direction_key="entry",
            flow_gwh_per_day=15_000.0,
        )
        assert r.data_provider == "entsog"
        assert r.flow_gwh_per_day == 15_000.0

    def test_optional_fields_have_defaults(self):
        r = EntsogPhysicalFlow(timestamp_utc=self._TS, point_key="BBL")
        assert r.point_label == ""
        assert r.direction_key == ""
        # A missing vendor flow remains distinguishable from a real zero flow.
        assert r.flow_gwh_per_day is None

    def test_naive_timestamp_rejected(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            EntsogPhysicalFlow(
                timestamp_utc=datetime(2024, 1, 15, 6, 0, 0),  # naive
                point_key="IUK",
            )
