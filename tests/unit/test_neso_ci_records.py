"""The thirteen carbon-intensity and capacity-market frozen records (v0.22-K-CI).

Every test writes a recorded fixture capture (a slice of the 2026-10-08 swept
bronze, ``tests/fixtures/neso_data_portal/ci/``) into a short data root and
runs the transformer the **real package registry** generates for the family,
so a record that does not match its vendor body fails here, not at activation.
On master none of these families has a record, so ``get_transformer`` raises
for every key below.

``git`` and ``core.autocrlf`` normalise a committed fixture's line endings, so
:func:`ci_body` re-terminates every record with the bronze original's
convention and no test asserts on a fixture's raw bytes.
"""

from __future__ import annotations

import csv
import io
import os
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import Held
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    read_failure,
)
from gridflow.silver.registry import get_transformer
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from collections.abc import Iterator

SOURCE = "neso_data_portal"
CI_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "ci"
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 9, 0, tzinfo=UTC)

# package slug, package id, resource id, resource name, vendor filename, ckan_last_modified:
# the real sidecar identities of the thirteen captures of record (2026-10-08).
_CAP = "0b3ab475-5622-4465-8825-9d35d03bb28b"
SIDECARS: dict[str, tuple[str, str, str, str, str, str]] = {
    "national_ci_forecast": (
        "national-carbon-intensity-forecast",
        "f406810a-1a36-48d2-b542-1dfb1348096e",
        "0e5fde43-2de7-4fb4-833d-c7bca3b658b0",
        "National Carbon Intensity Forecast",
        "gb_carbon_intensity.csv",
        "2026-10-08T11:11:23.285254",
    ),
    "regional_ci_forecast": (
        "regional-carbon-intensity-forecast",
        "bf3d723b-b4ae-4ec7-9fc9-e584c651a9da",
        "c16b0e19-c02a-44a8-ba05-4db2c0545a2a",
        "Regional Carbon Intensity Forecast",
        "regional_carbon_intensity.csv",
        "2026-10-08T11:11:10.800726",
    ),
    "country_ci_forecast": (
        "country-carbon-intensity-forecast",
        "d8084aa3-8c9e-425c-bf0d-51e0441fc241",
        "e032e7aa-dea5-4695-80d6-19739142021d",
        "Country Carbon Intensity Forecast",
        "country_carbon_intensity.csv",
        "2026-10-08T09:10:31.323607",
    ),
    "ci_balancing_actions": (
        "carbon-intensity-of-balancing-actions",
        "5d3a7f30-020b-4bf2-9f56-1a7522ece994",
        "0b7edf10-4328-4b85-a0af-e6daa6bb8428",
        "Carbon Intensity of Balancing Actions",
        "balancing_actions_carbon_intensity_ckan.csv",
        "2026-10-07T11:38:19.659640",
    ),
    "portal_known_issues": (
        "data-portal-planned-changes-known-issues",
        "a10cb561-1be2-4567-880d-5bbac1ee9f4f",
        "23021dfa-a3b8-4f62-93bc-b60186c53fa4",
        "Planned Changes and Issues Log",
        "23021dfa-a3b8-4f62-93bc-b60186c53fa4_08.10.2026.csv",
        "2026-10-08T09:43:34.968862",
    ),
    "capacity_market_auction_cost": (
        "capacity-market-register",
        _CAP,
        "b1b58919-eaa4-40ea-80df-3d3e526f8223",
        "Capacity Market Auction Capacity and Cost",
        "capacity-market-auction-capacity-and-cost.csv",
        "2026-09-09T11:54:02.061035",
    ),
    "capacity_market_auction_static": (
        "capacity-market-register",
        _CAP,
        "784fa703-1460-42cd-a8bb-fba4dcabac8d",
        "Capacity Market Auction Static Data",
        "capacity-market-auction-static-data.csv",
        "2026-09-09T11:51:45.161649",
    ),
    "capacity_market_de_rating_factors": (
        "capacity-market-register",
        _CAP,
        "d94ff98d-39ba-40d6-9672-7433de0631ac",
        "Capacity Market De-Rating Factors",
        "capacity-market-de-rating-factors.csv",
        "2026-09-09T11:52:54.749384",
    ),
    "capacity_market_unit_cmu": (
        "capacity-market-register",
        _CAP,
        "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6",
        "Capacity Market Unit (CMU)",
        "cmu_20261007.csv",
        "2026-10-07T13:21:26.523270",
    ),
    "capacity_market_unit_cmu_history": (
        "capacity-market-register",
        _CAP,
        "cd034839-73e7-4c37-b4e5-6ebea25627d8",
        "Capacity Market Unit (CMU) History",
        "cmu_changes_20261007.csv",
        "2026-10-07T13:25:45.859603",
    ),
    "capacity_market_component_history": (
        "capacity-market-register",
        _CAP,
        "62bca39c-d4f0-4cd6-b2ff-8ef42dca7f23",
        "Component History",
        "component_changes_20261007.csv",
        "2026-10-07T14:26:38.881535",
    ),
    "capacity_market_component_history_pre": (
        "capacity-market-register",
        _CAP,
        "015453e0-c73c-416a-901d-623f914c8e70",
        "Component History - Pre Aggregation",
        "component_changes_20260715.csv",
        "2026-07-15T10:09:26.879666",
    ),
    "capacity_market_components": (
        "capacity-market-register",
        _CAP,
        "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
        "Components",
        "component_20261007.csv",
        "2026-10-07T13:03:44.960185",
    ),
}
CI_KEYS = tuple(SIDECARS)

# Bronze originals that end every record in CRLF (the rest are LF).
CRLF_FAMILIES = frozenset(
    {
        "portal_known_issues",
        "capacity_market_auction_cost",
        "capacity_market_auction_static",
        "capacity_market_de_rating_factors",
        "capacity_market_unit_cmu",
        "capacity_market_component_history_pre",
        "capacity_market_components",
    }
)

NATIONAL_Q = (
    "TODO: no issue column; which forecast issue a past target's value reflects, and whether "
    "values are revised after the period, is undocumented"
)
REGIONAL_Q = (
    NATIONAL_Q
    + "; whether datetime marks period start or end; meaning of negative intensities (min -13.0)"
)
BALANCING_Q = "TODO: whether DATETIME marks period start or end"
HELD: dict[str, str] = {
    "national_ci_forecast": NATIONAL_Q,
    "country_ci_forecast": NATIONAL_Q,
    "regional_ci_forecast": REGIONAL_Q,
    "ci_balancing_actions": BALANCING_Q,
}

TEMPORAL: dict[str, str] = {
    "national_ci_forecast": "utc_instant",
    "country_ci_forecast": "utc_instant",
    "capacity_market_unit_cmu_history": "date_sp1",
    "capacity_market_component_history": "date_sp1",
    "capacity_market_component_history_pre": "date_sp1",
}

ENTITY_KEYS: dict[str, tuple[str, ...]] = {
    "national_ci_forecast": ("datetime",),
    "country_ci_forecast": ("datetime",),
    "regional_ci_forecast": ("datetime",),
    "ci_balancing_actions": ("datetime",),
    "portal_known_issues": ("po",),
    "capacity_market_auction_cost": ("auction_name", "delivery_year"),
    "capacity_market_auction_static": ("auction_name",),
    "capacity_market_de_rating_factors": ("auction_name", "generating_technology_class"),
    "capacity_market_unit_cmu": ("auction_name", "application_id"),
    "capacity_market_components": ("auction_name", "application_id", "component_id"),
}

POLARS_DTYPES: dict[str, Any] = {
    "string": pl.Utf8,
    "int64": pl.Int64,
    "float64": pl.Float64,
    "date": pl.Date,
    "datetime": pl.Datetime("us", "UTC"),
}


def _short_base() -> str:
    """The drive root on Windows (a 38-character family key plus the engine's run-id names
    pass MAX_PATH under the long per-user temp directory); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data() -> Iterator[Path]:
    """A data root with a short path (ADR-036: the engine's names pass MAX_PATH below tmp_path)."""
    with tempfile.TemporaryDirectory(
        prefix="ci", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def _records(raw: bytes) -> int:
    return len(list(csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""))))


def ci_body(key: str) -> bytes:
    """The fixture ``<key>.csv`` terminated by the bronze original's line convention."""
    raw = (CI_DIR / f"{key}.csv").read_bytes().replace(b"\r\n", b"\n")
    if key in CRLF_FAMILIES and _records(raw) == raw.count(b"\n"):
        return raw.replace(b"\n", b"\r\n")
    return raw


def _rows(body: bytes) -> int:
    return _records(body) - 1


def _capture(data: Path, key: str, body: bytes, *, written_at: datetime = WRITTEN) -> str:
    slug, package_id, resource_id, name, filename, modified = SIDECARS[key]
    path, _sidecar = write_capture(
        data,
        key,
        body=body,
        written_at=written_at,
        partition=DAY,
        package_slug=slug,
        package_id=package_id,
        resource_id=resource_id,
        resource_name=name,
        resource_filename=filename,
        ckan_last_modified=modified,
    )
    return capture_id_for(path, data)


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _record(key: str) -> Any:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _run_one(data: Path, key: str, body: bytes | None = None) -> pl.DataFrame:
    _capture(data, key, body if body is not None else ci_body(key))
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    return _silver(data, key)


def _naive_utc(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp).replace(tzinfo=UTC)


@pytest.mark.parametrize("key", CI_KEYS)
def test_every_ci_family_types_its_vendor_body(data: Path, key: str) -> None:
    """Detects a family without a generated transformer, a record whose header or
    dtypes the vendor body does not satisfy, an entity key that is not unique on the
    body, and a clock taken from anywhere but the sidecar (``published_at``), the
    capture (``timestamp_utc`` for recipe ``none``) or the declared recipe.
    """
    body = ci_body(key)
    capture_id = _capture(data, key, body)

    transformer = get_transformer(SOURCE, key, data)
    written_rows = transformer.run(DAY, run_id="r")

    assert written_rows == _rows(body)
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, key, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == _rows(body)
    assert completion["rows_excluded"] == 0

    record = _record(key)
    frame = _silver(data, key)
    expected = [name for name, _type in generic.output_columns(record)]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected if c not in ("year", "month")
    ]
    assert not frame.select(list(record.entity_key)).is_duplicated().any()
    for column in record.epochs[0].columns:
        assert frame.schema[column.name] == POLARS_DTYPES[column.dtype], column.name

    published = _naive_utc(SIDECARS[key][5])
    assert frame["published_at"].unique().to_list() == [published]
    assert frame["available_at"].unique().to_list() == [published]
    assert frame["capture_written_at"].unique().to_list() == [WRITTEN]
    recipe = record.temporal
    if recipe.kind == "none":
        assert frame["timestamp_utc"].unique().to_list() == [WRITTEN]
    elif recipe.kind == "utc_instant":
        assert frame["timestamp_utc"].to_list() == frame[recipe.column].to_list()
    else:
        expected_instants = [
            settlement_period_to_utc(value, 1) for value in frame[recipe.date_column].to_list()
        ]
        assert frame["timestamp_utc"].to_list() == expected_instants


def test_ci_record_shapes_match_the_unit_table() -> None:
    """Detects a record drifting from the unit spec: reader, one header epoch, clocks, key,
    selection, temporal recipe, or eligibility (held questions are the FACTS TODOs).
    """
    for key in CI_KEYS:
        record = _record(key)
        assert record.reader == "csv", key
        assert len(record.epochs) == 1, key
        assert record.epochs[0].issue.kind == "none", key
        assert record.vintage == "ckan_last_modified", key
        assert record.latest == "whole_capture", key
        assert record.temporal.kind == TEMPORAL.get(key, "none"), key
        if key in HELD:
            assert isinstance(record.eligibility, Held), key
            assert record.eligibility.unit == "E-SEM", key
            assert record.eligibility.question == HELD[key], key
        else:
            assert record.eligibility is None, key
        if key in ENTITY_KEYS:
            assert record.entity_key == ENTITY_KEYS[key], key
    for key in (
        "capacity_market_unit_cmu_history",
        "capacity_market_component_history",
        "capacity_market_component_history_pre",
    ):
        record = _record(key)
        assert record.temporal.date_column == "change_date", key
        assert set(record.entity_key) == {c.name for c in record.epochs[0].columns}, key


def test_component_history_pre_and_post_stay_separate_owners() -> None:
    """Detects the pre-aggregation and post-aggregation histories being merged under one
    owner: identical headers must not collapse the 2026-07-22 aggregation boundary (CI-3).
    """
    pre = _record("capacity_market_component_history_pre")
    post = _record("capacity_market_component_history")
    assert pre.epochs[0].header == post.epochs[0].header
    assert pre.siblings == () and post.siblings == ()
    families = registry_module.load_registry().families
    assert families["capacity_market_component_history_pre"][1].record is not None
    assert families["capacity_market_component_history"][1].record is not None


def _london_days(frame: pl.DataFrame) -> dict[date, int]:
    local = frame.select(
        pl.col("timestamp_utc").dt.convert_time_zone("Europe/London").dt.date().alias("day")
    )
    return dict(local.group_by("day").len().iter_rows())


@pytest.mark.parametrize("key", ["national_ci_forecast", "country_ci_forecast"])
def test_national_and_country_datetime_is_a_utc_period_start_with_dst_day_lengths(
    data: Path, key: str
) -> None:
    """Detects ``datetime`` read as London local time (a 48-slot day on both transitions),
    as naive/capture time, or its period position shifted: ``timestamp_utc`` must equal the
    parsed UTC label and the London-local transition days must hold 46 and 50 periods.
    """
    frame = _run_one(data, key)
    raw = [row[0] for row in list(csv.reader(io.StringIO(ci_body(key).decode())))[1:]]
    assert sorted(frame["timestamp_utc"].to_list()) == sorted(_naive_utc(v) for v in raw)
    assert frame["timestamp_utc"].to_list() == frame["datetime"].to_list()
    days = _london_days(frame)
    assert days[date(2024, 3, 31)] == 46
    assert days[date(2024, 10, 27)] == 50
    assert len(days) > 2  # the window spans neighbouring local days, so the counts are per day
    # the 01:00Z and 01:30Z slots of the spring-forward day exist as UTC instants
    stamps = set(frame["timestamp_utc"].to_list())
    assert datetime(2024, 3, 31, 1, 0, tzinfo=UTC) in stamps
    assert datetime(2024, 10, 27, 1, 0, tzinfo=UTC) in stamps


def test_national_and_country_datetime_not_matching_the_format_fails_the_capture(
    data: Path,
) -> None:
    """Detects a changed timestamp spelling being nulled or excluded silently: the strict
    ``%Y-%m-%dT%H:%M:%S`` cast fails the capture loudly and writes no silver.
    """
    key = "national_ci_forecast"
    body = ci_body(key)
    first = body.split(b"\n")[1].split(b",")[0]
    body = body.replace(first, first.replace(b"T", b" "), 1)
    capture_id = _capture(data, key, body)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    assert read_failure(data, key, capture_id) is not None
    assert not list((data / "silver").rglob("*.parquet"))


@pytest.mark.parametrize("key", ["regional_ci_forecast", "ci_balancing_actions"])
def test_regional_and_balancing_datetime_stays_the_raw_label_with_capture_time(
    data: Path, key: str
) -> None:
    """Detects an unevidenced period position being baked in: recipe ``none`` keeps the
    vendor ``datetime`` as text and stamps ``timestamp_utc`` with the capture time.
    """
    frame = _run_one(data, key)
    assert frame.schema["datetime"] == pl.Utf8
    assert frame["datetime"][0].count("T") == 1
    assert frame["timestamp_utc"].unique().to_list() == [WRITTEN]


def test_regional_keeps_fourteen_wide_columns_and_signed_values(data: Path) -> None:
    """Detects an unpivot, a lost/renamed region column, ``-1`` read as null, or a
    negative intensity or zero being bounded away.
    """
    frame = _run_one(data, "regional_ci_forecast")
    names = [c.name for c in _record("regional_ci_forecast").epochs[0].columns]
    assert names[0] == "datetime" and len(names) == 15
    assert "north_wales_and_merseyside" in names and "yorkshire" in names
    assert -1.0 in frame["london"].to_list()
    assert frame["west_midlands"].min() < -1.0
    assert 0.0 in frame["north_scotland"].to_list()
    assert frame["london"].null_count() > 0


def test_balancing_difference_of_minus_one_and_zero_survive(data: Path) -> None:
    """Detects ``-1`` treated as a null sentinel (it is a valid BOA - FPN difference)."""
    frame = _run_one(data, "ci_balancing_actions")
    assert -1.0 in frame["difference"].to_list()
    assert 0.0 in frame["difference"].to_list()
    assert frame["difference"].null_count() == 0
    assert (frame["difference"] - (frame["boa"] - frame["fpn"])).abs().max() < 1e-9


def test_national_zero_blank_and_extreme_values_are_preserved(data: Path) -> None:
    """Detects blanks filled, a zero actual dropped, or the 1000 gCO2/kWh forecast outliers
    clipped by an invented bound.
    """
    frame = _run_one(data, "national_ci_forecast")
    assert 0.0 in frame["actual"].to_list()
    assert frame["forecast"].null_count() > 0 and frame["actual"].null_count() > 0
    assert frame["forecast"].max() > 1000.0
    assert frame.schema["index"] == pl.Utf8


def test_country_zero_and_blank_values_are_preserved(data: Path) -> None:
    """Detects a zero read as missing or a blank filled in the country columns."""
    frame = _run_one(data, "country_ci_forecast")
    assert 0.0 in frame["scotland"].to_list() and 0.0 in frame["wales"].to_list()
    assert frame["scotland"].null_count() > 0


def test_portal_known_issues_keeps_n_a_text_and_nullable_utc_timestamps(data: Path) -> None:
    """Detects ``N/A`` nulled globally, blank timestamps failing/excluding the row, the
    item number typed float, or the three descriptive timestamps left as strings.
    """
    frame = _run_one(data, "portal_known_issues")
    assert frame.schema["po"] == pl.Int64
    assert "N/A" in frame["dataset_specific"].to_list()
    assert "N/A" in frame["file_specific"].to_list()
    for name in ("change_datetime_from", "change_datetime_to", "last_updated"):
        assert frame.schema[name] == pl.Datetime("us", "UTC"), name
        assert frame[name].null_count() > 0, name
        assert frame[name].drop_nulls().len() > 0, name
    assert frame["notes"].null_count() > 0
    assert frame.height == _rows(ci_body("portal_known_issues"))


def test_cmu_deferred_tec_date_stays_text_and_other_dates_are_day_first(data: Path) -> None:
    """Detects the ambiguous deferred-TEC date being cast (its day/month order is
    unestablished) and the other CMU dates being read month-first or as text; zero credit
    cover must survive and blank capacity stay null.
    """
    frame = _run_one(data, "capacity_market_unit_cmu")
    assert frame.schema["date_for_provision_of_deferred_tec"] == pl.Utf8
    assert "01/04/2017" in frame["date_for_provision_of_deferred_tec"].to_list()
    for name in (
        "date_of_issue_of_capacity_agreement",
        "date_for_financial_commitment_milestone",
        "date_of_termination_of_capacity_agreement",
    ):
        assert frame.schema[name] == pl.Date, name
        assert frame[name].drop_nulls().len() > 0, name
    assert 0.0 in frame["amount_of_credit_cover"].to_list()
    assert frame["amount_of_credit_cover"].null_count() > 0
    assert frame.schema["beta_value"] == pl.Utf8
    assert frame.schema["delivery_year"] == pl.Utf8


def test_auction_tables_keep_labels_and_rates_as_text(data: Path) -> None:
    """Detects the delivery-year label, period/rate strings or the de-rating multiplier
    being coerced, rescaled, or a zero factor dropped.
    """
    cost = _run_one(data, "capacity_market_auction_cost")
    assert "2016/2017" in cost["delivery_year"].to_list()
    assert cost.schema["capacity_awarded"] == pl.Float64
    static = _run_one(data, "capacity_market_auction_static")
    for name in ("base_period_for_the_agreement", "termination_fee_1_rate", "annual_penalty_cap"):
        assert static.schema[name] == pl.Utf8, name
    assert static["clearing_price_gbp_per_kw_per_year"].null_count() > 0
    rating = _run_one(data, "capacity_market_de_rating_factors")
    assert rating.schema["de_rating_factor"] == pl.Float64
    assert 0.0 in rating["de_rating_factor"].to_list()
    assert rating["de_rating_factor"].max() <= 1.0


def test_components_keep_missing_id_markers_and_zero_padded_ids_as_text(data: Path) -> None:
    """Detects ``N/A`` or ``Aggregated`` nulled, a zero-padded Component ID (``000000``)
    read as a number, or the component count typed float.
    """
    frame = _run_one(data, "capacity_market_components")
    assert "N/A" in frame["component_id"].to_list()
    assert "000000" in frame["component_id"].to_list()
    assert "Aggregated" in frame["location_and_post_code"].to_list()
    assert frame.schema["number_of_components"] == pl.Int64
    assert frame["number_of_components"].max() > 1
    assert frame.schema["component_id"] == pl.Utf8


def test_histories_keep_previous_and_latest_as_text_and_anchor_on_change_date(
    data: Path,
) -> None:
    """Detects Previous/Latest typed numeric (heterogeneous per Attribute), ``N/A`` or
    ``000000`` normalised, and ``timestamp_utc`` not anchored on the reporting date.
    """
    pre = _run_one(data, "capacity_market_component_history_pre")
    assert pre.schema["previous"] == pre.schema["latest"] == pl.Utf8
    assert "N/A" in pre["latest"].to_list()
    assert "000000" in pre["component_id"].to_list()
    assert pre.schema["change_date"] == pl.Date
    row = pre.row(0, named=True)
    assert row["timestamp_utc"] == settlement_period_to_utc(row["change_date"], 1)

    post = _run_one(data, "capacity_market_component_history")
    assert post["change_type"].is_in(["New", "Removed", "Detail"]).all()
    assert post.filter(pl.col("change_type") == "New")["previous"].null_count() > 0

    cmu = _run_one(data, "capacity_market_unit_cmu_history")
    removed = cmu.filter(pl.col("change_type") == "Removed")
    assert removed["latest"].null_count() > 0


def test_a_history_row_without_a_change_date_is_excluded_and_counted(data: Path) -> None:
    """Detects a row with no reporting date reaching silver with a guessed anchor:
    ``change_date`` is the non-nullable temporal input, so the row is excluded and counted.
    """
    key = "capacity_market_component_history"
    body = ci_body(key)
    lines = body.split(b"\n")
    cells = lines[1].rsplit(b",", 1)
    lines[1] = cells[0] + b","
    capture_id = _capture(data, key, b"\n".join(lines))
    transformer = get_transformer(SOURCE, key, data)
    assert transformer.run(DAY, run_id="r") == _rows(body) - 1
    assert transformer.last_excluded_row_count == 1
    completion = read_completion(data, key, capture_id)
    assert completion is not None and completion["rows_excluded"] == 1
