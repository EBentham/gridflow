"""The six pilot frozen records type their real vendor bodies (v0.22-E P-10).

Every test writes recorded fixture captures (byte slices of the swept bronze,
``tests/fixtures/neso_data_portal/pilot/``) into ``data`` and runs the
transformer the **real package registry** generates for the family, so a
record that does not match its vendor body fails here, not at activation.

``git`` normalises the committed fixtures' line endings (``core.autocrlf``),
so :func:`pilot_body` re-terminates every line with CRLF, as all six vendor
originals are (E7); no test asserts on a committed file's raw bytes.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import Held
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    read_failure,
    scan_completions,
)
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from collections.abc import Iterator

SOURCE = "neso_data_portal"
PILOT_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "pilot"
DAY = date(2026, 10, 8)

# The real sidecar identities of the six captures of record (S sweep).
SIDECARS: dict[str, dict[str, str]] = {
    "tec_register": {
        "package_slug": "transmission-entry-capacity-tec-register",
        "package_id": "cbd45e54-e6e2-4a38-99f1-8de6fd96d7c1",
        "resource_id": "17becbab-e3e8-473f-b303-3806f43a6a10",
        "resource_name": "TEC Register",
        "resource_filename": "tec-register-05-october-2026.csv",
        "ckan_last_modified": "2026-10-06T10:47:29.757067",
    },
    "interconnector_register": {
        "package_slug": "interconnector-register",
        "package_id": "a7cca714-9dbb-42b1-99c8-4bc7211605a8",
        "resource_id": "64f7908f-f787-4977-93e1-5342a5f1357f",
        "resource_name": "Interconnector Register",
        "resource_filename": "interconnector-register-05-october-2026.csv",
        "ckan_last_modified": "2026-10-06T10:47:52.001303",
    },
    "embedded_register": {
        "package_slug": "embedded-register",
        "package_id": "4b0e109c-aa23-45ac-86c1-0bda557d1aab",
        "resource_id": "68b6f3a1-e1bf-403b-9062-0269fc758d77",
        "resource_name": "Embedded Register",
        "resource_filename": "embedded-register-05-october-2026.csv",
        "ckan_last_modified": "2026-10-06T10:47:45.071632",
    },
    "demand_forecast_2_52w": {
        "package_slug": "long-term-2-52-weeks-ahead-national-demand-forecast",
        "package_id": "edd6190a-66d4-480d-a125-88d32bd11c91",
        "resource_id": "903302b4-b577-4228-a347-b9917568b4e1",
        "resource_name": "Long term demand forecast",
        "resource_filename": "year_ahead_weekly.csv",
        "ckan_last_modified": "2026-09-29T12:28:44.374647",
    },
    "da_demand_fc_performance": {
        "package_slug": "day-ahead-half-hourly-demand-forecast-performance",
        "package_id": "5281b494-5566-42dd-bf1a-9ce38642fabf",
        "resource_id": "08e41551-80f8-4e28-a416-ea473a695db9",
        "resource_name": "Day Ahead Half Hourly Demand Forecast Performance",
        "resource_filename": "1b-incentive.csv",
        "ckan_last_modified": "2026-10-08T09:15:10.605360",
    },
    "constraint_cost_fc_24m": {
        "package_slug": "24-months-ahead-constraint-cost-forecast",
        "package_id": "51bb8f5a-3c95-4f7f-9a72-3b36cb7f1dc0",
        "resource_id": "28b85d3f-a1cc-4bb9-80af-600f2cca266a",
        "resource_name": "24 Months Ahead Constraint Cost Forecast",
        "resource_filename": "24-months-ahead-constraint-cost-forecast_sept26.csv",
        "ckan_last_modified": "2026-09-10T09:38:53.501869",
    },
}
PILOT_KEYS = tuple(SIDECARS)

Q1 = (
    "TODO: ESI week definition (start and end day, numbering, rollover against calendar_year "
    "and financial_year) and why 2027 week 29 appears twice with CDATE_peak 2027-07-21 and "
    "2027-07-22; the NESO dictionary is silent"
)
Q2 = (
    "TODO: Datetime and Publish_Datetime end in 'Z' but the dictionary states GMT/BST; whether "
    "Datetime marks period start or end; Settlement_Period repeats SP4/SP5 on 2021-10-31 and "
    "SP2/SP3 on 2022-10-30; whether Publish_Datetime is the publication instant"
)
Q3 = (
    "TODO: currency unit of Constraint Cost; vendor metadata shows an undecodable symbol "
    "before 'm'; confirm GBP million from the NESO data dictionary"
)


@pytest.fixture
def data() -> Iterator[Path]:
    """A data root with a short path.

    The engine's output and completion names run to ~120 characters under
    ``silver/neso_data_portal/<family>/``; below pytest's long ``tmp_path`` the
    pilot families' longest keys pass Windows' 260-character MAX_PATH. The
    production root (``C:/gridflow-data``) is short.
    """
    with tempfile.TemporaryDirectory(prefix="pr", ignore_cleanup_errors=True) as root:
        yield Path(root)


def pilot_body(name: str) -> bytes:
    """The fixture ``<name>.csv`` with every line terminated by CRLF (as the vendor's)."""
    raw = (PILOT_DIR / f"{name}.csv").read_bytes()
    lines = raw.replace(b"\r\n", b"\n").split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return b"".join(line + b"\r\n" for line in lines)


def _rows(body: bytes) -> int:
    return body.count(b"\r\n") - 1


def _capture(
    data: Path,
    key: str,
    body: bytes,
    *,
    written_at: datetime | None = None,
    ckan_last_modified: str | None = None,
) -> str:
    facts = dict(SIDECARS[key])
    if ckan_last_modified is not None:
        facts["ckan_last_modified"] = ckan_last_modified
    path, _sidecar = write_capture(
        data,
        key,
        body=body,
        written_at=written_at or datetime(2026, 10, 8, 9, 0, tzinfo=UTC),
        partition=DAY,
        **facts,  # type: ignore[arg-type]
    )
    return capture_id_for(path, data)


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _record(key: str) -> Any:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _naive_utc(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp).replace(tzinfo=UTC)


@pytest.mark.parametrize("key", PILOT_KEYS)
def test_t_pr1_every_pilot_family_types_its_vendor_body(data: Path, key: str) -> None:
    """T-PR1: the record types every fixture row, excluding none.

    Detects a pilot family without a generated transformer (master: no record,
    ``get_transformer`` raises), a header the record's epoch does not match, a
    dtype a vendor value cannot take, and a clock taken from anywhere but the
    sidecar (``published_at``) or the capture (``timestamp_utc``, R-1).
    """
    body = pilot_body(key)
    written = datetime(2026, 10, 8, 9, 0, tzinfo=UTC)
    capture_id = _capture(data, key, body, written_at=written)

    transformer = get_transformer(SOURCE, key, data)
    written_rows = transformer.run(DAY, run_id="r")

    assert written_rows == _rows(body)
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, key, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == _rows(body)
    assert completion["rows_excluded"] == 0
    assert len(scan_completions(data, key).collect()) == 1

    frame = _silver(data, key)
    expected = [name for name, _type in generic.output_columns(_record(key))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected if c not in ("year", "month")
    ]
    published = _naive_utc(SIDECARS[key]["ckan_last_modified"])
    assert frame["published_at"].unique().to_list() == [published]
    assert frame["available_at"].unique().to_list() == [published]
    assert frame["timestamp_utc"].unique().to_list() == [written]
    assert frame["capture_written_at"].unique().to_list() == [written]


def _run_one(data: Path, key: str, body: bytes | None = None) -> pl.DataFrame:
    _capture(data, key, body if body is not None else pilot_body(key))
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    return _silver(data, key)


def test_t_pr2_tec_dates_are_day_first_and_both_pro_000053_rows_survive(
    data: Path,
) -> None:
    """T-PR2 (TEC): detects a month-first date read and a merge of the duplicated project."""
    frame = _run_one(data, "tec_register")
    first = frame.filter(pl.col("project_number") == "PRO-003804")
    assert first["mw_effective_from"].to_list() == [date(2034, 10, 31)]
    assert frame["mw_effective_from"].null_count() > 0
    assert frame.filter(pl.col("project_number") == "PRO-000053").height == 2


def test_t_pr2_two_52w_keeps_both_week_29_rows(data: Path) -> None:
    """T-PR2 (2-52w): detects a (calendar_year, esiwk) key collapsing 2027 week 29."""
    frame = _run_one(data, "demand_forecast_2_52w")
    week = frame.filter((pl.col("calendar_year") == 2027) & (pl.col("esiwk") == 29))
    assert sorted(week["cdate_peak"].to_list()) == [date(2027, 7, 21), date(2027, 7, 22)]


def test_t_pr2_da_perf_keeps_every_row_of_the_fold_day_as_vendor_strings(
    data: Path,
) -> None:
    """T-PR2 (DA perf): detects a (date, SP) key merging 2021-10-31's repeated SP4/SP5.

    Also detects the datetime columns being parsed as instants: their ``Z``
    labels a local clock (C-4), so they stay the vendor's strings.
    """
    frame = _run_one(data, "da_demand_fc_performance")
    fold = frame.filter(pl.col("outturn_date") == date(2021, 10, 31))
    assert fold.height == 50
    assert fold["settlement_period"].n_unique() == 48
    assert frame.schema["settlement_period"] == pl.Int64
    assert frame.schema["outturn_datetime"] == pl.Utf8
    assert frame["outturn_datetime"].str.ends_with("Z").all()
    assert "2021-04-01T00:30:00Z" in frame["outturn_datetime"].to_list()


def test_t_pr2_constraint_cost_month_labels_are_first_of_month(data: Path) -> None:
    """T-PR2 (constraint cost): detects ``Oct-26`` not reading as 2026-10-01."""
    frame = _run_one(data, "constraint_cost_fc_24m")
    row = frame.filter(pl.col("forecast_month") == date(2026, 10, 1))
    assert row["constraint_cost"].to_list() == [606.2]
    assert frame["forecast_month"].n_unique() == frame.height == 24


def test_t_pr3_an_iso_tec_date_fails_the_capture_loudly(data: Path) -> None:
    """T-PR3 (FM-6): detects an unparseable date being nulled instead of failing.

    ``MW Effective From`` is nullable (blank dates are real), so only the
    strict cast stops a format change (ISO instead of ``dd/mm/yyyy``) silently
    becoming nulls: the capture fails, its failure is recorded and no silver is
    written.
    """
    body = pilot_body("tec_register").replace(b",31/10/2034,", b",2034-10-31,", 1)
    capture_id = _capture(data, "tec_register", body)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "tec_register", data).run(DAY, run_id="r")
    assert read_failure(data, "tec_register", capture_id) is not None
    assert read_completion(data, "tec_register", capture_id) is None
    assert not list((data / "silver").rglob("*.parquet"))


def test_t_pr3_an_identical_2_52w_row_fails_with_duplicate_entity_key(data: Path) -> None:
    """T-PR3 (FM-6): detects two identical rows being merged rather than refused."""
    body = pilot_body("demand_forecast_2_52w")
    lines = body.split(b"\r\n")
    body = b"\r\n".join([*lines[:2], lines[1], *lines[2:]])
    capture_id = _capture(data, "demand_forecast_2_52w", body)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "demand_forecast_2_52w", data).run(DAY, run_id="r")
    failure = read_failure(data, "demand_forecast_2_52w", capture_id)
    assert failure is not None
    assert failure["error_class"] == "DuplicateEntityKeyError"
    assert not list((data / "silver").rglob("*.parquet"))


def _latest(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    spec = LATEST_VIEW_SPECS[(SOURCE, key)]
    completions = scan_completions(data) if spec.mode == "whole_capture" else None
    lf = pl.scan_parquet(files, hive_partitioning=False)
    return select_latest_vintage(lf, spec, None, completions=completions).collect()


def test_t_pr4_interconnector_latest_is_the_whole_newest_capture(data: Path) -> None:
    """T-PR4 (whole_capture): detects a project dropped from the register surviving in _latest.

    Capture 2 removes one project; a key-latest selection would keep capture
    1's row for it, so the register would still list a withdrawn project.
    """
    key = "interconnector_register"
    body = pilot_body(key)
    lines = body.split(b"\r\n")
    dropped = lines[1].split(b",")[13].decode()
    second = b"\r\n".join([lines[0], *lines[2:]])
    _capture(data, key, body, written_at=datetime(2026, 10, 8, 8, tzinfo=UTC))
    capture_2 = _capture(
        data,
        key,
        second,
        written_at=datetime(2026, 10, 8, 12, tzinfo=UTC),
        ckan_last_modified="2026-10-08T11:00:00.000001",
    )
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")

    latest = _latest(data, key)
    assert _rows(body) == 35
    assert latest.height == 34
    assert latest["bronze_capture_id"].unique().to_list() == [capture_2]
    assert dropped not in latest["project_number"].to_list()


def test_t_pr4_constraint_cost_latest_is_per_month(data: Path) -> None:
    """T-PR4 (key_latest): detects a month a newer capture omits vanishing from _latest.

    Capture 1 carries Sep-26; capture 2 revises Oct-26 and drops Sep-26.
    ``_latest`` must hold capture 2's values plus capture 1's Sep-26.
    """
    key = "constraint_cost_fc_24m"
    lines = pilot_body(key).split(b"\r\n")
    header, rows = lines[0], [line for line in lines[1:] if line]
    first = b"\r\n".join([header, b"Sep-26,500.0", *rows, b""])
    revised = [b"Oct-26,999.9" if row.startswith(b"Oct-26,") else row for row in rows]
    second = b"\r\n".join([header, *revised, b""])
    capture_1 = _capture(data, key, first, written_at=datetime(2026, 10, 8, 8, tzinfo=UTC))
    capture_2 = _capture(
        data,
        key,
        second,
        written_at=datetime(2026, 10, 8, 12, tzinfo=UTC),
        ckan_last_modified="2026-10-08T11:00:00.000001",
    )
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")

    latest = _latest(data, key).sort("forecast_month")
    assert latest.height == 25
    sep = latest.filter(pl.col("forecast_month") == date(2026, 9, 1))
    assert sep["bronze_capture_id"].to_list() == [capture_1]
    assert sep["constraint_cost"].to_list() == [500.0]
    oct_ = latest.filter(pl.col("forecast_month") == date(2026, 10, 1))
    assert oct_["constraint_cost"].to_list() == [999.9]
    rest = latest.filter(pl.col("forecast_month") != date(2026, 9, 1))
    assert rest["bronze_capture_id"].unique().to_list() == [capture_2]


def test_t_pr5_eligibility_and_no_time_recipe() -> None:
    """T-PR5: detects a held output losing its hold or question, or a time recipe

    the research holds as class 3 (``local_instant``, ``utc_instant``,
    ``sp_pair``, ``date_sp1``, ``month``) entering a pilot record.
    """
    for key in ("tec_register", "interconnector_register", "embedded_register"):
        assert _record(key).eligibility is None, key
    for key, question in (
        ("demand_forecast_2_52w", Q1),
        ("da_demand_fc_performance", Q2),
        ("constraint_cost_fc_24m", Q3),
    ):
        eligibility = _record(key).eligibility
        assert isinstance(eligibility, Held), key
        assert eligibility.unit == "E-SEM"
        assert eligibility.question == question
    for key in PILOT_KEYS:
        record = _record(key)
        assert record.temporal.kind == "none", key
        assert record.vintage == "ckan_last_modified", key
        assert all(
            column.dtype != "datetime" for epoch in record.epochs for column in epoch.columns
        ), key


def test_t_pr6_a_new_vendor_column_fails_the_header_epoch(data: Path) -> None:
    """T-PR6: detects a changed vendor header being typed by the old epoch."""
    key = "constraint_cost_fc_24m"
    lines = pilot_body(key).split(b"\r\n")
    widened = [lines[0] + b",Extra", *(line + b",x" for line in lines[1:] if line), b""]
    capture_id = _capture(data, key, b"\r\n".join(widened))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    failure = read_failure(data, key, capture_id)
    assert failure is not None
    assert failure["error_class"] == "HeaderEpochError"
    assert not list((data / "silver").rglob("*.parquet"))
