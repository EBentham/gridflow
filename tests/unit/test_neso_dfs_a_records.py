"""Demand Flexibility Service (current + Archive 2025/2026) and the STOR day-ahead buy curve: five
frozen records, four eligible and one held, plus the ADR-040 ledger entries (v0.22-K-DFS-a).

Two package files carry the records: ``demand-flexibility``
(``demand_flexibility_service_requirement``,
``..._industry_notification``, ``..._utilisation_report``, ``..._utilisation_summary``) and
``short-term-operating-reserve-stor-day-ahead-buy-curve`` (``stor_da_buy_curve``; the vendor-hash
TXT
``stor_da_buy_curve_files`` stays ``DOC``). Every test writes recorded fixture captures (cuts of the
2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/dfsa/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation.

Record decisions under test (K-DFS-FACTS, K-DFS-a-SPEC):

- ``issue: none`` everywhere, vintage ``ckan_last_modified`` (every resource is an upload). The four
  DFS families are resource-partitioned (``latest_partition: resource_id``) with ``resource_id`` in
  the key: the Archive 2025/2026 and current resources are disjoint seasons, one record per family.
- Every DFS window clock label is a raw ``string`` (no date + clock-label recipe; the archive clock
  is
  disputed). The one typed DFS instant is the current ``DFS Submission Time_Local`` (UK local).
- Archive rows carry null ``event_id`` / ``window_from_utc`` (never fabricated). Zeros and negative
  settled values stay as published.
- The service requirement is held (REQUIREMENT-ISSUE-AVAILABILITY); the archive utilisation report
  repeats four whole rows and fails the capture as ``DuplicateEntityKeyError`` under its ledger
  entry; the TR127 / TR136 FFR workbook editions are two ``overlap`` ledger entries.
- STOR buy-curve ``Effective From`` / ``To`` carry a literal ``Z`` that agrees with the documented
  05:00 Europe/London service day (23, 24 and 25 hour days).
"""

from __future__ import annotations

import csv
import io
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from _neso_generic_support import write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings

from gridflow.connectors.neso_data_portal import eligibility as eligibility_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Eligible, Held, load_registry
from gridflow.connectors.neso_data_portal.registry.record import has_issue_time
from gridflow.silver.date_columns import DATE_COL_SQL_TYPES
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    read_failure,
)
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "dfsa"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
LONDON = ZoneInfo("Europe/London")
UNIT = "v0.22-K-DFS-a"

RQ = "demand_flexibility_service_requirement"
NT = "demand_flexibility_industry_notification"
RP = "demand_flexibility_utilisation_report"
SM = "demand_flexibility_utilisation_summary"
STOR = "stor_da_buy_curve"
STOR_FILES = "stor_da_buy_curve_files"
DFS = (RQ, NT, RP, SM)
RECORDED = (RQ, NT, RP, SM, STOR)
ELIGIBLE = (NT, RP, SM, STOR)
HELD = (RQ,)

DFS_PACKAGE = "demand-flexibility"
STOR_PACKAGE = "short-term-operating-reserve-stor-day-ahead-buy-curve"
PACKAGE_OF = {
    RQ: DFS_PACKAGE,
    NT: DFS_PACKAGE,
    RP: DFS_PACKAGE,
    SM: DFS_PACKAGE,
    STOR: STOR_PACKAGE,
}
PACKAGE_ID_OF = {
    DFS_PACKAGE: "829e325f-8926-40ab-9dd6-8b7aea6cb244",
    STOR_PACKAGE: "0eb97ab8-61bd-4ac2-9619-10d80ae50e77",
}

Q8 = [
    "Delivery Date",
    "From",
    "To",
    "Service Requirement MW",
    "Service Requirement Type",
    "Guaranteed Acceptance Price GBP per MWh",
    "Dispatch Type",
    "Participant Bids Eligible",
]
Q15 = [
    "Event ID",
    "DFS Submission Time_Local",
    "Event Type",
    "Event Tag",
    "Delivery Date",
    "From_Local",
    "To_Local",
    "From_UTC",
    "To_UTC",
    "Service Requirement MW",
    "Service Requirement Type",
    "Guaranteed Acceptance Price GBP per MWh",
    "Dispatch Type",
    "Participant Bids Eligible",
    "Zonal Cap",
]
N6 = [
    "Notification Issued Date",
    "Notification Issued Time",
    "Requirement For",
    "Requirement Type",
    "Status",
    "Notification Type",
]
REGIONS = [
    "North Scotland",
    "South and Central Scotland",
    "North East England",
    "North West England",
    "Yorkshire",
    "East Midlands",
    "West Midlands",
    "London",
    "East England",
    "South East England",
    "South West England",
    "Southern England",
    "North Wales Merseyside and Cheshire",
    "South Wales",
    "Other",
    "Total",
]
B25 = [
    "Delivery Date",
    "Registered DFS Participant",
    "DFS Unit ID",
    "DFS Volume MW",
    "From",
    "To",
    "Service Requirement Type",
    "Utilisation Price GBP per MWh",
    "Status",
    *REGIONS,
]
B15 = [
    "Event ID",
    "Event Type",
    "Event Tag",
    "Delivery Date",
    "From_Local",
    "To_Local",
    "From_UTC",
    "To_UTC",
    "Registered DFS Participant",
    "DFS Unit ID",
    "Zone",
    "DFS Procured MW",
    "Service Requirement Type",
    "Utilisation Price GBP per MWh",
    "Status",
]
S9 = [
    "Delivery Date",
    "From",
    "To",
    "Service Requirement Type",
    "Service Requirement MW",
    "DFS Procured MW",
    "DFS Provider Bids Accepted Total Cost GBP",
    "Settled Volume MW",
    "Settled Cost GBP",
]
S14 = [
    "Event ID",
    "Event Type",
    "Event Tag",
    "Delivery Date",
    "From_Local",
    "To_Local",
    "From_UTC",
    "To_UTC",
    "Service Requirement Type",
    "Service Requirement MW",
    "DFS Procured MW",
    "DFS Provider Bids Accepted Total Cost GBP",
    "Settled Volume MW",
    "Settled Cost GBP",
]
C9 = [
    "Buy Curve Name",
    "Service Catalog",
    "Curve to be applied to",
    "Effective From Date",
    "Effective To Date",
    "Buy Curve Step: Buy Curve Step Id",
    "MW",
    "Availability Price (£/MWh)",
    "Buy Curve: Buy Curve ID",
]
HEADERS_OF: dict[str, list[list[str]]] = {
    RQ: [Q8, Q15],
    NT: [N6],
    RP: [B25, B15],
    SM: [S9, S14],
    STOR: [C9],
}

# Vendor name -> silver name where the spec's rule (lower-case, runs of non-[a-z0-9] as one `_`) is
# overridden: the window labels (one shared start name across epochs, `from` / `to` are SQL words)
# and the STOR names.
OVERRIDE = {
    "From": "window_from",
    "From_Local": "window_from",
    "To": "window_to",
    "To_Local": "window_to",
    "From_UTC": "window_from_utc",
    "To_UTC": "window_to_utc",
    "Buy Curve Step: Buy Curve Step Id": "buy_curve_step_id",
    "Availability Price (£/MWh)": "availability_price",
    "Buy Curve: Buy Curve ID": "buy_curve_id",
    "DFS Submission Time_Local": "dfs_submission_time",
}
# kind: s string, f float64, d date (%d/%m/%Y), l UK-local datetime, t UTC datetime (literal Z).
FLOAT_SOURCES = {
    "Service Requirement MW",
    "Guaranteed Acceptance Price GBP per MWh",
    "DFS Volume MW",
    "Utilisation Price GBP per MWh",
    "DFS Procured MW",
    "DFS Provider Bids Accepted Total Cost GBP",
    "Settled Volume MW",
    "Settled Cost GBP",
    "MW",
    "Availability Price (£/MWh)",
    *REGIONS,
}
DATE_SOURCES = {"Delivery Date", "Notification Issued Date", "Requirement For"}
LOCAL_SOURCES = {"DFS Submission Time_Local"}
UTC_SOURCES = {"Effective From Date", "Effective To Date"}
CLOCK_SOURCES = {"From", "To", "From_Local", "To_Local", "From_UTC", "To_UTC"}


def silver_name(source: str) -> str:
    """The spec's silver-name rule."""
    if source in OVERRIDE:
        return OVERRIDE[source]
    return re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")


def kind_of(source: str) -> str:
    """The silver kind of a vendor column."""
    if source in FLOAT_SOURCES:
        return "f"
    if source in DATE_SOURCES:
        return "d"
    if source in LOCAL_SOURCES:
        return "l"
    if source in UTC_SOURCES:
        return "t"
    return "s"


SILVER_OF = {
    family: {s: (silver_name(s), kind_of(s)) for header in headers for s in header}
    for family, headers in HEADERS_OF.items()
}

K4 = ["resource_id", "delivery_date", "window_from", "service_requirement_type"]
KEY_OF: dict[str, list[str]] = {
    RQ: K4,
    NT: [
        "resource_id",
        "notification_issued_date",
        "notification_issued_time",
        "requirement_for",
        "requirement_type",
        "notification_type",
    ],
    RP: [*K4, "dfs_unit_id"],
    SM: K4,
    STOR: ["buy_curve_step_id"],
}
TEMPORAL_OF: dict[str, dict[str, str | None]] = {
    RQ: {"kind": "date_sp1", "date_column": "delivery_date"},
    NT: {"kind": "date_sp1", "date_column": "notification_issued_date"},
    RP: {"kind": "date_sp1", "date_column": "delivery_date"},
    SM: {"kind": "date_sp1", "date_column": "delivery_date"},
    STOR: {"kind": "utc_instant", "column": "effective_from_date"},
}
TEMPORAL_INPUT = {
    RQ: "delivery_date",
    NT: "notification_issued_date",
    RP: "delivery_date",
    SM: "delivery_date",
    STOR: "effective_from_date",
}
STOR_TIME = "%Y-%m-%d %H:%M:%SZ"

QUESTION = (
    "TODO: REQUIREMENT-ISSUE-AVAILABILITY — the DFS service requirement resources (current and "
    "Archive 2025/2026) carry no issue column and publish delivery windows starting on or before "
    "their last_modified (archive 1,126 of 1,132 starts, current 1,230 of 1,238), and the current "
    "DFS Submission Time_Local is the auction-close deadline, not a publication time; when was "
    "each "
    "requirement first published, and does NESO retain earlier requirement editions? (The archive "
    "requirement dictionary labels From/To GMT while the archive report and summary dictionaries "
    "label theirs local time — TODO: ARCHIVE-CLOCK-CONSISTENCY; window_from holds the vendor label "
    "of each resource as published.)"
)


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    family: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str


CAPTURES: dict[str, Capture] = {
    "qa": Capture(
        RQ,
        "f5605e2b-b677-424c-8df7-d0ce4ee03cef",
        "DFS Service Requirement (Archive 2025/2026)",
        "dfs-service-requirement.csv",
        "2026-03-27T10:45:09.034050",
        "2026-10-08T10:05:29.935234+00:00",
    ),
    "qc": Capture(
        RQ,
        "3635fd80-49d7-4d02-964d-cc8c08d50302",
        "DFS Service Requirement",
        "dfs-service-requirement.csv",
        "2026-10-08T09:47:40.084869",
        "2026-10-08T10:05:33.451364+00:00",
    ),
    "na": Capture(
        NT,
        "3d042450-b556-450f-a4b5-cdb075cdf9c1",
        "DFS Industry Notification (Archive 2025/2026)",
        "dfs-industry-notification.csv",
        "2026-03-27T10:44:31.904341",
        "2026-10-08T10:05:25.790632+00:00",
    ),
    "ba": Capture(
        RP,
        "cc36fff5-5f6f-4fde-8932-c935d982ecd8",
        "DFS Utilisation Report (Archive 2025/2026)",
        "dfs-utilisation-report.csv",
        "2026-03-27T12:04:05.656144",
        "2026-10-08T10:05:37.181761+00:00",
    ),
    "bd": Capture(
        RP,
        "cc36fff5-5f6f-4fde-8932-c935d982ecd8",
        "DFS Utilisation Report (Archive 2025/2026)",
        "dfs-utilisation-report.csv",
        "2026-03-27T12:04:05.656144",
        "2026-10-08T10:05:37.181761+00:00",
    ),
    "bc": Capture(
        RP,
        "3ebf77d7-05df-466e-a023-dc45a90efeea",
        "DFS Utilisation Report",
        "dfs-utilisation-report.csv",
        "2026-10-06T11:06:32.694169",
        "2026-10-08T10:05:40.167599+00:00",
    ),
    "sa": Capture(
        SM,
        "25698259-0b66-42f0-ac59-ef0df5245812",
        "DFS Utilisation Report Summary (Archive 2025/2026)",
        "dfs-utilisation-report-summary.csv",
        "2026-04-09T08:55:59.349201",
        "2026-10-08T10:05:43.975993+00:00",
    ),
    "sc": Capture(
        SM,
        "705e573c-ddac-4675-b410-82916b35c4fe",
        "DFS Utilisation Report Summary",
        "dfs-utilisation-report-summary.csv",
        "2026-10-06T11:06:52.944763",
        "2026-10-08T10:05:46.822895+00:00",
    ),
    "st": Capture(
        STOR,
        "8abc71ba-4484-4683-be68-5bc241b6ae7a",
        "STOR DA Auction Buy Curve Report",
        "stor-da-auction-buy-curve-report.csv",
        "2026-03-30T04:22:46.013246",
        "2026-10-08T11:27:35.997765+00:00",
    ),
}
FIRST_OF = {RQ: "qa", NT: "na", RP: "bc", SM: "sa", STOR: "st"}
LOADING = [a for a in CAPTURES if a != "bd"]
H_ST_ID = "1eacef3e-c7e5-4ce2-a1e3-c667f39c0a17"
ARCHIVE_ALIASES = {"qa", "na", "ba", "sa"}


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings point at."""
    with tempfile.TemporaryDirectory(
        prefix="dfsa", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: the bronze bytes)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed with edge-stripped names (cells verbatim)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8")
    reader = csv.reader(io.StringIO(text, newline=""))
    names = [n.strip() for n in next(reader)]
    return [dict(zip(names, r, strict=False)) for r in reader if any(r)]


def header(alias: str) -> list[str]:
    """The fixture's header as the production reader returns it (edge whitespace stripped)."""
    first = body(alias).decode("utf-8").splitlines()[0]
    return [n.strip() for n in next(csv.reader([first]))]


def capture(data: Path, alias: str, *, raw: bytes | None = None) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    package = PACKAGE_OF[meta.family]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=package,
        package_id=PACKAGE_ID_OF[package],
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
        ckan_format="CSV",
        extension="csv",
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _no_silver(data: Path, key: str) -> bool:
    return not list((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))


def _package_doc(package: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / f"{package}.json").read_text("utf-8"))
    return document


def _columns(key: str) -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record(key)))
        if c not in ("year", "month")
    ]


def _expected(kind: str, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank cell is null)."""
    if cell == "":
        return None
    if kind == "f":
        return float(cell)
    if kind == "d":
        return datetime.strptime(cell, "%d/%m/%Y").date()
    if kind == "l":
        local = datetime.strptime(cell, "%d/%m/%Y %H:%M").replace(tzinfo=LONDON)
        return local.astimezone(UTC)
    if kind == "t":
        return datetime.strptime(cell, STOR_TIME).replace(tzinfo=UTC)
    return cell


def _assert_cells(alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, date, instant and measure of ``frame`` equals the body cell."""
    family = CAPTURES[alias].family
    source_rows = rows(alias)
    for source in header(alias):
        name, kind = SILVER_OF[family][source]
        assert frame[name].to_list() == [_expected(kind, r[source]) for r in source_rows], (
            alias,
            source,
        )


def _anchor(day: date) -> datetime:
    """The UTC instant of UK-local midnight on ``day`` (the ``date_sp1`` anchor)."""
    return datetime(day.year, day.month, day.day, tzinfo=LONDON).astimezone(UTC)


def _load_clean(data: Path, *aliases: str) -> pl.DataFrame:
    """Capture and run the aliases of one family: every row written, none excluded, the generic
    output columns, one completion per capture, the temporal column tz-aware UTC."""
    (family,) = {CAPTURES[a].family for a in aliases}
    capture_ids = {a: capture(data, a) for a in aliases}
    written = get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert written == sum(len(rows(a)) for a in aliases)
    for alias, capture_id in capture_ids.items():
        completion = read_completion(data, family, capture_id)
        assert completion is not None, alias
        assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
        assert read_failure(data, family, capture_id) is None
    frame = _silver(data, family)
    assert frame.height == written
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(family)
    assert frame.schema["timestamp_utc"] == pl.Datetime("us", "UTC")
    return frame


def _fail(data: Path, alias: str, *, raw: bytes | None = None) -> tuple[str, str]:
    """Run the family of ``alias`` on a capture that must fail: ``(capture id, error class)``;
    nothing is loaded and no completion is written."""
    family = CAPTURES[alias].family
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    ((got_id, cls, _message),) = info.value.failures
    assert got_id == capture_id
    assert read_completion(data, family, capture_id) is None
    assert read_failure(data, family, capture_id) is not None
    assert _no_silver(data, family)
    return got_id, cls


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the exact vendor headers (Q8,
    Q15, N6, B25, B15, S9, S14, C9), CRLF terminators and no BOM on every body, the archive
    requirement rows with a quoted participant list, a winter ``From`` label and the constant zero
    GAP, the current requirement slice holding Events 117 (Test) and 118 (Live) at 2026-09-10 19:00,
    the notification slice with two issue times for one requirement date, the current report slice
    with an Accepted and a Rejected bid, the archive report duplicate pair, a Total of zero next to
    a nonzero DFS volume, blank settlement in both summaries, negative settled values, and the STOR
    slice holding a full three-vertex curve with a zero MW and a zero price, a 23 hour and a 25 hour
    service day."""
    for alias in CAPTURES:
        raw = body(alias)
        raw.decode("utf-8")
        assert not raw.startswith(b"\xef\xbb\xbf"), alias
        assert raw.endswith(b"\r\n") and raw.count(b"\r\n") == raw.count(b"\n"), alias
    assert header("qa") == Q8 and header("qc") == Q15 and header("na") == N6
    assert header("ba") == header("bd") == B25 and header("bc") == B15
    assert header("sa") == S9 and header("sc") == S14 and header("st") == C9
    assert len(B25) == 25 and len(B15) == 15 and len(C9) == 9
    assert b'"' in body("qa") and b'"' in body("qc")
    assert {r["Guaranteed Acceptance Price GBP per MWh"] for r in rows("qa")} == {"0"}
    qc_keys = {
        (r["Event ID"], r["Service Requirement Type"], r["From_Local"])
        for r in rows("qc")
        if r["Delivery Date"] == "10/09/2026"
    }
    assert {("117", "Test", "19:00"), ("118", "Live", "19:00")} <= qc_keys
    shared = [r["Requirement For"] for r in rows("na")]
    assert len(shared) > len(set(shared))
    assert {r["Status"] for r in rows("bc")} == {"Accepted", "Rejected"}
    assert all(float(r["DFS Procured MW"]) > 0 for r in rows("bc"))
    assert len(rows("bd")) == 2 and rows("bd")[0] == rows("bd")[1]
    assert any(r["Total"] == "0" and float(r["DFS Volume MW"]) > 0 for r in rows("ba"))
    assert any(r["Settled Volume MW"] == "" for r in rows("sa") + rows("sc"))
    assert any(r["Settled Volume MW"].startswith("-") for r in rows("sc"))
    assert any(r["Settled Cost GBP"].startswith("-") for r in rows("sc"))
    assert any(r["MW"] == "0" for r in rows("st")) and any(
        r["Availability Price (£/MWh)"] == "0" for r in rows("st")
    )
    hours = {
        (
            datetime.strptime(r["Effective To Date"], STOR_TIME)
            - datetime.strptime(r["Effective From Date"], STOR_TIME)
        ).total_seconds()
        // 3600
        for r in rows("st")
    }
    assert {23.0, 24.0, 25.0} <= hours


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_partition_vintage_and_no_issue(
    key: str,
) -> None:
    """Detects a record that invents an issue time (no issue column exists; the current submission
    time is an auction-close deadline), a ``key_latest`` selection, a ``run_type``, a missing
    resource partition on a DFS family (archive and current are disjoint seasons of one family), a
    partition on the single-resource STOR curve or the wrong vintage: every record is version 1,
    utf-8 CSV, whole-capture selection, the spec's entity key, ``ckan_last_modified``, with the
    vendor header(s) as its epochs and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key in DFS else None)
    assert record.vintage == "ckan_last_modified" and record.vintage_evidence is None
    assert list(record.entity_key) == KEY_OF[key]
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None
    assert [list(e.header) for e in record.epochs] == HEADERS_OF[key]
    for epoch in record.epochs:
        assert epoch.issue.kind == "none" and epoch.unpivot is None
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)


def test_each_epoch_header_is_read_from_the_body_not_retyped() -> None:
    """Detects an epoch whose header was retyped (the STOR ``£`` as the proposal's mojibake) rather
    than read from the body: the first record of every fixture, edge-stripped as the production
    reader returns it, equals the frozen epoch, and each paired family carries exactly the archive
    and the current epoch."""
    for alias, key, epoch_no in [
        ("qa", RQ, 0),
        ("qc", RQ, 1),
        ("na", NT, 0),
        ("ba", RP, 0),
        ("bc", RP, 1),
        ("sa", SM, 0),
        ("sc", SM, 1),
        ("st", STOR, 0),
    ]:
        assert header(alias) == list(_record(key).epochs[epoch_no].header), alias
    assert [len(e.header) for e in _record(RQ).epochs] == [8, 15]
    assert [len(e.header) for e in _record(NT).epochs] == [6]
    assert [len(e.header) for e in _record(RP).epochs] == [25, 15]
    assert [len(e.header) for e in _record(SM).epochs] == [9, 14]
    assert "£" in _record(STOR).epochs[0].header[7] and "Â" not in _record(STOR).epochs[0].header[7]


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: measures are
    nullable float64 (zero is a value), identifiers, codes and categories are strings (``Event ID``
    and ``Zone`` are never cast), every DFS window clock label is a string, dates are ``%d/%m/%Y``,
    the current ``DFS Submission Time_Local`` is a nullable ``%d/%m/%Y %H:%M`` datetime in
    ``Europe/London`` with ``ambiguous: raise`` and evidence, the STOR effective cells are
    ``%Y-%m-%d %H:%M:%SZ`` datetimes with zone ``UTC``, key columns and the temporal input are
    ``nullable: false`` and nothing else is, and no column carries a bound."""
    record = _record(key)
    non_null = set(KEY_OF[key]) - {"resource_id"}
    non_null.add(TEMPORAL_INPUT[key])
    for epoch, expected_header in zip(record.epochs, HEADERS_OF[key], strict=True):
        assert [c.source for c in epoch.columns] == expected_header
        for column in epoch.columns:
            name, kind = SILVER_OF[key][column.source]
            assert column.name == name
            assert column.trim is None and column.null_tokens == ()
            assert column.formats_by_filename is None
            assert (column.min, column.max) == (None, None), name
            if kind == "l":
                assert column.nullable is True
            else:
                assert column.nullable == (name not in non_null), name
            if kind == "d":
                assert (column.dtype, column.format, column.zone) == ("date", "%d/%m/%Y", None)
            elif kind == "l":
                assert (column.dtype, column.format, column.zone) == (
                    "datetime",
                    "%d/%m/%Y %H:%M",
                    "Europe/London",
                )
                assert column.ambiguous == "raise" and "document 346011" in (
                    column.zone_evidence or ""
                )
            elif kind == "t":
                assert (column.dtype, column.format, column.zone) == ("datetime", STOR_TIME, "UTC")
            else:
                assert column.format is None and column.zone is None
                assert column.dtype == {"f": "float64", "s": "string"}[kind], name
            if kind != "l":
                assert column.zone_evidence is None and column.ambiguous is None


def test_the_spec_silver_names_and_window_labels() -> None:
    """Detects a renamed silver column: the window labels share ``window_from`` / ``window_to``
    across the archive ``From`` / ``To`` and the current ``From_Local`` / ``To_Local`` (one shared
    start name lets the cross-epoch key pass), the UTC labels are ``window_from_utc`` /
    ``window_to_utc``, ``dfs_volume_mw`` and ``dfs_procured_mw`` stay separate names, and the STOR
    names are the spec's."""
    names = {key: {c.name for e in _record(key).epochs for c in e.columns} for key in RECORDED}
    for key in (RQ, RP, SM):
        assert {"window_from", "window_to", "window_from_utc", "window_to_utc"} <= names[key]
    assert not {"from", "to"} & set().union(*names.values())
    assert {"dfs_volume_mw", "dfs_procured_mw"} <= names[RP]
    assert "zone" in names[RP] and "zonal_cap" in names[RQ]
    assert "guaranteed_acceptance_price_gbp_per_mwh" in names[RQ]
    assert "dfs_provider_bids_accepted_total_cost_gbp" in names[SM]
    assert names[STOR] == {
        "buy_curve_name",
        "service_catalog",
        "curve_to_be_applied_to",
        "effective_from_date",
        "effective_to_date",
        "buy_curve_step_id",
        "mw",
        "availability_price",
        "buy_curve_id",
    }
    assert "north_wales_merseyside_and_cheshire" in names[RP]


def test_every_dfs_window_clock_is_a_raw_string_and_only_the_submission_time_is_typed() -> None:
    """Detects a typed DFS window clock (RULINGS 572 / 588: the engine has no date + clock-label
    recipe and the archive clock is disputed): ``window_from``, ``window_to``, ``window_from_utc``,
    ``window_to_utc`` and the notification issue time are ``string`` in every epoch, and the only
    ``datetime`` column of the four DFS records is ``dfs_submission_time`` of the current
    requirement epoch."""
    typed: list[tuple[str, int, str]] = []
    for key in DFS:
        for number, epoch in enumerate(_record(key).epochs):
            types = {c.name: c.dtype for c in epoch.columns}
            for name in ("window_from", "window_to", "window_from_utc", "window_to_utc"):
                assert types.get(name, "string") == "string", (key, name)
            if key == NT:
                assert types["notification_issued_time"] == "string"
            typed += [(key, number, c.name) for c in epoch.columns if c.dtype == "datetime"]
    assert typed == [(RQ, 1, "dfs_submission_time")]


def test_archive_epochs_have_no_event_columns_and_nothing_is_fabricated() -> None:
    """Detects an archive epoch gaining event columns (never fabricate ``event_id`` / ``event_type``
    / ``event_tag`` / ``window_*_utc`` for archive rows): the archive epochs name none of them and
    the current epochs name all, so the silver nulls come from the epoch union alone."""
    event = {"event_id", "event_type", "event_tag", "window_from_utc", "window_to_utc"}
    for key in (RQ, RP, SM):
        archive, current = _record(key).epochs
        assert not event & {c.name for c in archive.columns}, key
        assert event <= {c.name for c in current.columns}, key


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_spec_recipe(key: str) -> None:
    """Detects a wrong anchor: the DFS families take ``date_sp1`` of the service date (the
    notification of its posting date, not a window start) and the STOR curve ``utc_instant`` of
    ``effective_from_date``."""
    recipe = _record(key).temporal
    expected = TEMPORAL_OF[key]
    assert recipe.kind == expected["kind"]
    assert recipe.date_column == expected.get("date_column")
    assert recipe.column == expected.get("column")


def test_the_two_designated_date_names_are_registered_as_dates() -> None:
    """Detects a ``date_sp1`` input without an SQL type (the manifest requires every designated date
    to have one): ``delivery_date`` and ``notification_issued_date`` are ``DATE`` in the shared
    map."""
    assert DATE_COL_SQL_TYPES["delivery_date"] == "DATE"
    assert DATE_COL_SQL_TYPES["notification_issued_date"] == "DATE"


def test_every_dfs_a_resource_is_an_upload() -> None:
    """Detects the V-8 premise drifting: ``ckan_last_modified`` is the vintage because every
    resource of both packages is an upload."""
    for package in (DFS_PACKAGE, STOR_PACKAGE):
        assert {r["url_type"] for r in _package_doc(package)["resources"]} == {"upload"}


# --------------------------------------------------------------------------- #
# Eligibility, dispositions and the ledger
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_four_eligible_records_carry_no_override(key: str) -> None:
    """Detects a hold or a per-record override on a clean family: each is effectively eligible, in
    an eligible package."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.eligibility == Eligible(status="eligible")


def test_the_service_requirement_is_held_with_the_verbatim_question_and_the_unit() -> None:
    """Detects the hold dropped, reworded, filed under the wrong unit, or a package-wide hold: the
    requirement record is ``held`` with the spec's question verbatim and unit ``v0.22-K-DFS-a``,
    effectively held, while its package stays eligible."""
    package, family = registry_module.load_registry().families[RQ]
    eligibility = _record(RQ).eligibility
    assert eligibility == Held(status="held", question=QUESTION, unit=UNIT)
    assert effective_eligibility(package, family) == eligibility
    assert package.eligibility == Eligible(status="eligible")
    assert "\n" not in eligibility.question


def test_the_two_packages_stay_eligible_and_hold_exactly_the_five_records() -> None:
    """Detects a package held by accident, a record on the wrong family or a family gaining a
    transformer: both package files are ``eligible``, hold exactly the five records and no family
    has a bespoke transformer."""
    found: set[str] = set()
    for package in sorted(set(PACKAGE_OF.values())):
        document = _package_doc(package)
        assert document["eligibility"] == {"status": "eligible"}, package
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), package
    assert found == set(RECORDED)


def test_every_resource_disposition_is_unchanged_and_the_hash_file_stays_doc() -> None:
    """Detects a changed disposition: every DFS resource stays ``SILVER`` to its own family (the
    archive report capture fails at transform under its ledger entry; it is not a resource HOLD),
    the STOR curve CSV stays ``SILVER`` and the 64-byte vendor-hash TXT stays ``DOC`` of the
    ``stor_da_buy_curve_files`` family."""
    dfs = _package_doc(DFS_PACKAGE)["resources"]
    assert len(dfs) == 7
    for resource in dfs:
        assert resource["disposition"] == {"kind": "SILVER", "key": resource["family"]}
        assert resource["family"] in DFS
    by_family: dict[str, int] = {}
    for resource in dfs:
        by_family[resource["family"]] = by_family.get(resource["family"], 0) + 1
    assert by_family == {RQ: 2, NT: 1, RP: 2, SM: 2}
    stor = {r["id"]: r for r in _package_doc(STOR_PACKAGE)["resources"]}
    assert stor[H_ST_ID]["disposition"] == {"kind": "DOC"}
    assert stor[H_ST_ID]["family"] == STOR_FILES
    assert stor[CAPTURES["st"].resource_id]["disposition"] == {"kind": "SILVER", "key": STOR}
    files = registry_module.load_registry().families[STOR_FILES][1]
    assert files.record is None and files.transformer is None


def _report_line(key: str) -> str:
    lines = eligibility_module.render(load_registry()).splitlines()
    (line,) = [ln for ln in lines if f"| `{key}` |" in ln]
    return line


def test_the_eligibility_report_lists_four_eligible_and_the_requirement_held() -> None:
    """Detects a family reported with the wrong status: each eligible family is ``silver`` /
    ``eligible`` (``whole_capture per resource_id`` for the DFS families, the CKAN last-modified
    vintage for all), the requirement is ``silver`` / ``held: TODO: REQUIREMENT-ISSUE-AVAILABILITY``
    with the unit."""
    for key in ELIGIBLE:
        line = _report_line(key)
        assert "| silver | eligible |" in line, key
        assert ("whole_capture per resource_id" in line) == (key in DFS), key
    assert "date_sp1" in _report_line(NT) and "utc_instant" in _report_line(STOR)
    line = _report_line(RQ)
    assert "| silver | held: TODO: REQUIREMENT-ISSUE-AVAILABILITY" in line
    assert f"(unit {UNIT})" in line


def _entries(family: str) -> list[dict[str, Any]]:
    path = REGISTRY_DIR / registry_module.RECONCILE_ADJUDICATIONS_FILE
    return [e for e in json.loads(path.read_text(encoding="utf-8")) if e["family"] == family]


FFR = "ffr_post_tender_reports_workbook"


def test_the_ledger_appends_exactly_the_three_dfs_a_entries_last() -> None:
    """Detects an open reconcile gap, an entry naming other captures, a wider scope, a dropped
    ruling or an entry the registry does not back. The ledger validates against the registry and
    ends with, in order (before K-SCN-3's single WS1 2025 entry; v0.22-H's October 2014 entry
    was retired when its repeated hour became keyed by ``dtm_occurrence``): the TR127
    ``overlap`` (ruling 798, the original 68529290 and the amended 16773f53 workbooks), the
    TR136 ``overlap`` (ruling 798, 7912d65a and a85e6149) and the archive utilisation
    report as a ``failed`` ``DuplicateEntityKeyError`` (ruling 804, cc36fff5). Every
    reason / question / evidence is a non-empty single line, every reason says ``never
    deduplicated``, the overlap entries name both editions, and no other DFS-a family has an
    entry."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    tr127, tr136, report = entries[-4:-1]
    assert [(e.family, e.category, e.cause, e.ruling) for e in (tr127, tr136, report)] == [
        (FFR, "overlap", None, "798"),
        (FFR, "overlap", None, "798"),
        (RP, "failed", "DuplicateEntityKeyError", "804"),
    ]

    def resources(entry: Any) -> list[str]:
        found = []
        for path in entry.captures:
            match = registry_module.CAPTURE_ID_PATTERN.fullmatch(path)
            assert match is not None
            directory = "ffr_post_tender_reports_files" if entry.family == FFR else entry.family
            assert path.startswith(f"bronze/neso_data_portal/{directory}/2026/10/08/raw_")
            found.append(match["rid"])
        return found

    assert resources(tr127) == [
        "68529290-7992-4891-90bc-68fa9bcb5aeb",
        "16773f53-c0f9-4b8c-aaee-ad819e93d405",
    ]
    assert resources(tr136) == [
        "7912d65a-451a-4dea-b8eb-4bc5c7af254b",
        "a85e6149-3893-48f6-953a-0a10e796ce8e",
    ]
    assert resources(report) == [CAPTURES["ba"].resource_id]
    for entry in (tr127, tr136, report):
        for text in (entry.reason, entry.question, entry.evidence):
            assert "\n" not in text and text.strip()
        assert "never deduplicated" in entry.reason and "ADR-040" in entry.evidence
    assert "175 tender refs" in tr127.reason and "132 tender refs" in tr136.reason
    assert "TR127" in tr127.evidence and "TR136" in tr136.evidence
    assert "K-DFS-a SW-5" in tr127.evidence and "K-DFS-a SW-5" in tr136.evidence
    assert "AXLE-01 and AXLE-03" in report.reason and "ARCHIVE-REPORT-IDENTITY" in report.evidence
    for family in (RQ, NT, SM, STOR, STOR_FILES):
        assert _entries(family) == [], family
    assert len(_entries(RP)) == 1 and len(_entries(FFR)) == 2


def test_the_ffr_registry_is_not_touched_only_the_ledger_names_it() -> None:
    """Detects an FFR record change riding with this unit: the workbook family still keeps both
    editions of each tender report (the key includes ``resource_id``, ruling 798) and the
    supersession
    question lives in the ledger only."""
    record = _record(FFR)
    assert "resource_id" in record.entity_key
    assert record.eligibility is None


@pytest.mark.parametrize("key", RECORDED)
def test_the_skeleton_page_renders_the_new_records(key: str) -> None:
    """Detects a record the docs generator cannot render (the two-epoch DFS records, the held
    requirement, the resource-partitioned keys)."""
    package = PACKAGE_OF[key]
    page = skeleton.render_package(
        load_registry(),
        {
            "name": package,
            "title": package,
            "organization": {"title": "NESO"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    assert f"`{key}`" in page


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", LOADING)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a
    duplicate key or an excluded row: the slice loads every populated row with zero exclusions and
    no failure, every identifier, label, date, instant and measure equals the body cell, the entity
    key is unique and non-null, and ``timestamp_utc`` is the ``date_sp1`` anchor of the service /
    posting date (UK-local midnight, not a window start) or the STOR ``effective_from_date``. A cell
    failing its strict cast (a date, the submission time, a STOR instant or any ``float64``) would
    fail here."""
    family = CAPTURES[alias].family
    frame = _load_clean(data, alias)
    _assert_cells(alias, frame)
    assert set(frame["resource_id"].to_list() if "resource_id" in frame.columns else []) <= {
        CAPTURES[alias].resource_id
    }
    keys = [k for k in KEY_OF[family] if k in frame.columns]
    assert not frame.select(keys).is_duplicated().any()
    for name in keys:
        assert frame[name].null_count() == 0, name
    recipe = TEMPORAL_OF[family]
    if recipe["kind"] == "utc_instant":
        assert frame["timestamp_utc"].to_list() == frame["effective_from_date"].to_list()
    else:
        column = str(recipe["date_column"])
        assert frame["timestamp_utc"].to_list() == [_anchor(d) for d in frame[column].to_list()]
    assert frame["event_time"].to_list() == frame["timestamp_utc"].to_list()


def test_the_archive_requirement_keeps_the_winter_label_zero_gap_and_quoted_list(
    data: Path,
) -> None:
    """Detects an archive requirement given event columns, a coerced clock label, a zero GAP read
    as missing or a quoted participant list split: the three rows keep ``window_from`` ``16:00``
    byte-equal as a string, ``guaranteed_acceptance_price_gbp_per_mwh`` ``0.0`` (never null), the
    comma-separated ``participant_bids_eligible`` whole, the Dispatch Type label as published, and
    null ``event_id`` / ``event_type`` / ``event_tag`` / ``dfs_submission_time`` / ``zonal_cap`` /
    ``window_*_utc`` (never fabricated)."""
    frame = _load_clean(data, "qa")
    assert frame["window_from"].to_list()[0] == "16:00" and frame.schema["window_from"] == pl.String
    assert frame["delivery_date"].to_list()[-1] == date(2025, 12, 31)
    assert set(frame["guaranteed_acceptance_price_gbp_per_mwh"].to_list()) == {0.0}
    assert frame["participant_bids_eligible"].to_list() == [
        r["Participant Bids Eligible"] for r in rows("qa")
    ]
    assert "," in frame["participant_bids_eligible"].to_list()[0]
    assert set(frame["dispatch_type"].to_list()) == {"All Providers"}
    for name in (
        "event_id",
        "event_type",
        "event_tag",
        "dfs_submission_time",
        "zonal_cap",
        "window_from_utc",
        "window_to_utc",
    ):
        assert frame[name].null_count() == frame.height, name
    assert frame.schema["service_requirement_mw"] == pl.Float64


def test_the_current_requirement_keeps_events_117_and_118_and_converts_the_bst_deadline(
    data: Path,
) -> None:
    """Detects the Test event collapsing into the Live one or an unshifted deadline: Event 117
    (Test) and Event 118 (Live) of 2026-09-10 19:00 are both kept under distinct
    ``service_requirement_type``; ``event_id`` stays a string byte-equal; the BST
    ``DFS Submission Time_Local`` 10/09/2026 12:00 is 11:00 UTC (one hour earlier), tz-aware;
    ``zonal_cap`` stays the unparsed structured text; local and UTC window labels stay raw strings
    that differ by the BST hour."""
    frame = _load_clean(data, "qc")
    at_19 = frame.filter(
        (pl.col("delivery_date") == date(2026, 9, 10)) & (pl.col("window_from") == "19:00")
    )
    assert sorted(zip(at_19["event_id"], at_19["service_requirement_type"], strict=True)) == [
        ("117", "Test"),
        ("118", "Live"),
    ]
    assert frame.schema["event_id"] == pl.String
    assert frame.schema["dfs_submission_time"] == pl.Datetime("us", "UTC")
    assert set(at_19["dfs_submission_time"].to_list()) == {datetime(2026, 9, 10, 11, 0, tzinfo=UTC)}
    first = frame.row(0, named=True)
    assert first["dfs_submission_time"] == datetime(2026, 10, 8, 11, 0, tzinfo=UTC)
    assert (first["window_from"], first["window_from_utc"]) == ("19:00", "18:00")
    assert first["zonal_cap"].startswith("Z1:500,Z2:500")
    assert frame["zonal_cap"].to_list() == [r["Zonal Cap"] for r in rows("qc")]


def test_archive_and_current_requirements_coexist_under_their_own_resources(data: Path) -> None:
    """Detects one epoch shadowing the other or a key without ``resource_id``: both captures load
    into one family, every key is unique and the two resources keep disjoint seasons; the archive
    rows keep null current-only columns while the current rows carry them."""
    frame = _load_clean(data, "qa", "qc")
    assert set(frame["resource_id"].to_list()) == {
        CAPTURES["qa"].resource_id,
        CAPTURES["qc"].resource_id,
    }
    archive = frame.filter(pl.col("resource_id") == CAPTURES["qa"].resource_id)
    current = frame.filter(pl.col("resource_id") == CAPTURES["qc"].resource_id)
    assert archive["event_id"].null_count() == archive.height
    assert current["event_id"].null_count() == 0 and current["window_from_utc"].null_count() == 0
    assert not frame.select(KEY_OF[RQ]).is_duplicated().any()


def test_notifications_keep_two_issue_times_for_one_requirement_date(data: Path) -> None:
    """Detects the requirement date used as the identity or an issue time composed from the split
    date and time fields: 08/01/2025 is published at 10:00 (Requirement Published) and 10:15
    (Revised Requirement Published), both are kept under the five-column key plus ``resource_id``,
    ``notification_issued_time`` stays the raw ``string``, ``requirement_for`` and the posting date
    are dates, and ``timestamp_utc`` is the posting date's anchor."""
    frame = _load_clean(data, "na")
    shared = frame.filter(pl.col("requirement_for") == date(2025, 1, 8))
    assert sorted(shared["notification_issued_time"].to_list()) == ["10:00", "10:15"]
    assert shared.select(KEY_OF[NT]).n_unique() == 2
    assert frame.schema["notification_issued_time"] == pl.String
    assert frame.schema["requirement_for"] == pl.Date
    assert frame.schema["notification_issued_date"] == pl.Date
    assert "event_id" not in frame.columns
    assert frame["status"].to_list() == [r["Status"] for r in rows("na")]


def test_current_report_keeps_accepted_and_rejected_bids_with_positive_procured_mw(
    data: Path,
) -> None:
    """Detects procured MW gated by status in silver or a zone cast: an Accepted and a Rejected bid
    of
    one participant in one window are both kept, ``dfs_procured_mw`` is positive on both (the
    accepted procurement is gated by ``status`` downstream), ``zone`` and ``dfs_unit_id`` stay
    strings byte-equal, the archive-only ``dfs_volume_mw`` and the regional columns are absent from
    the
    current epoch's data (null)."""
    frame = _load_clean(data, "bc")
    assert sorted(frame["status"].to_list()) == ["Accepted", "Accepted", "Rejected"]
    assert all(v > 0 for v in frame["dfs_procured_mw"].to_list())
    assert frame.schema["zone"] == pl.String
    assert frame["zone"].to_list() == [r["Zone"] for r in rows("bc")]
    assert frame["dfs_unit_id"].to_list() == [r["DFS Unit ID"] for r in rows("bc")]
    assert frame["dfs_volume_mw"].null_count() == frame.height
    assert frame["total"].null_count() == frame.height


def test_archive_report_types_regions_and_keeps_total_zero_next_to_a_volume(data: Path) -> None:
    """Detects regional columns summed, a zero Total read as missing, the volume and the procured
    names coalesced or a Rejected row dropped: the non-duplicated archive slice loads with 16
    ``float64`` regional columns, ``total`` ``0.0`` beside a nonzero ``dfs_volume_mw``, the tendered
    ``dfs_volume_mw`` and the null ``dfs_procured_mw``."""
    frame = _load_clean(data, "ba")
    for region in REGIONS:
        assert frame.schema[silver_name(region)] == pl.Float64, region
    zero_total = frame.filter(pl.col("total") == 0.0)
    assert zero_total.height >= 1 and all(v > 0 for v in zero_total["dfs_volume_mw"].to_list())
    assert frame["dfs_procured_mw"].null_count() == frame.height
    assert set(frame["status"].to_list()) == {"Rejected"}


def test_the_archive_report_duplicate_pair_fails_the_capture_as_a_duplicate_key(
    data: Path,
) -> None:
    """Detects the four repeated whole rows of the archive report loaded, deduplicated, given an
    occurrence index or a widened key (the ledger cause is the load path): the AXLE-01 2025-01-05
    16:00 pair, identical on every published column, fails the capture with
    ``DuplicateEntityKeyError``, loads nothing and leaves no completion."""
    _capture_id, cls = _fail(data, "bd")
    assert cls == "DuplicateEntityKeyError"


def test_summary_keeps_blank_settlement_null_and_negative_settled_values_signed(
    data: Path,
) -> None:
    """Detects blank settlement read as zero, a sign flipped or a bound applied: the archive slice
    keeps its two blank settlement cells null next to a present settlement, the current slice keeps
    Event 75's negative ``settled_volume_mw`` (``-0.03``) and ``settled_cost_gbp`` (``-2.65``)
    signed and its blank settlement null, ``dfs_procured_mw`` is the accepted procurement as
    published,
    the archive epoch has no event columns."""
    archive = _load_clean(data, "sa")
    blank = archive.filter(pl.col("delivery_date") == date(2025, 6, 5))
    assert blank["settled_volume_mw"].null_count() == blank.height == 2
    assert blank["settled_cost_gbp"].null_count() == 2
    assert archive.filter(pl.col("settled_volume_mw").is_not_null()).height == 1
    assert archive["event_id"].null_count() == archive.height


def test_current_summary_negative_settlement(data: Path) -> None:
    """Detects a negative settled value bounded, flipped or nulled: Event 75 keeps
    ``settled_volume_mw`` ``-0.03`` and ``settled_cost_gbp`` ``-2.65`` signed, the next row keeps
    its positive values, the 2026-10-06 row with blank settlement stays null, and ``event_id`` is
    the string."""
    frame = _load_clean(data, "sc")
    negative = frame.filter(pl.col("event_id") == "75").sort("window_from")
    assert negative["settled_volume_mw"].to_list() == [-0.03, 12.07]
    assert negative["settled_cost_gbp"].to_list() == [-2.65, 806.42]
    blank = frame.filter(pl.col("event_id") == "142")
    assert blank["settled_volume_mw"].to_list() == [None] and blank[
        "settled_cost_gbp"
    ].to_list() == [None]
    assert frame.schema["event_id"] == pl.String


def test_stor_buy_curve_keeps_every_vertex_the_zeros_and_23_24_25_hour_days(data: Path) -> None:
    """Detects an instant shifted as if it were UK local time, a vertex dropped or a zero read as
    missing: three curves of three vertices each load, the literal-``Z`` cells are UTC (2021-04-01
    04:00Z to 2021-04-02 04:00Z is 24 hours, 2022-03-26 05:00Z to 2022-03-27 04:00Z is 23,
    2021-10-30
    04:00Z to 2021-10-31 05:00Z is 25), a zero ``mw`` (1,825 of the body) and a zero
    ``availability_price`` stay ``0.0`` (never null), ``availability_price`` is the £/MWh column,
    and ``timestamp_utc`` is ``effective_from_date``."""
    frame = _load_clean(data, "st")
    assert frame.height == 9
    spans = {
        (a, b): (b - a).total_seconds() / 3600
        for a, b in zip(frame["effective_from_date"], frame["effective_to_date"], strict=True)
    }
    assert sorted(spans.values()) == [23.0, 24.0, 25.0]
    assert (
        spans[(datetime(2022, 3, 26, 5, 0, tzinfo=UTC), datetime(2022, 3, 27, 4, 0, tzinfo=UTC))]
        == 23.0
    )
    assert (
        spans[(datetime(2021, 10, 30, 4, 0, tzinfo=UTC), datetime(2021, 10, 31, 5, 0, tzinfo=UTC))]
        == 25.0
    )
    assert frame.schema["effective_from_date"] == pl.Datetime("us", "UTC")
    assert frame.schema["effective_to_date"] == pl.Datetime("us", "UTC")
    first = frame.filter(pl.col("buy_curve_id") == "BCN-00000000").sort("buy_curve_step_id")
    assert first["buy_curve_step_id"].to_list() == ["BCS-0000", "BCS-0001", "BCS-0002"]
    assert first["mw"].to_list() == [1311.0, 1311.0, 0.0]
    assert first["availability_price"].to_list() == [0.0, 6.78, 6.78]
    assert frame["timestamp_utc"].to_list() == frame["effective_from_date"].to_list()
    assert frame["buy_curve_step_id"].to_list() == [
        r["Buy Curve Step: Buy Curve Step Id"] for r in rows("st")
    ]


def test_dfs_a_carries_no_join_or_sum_columns() -> None:
    """Detects a derived column riding in silver: the STOR curve has no result-join column (the
    curve joins ``stor_da_auction_results`` on Buy Curve ID in docs only) and the report carries no
    summed regional or coalesced volume column."""
    assert not {"curve_mw_total", "results_join", "contracted_mw"} & set(_columns(STOR))
    assert not {"regional_total", "volume_mw", "procured_or_volume"} & set(_columns(RP))


# --------------------------------------------------------------------------- #
# Loud failure and vintage
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epochs_fails_loud(data: Path, family: str) -> None:
    """Detects a silent header drift (a renamed vendor column loaded under the old typing): a body
    whose first header cell is renamed fails the capture with no completion and no silver."""
    alias = FIRST_OF[family]
    raw = body(alias)
    first_cell = header(alias)[0].encode()
    renamed = raw.replace(first_cell, first_cell + b"_x", 1)
    assert renamed != raw
    _fail(data, alias, raw=renamed)


@pytest.mark.parametrize("family", RECORDED)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, family: str) -> None:
    """Detects a duplicate key loaded silently or deduplicated: the first populated data row
    repeated at the end of the body fails the capture with ``DuplicateEntityKeyError``, loads
    nothing and leaves no completion (the held requirement fails the same way)."""
    alias = FIRST_OF[family]
    lines = body(alias).split(b"\r\n")
    assert lines[-1] == b""
    raw = b"\r\n".join([*lines[:-1], lines[1], b""])
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["qa", "qc", "na", "bc", "sa", "sc", "st"])
def test_the_vintage_is_ckan_last_modified(data: Path, alias: str) -> None:
    """Detects the vintage drifting: ``available_at`` is the CKAN ``last_modified`` read as UTC. An
    as-of read before it serves nothing and one after serves the capture, in the DuckDB view and in
    Polars."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    from datetime import timedelta

    assert both_as_of(db, data, meta.family, vintage - timedelta(microseconds=1)) == []
    assert set(both_as_of(db, data, meta.family, vintage + timedelta(days=1))) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}
