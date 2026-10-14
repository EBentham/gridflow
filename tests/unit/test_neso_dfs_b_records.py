"""Demand Flexibility Service frozen packages: the 2023/24 service, the 2022/23 live events and the
2022/23 test events, twelve frozen records, ten eligible and two held (v0.22-K-DFS-b).

Three package files carry the records: ``demand-flexibility-service``
(``dfs_industry_notification``, ``dfs_service_requirements``, ``dfs_utilisation_report``,
``dfs_utilisation_report_sum``), ``demand-flexibility-service-live-events`` and
``demand-flexibility-service-test-events`` (each with a requirement, a
``..._service_update_industry`` notification, a ``..._utilisation_report`` and a
``..._utilisation_report_sum``). Every test writes recorded fixture captures (cuts of the
2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/dfsb/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation.

Record decisions under test (K-DFS-FACTS, K-DFS-b-SPEC):

- ``issue: none`` everywhere, vintage ``ckan_last_modified`` (every resource is an upload), whole
  capture of the family's one frozen resource (no ``resource_id`` in the key, no
  ``latest_partition``).
- Every window and notification clock label is a raw ``string``; no column of any record is a
  datetime (no recipe composes a date with a clock label, RULINGS 572 / 588).
- The 2023/24 requirement and the 2022/23 test-event requirement are held; the 2022/23 live
  requirement is eligible (every window starts after its upload). Padding, zeros, blank D0 cells and
  negative regional values stay as published. Live and Test are separate records, never unioned.
"""

from __future__ import annotations

import csv
import io
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "dfsb"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
LONDON = ZoneInfo("Europe/London")
UNIT = "v0.22-K-DFS-b"

SERVICE = "demand-flexibility-service"
LIVE = "demand-flexibility-service-live-events"
TEST = "demand-flexibility-service-test-events"
PACKAGE_ID_OF = {
    SERVICE: "e5277fca-2a2b-4836-933b-f24a676b9ed8",
    LIVE: "328b830b-4a8e-4dce-a14b-be91ed926b95",
    TEST: "22bc9bdd-4919-439a-95b8-5213bb3bdf4c",
}

# Fixture alias -> family. The alias is the fixture file stem.
FAMILY_OF = {
    "n23": "dfs_industry_notification",
    "q23": "dfs_service_requirements",
    "b23": "dfs_utilisation_report",
    "s23": "dfs_utilisation_report_sum",
    "ql": "dfs_live_events_service_requirement",
    "nl": "dfs_live_events_service_update_industry",
    "bl": "dfs_live_events_utilisation_report",
    "sl": "dfs_live_events_utilisation_report_sum",
    "qt": "dfs_test_events_service_requirement",
    "nt": "dfs_test_events_service_update_industry",
    "bt": "dfs_test_events_utilisation_report",
    "st": "dfs_test_events_utilisation_report_sum",
}
ALIAS_OF = {family: alias for alias, family in FAMILY_OF.items()}
RECORDED = tuple(FAMILY_OF.values())
HELD = ("dfs_service_requirements", "dfs_test_events_service_requirement")
ELIGIBLE = tuple(f for f in RECORDED if f not in HELD)
PACKAGE_OF = {
    **{FAMILY_OF[a]: SERVICE for a in ("n23", "q23", "b23", "s23")},
    **{FAMILY_OF[a]: LIVE for a in ("ql", "nl", "bl", "sl")},
    **{FAMILY_OF[a]: TEST for a in ("qt", "nt", "bt", "st")},
}

R12 = [
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
]
REG_2324 = [*R12, "North Wales, Merseyside and Cheshire", "South Wales", "Other", "Total"]
REG_LT = [*R12, "North Wales_Merseyside_Cheshire", "South Wales", "Other", "Total"]
REG_D0 = [
    f"D0 {name}"
    for name in [*R12, "North Wales Merseyside and Cheshire", "South Wales", "Other", "Total"]
]
N6 = [
    "Notification Issued Date",
    "Notification Issued Time",
    "Requirement For",
    "Requirement Type",
    "Status",
    "Notification Type",
]
Q8 = [
    "Delivery Date",
    "From",
    "To",
    "DFS Required MW",
    "Service Requirement Type",
    "Guaranteed Acceptance Price GBP per MWh",
    "Despatch Type",
    "Participant Bids Eligible",
]
B26 = [
    "Delivery Date",
    "Registered DFS Participant",
    "DFS Unit ID",
    "DFS Volume MW",
    "From",
    "To",
    "Service Requirement Type",
    "Despatch Time",
    "Utilisation Price GBP per MWh",
    "Status",
    *REG_2324,
]
S9 = [
    "Delivery Date",
    "From",
    "To",
    "Service Requirement Type",
    "DFS Required MW",
    "DFS Procured MW",
    "DFS Provider Bids Accepted Total Cost GBP",
    "Settled Volume MW",
    "Settled Cost GBP",
]
Q4 = ["Delivery Date", "From GMT", "To GMT", "DFS Required MW"]
N4 = ["Date", "Status", "Type", "Time"]
B40 = [
    "Date",
    "DFS Provider",
    "Unit",
    "DFS Volume",
    "From",
    "To",
    "Price",
    "Status",
    *REG_LT,
    *REG_D0,
]
S10 = [
    "Date",
    "From",
    "To",
    "DFS Required",
    "DFS Procured",
    "Bids Accepted Total Cost",
    "D0 DFS Procured",
    "D0 Bids Accepted Total Cost",
    "Settled Volume",
    "Settled Cost",
]
HEADER_OF = {
    "n23": N6,
    "q23": Q8,
    "b23": B26,
    "s23": S9,
    "ql": Q4,
    "nl": N4,
    "bl": B40,
    "sl": S10,
    "qt": Q4,
    "nt": N4,
    "bt": B40,
    "st": S10,
}
HAS_BOM = {"s23", "nl", "sl", "qt", "nt", "bl", "bt", "st"}

# Vendor name -> silver name where the spec's rule (lower-case, runs of non-[a-z0-9] as one `_`) is
# overridden: the window labels (`from` / `to` are SQL words) and the notification clock label.
OVERRIDE = {
    "From": "window_from",
    "From GMT": "window_from",
    "To": "window_to",
    "To GMT": "window_to",
}

FLOAT_OF: dict[str, set[str]] = {
    "q23": {"DFS Required MW", "Guaranteed Acceptance Price GBP per MWh"},
    "b23": {"DFS Volume MW", "Utilisation Price GBP per MWh", *REG_2324},
    "s23": {
        "DFS Required MW",
        "DFS Procured MW",
        "DFS Provider Bids Accepted Total Cost GBP",
        "Settled Volume MW",
        "Settled Cost GBP",
    },
    "ql": {"DFS Required MW"},
    "qt": {"DFS Required MW"},
    "bl": {"DFS Volume", "Price", *REG_LT, *REG_D0},
    "bt": {"DFS Volume", "Price", *REG_LT, *REG_D0},
    "sl": set(S10) - {"Date", "From", "To"},
    "st": set(S10) - {"Date", "From", "To"},
    "n23": set(),
    "nl": set(),
    "nt": set(),
}
DMY = "%d/%m/%Y"
YMD = "%Y-%m-%d"
DATE_OF: dict[str, dict[str, str]] = {
    "n23": {"Notification Issued Date": DMY, "Requirement For": DMY},
    "q23": {"Delivery Date": DMY},
    "b23": {"Delivery Date": DMY},
    "s23": {"Delivery Date": DMY},
    "ql": {"Delivery Date": YMD},
    "qt": {"Delivery Date": YMD},
    "nl": {"Date": YMD},
    "nt": {"Date": YMD},
    "bl": {"Date": DMY},
    "bt": {"Date": DMY},
    "sl": {"Date": DMY},
    "st": {"Date": DMY},
}

KEY_OF = {
    "n23": [
        "notification_issued_date",
        "notification_issued_time",
        "requirement_for",
        "requirement_type",
        "notification_type",
    ],
    "q23": ["delivery_date", "window_from"],
    "b23": ["delivery_date", "window_from", "dfs_unit_id"],
    "s23": ["delivery_date", "window_from"],
    "ql": ["delivery_date", "window_from"],
    "qt": ["delivery_date", "window_from"],
    "nl": ["date", "time_label", "type"],
    "nt": ["date", "time_label", "type"],
    "bl": ["date", "window_from", "unit"],
    "bt": ["date", "window_from", "unit"],
    "sl": ["date", "window_from"],
    "st": ["date", "window_from"],
}
TEMPORAL_INPUT = {
    "n23": "notification_issued_date",
    "q23": "delivery_date",
    "b23": "delivery_date",
    "s23": "delivery_date",
    "ql": "delivery_date",
    "qt": "delivery_date",
    "nl": "date",
    "nt": "date",
    "bl": "date",
    "bt": "date",
    "sl": "date",
    "st": "date",
}

Q_23_QUESTION = (
    "TODO: REQUIREMENT-ISSUE-AVAILABILITY — the 2023/24 DFS service requirement upload "
    "(last_modified 2024-03-14) carries no issue column and publishes 32 delivery windows, 29 of "
    "them starting on or before its last_modified; when was each requirement first published, and "
    "does NESO retain earlier requirement editions?"
)
Q_TEST_QUESTION = (
    "TODO: REQUIREMENT-ISSUE-AVAILABILITY — the 2022/23 DFS test-event service requirement upload "
    "(last_modified 2023-03-27) carries no issue column and publishes 40 delivery windows, 38 of "
    "them starting on or before its last_modified; when was each requirement first published, and "
    "does NESO retain earlier requirement editions?"
)
QUESTION_OF = {
    "dfs_service_requirements": Q_23_QUESTION,
    "dfs_test_events_service_requirement": Q_TEST_QUESTION,
}


def silver_name(source: str, alias: str) -> str:
    """The spec's silver-name rule."""
    if source in OVERRIDE:
        return OVERRIDE[source]
    if source == "Time" and alias in ("nl", "nt"):
        return "time_label"
    return re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")


def kind_of(alias: str, source: str) -> str:
    """The silver kind of a vendor column: ``f`` float64, ``d`` date, ``s`` string."""
    if source in FLOAT_OF[alias]:
        return "f"
    if source in DATE_OF[alias]:
        return "d"
    return "s"


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    resource_id: str
    name: str
    filename: str
    modified: str
    written: str


CAPTURES: dict[str, Capture] = {
    "n23": Capture(
        "94b4d470-a54e-41d7-ae6e-be158db9b9d4",
        "DFS Industry Notification",
        "dfs-industry-notification.csv",
        "2024-03-21T11:54:07.592418",
        "2026-10-08T10:06:22.110416+00:00",
    ),
    "q23": Capture(
        "7914dd99-fe1c-41ba-9989-5784531c58bb",
        "DFS Service Requirements",
        "dfs-service-requirements.csv",
        "2024-03-14T12:00:12.712070",
        "2026-10-08T10:06:40.917544+00:00",
    ),
    "b23": Capture(
        "ed7019b0-32b7-425c-a2fb-5ba9e32733fb",
        "DFS Utilisation Report",
        "dfs-utilisation-report.csv",
        "2024-03-21T14:04:20.246404",
        "2026-10-08T10:06:59.909874+00:00",
    ),
    "s23": Capture(
        "71a9cb04-8935-4e3a-bdd6-17f6fff0a7c6",
        "DFS Utilisation Report Summary",
        "dfs-utilisation-report-summary.csv",
        "2024-04-10T07:56:25.272181",
        "2026-10-08T10:07:05.385628+00:00",
    ),
    "ql": Capture(
        "663f3f82-fec8-4c9a-a837-df5db8690a6f",
        "DFS Service Requirement - LIVE",
        "dfs-service-requirement-live230120231430.csv",
        "2023-01-23T14:59:08.435587",
        "2026-10-08T10:06:26.244788+00:00",
    ),
    "nl": Capture(
        "a59a9d56-af4b-4044-b0c8-6379ce1213f8",
        "Service Update Industry Notifications - LIVE",
        "service-update-industry-notifications-live070320231430.csv",
        "2023-03-07T14:29:05.792893",
        "2026-10-08T10:06:29.757293+00:00",
    ),
    "bl": Capture(
        "4e87244e-2479-4e6e-84c2-698dffaca41f",
        "DFS Utilisation Report - LIVE",
        "dfs-participant-utilisation-report-live2501231600.csv",
        "2026-07-27T14:01:36.453412",
        "2026-10-08T10:06:33.337275+00:00",
    ),
    "sl": Capture(
        "011f1a15-f63f-4a95-b1eb-453e5ffcd176",
        "Utilisation Report Summary - LIVE",
        "utilisation-report-summary-live2501231600-fs.csv",
        "2026-07-27T13:22:45.267471",
        "2026-10-08T10:06:36.903669+00:00",
    ),
    "qt": Capture(
        "000066dc-aa75-4bec-a5a2-9c935b1873d2",
        "DFS Service Requirement - TEST",
        "dfs-service-requirement-test270320231430.csv",
        "2023-03-27T13:29:56.815912",
        "2026-10-08T10:06:45.356846+00:00",
    ),
    "nt": Capture(
        "a2dd7992-32d5-4723-b0f5-273caaecbde1",
        "Service Update Industry Notifications - TEST",
        "service-update-industry-notifications-test270320231430.csv",
        "2023-03-27T13:28:50.783881",
        "2026-10-08T10:06:49.085275+00:00",
    ),
    "bt": Capture(
        "fc466b76-6823-4dd9-8b4a-aab464e0e77b",
        "DFS Utilisation Report - TEST",
        "dfs-participant-utilisation-report-test280320231300.csv",
        "2026-07-27T13:58:49.672458",
        "2026-10-08T10:06:52.682394+00:00",
    ),
    "st": Capture(
        "e4804faf-92be-43d0-a1ec-3e52a50f9666",
        "Utilisation Report Summary - TEST",
        "utilisation-report-summary-test280320231300-fs.csv",
        "2026-07-27T13:46:55.144778",
        "2026-10-08T10:06:56.291242+00:00",
    ),
}


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
        prefix="dfsb", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: the bronze bytes)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed with edge-stripped names (cells verbatim)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text, newline=""))
    names = [n.strip() for n in next(reader)]
    return [dict(zip(names, r, strict=False)) for r in reader if any(r)]


def header(alias: str) -> list[str]:
    """The fixture's header as the production reader returns it (BOM and edge whitespace
    stripped)."""
    first = body(alias).decode("utf-8-sig").split("\r\n")[0]
    return [n.strip() for n in next(csv.reader([first]))]


def capture(data: Path, alias: str, *, raw: bytes | None = None) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    package = PACKAGE_OF[FAMILY_OF[alias]]
    path, _sidecar = write_capture(
        data,
        FAMILY_OF[alias],
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


def _expected(kind: str, fmt: str | None, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank cell is null)."""
    if cell == "":
        return None
    if kind == "f":
        return float(cell)
    if kind == "d":
        assert fmt is not None
        return datetime.strptime(cell, fmt).date()
    return cell


def _assert_cells(alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, date and measure of ``frame`` equals the body cell."""
    source_rows = rows(alias)
    for source in header(alias):
        name = silver_name(source, alias)
        kind = kind_of(alias, source)
        fmt = DATE_OF[alias].get(source)
        assert frame[name].to_list() == [_expected(kind, fmt, r[source]) for r in source_rows], (
            alias,
            source,
        )


def _anchor(day: date) -> datetime:
    """The UTC instant of UK-local midnight on ``day`` (the ``date_sp1`` anchor)."""
    return datetime(day.year, day.month, day.day, tzinfo=LONDON).astimezone(UTC)


def _load_clean(data: Path, *aliases: str) -> pl.DataFrame:
    """Capture and run the aliases of one family: every row written, none excluded, the generic
    output columns, one completion per capture, the temporal column tz-aware UTC."""
    (family,) = {FAMILY_OF[a] for a in aliases}
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
    family = FAMILY_OF[alias]
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
    """Detects a fixture cut that lost what the tests below rely on: the exact vendor headers,
    CRLF terminators on every body, the BOM exactly where the bronze body had one, the physical
    trailing spaces of the Live / Test summary headers, a Test and a Live 2023/24 notification and a
    trailing-space 2023/24 status, a quoted participant list and a zero GAP, the comma-bearing
    2023/24 regional header cell, a ``within-day 2`` despatch category, an Accepted and a Rejected
    bid, three padded Live ``Type`` cells, a quoted-comma padded Test notification, blank D0 cells
    next to present ones, a zero Live price, negative Test ``Other`` / ``D0 Other`` and the
    2023-01-17 17:30 Test summary window."""
    for alias, expected in HEADER_OF.items():
        raw = body(alias)
        raw.decode("utf-8")
        assert raw.startswith(b"\xef\xbb\xbf") == (alias in HAS_BOM), alias
        assert raw.endswith(b"\r\n") and raw.count(b"\r\n") == raw.count(b"\n"), alias
        assert header(alias) == expected, alias
    assert [len(h) for h in (N6, Q8, B26, S9, Q4, N4, B40, S10)] == [6, 8, 26, 9, 4, 4, 40, 10]
    assert b"DFS Procured ," in body("st") and b"Settled Cost \r\n" in body("st")
    assert b"Settled Cost \r\n" in body("sl") and b"DFS Procured ," not in body("sl")
    assert b"\xef\xbb\xbfNotification" not in body("n23")
    assert {r["Requirement Type"] for r in rows("n23")} == {"Test", "Live"}
    assert any(r["Status"] != r["Status"].strip() for r in rows("n23"))
    assert b'"' in body("q23") and {
        r["Guaranteed Acceptance Price GBP per MWh"] for r in rows("q23")
    } == {"0"}
    assert {r["Service Requirement Type"] for r in rows("q23")} == {"Test", "Live"}
    assert b'"North Wales, Merseyside and Cheshire"' in body("b23")
    assert "within-day 2" in {r["Despatch Time"] for r in rows("b23")}
    assert {r["Status"] for r in rows("b23")} == {"Accepted", "Rejected"}
    assert sum(r["Type"] != r["Type"].strip() for r in rows("nl")) == 3
    assert any(r["Status"] != r["Status"].strip() for r in rows("nt"))
    assert any("," in r["Status"] and r["Type"] != r["Type"].strip() for r in rows("nt"))
    for alias in ("bl", "bt"):
        d0 = [r["D0 Total"] for r in rows(alias)]
        assert "" in d0 and any(c != "" for c in d0), alias
    assert any(r["Price"] == "0" and r["Status"] == "Rejected" for r in rows("bl"))
    assert any(r["Other"].startswith("-") for r in rows("bt"))
    assert any(r["D0 Other"].startswith("-") for r in rows("bt"))
    assert any(r["Date"] == "17/01/2023" and r["From"] == "17:30" for r in rows("st"))


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_whole_capture_vintage_and_no_issue(
    key: str,
) -> None:
    """Detects a record that invents an issue time (no issue column exists and the Live / Test
    filename tokens do not identify every row's issue), a ``key_latest`` selection, a
    ``resource_id`` key or ``latest_partition`` on a one-resource family, a ``run_type`` or the
    wrong vintage: every record is version 1, utf-8 CSV, whole-capture selection, the spec's entity
    key, ``ckan_last_modified``, with the vendor header as its one epoch and ``issue: none``."""
    alias = ALIAS_OF[key]
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.latest == "whole_capture" and record.latest_partition is None
    assert record.vintage == "ckan_last_modified" and record.vintage_evidence is None
    assert list(record.entity_key) == KEY_OF[alias]
    assert "resource_id" not in record.entity_key
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None
    assert [list(e.header) for e in record.epochs] == [HEADER_OF[alias]]
    (epoch,) = record.epochs
    assert epoch.issue.kind == "none" and epoch.unpivot is None
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)


def test_each_epoch_header_is_read_from_the_body_not_retyped() -> None:
    """Detects an epoch whose header was retyped rather than read from the body: the first record of
    every fixture, BOM and edge whitespace stripped as the production reader returns it (the
    Live / Test summaries' physical trailing spaces), equals the frozen epoch; the column counts are
    6, 8, 26, 9, 4, 4, 40, 10."""
    for alias, key in FAMILY_OF.items():
        assert header(alias) == list(_record(key).epochs[0].header), alias
    counts = {a: len(_record(f).epochs[0].header) for a, f in FAMILY_OF.items()}
    assert counts == {
        "n23": 6,
        "q23": 8,
        "b23": 26,
        "s23": 9,
        "ql": 4,
        "nl": 4,
        "bl": 40,
        "sl": 10,
        "qt": 4,
        "nt": 4,
        "bt": 40,
        "st": 10,
    }
    assert header("bl") == header("bt") and header("ql") == header("qt")
    assert header("nl") == header("nt") and header("sl") == header("st")
    assert (
        "North Wales, Merseyside and Cheshire" in _record("dfs_utilisation_report").epochs[0].header
    )


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: measures are
    nullable float64 (zero is a value), identifiers, codes, categories, free text and every window /
    notification clock label are strings (``Despatch Time`` is a category such as ``within-day 2``,
    never a time), dates carry their body format (2023/24 ``%d/%m/%Y``; Live / Test requirement and
    notification ``%Y-%m-%d``, report and summary ``%d/%m/%Y``) with no zone, key columns and the
    temporal input are ``nullable: false`` and nothing else is, and no column carries a bound."""
    alias = ALIAS_OF[key]
    non_null = set(KEY_OF[alias]) | {TEMPORAL_INPUT[alias]}
    (epoch,) = _record(key).epochs
    assert [c.source for c in epoch.columns] == HEADER_OF[alias]
    for column in epoch.columns:
        name = silver_name(column.source, alias)
        kind = kind_of(alias, column.source)
        assert column.name == name
        assert column.trim is None and column.null_tokens == ()
        assert column.formats_by_filename is None
        assert (column.min, column.max) == (None, None), name
        assert column.zone is None and column.zone_evidence is None and column.ambiguous is None
        assert column.nullable == (name not in non_null), name
        if kind == "d":
            assert (column.dtype, column.format) == ("date", DATE_OF[alias][column.source])
        else:
            assert column.format is None
            assert column.dtype == {"f": "float64", "s": "string"}[kind], name


def test_the_spec_silver_names_and_window_labels() -> None:
    """Detects a renamed silver column: ``From`` / ``From GMT`` share ``window_from`` and ``To`` /
    ``To GMT`` share ``window_to`` (``from`` / ``to`` are SQL words), the Live / Test notification
    ``Time`` is ``time_label``, the comma-bearing 2023/24 regional cell is the one silver column
    ``north_wales_merseyside_and_cheshire``, the Live / Test report spells ``north_wales_merseyside
    _cheshire`` and the 16 ``d0_`` counterparts, and the summaries keep ``d0_dfs_procured``."""
    names = {k: [c.name for c in _record(k).epochs[0].columns] for k in RECORDED}
    for key in RECORDED:
        assert not {"from", "to"} & set(names[key]), key
        assert len(set(names[key])) == len(names[key]), key
    for alias in ("q23", "b23", "s23", "ql", "qt", "bl", "bt", "sl", "st"):
        assert {"window_from", "window_to"} <= set(names[FAMILY_OF[alias]]), alias
    assert "north_wales_merseyside_and_cheshire" in names[FAMILY_OF["b23"]]
    assert "," not in "".join(names[FAMILY_OF["b23"]])
    for alias in ("bl", "bt"):
        cols = names[FAMILY_OF[alias]]
        assert "north_wales_merseyside_cheshire" in cols
        assert "d0_north_wales_merseyside_and_cheshire" in cols
        assert sum(c.startswith("d0_") for c in cols) == 16
        assert {"dfs_provider", "unit", "dfs_volume", "price"} <= set(cols)
    for alias in ("nl", "nt"):
        assert names[FAMILY_OF[alias]] == ["date", "status", "type", "time_label"]
    for alias in ("sl", "st"):
        assert {"dfs_required", "dfs_procured", "d0_dfs_procured", "settled_cost"} <= set(
            names[FAMILY_OF[alias]]
        )
    assert "despatch_time" in names[FAMILY_OF["b23"]]
    assert "despatch_type" in names[FAMILY_OF["q23"]]


def test_no_window_or_notification_clock_is_typed() -> None:
    """Detects a typed clock label (RULINGS 572 / 588: the engine has no date + clock-label recipe,
    FACTS l.785): ``window_from``, ``window_to``, ``notification_issued_time``, ``time_label`` and
    ``despatch_time`` are ``string``, and no column of any of the twelve records is a datetime."""
    typed: list[tuple[str, str]] = []
    for key in RECORDED:
        (epoch,) = _record(key).epochs
        types = {c.name: c.dtype for c in epoch.columns}
        for name in (
            "window_from",
            "window_to",
            "notification_issued_time",
            "time_label",
            "despatch_time",
        ):
            assert types.get(name, "string") == "string", (key, name)
        typed += [(key, c.name) for c in epoch.columns if c.dtype == "datetime"]
    assert typed == []


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_spec_recipe(key: str) -> None:
    """Detects a wrong anchor: every record takes ``date_sp1`` of its service date (the
    notifications of their posting date, not a window start or the requirement date)."""
    recipe = _record(key).temporal
    assert recipe.kind == "date_sp1"
    assert recipe.date_column == TEMPORAL_INPUT[ALIAS_OF[key]]
    assert recipe.column is None


def test_the_designated_date_names_are_registered_as_dates() -> None:
    """Detects a ``date_sp1`` input without an SQL type (the manifest requires every designated date
    to have one): ``delivery_date``, ``notification_issued_date`` and ``date`` are ``DATE`` in the
    shared map."""
    for name in ("delivery_date", "notification_issued_date", "date"):
        assert DATE_COL_SQL_TYPES[name] == "DATE", name


def test_every_dfs_b_resource_is_an_upload_with_one_resource_per_family() -> None:
    """Detects the V-8 premise drifting (``ckan_last_modified`` is the vintage because every
    resource is an upload) or a second resource arriving unrecorded: each package holds exactly
    four resources, one per family, with the captured resource ids."""
    for package in (SERVICE, LIVE, TEST):
        document = _package_doc(package)
        assert document["package_id"] == PACKAGE_ID_OF[package]
        assert {r["url_type"] for r in document["resources"]} == {"upload"}
        assert len(document["resources"]) == 4
        assert {r["family"] for r in document["resources"]} == {
            f for f, p in PACKAGE_OF.items() if p == package
        }
    for alias, meta in CAPTURES.items():
        package = PACKAGE_OF[FAMILY_OF[alias]]
        (resource,) = [r for r in _package_doc(package)["resources"] if r["id"] == meta.resource_id]
        assert resource["family"] == FAMILY_OF[alias]
        assert resource["name"] == meta.name


# --------------------------------------------------------------------------- #
# Eligibility, dispositions and the ledger
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_ten_eligible_records_carry_no_override(key: str) -> None:
    """Detects a hold or a per-record override on a clean family (the 2022/23 live requirement
    included: all five windows start after its upload): each is effectively eligible, in an eligible
    package."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", HELD)
def test_the_two_requirements_are_held_with_the_verbatim_question_and_the_unit(key: str) -> None:
    """Detects a hold dropped, reworded, filed under the wrong unit, or a package-wide hold: the
    2023/24 and the 2022/23 test-event requirements are ``held`` with the spec's question verbatim
    and unit ``v0.22-K-DFS-b``, effectively held, while their package stays eligible."""
    package, family = registry_module.load_registry().families[key]
    eligibility = _record(key).eligibility
    assert eligibility == Held(status="held", question=QUESTION_OF[key], unit=UNIT)
    assert effective_eligibility(package, family) == eligibility
    assert package.eligibility == Eligible(status="eligible")
    assert "\n" not in eligibility.question


def test_the_three_packages_stay_eligible_and_hold_exactly_the_twelve_records() -> None:
    """Detects a package held by accident, a record on the wrong family, a family gaining a
    transformer or the live requirement held: all three package files are ``eligible``, hold
    exactly the twelve records (10 eligible, 2 held) and no family has a bespoke transformer."""
    found: set[str] = set()
    held: set[str] = set()
    for package in (SERVICE, LIVE, TEST):
        document = _package_doc(package)
        assert document["eligibility"] == {"status": "eligible"}, package
        for family in document["families"]:
            assert family["transformer"] is None, family["key"]
            if "record" in family:
                found.add(family["key"])
                if "eligibility" in family["record"]:
                    held.add(family["key"])
    assert found == set(RECORDED)
    assert held == set(HELD)
    assert len(ELIGIBLE) == 10


def test_every_resource_disposition_is_unchanged() -> None:
    """Detects a changed disposition: every resource of the three packages stays ``SILVER`` to its
    own family (nothing becomes ``DOC`` or ``HOLD``)."""
    for package in (SERVICE, LIVE, TEST):
        for resource in _package_doc(package)["resources"]:
            assert resource["disposition"] == {"kind": "SILVER", "key": resource["family"]}


def _report_line(key: str) -> str:
    lines = eligibility_module.render(load_registry()).splitlines()
    (line,) = [ln for ln in lines if f"| `{key}` |" in ln]
    return line


def test_the_eligibility_report_lists_ten_eligible_and_two_held() -> None:
    """Detects a family reported with the wrong status: each eligible family is ``silver`` /
    ``eligible`` with the CKAN last-modified vintage and a whole-capture selection (not per
    resource), the two requirements are ``silver`` / ``held: TODO: REQUIREMENT-ISSUE-AVAILABILITY``
    with the unit."""
    for key in ELIGIBLE:
        line = _report_line(key)
        assert "| silver | eligible |" in line, key
        assert "date_sp1" in line and "per resource_id" not in line, key
    for key in HELD:
        line = _report_line(key)
        assert "| silver | held: TODO: REQUIREMENT-ISSUE-AVAILABILITY" in line, key
        assert f"(unit {UNIT})" in line, key


def test_the_reconcile_ledger_is_untouched_by_dfs_b() -> None:
    """Detects an ADR-040 entry riding with this unit (every body's header, casts and key pass, so
    none is recorded) or a ledger the registry no longer backs: the ledger validates against the
    registry and names none of the twelve families."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    assert {e.family for e in entries}.isdisjoint(RECORDED)


@pytest.mark.parametrize("package", [SERVICE, LIVE, TEST])
def test_the_skeleton_page_renders_the_new_records(package: str) -> None:
    """Detects a record the docs generator cannot render (the held requirements, the date-keyed
    records without a resource partition)."""
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
    for family, owner in PACKAGE_OF.items():
        if owner == package:
            assert f"`{family}`" in page


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", sorted(FAMILY_OF))
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a
    duplicate key or an excluded row: the slice loads every populated row with zero exclusions and
    no failure, every identifier, label (padding kept), date and measure equals the body cell, the
    entity key is unique and non-null, and ``timestamp_utc`` is the ``date_sp1`` anchor of the
    service / posting date (UK-local midnight, not a window start). A cell failing its strict cast
    (a date or any ``float64``) would fail here."""
    family = FAMILY_OF[alias]
    frame = _load_clean(data, alias)
    _assert_cells(alias, frame)
    assert "resource_id" not in frame.columns
    keys = KEY_OF[alias]
    assert not frame.select(keys).is_duplicated().any()
    for name in keys:
        assert frame[name].null_count() == 0, name
    column = TEMPORAL_INPUT[alias]
    assert frame["timestamp_utc"].to_list() == [_anchor(d) for d in frame[column].to_list()]
    assert frame["event_time"].to_list() == frame["timestamp_utc"].to_list()
    assert family in RECORDED


def test_the_2023_notification_keeps_test_and_live_and_the_padded_status(data: Path) -> None:
    """Detects the status used as identity, a trimmed status, a composed issue time or the Test and
    Live rows collapsed: a Test and a Live ``requirement_type`` both load, the trailing-space
    ``Anticipated Requirement Notice`` status stays byte-equal (V-19: no trim on a key family),
    ``notification_issued_time`` stays the raw ``string``, ``requirement_for`` and the posting date
    are dates and the posting date differs from the requirement date for a Live row (posted the day
    before delivery)."""
    frame = _load_clean(data, "n23")
    assert sorted(set(frame["requirement_type"].to_list())) == ["Live", "Test"]
    padded = [s for s in frame["status"].to_list() if s != s.strip()]
    assert padded and padded == [r["Status"] for r in rows("n23") if r["Status"].endswith(" ")]
    assert frame.schema["notification_issued_time"] == pl.String
    assert frame["notification_issued_time"].to_list() == [
        r["Notification Issued Time"] for r in rows("n23")
    ]
    assert frame.schema["requirement_for"] == pl.Date
    assert frame.schema["notification_issued_date"] == pl.Date
    live = frame.filter(pl.col("requirement_type") == "Live")
    assert (live["notification_issued_date"] != live["requirement_for"]).all()
    assert "event_id" not in frame.columns and "issue_time" not in frame.columns


def test_the_2023_requirement_keeps_zero_gap_the_quoted_list_and_both_types(data: Path) -> None:
    """Detects a zero GAP read as missing, a quoted participant list split or the Test and Live rows
    mixed: ``guaranteed_acceptance_price_gbp_per_mwh`` is ``0.0`` (never null) on every row, the
    comma-separated ``participant_bids_eligible`` is whole and byte-equal, ``window_from`` is a
    string, ``service_requirement_type`` keeps ``Test`` and ``Live`` as published, and
    ``despatch_type`` is the vendor label."""
    frame = _load_clean(data, "q23")
    assert set(frame["guaranteed_acceptance_price_gbp_per_mwh"].to_list()) == {0.0}
    assert frame.schema["window_from"] == pl.String
    assert frame["participant_bids_eligible"].to_list() == [
        r["Participant Bids Eligible"] for r in rows("q23")
    ]
    assert all("," in c for c in frame["participant_bids_eligible"].to_list())
    assert sorted(set(frame["service_requirement_type"].to_list())) == ["Live", "Test"]
    assert set(frame["despatch_type"].to_list()) == {"All Providers"}
    assert frame.schema["dfs_required_mw"] == pl.Float64


def test_the_2023_report_keeps_the_comma_header_as_one_column_and_the_despatch_category(
    data: Path,
) -> None:
    """Detects the comma-bearing regional header split into two columns or a despatch category read
    as a time: ``North Wales, Merseyside and Cheshire`` is the single float64 column
    ``north_wales_merseyside_and_cheshire``, ``despatch_time`` keeps ``within-day 2`` /
    ``within-day 1`` / ``day-ahead`` as strings, Accepted and Rejected bids both load, a bid of
    10 MW tendered keeps ``total`` 10.0 with ``other`` 10.0 (regional columns are never summed into
    the bid volume) and ``dfs_unit_id`` stays byte-equal."""
    frame = _load_clean(data, "b23")
    assert frame.schema["north_wales_merseyside_and_cheshire"] == pl.Float64
    assert frame.schema["despatch_time"] == pl.String
    assert set(frame["despatch_time"].to_list()) == {"within-day 2", "within-day 1", "day-ahead"}
    assert sorted(set(frame["status"].to_list())) == ["Accepted", "Rejected"]
    for region in REG_2324:
        assert frame.schema[silver_name(region, "b23")] == pl.Float64, region
    first = frame.row(0, named=True)
    assert (first["total"], first["other"], first["dfs_volume_mw"]) == (10.0, 10.0, 10.0)
    assert frame["dfs_unit_id"].to_list() == [r["DFS Unit ID"] for r in rows("b23")]
    assert frame["service_requirement_type"].to_list().count("Live") == 1


def test_the_2023_summary_keeps_published_values(data: Path) -> None:
    """Detects a recalculated or rescaled summary: a window keeps the published ``dfs_required_mw``
    75, ``dfs_procured_mw`` 80, accepted cost 26372, settled volume 71 and settled cost 24363 (the
    summary is never recomputed from, or added to, the report's procurement)."""
    frame = _load_clean(data, "s23")
    first = frame.row(0, named=True)
    assert (first["window_from"], first["window_to"]) == ("17:30", "18:00")
    assert first["dfs_required_mw"] == 75.0 and first["dfs_procured_mw"] == 80.0
    assert first["dfs_provider_bids_accepted_total_cost_gbp"] == 26372.0
    assert (first["settled_volume_mw"], first["settled_cost_gbp"]) == (71.0, 24363.0)


def test_the_live_requirement_loads_forward_windows_with_string_clocks(data: Path) -> None:
    """Detects a typed clock, a dropped window or a Delivery Date read with the wrong format: the
    five Live windows load (delivery dates 2023-01-23 and 2023-01-24, ``%Y-%m-%d``), every one
    after the upload's last_modified 2023-01-23T14:59:08 (the forward-target exception that keeps
    the record eligible), ``window_from`` and ``window_to`` stay strings and ``dfs_required_mw`` is
    a float."""
    frame = _load_clean(data, "ql")
    assert sorted(set(frame["delivery_date"].to_list())) == [date(2023, 1, 23), date(2023, 1, 24)]
    vintage = datetime.fromisoformat(CAPTURES["ql"].modified)
    for d, start in zip(frame["delivery_date"], frame["window_from"], strict=True):
        hour, minute = map(int, start.split(":"))
        assert datetime(d.year, d.month, d.day, hour, minute) > vintage
    assert frame.schema["window_from"] == pl.String and frame.schema["window_to"] == pl.String
    assert frame.schema["dfs_required_mw"] == pl.Float64


def test_the_live_notification_keeps_padded_types_as_published_keys(data: Path) -> None:
    """Detects a trimmed ``type`` or a notification keyed on the status: the three
    ``Anticipated Requirement Notice `` types keep their trailing space byte-equal (padding is part
    of the published key value), the seven notifications over four posting dates all load, and
    ``time_label`` is the raw GMT schedule string."""
    frame = _load_clean(data, "nl")
    assert frame["type"].to_list() == [r["Type"] for r in rows("nl")]
    assert sum(t.endswith(" ") for t in frame["type"].to_list()) == 3
    assert frame["status"].to_list() == [r["Status"] for r in rows("nl")]
    assert frame["date"].n_unique() == 4
    assert set(frame["time_label"].to_list()) == {"14:30", "10:00"}
    assert frame.schema["time_label"] == pl.String and frame.schema["date"] == pl.Date


def test_the_live_report_keeps_blank_d0_null_and_zero_price_and_regions_zero(data: Path) -> None:
    """Detects a blank D0 cell read as zero, a zero read as missing or the D0 set merged into the
    D-1 set: the two Rejected rows keep all sixteen ``d0_`` columns null while their D-1 regional
    columns and ``price`` stay ``0.0`` (never null), the Accepted rows carry D0 values,
    ``dfs_volume`` is the D-1 tendered volume and ``d0_total`` a separate column."""
    frame = _load_clean(data, "bl")
    d0 = [c for c in frame.columns if c.startswith("d0_")]
    assert len(d0) == 16
    rejected = frame.filter(pl.col("status") == "Rejected")
    accepted = frame.filter(pl.col("status") == "Accepted")
    assert rejected.height == 2 and accepted.height == 2
    for column in d0:
        assert rejected[column].null_count() == rejected.height, column
        assert accepted[column].null_count() == 0, column
    assert set(rejected["price"].to_list()) == {0.0}
    assert set(rejected["north_wales_merseyside_cheshire"].to_list()) == {0.0}
    assert set(rejected["total"].to_list()) == {0.0}
    assert accepted["total"].to_list() == [12.0, 12.0]
    assert frame.schema["dfs_volume"] == pl.Float64 and frame.schema["price"] == pl.Float64


def test_the_live_summary_keeps_the_d0_and_settlement_values_as_published(data: Path) -> None:
    """Detects a summary reconciled to the report or its D0 columns dropped: the 2023-01-23
    17:00 window keeps ``dfs_procured`` 323.81, ``d0_dfs_procured`` 224.66 (not equal), settled
    volume 271.73 and settled cost 539476."""
    frame = _load_clean(data, "sl")
    row = frame.filter(
        (pl.col("date") == date(2023, 1, 23)) & (pl.col("window_from") == "17:00")
    ).row(0, named=True)
    assert row["dfs_procured"] == 323.81 and row["d0_dfs_procured"] == 224.66
    assert (row["settled_volume"], row["settled_cost"]) == (271.73, 539476.0)
    assert row["d0_bids_accepted_total_cost"] == 405849.0


def test_the_test_requirement_loads_a_window_after_its_upload_date(data: Path) -> None:
    """Detects the held test-event requirement failing to load (a hold is not a load failure): the
    four windows load with ``%Y-%m-%d`` dates, string clocks and float MW; the 2023-03-28 windows
    start after the upload's last_modified 2023-03-27 (2 of 40 do, hence the 38-of-40 hold)."""
    frame = _load_clean(data, "qt")
    assert sorted(set(frame["delivery_date"].to_list())) == [date(2022, 11, 15), date(2023, 3, 28)]
    assert frame["dfs_required_mw"].to_list() == [25.0, 25.0, 200.0, 200.0]
    assert frame.schema["window_from"] == pl.String


def test_the_test_notification_keeps_padding_the_quoted_comma_and_same_day_pairs(
    data: Path,
) -> None:
    """Detects a trimmed status or type, a quoted comma split, or the Test and Live cancellation
    day collapsed: the padded ``Test DFS Requirement not issued `` type stays byte-equal, the
    quoted-comma status stays whole, the two 2023-01-22 notifications (the Test cancellation and the
    anticipated notice) are distinct keys, and padded status cells stay padded."""
    frame = _load_clean(data, "nt")
    assert frame["status"].to_list() == [r["Status"] for r in rows("nt")]
    assert frame["type"].to_list() == [r["Type"] for r in rows("nt")]
    assert "Test DFS Requirement not issued " in frame["type"].to_list()
    assert any("," in s for s in frame["status"].to_list())
    assert any(s != s.strip() for s in frame["status"].to_list())
    same_day = frame.filter(pl.col("date") == date(2023, 1, 22))
    assert same_day.height == 2 and same_day.select(KEY_OF["nt"]).n_unique() == 2


def test_the_test_report_keeps_negative_regional_values_signed_and_blank_d0_null(
    data: Path,
) -> None:
    """Detects a clamp, a flipped sign or a blank D0 read as zero: SMS-01 keeps ``other``
    -0.093121335 and ``d0_other`` -0.093121337 (2023-02-21 18:00) and -0.271 / -0.277 on
    2023-01-19, signed, the CUB-02 Rejected rows keep every D0 cell null and ``total`` 2.0, and the
    regional columns are not summed into ``total``."""
    frame = _load_clean(data, "bt")
    sms = frame.filter(pl.col("unit") == "SMS-01").sort(["date", "window_from"])
    assert sms["other"].to_list() == [-0.271, -0.277, -0.093121335]
    assert sms["d0_other"].to_list() == [-0.271, -0.277, -0.093121337]
    cub = frame.filter(pl.col("unit") == "CUB-02")
    assert cub.height == 2
    assert all(cub[c].null_count() == 2 for c in frame.columns if c.startswith("d0_"))
    assert cub["total"].to_list() == [2.0, 2.0]


def test_the_test_summary_keeps_the_2023_01_17_window(data: Path) -> None:
    """Detects a recalculated summary or a mis-keyed window: the 17/01/2023 17:30 window keeps
    ``dfs_required`` 100, ``dfs_procured`` 55.89, accepted cost 83835, settled volume 50.66 and
    settled cost 75997, and the two windows of the same day are distinct keys."""
    frame = _load_clean(data, "st")
    day = frame.filter(pl.col("date") == date(2023, 1, 17)).sort("window_from")
    assert day["window_from"].to_list() == ["17:30", "18:00"]
    row = day.row(0, named=True)
    assert (row["dfs_required"], row["dfs_procured"]) == (100.0, 55.89)
    assert row["bids_accepted_total_cost"] == 83835.0
    assert (row["settled_volume"], row["settled_cost"]) == (50.66, 75997.0)


def test_live_and_test_records_never_share_a_silver_dataset(data: Path) -> None:
    """Detects a Live and Test union (their headers are equal and their events disjoint): the Live
    and Test reports have equal epoch headers yet distinct family keys, loading both leaves each
    silver dataset with only its own rows and no shared delivery date in the slices, and the
    2023/24 table is a third dataset that is never joined to either."""
    assert list(_record("dfs_live_events_utilisation_report").epochs[0].header) == list(
        _record("dfs_test_events_utilisation_report").epochs[0].header
    )
    assert FAMILY_OF["bl"] != FAMILY_OF["bt"]
    live = _load_clean(data, "bl")
    test = _load_clean(data, "bt")
    assert live.height == len(rows("bl")) and test.height == len(rows("bt"))
    assert set(live["date"].to_list()).isdisjoint(test["date"].to_list())
    live_dir = data / "silver" / SOURCE / FAMILY_OF["bl"]
    test_dir = data / "silver" / SOURCE / FAMILY_OF["bt"]
    assert live_dir != test_dir and live_dir.is_dir() and test_dir.is_dir()
    assert {PACKAGE_OF[FAMILY_OF[a]] for a in ("bl", "bt", "b23")} == {SERVICE, LIVE, TEST}


def test_dfs_b_carries_no_join_sum_or_composed_columns() -> None:
    """Detects a derived column riding in silver: no summed regional total, no coalesced volume, no
    composed window instant, no event id (none of these packages has one) and no Live + Test or
    summary + report procurement column."""
    banned = {
        "regional_total",
        "volume_mw",
        "procured_or_volume",
        "window_start",
        "window_start_utc",
        "event_id",
        "issue_time",
        "procured_total",
    }
    for key in RECORDED:
        assert not banned & set(_columns(key)), key


# --------------------------------------------------------------------------- #
# Loud failure and vintage
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", sorted(FAMILY_OF))
def test_a_header_outside_the_epoch_fails_loud(data: Path, alias: str) -> None:
    """Detects a silent header drift (a renamed vendor column loaded under the old typing): a body
    whose first header cell is renamed fails the capture with no completion and no silver."""
    raw = body(alias)
    first_cell = header(alias)[0].encode()
    renamed = raw.replace(first_cell, first_cell + b"_x", 1)
    assert renamed != raw
    _fail(data, alias, raw=renamed)


@pytest.mark.parametrize("alias", sorted(FAMILY_OF))
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, alias: str) -> None:
    """Detects a duplicate key loaded silently or deduplicated: the first populated data row
    repeated at the end of the body fails the capture with ``DuplicateEntityKeyError``, loads
    nothing and leaves no completion (the held requirements fail the same way)."""
    family = FAMILY_OF[alias]
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


def test_the_padded_notification_type_is_a_distinct_key_not_a_duplicate(data: Path) -> None:
    """Detects a key comparison that trims: a body whose first Live notification is repeated with
    its ``Type`` trailing space removed is a different published key and loads (padding is kept
    byte-equal, so the padded and the unpadded type are two rows), not a duplicate."""
    lines = body("nl").split(b"\r\n")
    second = lines[2]
    assert b"Notice ,10:00" in second
    unpadded = second.replace(b"Notice ,10:00", b"Notice,10:00")
    raw = b"\r\n".join([*lines[:-1], unpadded, b""])
    capture(data, "nl", raw=raw)
    written = get_transformer(SOURCE, FAMILY_OF["nl"], data).run(DAY, run_id="r")
    assert written == len(rows("nl")) + 1
    assert _silver(data, FAMILY_OF["nl"]).select(KEY_OF["nl"]).n_unique() == written


@pytest.mark.parametrize("alias", sorted(FAMILY_OF))
def test_the_vintage_is_ckan_last_modified(data: Path, alias: str) -> None:
    """Detects the vintage drifting: ``available_at`` is the CKAN ``last_modified`` read as UTC. An
    as-of read before it serves nothing and one after serves the capture, in the DuckDB view and in
    Polars."""
    family = FAMILY_OF[alias]
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    frame = _silver(data, family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, family, vintage - timedelta(microseconds=1)) == []
    assert set(both_as_of(db, data, family, vintage + timedelta(days=1))) == {capture_id}
    assert set(both_as_of(db, data, family, None)) == {capture_id}
