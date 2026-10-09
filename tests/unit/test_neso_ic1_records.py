"""The interconnector limits batch 1 frozen records (v0.22-K-IC-1): eleven held families.

Every test writes recorded fixture captures (slices of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/ic1/``) into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here,
not at activation. On master none of the eleven families has a record, so ``get_transformer``
raises for each of them.

Units (K-IC-1-FACTS g4; a ``ColumnSpec`` has no unit field, so they are recorded here): every
``Flow ... To GB`` / ``From GB`` column is a directional **maximum transfer limit in MW**
(import = to GB, export = from GB; never signed or netted; zeros are real zeros). Auction type,
reasons, ``Version``, ``Operational Date`` and the hourly labels are text with no unit.

Silver names (one name per meaning across the eleven; the headers differ by epoch):
``Flow ... To/From GB`` -> ``flow_to_gb_mw`` / ``flow_from_gb_mw``; the M reasons keep their
direction (``reason_for_restriction_to_gb`` / ``_from_gb``); the A, W and O reason column is one
undirected reason (``Reason For Reduction`` / ``Reason for restriction``) ->
``reason_for_restriction``; ``Hourly Time Period (GMT)`` -> ``hourly_time_period``; the W
combined label -> ``operational_date_and_hour``.

Epochs: M = the current dump header, A = the daily archive header, W = the weekly archive's
combined-label header, O = the NemoLink one-off header (K-IC-1-FACTS §1).

Fixture cut (a scratch script, not committed): the header, the first 60-150 rows, the first
rows with a zero limit in each direction, a blank reason, a non-default reason, one row per
auction spelling and (M) a target before its upload time; B4 also keeps every ``20241027`` row
(25, with the ``(a)``/``(b)`` repeated hour) and ``20221030``; B12 keeps the whole repeated
2023-10-29 pair (bronze file lines 7,825 and 7,849) with two neighbours each side; B13 keeps
``20240331``; B9 is whole. ``git`` normalises a committed fixture's line endings, so
:func:`body` rebuilds each bronze original's convention (only B4 is CRLF).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import install_generated, write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings, run_cli

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import RECONCILE_ADJUDICATIONS_FILE, Held
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.casting import DuplicateEntityKeyError
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "ic1"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DAY = date(2026, 10, 8)
UPLOAD_FORMAT = "%Y-%m-%dT%H:%M:%S"

HEADER_M = [
    "Data Upload Time GMT",
    "Auction Type",
    "Operational Period Start Date and Time GMT",
    "Flow in MW To GB",
    "Reason For Restriction To GB",
    "Flow in MW From GB",
    "Reason For Restriction From GB",
]
HEADER_A = [
    "Operational Date",
    "Auction Type",
    "Version",
    "Hourly Time Period (GMT)",
    "Flow (MW) To GB",
    "Flow (MW) From GB",
    "Reason For Reduction",
]
HEADER_W = [
    "Operational Date (YYYY-MM-DD) & Time GMT/BST (HH:MM - HH:MM)",
    "Flow (MW) to GB",
    "Flow (MW) from GB",
    "Reason for restriction",
]
HEADER_W_NEMO = [*HEADER_W[:3], "Reason For Restriction"]
HEADER_O = [
    "Hourly Time Period (GMT)",
    "Flow (MW) To GB",
    "Flow (MW) From GB",
    "Reason For Reduction",
]

PAST_TARGETS = (
    "the dump returns past targets (rows before the capture) and NESO does not state that a "
    "row is unchanged since its `Data Upload Time GMT`, so historical issued limits are not "
    "evidenced"
)
HOLD_A = f"TODO: {PAST_TARGETS}."
HOLD_B = (
    f"TODO: {PAST_TARGETS}, and the archive's Operational Date calendar / rollover and "
    "Version meaning are undocumented."
)
HOLD_C = (
    "TODO: the archive's operational-date rollover and GMT/BST interpretation are undocumented, "
    "and every target precedes the upload's last_modified, so the forward-target rule "
    "(RULINGS 529) cannot apply."
)
HOLD_D = (
    "TODO: the archive's operational-date rollover, GMT/BST interpretation and `(a)`/`(b)` "
    "repeated-hour labels are undocumented, and every target precedes the upload's "
    "last_modified, so the forward-target rule (RULINGS 529) cannot apply."
)
HOLD_E = (
    "TODO: the body has no date, and the resource's lifecycle and the date in its name are "
    "undocumented."
)

A_KEY = ("issue_time", "auction_type", "operational_period_start_gmt")
B_KEY = (
    "resource_id",
    "issue_time",
    "auction_type",
    "operational_period_start_gmt",
    "operational_date",
    "hourly_time_period",
)
ARCHIVE_KEY = ("operational_date", "auction_type", "hourly_time_period")


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture (B1-B14) and its sidecar provenance."""

    fixture: str
    family: str
    package: str
    package_id: str
    resource_id: str
    name: str
    filename: str
    modified: str
    url_type: str
    written: str


CAPTURES: dict[str, Capture] = {
    "B1": Capture(
        "b01_eleclink_m",
        "eleclink",
        "eleclink",
        "46daeb44-95a7-4032-8a22-914c896ed261",
        "1f748d48-1daf-4f1a-a5f1-d581e86a2190",
        "ElecLink NTC Data",
        "1f748d48-1daf-4f1a-a5f1-d581e86a2190",
        "2026-09-17T08:45:57.592028",
        "datastore",
        "2026-10-08T10:36:40.095321+00:00",
    ),
    "B2": Capture(
        "b02_eleclink_a",
        "eleclink",
        "eleclink",
        "46daeb44-95a7-4032-8a22-914c896ed261",
        "991a79f8-2e8c-4f42-b308-e3fb0346700d",
        "Archived ElecLink NTC Data",
        "archived_eleclink_link_ntc_data.csv",
        "2024-11-27T14:34:10.857133",
        "upload",
        "2026-10-08T10:36:43.047166+00:00",
    ),
    "B3": Capture(
        "b03_ifa_itl",
        "ifa_itl",
        "ifa",
        "a95d222b-e390-4ad0-af17-09de957616ed",
        "9e539e05-e09c-4983-91fd-c766f03d0339",
        "IFA ITL Data",
        "9e539e05-e09c-4983-91fd-c766f03d0339",
        "2026-09-17T08:52:40.342385",
        "datastore",
        "2026-10-08T11:09:50.125710+00:00",
    ),
    "B4": Capture(
        "b04_ifa_w",
        "ifa_da_id_weekly_itls",
        "ifa",
        "a95d222b-e390-4ad0-af17-09de957616ed",
        "2b38e754-a92e-4383-827d-5e6c10c882b0",
        "Archived IFA DA & ID Weekly ITLs",
        "archived_ifa-2.csv",
        "2025-08-19T09:35:34.041522",
        "upload",
        "2026-10-08T11:09:23.434751+00:00",
    ),
    "B5": Capture(
        "b05_ifa2_itl",
        "ifa2_ifa_itl",
        "ifa2",
        "860c8221-c656-4af3-800a-281b9e500489",
        "f9ff9381-6eb1-40cd-903b-ca7282b9f2a9",
        "IFA2 ITL Data",
        "f9ff9381-6eb1-40cd-903b-ca7282b9f2a9",
        "2026-09-17T08:49:23.299201",
        "datastore",
        "2026-10-08T11:09:20.101233+00:00",
    ),
    "B6": Capture(
        "b06_ifa2_w",
        "ifa2_ifa_da_id_weekly_itls",
        "ifa2",
        "860c8221-c656-4af3-800a-281b9e500489",
        "19725dae-1d69-423e-8d5b-0f4dc8220b6b",
        "Archived IFA2 DA & ID Weekly ITLs",
        "archived_ifa2.csv",
        "2025-02-19T11:53:27.927475",
        "upload",
        "2026-10-08T11:08:57.403566+00:00",
    ),
    "B7": Capture(
        "b07_nemolink_ntc_m",
        "nemolink_ntc",
        "nemolink",
        "24b229de-2806-45a6-89a9-88097d67e5f2",
        "7d43e2a0-1c22-4e05-895d-756bae210756",
        "NemoLink NTC Data",
        "7d43e2a0-1c22-4e05-895d-756bae210756",
        "2026-09-17T09:11:37.682451",
        "datastore",
        "2026-10-08T11:13:50.731833+00:00",
    ),
    "B8": Capture(
        "b08_nemolink_ntc_a",
        "nemolink_ntc",
        "nemolink",
        "24b229de-2806-45a6-89a9-88097d67e5f2",
        "f169676e-67ec-4f2f-befd-1863c713179c",
        "Archived NemoLink NTC Data",
        "archived_nemolink_link_ntc_data.csv",
        "2024-11-27T14:20:59.758228",
        "upload",
        "2026-10-08T11:13:52.336428+00:00",
    ),
    "B9": Capture(
        "b09_nemolink_intraday",
        "nemolink_intraday",
        "nemolink",
        "24b229de-2806-45a6-89a9-88097d67e5f2",
        "aae5acff-fd30-4c46-b996-a10e50cf4d50",
        "NemoLink-Intraday1-20240127-001",
        "aae5acff-fd30-4c46-b996-a10e50cf4d50",
        "",
        "datastore",
        "2026-10-08T11:13:20.602585+00:00",
    ),
    "B10": Capture(
        "b10_nemo_w",
        "nemolink_nemo_da_id_weekly_ntcs",
        "nemolink",
        "24b229de-2806-45a6-89a9-88097d67e5f2",
        "ed977959-ccec-4cb6-b26f-c528863a243b",
        "Archived Nemo DA & ID Weekly NTCs",
        "archived_weekly_nemolink_link_ntc_data.csv",
        "2024-11-27T14:25:06.891464",
        "upload",
        "2026-10-08T11:13:24.693034+00:00",
    ),
    "B11": Capture(
        "b11_nsl_m",
        "nsl",
        "nsl",
        "7bf2e43d-1a24-4a0e-95b0-6ba4eba3daa9",
        "9db51c01-922d-413d-8b67-3938fdc14bdb",
        "NSL NTC Data",
        "9db51c01-922d-413d-8b67-3938fdc14bdb",
        "2026-09-17T08:57:32.333754",
        "datastore",
        "2026-10-08T11:14:50.796409+00:00",
    ),
    "B12": Capture(
        "b12_nsl_a",
        "nsl",
        "nsl",
        "7bf2e43d-1a24-4a0e-95b0-6ba4eba3daa9",
        "33646e05-1af1-41b5-a056-9e95fb9f0534",
        "Archived NSL NTC Data",
        "archived_nsl_link_ntc_data.csv",
        "2024-11-27T13:26:08.527346",
        "upload",
        "2026-10-08T11:14:52.643813+00:00",
    ),
    "B13": Capture(
        "b13_viking_ntc",
        "viking_ntc",
        "viking",
        "53a942cc-f9ab-4ad3-a274-5beed51635ec",
        "0d20456e-85cc-47a9-b476-e2d8e7c1b8eb",
        "Archived Viking NTC Data",
        "archived_viking_data-1.csv",
        "2024-12-18T11:32:34.909486",
        "upload",
        "2026-10-08T11:41:01.546781+00:00",
    ),
    "B14": Capture(
        "b14_viking_link_ntc",
        "viking_link_ntc",
        "viking",
        "53a942cc-f9ab-4ad3-a274-5beed51635ec",
        "f4ee9a34-1bb5-405e-b4a0-bacb193fb188",
        "Viking Link NTC Data",
        "f4ee9a34-1bb5-405e-b4a0-bacb193fb188",
        "2026-09-17T09:02:57.546341",
        "datastore",
        "2026-10-08T11:40:58.152573+00:00",
    ),
}

HEADERS = {
    "B1": HEADER_M,
    "B2": HEADER_A,
    "B3": HEADER_M,
    "B4": HEADER_W,
    "B5": HEADER_M,
    "B6": HEADER_W,
    "B7": HEADER_M,
    "B8": HEADER_A,
    "B9": HEADER_O,
    "B10": HEADER_W_NEMO,
    "B11": HEADER_M,
    "B12": HEADER_A,
    "B13": HEADER_A,
    "B14": HEADER_M,
}
M_ALIASES = ("B1", "B3", "B5", "B7", "B11", "B14")
A_ALIASES = ("B2", "B8", "B12", "B13")
W_ALIASES = ("B4", "B6", "B10")
LOADABLE = tuple(alias for alias in CAPTURES if alias != "B12")
"""Every capture whose body passes the duplicate guard (B12 repeats a whole row)."""


@dataclass(frozen=True)
class Shape:
    """What the unit spec fixes for one family's record."""

    package_file: str
    aliases: tuple[str, ...]
    headers: tuple[list[str], ...]
    issues: tuple[str, ...]
    temporal: str | None
    key: tuple[str, ...]
    partition: str | None
    vintage: str
    hold: str


SHAPES: dict[str, Shape] = {
    "ifa_itl": Shape(
        "ifa.json",
        ("B3",),
        (HEADER_M,),
        ("data_column",),
        "operational_period_start_gmt",
        A_KEY,
        None,
        "capture_fallback",
        HOLD_A,
    ),
    "ifa2_ifa_itl": Shape(
        "ifa2.json",
        ("B5",),
        (HEADER_M,),
        ("data_column",),
        "operational_period_start_gmt",
        A_KEY,
        None,
        "capture_fallback",
        HOLD_A,
    ),
    "viking_link_ntc": Shape(
        "viking.json",
        ("B14",),
        (HEADER_M,),
        ("data_column",),
        "operational_period_start_gmt",
        A_KEY,
        None,
        "capture_fallback",
        HOLD_A,
    ),
    "eleclink": Shape(
        "eleclink.json",
        ("B1", "B2"),
        (HEADER_M, HEADER_A),
        ("data_column", "none"),
        None,
        B_KEY,
        "resource_id",
        "capture_fallback",
        HOLD_B,
    ),
    "nemolink_ntc": Shape(
        "nemolink.json",
        ("B7", "B8"),
        (HEADER_M, HEADER_A),
        ("data_column", "none"),
        None,
        B_KEY,
        "resource_id",
        "capture_fallback",
        HOLD_B,
    ),
    "nsl": Shape(
        "nsl.json",
        ("B11", "B12"),
        (HEADER_M, HEADER_A),
        ("data_column", "none"),
        None,
        B_KEY,
        "resource_id",
        "capture_fallback",
        HOLD_B,
    ),
    "viking_ntc": Shape(
        "viking.json",
        ("B13",),
        (HEADER_A,),
        ("none",),
        None,
        ARCHIVE_KEY,
        None,
        "ckan_last_modified",
        HOLD_C,
    ),
    "ifa_da_id_weekly_itls": Shape(
        "ifa.json",
        ("B4",),
        (HEADER_W,),
        ("none",),
        None,
        ("operational_date_and_hour",),
        None,
        "ckan_last_modified",
        HOLD_D,
    ),
    "ifa2_ifa_da_id_weekly_itls": Shape(
        "ifa2.json",
        ("B6",),
        (HEADER_W,),
        ("none",),
        None,
        ("operational_date_and_hour",),
        None,
        "ckan_last_modified",
        HOLD_C,
    ),
    "nemolink_nemo_da_id_weekly_ntcs": Shape(
        "nemolink.json",
        ("B10",),
        (HEADER_W_NEMO,),
        ("none",),
        None,
        ("operational_date_and_hour",),
        None,
        "ckan_last_modified",
        HOLD_C,
    ),
    "nemolink_intraday": Shape(
        "nemolink.json",
        ("B9",),
        (HEADER_O,),
        ("none",),
        None,
        ("hourly_time_period",),
        None,
        "capture_fallback",
        HOLD_E,
    ),
}
"""The eleven records, key by key, from the unit spec's scope table."""

EXPECTED_DTYPES = {
    "data_upload_time_gmt": "datetime",
    "auction_type": "string",
    "operational_period_start_gmt": "datetime",
    "flow_to_gb_mw": "float64",
    "flow_from_gb_mw": "float64",
    "reason_for_restriction_to_gb": "string",
    "reason_for_restriction_from_gb": "string",
    "operational_date": "string",
    "version": "string",
    "hourly_time_period": "string",
    "reason_for_restriction": "string",
    "operational_date_and_hour": "string",
}
"""One dtype per silver name across all eleven records (V-1 holds per family; this holds it
across the batch, so G-3 sees one type per meaning)."""


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings (and so the CLI) point at."""
    with tempfile.TemporaryDirectory(
        prefix="ic", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with its bronze original's line ending (B4 CRLF, the rest LF)."""
    raw = (FIXTURES / f"{CAPTURES[alias].fixture}.csv").read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n") if alias == "B4" else raw


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed."""
    text = (raw if raw is not None else body(alias)).decode("utf-8")
    return list(csv.DictReader(io.StringIO(text, newline="")))


def capture(
    data: Path, alias: str, *, raw: bytes | None = None, written: datetime | None = None
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=written or datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=meta.package,
        package_id=meta.package_id,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=meta.filename,
        ckan_last_modified=meta.modified or None,
        url_type=meta.url_type,
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _load(data: Path, *aliases: str) -> dict[str, str]:
    """Capture ``aliases`` of one family and transform them; the capture id of each."""
    ids = {alias: capture(data, alias) for alias in aliases}
    (family,) = {CAPTURES[alias].family for alias in aliases}
    get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    return ids


def _of(frame: pl.DataFrame, alias: str) -> pl.DataFrame:
    return frame.filter(pl.col("resource_id") == CAPTURES[alias].resource_id)


def _upload(value: str) -> datetime:
    return datetime.strptime(value, UPLOAD_FORMAT).replace(tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Fixtures and record shapes
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact header, the
    zero limits, blank and non-default reasons, the raw auction spellings, the IFA 2024-10-27
    repeated hour with its ``(a)``/``(b)`` labels, the 24-row ``20221030`` days, the NSL
    archive's whole-row duplicate pair and the one-off's 24 hours."""
    for alias, header in HEADERS.items():
        assert list(rows(alias)[0]) == header, alias
    assert body("B4").count(b"\r\n") == len(rows("B4")) + 1
    assert all(b"\r" not in body(alias) for alias in HEADERS if alias != "B4")
    assert any(r["Flow in MW To GB"] == "0" for r in rows("B1"))
    assert any(r["Flow in MW From GB"] == "0" for r in rows("B11"))
    assert any(r["Reason For Reduction"] == "" for r in rows("B2"))
    assert any(r["Reason for restriction"] == "" for r in rows("B4"))
    assert any(r["Reason For Restriction"] == "" for r in rows("B10"))
    assert any(r["Flow (MW) from GB"].endswith(".75") for r in rows("B10"))
    assert any(r["Flow (MW) from GB"] == "927.0" for r in rows("B10"))
    assert {r["Auction Type"] for r in rows("B1")} >= {"Day Ahead", "Intraday 1"}
    assert {r["Auction Type"] for r in rows("B2")} >= {"DayAhead", "Intraday1"}
    assert any(
        r["Data Upload Time GMT"] > r["Operational Period Start Date and Time GMT"]
        for r in rows("B1")
    )
    label = HEADER_W[0]
    fold = [r[label] for r in rows("B4") if r[label].startswith("20241027 ")]
    assert len(fold) == 25
    assert "20241027 01:00-02:00 (a)" in fold and "20241027 01:00-02:00 (b)" in fold
    for alias in W_ALIASES:
        first = HEADERS[alias][0]
        day = [r[first] for r in rows(alias) if r[first].startswith("20221030 ")]
        assert len(day) == 24 and not any("(" in cell for cell in day), alias
    assert "20221030 23:00 - 00:00" in {r[HEADER_W[0]] for r in rows("B10")}
    assert "20241218 23:00-00:00" in {r[HEADER_W[0]] for r in rows("B4")}
    repeated = [tuple(r.values()) for r in rows("B12")]
    assert len(repeated) - len(set(repeated)) == 1
    assert (
        "20231029",
        "DayAhead",
        "001",
        "22:00 to 23:00",
        "1400",
        "1437",
        "Network Constraints",
    ) in {value for value in repeated if repeated.count(value) == 2}
    assert len(rows("B9")) == 24


@pytest.mark.parametrize("key", sorted(SHAPES))
def test_record_shape_matches_the_unit_spec(key: str) -> None:
    """Detects a record drifting from the spec: csv reader, utf-8, exactly the observed
    header epochs in order, the issue recipe per epoch (a data column on the M epoch only),
    the temporal recipe (a UTC instant only where every epoch carries one), the measured key,
    whole-capture selection (per resource where two resources share the family), the vintage
    ADR-035 fixes by ``url_type``, and no invented null token, bound or format on a text or
    integer column."""
    shape, record = SHAPES[key], _record(key)
    assert (record.reader, record.encoding, record.version) == ("csv", "utf-8", "1")
    assert [list(epoch.header) for epoch in record.epochs] == list(shape.headers)
    assert tuple(epoch.issue.kind for epoch in record.epochs) == shape.issues
    for epoch in record.epochs:
        if epoch.issue.kind == "data_column":
            assert epoch.issue.column == "data_upload_time_gmt"
    if shape.temporal is None:
        assert record.temporal.kind == "none"
    else:
        assert (record.temporal.kind, record.temporal.column) == ("utc_instant", shape.temporal)
    assert record.entity_key == shape.key
    assert record.latest == "whole_capture"
    assert record.latest_partition == shape.partition
    assert record.vintage == shape.vintage
    assert record.siblings == ()
    for epoch in record.epochs:
        for column in epoch.columns:
            assert not column.null_tokens, column.name
            assert column.min is None and column.max is None, column.name
            assert EXPECTED_DTYPES[column.name] == column.dtype, column.name
            if column.dtype == "datetime":
                assert (column.format, column.zone) == (UPLOAD_FORMAT, "UTC"), column.name
            else:
                assert column.format is None, column.name
            # only the hourly target start refuses a blank (a temporal input must, V-3); every
            # other column keeps what the vendor sent
            assert column.nullable is not (column.name == "operational_period_start_gmt"), (
                column.name
            )


def test_the_silver_names_are_one_name_per_meaning() -> None:
    """Detects a header mapped to a second silver name for a meaning another family already
    names: across the eleven records the M flows, the A/W/O flows, the undirected reason and
    the hourly label each take one name, and every source header maps to the same name in
    every family that carries it."""
    by_source: dict[str, set[str]] = {}
    for key in SHAPES:
        for epoch in _record(key).epochs:
            for column in epoch.columns:
                by_source.setdefault(column.source, set()).add(column.name)
    assert by_source["Flow in MW To GB"] == by_source["Flow (MW) To GB"] == {"flow_to_gb_mw"}
    assert by_source["Flow (MW) to GB"] == {"flow_to_gb_mw"}
    assert by_source["Flow in MW From GB"] == by_source["Flow (MW) From GB"] == {"flow_from_gb_mw"}
    assert by_source["Flow (MW) from GB"] == {"flow_from_gb_mw"}
    assert (
        by_source["Reason For Reduction"]
        == by_source["Reason for restriction"]
        == by_source["Reason For Restriction"]
        == {"reason_for_restriction"}
    )
    assert all(len(names) == 1 for names in by_source.values())


@pytest.mark.parametrize("key", sorted(SHAPES))
def test_every_family_is_held_with_its_question(key: str) -> None:
    """Detects a family published without its hold: the record carries the spec's one-sentence
    ``E-SEM`` question and that is the family's effective eligibility (the package itself
    stays eligible)."""
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert record.eligibility.unit == "E-SEM"
    assert record.eligibility.question == SHAPES[key].hold
    package, family = registry_module.load_registry().families[key]
    assert effective_eligibility(package, family) == record.eligibility


def test_each_family_is_in_the_package_file_the_spec_names() -> None:
    """Detects a record landing in the wrong package file, or a family split: the six package
    files carry exactly these eleven records (membership also fixes the bronze directory) and
    every family's resources keep their SILVER disposition."""
    root = Path(registry_module.__file__).parent
    for filename in sorted({shape.package_file for shape in SHAPES.values()}):
        document: dict[str, Any] = json.loads((root / filename).read_text(encoding="utf-8"))
        recorded = {f["key"] for f in document["families"] if "record" in f}
        assert recorded == {k for k, s in SHAPES.items() if s.package_file == filename}, filename
        for resource in document["resources"]:
            assert resource["disposition"] == {"kind": "SILVER", "key": resource["family"]}
    for alias, meta in CAPTURES.items():
        _package, resource = registry_module.load_registry().resources[meta.resource_id]
        assert resource.family == meta.family, alias


# --------------------------------------------------------------------------- #
# Typing the real bodies
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", LOADABLE)
def test_fixture_types_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch, a cast
    the vendor body does not satisfy (a fractional MW, a ``%Y-%m-%dT%H:%M:%S`` instant), a
    clock taken from anywhere but the declared recipe and any row excluded: the capture
    completes with every populated row, zero exclusions, and the generic output columns."""
    meta = CAPTURES[alias]
    transformer = get_transformer(SOURCE, meta.family, data)
    capture_id = capture(data, alias)
    written = transformer.run(DAY, run_id="r")
    assert written == len(rows(alias))
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == len(rows(alias))
    assert completion["rows_excluded"] == 0

    frame = _silver(data, meta.family)
    expected = [name for name, _type in generic.output_columns(_record(meta.family))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected if c not in ("year", "month")
    ]
    assert frame["timestamp_utc"].null_count() == 0
    assert frame.select(list(SHAPES[meta.family].key)).is_duplicated().sum() == 0


@pytest.mark.parametrize("alias", LOADABLE)
def test_text_labels_and_values_survive_byte_identical(data: Path, alias: str) -> None:
    """Detects a normalised label, a parsed version or date, a compacted auction spelling, a
    stripped fold suffix or a dropped zero: every text column equals its CSV cell and every
    flow equals its integer cell, in order, so ``Day Ahead`` / ``DayAhead`` stay different
    strings, ``001`` and ``20240924`` stay text, ``(a)``/``(b)`` stay on the W label, and a
    zero limit stays a zero (no null conversion, no nameplate fill)."""
    meta = CAPTURES[alias]
    _load(data, alias)
    frame = _silver(data, meta.family)
    source = rows(alias)
    to_name = {
        "Auction Type": "auction_type",
        "Version": "version",
        "Operational Date": "operational_date",
        "Hourly Time Period (GMT)": "hourly_time_period",
        HEADER_W[0]: "operational_date_and_hour",
    }
    for header, name in to_name.items():
        if header in HEADERS[alias]:
            assert frame[name].to_list() == [r[header] for r in source], (alias, name)
            assert frame.schema[name] == pl.Utf8
    for header in HEADERS[alias]:
        if header.lower().startswith("flow"):
            name = "flow_to_gb_mw" if header.lower().endswith("to gb") else "flow_from_gb_mw"
            assert frame[name].to_list() == [float(r[header]) for r in source], (alias, name)
            assert frame.schema[name] == pl.Float64
            zero = [r[header] for r in source].count("0")
            assert frame[name].to_list().count(0) == zero
            assert frame[name].null_count() == 0


def test_a_fractional_limit_is_kept_not_a_capture_failure(data: Path) -> None:
    """Detects a flow column typed as an integer: the Nemo weekly archive publishes limits such
    as ``927.0`` and ``972.75`` MW, an ``int64`` cast raises and loses the whole capture. Every
    directional limit in every record is ``float64`` and the fractional values arrive exact."""
    for key in SHAPES:
        for epoch in _record(key).epochs:
            for column in epoch.columns:
                if column.name.startswith("flow_"):
                    assert column.dtype == "float64", (key, column.name)
    _load(data, "B10")
    frame = _silver(data, "nemolink_nemo_da_id_weekly_ntcs")
    assert frame.schema["flow_from_gb_mw"] == pl.Float64
    assert {927.0, 972.75, 559.75} <= set(frame["flow_from_gb_mw"].to_list())


@pytest.mark.parametrize("alias", LOADABLE)
def test_reasons_keep_their_text_and_a_blank_is_null(data: Path, alias: str) -> None:
    """Detects an invented reason (a blank filled with ``No Restriction`` or a placeholder)
    or a reason text altered: every reason column equals its CSV cell, a blank cell is null,
    and nothing else is."""
    meta = CAPTURES[alias]
    _load(data, alias)
    frame = _silver(data, meta.family)
    source = rows(alias)
    for header in HEADERS[alias]:
        if header.lower().startswith("reason"):
            name = (
                "reason_for_restriction_to_gb"
                if header.endswith("To GB")
                else "reason_for_restriction_from_gb"
                if header.endswith("From GB")
                else "reason_for_restriction"
            )
            assert frame[name].to_list() == [r[header] or None for r in source], (alias, name)
    if alias in ("B2", "B4", "B10"):
        assert frame["reason_for_restriction"].null_count() > 0


@pytest.mark.parametrize("alias", M_ALIASES)
def test_m_instants_are_utc_and_the_issue_is_the_upload_time(data: Path, alias: str) -> None:
    """Detects a naive or shifted instant, an issue time taken from anywhere but the upload
    column, or a temporal column that is not the hourly target start: the upload and start
    columns are tz-aware UTC and equal their CSV cells, ``issue_time`` equals the upload
    time, and where the record declares a temporal recipe ``timestamp_utc`` is the target
    start (otherwise the capture time, never a derived instant)."""
    meta = CAPTURES[alias]
    _load(data, alias)
    frame = _silver(data, meta.family)
    source = rows(alias)
    for name in ("data_upload_time_gmt", "operational_period_start_gmt", "issue_time"):
        assert frame.schema[name] == pl.Datetime("us", "UTC"), name
    assert frame["data_upload_time_gmt"].to_list() == [
        _upload(r["Data Upload Time GMT"]) for r in source
    ]
    starts = [_upload(r["Operational Period Start Date and Time GMT"]) for r in source]
    assert frame["operational_period_start_gmt"].to_list() == starts
    assert frame["issue_time"].to_list() == frame["data_upload_time_gmt"].to_list()
    if _record(meta.family).temporal.kind == "utc_instant":
        assert frame["timestamp_utc"].to_list() == starts
    else:
        written = datetime.fromisoformat(meta.written).astimezone(UTC)
        assert set(frame["timestamp_utc"].to_list()) == {written}


@pytest.mark.parametrize("alias", (*A_ALIASES[:2], "B13", "B4", "B6", "B10", "B9"))
def test_archives_derive_no_instant_from_their_labels(data: Path, alias: str) -> None:
    """Detects a target instant, week or issue time invented from an operational-date label,
    an hourly label or the ``Version``: the A, W and O bodies carry no issue, ``timestamp_utc``
    stays the capture time and no date or datetime column exists beyond the engine's."""
    meta = CAPTURES[alias]
    _load(data, alias)
    frame = _silver(data, meta.family)
    written = datetime.fromisoformat(meta.written).astimezone(UTC)
    assert set(frame["timestamp_utc"].to_list()) == {written}
    assert "issue_time" not in frame.columns or frame["issue_time"].null_count() == frame.height
    forbidden = [c for c in frame.columns if "week" in c or c in ("date", "settlement_date")]
    assert forbidden == []


def test_ifa_weekly_keeps_the_repeated_hour_and_the_unmarked_autumn_day(data: Path) -> None:
    """Detects a fold suffix stripped or an hour deduplicated: on ``20241027`` the 25 rows
    keep ``01:00-02:00 (a)`` and ``(b)`` as two distinct keys, and ``20221030`` keeps its 24
    unmarked rows (this vendor labelling is preserved, not interpreted)."""
    _load(data, "B4")
    frame = _silver(data, "ifa_da_id_weekly_itls")
    labels = frame["operational_date_and_hour"]
    fold = labels.filter(labels.str.starts_with("20241027 "))
    assert fold.len() == 25 and fold.n_unique() == 25
    assert {"20241027 01:00-02:00 (a)", "20241027 01:00-02:00 (b)"} <= set(fold.to_list())
    assert labels.filter(labels.str.starts_with("20221030 ")).len() == 24


def test_nemolink_one_off_keeps_its_24_hours_undated(data: Path) -> None:
    """Detects a date invented for the undated one-off (from the resource name) or a lost
    hour: the 24 hourly labels survive in order, the family's only key is the hourly label
    and the resource stays a SILVER disposition (DOC needs vendor lifecycle evidence)."""
    _load(data, "B9")
    frame = _silver(data, "nemolink_intraday")
    assert frame["hourly_time_period"].to_list() == [
        r["Hourly Time Period (GMT)"] for r in rows("B9")
    ]
    assert frame.height == 24
    _package, resource = registry_module.load_registry().resources[CAPTURES["B9"].resource_id]
    assert resource.disposition.kind == "SILVER"


# --------------------------------------------------------------------------- #
# Two resources in one family (eleclink, nemolink_ntc, nsl)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("key", "m_alias", "a_alias"), [("eleclink", "B1", "B2"), ("nemolink_ntc", "B7", "B8")]
)
def test_both_epochs_load_under_one_family_and_latest_serves_each(
    data: Path, key: str, m_alias: str, a_alias: str
) -> None:
    """Detects one epoch's capture displacing the other's (a family-wide newest-capture
    selection), an epoch row excluded by the other epoch's missing columns, or an unstamped
    ``resource_id``: both captures complete with every row, every row is stamped with its
    own resource, the other epoch's key columns are null on a row, and ``_latest`` (the
    catalogue view and Polars agree) serves both captures, then serves a later dump capture
    with the archive still present."""
    ids = _load(data, m_alias, a_alias)
    frame = _silver(data, key)
    m_rows, a_rows = _of(frame, m_alias), _of(frame, a_alias)
    assert (m_rows.height, a_rows.height) == (len(rows(m_alias)), len(rows(a_alias)))
    assert m_rows["operational_date"].null_count() == m_rows.height
    assert m_rows["hourly_time_period"].null_count() == m_rows.height
    assert a_rows["issue_time"].null_count() == a_rows.height
    assert a_rows["operational_period_start_gmt"].null_count() == a_rows.height
    assert m_rows["issue_time"].null_count() == 0
    # the two epochs spell the auction differently; both are kept as written
    assert {"Day Ahead", "DayAhead"} <= set(frame["auction_type"].to_list())

    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, key, None)) == set(ids.values())

    later = capture(data, m_alias, written=datetime(2026, 10, 8, 18, 0, tzinfo=UTC))
    get_transformer(SOURCE, key, data).run(DAY, run_id="r2")
    assert set(both_as_of(db, data, key, None)) == {later, ids[a_alias]}


def _inject(raw: bytes, column: int, value: str) -> bytes:
    """``raw`` plus a copy of its first data row whose limit at ``column`` is ``value``: the
    same entity key with another value, which no key column may absorb."""
    fields = raw.split(b"\n")[1].rstrip(b"\r").decode().split(",")
    fields[column] = value
    return raw + ",".join(fields).encode() + b"\n"


@pytest.mark.parametrize(
    ("key", "m_alias", "a_alias"),
    [("eleclink", "B1", "B2"), ("nemolink_ntc", "B7", "B8"), ("nsl", "B11", None)],
)
def test_the_guard_passes_each_real_epoch_and_fails_an_injected_duplicate(
    data: Path, key: str, m_alias: str, a_alias: str | None
) -> None:
    """Detects a union key that is too coarse to guard an epoch (the other epoch's null key
    columns collapsing rows) or too fine (a duplicate slipping past): each real body passes
    (``test_fixture_types_with_no_exclusion``); an injected row repeating a row's key with
    another limit fails the capture with ``DuplicateEntityKeyError`` — in the dump epoch and,
    separately, in the archive epoch — leaves no completion, and the clean sibling resource
    still completes.
    """
    m_id = capture(data, m_alias, raw=_inject(body(m_alias), 3, "9999"))
    clean = capture(data, a_alias) if a_alias else None
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [(m_id, "DuplicateEntityKeyError")]
    assert read_completion(data, key, m_id) is None
    failure = read_failure(data, key, m_id)
    assert failure is not None and failure["error_class"] == DuplicateEntityKeyError.__name__
    if clean is not None:
        completion = read_completion(data, key, clean)
        assert completion is not None and completion["rows_excluded"] == 0

    if a_alias is not None:
        a_id = capture(data, a_alias, raw=_inject(body(a_alias), 4, "9998"))
        with pytest.raises(NesoCaptureFailedError) as info:
            get_transformer(SOURCE, key, data).run(DAY, run_id="r2")
        assert (a_id, "DuplicateEntityKeyError") in [(c, cls) for c, cls, _m in info.value.failures]
        assert read_completion(data, key, a_id) is None


# --------------------------------------------------------------------------- #
# The NSL archive: an expected failed capture, adjudicated (ADR-040)
# --------------------------------------------------------------------------- #

REGISTRY_DIR = Path(registry_module.__file__).parent
NSL_ARCHIVE_PATH = (
    "bronze/neso_data_portal/nsl/2026/10/08/"
    "raw_20261008T111452Z_33646e05-1af1-41b5-a056-9e95fb9f0534_0519d13a.csv"
)
NSL_ENTRY = {
    "family": "nsl",
    "category": "failed",
    "cause": "DuplicateEntityKeyError",
    "captures": [NSL_ARCHIVE_PATH],
    "reason": (
        "the archive repeats one whole row (20231029,DayAhead,001,22:00 to 23:00,1400,1437,"
        "Network Constraints), so no field key is lossless and the duplicate guard fails the "
        "capture; it stays unloaded with its failure record, and the archive's spring and "
        "autumn day labels are also irregular"
    ),
    "question": (
        "Is the repeated 2023-10-29 DayAhead 22:00–23:00 row a publication defect, and "
        "which value set is authoritative?"
    ),
    "evidence": "K-IC-1-FACTS nsl T08/T10; ADR-040",
    "ruling": "565",
}
"""The ledger entry the unit spec fixes, verbatim (ruling 565 is the IC-1 spec ruling)."""


def _bytes_under(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_nsl_archive_fails_the_guard_and_the_dump_loads(data: Path) -> None:
    """Detects an NSL archive that is deduplicated, given a row index, or has a key that
    absorbs its repeated row: B12 fails with a ``DuplicateEntityKeyError`` failure record, no
    completion and no output; the fixture still holds the identical pair (nothing was
    dropped from the source); the B11 dump completes with every row and zero exclusions and
    ``_latest`` serves it alone."""
    pair = tuple(
        value for value in map(tuple, map(dict.values, rows("B12"))) if rows_count(value) == 2
    )
    assert len(pair) == 2
    b11, b12 = capture(data, "B11"), capture(data, "B12")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, "nsl", data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _message in info.value.failures] == [
        (b12, "DuplicateEntityKeyError")
    ]
    failure = read_failure(data, "nsl", b12)
    assert failure is not None and failure["error_class"] == DuplicateEntityKeyError.__name__
    assert read_completion(data, "nsl", b12) is None
    completion = read_completion(data, "nsl", b11)
    assert completion is not None and completion["rows_excluded"] == 0
    assert completion["row_count"] == len(rows("B11"))
    silver = _silver(data, "nsl")
    assert set(silver["bronze_capture_id"].to_list()) == {b11}
    assert silver.height == len(rows("B11"))

    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, "nsl", None)) == {b11}


def rows_count(value: tuple[str, ...]) -> int:
    """How many B12 records equal ``value`` (the whole-row repeat the guard cannot key)."""
    return sum(tuple(r.values()) == value for r in rows("B12"))


def test_nsl_failure_is_adjudicated_not_open(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Detects an expected vendor-caused failure that stays an open gap (reconcile exits 1
    forever), or an adjudication that alters data: without the entry reconcile reports the
    B12 failure and exits 1; with the committed entry (its capture id swapped for the fixture
    capture's) it exits 0 reporting one adjudicated gap, and the silver, state and ``_latest``
    bytes are equal before and after."""
    install_generated(monkeypatch, data / "_registry", [_package_doc("nsl.json")])
    b11, b12 = capture(data, "B11"), capture(data, "B12")
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "nsl", data).run(DAY, run_id="r")
    assert read_completion(data, "nsl", b11) is not None

    before = (_bytes_under(data / "silver"), _bytes_under(data / "state"))
    code, lines = run_cli("nsl", "--cutoff", DAY.isoformat())
    assert code == 1, lines
    assert [line for line in lines if line.startswith("GAP failed")] != []
    assert all(b12 in line for line in lines if line.startswith("GAP failed"))

    entry = {**NSL_ENTRY, "captures": [b12]}
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json([entry]), encoding="utf-8"
    )
    code, lines = run_cli("nsl", "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert "SUMMARY adjudicated 1" in lines
    assert (_bytes_under(data / "silver"), _bytes_under(data / "state")) == before


def _package_doc(filename: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / filename).read_text(encoding="utf-8"))
    return document


def _fresh_interpreter(code: str) -> Any:
    """Run ``code`` in a fresh interpreter (nothing collection imported can mask it); the
    JSON of its last stdout line."""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_the_committed_ledger_carries_the_nsl_entry() -> None:
    """Detects the committed NSL entry drifting from the ruled text (another capture, a wider
    scope, an edited reason or question), a ledger the registry rejects, or an entry naming a
    resource that is not the family's archive; in a fresh interpreter."""
    loaded = _fresh_interpreter(
        """
        import json
        from gridflow.connectors.neso_data_portal.registry import (
            CAPTURE_ID_PATTERN, load_reconcile_adjudications, load_registry,
            reconcile_adjudication_problems,
        )
        registry = load_registry()
        entries = load_reconcile_adjudications()
        assert reconcile_adjudication_problems(registry, entries) == []
        nsl = [e for e in entries if e.family == "nsl"]
        names = sorted(
            registry.resources[CAPTURE_ID_PATTERN.fullmatch(c)["rid"]][1].name
            for e in nsl for c in e.captures
        )
        print(json.dumps({
            "entries": [e.model_dump(mode="json") for e in nsl],
            "names": names,
            "families": [e.family for e in entries],
        }))
        """
    )
    # the whole ledger is the three ruled families (GEN-2H's two, RULINGS 547, and this one):
    # a stray well-formed entry for another family fails here
    assert loaded["families"] == [
        "metered_wind_output_monthly",
        "wind_bmu_boa_volumes",
        "nsl",
    ]
    assert loaded["entries"] == [NSL_ENTRY]
    assert loaded["names"] == ["Archived NSL NTC Data"]


def test_the_viking_package_id_is_unchanged() -> None:
    """Detects an edit to ``viking.json``'s ``package_id`` (the FACTS note suspected a
    registry/catalogue mismatch; the seat measured both as the same id, RULINGS 565): the
    registry's id is the bronze sidecar's for both Viking resources, and the file's literal
    is unchanged."""
    expected = CAPTURES["B13"].package_id
    assert expected == CAPTURES["B14"].package_id == "53a942cc-f9ab-4ad3-a274-5beed51635ec"
    loaded = _fresh_interpreter(
        """
        import json
        from gridflow.connectors.neso_data_portal.registry import load_registry
        registry = load_registry()
        print(json.dumps({
            "package_id": next(p for p in registry.packages if p.package == "viking").package_id,
            "families": sorted(
                k for k, (p, _f) in registry.families.items() if p.package == "viking"
            ),
        }))
        """
    )
    assert loaded["package_id"] == expected
    assert loaded["families"] == ["viking_link_ntc", "viking_ntc"]
    assert _package_doc("viking.json")["package_id"] == expected
