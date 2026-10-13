"""The ancillary-services batch 2: eleven frozen records (nine eligible, two held) and one family
with no record (v0.22-K-AS-2).

Eligible: the balancing, DC/DM/DR, Quick Reserve, Slow Reserve and STOR industry notifications, the
Slow Reserve requirement forecast, the SFFR auction results, the SFFR requirement and the STOR
windows. Held: the SFFR industry notifications and the SFFR buy orders.
``contract_transfer_obligation`` has **no record** (RULINGS 538) and stays ingest-only. Every test
writes recorded fixture captures (cuts of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/as2/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation. On master
none of the twelve families has a record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-AS-2-FACTS g1-g6 and section 5, K-AS-2-SPEC):

- Notifications carry **no** ``issue_time``: every newest issue label converts to an instant after
  the upload's last-modified, so the label is the event time of an event table. The issue,
  effective-from and effective-to labels are ``dd/mm/YYYY HH:MM`` on the UK local clock (vendor
  dictionary), ``ambiguous: raise``, and the temporal recipe is ``local_instant`` of the issue
  label. Effective-to closure is undocumented: endpoints are kept as given.
- The SFFR notification singleton keeps all three labels **raw** and its temporal recipe ``none``
  (``01/03/2024`` is day-first or month-first, and ``Example note`` may be a template).
- EFA blocks, Slow Reserve service windows, SFFR delivery clocks and STOR window clocks stay **raw
  strings**: the engine cannot compose a date + label (RULINGS 572/588) and the autumn windows
  ``5``/``5x``, ``6``/``6x`` repeat their local labels. The temporal recipe is a ``date_sp1`` label
  anchor, never a block or window start.
- Zeros stay zero (``Accepted Volume`` 0.0 on a rejected bid is a result). Units only as FACTS g4
  cites them. ``Season`` is a categorical string (``15.1`` ... ``19.6``), never a float. No trim,
  no token map, no ``run_type``.
- ``contract_transfer_obligation``: 12 rows, 7 distinct, 5 duplicate excess; even every source
  column together fails the duplicate guard, so there is no lossless key and no record. It is never
  deduplicated and never given a row ordinal. TODO: CONTRACT-TRANSFER-IDENTITY (settle with a vendor
  transfer identifier or a revision carrying distinguishing columns). TODO: CONTRACT-TRANSFER-PERIOD
  (clock, endpoint closure and omitted-time meaning of the date-only delivery labels).

``.gitattributes`` stores the fixtures byte-for-byte (``-text``): the balancing notification carries
a lone LF inside a quoted cell, which an end-of-line rewrite would corrupt.
"""

from __future__ import annotations

import csv
import io
import json
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
from gridflow.connectors.neso_data_portal.profile import silver_name
from gridflow.connectors.neso_data_portal.registry import Eligible, Held, load_registry
from gridflow.connectors.neso_data_portal.registry.record import has_issue_time
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as2"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-AS-2"
BOM = b"\xef\xbb\xbf"
LONDON = ZoneInfo("Europe/London")
ISO = "%Y-%m-%d"
DMY = "%d/%m/%Y"
DT = "%d/%m/%Y %H:%M"
DASH = "—"

N_BAL = "as_industry_notifications_balancing"
N_DX = "as_industry_notifications_dc_dm_dr"
N_QR = "as_industry_notifications_quick_reserve"
N_SFFR = "as_industry_notifications_sffr_important"
N_SR = "as_industry_notifications_slow_reserve"
N_STOR = "as_industry_notifications_stor_important"
CT = "contract_transfer_obligation"
SR = "slow_reserve_requirement_fc"
A_MAIN = "sffr_auction_results_main"
A_BUY = "sffr_auction_results_buy_orders"
F_REQ = "sffr_requirement"
STOR = "stor_windows"

HELD = {
    N_SFFR: (
        f"TODO: SFFR-NOTIFICATION-DATE-ORDER {DASH} is `01/03/2024 10:00` day-first or "
        'month-first, and is the single "Example note" row a real notification or a template '
        "placeholder?"
    ),
    A_BUY: (
        f"TODO: SFFR-BUY-CURVE-CELL-UNITS {DASH} the dictionary labels the band columns MW but "
        "describes their cells as prices (the `200` entry has no unit); what monetary unit are "
        "the cells?"
    ),
}
TYPED_NOTIFICATIONS = (N_BAL, N_DX, N_QR, N_SR, N_STOR)
ELIGIBLE = (*TYPED_NOTIFICATIONS, SR, A_MAIN, F_REQ, STOR)
RECORDED = (*ELIGIBLE, *HELD)

PACKAGES = {
    "ancillary-services-important-industry-notifications": "fc8dadc7-235d-44b0-bab2-320dc76095de",
    "contract-transfer-of-obligation": "cb295f83-bfb8-4aa0-9b92-0a6791342c7e",
    "slow-reserve-requirement-forecast": "87d77cd4-f3c8-4fda-973b-91149ee7a0e6",
    "static-firm-frequency-response-auction-results": "75457796-f9de-49c2-8e64-81f97131c56a",
    "static-firm-frequency-response-requirement": "1f641071-ffc1-4fca-b83e-2c30772530a3",
    "stor-windows": "d7371697-0a03-43a9-a3f3-572d06b25e59",
}
_NOTIFICATIONS_PACKAGE = "ancillary-services-important-industry-notifications"
PACKAGE_OF = {
    N_BAL: _NOTIFICATIONS_PACKAGE,
    N_DX: _NOTIFICATIONS_PACKAGE,
    N_QR: _NOTIFICATIONS_PACKAGE,
    N_SFFR: _NOTIFICATIONS_PACKAGE,
    N_SR: _NOTIFICATIONS_PACKAGE,
    N_STOR: _NOTIFICATIONS_PACKAGE,
    CT: "contract-transfer-of-obligation",
    SR: "slow-reserve-requirement-forecast",
    A_MAIN: "static-firm-frequency-response-auction-results",
    A_BUY: "static-firm-frequency-response-auction-results",
    F_REQ: "static-firm-frequency-response-requirement",
    STOR: "stor-windows",
}

H_N6 = [
    "Notification Issued Date Time",
    "Service",
    "Description",
    "Effective From Date Time",
    "Effective To Date Time",
    "Notes",
]
H_N5 = [h for h in H_N6 if h != "Service"]
H_SR = [
    "Auction Date",
    "Service Delivery Date",
    "Service Window",
    "Service Window Start Time",
    "Service Window End Time",
    "Positive Slow Reserve Requirement MW",
    "Negative Slow Reserve Requirement MW",
    "Forecast produced on",
]
H_A1 = [
    "Date",
    "Provider",
    "Unit",
    "Delivery Start",
    "Delivery End",
    "EFA",
    "Service",
    "Status",
    "Volume",
    "Accepted Volume",
    "Price",
    "Clearing Price",
    "Technology Type",
    "Location",
    "Rank",
]
BANDS = [str(band) for band in range(25, 251, 25)]
H_A2 = ["EFA Date", "EFA", *BANDS]
H_F1 = ["EFA_DATE", "Service", *(f"EFA{i}" for i in range(1, 7))]
H_W1 = [
    "Date",
    "Season",
    "WD_NWD",
    "Window 1 Start Time",
    "Window 1 End Time",
    "Window 2 Start Time",
    "Window 2 End Time",
]

# (source, silver name, kind, date format): "id" non-null string; "s" nullable string; "f" nullable
# float64; "dk" non-null date; "dn" nullable date; "i" non-null int64; "in" nullable int64; "tk"
# non-null and "tn" nullable Europe/London datetime.
Spec = tuple[str, str, str, str]


def _n(source: str, kind: str = "s", fmt: str = "") -> Spec:
    return (source, silver_name(source, 1), kind, fmt)


NOTIFIED: list[Spec] = [
    _n("Notification Issued Date Time", "tk", DT),
    _n("Service"),
    _n("Description"),
    _n("Effective From Date Time", "tk", DT),
    _n("Effective To Date Time", "tn", DT),
    _n("Notes"),
]
COLS: dict[str, list[Spec]] = {
    N_BAL: NOTIFIED,
    N_DX: NOTIFIED,
    N_QR: [s for s in NOTIFIED if s[0] != "Service"],
    N_SR: [s for s in NOTIFIED if s[0] != "Service"],
    N_STOR: NOTIFIED,
    N_SFFR: [
        _n("Notification Issued Date Time", "id"),
        _n("Service"),
        _n("Description"),
        _n("Effective From Date Time"),
        _n("Effective To Date Time"),
        _n("Notes"),
    ],
    SR: [
        _n("Auction Date", "dk", DMY),
        _n("Service Delivery Date", "dk", DMY),
        _n("Service Window", "id"),
        _n("Service Window Start Time"),
        _n("Service Window End Time"),
        _n(H_SR[5], "f"),
        _n(H_SR[6], "f"),
        _n("Forecast produced on", "dn", DMY),
    ],
    A_MAIN: [
        _n("Date", "dk", ISO),
        _n("Provider"),
        _n("Unit", "id"),
        _n("Delivery Start"),
        _n("Delivery End"),
        _n("EFA", "i"),
        _n("Service"),
        _n("Status"),
        _n("Volume", "f"),
        _n("Accepted Volume", "f"),
        _n("Price", "f"),
        _n("Clearing Price", "f"),
        _n("Technology Type"),
        _n("Location"),
        _n("Rank", "in"),
    ],
    A_BUY: [_n("EFA Date", "dk", ISO), _n("EFA", "i"), *(_n(b, "f") for b in BANDS)],
    F_REQ: [
        _n("EFA_DATE", "dk", DMY),
        _n("Service"),
        *(_n(f"EFA{i}", "f") for i in range(1, 7)),
    ],
    STOR: [
        _n("Date", "dk", DMY),
        _n("Season"),
        _n("WD_NWD"),
        _n("Window 1 Start Time"),
        _n("Window 1 End Time"),
        _n("Window 2 Start Time"),
        _n("Window 2 End Time"),
    ],
}
KEY_OF: dict[str, list[str]] = {
    **dict.fromkeys(
        (N_BAL, N_DX, N_QR, N_SFFR, N_SR, N_STOR),
        ["notification_issued_date_time"],
    ),
    SR: ["auction_date", "service_window"],
    A_MAIN: ["date", "unit", "efa"],
    A_BUY: ["efa_date", "efa"],
    F_REQ: ["efa_date"],
    STOR: ["date"],
}
TEMPORAL: dict[str, tuple[str, str | None]] = {
    **dict.fromkeys(TYPED_NOTIFICATIONS, ("local_instant", "notification_issued_date_time")),
    N_SFFR: ("none", None),
    SR: ("date_sp1", "service_delivery_date"),
    A_MAIN: ("date_sp1", "date"),
    A_BUY: ("date_sp1", "efa_date"),
    F_REQ: ("date_sp1", "efa_date"),
    STOR: ("date_sp1", "date"),
}
HEADER_OF = {
    N_BAL: H_N6,
    N_DX: H_N6,
    N_QR: H_N5,
    N_SFFR: H_N6,
    N_SR: H_N5,
    N_STOR: H_N6,
    SR: H_SR,
    A_MAIN: H_A1,
    A_BUY: H_A2,
    F_REQ: H_F1,
    STOR: H_W1,
}
BOM_ALIASES = {"n1", "n2", "n3", "n5", "c1", "f1", "w1"}


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
    "n1": Capture(
        N_BAL,
        "13f8f6b1-690b-4695-993e-dbc26f9bbe54",
        "Balancing Reserve Important Industry Notifications",
        "balancing-reserve-important-industry-notifications.csv",
        "2026-09-04T10:50:39.380805",
        "2026-10-08T08:16:01.889466+00:00",
    ),
    "n2": Capture(
        N_DX,
        "5d6c5cf8-7f18-43d9-ab8c-ba3f88352f30",
        "DC, DM, DR Important Industry Notifications",
        "dc-dm-dr-important-industry-notifications.csv",
        "2026-08-19T15:12:17.130148",
        "2026-10-08T08:16:06.549798+00:00",
    ),
    "n3": Capture(
        N_QR,
        "a72085a8-e167-406d-8504-ed72d7e142af",
        "Quick Reserve Important Industry Notifications",
        "qr-important-industry-notifications.csv",
        "2026-08-19T15:11:33.729145",
        "2026-10-08T08:16:11.641248+00:00",
    ),
    "n4": Capture(
        N_SFFR,
        "9b51636d-fca5-4d00-946b-4e19770f1904",
        "SFFR Important Industry Notifications",
        "sffr-important-industry-notifications.csv",
        "2024-03-01T10:05:43.277634",
        "2026-10-08T08:16:15.924235+00:00",
    ),
    "n5": Capture(
        N_SR,
        "11a90fe4-27c1-46b5-9619-016f2752413d",
        "Slow Reserve Important Industry Notification",
        "sr-important-industry-notifications.csv",
        "2026-08-19T15:11:10.135838",
        "2026-10-08T08:16:21.093292+00:00",
    ),
    "n6": Capture(
        N_STOR,
        "1dbebd85-6b42-4d8f-8542-16a83d17dc14",
        "STOR Important Industry Notifications",
        "stor-important-industry-notifications.csv",
        "2025-04-01T07:32:28.323998",
        "2026-10-08T08:16:24.805499+00:00",
    ),
    "c1": Capture(
        CT,
        "dbd7f746-07c9-4804-8ab2-9bec2d7ef0ec",
        "Contract Transfer of Obligation 2025-2026",
        "contract-transfers.csv",
        "2026-10-05T09:45:24.439961",
        "2026-10-08T09:15:55.090300+00:00",
    ),
    "s1": Capture(
        SR,
        "ccde1a9e-1885-4271-84a3-bb064194bce9",
        "Slow Reserve Requirement Forecast",
        "slow-reserve-requirement-forecast.csv",
        "2026-09-22T14:48:32.823004",
        "2026-10-08T11:24:21.214722+00:00",
    ),
    "a1": Capture(
        A_MAIN,
        "76bc5d39-c39f-490b-984e-58eb8ff296ef",
        "Static Firm Frequency Response Auction Results",
        "static-firm-frequency-response-auction-results_76bc5d39-c39f-490b-984e-58eb8ff296ef_patch.csv",
        "2026-10-08T10:16:13.970875",
        "2026-10-08T11:17:37.751883+00:00",
    ),
    "a2": Capture(
        A_BUY,
        "f9f4b295-6de6-4379-9211-b0b2117a0c48",
        "Static Firm Frequency Response Auction Buy Orders",
        "static-firm-frequency-response-buy-order_f9f4b295-6de6-4379-9211-b0b2117a0c48_patch.csv",
        "2026-10-08T10:16:20.134250",
        "2026-10-08T11:17:33.484019+00:00",
    ),
    "f1": Capture(
        F_REQ,
        "bf236101-7167-4351-9fa0-c0b2e7c85747",
        "Static Firm Frequency Response Requirement",
        "sffr-data.csv",
        "2026-09-22T14:48:22.737387",
        "2026-10-08T11:17:40.964081+00:00",
    ),
    "w1": Capture(
        STOR,
        "90d4a1a4-d311-4a22-9138-84cb7ead2599",
        "STOR Windows",
        "stor_committed_windows.csv",
        "2026-07-27T14:25:54.738965",
        "2026-10-08T11:27:43.094353+00:00",
    ),
}
FIRST_OF = {
    N_BAL: "n1",
    N_DX: "n2",
    N_QR: "n3",
    N_SFFR: "n4",
    N_SR: "n5",
    N_STOR: "n6",
    SR: "s1",
    A_MAIN: "a1",
    A_BUY: "a2",
    F_REQ: "f1",
    STOR: "w1",
}
RECORD_ALIASES = list(FIRST_OF.values())


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
        prefix="as2", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: CRLF terminators, a BOM and the
    notification body's lone LF inside a quoted cell are the bronze original's bytes)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8-sig")
    return [r for r in csv.DictReader(io.StringIO(text, newline="")) if any(r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8-sig").split("\r\n", 1)[0]
    return next(csv.reader([first]))


def capture(data: Path, alias: str, *, raw: bytes | None = None) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    slug = PACKAGE_OF[meta.family]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=slug,
        package_id=PACKAGES[slug],
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


def _package_doc(slug: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / f"{slug}.json").read_text("utf-8"))
    return document


def _columns(key: str) -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record(key)))
        if c not in ("year", "month")
    ]


def _anchor(day: date) -> datetime:
    """The UTC instant of local midnight on ``day`` (the label anchor ``date_sp1``)."""
    return datetime(day.year, day.month, day.day, tzinfo=LONDON).astimezone(UTC)


def _london(label: str) -> datetime:
    """A ``dd/mm/YYYY HH:MM`` UK-local label as the tz-aware UTC instant."""
    return datetime.strptime(label, DT).replace(tzinfo=LONDON).astimezone(UTC)


def _expected(spec: Spec, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank cell is null)."""
    _source, _name, kind, fmt = spec
    if kind == "f":
        return float(cell) if cell != "" else None
    if kind in ("i", "in"):
        return int(cell) if cell != "" else None
    if kind in ("dk", "dn"):
        return datetime.strptime(cell, fmt).date() if cell != "" else None
    if kind in ("tk", "tn"):
        return _london(cell) if cell != "" else None
    return cell if cell != "" else None


def _assert_cells(family: str, frame: pl.DataFrame, source_rows: list[dict[str, str]]) -> None:
    """Every identifier, label, date, instant and measure of ``frame`` equals the body cell."""
    for spec in COLS[family]:
        assert frame[spec[1]].to_list() == [_expected(spec, r[spec[0]]) for r in source_rows], spec[
            1
        ]


def _load_clean(data: Path, alias: str) -> pl.DataFrame:
    """Capture and run the alias: every row written, none excluded, the generic output columns,
    one completion for the capture, the temporal column tz-aware UTC."""
    family = CAPTURES[alias].family
    capture_id = capture(data, alias)
    written = get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert written == len(rows(alias))
    completion = read_completion(data, family, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
    assert read_failure(data, family, capture_id) is None
    frame = _silver(data, family)
    assert frame.height == written
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(family)
    assert not [c for c in frame.columns if "﻿" in c]
    assert frame.schema["timestamp_utc"] == pl.Datetime("us", "UTC")
    return frame


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header, the
    BOM on exactly the seven uploads that carry it, CRLF row terminators, the balancing
    notification's embedded newline and padded description, GMT and BST issue labels in the STOR
    notifications, the SFFR singleton's example note, the contract body's duplicate rows, the
    autumn windows 5 / 5x / 6 / 6x in the Slow Reserve cut, a rejected zero-accepted row and a
    blank technology type in the auction results cut, and seasons 15.1 and 19.6 in the STOR
    windows cut."""
    assert set(CAPTURES) == set(FIRST_OF.values()) | {"c1"}
    for alias, meta in CAPTURES.items():
        if meta.family != CT:
            assert header(alias) == HEADER_OF[meta.family], alias
        raw = body(alias)
        assert raw.startswith(BOM) == (alias in BOM_ALIASES), alias
        assert raw.endswith(b"\r\n"), alias
    (padded,) = [r for r in rows("n1") if "\n" in r["Description"]]
    assert (
        padded["Description"].endswith("\n")
        and padded["Description"] != padded["Description"].strip()
    )
    assert [r["Notes"] for r in rows("n4")] == ["Example note"]
    assert [r["Notification Issued Date Time"] for r in rows("n4")] == ["01/03/2024 10:00"]
    assert {"01/04/2025 09:00", "07/02/2025 11:20"} <= {
        r["Notification Issued Date Time"] for r in rows("n6")
    }
    full_rows = [tuple(r.values()) for r in rows("c1")]
    assert len(full_rows) == 12 and len(set(full_rows)) == 7
    assert {r["Service Window"] for r in rows("s1")} >= {"5", "5x", "6", "6x"}
    assert any(r["Status"] == "Rejected" and r["Accepted Volume"] == "0.0" for r in rows("a1"))
    assert any(r["Technology Type"] == "" for r in rows("a1"))
    assert {r["Season"] for r in rows("w1")} == {"15.1", "19.6"}


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_selection_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time from a notification or production label, a
    ``key_latest`` selection with no vendor identity, a trim or an epoch other than the vendor
    header: every record is version 1, utf-8 CSV, whole-capture selection, ``ckan_last_modified``
    vintage, the spec's entity key, the vendor header as its single epoch and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.vintage == "ckan_last_modified" and record.vintage_evidence is None
    assert record.latest == "whole_capture" and record.latest_partition is None
    assert list(record.entity_key) == KEY_OF[key]
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None
    (epoch,) = record.epochs
    assert epoch.issue.kind == "none" and epoch.unpivot is None
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)
    assert list(epoch.header) == HEADER_OF[key] == header(FIRST_OF[key])


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: free text, codes,
    window and delivery clocks and the season are strings, measures are nullable float64 (zero is a
    value), ``EFA`` and ``Rank`` are int64 (the vendor's dimensionless block id and ordinal), dates
    parse in the body's own format, the notification labels are Europe/London datetimes
    (``ambiguous: raise``, with evidence), and no column declares ``trim``, ``null_tokens``,
    ``formats_by_filename`` or a bound."""
    columns = _record(key).epochs[0].columns
    assert [c.source for c in columns] == [s[0] for s in COLS[key]]
    assert [c.name for c in columns] == [s[1] for s in COLS[key]]
    for column, (_source, _name, kind, fmt) in zip(columns, COLS[key], strict=True):
        assert column.trim is None and column.null_tokens == ()
        assert (column.min, column.max) == (None, None) and column.formats_by_filename is None
        if kind in ("tk", "tn"):
            assert (column.dtype, column.format, column.nullable) == ("datetime", fmt, kind == "tn")
            assert (column.zone, column.ambiguous) == ("Europe/London", "raise")
            assert column.zone_evidence and "UK local time" in column.zone_evidence
            continue
        assert column.zone is None and column.zone_evidence is None and column.ambiguous is None
        if kind == "id":
            assert (column.dtype, column.nullable, column.format) == ("string", False, None)
        elif kind == "s":
            assert (column.dtype, column.nullable, column.format) == ("string", True, None)
        elif kind == "f":
            assert (column.dtype, column.nullable) == ("float64", True), column.name
        elif kind in ("i", "in"):
            assert (column.dtype, column.nullable) == ("int64", kind == "in")
        else:
            assert (column.dtype, column.format, column.nullable) == ("date", fmt, kind == "dn")


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_issue_label_or_the_date_anchor_never_a_composed_block(
    key: str,
) -> None:
    """Detects an EFA / window start fabricated from a date + label (the engine cannot compose
    one, RULINGS 572/588): the typed notifications take ``local_instant`` of their issue label,
    the SFFR notification singleton takes ``none`` (its slash date order is unsettled), and the
    Slow Reserve forecast, the two SFFR result tables, the SFFR requirement and the STOR windows
    anchor on their date label with ``date_sp1``."""
    kind, column = TEMPORAL[key]
    recipe = _record(key).temporal
    expected_column = column if kind == "local_instant" else None
    expected_date = column if kind == "date_sp1" else None
    assert (recipe.kind, recipe.date_column, recipe.column, recipe.period_column) == (
        kind,
        expected_date,
        expected_column,
        None,
    )


@pytest.mark.parametrize("key", [SR, A_MAIN, STOR])
def test_window_and_delivery_clocks_are_raw_strings(key: str) -> None:
    """Detects a parsed window or delivery clock (the autumn labels 5 / 5x, 6 / 6x repeat one
    local start, so a constant ``ambiguous`` policy would misplace one of each pair; the SFFR
    delivery clock zone is undeclared; the STOR window labels are bare ``HH:MM``): every such
    clock is a string."""
    types = {c.name: c.dtype for c in _record(key).epochs[0].columns}
    clocks = [n for n in types if "time" in n or n in ("delivery_start", "delivery_end")]
    assert clocks and all(types[n] == "string" for n in clocks)


def test_the_sffr_notification_keeps_every_label_raw_and_the_buy_bands_use_the_profiler_names() -> (
    None
):
    """Detects a guessed date order on the SFFR notification (``01/03/2024`` is day-first or
    month-first) or an invented band column name: the singleton's three labels are strings and the
    ten buy-order band columns are the profiler's ``silver_name`` of the numeric header
    (``c_25`` ... ``c_250``)."""
    types = {c.name: c.dtype for c in _record(N_SFFR).epochs[0].columns}
    assert {types[n] for n in types} == {"string"}
    assert [c.name for c in _record(A_BUY).epochs[0].columns][2:] == [f"c_{b}" for b in BANDS]


def test_season_is_a_string_and_the_efa_and_rank_are_int64_ordinals() -> None:
    """Detects the STOR ``Season`` typed as a float (``19.10`` would equal ``19.1``), and ``EFA`` /
    ``Rank`` typed as measures: seasons are strings, EFA and Rank are int64 identifiers."""
    stor = {c.name: c.dtype for c in _record(STOR).epochs[0].columns}
    assert stor["season"] == "string" and stor["wd_nwd"] == "string"
    main = {c.name: c.dtype for c in _record(A_MAIN).epochs[0].columns}
    assert (main["efa"], main["rank"]) == ("int64", "int64")


@pytest.mark.parametrize("key", list(HELD))
def test_two_records_are_held_with_the_verbatim_question_and_the_package_stays_eligible(
    key: str,
) -> None:
    """Detects a hold lost, reworded or moved to the package: each of the two records carries its
    verbatim question and this unit, is effectively held, and its package stays eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == (UNIT, HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", ELIGIBLE)
def test_nine_records_are_eligible(key: str) -> None:
    """Detects a hold on a clean family: the nine eligible families carry no eligibility override
    and are effectively eligible."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_contract_transfer_obligation_has_no_record() -> None:
    """Detects ``contract_transfer_obligation`` gaining a record (RULINGS 538). Its 12 captured
    rows hold 7 distinct rows and 5 duplicate excess, so even the key made of every source column
    fails the engine's duplicate guard: no lossless key exists and silent deduplication or a row
    ordinal is forbidden. The family stays ingest-only (no transformer is generated).
    TODO: CONTRACT-TRANSFER-IDENTITY (a vendor transfer identifier or a revision carrying
    distinguishing columns settles it); TODO: CONTRACT-TRANSFER-PERIOD (clock, endpoint closure and
    omitted-time meaning of the date-only delivery labels)."""
    package, entry = registry_module.load_registry().families[CT]
    assert entry.record is None and entry.transformer is None
    assert package.package == "contract-transfer-of-obligation"
    full_rows = [tuple(r.values()) for r in rows("c1")]
    assert (len(full_rows), len(set(full_rows))) == (12, 7)
    assert all(r["auctionUnit"] == "" for r in rows("c1"))
    assert any(r["Transfer To Unit"].endswith(" ") for r in rows("c1"))
    with pytest.raises(ValueError, match="No transformer registered"):
        get_transformer(SOURCE, CT, Path("__test__"))
    line = [ln for ln in eligibility_module.render(load_registry()).splitlines() if f"`{CT}`" in ln]
    assert len(line) == 1 and "ingest-only (no silver output)" in line[0]


def test_the_package_files_carry_exactly_the_eleven_records_and_no_ledger_entry() -> None:
    """Detects a record on the wrong family, a family gaining a transformer, or a reconcile
    adjudication this batch does not need: the six package files hold exactly the eleven records
    (none in the contract-transfer package), no family in them has a bespoke transformer, every
    resource stays a SILVER disposition of its family and the reconcile ledger names none of
    them."""
    found: set[str] = set()
    for slug in PACKAGES:
        document = _package_doc(slug)
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), slug
        assert all(r["disposition"]["kind"] == "SILVER" for r in document["resources"]), slug
    assert found == set(RECORDED)
    entries = registry_module.load_reconcile_adjudications()
    assert {e.family for e in entries} & {*RECORDED, CT} == set()


def test_eleven_are_reported_as_silver_outputs_two_held_and_one_stays_ingest_only() -> None:
    """Detects a family reported as ingest-only or the wrong eligibility: the eligibility report
    lists each of the eleven as ``silver`` (nine ``eligible``, two ``held: TODO:`` with this
    unit), all ``whole_capture``."""
    report = eligibility_module.render(load_registry())
    for key in RECORDED:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver |" in line, key
        if key in HELD:
            assert "| silver | held: TODO:" in line and f"(unit {UNIT})" in line, key
        else:
            assert "| silver | eligible |" in line, key
        assert "| whole_capture |" in line


@pytest.mark.parametrize("key", RECORDED)
def test_every_family_is_generic_tabular_silver_with_its_own_record(key: str) -> None:
    """Detects a family wired to a bespoke transformer instead of the generic engine."""
    package, entry = load_registry().families[key]
    assert entry.record is not None and entry.transformer is None
    assert entry.kind == "tabular"
    assert not isinstance(package.eligibility, Held)


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", RECORD_ALIASES)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a BOM
    reaching the first silver column name, a duplicate key or an excluded row: the slice loads every
    row with zero exclusions and no failure, every identifier, label, date, instant and measure
    equals the body cell, the entity key is unique, and the temporal column is the issue-label
    instant or the date label anchor (UTC, tz-aware) or, with no recipe, the capture time."""
    family = CAPTURES[alias].family
    frame = _load_clean(data, alias)
    _assert_cells(family, frame, rows(alias))
    assert not frame.select(KEY_OF[family]).is_duplicated().any()
    kind, column = TEMPORAL[family]
    if kind == "none":
        written = datetime.fromisoformat(CAPTURES[alias].written)
        assert set(frame["timestamp_utc"].to_list()) == {written}
    elif kind == "local_instant":
        assert frame["timestamp_utc"].to_list() == frame[str(column)].to_list()
    else:
        assert frame["timestamp_utc"].to_list() == [_anchor(d) for d in frame[str(column)]]
        assert frame[str(column)].dtype == pl.Date


def test_balancing_notification_text_survives_embedded_newlines_quotes_and_padding(
    data: Path,
) -> None:
    """Detects free text normalised or trimmed (the embedded newline, the trailing newline padding,
    commas and quotes are the vendor's bytes) and the blank effective-to / notes read as anything
    but null: every ``Description`` is byte-equal to the body, the padded one keeps both newlines,
    and 12 effective-to / 8 notes cells are null (FACTS g5)."""
    frame = _load_clean(data, "n1")
    source = rows("n1")
    assert frame["description"].to_list() == [r["Description"] for r in source]
    padded = [d for d in frame["description"].to_list() if "\n" in d]
    assert len(padded) == 1 and padded[0].endswith("\n")
    assert frame["effective_to_date_time"].null_count() == 12
    assert frame["notes"].null_count() == 8
    assert frame["service"].to_list() == ["BR"] * 13


@pytest.mark.parametrize("alias", ["n2", "n6"])
def test_notification_issue_labels_convert_to_utc_across_the_clock_change(
    data: Path, alias: str
) -> None:
    """Detects a notification label read as UTC (or as a fixed +01:00): an issue, effective-from
    and effective-to label converts on the UK local clock, so a BST label is an hour earlier in UTC
    and a GMT label is unchanged, in the timestamp and in every notification instant column, and
    every instant is tz-aware UTC."""
    frame = _load_clean(data, alias)
    for name in (
        "notification_issued_date_time",
        "effective_from_date_time",
        "effective_to_date_time",
    ):
        assert frame.schema[name] == pl.Datetime("us", "UTC"), name
    issued = dict(zip(frame["notification_issued_date_time"], frame["description"], strict=True))
    labels = {r["Notification Issued Date Time"] for r in rows(alias)}
    bst = [lab for lab in labels if _is_bst(lab)]
    gmt = [lab for lab in labels if not _is_bst(lab)]
    assert bst and gmt, alias
    for label in bst:
        local = datetime.strptime(label, DT)
        assert _london(label) == local.replace(tzinfo=UTC) - timedelta(hours=1)
        assert _london(label) in issued
    for label in gmt:
        assert _london(label) == datetime.strptime(label, DT).replace(tzinfo=UTC)
        assert _london(label) in issued
    if alias == "n6":
        assert datetime(2025, 4, 1, 8, 0, tzinfo=UTC) in issued
        assert datetime(2025, 2, 7, 11, 20, tzinfo=UTC) in issued


def _is_bst(label: str) -> bool:
    """Whether the UK local ``label`` falls in British Summer Time."""
    local = datetime.strptime(label, DT).replace(tzinfo=LONDON)
    return local.utcoffset() == timedelta(hours=1)


@pytest.mark.parametrize("alias", ["n3", "n5"])
def test_quick_and_slow_reserve_notifications_have_no_service_column(
    data: Path, alias: str
) -> None:
    """Detects a fabricated ``service`` column on the two H-N5 notification tables (the body has
    none): the silver columns are the five source columns plus the engine's."""
    frame = _load_clean(data, alias)
    assert "service" not in frame.columns
    assert "Service" not in header(alias)
    assert frame["notes"].null_count() >= 1


def test_sffr_notification_loads_raw_with_the_capture_time(data: Path) -> None:
    """Detects the SFFR singleton's ambiguous slash dates parsed or its ``Example note`` dropped:
    the three labels load as the body strings, the note is kept, ``timestamp_utc`` is the capture
    time and the description is null."""
    frame = _load_clean(data, "n4")
    assert frame.height == 1
    assert frame["notification_issued_date_time"].to_list() == ["01/03/2024 10:00"]
    assert frame["effective_from_date_time"].to_list() == ["03/03/2024 05:00"]
    assert frame["effective_to_date_time"].to_list() == ["03/03/2024 07:30"]
    assert frame["notes"].to_list() == ["Example note"]
    assert frame["description"].to_list() == [None]
    assert frame.schema["notification_issued_date_time"] == pl.String


def test_slow_reserve_autumn_windows_all_survive_with_distinct_keys_and_raw_labels(
    data: Path,
) -> None:
    """Detects the repeated autumn window (5 / 5x, 6 / 6x share one local start label) dropped,
    merged on the start label or its clock parsed: all four windows of 2026-10-25 load as distinct
    rows keyed ``(auction_date, service_window)``, the start / end labels are byte-equal to the body
    (two rows show ``25/10/2026 01:00``, two ``25/10/2026 01:30``), the requirement cells stay MW
    floats, and the delivery-date anchor is local midnight of the clock-change day (23:00 UTC the
    evening before)."""
    frame = _load_clean(data, "s1")
    autumn = frame.filter(pl.col("service_delivery_date") == date(2026, 10, 25))
    assert autumn.height == 50
    windows = autumn["service_window"].to_list()
    assert {"4", "5", "5x", "6", "6x", "7"} <= set(windows) and len(set(windows)) == 50
    by_window = dict(zip(windows, autumn["service_window_start_time"], strict=True))
    assert by_window["5"] == by_window["5x"] == "25/10/2026 01:00"
    assert by_window["6"] == by_window["6x"] == "25/10/2026 01:30"
    assert autumn["service_window_end_time"].to_list() == [
        r["Service Window End Time"]
        for r in rows("s1")
        if r["Service Delivery Date"] == "25/10/2026"
    ]
    assert not frame.select(["auction_date", "service_window"]).is_duplicated().any()
    assert set(autumn["timestamp_utc"].to_list()) == {datetime(2026, 10, 24, 23, 0, tzinfo=UTC)}
    assert frame.schema["service_window_start_time"] == pl.String
    assert frame.schema["positive_slow_reserve_requirement_mw"] == pl.Float64
    assert set(frame["forecast_produced_on"].to_list()) == {date(2026, 8, 31), date(2026, 9, 22)}


def test_auction_results_keep_the_rejected_zero_and_the_blank_technology_type(
    data: Path,
) -> None:
    """Detects a zero accepted volume read as missing or a sentinel (36,999 rejected bids carry
    0.0: they are results), a blank technology type or location filled, a delivery clock parsed
    or the EFA / rank id typed as a measure: the rejected rows keep ``0.0``, the blank cells are
    null, delivery start / end are byte-equal strings, EFA and Rank are int64, and the date anchors
    on local midnight (2023-04-01 is 23:00 UTC the evening before, BST)."""
    frame = _load_clean(data, "a1")
    rejected = frame.filter(pl.col("status") == "Rejected")
    assert rejected.height == 2 and rejected["accepted_volume"].to_list() == [0.0, 0.0]
    assert frame["accepted_volume"].null_count() == 0
    blank = frame.filter(pl.col("unit") == "EVZE-01")
    assert blank["technology_type"].to_list() == [None] and blank["location"].to_list() == [None]
    assert frame["delivery_start"].to_list() == [r["Delivery Start"] for r in rows("a1")]
    assert frame.schema["delivery_start"] == pl.String
    assert (frame.schema["efa"], frame.schema["rank"]) == (pl.Int64, pl.Int64)
    assert frame.schema["clearing_price"] == pl.Float64
    first = frame.filter(pl.col("date") == date(2023, 4, 1))
    assert set(first["timestamp_utc"].to_list()) == {datetime(2023, 3, 31, 23, 0, tzinfo=UTC)}


def test_buy_orders_load_wide_bands_as_floats_even_though_they_are_held(data: Path) -> None:
    """Detects the band columns renamed, typed as integers or reshaped: the ten ``c_25`` ...
    ``c_250`` columns are float64 cells equal to the body, the key is ``(efa_date, efa)``, and the
    hold changes publication only, not the transform."""
    frame = _load_clean(data, "a2")
    assert [c for c in frame.columns if c.startswith("c_")] == [f"c_{b}" for b in BANDS]
    assert all(frame.schema[f"c_{b}"] == pl.Float64 for b in BANDS)
    assert frame.height == 6 and frame["efa"].to_list() == [1, 2, 3, 4, 5, 6]
    assert isinstance(_record(A_BUY).eligibility, Held)


def test_sffr_requirement_keeps_the_efa_columns_wide_with_the_bom(data: Path) -> None:
    """Detects a BOM reaching the first column name or the EFA values reshaped or rescaled: the
    first silver column is ``efa_date``, the six EFA columns are wide float64 MW and each date
    anchors on local midnight (23 September 2026 is 23:00 UTC the evening before, BST)."""
    assert body("f1").startswith(BOM)
    frame = _load_clean(data, "f1")
    assert frame.columns[0] == "efa_date"
    assert all(frame.schema[f"efa{i}"] == pl.Float64 for i in range(1, 7))
    assert {frame[f"efa{i}"].unique().to_list()[0] for i in range(1, 7)} == {250.0}
    first = frame.filter(pl.col("efa_date") == date(2026, 9, 23))
    assert set(first["timestamp_utc"].to_list()) == {datetime(2026, 9, 22, 23, 0, tzinfo=UTC)}


def test_stor_seasons_load_as_strings_and_the_window_clocks_stay_raw(data: Path) -> None:
    """Detects ``Season`` read as a float (``15.1``, ``19.6`` become ``15.1`` / ``19.6`` numbers)
    or a window clock parsed: the seasons are the strings ``15.1`` and ``19.6``, ``WD`` / ``NWD``
    are byte-equal, the four window clocks are the body's ``HH:MM`` strings and the BOM does not
    reach the first column."""
    frame = _load_clean(data, "w1")
    assert frame.columns[0] == "date"
    assert frame.schema["season"] == pl.String
    assert frame["season"].to_list() == ["15.1", "15.1", "19.6", "19.6"]
    assert frame["wd_nwd"].to_list() == [r["WD_NWD"] for r in rows("w1")]
    assert frame["window_1_start_time"].to_list() == [r["Window 1 Start Time"] for r in rows("w1")]
    assert frame.schema["window_2_end_time"] == pl.String


# --------------------------------------------------------------------------- #
# Loud failure, vintage and catalogue
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epoch_fails_loud(data: Path, family: str) -> None:
    """Detects a silent header drift (a renamed vendor column loaded under the old typing): a body
    whose first header cell is renamed fails the capture with no completion and no silver."""
    alias = FIRST_OF[family]
    raw = body(alias)
    first_cell = HEADER_OF[family][0].encode()
    renamed = raw.replace(first_cell, first_cell + b"_x", 1)
    assert renamed != raw
    capture_id = capture(data, alias, raw=renamed)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("family", RECORDED)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, family: str) -> None:
    """Detects a duplicate key loaded silently: the last data row repeated fails the capture with
    ``DuplicateEntityKeyError``, loads nothing and leaves no completion."""
    alias = FIRST_OF[family]
    terminated = body(alias).split(b"\r\n")
    assert terminated[-1] == b""
    raw = b"\r\n".join([*terminated[:-1], terminated[-2], b""])
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["n2", "s1", "a2", "w1"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded(data: Path, alias: str) -> None:
    """Detects an issue-time proxy from a notification or production label or a catalogue view that
    cannot carry the new columns: ``available_at`` is the CKAN ``last_modified`` read as UTC (not
    the issue label, not the capture time); an as-of read before the vintage serves nothing and one
    after serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(microseconds=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, meta.family, before) == []
    assert set(both_as_of(db, data, meta.family, after)) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}


@pytest.mark.parametrize("alias", ["s1", "f1"])
def test_the_forecast_captures_lie_wholly_after_the_vintage(alias: str) -> None:
    """Detects an eligible forecast record whose captured targets are not all forward (RULINGS
    529's exception): every delivery / EFA date in the Slow Reserve and SFFR requirement captures is
    after the CKAN ``last_modified`` date."""
    meta = CAPTURES[alias]
    vintage = datetime.fromisoformat(meta.modified).date()
    for spec in COLS[meta.family]:
        if spec[1] in ("service_delivery_date", "efa_date"):
            targets = [_expected(spec, r[spec[0]]) for r in rows(alias)]
            assert all(t > vintage for t in targets), alias


@pytest.mark.parametrize("slug", [s for s in PACKAGES if s != "contract-transfer-of-obligation"])
def test_the_skeleton_pages_render_the_new_records(slug: str) -> None:
    """Detects a record the docs generator cannot render (the held questions, the backticks, the
    numeric band headers)."""
    page = skeleton.render_package(
        load_registry(),
        {
            "name": slug,
            "title": slug,
            "organization": {"title": "NESO"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    for key in (k for k in RECORDED if PACKAGE_OF[k] == slug):
        assert f"`{key}`" in page, key
