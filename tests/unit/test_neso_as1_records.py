"""The ancillary-services batch 1's nine frozen records: four eligible, five held (v0.22-K-AS-1).

``br_auction_requirement_fc_da``, ``dm_requirements``, ``dr_requirements`` and
``qr_auction_requirement_fc`` are eligible (every captured target lies after the capture vintage).
``br_auction_requirement_fc_da_archive``, ``br_auction_requirement_fc_medium_term``,
``dc_forecast_4d``, ``dc_forecast_4d_history`` and ``dc_dm_dr_long_term_fc`` carry a record whose
eligibility is **held**. Every test writes recorded fixture captures (cuts of the 2026-10-08 swept
bronze under ``tests/fixtures/neso_data_portal/as1/``, provenance in ``PROVENANCE.md``) into a short
data root and runs the transformer the **real package registry** generates, so a record that does
not fit its vendor body fails here, not at activation. On master none of the nine families has a
record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-AS-1-FACTS g1-g5 and section 5, K-AS-1-SPEC):

- ``issue: none`` everywhere. ``Forecast produced on`` / ``Forecast_Created`` / ``Forecast Created``
  are creation **date** labels with no clock or publication instant: ordinary columns, never an
  ``issue_time``, never promoted to a midnight instant. Vintage is ``ckan_last_modified`` (every
  resource is an upload, ADR-035).
- EFA blocks and reserve service windows are labelled on the UK local clock and the engine cannot
  compose a date + block or date + window label into a row-specific instant (the autumn windows
  ``5``/``5x``, ``6``/``6x`` repeat one local start label), so the BR / QR window start / end clocks
  are **raw strings**, the EFA requirement columns stay **wide**, and the temporal recipe is a
  ``date_sp1`` / ``month`` **label anchor** (the first settlement-period start of the date, not EFA
  1's or window 1's start).
- Zeros stay zero, service / product codes (``DC-H``, ``DML``, ``DR_L``, ...) and window labels
  (``5x``) are byte-equal to the body, units are MW except ``Possibilities`` (unitless, 0-1) and
  ``EFA`` (a categorical block); no trim, no token map, no ``run_type``.
- ``br_auction_requirement_fc_medium_term`` serves two resources (archive B3 and current B4) and so
  selects per resource (``latest_partition: resource_id``, ``resource_id`` in the key, ADR-039). Its
  MW unit is the shared Balancing Reserve requirement definition (FACTS section 4.3: the medium-term
  field dictionary is blank), an evidence transfer recorded here, not guessed from magnitudes.
- ``dm_requirements``: the vendor ``Service`` dictionary text describes DR, not DM. TODO: NESO to
  correct the DM field dictionary; the DM codes ``DML`` / ``DMH`` are kept and never relabelled DR.
- ``dc_forecast_4d`` keeps ``Date`` raw with temporal ``none`` (every captured slash date is
  ambiguous); ``dc_forecast_4d_history`` keeps the mixed-format ``Forecast_Created`` raw and keys on
  it, so a target with several issues loads every issue.

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the CRLF of the
five upload originals that carry it (a BOM stays); the BR and long-term originals are LF."""

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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as1"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-AS-1"
BOM = b"\xef\xbb\xbf"
LONDON = ZoneInfo("Europe/London")
ISO = "%Y-%m-%d"
DMY = "%d/%m/%Y"
DASH = "—"

BR_DA = "br_auction_requirement_fc_da"
BR_ARCHIVE = "br_auction_requirement_fc_da_archive"
BR_MT = "br_auction_requirement_fc_medium_term"
DC = "dc_forecast_4d"
DC_HIST = "dc_forecast_4d_history"
DM = "dm_requirements"
DR = "dr_requirements"
LT = "dc_dm_dr_long_term_fc"
QR = "qr_auction_requirement_fc"

HELD = {
    BR_ARCHIVE: (
        f"TODO: BR-ARCHIVE-AVAILABILITY {DASH} archive rows carry only a production date (944 "
        "labels, leads of 2 and 15 days); when was each forecast published, and are archived "
        "values ever amended?"
    ),
    BR_MT: (
        f"TODO: BR-MT-ARCHIVE-AVAILABILITY {DASH} the archive resource's targets are past (46,349 "
        "window starts on/before the vintage) and its production dates carry no publication "
        "instant; when was each medium-term forecast available?"
    ),
    DC: (
        f"TODO: DC-DATE-ORDER {DASH} `Date` is published as dd/mm/yyyy or mm/dd/yyyy? The "
        "dictionary documents ISO dates; every captured value is ambiguous."
    ),
    DC_HIST: (
        f"TODO: DC-HISTORY-ISSUE {DASH} when was each creation-labelled issue published? 8 rows "
        "are created after the upload's last-modified date and 15 creation dates are missing "
        "from the archive."
    ),
    LT: (
        f"TODO: DX-LT-INTERVAL {DASH} what unit and which inclusive endpoint do the `Interval` "
        "bands (`0-200` … `1600-1800`) use, and when was the creation-dated forecast "
        "available? 324 rows target the creation month."
    ),
}
ELIGIBLE = (BR_DA, DM, DR, QR)
RECORDED = (*ELIGIBLE, *HELD)

PACKAGES = {
    "balancing-reserve-auction-requirement-forecast": "6b31e0f0-f711-461c-84bb-4ed4775a9992",
    "dynamic-containment-4-day-forecast": "c8cd1e99-bc7e-454c-8ac1-ebd9264f4d0f",
    "dynamic-moderation-requirements": "c6e7dc26-64c3-4651-a664-16a8512768ab",
    "dynamic-regulation-requirements": "bcc9b338-00a4-4858-a1ed-0b7728048462",
    "long-term-forecasts-for-dc-dm-dr-requirements": "a1256c20-f657-4186-b288-e2c238a394c0",
    "quick-reserve-auction-requirement-forecast": "7247a5da-563e-4159-b8c1-e388e39ff5a2",
}
PACKAGE_OF = {
    BR_DA: "balancing-reserve-auction-requirement-forecast",
    BR_ARCHIVE: "balancing-reserve-auction-requirement-forecast",
    BR_MT: "balancing-reserve-auction-requirement-forecast",
    DC: "dynamic-containment-4-day-forecast",
    DC_HIST: "dynamic-containment-4-day-forecast",
    DM: "dynamic-moderation-requirements",
    DR: "dynamic-regulation-requirements",
    LT: "long-term-forecasts-for-dc-dm-dr-requirements",
    QR: "quick-reserve-auction-requirement-forecast",
}

H_BR = [
    "Auction Date",
    "Service Delivery Date",
    "Service Window",
    "Service window start time",
    "Service Window End Time",
    "Positive Balancing Reserve Requirement MW",
    "Negative Balancing Reserve Requirement MW",
    "Forecast produced on",
    "Forecast type",
]
H_DC = ["Date", "Service_Type", *(f"EFA_{i}" for i in range(1, 7))]
H_DCH = [
    "Forecast_Created",
    "Forecast_Target_Date",
    "Service_Type",
    *(f"EFA_{i}" for i in range(1, 7)),
]
H_DX = ["EFA_DATE", "Service", *(f"EFA{i}" for i in range(1, 7))]
H_LT = [
    "Forecast Created",
    "Forecast Target Month",
    "Service Type",
    "EFA",
    "Interval",
    "Possibilities",
]
H_QR = [
    "Auction Date",
    "Service Delivery Date",
    "Service Window",
    "Service window Start Time",
    "Service Window End Time",
    "Positive Quick Reserve Requirement MW",
    "Negative Quick Reserve Requirement MW",
    "Forecast produced on",
]

# (source, silver name, kind, date format): "id" non-null string; "s" nullable string; "f" nullable
# float64; "dk" non-null date; "dn" nullable date; "i" non-null int64.
Spec = tuple[str, str, str, str]


def _efa(prefix: str, label: str) -> list[Spec]:
    return [(f"{label}{i}", f"{prefix}{i}", "f", "") for i in range(1, 7)]


BR_COLS: list[Spec] = [
    ("Auction Date", "auction_date", "dk", ISO),
    ("Service Delivery Date", "service_delivery_date", "dk", ISO),
    ("Service Window", "service_window", "id", ""),
    ("Service window start time", "service_window_start_time", "s", ""),
    ("Service Window End Time", "service_window_end_time", "s", ""),
    (H_BR[5], "positive_balancing_reserve_requirement_mw", "f", ""),
    (H_BR[6], "negative_balancing_reserve_requirement_mw", "f", ""),
    ("Forecast produced on", "forecast_produced_on", "dn", ISO),
    ("Forecast type", "forecast_type", "s", ""),
]
COLS: dict[str, list[Spec]] = {
    BR_DA: BR_COLS,
    BR_ARCHIVE: BR_COLS,
    BR_MT: BR_COLS,
    DC: [
        ("Date", "date", "id", ""),
        ("Service_Type", "service_type", "id", ""),
        *_efa("efa_", "EFA_"),
    ],
    DC_HIST: [
        ("Forecast_Created", "forecast_created", "id", ""),
        ("Forecast_Target_Date", "forecast_target_date", "dk", DMY),
        ("Service_Type", "service_type", "id", ""),
        *_efa("efa_", "EFA_"),
    ],
    DM: [
        ("EFA_DATE", "efa_date", "dk", DMY),
        ("Service", "service", "id", ""),
        *_efa("efa", "EFA"),
    ],
    DR: [
        ("EFA_DATE", "efa_date", "dk", DMY),
        ("Service", "service", "id", ""),
        *_efa("efa", "EFA"),
    ],
    LT: [
        ("Forecast Created", "forecast_created", "dk", ISO),
        ("Forecast Target Month", "forecast_target_month", "dk", ISO),
        ("Service Type", "service_type", "id", ""),
        ("EFA", "efa", "i", ""),
        ("Interval", "interval", "id", ""),
        ("Possibilities", "possibilities", "f", ""),
    ],
    QR: [
        ("Auction Date", "auction_date", "dk", DMY),
        ("Service Delivery Date", "service_delivery_date", "dk", DMY),
        ("Service Window", "service_window", "id", ""),
        ("Service window Start Time", "service_window_start_time", "s", ""),
        ("Service Window End Time", "service_window_end_time", "s", ""),
        (H_QR[5], "positive_quick_reserve_requirement_mw", "f", ""),
        (H_QR[6], "negative_quick_reserve_requirement_mw", "f", ""),
        ("Forecast produced on", "forecast_produced_on", "dn", DMY),
    ],
}
KEY_OF: dict[str, list[str]] = {
    BR_DA: ["auction_date", "service_window"],
    BR_ARCHIVE: ["auction_date", "service_window"],
    BR_MT: ["resource_id", "auction_date", "service_window"],
    DC: ["date", "service_type"],
    DC_HIST: ["forecast_created", "forecast_target_date", "service_type"],
    DM: ["efa_date", "service"],
    DR: ["efa_date", "service"],
    LT: ["forecast_created", "forecast_target_month", "service_type", "efa", "interval"],
    QR: ["auction_date", "service_window"],
}
TEMPORAL: dict[str, tuple[str, str | None]] = {
    BR_DA: ("date_sp1", "service_delivery_date"),
    BR_ARCHIVE: ("date_sp1", "service_delivery_date"),
    BR_MT: ("date_sp1", "service_delivery_date"),
    DC: ("none", None),
    DC_HIST: ("date_sp1", "forecast_target_date"),
    DM: ("date_sp1", "efa_date"),
    DR: ("date_sp1", "efa_date"),
    LT: ("month", "forecast_target_month"),
    QR: ("date_sp1", "service_delivery_date"),
}
HEADER_OF = {
    BR_DA: H_BR,
    BR_ARCHIVE: H_BR,
    BR_MT: H_BR,
    DC: H_DC,
    DC_HIST: H_DCH,
    DM: H_DX,
    DR: H_DX,
    LT: H_LT,
    QR: H_QR,
}
CRLF_ALIASES = {"d1", "d2", "m1", "r1", "q1"}
BOM_ALIASES = {"m1", "r1"}


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
    "b1": Capture(
        BR_DA,
        "019f6fba-4f17-4056-9ef8-31df44ff2e30",
        "Balancing Reserve Day ahead auction requirements forecast",
        "3a27b093f23b46e4ab29472e6385cc41_balancing-reserve-day-ahead-auction-requirements-forecast.csv",
        "2026-10-07T13:05:02.227904",
        "2026-10-08T08:52:01.510461+00:00",
    ),
    "b2": Capture(
        BR_ARCHIVE,
        "b9ce03d0-c189-44ef-92e4-c02e91feb65d",
        "Balancing Reserve Day ahead auction requirement forecast (Archive)",
        "f96b7ba699ca462285e7e3ca92fdfb8a_balancing-reserve-day-ahead-auction-requirement-forecast-archiv.csv",
        "2026-10-07T13:05:08.053897",
        "2026-10-08T08:52:05.326294+00:00",
    ),
    "b3": Capture(
        BR_MT,
        "fb2e0eea-b1c0-4ba3-a817-0561182a6e26",
        "Balancing Reserve Requirements Medium term forecast (Archive)",
        "510b11f90bf7456b8d6293ff4fd62acb_balancing-reserve-requirements-medium-term-forecast-archive.csv",
        "2026-10-07T13:05:18.722316",
        "2026-10-08T08:52:08.821173+00:00",
    ),
    "b4": Capture(
        BR_MT,
        "a9470515-c1de-4c4b-9597-2d78bf24c29f",
        "Balancing Reserve Requirements Medium term forecast",
        "64375eba1e78491b83ebd7917cd0c5b3_balancing-reserve-requirements-medium-term-forecast.csv",
        "2026-10-07T13:05:12.119883",
        "2026-10-08T08:52:11.282055+00:00",
    ),
    "d1": Capture(
        DC,
        "1b85a3f3-80f0-49cf-9b0e-49648fa0cae6",
        "Dynamic Containment - 4 Day forecast",
        "f9bf31f0d49f45baa72649f0a8d2e254_dcrequirements_1b85a3f3-80f0-49cf-9b0e-49648fa0cae6_patch.csv",
        "2026-10-07T13:05:22.967331",
        "2026-10-08T10:05:17.614229+00:00",
    ),
    "d2": Capture(
        DC_HIST,
        "d5c3b48a-a0a9-4d57-a02a-ac0af09a6298",
        "Dynamic Containment 4 Day Forecast - History",
        "b4634e50af5e4d3bb7f610422776a819_dcrequirementshistorical1_d5c3b48a-a0a9-4d57-a02a-ac0af09a6298_.csv",
        "2026-10-07T13:05:28.053180",
        "2026-10-08T10:05:21.287200+00:00",
    ),
    "m1": Capture(
        DM,
        "2aae8747-776d-4fe5-af9c-adcf38f1af8a",
        "Indicative DM Requirements",
        "dm-requirements.csv",
        "2026-09-22T14:48:12.082389",
        "2026-10-08T10:10:04.991898+00:00",
    ),
    "r1": Capture(
        DR,
        "d6c576b9-91d5-4c48-bf6d-300c7d7aa6ad",
        "Indicative DR Requirements",
        "dr-requirements.csv",
        "2026-09-22T14:48:17.409242",
        "2026-10-08T10:10:08.916643+00:00",
    ),
    "l1": Capture(
        LT,
        "5162c4d1-6d72-497b-a2ad-3a2449bc8952",
        "DX Future Requirement",
        "dx_future_requirement.csv",
        "2026-04-01T13:52:35.778789",
        "2026-10-08T10:05:13.865164+00:00",
    ),
    "q1": Capture(
        QR,
        "f012de08-b258-408b-bc41-f885e183f97f",
        "Quick Reserve Auction Requirement Forecast",
        "qr.csv",
        "2026-09-22T14:48:27.491459",
        "2026-10-08T11:16:53.816423+00:00",
    ),
}
FIRST_OF = {
    BR_DA: "b1",
    BR_ARCHIVE: "b2",
    BR_MT: "b3",
    DC: "d1",
    DC_HIST: "d2",
    DM: "m1",
    DR: "r1",
    LT: "l1",
    QR: "q1",
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
        prefix="as1", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's line endings (CRLF for five uploads, LF for the
    BR and long-term ones); a BOM stays."""
    raw = (FIXTURES / f"{alias}.csv").read_bytes()
    bom = BOM if raw.startswith(BOM) else b""
    text = raw[len(bom) :].replace(b"\r\n", b"\n")
    if alias in CRLF_ALIASES:
        text = text.replace(b"\n", b"\r\n")
    return bom + text


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8-sig")
    return [r for r in csv.DictReader(io.StringIO(text, newline="")) if any(r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8-sig").splitlines()[0]
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
    """The UTC instant of local midnight on ``day`` (the label anchor ``date_sp1`` / ``month``)."""
    return datetime(day.year, day.month, day.day, tzinfo=LONDON).astimezone(UTC)


def _expected(spec: Spec, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank cell is null)."""
    _source, _name, kind, fmt = spec
    if kind == "f":
        return float(cell) if cell != "" else None
    if kind == "i":
        return int(cell)
    if kind in ("dk", "dn"):
        return datetime.strptime(cell, fmt).date() if cell != "" else None
    return cell if cell != "" else None


def _assert_cells(family: str, frame: pl.DataFrame, source_rows: list[dict[str, str]]) -> None:
    """Every identifier, label, date and measure of ``frame`` equals the body cell."""
    for spec in COLS[family]:
        assert frame[spec[1]].to_list() == [_expected(spec, r[spec[0]]) for r in source_rows], spec[
            1
        ]


def _load_clean(data: Path, *aliases: str) -> pl.DataFrame:
    """Capture and run the aliases (one family): every row written, none excluded, the generic
    output columns, one completion per capture, the temporal column tz-aware UTC."""
    (family,) = {CAPTURES[a].family for a in aliases}
    ids = {alias: capture(data, alias) for alias in aliases}
    written = get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert written == sum(len(rows(a)) for a in aliases)
    for capture_id in ids.values():
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
    BOM on exactly the two DM / DR uploads, CRLF rebuilt for the five uploads that carry it, a zero
    requirement pair in the BR archive and medium-term cuts, the autumn windows 5 / 5x / 6 / 6x in
    the Quick Reserve cut, both creation-date spellings and a target with two issues in the DC
    history cut, a zero row there, a DR_L code and a zero Possibilities in the long-term cut, and
    one medium-term cut from each of the two resources."""
    assert {a for a in CAPTURES} == set(FIRST_OF.values()) | {"b4"}
    for alias, meta in CAPTURES.items():
        assert header(alias) == HEADER_OF[meta.family], alias
        raw = (FIXTURES / f"{alias}.csv").read_bytes()
        assert raw.startswith(BOM) == (alias in BOM_ALIASES), alias
        assert (b"\r\n" in body(alias)) == (alias in CRLF_ALIASES), alias
    assert any(r["Positive Balancing Reserve Requirement MW"] == "0" for r in rows("b2"))
    assert any(r["Negative Balancing Reserve Requirement MW"] == "0" for r in rows("b3"))
    assert {r["Service Window"] for r in rows("q1")} >= {"5", "5x", "6", "6x"}
    starts = [
        r["Service window Start Time"] for r in rows("q1") if r["Service Window"] in ("5", "5x")
    ]
    assert starts == ["25/10/2026 01:00", "25/10/2026 01:00"]
    created = {r["Forecast_Created"] for r in rows("d2")}
    assert {"04/08/2026", "2026-08-06"} <= created
    keys = [(r["Forecast_Target_Date"], r["Service_Type"]) for r in rows("d2")]
    assert len(keys) > len(set(keys))
    assert any(r["EFA_1"] == "0" for r in rows("d2"))
    assert "DR_L" in {r["Service Type"] for r in rows("l1")}
    assert any(r["Possibilities"] == "0.0" for r in rows("l1"))
    assert {r["Forecast type"] for r in rows("b3")} == {"Medium term"}
    assert {r["Forecast type"] for r in rows("b4")} == {"Medium term"}
    assert {r["Auction Date"] for r in rows("b3")} != {r["Auction Date"] for r in rows("b4")}


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_selection_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time from a creation date, a run, a sibling, a trim or
    an epoch other than the vendor header: every record is version 1, utf-8 CSV, whole-capture
    selection (partitioned by resource only for the medium-term BR family), ``ckan_last_modified``
    vintage, the spec's entity key, the vendor header as its single epoch and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.vintage == "ckan_last_modified" and record.vintage_evidence is None
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key == BR_MT else None)
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
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: codes, labels and
    window clocks are strings (a creation label and the DC ``Date`` too), requirements are nullable
    float64 (zero is a value), the long-term ``EFA`` is a non-null int64, dates parse in the body's
    own format, and no column declares ``trim``, ``null_tokens``, ``formats_by_filename``, a bound
    or a zone."""
    columns = _record(key).epochs[0].columns
    assert [c.source for c in columns] == [s[0] for s in COLS[key]]
    assert [c.name for c in columns] == [s[1] for s in COLS[key]]
    for column, (_source, _name, kind, fmt) in zip(columns, COLS[key], strict=True):
        assert column.trim is None and column.null_tokens == ()
        assert (column.min, column.max) == (None, None)
        assert column.formats_by_filename is None and column.zone is None
        if kind == "id":
            assert (column.dtype, column.nullable, column.format) == ("string", False, None)
        elif kind == "s":
            assert (column.dtype, column.nullable, column.format) == ("string", True, None)
        elif kind == "f":
            assert (column.dtype, column.nullable) == ("float64", True), column.name
        elif kind == "i":
            assert (column.dtype, column.nullable) == ("int64", False)
        else:
            assert (column.dtype, column.format, column.nullable) == ("date", fmt, kind == "dn")


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_date_or_month_label_anchor_never_an_instant(key: str) -> None:
    """Detects an EFA / window start fabricated from a date + label (the engine cannot compose
    one), a ``local_instant`` on a repeating window clock, or a date promoted to an instant: the
    reserve, DM / DR and DC-history records anchor on their date label with ``date_sp1``, the
    long-term record on its target month with ``month``, and the DC current record has no recipe
    (its slash dates are ambiguous)."""
    kind, column = TEMPORAL[key]
    recipe = _record(key).temporal
    assert (recipe.kind, recipe.date_column, recipe.column, recipe.period_column) == (
        kind,
        column,
        None,
        None,
    )


@pytest.mark.parametrize("key", [BR_DA, BR_ARCHIVE, BR_MT, QR])
def test_reserve_window_clocks_are_raw_strings(key: str) -> None:
    """Detects a parsed window clock (the autumn labels 5 / 5x, 6 / 6x repeat one local start, so a
    constant ``ambiguous`` policy would misplace one of each pair): start and end are strings."""
    types = {c.name: c.dtype for c in _record(key).epochs[0].columns}
    assert types["service_window_start_time"] == types["service_window_end_time"] == "string"
    assert types["service_window"] == "string"


def test_the_dc_dates_and_creation_labels_are_raw_strings() -> None:
    """Detects a guessed date order on the DC current ``Date`` (every captured slash date is
    ambiguous) or a typed creation label on the mixed-format history column: both stay strings and
    the history key carries the creation label (never target + service alone)."""
    assert {c.name: c.dtype for c in _record(DC).epochs[0].columns}["date"] == "string"
    types = {c.name: c.dtype for c in _record(DC_HIST).epochs[0].columns}
    assert types["forecast_created"] == "string"
    assert types["forecast_target_date"] == "date"
    assert "forecast_created" in _record(DC_HIST).entity_key


def test_the_long_term_key_excludes_the_value() -> None:
    """Detects ``Possibilities`` in the identity (a changed forecast value would become a new key):
    the key is the five descriptive dimensions."""
    assert "possibilities" not in _record(LT).entity_key
    assert len(_record(LT).entity_key) == 5


@pytest.mark.parametrize("key", list(HELD))
def test_five_records_are_held_with_the_verbatim_question_and_the_package_stays_eligible(
    key: str,
) -> None:
    """Detects a hold lost, reworded or moved to the package: each of the five records carries its
    verbatim question and this unit, is effectively held, and its package stays eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == (UNIT, HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", ELIGIBLE)
def test_four_records_are_eligible(key: str) -> None:
    """Detects a hold on a clean family: the four forward-target families carry no eligibility
    override and are effectively eligible."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_the_package_files_carry_exactly_the_nine_records_and_no_ledger_entry() -> None:
    """Detects a record on the wrong family, a family gaining a transformer, or a reconcile
    adjudication this batch does not need: the six package files hold exactly the nine records, no
    family in them has a bespoke transformer, every resource stays a SILVER disposition of its
    family and the reconcile ledger names none of them."""
    found: set[str] = set()
    for slug in PACKAGES:
        document = _package_doc(slug)
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), slug
        assert all(r["disposition"]["kind"] == "SILVER" for r in document["resources"]), slug
    assert found == set(RECORDED)
    entries = registry_module.load_reconcile_adjudications()
    assert {e.family for e in entries} & set(RECORDED) == set()


def test_all_nine_are_reported_as_silver_outputs_five_held() -> None:
    """Detects a family reported as ingest-only or the wrong eligibility: the eligibility report
    lists each of the nine as ``silver``, four ``eligible`` and five ``held: TODO:`` with this
    unit, all ``whole_capture`` (the medium-term one per resource)."""
    report = eligibility_module.render(load_registry())
    for key in RECORDED:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver |" in line, key
        if key in HELD:
            assert "| silver | held: TODO:" in line and f"(unit {UNIT})" in line, key
        else:
            assert "| silver | eligible |" in line, key
        assert ("| whole_capture per resource_id" if key == BR_MT else "| whole_capture") in line


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


@pytest.mark.parametrize("alias", list(CAPTURES))
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a BOM
    reaching the first silver column name, a duplicate key or an excluded row: the slice loads every
    row with zero exclusions and no failure, every identifier, label, date and measure equals the
    body cell, the entity key is unique, and the temporal column is the date / month label anchor
    (UTC, tz-aware) or, with no recipe, the capture time."""
    family = CAPTURES[alias].family
    frame = _load_clean(data, alias)
    _assert_cells(family, frame, rows(alias))
    assert not frame.select(KEY_OF[family]).is_duplicated().any()
    kind, column = TEMPORAL[family]
    if column is None:
        written = datetime.fromisoformat(CAPTURES[alias].written)
        assert set(frame["timestamp_utc"].to_list()) == {written}
    else:
        assert frame["timestamp_utc"].to_list() == [_anchor(d) for d in frame[column].to_list()]
        assert frame[column].dtype == pl.Date


def test_balancing_reserve_zeros_stay_zero_and_distinct_issues_of_one_target_both_load(
    data: Path,
) -> None:
    """Detects a zero requirement read as missing or a sentinel (a long zero run is a value, FACTS
    g5) or the two production labels of one delivery date collapsed: the zero pair stays 0.0 in
    both columns with no null, delivery 2024-07-05 loads window 46 produced 2024-06-20 and window 1
    produced 2024-07-03 as distinct rows, and the production date is an ordinary column."""
    frame = _load_clean(data, "b2")
    zero = frame.filter(pl.col("auction_date") == date(2024, 2, 27))
    assert zero.height == 2
    for name in (
        "positive_balancing_reserve_requirement_mw",
        "negative_balancing_reserve_requirement_mw",
    ):
        assert zero[name].to_list() == [0.0, 0.0]
        assert frame[name].null_count() == 0
    july = frame.filter(pl.col("service_delivery_date") == date(2024, 7, 5))
    assert sorted(july["forecast_produced_on"].to_list()) == [date(2024, 6, 20), date(2024, 7, 3)]
    assert sorted(july["service_window"].to_list()) == ["1", "46"]


def test_medium_term_keeps_both_resources_under_the_resource_partition(data: Path) -> None:
    """Detects a family-level whole-capture selection that serves only the newest of the archive and
    current resources: both bodies load every row with zero exclusions, each row is tagged with its
    own ``resource_id``, the key includes it (the same window label recurs across resources), the
    catalogue's latest view and Polars both serve both captures, and the current resource keeps its
    own production date."""
    ids = {alias: capture(data, alias) for alias in ("b3", "b4")}
    frame = _load_clean_reuse(data, "b3", "b4")
    assert sorted(frame["resource_id"].unique().to_list()) == sorted(
        CAPTURES[a].resource_id for a in ("b3", "b4")
    )
    assert not frame.select(KEY_OF[BR_MT]).is_duplicated().any()
    assert frame.filter(pl.col("resource_id") == CAPTURES["b3"].resource_id).height == len(
        rows("b3")
    )
    assert frame.filter(pl.col("resource_id") == CAPTURES["b4"].resource_id).height == len(
        rows("b4")
    )
    assert set(frame["forecast_type"].to_list()) == {"Medium term"}
    current = frame.filter(pl.col("resource_id") == CAPTURES["b4"].resource_id)
    assert set(current["forecast_produced_on"].to_list()) == {date(2026, 9, 24)}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, BR_MT, None)) == set(ids.values())


def _load_clean_reuse(data: Path, *aliases: str) -> pl.DataFrame:
    """Run the already-written captures of one family and return its silver (every row written)."""
    (family,) = {CAPTURES[a].family for a in aliases}
    written = get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert written == sum(len(rows(a)) for a in aliases)
    return _silver(data, family)


def test_quick_reserve_autumn_windows_all_survive_with_distinct_keys_and_raw_labels(
    data: Path,
) -> None:
    """Detects the repeated autumn window (5 / 5x, 6 / 6x share one local start label) dropped,
    merged on the start label or its clock parsed: all four windows load as distinct rows keyed
    ``(auction_date, service_window)``, the start / end labels are byte-equal to the body (two rows
    show ``25/10/2026 01:00``, two ``25/10/2026 01:30``), and the delivery-date anchor is local
    midnight of the clock-change day (23:00 UTC the evening before)."""
    frame = _load_clean(data, "q1")
    autumn = frame.filter(pl.col("service_delivery_date") == date(2026, 10, 25))
    assert sorted(autumn["service_window"].to_list()) == ["4", "5", "5x", "6", "6x", "7"]
    by_window = {
        w: s
        for w, s in zip(autumn["service_window"], autumn["service_window_start_time"], strict=True)
    }
    assert by_window["5"] == by_window["5x"] == "25/10/2026 01:00"
    assert by_window["6"] == by_window["6x"] == "25/10/2026 01:30"
    assert autumn["service_window_end_time"].to_list() == [
        r["Service Window End Time"]
        for r in rows("q1")
        if r["Service Delivery Date"] == "25/10/2026"
    ]
    assert not frame.select(["auction_date", "service_window"]).is_duplicated().any()
    assert set(autumn["timestamp_utc"].to_list()) == {datetime(2026, 10, 24, 23, 0, tzinfo=UTC)}
    assert frame.schema["service_window_start_time"] == pl.String
    assert frame.schema["positive_quick_reserve_requirement_mw"] == pl.Float64


def test_dc_history_keeps_both_creation_spellings_and_every_issue_of_a_target(data: Path) -> None:
    """Detects a creation label parsed (the body mixes ``%d/%m/%Y`` and ``%Y-%m-%d``, so a typed
    column drops or rejects one spelling), a target collapsed to one issue, or the zero row read
    as missing: every row loads, ``forecast_created`` is byte-equal to the body in both spellings,
    target 06/08/2026 DC-L loads under both creation labels (the key is unique), the all-zero
    row stays 0.0, and the target date anchors on local midnight."""
    frame = _load_clean(data, "d2")
    source = rows("d2")
    assert frame["forecast_created"].to_list() == [r["Forecast_Created"] for r in source]
    assert {"04/08/2026", "2026-08-06"} <= set(frame["forecast_created"].to_list())
    pair = frame.filter(
        (pl.col("forecast_target_date") == date(2026, 8, 6)) & (pl.col("service_type") == "DC-L")
    )
    assert sorted(pair["forecast_created"].to_list()) == ["04/08/2026", "2026-08-06"]
    target_service = frame.select("forecast_target_date", "service_type")
    assert target_service.is_duplicated().any()
    assert not frame.select(KEY_OF[DC_HIST]).is_duplicated().any()
    zero = frame.filter(pl.col("forecast_target_date") == date(2022, 12, 27))
    assert [zero[f"efa_{i}"][0] for i in range(1, 7)] == [0.0] * 6
    assert frame.schema["forecast_created"] == pl.String
    assert frame.schema["efa_1"] == pl.Float64


def test_dc_current_keeps_the_slash_date_raw_and_anchors_nothing(data: Path) -> None:
    """Detects a guessed day / month order or a fabricated instant on the DC current table: the
    ``Date`` cells are the body strings (``08/10/2026`` ...), the service codes are byte-equal, the
    requirement cells are 1495.0 MW and ``timestamp_utc`` is the capture time."""
    frame = _load_clean(data, "d1")
    assert frame.schema["date"] == pl.String
    assert frame["date"].to_list() == [r["Date"] for r in rows("d1")]
    assert set(frame["service_type"].to_list()) == {"DC-H", "DC-L"}
    assert {frame[f"efa_{i}"].unique().to_list()[0] for i in range(1, 7)} == {1495.0}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(CAPTURES["d1"].written)}


@pytest.mark.parametrize(("alias", "codes"), [("m1", {"DML", "DMH"}), ("r1", {"DRL", "DRH"})])
def test_dm_and_dr_load_with_the_bom_and_their_own_service_codes(
    data: Path, alias: str, codes: set[str]
) -> None:
    """Detects a BOM reaching the first column name, the DM codes relabelled DR (the vendor
    dictionary text describes DR for DM: TODO NESO to correct it; the codes stay) or an EFA value
    rescaled: the first silver column is ``efa_date``, the codes are byte-equal to the body, the six
    EFA columns are wide float64 MW, and each date anchors on local midnight (23 September 2026 is
    23:00 UTC the evening before, BST)."""
    assert (FIXTURES / f"{alias}.csv").read_bytes().startswith(BOM)
    frame = _load_clean(data, alias)
    assert frame.columns[0] == "efa_date"
    assert set(frame["service"].to_list()) == codes
    assert frame["service"].to_list() == [r["Service"] for r in rows(alias)]
    assert all(frame.schema[f"efa{i}"] == pl.Float64 for i in range(1, 7))
    first = frame.filter(pl.col("efa_date") == date(2026, 9, 23))
    assert set(first["timestamp_utc"].to_list()) == {datetime(2026, 9, 22, 23, 0, tzinfo=UTC)}


def test_long_term_keeps_dr_l_zero_possibilities_and_the_month_anchor(data: Path) -> None:
    """Detects ``DR_L`` normalised to the dictionary's ``DR-L``, a zero ``Possibilities`` read as
    missing or rescaled to a percentage, ``EFA`` typed as a measure, or an ``Interval`` band parsed:
    ``service_type`` and ``interval`` are byte-equal strings, ``possibilities`` keeps 0.0 and 1.0
    unscaled, ``efa`` is an int64 block, and the target month anchors on local midnight of its first
    day (2026-03-31 23:00 UTC for April, BST)."""
    frame = _load_clean(data, "l1")
    assert frame["service_type"].to_list() == [r["Service Type"] for r in rows("l1")]
    assert (
        "DR_L" in frame["service_type"].to_list() and "DR-L" not in frame["service_type"].to_list()
    )
    assert frame["interval"].to_list() == [r["Interval"] for r in rows("l1")]
    assert frame.schema["interval"] == pl.String and frame.schema["efa"] == pl.Int64
    assert frame.schema["possibilities"] == pl.Float64
    assert 0.0 in frame["possibilities"].to_list() and 1.0 in frame["possibilities"].to_list()
    assert frame["possibilities"].null_count() == 0
    april = frame.filter(pl.col("forecast_target_month") == date(2026, 4, 1))
    assert set(april["timestamp_utc"].to_list()) == {datetime(2026, 3, 31, 23, 0, tzinfo=UTC)}
    assert frame.filter(pl.col("forecast_target_month") == date(2026, 5, 1))["timestamp_utc"][
        0
    ] == (datetime(2026, 4, 30, 23, 0, tzinfo=UTC))


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
    lines = body(alias).decode("utf-8").splitlines()
    newline = "\r\n" if alias in CRLF_ALIASES else "\n"
    raw = (newline.join([*lines, lines[-1]]) + newline).encode("utf-8")
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["b1", "d2", "m1", "q1"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded(data: Path, alias: str) -> None:
    """Detects an issue-time proxy from a creation date or a catalogue view that cannot carry the
    new columns: ``available_at`` is the CKAN ``last_modified`` read as UTC (not the creation date,
    not the capture time); an as-of read before the vintage serves nothing and one after serves the
    capture, in the DuckDB view and in Polars."""
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


@pytest.mark.parametrize("alias", ["b1", "m1", "r1", "q1"])
def test_the_eligible_captures_lie_wholly_after_the_vintage(alias: str) -> None:
    """Detects an eligible record whose captured targets are not all forward (RULINGS 529's
    exception): every delivery / EFA date in the four eligible families' captures is after
    the CKAN ``last_modified`` date."""
    meta = CAPTURES[alias]
    vintage = datetime.fromisoformat(meta.modified).date()
    for spec in COLS[meta.family]:
        if spec[1] in ("service_delivery_date", "efa_date"):
            targets = [_expected(spec, r[spec[0]]) for r in rows(alias)]
            assert all(t > vintage for t in targets), alias


@pytest.mark.parametrize("slug", list(PACKAGES))
def test_the_skeleton_pages_render_the_new_records(slug: str) -> None:
    """Detects a record the docs generator cannot render (the held questions, the ellipsis, the
    backticks)."""
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
