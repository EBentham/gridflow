"""The mock EAC results, ORPS utilisation and OBP: ten frozen records, three eligible, seven held
(v0.22-K-AS-3b).

Fifteen families sit in five packages. Ten get a record: the four mock EAC tables (held twice:
the package carries the test-data hold, each family the clock question), the three ORPS CSV tables
(held) and the three OBP gate-closure tables (eligible). Five stay record-less: the four
header-only daily mock resources (``empty_allowed: true``, ingest-only) and the ORPS file
container (workbook ``HOLD``, PDFs ``DOC``). Every test writes recorded fixture captures (cuts of
the 2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/as3b/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation. On master
none of the ten families has a record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-AS-3-FACTS g1-g6 and sections 4-8, K-AS-3b-SPEC):

- ``issue: none`` everywhere. The OBP ``Issue Datetime`` is a nullable **data column** (blank on
  the VP / SPP rows and three STOP rows), never an issue recipe; it is never propagated to other
  points.
- Mock EAC: test data. The cells of ``deliveryStart`` / ``deliveryEnd`` / ``orderEntryTime`` stay
  raw strings with temporal ``none`` (TODO: EAC-MOCK-CLOCK - the live UTC declaration does not
  extend to the mock package and every captured date falls in GMT). Never join mock to live on
  auction id (mock ids 1-22 are reused for different dates). The four daily mock resources are
  served header-only by the vendor (HTTP 200, 0 rows, unchanged since 2026-06-04):
  ``empty_allowed: true``, no record.
- ORPS reactive: the record has the single wide-archive epoch (176 columns). The current long table
  repeats the complete row ``SKWF,SAKNW-1,D,SC,May-25,0,0,0`` so no key passes (RULINGS 538): that
  resource is a resource-level ``HOLD``, never an ADR-040 adjudication, dedup or row ordinal.
- ORPS historic: every value column is a string (20,950 comma numerals, 32 padded dashes; no trim,
  no token map, no lexical-number mechanism); the key is all 238 silver columns, a guard and not a
  vendor identity, because (SPLN, SPLN_01Z, D, M) repeats with 234 different values. The dash
  meaning is open (TODO: ORPS-HIST-DASH). The unit is MVArh per the package text (the dictionary's
  ``MVAr`` conflicts; recorded in PROVENANCE).
- OBP are gate-closure declaration / instruction event tables, not forecasts: the CKAN last-modified
  vintage is conservative for every row. TODO: OBP-INSTRUCTION-IMMUTABILITY (no repeated capture
  proves whether points or statuses are amended), cross-capture identity of the PN and availability
  rows, and INTERVAL-CLOSURE are not holds.
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

import polars as pl
import pytest
from _neso_generic_support import write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings

from gridflow.connectors.neso_data_portal import client as client_module
from gridflow.connectors.neso_data_portal import eligibility as eligibility_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.client import NesoEmptyResourceError
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Eligible, Held, load_registry
from gridflow.connectors.neso_data_portal.registry.record import has_issue_time
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as3b"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-AS-3b"
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
DASH = "—"

MOCK_PKG = "eac-mock-auction-results"
ORPS_PKG = "obligatory-reactive-power-service-orps-utilisation"
PN_PKG = "obp-non-bm-physical-notifications"
INSTR_PKG = "obp-non-bm-reserve-instructions"
AVAIL_PKG = "obp-reserve-availability-utilisation-price"
PACKAGE_IDS = {
    MOCK_PKG: "8a6f23d7-8489-49ac-9b0a-566fb53d32f8",
    ORPS_PKG: "7e142b03-8650-4f46-8420-7ce1e84e1e5b",
    PN_PKG: "73edf630-fe9e-45a6-a2be-fa0d31a8ec1d",
    INSTR_PKG: "0ba217bb-846b-470e-9717-ff152d97d009",
    AVAIL_PKG: "769ec53a-9940-4df3-a867-6fa62d483097",
}

M_UNIT = "eac_mock_results_by_unit"
M_BUY = "eac_mock_results_buy_orders"
M_SUM = "eac_mock_results_summary"
M_SELL = "eac_mock_results_sell_orders"
D_UNIT = "eac_mock_results_daily_by_unit"
D_BUY = "eac_mock_results_daily_buy_orders"
D_SUM = "eac_mock_results_daily_summary"
D_SELL = "eac_mock_results_daily_sell_orders"
HIST = "orps_utilisation_historic_reactive"
REACT = "orps_utilisation_reactive"
DEFAULT = "orps_utilisation_reactive_default"
FILES = "orps_utilisation_files"
PN = "obp_physical_notifications"
INSTR = "obp_reserve_instructions"
AVAIL = "obp_reserve_avail_price"

MOCKS = (M_UNIT, M_BUY, M_SUM, M_SELL)
DAILY_MOCKS = (D_UNIT, D_BUY, D_SUM, D_SELL)
ORPS_HELD = (HIST, REACT, DEFAULT)
OBP = (PN, INSTR, AVAIL)
HELD = (*MOCKS, *ORPS_HELD)
RECORDED = (*MOCKS, *ORPS_HELD, *OBP)
PACKAGE_OF = {
    **dict.fromkeys((*MOCKS, *DAILY_MOCKS), MOCK_PKG),
    **dict.fromkeys((*ORPS_HELD, FILES), ORPS_PKG),
    PN: PN_PKG,
    INSTR: INSTR_PKG,
    AVAIL: AVAIL_PKG,
}

CLOCK_QUESTION = (
    f"TODO: EAC-MOCK-CLOCK {DASH} the mock package and its dictionaries do not state the clock of "
    "the naive deliveryStart / deliveryEnd / orderEntryTime cells, the live package's UTC "
    "declaration does not extend to it, and every captured date falls in GMT so UTC and UK local "
    "time cannot be told apart; which clock do the mock exports use? (Mock / test data, not "
    "procurement results.)"
)
TEST_DATA_QUESTION = (
    f"TODO: EAC-MOCK-TEST-DATA {DASH} NESO publishes these resources as outcomes of mock auction "
    "runs, not live procurement; publish only under an explicit test/mock label with separate "
    "provenance, and never join them to live EAC on auction id (mock ids 1–22 are reused for "
    "different dates)."
)
HELD_QUESTIONS = {
    **dict.fromkeys(MOCKS, CLOCK_QUESTION),
    HIST: (
        f"TODO: ORPS-HIST-DASH / ORPS-HIST-IDENTITY {DASH} what does the padded `-` cell mean "
        "(zero, not reported, not applicable)? And the identity (SPLN, SPLN_01Z, D, M) appears "
        "twice (rows 181 and 277) with 234 different values: are these two distinct entries, and "
        "what distinguishes them?"
    ),
    REACT: (
        f"TODO: ORPS-REACTIVE-ARCHIVE {DASH} the current long table restates every unit/month of "
        "this Apr-20 … Dec-24 archive but differs on 241 values and publishes 2,895 of its "
        "blanks as 0; is this archive superseded, and does a blank mean zero or not reported?"
    ),
    DEFAULT: (
        f"TODO: ORPS-PRICE-INPUT-UNITS {DASH} the dictionary labels HPIm, PAPIm and PPIm only "
        '"£" and NESO\'s default-payment guidance calls them power-price inputs without a '
        "denominator; what unit are they in this export? (FRPIm is a dimensionless RPI index, Im "
        "a dimensionless indexation factor, X = 1 / X = 0.2 are £/MVArh rates.)"
    ),
}
R2_ID = "528b03d2-4008-4b65-a732-a001cd835a3b"
R2_HOLD = (
    f"TODO: ORPS-REACTIVE-LONG-DUPLICATE {DASH} the current long table repeats the complete row "
    "SKWF,SAKNW-1,D,SC,May-25,0,0,0 twice, so no key passes the duplicate guard (RULINGS 538); "
    "publication defect or two distinct utilisations? Re-disposition only after a corrected NESO "
    "upload (a new last_modified) or a vendor identity column"
)
X1_ID = "d91e4fd2-1f27-4d0b-8473-b4b19af7f3dc"
D1_ID = "e2e6f74c-ebca-48b3-b2ae-e4652d296dca"
D2_ID = "0a74a789-d9fc-46cc-9554-e4e04c1e8014"
WORKBOOK_HOLD = (
    f"TODO: ORPS-WORKBOOK-LAYOUT {DASH} merged monthly headers, notes and totals; the ADR-037 "
    "single-block XLSX reader cannot read a sheet faithfully; re-disposition only after a reader "
    "unit with per-sheet flattened headers and verified table boundaries"
)

# Header lines of the four header-only daily mock resources, as the vendor served them
# (LIVE-PROBE P1, 2026-10-13: HTTP 200, 0 data rows, datastore_search total 0). The sell header
# ends in the singular ``minimumConsecutiveWindow``.
PROBE_HEADERS: dict[str, str] = {
    D_UNIT: (
        "registeredAuctionParticipant,auctionUnit,serviceType,auctionProduct,executedQuantity,"
        "clearingPrice,deliveryStart,deliveryEnd,technologyType,postCode,unitResultID"
    ),
    D_BUY: (
        "auctionID,orderID,serviceType,auctionProduct,deliveryStart,deliveryEnd,quantity,price,"
        "substitutabilityFamily,paradoxicallyAcceptanceAllowed,orderEntryTime,acceptanceRatio,"
        "status,executedQuantity,clearingPrice,reasonRejected,joinedFamily"
    ),
    D_SUM: (
        "auctionID,auctionProduct,serviceType,deliveryStart,deliveryEnd,clearedVolume,"
        "clearingPrice,linkedServiceWindowID"
    ),
    D_SELL: (
        "auctionID,registeredAuctionParticipant,auctionUnit,basketID,serviceType,deliveryStart,"
        "deliveryEnd,orderID,orderType,auctionProduct,productID,quantity,priceLimit,"
        "loopedBasketID,orderEntryTime,acceptanceRatio,status,executedQuantity,clearingPrice,"
        "reasonRejected,flexibleGroupID,minimumConsecutiveWindow"
    ),
}

EAC_SILVER = {
    "registeredAuctionParticipant": "registered_auction_participant",
    "auctionUnit": "auction_unit",
    "serviceType": "service_type",
    "auctionProduct": "auction_product",
    "executedQuantity": "executed_quantity",
    "clearingPrice": "clearing_price",
    "deliveryStart": "delivery_start",
    "deliveryEnd": "delivery_end",
    "technologyType": "technology_type",
    "postCode": "post_code",
    "unitResultID": "unit_result_id",
    "auctionID": "auction_id",
    "orderID": "order_id",
    "quantity": "quantity",
    "price": "price",
    "substitutabilityFamily": "substitutability_family",
    "paradoxicallyAcceptanceAllowed": "paradoxically_acceptance_allowed",
    "orderEntryTime": "order_entry_time",
    "acceptanceRatio": "acceptance_ratio",
    "status": "status",
    "reasonRejected": "reason_rejected",
    "joinedFamily": "joined_family",
    "clearedVolume": "cleared_volume",
    "linkedServiceWindowID": "linked_service_window_id",
    "basketID": "basket_id",
    "orderType": "order_type",
    "productID": "product_id",
    "priceLimit": "price_limit",
    "loopedBasketID": "looped_basket_id",
    "flexibleGroupID": "flexible_group_id",
    "minimumConsecutiveWindows": "minimum_consecutive_windows",
}
EAC_FLOATS = {
    "executedQuantity",
    "clearingPrice",
    "quantity",
    "price",
    "priceLimit",
    "acceptanceRatio",
    "clearedVolume",
}
EAC_INTS = {"minimumConsecutiveWindows"}
EAC_TIMES = {"deliveryStart", "deliveryEnd", "orderEntryTime"}
ORPS_IDENTITY = ["company", "unit", "default_market", "location"]
OBP_KINDS: dict[str, dict[str, str]] = {
    PN: {"Time From": "t", "Time To": "t", "Level From": "f", "Level To": "f"},
    INSTR: {
        "Sequence": "i",
        "Issue Datetime": "t",
        "Point Datetime": "t",
        "Target MW": "f",
        "Target Price": "f",
    },
    AVAIL: {
        "Time From": "t",
        "Time To": "t",
        "Pair ID": "i",
        "Availability Power": "f",
        "Utilisation Price": "f",
    },
}

KEY_OF: dict[str, list[str]] = {
    M_UNIT: ["unit_result_id"],
    M_BUY: ["auction_id", "order_id"],
    M_SUM: ["auction_id", "auction_product", "delivery_start"],
    M_SELL: ["auction_id", "order_id"],
    REACT: ORPS_IDENTITY,
    DEFAULT: ["year_vendor", "month_vendor"],
    PN: ["unit_id", "time_from"],
    INSTR: ["instruction_point_id"],
    AVAIL: ["unit_id", "time_from", "pair_id", "service_id"],
}
TEMPORAL_COLUMN = {PN: "time_from", INSTR: "point_datetime", AVAIL: "time_from"}
UPLOAD_ONLY = {*ORPS_HELD, *OBP}

Spec = tuple[str, str, str]


def silver_name(source: str) -> str:
    """The spec's naming rule: lowercase, each run of non-alphanumerics to ``_``, edges stripped;
    ``Year`` / ``Month`` are reserved Hive names so they become ``*_vendor``."""
    if source == "Year":
        return "year_vendor"
    if source == "Month":
        return "month_vendor"
    return re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")


def kind_of(family: str, source: str) -> str:
    """``f`` float64, ``i`` int64, ``t`` UTC datetime or ``s`` string for one source column."""
    if family in MOCKS:
        if source in EAC_FLOATS:
            return "f"
        return "i" if source in EAC_INTS else "s"
    if family == HIST:
        return "s"
    if family == REACT:
        return "s" if silver_name(source) in {"id", *ORPS_IDENTITY} else "f"
    if family == DEFAULT:
        return "i" if source in {"Year", "Month"} else "f"
    return OBP_KINDS[family].get(source, "s")


def name_of(family: str, source: str) -> str:
    """The silver name of ``source`` in ``family``."""
    return EAC_SILVER[source] if family in MOCKS else silver_name(source)


def cols_of(family: str, header: list[str]) -> list[Spec]:
    """(source, silver name, kind) for one epoch header."""
    return [(s, name_of(family, s), kind_of(family, s)) for s in header]


def key_of(family: str, header: list[str]) -> list[str]:
    """The entity key: the spec's columns, or every silver column for the historic ORPS table."""
    if family == HIST:
        return [name_of(family, s) for s in header]
    return KEY_OF[family]


def non_null_names(family: str) -> set[str]:
    """The silver names that must be ``nullable: false``: the identity columns that carry no blank
    in the body and the temporal input. The historic key also contains 234 value columns that carry
    11,418 blanks, so they stay nullable (``nullable: false`` would exclude those rows)."""
    if family == HIST:
        return set(ORPS_IDENTITY)
    names = set(KEY_OF[family])
    if family in TEMPORAL_COLUMN:
        names.add(TEMPORAL_COLUMN[family])
    return names


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    family: str
    package: str
    resource_id: str
    name: str
    filename: str
    modified: str | None
    written: str
    url_type: str


CAPTURES: dict[str, Capture] = {
    "m1": Capture(
        M_UNIT,
        MOCK_PKG,
        "955cf89f-232e-4b9c-8414-6351080f49fa",
        "NESO Mock Response-Reserve Results By Unit 2025-2026",
        "955cf89f-232e-4b9c-8414-6351080f49fa",
        None,
        "2026-10-08T10:27:22.948099+00:00",
        "datastore",
    ),
    "m2": Capture(
        M_BUY,
        MOCK_PKG,
        "700a73ff-38e9-45ff-a674-085bcbfc9faf",
        "NESO Mock Response-Reserve Buy Orders 2025-2026",
        "700a73ff-38e9-45ff-a674-085bcbfc9faf",
        None,
        "2026-10-08T10:26:53.016299+00:00",
        "datastore",
    ),
    "m3": Capture(
        M_SUM,
        MOCK_PKG,
        "454216d8-61f3-40b6-b787-4dff035ce3c3",
        "NESO Mock Response-Reserve Results Summary 2025-2026",
        "454216d8-61f3-40b6-b787-4dff035ce3c3",
        None,
        "2026-10-08T10:28:22.964546+00:00",
        "datastore",
    ),
    "m4": Capture(
        M_SELL,
        MOCK_PKG,
        "3b18345a-1933-44d7-abd9-062a2e695ccd",
        "NESO Mock Response-Reserve Sell Orders 2025-2026",
        "3b18345a-1933-44d7-abd9-062a2e695ccd",
        None,
        "2026-10-08T10:27:53.401265+00:00",
        "datastore",
    ),
    "r1": Capture(
        REACT,
        ORPS_PKG,
        "a61c6c26-62ec-41e1-ae25-ed95f4562274",
        "Reactive Utilisation Data - Apr-2020 - Dec-2024",
        "reactive-utilisation-data-apr-2020-dec-2024.csv",
        "2025-02-14T13:27:21.557694",
        "2026-10-08T11:15:58.937852+00:00",
        "upload",
    ),
    "r2": Capture(
        REACT,
        ORPS_PKG,
        R2_ID,
        "Reactive Utilisation Data",
        "reactive-utilisation-data.csv",
        "2026-10-07T15:58:09.723806",
        "2026-10-08T11:16:02.015210+00:00",
        "upload",
    ),
    "r3": Capture(
        HIST,
        ORPS_PKG,
        "3ee65326-3729-421d-b667-b2164501e4a5",
        "Historic Reactive Utilisation Data - Apr-2013 - March 2020",
        "reactive-historical-data-apr-2013-to-mar-2020.csv",
        "2021-07-26T11:19:03.229125",
        "2026-10-08T11:15:55.409392+00:00",
        "upload",
    ),
    "r4": Capture(
        DEFAULT,
        ORPS_PKG,
        "b00df5b1-4179-44e3-b556-28a836ad619f",
        "Reactive Default Payment Rate - Oct - 2026",
        "reactive-default-payment-rate-oct-2026.csv",
        "2026-09-17T07:44:25.572388",
        "2026-10-08T11:16:05.568105+00:00",
        "upload",
    ),
    "p1": Capture(
        PN,
        PN_PKG,
        "dfe1d229-e973-43c6-b5eb-c28ad59d7136",
        "Non-BM Physical Notifications",
        "pn_20261008_27_nbm.csv",
        "2026-10-08T11:01:24.343784",
        "2026-10-08T11:14:57.154311+00:00",
        "upload",
    ),
    "i1": Capture(
        INSTR,
        INSTR_PKG,
        "dad63c1b-8a8e-47de-9922-7880384ef104",
        "Non-BM Instructions from OBP",
        "instr_20261002064426_qr_sr_nbm.csv",
        "2026-10-02T06:44:33.852998",
        "2026-10-08T11:15:04.114361+00:00",
        "upload",
    ),
    "a1": Capture(
        AVAIL,
        AVAIL_PKG,
        "6bfe7df0-60aa-462d-94cd-44ac0a4abb2c",
        " Non-BM Reserve Availability MW and Utilisation Price",
        "av_20261008_27_qr_sr_nbm.csv",
        "2026-10-08T11:01:23.882138",
        "2026-10-08T11:15:00.909612+00:00",
        "upload",
    ),
}
FIRST_OF = {
    M_UNIT: "m1",
    M_BUY: "m2",
    M_SUM: "m3",
    M_SELL: "m4",
    HIST: "r3",
    REACT: "r1",
    DEFAULT: "r4",
    PN: "p1",
    INSTR: "i1",
    AVAIL: "a1",
}
LOADABLE = [a for a in CAPTURES if a != "r2"]
BOM_ALIASES = {"r1", "r3", "r4"}
CRLF_ALIASES = {"r1", "r2", "r3", "r4"}


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
        prefix="as3b", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: the bronze bytes, CRLF or LF)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim, BOM removed)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8-sig")
    table = list(csv.reader(io.StringIO(text, newline="")))
    names = [c.strip() for c in table[0]]
    return [dict(zip(names, cells, strict=True)) for cells in table[1:] if any(cells)]


def header(alias: str) -> list[str]:
    """The fixture's header as the production reader returns it (BOM removed, edges stripped)."""
    first = body(alias).decode("utf-8-sig").splitlines()[0]
    return [c.strip() for c in next(csv.reader([first]))]


def capture(
    data: Path, alias: str, *, raw: bytes | None = None, resource_id: str | None = None
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=meta.package,
        package_id=PACKAGE_IDS[meta.package],
        resource_id=resource_id or meta.resource_id,
        resource_name=meta.name,
        resource_filename=meta.filename,
        ckan_last_modified=meta.modified,
        url_type=meta.url_type,
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


def _utc(cell: str) -> datetime:
    """A ``%Y-%m-%dT%H:%M:%S`` body cell as the tz-aware UTC instant (no shift)."""
    return datetime.strptime(cell, TIME_FORMAT).replace(tzinfo=UTC)


def _expected(spec: Spec, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank cell is null)."""
    _source, _name, kind = spec
    if cell == "":
        return None
    if kind == "f":
        return float(cell)
    if kind == "i":
        return int(cell)
    if kind == "t":
        return _utc(cell)
    return cell


def _assert_cells(family: str, alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, instant and measure of ``frame`` equals the body cell."""
    source_rows = rows(alias)
    for spec in cols_of(family, header(alias)):
        assert frame[spec[1]].to_list() == [_expected(spec, r[spec[0]]) for r in source_rows], spec[
            1
        ]


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


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the bronze row terminators
    (CRLF on the four ORPS bodies, LF on the rest), the UTF-8 BOM on exactly R1 / R3 / R4 (and not
    on the others), each family's exact vendor header, the two ``SPLN_01Z`` rows with a comma
    numeral and a padded dash in the historic cut, a blank monthly cell in the R1 cut, the
    60-second PN segment and the BST-season PN start, the START and VP points of one instruction,
    and the ``-1`` pair and ``+-10000`` prices in the availability cut."""
    for alias, meta in CAPTURES.items():
        raw = body(alias)
        assert raw.startswith(b"\xef\xbb\xbf") == (alias in BOM_ALIASES), alias
        assert raw.endswith(b"\n"), alias
        assert raw.count(b"\r\n") == (raw.count(b"\n") if alias in CRLF_ALIASES else 0), alias
        assert len(header(alias)) == len(set(header(alias))), (alias, meta.family)
    for family in MOCKS:
        assert len(header(FIRST_OF[family])) in (11, 17, 8, 22)
    assert len(header("r3")) == 238 and len(header("r1")) == 176 and len(header("r2")) == 8
    assert header("r2")[4] == "Month-Year" and header("r1")[0] == "id"
    spln = [r for r in rows("r3") if r["Unit"] == "SPLN_01Z"]
    assert len(spln) == 2 and len(spln[0]) == 238
    assert any("," in c for r in rows("r3") for k, c in r.items() if k not in ORPS_IDENTITY_SOURCES)
    assert any(c.strip() == "-" and c != "-" for r in rows("r3") for c in r.values())
    assert any("" in list(r.values())[5:] for r in rows("r1"))
    assert any(
        (_utc(r["Time To"]) - _utc(r["Time From"])) == timedelta(seconds=60) for r in rows("p1")
    )
    assert any(r["Time From"].startswith("2026-07-01T05:") for r in rows("p1"))
    points = {
        r["Instruction Point Type"]: r for r in rows("i1") if r["Instruction ID"] == "3192267"
    }
    assert points["START"]["Issue Datetime"] != "" and points["VP"]["Issue Datetime"] == ""
    assert any(r["Pair ID"] == "-1" for r in rows("a1"))
    assert {"10000.0", "-10000.0"} <= {r["Utilisation Price"] for r in rows("a1")}


ORPS_IDENTITY_SOURCES = {"Company", "Unit", "Default/Market", "Location"}


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_vintage_and_no_issue(key: str) -> None:
    """Detects an issue time invented from a data column, a ``key_latest`` selection with no vendor
    identity, a ``run_type``, a partition on a single-resource family, or the wrong vintage: every
    record is version 1, utf-8 CSV, whole-capture, the spec's entity key (every silver column for
    the historic ORPS table), ``capture_fallback`` for the four mock tables (their sidecars carry no
    last-modified) and ``ckan_last_modified`` for the upload-only ORPS and OBP families, with
    exactly one epoch whose header is the vendor body's and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.latest == "whole_capture" and record.latest_partition is None
    assert record.vintage == ("ckan_last_modified" if key in UPLOAD_ONLY else "capture_fallback")
    assert record.vintage_evidence is None
    assert len(record.epochs) == 1
    epoch = record.epochs[0]
    assert list(epoch.header) == header(FIRST_OF[key])
    assert list(record.entity_key) == key_of(key, header(FIRST_OF[key]))
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None
    assert epoch.issue.kind == "none" and epoch.unpivot is None
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: opaque identifiers,
    labels, the mock clocks and every historic ORPS value are strings; EAC volumes / prices / ratio,
    the 171 reactive monthly values, the default-rate inputs, OBP levels / targets / prices are
    nullable float64; ``minimumConsecutiveWindows``, ``Year`` / ``Month``, ``Sequence`` and
    ``Pair ID`` are int64; the OBP clocks are ``%Y-%m-%dT%H:%M:%S`` UTC datetimes. Key columns and
    the temporal input are ``nullable: false`` (the historic value columns, 11,418 blanks, are
    not), and no column declares ``trim``, ``null_tokens``, ``formats_by_filename``, a bound or a
    zone rule other than ``UTC``."""
    record = _record(key)
    expected = cols_of(key, header(FIRST_OF[key]))
    columns = record.epochs[0].columns
    assert [c.source for c in columns] == [s[0] for s in expected]
    assert [c.name for c in columns] == [s[1] for s in expected]
    assert len({c.name for c in columns}) == len(columns)
    for column, (source, name, kind) in zip(columns, expected, strict=True):
        assert column.trim is None and column.null_tokens == ()
        assert (column.min, column.max) == (None, None)
        assert column.formats_by_filename is None
        assert column.nullable == (name not in non_null_names(key)), name
        if kind == "t":
            assert (column.dtype, column.format) == ("datetime", TIME_FORMAT), source
            assert (column.zone, column.zone_evidence, column.ambiguous) == ("UTC", None, None)
            continue
        assert column.format is None and column.zone is None, source
        assert column.zone_evidence is None and column.ambiguous is None, source
        assert column.dtype == {"f": "float64", "i": "int64", "s": "string"}[kind], source


@pytest.mark.parametrize("key", [*MOCKS, HIST])
def test_mock_and_historic_values_are_strings_where_the_meaning_is_open(key: str) -> None:
    """Detects a mock clock parsed (the live package's UTC declaration does not extend to it) or a
    historic ORPS comma numeral / padded dash cast or cleaned: the mock delivery / submission
    columns and all 234 historic monthly value columns are ``string``, and the historic record
    declares no trim, token map or number format."""
    record = _record(key)
    types = {c.name: c.dtype for c in record.epochs[0].columns}
    if key in MOCKS:
        present = {"delivery_start", "delivery_end", "order_entry_time"} & set(types)
        assert present and {types[n] for n in present} == {"string"}
    else:
        value_types = {t for n, t in types.items() if n not in ORPS_IDENTITY}
        assert len(types) == 238 and value_types == {"string"}


@pytest.mark.parametrize("key", [*MOCKS, HIST, REACT, DEFAULT])
def test_the_temporal_recipe_is_none_on_the_seven_held_records(key: str) -> None:
    """Detects an event time fabricated for a table that has none: the mock clocks are undecided,
    the ORPS months live in header text (historic, reactive) or in separate ``Year`` / ``Month``
    integers (default), and nothing composes a date, so the temporal recipe is ``none`` and
    ``timestamp_utc`` is the capture time."""
    recipe = _record(key).temporal
    assert (recipe.kind, recipe.column, recipe.date_column) == ("none", None, None)


@pytest.mark.parametrize("key", OBP)
def test_obp_events_are_anchored_on_the_utc_window_start_or_instruction_point(key: str) -> None:
    """Detects an OBP table anchored on the wrong instant: PN and availability on ``time_from``, the
    instruction table on ``point_datetime``, all ``utc_instant``."""
    recipe = _record(key).temporal
    assert (recipe.kind, recipe.column) == ("utc_instant", TEMPORAL_COLUMN[key])


def test_the_instruction_issue_datetime_is_a_nullable_data_column_not_an_issue_recipe() -> None:
    """Detects ``Issue Datetime`` used as the issue recipe (null issuance is rejected by the engine
    and ``issue_time`` would have to join the key) or made ``nullable: false`` (which would exclude
    the 259 VP / SPP / STOP points): it is a nullable UTC datetime column and the epoch's issue kind
    is ``none``."""
    record = _record(INSTR)
    columns = {c.name: c for c in record.epochs[0].columns}
    issue = columns["issue_datetime"]
    assert (issue.dtype, issue.nullable, issue.zone) == ("datetime", True, "UTC")
    assert record.epochs[0].issue.kind == "none"
    assert columns["point_datetime"].nullable is False
    assert columns["instruction_status"].nullable is True


# --------------------------------------------------------------------------- #
# Eligibility, holds and dispositions
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", HELD)
def test_seven_records_are_held_with_the_verbatim_question(key: str) -> None:
    """Detects a hold lost, reworded or moved: each of the seven records carries its verbatim
    question and this unit. The four mock families are effectively held by their package (the test
    data hold wins, rule 1), the three ORPS ones by their own record and their package stays
    eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == (UNIT, HELD_QUESTIONS[key])
    if key in MOCKS:
        assert effective_eligibility(package, family) == package.eligibility
    else:
        assert effective_eligibility(package, family) == record.eligibility
        assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", OBP)
def test_three_records_are_eligible(key: str) -> None:
    """Detects a hold or a per-record override on a clean OBP family: the three carry no
    eligibility override, each is effectively eligible and so is its package."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.eligibility == Eligible(status="eligible")


def test_the_mock_package_is_held_as_test_data_and_the_other_four_stay_eligible() -> None:
    """Detects the mock tables publishable the moment the clock question is answered, or a
    collateral hold: the mock package is ``held`` with EAC-MOCK-TEST-DATA and this unit (so the rows
    stay unpublished until a test-data label exists), and the ORPS and three OBP packages stay
    ``eligible``."""
    packages = {p.package: p for p in load_registry().packages}
    mock = packages[MOCK_PKG].eligibility
    assert isinstance(mock, Held)
    assert (mock.unit, mock.question) == (UNIT, TEST_DATA_QUESTION)
    for slug in (ORPS_PKG, PN_PKG, INSTR_PKG, AVAIL_PKG):
        assert packages[slug].eligibility == Eligible(status="eligible"), slug


def test_the_four_daily_mock_families_are_empty_allowed_and_have_no_record() -> None:
    """Detects a record frozen on a header-only resource (a header cannot establish a populated
    record; the sell header even differs, singular ``minimumConsecutiveWindow``) or the flag
    missing: each daily mock family has ``empty_allowed: true``, no record, no transformer and an
    ingest-only report line, while the four populated mock families and every other family of the
    five packages stay ``empty_allowed: false``."""
    registry = load_registry()
    for key in DAILY_MOCKS:
        package, entry = registry.families[key]
        assert entry.empty_allowed is True, key
        assert entry.record is None and entry.transformer is None, key
        assert package.package == MOCK_PKG
        with pytest.raises(ValueError, match="No transformer registered"):
            get_transformer(SOURCE, key, Path("__test__"))
    flagged = {k for k, (_p, e) in registry.families.items() if e.empty_allowed}
    assert set(DAILY_MOCKS) <= flagged
    for key in (*MOCKS, *ORPS_HELD, *OBP, FILES):
        assert registry.families[key][1].empty_allowed is False, key
    report = eligibility_module.render(registry)
    for key in DAILY_MOCKS:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "ingest-only (no silver output)" in line, key


@pytest.mark.parametrize("key", DAILY_MOCKS)
def test_a_header_only_daily_mock_body_is_admitted_as_empty_and_refused_without_the_flag(
    key: str,
) -> None:
    """Detects the S-sweep failure repeating (a header-only body on a non-``empty_allowed`` family
    fails the capture and blocks close as ``missing``): the vendor's header line (LIVE-PROBE) plus a
    newline is admitted by the same admission path with the registry's flag and marked empty, and
    the identical body is refused when the flag is false."""
    entry = load_registry().families[key][1]
    probe = (PROBE_HEADERS[key] + "\n").encode()
    assert client_module._admit_member_body(
        probe, declared_format="CSV", filename="x.csv", empty_allowed=entry.empty_allowed, label=key
    ) == ("csv", True)
    with pytest.raises(NesoEmptyResourceError):
        client_module._admit_member_body(
            probe, declared_format="CSV", filename="x.csv", empty_allowed=False, label=key
        )


def test_the_populated_mock_headers_equal_the_probe_headers_except_the_sell_window_name() -> None:
    """Detects the populated sell header frozen for the empty resource or vice versa: the by-unit,
    buy and summary probe headers equal their populated counterparts; the daily sell probe ends in
    the singular ``minimumConsecutiveWindow`` while the populated table says
    ``minimumConsecutiveWindows`` (so no record is shared between them)."""
    for daily, populated in ((D_UNIT, M_UNIT), (D_BUY, M_BUY), (D_SUM, M_SUM)):
        assert PROBE_HEADERS[daily].split(",") == header(FIRST_OF[populated])
    sell = header(FIRST_OF[M_SELL])
    assert PROBE_HEADERS[D_SELL].split(",")[:-1] == sell[:-1]
    assert (PROBE_HEADERS[D_SELL].split(",")[-1], sell[-1]) == (
        "minimumConsecutiveWindow",
        "minimumConsecutiveWindows",
    )


def test_the_reactive_record_has_one_wide_epoch_and_the_long_table_is_a_resource_hold() -> None:
    """Detects a record claiming both ORPS reactive resources, the long table silently included or
    deduplicated, or an ADR-040 adjudication: the family's record has exactly one epoch (the 176
    column wide archive, ``id`` first), the long table's header is not frozen, resource
    ``528b03d2`` is a ``HOLD`` with the verbatim question and this unit while the wide archive
    stays ``SILVER``, and the reconcile ledger names neither."""
    record = _record(REACT)
    assert len(record.epochs) == 1 and len(record.epochs[0].header) == 176
    assert record.epochs[0].header[0] == "id"
    assert "Month-Year" not in record.epochs[0].header
    registry = load_registry()
    (hold,) = [r for _p, r in registry.resources.values() if r.id == R2_ID]
    assert (hold.disposition.kind, hold.family) == ("HOLD", REACT)
    assert hold.disposition.reason == R2_HOLD and hold.disposition.unit == UNIT  # type: ignore[union-attr]
    (wide,) = [r for _p, r in registry.resources.values() if r.id == CAPTURES["r1"].resource_id]
    assert wide.disposition.kind == "SILVER"
    entries = registry_module.load_reconcile_adjudications()
    assert {e.family for e in entries} & set(RECORDED) == set()


def test_the_workbook_container_and_children_are_held_and_the_pdfs_stay_doc() -> None:
    """Detects the workbook gaining a family or the stale ``record pending in AS-3`` reason: the X1
    container and each of its 33 children are ``HOLD`` with the verbatim layout question and this
    unit, the child names are unchanged (the trailing spaces of ``Apr12-Sep12 `` and
    ``Oct 20 - Mar 21 `` kept), the two PDFs stay ``DOC``, and no ``orps_utilisation_workbook``
    family or record exists."""
    registry = load_registry()
    resources = {r.id: r for _p, r in registry.resources.values() if r.family == FILES}
    assert set(resources) == {X1_ID, D1_ID, D2_ID}
    container = resources[X1_ID]
    assert container.disposition.kind == "HOLD"
    assert container.disposition.reason == WORKBOOK_HOLD  # type: ignore[union-attr]
    assert container.disposition.unit == UNIT  # type: ignore[union-attr]
    assert len(container.children) == 33
    names = [c.child for c in container.children]
    assert "Apr12-Sep12 " in names and "Oct 20 - Mar 21 " in names
    for child in container.children:
        assert child.disposition.kind == "HOLD", child.child
        assert child.disposition.reason == WORKBOOK_HOLD, child.child  # type: ignore[union-attr]
        assert child.disposition.unit == UNIT, child.child  # type: ignore[union-attr]
    assert resources[D1_ID].disposition.kind == resources[D2_ID].disposition.kind == "DOC"
    assert "orps_utilisation_workbook" not in registry.families
    assert registry.families[FILES][1].record is None


def test_the_package_files_carry_exactly_the_ten_records_and_no_ledger_entry() -> None:
    """Detects a record on the wrong family, a family gaining a transformer, or a reconcile
    adjudication this batch does not need: the five package files hold exactly the ten records
    (the four daily mock families and the ORPS file container have none), no family has a bespoke
    transformer, every non-ORPS resource stays ``SILVER``, and the reconcile ledger names none of
    the ten."""
    found: set[str] = set()
    for slug in PACKAGE_IDS:
        document = _package_doc(slug)
        assert document["package_id"] == PACKAGE_IDS[slug]
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), slug
        if slug != ORPS_PKG:
            assert all(r["disposition"]["kind"] == "SILVER" for r in document["resources"]), slug
    assert found == set(RECORDED)
    assert len([f for f in _package_doc(MOCK_PKG)["families"] if "record" in f]) == 4
    entries = registry_module.load_reconcile_adjudications()
    assert {e.family for e in entries} & {*RECORDED, *DAILY_MOCKS, FILES} == set()


def test_ten_are_reported_as_silver_outputs_three_eligible_and_seven_held() -> None:
    """Detects a family reported as ingest-only or the wrong eligibility: the report lists each of
    the ten as ``silver``, the three OBP ``eligible`` and ``utc_instant`` with the CKAN vintage,
    the seven others ``held: TODO:`` (the mock four with the test-data question), all
    ``whole_capture``."""
    report = eligibility_module.render(load_registry())
    for key in RECORDED:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver |" in line and "| whole_capture |" in line, key
        if key in OBP:
            assert "| silver | eligible |" in line and "| utc_instant |" in line, key
        else:
            assert "| silver | held: TODO:" in line, key
        if key in MOCKS:
            assert "EAC-MOCK-TEST-DATA" in line and "gridflow capture time" in line, key
        else:
            assert "CKAN last_modified of the captured file (ADR-030)" in line, key


@pytest.mark.parametrize("key", RECORDED)
def test_every_family_is_generic_tabular_silver_with_its_own_record(key: str) -> None:
    """Detects a family wired to a bespoke transformer instead of the generic engine."""
    _package, entry = load_registry().families[key]
    assert entry.record is not None and entry.transformer is None
    assert entry.kind == "tabular"


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", LOADABLE)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a BOM
    reaching the first silver column name, a duplicate key or an excluded row: the slice loads every
    row with zero exclusions and no failure, every identifier, label, instant and measure equals
    the body cell, the entity key is unique, and the temporal column is the OBP window / point
    start (tz-aware UTC, no shift) or, with no recipe, the capture time. A fixture that failed a
    strict datetime parse or a strict float / integer cast would fail here."""
    family = CAPTURES[alias].family
    frame = _load_clean(data, alias)
    _assert_cells(family, alias, frame)
    assert not frame.select(key_of(family, header(alias))).is_duplicated().any()
    if family in TEMPORAL_COLUMN:
        assert frame["timestamp_utc"].to_list() == frame[TEMPORAL_COLUMN[family]].to_list()
        assert frame.schema[TEMPORAL_COLUMN[family]] == pl.Datetime("us", "UTC")
        assert frame[TEMPORAL_COLUMN[family]].null_count() == 0
    else:
        written = datetime.fromisoformat(CAPTURES[alias].written)
        assert set(frame["timestamp_utc"].to_list()) == {written}


def test_mock_time_cells_are_raw_strings_and_ids_are_not_cast(data: Path) -> None:
    """Detects a mock clock parsed or shifted and an identifier cast: the delivery start / end and
    order entry cells of all four mock cuts are byte-equal strings (``2026-02-24T16:30:00`` stays
    text), the opaque ids (``unitResultID`` with its ``#||#`` separators, auction and order ids) are
    strings, and the mock's re-used small auction ids stay text."""
    frame = _load_clean(data, "m1")
    assert frame.schema["delivery_start"] == pl.String
    assert frame["delivery_start"].to_list() == [r["deliveryStart"] for r in rows("m1")]
    assert frame["delivery_start"].to_list()[0] == "2026-02-24T16:30:00"
    assert frame["unit_result_id"].to_list() == [r["unitResultID"] for r in rows("m1")]
    assert "#||#" in frame["unit_result_id"].to_list()[0]
    assert frame["technology_type"].null_count() == frame.height


@pytest.mark.parametrize("alias", ["m2", "m3", "m4"])
def test_mock_order_summary_and_sell_cuts_keep_time_strings_and_text_ids(
    data: Path, alias: str
) -> None:
    """Detects the mock buy / summary / sell clocks typed, or an auction id cast to an integer
    (mock ids 1-22 are reused for different dates, so an id is never a join key to live EAC): the
    delivery and submission columns present are strings equal to the body and the auction id is a
    string."""
    frame = _load_clean(data, alias)
    for column, source in (
        ("delivery_start", "deliveryStart"),
        ("delivery_end", "deliveryEnd"),
        ("order_entry_time", "orderEntryTime"),
    ):
        if column in frame.columns:
            assert frame.schema[column] == pl.String
            assert frame[column].to_list() == [r[source] for r in rows(alias)]
    assert frame.schema["auction_id"] == pl.String


def test_the_historic_orps_keeps_both_spln_rows_and_every_cell_as_text(data: Path) -> None:
    """Detects the repeated natural identity (SPLN, SPLN_01Z, D, M) collapsed or the value cells
    cleaned: both ``SPLN_01Z`` rows are kept (rows 181 and 277 of the body), the comma numeral
    ``1,120`` and the padded dash ``' -   '`` survive byte-equal as strings, blanks are null, all
    238 columns are text, and the 238-column key is unique although the four identity columns are
    not."""
    frame = _load_clean(data, "r3")
    assert frame.width - 4 >= 238  # 238 silver columns plus the engine stamps
    spln = frame.filter(pl.col("unit") == "SPLN_01Z")
    assert spln.height == 2
    assert spln.select(ORPS_IDENTITY).is_duplicated().all()
    assert not frame.select(key_of(HIST, header("r3"))).is_duplicated().any()
    names = [silver_name(c) for c in header("r3")]
    comma_row = next(r for r in rows("r3") if r["Unit"] == "KLYNA01Z")
    comma_source = next(k for k, v in comma_row.items() if v == "1,120")
    assert frame.filter(pl.col("unit") == "KLYNA01Z")[silver_name(comma_source)].to_list() == [
        "1,120"
    ]
    dash_row = next(r for r in rows("r3") if r["Unit"] == "CORB_01Z")
    dash_source = next(k for k, v in dash_row.items() if v.strip() == "-" and v != "-")
    padded = dash_row[dash_source]
    assert frame.filter(pl.col("unit") == "CORB_01Z")[silver_name(dash_source)].to_list() == [
        padded
    ]
    assert padded != padded.strip() and padded.strip() == "-"
    assert {frame.schema[n] for n in names} == {pl.String}
    assert frame.select(names).null_count().sum_horizontal().item() > 0


def test_the_reactive_archive_keeps_a_blank_monthly_cell_null_and_the_bom_off_the_first_name(
    data: Path,
) -> None:
    """Detects a blank monthly cell filled with zero (2,895 of them are blank in the archive and
    published as 0 by the long table; TODO: ORPS-REACTIVE-ARCHIVE) and a UTF-8 BOM leaking into the
    first column name: unit KILW's blank first month is null, the first silver column is ``id`` and
    an opaque string, the monthly values are float64, and zero values are kept as zero."""
    assert body("r1").startswith(b"\xef\xbb\xbf")
    frame = _load_clean(data, "r1")
    assert frame.columns[0] == "id" and frame.schema["id"] == pl.String
    assert not any("﻿" in c for c in frame.columns)
    blank = frame.filter(pl.col("unit") == "KILW_01Z")
    assert blank["apr_20_lead"].to_list() == [None]
    assert frame.schema["apr_20_lead"] == frame.schema["dec_24_total"] == pl.Float64
    assert frame.filter(pl.col("unit") != "KILW_01Z")["apr_20_lead"].null_count() == 0


def test_the_default_rates_keep_year_and_month_as_vendor_integers(data: Path) -> None:
    """Detects ``Year`` / ``Month`` colliding with the Hive partition names (the engine would drop
    or raise) or a composed date: the silver columns are ``year_vendor`` / ``month_vendor`` int64,
    the seven rate / index columns are float64 (``X = 0.2`` is ``x_0_2``), and the BOM does not
    reach the first name."""
    assert body("r4").startswith(b"\xef\xbb\xbf")
    frame = _load_clean(data, "r4")
    assert frame.schema["year_vendor"] == frame.schema["month_vendor"] == pl.Int64
    assert frame["year_vendor"].to_list() == [2026, 2026] and frame["month_vendor"].to_list() == [
        10,
        9,
    ]
    for name in ("hpim", "papim", "ppim", "frpim", "im", "x_1", "x_0_2"):
        assert frame.schema[name] == pl.Float64, name
    assert "year" not in frame.columns and "month" not in frame.columns


def test_a_long_table_body_under_the_wide_archive_fails_loud(data: Path) -> None:
    """Detects the held long table loading against the wide record (or its header being frozen as a
    second epoch): a body with R2's 8-column header filed as the wide archive's resource matches no
    epoch, so the capture fails with no completion and no silver."""
    capture_id = capture(data, "r1", raw=body("r2"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, REACT, data).run(DAY, run_id="r")
    assert read_completion(data, REACT, capture_id) is None
    assert _no_silver(data, REACT)


def test_the_held_long_table_resource_is_not_read_beside_the_wide_archive(data: Path) -> None:
    """Detects the HOLD resource leaking into silver: with both captures present only the wide
    archive's rows are written (the long table's duplicate row is neither deduplicated nor a
    failure, because that resource is not part of the family's output)."""
    capture(data, "r1")
    capture(data, "r2")
    written = get_transformer(SOURCE, REACT, data).run(DAY, run_id="r")
    assert written == len(rows("r1"))
    frame = _silver(data, REACT)
    assert frame.height == len(rows("r1"))
    assert "month_year" not in frame.columns


def test_pn_keeps_a_sixty_second_segment_and_a_bst_season_start_unshifted(data: Path) -> None:
    """Detects a PN segment rounded to the half hour, a UTC clock shifted as if it were UK local
    time and a level mistaken for a settlement value: the 60-second segment keeps both endpoints,
    the ``2026-07-01T05:00:00`` start is 05:00 UTC (not 04:00), the levels are floats in MW and the
    ``Data`` code ``PN`` is text."""
    frame = _load_clean(data, "p1")
    sixty = frame.filter(pl.col("time_to") - pl.col("time_from") == timedelta(seconds=60))
    assert sixty.height == 1 and sixty["level_from"].to_list() == [2.0]
    assert sixty["level_to"].to_list() == [6.0]
    starts = frame["time_from"].to_list()
    assert datetime(2026, 7, 1, 5, 0, tzinfo=UTC) in starts
    assert datetime(2026, 7, 1, 4, 0, tzinfo=UTC) not in starts
    assert frame.schema["data"] == pl.String and frame["data"].to_list() == ["PN"] * frame.height
    assert frame.schema["level_from"] == frame.schema["level_to"] == pl.Float64


def test_a_start_and_a_vp_of_one_instruction_keep_their_own_issue_time_and_the_row_survives(
    data: Path,
) -> None:
    """Detects the dispatch issue time propagated to its other points or a null issue excluding the
    row: instruction 3192267's START has issue ``2025-10-10T13:02:00``, its VP points have a null
    issue and are kept, ``sequence`` is int64, ids are strings and the unique reporting key is the
    point id."""
    frame = _load_clean(data, "i1")
    one = frame.filter(pl.col("instruction_id") == "3192267").sort("sequence")
    assert one["sequence"].to_list() == [1, 2, 3, 4]
    assert one["instruction_point_type"].to_list() == ["START", "VP", "STOP", "VP"]
    assert one["issue_datetime"].to_list() == [
        datetime(2025, 10, 10, 13, 2, tzinfo=UTC),
        None,
        datetime(2025, 10, 10, 13, 8, tzinfo=UTC),
        None,
    ]
    assert one["instruction_status"].to_list() == ["ACCEPTED", None, "ACCEPTED", None]
    assert one["point_datetime"].null_count() == 0
    assert frame.schema["sequence"] == pl.Int64
    assert {frame.schema[n] for n in ("instruction_id", "instruction_point_id")} == {pl.String}
    assert frame.schema["target_mw"] == frame.schema["target_price"] == pl.Float64
    assert "issue_time" not in frame.columns


def test_availability_keeps_the_negative_pair_and_the_price_extremes(data: Path) -> None:
    """Detects ``Pair ID`` ``-1`` read as a missing-value sentinel and the +-10000 prices clamped:
    the negative-service pair stays ``-1`` (int64, not null), the two extreme rows are both kept
    with their prices as published, zero availability stays ``0.0`` and the key including both band
    and service is unique."""
    frame = _load_clean(data, "a1")
    assert frame.schema["pair_id"] == pl.Int64
    assert -1 in frame["pair_id"].to_list() and frame["pair_id"].null_count() == 0
    assert {10000.0, -10000.0} <= set(frame["utilisation_price"].to_list())
    assert 0.0 in frame["availability_power"].to_list()
    assert frame["data"].to_list() == ["AV"] * frame.height
    same_slot = frame.filter(pl.col("time_from") == datetime(2025, 11, 18, 15, 30, tzinfo=UTC))
    assert same_slot.height == 2
    assert not same_slot.select(KEY_OF[AVAIL]).is_duplicated().any()


# --------------------------------------------------------------------------- #
# Loud failure, vintage and catalogue
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epoch_fails_loud(data: Path, family: str) -> None:
    """Detects a silent header drift (a renamed vendor column loaded under the old typing): a body
    whose first header cell is renamed fails the capture with no completion and no silver."""
    alias = FIRST_OF[family]
    raw = body(alias)
    first_cell = header(alias)[0].encode()
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
    ``DuplicateEntityKeyError``, loads nothing and leaves no completion (the historic ORPS table's
    key is all 238 columns, so only a repeated full row fails it)."""
    alias = FIRST_OF[family]
    terminated = body(alias).split(b"\n")
    assert terminated[-1] == b""
    raw = b"\n".join([*terminated[:-1], terminated[-2], b""])
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["r1", "r3", "r4", "p1", "i1", "a1"])
def test_an_upload_family_is_stamped_with_the_ckan_last_modified(data: Path, alias: str) -> None:
    """Detects an upload-only family falling back to capture time or a catalogue view that cannot
    carry the new columns: ``available_at`` is the CKAN ``last_modified`` read as UTC; an as-of read
    before it serves nothing and one after serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    assert meta.modified is not None
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


@pytest.mark.parametrize("alias", ["m1", "m2", "m3", "m4"])
def test_a_mock_datastore_family_is_stamped_with_the_capture_time(data: Path, alias: str) -> None:
    """Detects a datastore mock family (whose sidecar has no ``last_modified``) taking a CKAN
    vintage: ``available_at`` is the capture's ``written_at`` and an as-of read before it serves
    nothing."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    written = datetime.fromisoformat(meta.written).astimezone(UTC)
    assert set(frame["available_at"].to_list()) == {written}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, meta.family, written - timedelta(microseconds=1)) == []
    assert set(both_as_of(db, data, meta.family, written)) == {capture_id}


@pytest.mark.parametrize("key", RECORDED)
def test_the_skeleton_page_renders_the_new_records(key: str) -> None:
    """Detects a record the docs generator cannot render (the held questions, the em dashes and
    backticks, the 238-column key, the two-vintage mix)."""
    page = skeleton.render_package(
        load_registry(),
        {
            "name": PACKAGE_OF[key],
            "title": PACKAGE_OF[key],
            "organization": {"title": "NESO"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    assert f"`{key}`" in page
