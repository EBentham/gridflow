"""BSAD settlement aggregates, forward contracts and the legacy Dynamic Containment package: seven
frozen records, five eligible and two held, plus the disaggregated no-record family (v0.22-K-AS-4a).

Three package files carry the records: ``aggregated-bsad`` (``aggregated_bsad``),
``balancing-services-adjustment-data-forward-contracts`` (``bsad_forward_contracts``) and the
retired
``dynamic-containment-data`` package (masterdata, results summary, results by unit, linear orders,
block orders). ``disaggregated_bsad`` stays ingest-only with no record (RULINGS 538). Every test
writes recorded fixture captures (cuts of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/as4a/``, provenance in ``PROVENANCE.md``) into a short data root
and runs the transformer the **real package registry** generates, so a record that does not fit its
vendor body fails here, not at activation.

Record decisions under test (K-AS-4-FACTS g1-g6 and sections 3, 4 and 6, K-AS-4a-SPEC):

- ``issue: none`` everywhere. The BSAD tables carry no publication column (methodology permits
  amendments), so the CKAN last-modified is not an availability and the datastore resources take
  ``capture_fallback``; the upload-only DC masterdata takes ``ckan_last_modified``.
  ``OrderEntryTime``
  is the bid **submission** instant, not a result publication time, and stays a raw string.
- The BSAD families are resource-partitioned (``latest_partition: resource_id``) with the natural
  settlement pair plus ``resource_id`` as the key. No header carries a settlement run or
  revision, so
  there is no ``run_type`` and nothing is deduplicated on (date, period). The 2020-21 aggregated
  upload and the 2016-17 / 2020-21 forward uploads repeat settlement periods and fail the capture as
  ``DuplicateEntityKeyError``; the 2013-14 / 2014-15 aggregated uploads carry a standalone 0xA3
  header byte, are not valid UTF-8, and fail as ``UnicodeDecodeError`` (ADR-040 ledger entries,
  never re-decoded). The legacy A-H0 header is not frozen.
- Opaque identifiers (``Unique bid number``, ``OrderID``, ``OrderPeriodID``, ``TradeID``, codes and
  flags) are strings, never cast. Zeros stay zero, ``-1`` stays ``-1.0`` (signed BSA and block
  volumes) and the literal ``NA`` / ``N/A`` strings survive; there is no trim, token map or bound
  except ``settlement_period`` ``1..50``.
- The DC results summary and the linear orders are held on their open unit questions
  (DC-SUMMARY-PRICE-UNIT, DC-LINEAR-PRICE-UNITS). Result and order endpoints stay raw strings
  (DC-RESULT-EXPORT-CLOCK, DC-ORDER-EXPORT-CLOCK); ``efa_date`` and ``EFA`` are never composed into
  an instant (RULINGS 572 / 588).
- Legacy DC results are auction results, not requirement forecasts: never join or sum them with the
  AS-1 forecasts.
"""

from __future__ import annotations

import csv
import hashlib
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
from gridflow.connectors.neso_data_portal.registry.record import (
    RecordError,
    has_issue_time,
    validate_record,
)
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as4a"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
LONDON = ZoneInfo("Europe/London")
UNIT = "v0.22-K-AS-4a"
RULING = "748"

AGG = "aggregated_bsad"
FWD = "bsad_forward_contracts"
DIS = "disaggregated_bsad"
MASTER = "dynamic_containment_masterdata"
SUM = "dynamic_containment_dc_dr_dm_results_sum"
RES = "dynamic_containment_dc_dr_dm_results"
LIN = "dynamic_containment_dc_dr_dm_linear"
BLK = "dynamic_containment_dc_dr_dm_block"
RECORDED = (AGG, FWD, MASTER, SUM, RES, LIN, BLK)
ELIGIBLE = (AGG, FWD, MASTER, RES, BLK)
HELD = (SUM, LIN)
BSAD = (AGG, FWD)

PACKAGE_OF = {
    AGG: "aggregated-bsad",
    FWD: "balancing-services-adjustment-data-forward-contracts",
    DIS: "disaggregated-bsad",
    MASTER: "dynamic-containment-data",
    SUM: "dynamic-containment-data",
    RES: "dynamic-containment-data",
    LIN: "dynamic-containment-data",
    BLK: "dynamic-containment-data",
}
PACKAGE_ID_OF = {
    "aggregated-bsad": "69c869a5-e6c6-4f48-852b-395a05882ef9",
    "balancing-services-adjustment-data-forward-contracts": "ca51d03a-53e8-4397-aee1-2f40f9973b96",
    "dynamic-containment-data": "aca07dcb-f807-409c-a4ec-da5dc052b8ba",
}

H_A1 = [
    "Date",
    "Settlement Period",
    "EBCA (£)",
    "EBVA (MWh)",
    "SBVA (MWh)",
    "BPA (£/MWh)",
    "ESCA (£)",
    "ESVA (MWh)",
    "SSVA (MWh)",
    "SPA (£/MWh)",
]
H_A2 = [
    "Date",
    "SettlementPeriod",
    "EBCA",
    "EBVA",
    "SBVA",
    "BPA",
    "ESCA",
    "ESVA",
    "SSVA",
    "SPA",
]
H_A0 = [
    "Date",
    "Settlement Period",
    "EBCA (£)",
    "EBVA (MWh)",
    "ESCA (£)",
    "ESVA (MWh)",
    "SBVA (MWh)",
    "SSVA (MWh)",
    "BPA (£/MWh)",
    "SPA (£/MWh)",
]
H_F0 = [
    "Date",
    "Settlement Period",
    "BCA (£)",
    "BSA (MWh)",
    "BVA (MWh)",
    "SCA (£)",
    "SSA (MWh)",
    "SVA (MWh)",
]
H_F1 = ["Date", "SettlementPeriod", "BCA", "BSA", "BVA", "SCA", "SSA", "SVA"]
H_M = [
    "Market Name",
    "Delivery Date",
    "Unique bid number",
    "Response Unit",
    "Unit type",
    "Agent/Applicant",
    "Related Entity",
    "Volume offered",
    "Volume Accepted",
    "Delivery Start UTC",
    "Delivery End UTC",
    "Service Duration",
    "Availability Fee",
    "Total Cost",
    "RTM/no RTM",
    "Accepted/Rejected",
    "Rejection code",
    "Technology Type",
]
H_S = [
    "Service",
    "EFA Date",
    "Delivery Start",
    "Delivery End",
    "EFA",
    "Cleared Volume",
    "Clearing Price",
]
H_U = [
    "Company",
    "Unit Name",
    "EFA Date",
    "Delivery Start",
    "Delivery End",
    "EFA",
    "Service",
    "Cleared Volume",
    "Clearing Price",
    "Technology Type",
    "Location",
    "Cancelled",
]
CURVE = [f"{n}{k}" for n in range(1, 33) for k in "PV"]
H_L = [
    "MarketName",
    "BiddingLevelName",
    "MemberName",
    "OrderID",
    "Portfolio",
    "OrderEntryTime",
    "OrderEntryUser",
    "SettlementCurrency",
    "OrderPeriodID",
    "TradeID",
    "EFA",
    "DeliveryStart",
    "DeliveryEnd",
    "ExecutedVolume",
    *CURVE,
]
H_B = [
    "MarketName",
    "BiddingLevelName",
    "MemberName",
    "OrderID",
    "Portfolio",
    "OrderEntryTime",
    "OrderEntryUser",
    "SettlementCurrency",
    "ClearingPrice",
    "Price",
    "MAR",
    "Status",
    "BlockCode",
    "BlockCodePRM",
    "Paradoxically",
    "OrderPeriodID",
    "TradeID",
    "EFA",
    "DeliveryStart",
    "DeliveryEnd",
    "ExecutedVolume",
    "Volume",
    "Invalid",
    "Bid Validity",
]
HEADERS_OF: dict[str, list[list[str]]] = {
    AGG: [H_A1, H_A2],
    FWD: [H_F0, H_F1],
    MASTER: [H_M],
    SUM: [H_S],
    RES: [H_U],
    LIN: [H_L],
    BLK: [H_B],
}

# source header -> (silver name, kind); kind: f float64, i int64, d date, t UTC datetime, s string.
S_AGG = {
    "Date": ("settlement_date", "d"),
    "Settlement Period": ("settlement_period", "i"),
    "SettlementPeriod": ("settlement_period", "i"),
    **{
        f"{n} ({u})": (n.lower(), "f")
        for n, u in (
            ("EBCA", "£"),
            ("EBVA", "MWh"),
            ("SBVA", "MWh"),
            ("BPA", "£/MWh"),
            ("ESCA", "£"),
            ("ESVA", "MWh"),
            ("SSVA", "MWh"),
            ("SPA", "£/MWh"),
        )
    },
    **{n: (n.lower(), "f") for n in ("EBCA", "EBVA", "SBVA", "BPA", "ESCA", "ESVA", "SSVA", "SPA")},
}
S_FWD = {
    "Date": ("settlement_date", "d"),
    "Settlement Period": ("settlement_period", "i"),
    "SettlementPeriod": ("settlement_period", "i"),
    **{
        f"{n} ({u})": (n.lower(), "f")
        for n, u in (
            ("BCA", "£"),
            ("BSA", "MWh"),
            ("BVA", "MWh"),
            ("SCA", "£"),
            ("SSA", "MWh"),
            ("SVA", "MWh"),
        )
    },
    **{n: (n.lower(), "f") for n in ("BCA", "BSA", "BVA", "SCA", "SSA", "SVA")},
}
S_MASTER = {
    "Market Name": ("market_name", "s"),
    "Delivery Date": ("delivery_date", "d"),
    "Unique bid number": ("unique_bid_number", "s"),
    "Response Unit": ("response_unit", "s"),
    "Unit type": ("unit_type", "s"),
    "Agent/Applicant": ("agent_applicant", "s"),
    "Related Entity": ("related_entity", "s"),
    "Volume offered": ("volume_offered", "f"),
    "Volume Accepted": ("volume_accepted", "f"),
    "Delivery Start UTC": ("delivery_start", "t"),
    "Delivery End UTC": ("delivery_end", "t"),
    "Service Duration": ("service_duration", "i"),
    "Availability Fee": ("availability_fee", "f"),
    "Total Cost": ("total_cost", "f"),
    "RTM/no RTM": ("rtm_no_rtm", "s"),
    "Accepted/Rejected": ("accepted_rejected", "s"),
    "Rejection code": ("rejection_code", "s"),
    "Technology Type": ("technology_type", "s"),
}
S_SUM = {
    "Service": ("service", "s"),
    "EFA Date": ("efa_date", "d"),
    "Delivery Start": ("delivery_start", "s"),
    "Delivery End": ("delivery_end", "s"),
    "EFA": ("efa", "i"),
    "Cleared Volume": ("cleared_volume", "f"),
    "Clearing Price": ("clearing_price", "f"),
}
S_RES = {
    "Company": ("company", "s"),
    "Unit Name": ("unit_name", "s"),
    "EFA Date": ("efa_date", "d"),
    "Delivery Start": ("delivery_start", "s"),
    "Delivery End": ("delivery_end", "s"),
    "EFA": ("efa", "i"),
    "Service": ("service", "s"),
    "Cleared Volume": ("cleared_volume", "f"),
    "Clearing Price": ("clearing_price", "f"),
    "Technology Type": ("technology_type", "s"),
    "Location": ("location", "s"),
    "Cancelled": ("cancelled", "s"),
}
S_ORDER_HEAD = {
    "MarketName": ("market_name", "s"),
    "BiddingLevelName": ("bidding_level_name", "s"),
    "MemberName": ("member_name", "s"),
    "OrderID": ("order_id", "s"),
    "Portfolio": ("portfolio", "s"),
    "OrderEntryTime": ("order_entry_time", "s"),
    "OrderEntryUser": ("order_entry_user", "s"),
    "SettlementCurrency": ("settlement_currency", "s"),
    "OrderPeriodID": ("order_period_id", "s"),
    "TradeID": ("trade_id", "s"),
    "EFA": ("efa", "i"),
    "DeliveryStart": ("delivery_start", "s"),
    "DeliveryEnd": ("delivery_end", "s"),
    "ExecutedVolume": ("executed_volume", "f"),
}
S_LIN = {**S_ORDER_HEAD, **{c: (c[-1].lower() + c[:-1], "f") for c in CURVE}}
S_BLK = {
    **S_ORDER_HEAD,
    "ClearingPrice": ("clearing_price", "f"),
    "Price": ("price", "f"),
    "MAR": ("mar", "f"),
    "Status": ("status", "s"),
    "BlockCode": ("block_code", "s"),
    "BlockCodePRM": ("block_code_prm", "s"),
    "Paradoxically": ("paradoxically", "s"),
    "Volume": ("volume", "f"),
    "Invalid": ("invalid", "s"),
    "Bid Validity": ("bid_validity", "s"),
}
SILVER_OF = {
    AGG: S_AGG,
    FWD: S_FWD,
    MASTER: S_MASTER,
    SUM: S_SUM,
    RES: S_RES,
    LIN: S_LIN,
    BLK: S_BLK,
}

KEY_OF: dict[str, list[str]] = {
    AGG: ["resource_id", "settlement_date", "settlement_period"],
    FWD: ["resource_id", "settlement_date", "settlement_period"],
    MASTER: ["unique_bid_number"],
    SUM: ["service", "efa_date", "efa"],
    RES: ["unit_name", "efa_date", "efa", "service"],
    LIN: ["order_period_id"],
    BLK: ["order_period_id"],
}
TEMPORAL_OF: dict[str, dict[str, str | None]] = {
    AGG: {
        "kind": "sp_pair",
        "date_column": "settlement_date",
        "period_column": "settlement_period",
    },
    FWD: {
        "kind": "sp_pair",
        "date_column": "settlement_date",
        "period_column": "settlement_period",
    },
    MASTER: {"kind": "utc_instant", "column": "delivery_start"},
    SUM: {"kind": "date_sp1", "date_column": "efa_date"},
    RES: {"kind": "date_sp1", "date_column": "efa_date"},
    LIN: {"kind": "none"},
    BLK: {"kind": "none"},
}
PARTITIONED = {AGG, FWD}
UPLOAD_ONLY = {MASTER}
RAW_TIME = {SUM, RES, LIN, BLK}
DATE_FORMAT_OF = {
    "a03": "%d/%m/%Y",
    "a07": "%d/%m/%Y",
    "a10": "%Y-%m-%d",
    "f02": "%d/%m/%Y",
    "f05": "%d/%m/%Y",
    "f09": "%Y-%m-%d",
    "c01": "%d/%m/%Y",
    "c02": "%Y-%m-%d",
    "c03": "%Y-%m-%d",
}
MASTER_TIME = "%Y-%m-%dT%H:%M:%SZ"

# The held questions, verbatim (spec "7 linear orders" / "5 results summary").
QUESTION_OF = {
    SUM: (
        "TODO: DC-SUMMARY-PRICE-UNIT — the DC/DR/DM results-summary dictionary describes Clearing "
        "Price as a price per MW per hour but gives its unit as £/MW (the by-unit results table "
        "states £/MW/h); which rate dimension does this export publish? (The delivery start/end "
        "cells are naive and their export clock is also undocumented.)"
    ),
    LIN: (
        "TODO: DC-LINEAR-PRICE-UNITS — the linear-orders dictionary declares the nP price points "
        "in "
        "conflicting units (1P–4P, 6P–10P, 15P–20P, 25P–30P £/MW; 5P MW with "
        "test-placeholder text; "
        "11P–14P, 21P–24P, 31P–32P MW) while the auction guide calls every nP a price; what is the "
        "unit and hour basis of each nP column? (nV and ExecutedVolume are MW.)"
    ),
}

# sha256 of disaggregated-bsad.json with line endings normalised to LF (the file is byte-unchanged).
DISAGGREGATED_FILE_SHA256 = "e3365706681512c42d54be0087061669ab317d8fdd62f9be3e89f2e0d2e36e82"


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    family: str
    resource_id: str
    name: str
    filename: str
    modified: str | None
    written: str
    url_type: str


CAPTURES: dict[str, Capture] = {
    "a01": Capture(
        AGG,
        "d39fcf5e-32f9-4e44-a69d-b622effc165d",
        "Aggregated BSAD 2013-14",
        "bs_netbsd_01-04-2013_31-03-2014.csv",
        "2020-03-24T11:13:34.952296",
        "2026-10-08T08:13:04.367648+00:00",
        "upload",
    ),
    "a03": Capture(
        AGG,
        "15950c77-8f7d-4d12-8f29-25eaab6a5155",
        "Aggregated BSAD 2016-17",
        "bs_netbsd_01-04-2016_31-03-2017.csv",
        "2019-12-13T13:08:14.493080",
        "2026-10-08T08:13:11.077184+00:00",
        "upload",
    ),
    "a07": Capture(
        AGG,
        "ec526741-f629-4620-9e31-d14da373392e",
        "Aggregated BSAD 2020-21",
        "data.csv",
        "2021-11-25T18:17:13.412080",
        "2026-10-08T08:13:25.710672+00:00",
        "upload",
    ),
    "a10": Capture(
        AGG,
        "36070f3e-3d73-4776-8fb3-da9e76d4bf82",
        "Aggregated BSAD 2023-24",
        "36070f3e-3d73-4776-8fb3-da9e76d4bf82",
        None,
        "2026-10-08T08:14:26.565726+00:00",
        "datastore",
    ),
    "f02": Capture(
        FWD,
        "16fe508d-27da-4282-b8af-1fa519b6f480",
        "BSAD Forward Contracts 2017-18",
        "bs_forwardcontract_01-04-2017_31-03-2018.csv",
        "2019-12-13T13:00:03.669509",
        "2026-10-08T09:03:46.777374+00:00",
        "upload",
    ),
    "f05": Capture(
        FWD,
        "5d0f2119-1ba6-4d7d-8db5-ea4e23720aa9",
        "BSAD Forward Contracts 2020-21",
        "data.csv",
        "2021-11-25T18:16:36.056809",
        "2026-10-08T09:03:55.689175+00:00",
        "upload",
    ),
    "f09": Capture(
        FWD,
        "93f9bdde-1fd9-4619-99e2-d1c5e38239e1",
        "BSAD Forward Contracts 2024-25",
        "93f9bdde-1fd9-4619-99e2-d1c5e38239e1",
        None,
        "2026-10-08T09:05:38.318890+00:00",
        "datastore",
    ),
    "c01": Capture(
        MASTER,
        "0b8dbc3c-e05e-44a4-b855-7dd1aa079c68",
        "Dynamic Containment Masterdata",
        "dynamic_containment_masterdata.csv",
        "2026-05-29T12:35:35.769649",
        "2026-10-08T10:12:23.877435+00:00",
        "upload",
    ),
    "c02": Capture(
        SUM,
        "888e5029-f786-41d2-bc15-cbfd1d285e96",
        "DC, DR & DM Results Summary Master Data 2021-2023",
        "888e5029-f786-41d2-bc15-cbfd1d285e96",
        "2023-01-23T15:00:21.961017",
        "2026-10-08T10:12:20.379874+00:00",
        "datastore",
    ),
    "c03": Capture(
        RES,
        "ddc4afde-d2bd-424d-891c-56ad49c13d1a",
        "DC, DR & DM Results By Unit Master Data 2021-2023",
        "ddc4afde-d2bd-424d-891c-56ad49c13d1a",
        "2023-01-23T15:00:21.063804",
        "2026-10-08T10:12:05.000741+00:00",
        "datastore",
    ),
    "c04": Capture(
        LIN,
        "26aefbcc-fce0-403f-80e1-4a26af3fe84b",
        "DC, DR & DM Linear Orders Master Data 2021-2023",
        "26aefbcc-fce0-403f-80e1-4a26af3fe84b",
        "2023-01-23T15:00:17.792587",
        "2026-10-08T10:11:20.641555+00:00",
        "datastore",
    ),
    "c05": Capture(
        BLK,
        "6fd8e042-be27-4c67-ad59-5acdd2a7b0fd",
        "DC, DR & DM Block Orders Master Data 2021-2023",
        "6fd8e042-be27-4c67-ad59-5acdd2a7b0fd",
        "2023-01-23T15:00:15.354075",
        "2026-10-08T10:11:19.165870+00:00",
        "datastore",
    ),
}
FIRST_OF = {AGG: "a03", FWD: "f02", MASTER: "c01", SUM: "c02", RES: "c03", LIN: "c04", BLK: "c05"}
LOADING = [a for a in CAPTURES if a not in ("a01", "a07", "f05")]
CRLF_ALIASES = {"a01", "a03", "a07", "f02", "f05", "c01"}
BOM_ALIASES = {"a03", "f02"}
FAILING = {
    "a01": ("UnicodeDecodeError", AGG),
    "a07": ("DuplicateEntityKeyError", AGG),
    "f05": ("DuplicateEntityKeyError", FWD),
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
        prefix="as4a", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: the bronze bytes, CRLF or LF)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def _text(raw: bytes) -> str:
    return raw.decode("utf-8").removeprefix("﻿")


def rows(alias: str, raw: bytes | None = None, *, keep_blank: bool = False) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim); wholly blank rows only on
    request."""
    text = _text(raw if raw is not None else body(alias))
    reader = csv.DictReader(io.StringIO(text, newline=""))
    return [r for r in reader if keep_blank or any(r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header (BOM removed)."""
    first = _text(body(alias)).splitlines()[0]
    return next(csv.reader([first]))


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


def _package_doc(package: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / f"{package}.json").read_text("utf-8"))
    return document


def _columns(key: str) -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record(key)))
        if c not in ("year", "month")
    ]


QUOTED_BLANK = {("c03", "Location"), ("c03", "Cancelled"), ("c05", "Bid Validity")}
"""String cells the vendor writes as a quoted empty ``""``: the CSV reader keeps them as the empty
string exactly as published (the K-TRD ``Notes`` precedent); an unquoted blank is null."""


def _expected(alias: str, spec: tuple[str, str], cell: str, source: str = "") -> Any:
    """What the silver cell must be for body cell ``cell`` (an unquoted blank cell is null, a
    quoted empty string stays empty)."""
    _name, kind = spec
    if cell == "":
        return "" if (alias, source) in QUOTED_BLANK else None
    if kind == "f":
        return float(cell)
    if kind == "i":
        return int(cell)
    if kind == "d":
        return datetime.strptime(cell, DATE_FORMAT_OF[alias]).date()
    if kind == "t":
        return datetime.strptime(cell, MASTER_TIME).replace(tzinfo=UTC)
    return cell


def _assert_cells(alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, date, instant and measure of ``frame`` equals the body cell."""
    family = CAPTURES[alias].family
    source_rows = rows(alias)
    for source in header(alias):
        spec = SILVER_OF[family][source]
        assert frame[spec[0]].to_list() == [
            _expected(alias, spec, r[source], source) for r in source_rows
        ], (
            alias,
            source,
        )


def _anchor(day: date) -> datetime:
    """The UTC instant of local midnight on ``day`` (the label anchor ``date_sp1``)."""
    return datetime(day.year, day.month, day.day, tzinfo=LONDON).astimezone(UTC)


def _sp_start(day: date, period: int) -> datetime:
    """The UTC start of settlement period ``period``: elapsed half-hours from UK-local midnight."""
    return _anchor(day) + timedelta(minutes=30 * (period - 1))


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
    assert read_completion(data, family, capture_id) is None
    assert read_failure(data, family, capture_id) is not None
    assert _no_silver(data, family)
    return got_id, cls


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the exact vendor headers
    (BOM on the A-H1 upload and the F-H0 upload, none elsewhere), CRLF on the uploads and a lone LF
    on the datastore bodies, a01 not valid UTF-8 (its standalone 0xA3), the two wholly blank
    trailing rows of a03 and f02, the DST periods of a10, the repeated 27/06/2020 rows of a07, the
    conflicting 25/08/2020 P1 pair of f05, the ``-1`` BSA of f09, and the master / results / block
    rows each C-case needs (Withdraw, ``N/A``, ``NA``, blank total cost, 23 and 25 hour service,
    padded agent, fractional MAR, literal ``NA`` technology type)."""
    assert set(FIRST_OF.values()) <= set(CAPTURES)
    for alias in CAPTURES:
        raw = body(alias)
        assert raw.startswith(b"\xef\xbb\xbf") == (alias in BOM_ALIASES), alias
        assert raw.endswith(b"\n"), alias
        assert raw.count(b"\r\n") == (raw.count(b"\n") if alias in CRLF_ALIASES else 0), alias
    with pytest.raises(UnicodeDecodeError):
        body("a01").decode("utf-8")
    assert b"\xa3" in body("a01").splitlines()[0]
    for alias in CAPTURES:
        if alias != "a01":
            body(alias).decode("utf-8")
    assert header("a03") == header("a07") == H_A1 and header("a10") == H_A2
    assert header("f02") == header("f05") == H_F0 and header("f09") == H_F1
    assert header("c01") == H_M and header("c02") == H_S and header("c03") == H_U
    assert header("c04") == H_L and header("c05") == H_B
    assert len(rows("a03", keep_blank=True)) == len(rows("a03")) + 2
    assert len(rows("f02", keep_blank=True)) == len(rows("f02")) + 2
    a10 = [(r["Date"], int(r["SettlementPeriod"])) for r in rows("a10")]
    assert a10 == [
        ("2023-10-29", 1),
        ("2023-10-29", 2),
        ("2023-10-29", 49),
        ("2023-10-29", 50),
        ("2024-03-31", 1),
        ("2024-03-31", 3),
        ("2024-03-31", 46),
    ]
    a07 = [(r["Date"], r["Settlement Period"]) for r in rows("a07")]
    assert len(a07) == 4 and len(set(a07)) == 2 and {d for d, _p in a07} == {"27/06/2020"}
    f05 = rows("f05")
    assert [(r["Date"], r["Settlement Period"]) for r in f05] == [("25/08/2020", "1")] * 2
    assert {r["BSA (MWh)"] for r in f05} == {"235", "0"}
    assert [r["BSA"] for r in rows("f09") if r["Date"] == "2025-01-08"] == ["-1"]
    master = rows("c01")
    assert any(r["Market Name"] == "Withdraw" for r in master)
    assert {"N/A", "NA"} <= {r["Related Entity"] for r in master}
    assert any(r["Total Cost"] == "" for r in master)
    assert {"23", "25"} <= {r["Service Duration"] for r in master}
    assert any(r["Agent/Applicant"] != r["Agent/Applicant"].strip() for r in master)
    assert any(r["Technology Type"] == "NA" for r in rows("c03"))
    assert any(r["MAR"] not in ("0", "1") and "." in r["MAR"] for r in rows("c05"))
    assert {r["Volume"] for r in rows("c05")} >= {"-1"}


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_partition_vintage_and_no_issue(
    key: str,
) -> None:
    """Detects a record that invents an issue time (the BSAD tables carry none,
    ``OrderEntryTime`` is
    a bid submission), a ``key_latest`` selection, a ``run_type`` (no BSAD header carries a
    settlement run), a missing resource partition on the multi-resource BSAD families, a partition
    on a single-resource DC family, or the wrong vintage: every record is version 1, utf-8 CSV,
    whole-capture selection, the spec's entity key, ``capture_fallback`` for a family holding a
    datastore resource and ``ckan_last_modified`` for the upload-only masterdata, with the vendor
    header(s) as its epochs and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key in PARTITIONED else None)
    assert record.vintage == ("ckan_last_modified" if key in UPLOAD_ONLY else "capture_fallback")
    assert record.vintage_evidence is None
    assert list(record.entity_key) == KEY_OF[key]
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None
    assert [list(e.header) for e in record.epochs] == HEADERS_OF[key]
    for epoch in record.epochs:
        assert epoch.issue.kind == "none" and epoch.unpivot is None
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)


def test_the_bsad_epochs_are_the_exact_headers_and_the_legacy_one_is_not_frozen() -> None:
    """Detects a frozen A-H0 epoch (the 2013-14 / 2014-15 uploads' legacy header, ordered EBCA /
    EBVA / ESCA / ESVA / SBVA / SSVA / BPA / SPA and not valid UTF-8, FACTS l.252) or an epoch
    whose header was retyped rather than read from the body: the aggregated record has exactly the
    A-H1 (``Settlement Period``, ``(£)`` suffixes) and A-H2 (``SettlementPeriod``, bare names)
    headers, the forward record exactly F-H0 and F-H1, and H_A0 is in neither."""
    assert [list(e.header) for e in _record(AGG).epochs] == [H_A1, H_A2]
    assert [list(e.header) for e in _record(FWD).epochs] == [H_F0, H_F1]
    assert list(_record(AGG).epochs[0].header) != H_A0 != list(_record(AGG).epochs[1].header)
    assert header("a03") == list(_record(AGG).epochs[0].header)
    assert header("a10") == list(_record(AGG).epochs[1].header)
    assert header("f02") == list(_record(FWD).epochs[0].header)
    assert header("f09") == list(_record(FWD).epochs[1].header)


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: BSAD values are
    nullable float64 (zero is a value; the profiler's integer drafts are rejected) under the
    harmonised names of both epochs (V-1), ``settlement_period`` is the only bounded column
    (``1..50``), the BSAD date format is per epoch (``%d/%m/%Y`` for the uploads, ``%Y-%m-%d`` for
    the datastore bodies, no ``formats_by_filename``), identifiers and free text are strings, the
    DC master clocks are ``%Y-%m-%dT%H:%M:%SZ`` UTC datetimes while every other DC endpoint is a
    raw string, key columns and temporal inputs are ``nullable: false`` and nothing else is."""
    record = _record(key)
    non_null = set(KEY_OF[key]) - {"resource_id"}
    if key == MASTER:
        non_null.add("delivery_start")
    for epoch, expected_header in zip(record.epochs, HEADERS_OF[key], strict=True):
        assert [c.source for c in epoch.columns] == expected_header
        for column in epoch.columns:
            name, kind = SILVER_OF[key][column.source]
            assert column.name == name
            assert column.trim is None and column.null_tokens == ()
            assert column.formats_by_filename is None
            assert column.nullable == (name not in non_null), name
            if name == "settlement_period":
                assert (column.min, column.max) == (1, 50)
            else:
                assert (column.min, column.max) == (None, None), name
            assert column.zone_evidence is None and column.ambiguous is None
            if kind == "d":
                scalar = "%Y-%m-%d" if epoch.header[1] == "SettlementPeriod" else "%d/%m/%Y"
                if key in (SUM, RES):
                    scalar = "%Y-%m-%d"
                assert (column.dtype, column.format, column.zone) == ("date", scalar, None)
            elif kind == "t":
                assert (column.dtype, column.format, column.zone) == (
                    "datetime",
                    MASTER_TIME,
                    "UTC",
                )
            else:
                assert column.format is None and column.zone is None
                assert column.dtype == {"f": "float64", "i": "int64", "s": "string"}[kind], name


@pytest.mark.parametrize("key", RECORDED)
def test_identifiers_codes_and_endpoints_are_strings_never_cast(key: str) -> None:
    """Detects an opaque identifier cast to a number (the order ids are 15-digit integers and
    ``Unique bid number`` embeds a date and a unit name), a coded column cast (``Rejection code``,
    ``Status``, ``Invalid``) or a DC endpoint / submission time typed: every identifier column of
    every epoch is a string, and on the DC results and order families so are the delivery start and
    end and the order entry time."""
    identifiers = {
        "unique_bid_number",
        "order_id",
        "order_period_id",
        "trade_id",
        "rejection_code",
        "status",
        "invalid",
        "block_code",
        "block_code_prm",
        "service",
        "unit_name",
        "market_name",
    }
    raw_times = {"delivery_start", "delivery_end", "order_entry_time"}
    for epoch in _record(key).epochs:
        types = {c.name: c.dtype for c in epoch.columns}
        present = identifiers & set(types)
        if key in BSAD:
            assert not present
            continue
        assert present and {types[n] for n in present} == {"string"}
        if key in RAW_TIME:
            assert {types[n] for n in raw_times & set(types)} == {"string"}


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_spec_recipe(key: str) -> None:
    """Detects a composed DC block start (EFA + date, forbidden by RULINGS 572 / 588), a fabricated
    instant for the order tables (their clocks are undocumented) or a BSAD anchor other than the
    settlement pair: BSAD take ``sp_pair`` on the harmonised date and period, the master
    ``utc_instant`` of ``delivery_start``, results and summary ``date_sp1`` of ``efa_date`` and the
    two order tables ``none``."""
    recipe = _record(key).temporal
    expected = TEMPORAL_OF[key]
    assert recipe.kind == expected["kind"]
    assert recipe.date_column == expected.get("date_column")
    assert recipe.period_column == expected.get("period_column")
    assert recipe.column == expected.get("column")


@pytest.mark.parametrize("key", BSAD)
def test_a_bsad_family_cannot_take_ckan_last_modified(key: str) -> None:
    """Detects a BSAD record drifting to ``ckan_last_modified`` (V-8): each BSAD family holds
    datastore resources next to its uploads, so a copy of the record with that vintage fails the
    load rule, while the record itself passes it."""
    record = _record(key)
    kinds = frozenset({"datastore", "upload"})
    siblings = {family: True for family in RECORDED}

    def check(rec: SchemaRecord) -> None:
        validate_record(
            rec,
            key=key,
            kind="tabular",
            legacy=False,
            package_families=siblings,
            family_url_types=kinds,
        )

    check(record)
    with pytest.raises(RecordError, match=r"V-8:"):
        check(record.model_copy(update={"vintage": "ckan_last_modified"}))


def test_the_bsad_resource_lists_match_the_recorded_url_types() -> None:
    """Detects the V-8 premise drifting: the aggregated package holds 7 uploads and 6 datastore
    resources, the forward package 5 uploads and 6 datastore resources, the masterdata is the one
    upload-only DC family, and the other four DC families are datastore tables."""
    for package, expected in (
        (PACKAGE_OF[AGG], (7, 6)),
        (PACKAGE_OF[FWD], (5, 6)),
    ):
        kinds = [r["url_type"] for r in _package_doc(package)["resources"]]
        assert (kinds.count("upload"), kinds.count("datastore")) == expected
    by_family: dict[str, set[str]] = {}
    for resource in _package_doc("dynamic-containment-data")["resources"]:
        by_family.setdefault(resource["family"], set()).add(resource["url_type"])
    assert by_family == {
        MASTER: {"upload"},
        SUM: {"datastore"},
        RES: {"datastore"},
        LIN: {"datastore"},
        BLK: {"datastore"},
    }


# --------------------------------------------------------------------------- #
# Eligibility
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_five_eligible_records_carry_no_override(key: str) -> None:
    """Detects a hold or a per-record override on a clean family: each of the five is effectively
    eligible, in an eligible package."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", HELD)
def test_the_two_held_records_carry_their_verbatim_question_and_the_unit(key: str) -> None:
    """Detects a hold dropped, reworded or filed under the wrong unit (the summary price unit and
    the
    linear price points conflict inside the vendor dictionaries), or a package-wide hold: the record
    is ``held`` with the spec's question verbatim, unit ``v0.22-K-AS-4a``, effectively held, while
    its package stays eligible."""
    package, family = registry_module.load_registry().families[key]
    eligibility = _record(key).eligibility
    assert eligibility == Held(status="held", question=QUESTION_OF[key], unit=UNIT)
    assert effective_eligibility(package, family) == eligibility
    assert package.eligibility == Eligible(status="eligible")
    assert eligibility.question.startswith("TODO: DC-")


def test_the_four_packages_stay_eligible_and_the_package_files_hold_the_seven_records() -> None:
    """Detects a package held by accident, a record on the wrong family, a family gaining a
    transformer or a changed resource disposition: the four package files are ``eligible``, hold
    exactly the seven records, no family has a bespoke transformer and every resource stays a
    ``SILVER`` disposition of its own family."""
    found: set[str] = set()
    for package in sorted(set(PACKAGE_OF.values())):
        document = _package_doc(package)
        assert document["eligibility"] == {"status": "eligible"}, package
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), package
        assert all(r["disposition"]["kind"] == "SILVER" for r in document["resources"]), package
        assert all(r["disposition"]["key"] == r["family"] for r in document["resources"]), package
    assert found == set(RECORDED)


def test_the_eligibility_report_lists_five_eligible_two_held_and_disaggregated_ingest_only() -> (
    None
):
    """Detects a family reported with the wrong status: the report lists each eligible family as
    ``silver`` / ``eligible`` / ``whole_capture`` (``per resource_id`` for the two BSAD ones, with
    the capture-time vintage on every family but the upload-only masterdata), each held family as
    ``silver`` / ``held: TODO: DC-`` and ``disaggregated_bsad`` as ingest-only."""
    lines = eligibility_module.render(load_registry()).splitlines()

    def line_of(key: str) -> str:
        (line,) = [ln for ln in lines if f"| `{key}` |" in ln]
        return line

    for key in ELIGIBLE:
        line = line_of(key)
        assert "| silver | eligible |" in line, key
        assert ("whole_capture per resource_id" in line) == (key in PARTITIONED), key
        assert ("gridflow capture time" in line) == (key not in UPLOAD_ONLY), key
    for key in HELD:
        assert "| silver | held: TODO: DC-" in line_of(key), key
        assert f"(unit {UNIT})" in line_of(key), key
    assert "ingest-only (no silver output)" in line_of(DIS)


def test_disaggregated_bsad_has_no_record_and_its_file_is_byte_unchanged() -> None:
    """Detects ``disaggregated_bsad`` gaining a record, a transformer or an occurrence index
    (RULINGS 538): every captured body D01-D14 repeats identical complete rows, so no vendor-column
    key passes the duplicate guard and silent deduplication or a row ordinal is forbidden. The
    family
    stays ingest-only (``get_transformer`` raises), its resources stay ``SILVER`` dispositions, the
    ledger names no disaggregated or DC family, and the package file (line endings normalised to LF)
    matches the digest pinned from master. TODO: DISAGGREGATED-BSAD-ACTION-ID (a stable action
    identity settles it); TODO: DISAGGREGATED-BSAD-COST-SIGN; the D08 encoding; the D-H2 date-format
    map (D10 ``%Y-%m-%dT%H:%M:%S``) for a future record."""
    package, entry = registry_module.load_registry().families[DIS]
    assert entry.record is None and entry.transformer is None
    assert package.package == PACKAGE_OF[DIS]
    with pytest.raises(ValueError, match="No transformer registered"):
        get_transformer(SOURCE, DIS, Path("__test__"))
    document = _package_doc(PACKAGE_OF[DIS])
    assert all(r["disposition"] == {"kind": "SILVER", "key": DIS} for r in document["resources"])
    assert len(document["resources"]) == 14
    raw = (REGISTRY_DIR / f"{PACKAGE_OF[DIS]}.json").read_bytes().replace(b"\r\n", b"\n")
    assert hashlib.sha256(raw).hexdigest() == DISAGGREGATED_FILE_SHA256
    entries = registry_module.load_reconcile_adjudications()
    assert {e.family for e in entries} & {DIS, MASTER, SUM, RES, LIN, BLK} == set()


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", LOADING)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a duplicate
    key or an excluded row: the slice loads every populated row with zero exclusions and no failure,
    every identifier, label, date, instant and measure equals the body cell, the entity key is
    unique, and the temporal column is the settlement-period start, the UTC delivery start, the
    ``efa_date`` anchor or, with no recipe, the capture time. A cell failing its strict cast (a BSAD
    date or period, a master ``%Y-%m-%dT%H:%M:%SZ`` instant, ``efa_date``, ``efa``,
    ``service_duration``
    or any ``float64``) would fail here."""
    family = CAPTURES[alias].family
    frame = _load_clean(data, alias)
    _assert_cells(alias, frame)
    if "resource_id" in frame.columns:
        assert set(frame["resource_id"].to_list()) == {CAPTURES[alias].resource_id}
    keys = [k for k in KEY_OF[family] if k in frame.columns]
    assert not frame.select(keys).is_duplicated().any()
    for name in keys:
        assert frame[name].null_count() == 0, name
    recipe = TEMPORAL_OF[family]["kind"]
    if recipe == "none":
        written = datetime.fromisoformat(CAPTURES[alias].written)
        assert set(frame["timestamp_utc"].to_list()) == {written}
    elif recipe == "utc_instant":
        assert frame["timestamp_utc"].to_list() == frame["delivery_start"].to_list()
        assert frame.schema["delivery_start"] == pl.Datetime("us", "UTC")
    elif recipe == "date_sp1":
        assert frame["timestamp_utc"].to_list() == [_anchor(d) for d in frame["efa_date"]]
        assert frame.schema["efa_date"] == pl.Date
    else:
        assert frame["timestamp_utc"].to_list() == [
            _sp_start(d, p)
            for d, p in zip(frame["settlement_date"], frame["settlement_period"], strict=True)
        ]
    if family in RAW_TIME:
        for column in ("delivery_start", "delivery_end"):
            assert frame.schema[column] == pl.String
    assert frame["event_time"].to_list() == frame["timestamp_utc"].to_list()


def test_the_upload_with_trailing_blank_rows_loads_clean_with_zeros_kept(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects a wholly blank row (``,,,,,,,,,``) reaching silver as a null-key row or failing the
    capture, a zero read as missing, or a mis-formatted epoch: A03 (A-H1, ``%d/%m/%Y``, BOM, CRLF)
    drops its two blank rows before typing and logs the count (not an exclusion, so
    ``rows_excluded`` is 0), keeps the all-zero rows as ``0.0`` (never null), keeps the fractional
    BPA ``0.26`` at 04/04/2016 P31 and the 1-based period ``48`` row."""
    with caplog.at_level("INFO", logger="gridflow.silver.csv_bronze"):
        frame = _load_clean(data, "a03")
    assert len(rows("a03", keep_blank=True)) == frame.height + 2
    assert [m for m in (r.getMessage() for r in caplog.records) if "blank row" in m]
    assert frame.schema["settlement_date"] == pl.Date
    assert frame["settlement_date"].to_list()[0] == date(2016, 4, 1)
    assert frame["ebca"].to_list() == [0.0] * frame.height
    assert frame["ebca"].null_count() == 0 and frame.schema["ebca"] == pl.Float64
    assert 0.26 in frame["bpa"].to_list()
    assert 48 in frame["settlement_period"].to_list()
    assert frame.schema["settlement_period"] == pl.Int64


def test_the_datastore_resource_loads_a_dst_day_with_no_other_shift(data: Path) -> None:
    """Detects a settlement period placed on a naive clock or shifted against the UK settlement day:
    A10 (A-H2, ``%Y-%m-%d``, LF) carries the 50-period autumn day and the 46-period spring day, so
    2023-10-29 periods 1, 2, 49, 50 start 2023-10-28T23:00Z, 23:30Z, 2023-10-29T23:00Z, 23:30Z and
    2024-03-31 periods 1, 3, 46 start 00:00Z, 01:00Z, 22:30Z; the period survives its ``1..50``
    bound at 49 and 50."""
    frame = _load_clean(data, "a10")
    expected = [
        datetime(2023, 10, 28, 23, 0, tzinfo=UTC),
        datetime(2023, 10, 28, 23, 30, tzinfo=UTC),
        datetime(2023, 10, 29, 23, 0, tzinfo=UTC),
        datetime(2023, 10, 29, 23, 30, tzinfo=UTC),
        datetime(2024, 3, 31, 0, 0, tzinfo=UTC),
        datetime(2024, 3, 31, 1, 0, tzinfo=UTC),
        datetime(2024, 3, 31, 22, 30, tzinfo=UTC),
    ]
    assert frame["event_time"].to_list() == expected
    assert frame["timestamp_utc"].to_list() == expected
    assert frame["settlement_period"].to_list() == [1, 2, 49, 50, 1, 3, 46]


def test_the_two_bsad_epochs_of_a_family_load_together_under_distinct_resources(
    data: Path,
) -> None:
    """Detects one resource shadowing the other or an unstamped resource id: A03 (A-H1) and A10
    (A-H2) are different headers of one family, both load under their own ``resource_id``, the
    key is ``(resource_id, settlement_date, settlement_period)``, and the harmonised silver names
    carry one dtype (float64 values, date, int64 period)."""
    capture(data, "a03")
    capture(data, "a10")
    written = get_transformer(SOURCE, AGG, data).run(DAY, run_id="r")
    frame = _silver(data, AGG)
    assert written == frame.height == len(rows("a03")) + len(rows("a10"))
    assert set(frame["resource_id"].to_list()) == {
        CAPTURES["a03"].resource_id,
        CAPTURES["a10"].resource_id,
    }
    assert not frame.select(KEY_OF[AGG]).is_duplicated().any()
    assert frame.schema["settlement_date"] == pl.Date
    assert {
        frame.schema[n] for n in ("ebca", "ebva", "sbva", "bpa", "esca", "esva", "ssva", "spa")
    } == {pl.Float64}


def test_forward_contracts_load_both_epochs_and_the_signed_minus_one_stays_minus_one(
    data: Path,
) -> None:
    """Detects ``-1`` read as a missing-value sentinel (FACTS g5: no vendor source defines it as
    missing; TODO: FORWARD-BSA-MINUS-ONE, TODO: FORWARD-BSAD-SIGN-CONVENTION): F02 (F-H0, BOM, two
    blank rows dropped) and F09 (F-H1) load together under their own resources, F09 2025-01-08 P39
    has ``bsa`` ``-1.0``, not null, no bound is applied, and the non-integral volume ``307.5`` /
    ``12.5`` survives as float64."""
    both = _load_clean(data, "f02", "f09")
    assert set(both["resource_id"].to_list()) == {
        CAPTURES["f02"].resource_id,
        CAPTURES["f09"].resource_id,
    }
    frame = both.filter(pl.col("resource_id") == CAPTURES["f09"].resource_id)
    old = both.filter(pl.col("resource_id") == CAPTURES["f02"].resource_id)
    row = frame.filter(pl.col("settlement_date") == date(2025, 1, 8))
    assert row["settlement_period"].to_list() == [39]
    assert row["bsa"].to_list() == [-1.0] and row["bsa"].null_count() == 0
    assert row["timestamp_utc"].to_list() == [_sp_start(date(2025, 1, 8), 39)]
    assert 307.5 in old["sca"].to_list() and 12.5 in old["sva"].to_list()
    assert {both.schema[n] for n in ("bca", "bsa", "bva", "sca", "ssa", "sva")} == {pl.Float64}
    assert old["settlement_date"].to_list()[0] == date(2017, 4, 1)


def test_a_period_outside_one_to_fifty_is_excluded_with_a_tally(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects the ``1..50`` bound missing: a 51st period (an autumn day has 50) breaches the
    ``settlement_period`` bound, so the row is excluded with a tally (``rows_excluded`` 1, capture
    still populated) and the other six A10 rows load unchanged."""
    raw = body("a10").replace(b"2023-10-29,50,", b"2023-10-29,51,", 1)
    assert raw != body("a10")
    capture_id = capture(data, "a10", raw=raw)
    with caplog.at_level("WARNING"):
        written = get_transformer(SOURCE, AGG, data).run(DAY, run_id="r")
    assert written == len(rows("a10")) - 1
    completion = read_completion(data, AGG, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 1)
    assert 51 not in _silver(data, AGG)["settlement_period"].to_list()


def test_master_keeps_withdraw_both_not_applicable_spellings_and_unshifted_utc_instants(
    data: Path,
) -> None:
    """Detects a ``Withdraw`` row filtered as a malformed service, ``N/A`` and ``NA`` unified or
    nulled, a blank ``Total Cost`` filled, a padded agent trimmed, a rejection code cast, or the
    UTC instant shifted as if it were UK local time: the Withdraw row loads, ``N/A`` and ``NA``
    survive byte-equal in ``related_entity``, the blank total cost is null, the padded agent keeps
    its trailing space, code ``5`` is the string ``"5"``, the 23 / 24 / 25 hour services are int64,
    ``delivery_start`` 2020-10-01T22:00:00Z is 22:00 UTC (not 21:00) and ``timestamp_utc`` is that
    instant."""
    frame = _load_clean(data, "c01")
    assert "Withdraw" in frame["market_name"].to_list()
    assert {"N/A", "NA"} <= set(frame["related_entity"].to_list())
    assert frame.filter(pl.col("total_cost").is_null()).height == sum(
        1 for r in rows("c01") if r["Total Cost"] == ""
    )
    assert any(a != a.strip() for a in frame["agent_applicant"].to_list())
    assert frame.schema["rejection_code"] == pl.String
    assert "5" in frame["rejection_code"].to_list()
    assert {23, 24, 25} <= set(frame["service_duration"].to_list())
    assert frame.schema["service_duration"] == pl.Int64
    assert datetime(2020, 10, 1, 22, 0, tzinfo=UTC) in frame["delivery_start"].to_list()
    assert datetime(2020, 10, 1, 21, 0, tzinfo=UTC) not in frame["delivery_start"].to_list()
    assert frame["unique_bid_number"].to_list() == [r["Unique bid number"] for r in rows("c01")]
    assert (
        frame.schema["availability_fee"] == pl.Float64
        and frame.schema["volume_offered"] == pl.Float64
    )
    assert frame["delivery_date"].to_list()[0] == date(2020, 10, 2)


def test_summary_and_results_keep_raw_endpoints_and_anchor_on_the_efa_date(data: Path) -> None:
    """Detects an EFA date and block composed into a block start (RULINGS 572 / 588) or a naive
    delivery endpoint typed as an instant (DC-RESULT-EXPORT-CLOCK): the summary and by-unit results
    keep ``delivery_start`` / ``delivery_end`` byte-equal as strings (``2021-09-15T23:00:00``),
    ``efa`` is int64, ``timestamp_utc`` and ``event_time`` are the UK-midnight anchor of
    ``efa_date`` (2021-09-15T23:00Z for 2021-09-16, not the block start), the by-unit literal ``NA``
    technology type is kept, and the quoted-empty location and the always-blank ``Cancelled``
    column stay the
    empty string exactly as published (no cancellation state is invented, no token map)."""
    summary = _load_clean(data, "c02")
    assert summary["delivery_start"].to_list()[0] == "2021-09-15T23:00:00"
    assert summary["efa_date"].to_list()[0] == date(2021, 9, 16)
    assert summary["timestamp_utc"].to_list()[0] == datetime(2021, 9, 15, 23, 0, tzinfo=UTC)
    assert summary["efa"].to_list() == [1, 2] and summary.schema["efa"] == pl.Int64
    results = _load_clean(data, "c03")
    assert set(results["technology_type"].to_list()) == {"NA"}
    assert set(results["location"].to_list()) == {""}
    assert set(results["cancelled"].to_list()) == {""}
    assert results["timestamp_utc"].to_list()[0] == datetime(2021, 9, 15, 23, 0, tzinfo=UTC)
    assert results["delivery_end"].to_list()[1] == "2021-09-16T07:00:00"
    assert results.schema["clearing_price"] == pl.Float64


def test_linear_orders_keep_the_published_curve_width_and_raw_endpoints(data: Path) -> None:
    """Detects a truncated or renamed curve, a zero tail nulled, or order times typed: the linear
    record yields 64 curve columns ``p1, v1, p2, v2 ... p32, v32`` in header order (a silver name
    starts with a letter), all float64 with zeros kept, the order id and trade id are equal strings
    that are not cast, ``order_entry_time`` (a bid submission, not an issue instant) and the
    delivery
    endpoints are byte-equal strings, and ``timestamp_utc`` is the capture time."""
    frame = _load_clean(data, "c04")
    curve = [c for c in frame.columns if c[0] in "pv" and c[1:].isdigit()]
    assert curve == [f"{k}{n}" for n in range(1, 33) for k in "pv"] and len(curve) == 64
    assert {frame.schema[c] for c in curve} == {pl.Float64}
    assert all(frame[c].null_count() == 0 for c in curve)
    assert 0.0 in frame["v32"].to_list()
    assert frame["order_period_id"].to_list() == frame["trade_id"].to_list()
    assert frame.schema["order_period_id"] == pl.String
    assert frame["order_entry_time"].to_list() == [r["OrderEntryTime"] for r in rows("c04")]
    assert frame.schema["order_entry_time"] == pl.String
    assert frame["delivery_start"].to_list()[0] == "2021-09-15T23:00:00"


def test_block_orders_keep_the_fractional_mar_minus_one_volumes_and_na_literal(data: Path) -> None:
    """Detects ``MAR`` cast to an integer (526 live cells are fractional), a negative volume made
    absolute or null (negative is the sell convention), the literal ``NA`` block code PRM nulled, or
    a quoted-empty ``Bid Validity`` given a value or a token map: the fractional ``0.45`` MAR loads
    as a float,
    ``-1`` volumes stay ``-1.0``, ``-44`` stays ``-44.0``, ``block_code_prm`` keeps ``NA`` next to
    ``23049``, ``bid_validity`` is the published empty string, prices are float64 and the status
    flags are byte-equal."""
    frame = _load_clean(data, "c05")
    assert 0.45 in frame["mar"].to_list() and frame.schema["mar"] == pl.Float64
    assert -1.0 in frame["volume"].to_list() and -1.0 in frame["executed_volume"].to_list()
    assert -44.0 in frame["volume"].to_list()
    assert frame["volume"].max() < 0.0
    assert {"NA", "23049"} <= set(frame["block_code_prm"].to_list())
    assert set(frame["bid_validity"].to_list()) == {""}
    assert frame.schema["price"] == frame.schema["clearing_price"] == pl.Float64
    assert frame["status"].to_list() == [r["Status"] for r in rows("c05")]
    assert frame["invalid"].to_list() == [r["Invalid"] for r in rows("c05")]


# --------------------------------------------------------------------------- #
# Loud failure, vintage and the ADR-040 ledger
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epochs_fails_loud(data: Path, family: str) -> None:
    """Detects a silent header drift (a renamed vendor column loaded under the old typing): a body
    whose first header cell is renamed fails the capture with no completion and no silver."""
    alias = FIRST_OF[family]
    raw = body(alias)
    first_cell = HEADERS_OF[family][0][0].encode()
    renamed = raw.replace(first_cell, first_cell + b"_x", 1)
    assert renamed != raw
    _fail(data, alias, raw=renamed)


def test_the_legacy_aggregated_header_is_refused_even_as_valid_utf8(data: Path) -> None:
    """Detects the legacy A-H0 header (reordered EBCA / EBVA / ESCA / ESVA / SBVA / SSVA / BPA /
    SPA) being accepted by the nearest epoch: the same names re-encoded as UTF-8 match neither A-H1
    nor A-H2, so the capture fails and nothing loads. A-H0 is not frozen (its real bodies fail
    decoding first)."""
    legacy = ",".join(H_A0) + "\r\n01/04/2013,1,0,0,0,0,0,0,0,0\r\n"
    _fail(data, "a03", raw=legacy.encode("utf-8"))


def test_a_aggregated_body_with_a_standalone_a3_header_byte_fails_as_unicode_decode(
    data: Path,
) -> None:
    """Detects a 2013-14 / 2014-15 style upload (the pound sign as a lone 0xA3 header byte) loaded
    through a lossy re-decode: the A01 cut (header bytes plus two rows, cut in binary) fails the
    capture with ``UnicodeDecodeError`` under the declared UTF-8, writes no completion and loads
    nothing; the failure record stays for the ADR-040 entry."""
    capture_id, cls = _fail(data, "a01")
    assert cls == "UnicodeDecodeError"
    assert read_failure(data, AGG, capture_id) is not None


@pytest.mark.parametrize("alias", ["a07", "f05"])
def test_repeated_settlement_periods_fail_the_capture_and_nothing_is_deduplicated(
    data: Path, alias: str
) -> None:
    """Detects a repeated settlement period loaded silently, deduplicated, resolved to the last row
    or given an occurrence index: the A07 slice (the repeated all-zero 27/06/2020 rows, identical on
    every column) and the F05 slice (25/08/2020 P1 twice with BSA 235 / SSA 170.5 against all zero,
    no revision label) each fail with ``DuplicateEntityKeyError``, load nothing and write no
    completion."""
    _capture_id, cls = _fail(data, alias)
    assert cls == "DuplicateEntityKeyError"


@pytest.mark.parametrize("family", RECORDED)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, family: str) -> None:
    """Detects a duplicate key loaded silently: the first populated data row repeated at the end of
    the body fails the capture with ``DuplicateEntityKeyError``, loads nothing and leaves no
    completion (the held records fail the same way)."""
    alias = FIRST_OF[family]
    lines = body(alias).split(b"\n")
    assert lines[-1] == b""
    raw = b"\n".join([*lines[:-1], lines[1], b""])
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["a03", "f02", "c01"])
def test_an_upload_family_is_stamped_with_the_ckan_last_modified_only_when_upload_only(
    data: Path, alias: str
) -> None:
    """Detects the vintage drifting: ``available_at`` is the CKAN ``last_modified`` read as UTC for
    the upload-only masterdata, and the capture time for a BSAD upload (the BSAD families hold
    datastore resources too, so ``last_modified`` is not an availability). An as-of read before it
    serves nothing and one after serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    written = datetime.fromisoformat(meta.written).astimezone(UTC)
    assert meta.modified is not None
    modified = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    vintage = modified if meta.family in UPLOAD_ONLY else written
    assert set(frame["available_at"].to_list()) == {vintage}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(microseconds=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, meta.family, before) == []
    assert set(both_as_of(db, data, meta.family, after)) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}


@pytest.mark.parametrize("alias", ["a10", "f09", "c02", "c03", "c04", "c05"])
def test_a_datastore_family_is_stamped_with_the_capture_time(data: Path, alias: str) -> None:
    """Detects a datastore family (whose ``last_modified`` is an administrative instant, blank on
    four of these) taking the CKAN vintage: ``available_at`` is the capture's ``written_at`` and an
    as-of read before it serves nothing."""
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


def _entries(family: str) -> list[dict[str, Any]]:
    path = REGISTRY_DIR / registry_module.RECONCILE_ADJUDICATIONS_FILE
    return [e for e in json.loads(path.read_text(encoding="utf-8")) if e["family"] == family]


def test_the_committed_ledger_adjudicates_exactly_the_five_failed_captures() -> None:
    """Detects a failed capture left an open reconcile gap, an entry naming other captures, a wider
    scope, a dropped ruling or an entry the registry does not back: for these two families the
    ledger holds exactly three ``failed`` entries of ruling 748 (the 2013-14 and 2014-15 aggregated
    uploads as ``UnicodeDecodeError``, the 2020-21 aggregated upload as ``DuplicateEntityKeyError``
    and the 2016-17 and 2020-21 forward uploads as ``DuplicateEntityKeyError``), each capture named
    by its resource id, every reason / question / evidence a non-empty single line, the registry
    backing them; and no DC or disaggregated family has an entry."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    expected = {
        (AGG, "UnicodeDecodeError"): {
            CAPTURES["a01"].resource_id,
            "44851379-3eeb-426f-81ea-a4bc34f6edbb",
        },
        (AGG, "DuplicateEntityKeyError"): {CAPTURES["a07"].resource_id},
        (FWD, "DuplicateEntityKeyError"): {
            "30ba12a9-9a9e-4dbb-a69e-ad256d274b04",
            CAPTURES["f05"].resource_id,
        },
    }
    found: dict[tuple[str, str], set[str]] = {}
    for family in BSAD:
        for entry in _entries(family):
            assert (entry["category"], entry["ruling"]) == ("failed", RULING)
            assert entry["evidence"] == "K-AS-4-FACTS §3 / §4 (g3, g6); ADR-040"
            for text in ("reason", "question", "evidence"):
                assert "\n" not in entry[text] and entry[text].strip()
            resources = set()
            for path in entry["captures"]:
                assert path.startswith(f"bronze/neso_data_portal/{family}/2026/10/08/raw_")
                match = registry_module.CAPTURE_ID_PATTERN.fullmatch(path)
                assert match is not None
                resources.add(match["rid"])
            assert len(resources) == len(entry["captures"])
            assert (family, entry["cause"]) not in found
            found[(family, entry["cause"])] = resources
    assert found == expected
    assert sum(len(v) for v in found.values()) == 5
    assert {e.family for e in entries} & {DIS, MASTER, SUM, RES, LIN, BLK} == set()
    for family in BSAD:
        assert [e.family for e in entries].count(family) == (2 if family == AGG else 1)


@pytest.mark.parametrize("key", RECORDED)
def test_the_skeleton_page_renders_the_new_records(key: str) -> None:
    """Detects a record the docs generator cannot render (the two-epoch BSAD records, the held
    DC records, the 78-column linear epoch, the resource-partitioned keys)."""
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
