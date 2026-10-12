"""The trading batch's nine frozen records: two eligible, seven held (v0.22-K-TRD).

``gtma_trades_data`` (resources GU1-GU5; GU6 is a resource-level HOLD) and ``gtma_trades_data_pre``
are eligible. ``bs_contract_enactment_balancing``, ``bs_contract_enactment_balancing_service``,
``gtma_trades_fy``, ``index_linked_contract_volume``, ``ic_requirement_auction``,
``ssel_contract_enactment`` and ``upcoming_trades`` carry a record whose eligibility is **held**.
Every test writes recorded fixture captures (cuts of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/trd/``, provenance in ``PROVENANCE.md``) into a short data root
and runs the transformer the **real package registry** generates, so a record that does not fit its
vendor body fails here, not at activation. On master none of the nine families has a record, so
``get_transformer`` raises for each of them and GU6 is a SILVER resource.

Record decisions under test (K-TRD-FACTS g1-g5 and section 3, K-TRD-SPEC):

- Every ``Last_Updated`` / ``Last Updated`` / ``Published DateTime`` is an **update** column, never
  ``issue_time`` (``issue: none``); vintage is ``capture_fallback`` for the datastore captures and
  ``ckan_last_modified`` for the GTMA uploads (ADR-035).
- Signed values and ``-1`` stay values, zeros stay zero, a blank numeric cell is null, never zero.
  Reversed or equal start / end windows are preserved, never repaired.
- No ``run_type``: these are trades and enactment actions, not settlement accounting runs.
- The legacy balancing-service table keeps its start / end / update cells as raw strings with
  temporal ``none`` (the start is truncated to a date, the other clocks are undocumented).
- ``ssel_contract_enactment`` has two header epochs; legacy ``reduction_mw`` / ``price_mw_hr`` are
  their own columns and are never merged into the modern ``volume`` / ``price``.
- ``gtma_trades_data``'s GU6 repeats ten identifiers as identical full rows: the resource is a
  HOLD and is never deduplicated. GF1 and GF2 share nine identical rows: an ADR-040 overlap
  adjudication (ruling 707).

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the upload
originals' CRLF (a BOM stays); the datastore originals are LF."""

from __future__ import annotations

import csv
import io
import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import install_generated, write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings, run_cli

from gridflow.connectors.neso_data_portal import eligibility as eligibility_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import (
    Eligible,
    Held,
    SilverDisposition,
    load_registry,
)
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "trd"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-TRD"
BOM = b"\xef\xbb\xbf"

COST_Q = (
    "TODO: COST-DIMENSION — the dictionary labels `Cost` £/h but describes it as total cost "
    "(price × volume × duration); which is published?"
)
LEGACY_Q = (
    "TODO: LEGACY-BS-CLOCK — start times truncated to dates, end/update clocks undocumented; "
    "what instants were published?"
)
IC_Q = (
    "TODO: IC-ISSUE-AVAILABILITY — lots are mutable forward requirements later overwritten with "
    "outcomes (28,326 targets on/before the capture, 2 after); what was available when?"
)
UPCOMING_Q = (
    "TODO: UPCOMING-VOLUME-FORMULA — `Volume` is labelled MWh but described as average with "
    "duration in the cost formula; MW or MWh? Also `Last_Updated` clock undocumented."
)
GU6_Q = (
    "TODO: GTMA-2020-DUPLICATES — identifiers ES00104406–ES00104415 each appear twice as "
    "identical full rows; publication defect or distinct trades needing another identifier?"
)
HELD = {
    "bs_contract_enactment_balancing": COST_Q,
    "bs_contract_enactment_balancing_service": LEGACY_Q,
    "gtma_trades_fy": COST_Q,
    "index_linked_contract_volume": COST_Q,
    "ic_requirement_auction": IC_Q,
    "ssel_contract_enactment": COST_Q,
    "upcoming_trades": UPCOMING_Q,
}
ELIGIBLE = ("gtma_trades_data", "gtma_trades_data_pre")
RECORDED = (*ELIGIBLE, *HELD)
GU6_ID = "8e14ae8e-2091-4abc-a2e6-a4b82e5519ab"

PACKAGES = {
    "balancing-services-contract-enactment": "1a03230c-94c6-4816-bb93-e4450f31aa17",
    "historic-gtma-grid-trade-master-agreement-trades-data": "8e511255-6cd7-41c0-868e-8d4d29bced0b",
    "index-linked-contract-volume": "ba632bb7-0bf8-4370-bfee-df3236aff285",
    "interconnector-requirement-and-auction-summary-data": "fdcb1853-4279-4fc9-acb2-d603965eab12",
    "super-stable-export-limit-contract-enactment": "5bfdf87b-a7fc-4be1-9982-606e9b8fa904",
    "upcoming-trades": "932245c1-2322-4515-bf5e-ea2093ba1333",
}
PACKAGE_OF = {
    "bs_contract_enactment_balancing": "balancing-services-contract-enactment",
    "bs_contract_enactment_balancing_service": "balancing-services-contract-enactment",
    "gtma_trades_data": "historic-gtma-grid-trade-master-agreement-trades-data",
    "gtma_trades_data_pre": "historic-gtma-grid-trade-master-agreement-trades-data",
    "gtma_trades_fy": "historic-gtma-grid-trade-master-agreement-trades-data",
    "index_linked_contract_volume": "index-linked-contract-volume",
    "ic_requirement_auction": "interconnector-requirement-and-auction-summary-data",
    "ssel_contract_enactment": "super-stable-export-limit-contract-enactment",
    "upcoming_trades": "upcoming-trades",
}
PACKAGE_FILE_OF = {slug: f"{slug}.json" for slug in PACKAGES}

H_A = [
    "ID",
    "StartTime",
    "EndTime",
    "Volume",
    "Price",
    "Cost",
    "SO_Flag",
    "Reason",
    "AssetID",
    "CP_Name",
    "Area",
    "Note",
    "Last_Updated",
]
H_B = [
    "Record",
    "Counterparty",
    "Unit",
    "Start Time",
    "End Time",
    "Volume",
    "Price",
    "Notes",
    "Last Updated",
]
H_G = ["Trade ID", "Start Time", "End Time", "Volume", "Price", "Cost", "SO Flag"]
H_F = [
    "ID",
    "StartTime",
    "EndTime",
    "Volume",
    "Price",
    "Cost",
    "SO_Flag",
    "Reason",
    "Note",
    "Last_Updated",
]
H_C = [
    "Published DateTime",
    "Notes",
    "Auction ID",
    "Auction Lot ID",
    "Buy Sell",
    "Volume Required",
    "Qualified IC",
    "Start Time",
    "End Time",
    "Bid Deadline",
    "Default Price",
    "Clearing Price",
    "Best Price",
    "VWA Price",
    "Cleared Volume",
    "Total Bid Volume",
    "IFA1 Volume",
    "BN Volume",
    "NEMO Volume",
    "IFA2 Volume",
    "EL Volume",
    "VKL Volume",
    "System Flag",
]
H_S = ["Record", "Unit", "Start", "End", "Reduction (MW)", "Price (£/MW/hr)"]
H_U = ["ID", "Date", "SP", "Volume", "Price", "Cost", "SO_Flag", "Reason", "Note", "Last_Updated"]

NAIVE = "%Y-%m-%dT%H:%M:%S"
ZED = "%Y-%m-%dT%H:%M:%SZ"
# (source, silver name, kind): "id" non-null string; "s" nullable string; "f" nullable float64;
# "t" non-null UTC datetime; "n" nullable UTC datetime; "i" non-null int64; "d" non-null date.
Spec = tuple[str, str, str]
A_COLS: list[Spec] = [
    ("ID", "id", "id"),
    ("StartTime", "starttime", "t"),
    ("EndTime", "endtime", "n"),
    ("Volume", "volume", "f"),
    ("Price", "price", "f"),
    ("Cost", "cost", "f"),
    ("SO_Flag", "so_flag", "s"),
    ("Reason", "reason", "s"),
    ("AssetID", "asset_id", "s"),
    ("CP_Name", "cp_name", "s"),
    ("Area", "area", "s"),
    ("Note", "note", "s"),
    ("Last_Updated", "last_updated", "n"),
]
G_COLS: list[Spec] = [
    ("Trade ID", "trade_id", "id"),
    ("Start Time", "start_time", "t"),
    ("End Time", "end_time", "n"),
    ("Volume", "volume", "f"),
    ("Price", "price", "f"),
    ("Cost", "cost", "f"),
    ("SO Flag", "so_flag", "s"),
]
COLS: dict[str, list[Spec]] = {
    "bs_contract_enactment_balancing": A_COLS,
    "bs_contract_enactment_balancing_service": [
        ("Record", "record", "id"),
        ("Counterparty", "counterparty", "s"),
        ("Unit", "unit", "s"),
        ("Start Time", "start_time", "s"),
        ("End Time", "end_time", "s"),
        ("Volume", "volume", "f"),
        ("Price", "price", "f"),
        ("Notes", "notes", "s"),
        ("Last Updated", "last_updated", "s"),
    ],
    "gtma_trades_data": G_COLS,
    "gtma_trades_data_pre": G_COLS,
    "gtma_trades_fy": [
        ("ID", "id", "id"),
        ("StartTime", "starttime", "t"),
        ("EndTime", "endtime", "n"),
        ("Volume", "volume", "f"),
        ("Price", "price", "f"),
        ("Cost", "cost", "f"),
        ("SO_Flag", "so_flag", "s"),
        ("Reason", "reason", "s"),
        ("Note", "note", "s"),
        ("Last_Updated", "last_updated", "n"),
    ],
    "index_linked_contract_volume": A_COLS,
    "ic_requirement_auction": [
        ("Published DateTime", "published_datetime", "n"),
        ("Notes", "notes", "s"),
        ("Auction ID", "auction_id", "id"),
        ("Auction Lot ID", "auction_lot_id", "id"),
        ("Buy Sell", "buy_sell", "s"),
        ("Volume Required", "volume_required", "f"),
        ("Qualified IC", "qualified_ic", "s"),
        ("Start Time", "start_time", "t"),
        ("End Time", "end_time", "n"),
        ("Bid Deadline", "bid_deadline", "n"),
        ("Default Price", "default_price", "f"),
        ("Clearing Price", "clearing_price", "f"),
        ("Best Price", "best_price", "f"),
        ("VWA Price", "vwa_price", "f"),
        ("Cleared Volume", "cleared_volume", "f"),
        ("Total Bid Volume", "total_bid_volume", "f"),
        ("IFA1 Volume", "ifa1_volume", "f"),
        ("BN Volume", "bn_volume", "f"),
        ("NEMO Volume", "nemo_volume", "f"),
        ("IFA2 Volume", "ifa2_volume", "f"),
        ("EL Volume", "el_volume", "f"),
        ("VKL Volume", "vkl_volume", "f"),
        ("System Flag", "system_flag", "s"),
    ],
    "upcoming_trades": [
        ("ID", "id", "id"),
        ("Date", "date", "d"),
        ("SP", "sp", "i"),
        ("Volume", "volume", "f"),
        ("Price", "price", "f"),
        ("Cost", "cost", "f"),
        ("SO_Flag", "so_flag", "s"),
        ("Reason", "reason", "s"),
        ("Note", "note", "s"),
        ("Last_Updated", "last_updated", "s"),
    ],
}
KEY_OF: dict[str, list[str]] = {
    "bs_contract_enactment_balancing": ["resource_id", "id"],
    "bs_contract_enactment_balancing_service": ["record"],
    "gtma_trades_data": ["resource_id", "trade_id"],
    "gtma_trades_data_pre": ["trade_id"],
    "gtma_trades_fy": ["resource_id", "id"],
    "index_linked_contract_volume": ["resource_id", "id"],
    "ic_requirement_auction": ["auction_lot_id"],
    "ssel_contract_enactment": ["resource_id", "id"],
    "upcoming_trades": ["id", "date", "sp"],
}
PARTITIONED = {
    "bs_contract_enactment_balancing",
    "gtma_trades_data",
    "gtma_trades_fy",
    "index_linked_contract_volume",
    "ssel_contract_enactment",
}
TEMPORAL_COLUMN = {
    "bs_contract_enactment_balancing": "starttime",
    "gtma_trades_data": "start_time",
    "gtma_trades_data_pre": "start_time",
    "gtma_trades_fy": "starttime",
    "index_linked_contract_volume": "starttime",
    "ic_requirement_auction": "start_time",
    "ssel_contract_enactment": "starttime",
}
UPLOADS = {"gu5", "gu6", "gp"}


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    fixture: str
    family: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str
    kind: str


def _cap(
    alias: str, family: str, rid: str, name: str, filename: str, modified: str, written: str
) -> Capture:
    return Capture(
        f"{alias}.csv",
        family,
        rid,
        name,
        filename or rid,
        modified,
        written,
        "upload" if alias in UPLOADS else "datastore",
    )


CAPTURES: dict[str, Capture] = {
    "ba1": _cap(
        "ba1",
        "bs_contract_enactment_balancing",
        "84837a48-8f5d-40d8-bc9f-53dd5e519447",
        "Balancing Contract Enactment FY 21-22",
        "",
        "2022-06-02T08:03:03.868365",
        "2026-10-08T09:01:07.347347+00:00",
    ),
    "ba2": _cap(
        "ba2",
        "bs_contract_enactment_balancing",
        "ed753a4b-1b8c-4b07-9997-b5c0e09da041",
        "Balancing Contract Enactment FY 22-23",
        "",
        "2023-01-24T03:40:19.307106",
        "2026-10-08T09:01:37.339504+00:00",
    ),
    "bl": _cap(
        "bl",
        "bs_contract_enactment_balancing_service",
        "f75a1dcb-de6e-4c20-8407-e37d5e67cf79",
        "Balancing Service Contract Enactment 21-22 (Pre June 2021)",
        "",
        "2023-01-07T19:43:18.676821",
        "2026-10-08T09:03:37.485867+00:00",
    ),
    "gu5": _cap(
        "gu5",
        "gtma_trades_data",
        "6a03c415-475f-4005-bf55-b9108291c511",
        "Historic GTMA Trades Data - Apr 2019 - March 2020",
        "gtma_trades_2019-20.csv",
        "2020-12-09T16:54:00.343328",
        "2026-10-08T11:04:50.274119+00:00",
    ),
    "gu6": _cap(
        "gu6",
        "gtma_trades_data",
        GU6_ID,
        "Historic GTMA Trades Data - Apr 2020 - March 2021",
        "gtma_trades_2020-21.csv",
        "2021-04-06T11:20:05.467319",
        "2026-10-08T11:04:53.582808+00:00",
    ),
    "gp": _cap(
        "gp",
        "gtma_trades_data_pre",
        "8cf964b6-b94c-4d35-88f4-312834aa6e8c",
        "Historic GTMA Trades Data - Apr 2021 - Mar 2022 (Pre June 2021)",
        "gtma_trades_2021-22.csv",
        "2021-06-06T15:43:05.883360",
        "2026-10-08T11:04:56.939778+00:00",
    ),
    "gf1": _cap(
        "gf1",
        "gtma_trades_fy",
        "11a2f532-a7ce-46f8-9d82-88b3289daa82",
        "Historic GTMA Trades FY 21-22",
        "",
        "2022-04-01T08:53:40.828093",
        "2026-10-08T11:04:59.977534+00:00",
    ),
    "gf2": _cap(
        "gf2",
        "gtma_trades_fy",
        "83b1d432-f8b4-4393-a8fd-bcaeb6bd7620",
        "Historic GTMA Trades FY 22-23",
        "",
        "2023-01-23T06:03:04.523943",
        "2026-10-08T11:05:30.890610+00:00",
    ),
    "il": _cap(
        "il",
        "index_linked_contract_volume",
        "8f694d01-68a5-41b9-9542-dd27c42fdbbf",
        "Index Linked Contract Enactment FY 21-22",
        "",
        "2022-04-01T08:24:17.441490",
        "2026-10-08T11:10:20.129708+00:00",
    ),
    "ic": _cap(
        "ic",
        "ic_requirement_auction",
        "6a928369-bed3-445f-af8a-69cdb2cc5089",
        "Interconnector Requirements and Auction Summary",
        "",
        "2023-01-23T22:44:59.745674",
        "2026-10-08T11:08:50.312375+00:00",
    ),
    "se_legacy": _cap(
        "se_legacy",
        "ssel_contract_enactment",
        "835d50b9-2d7c-4318-bd04-c7f94b149041",
        "Super Stable Export Limit Contract Enactment 20-21",
        "",
        "2021-03-31T12:53:32.263801",
        "2026-10-08T11:24:22.987921+00:00",
    ),
    "se_modern": _cap(
        "se_modern",
        "ssel_contract_enactment",
        "34f185bc-087a-41b2-b514-305e31158e5a",
        "Super Stable Export Limit Contract Enactment FY 22-23",
        "",
        "2022-12-31T17:50:14.677118",
        "2026-10-08T11:25:23.100777+00:00",
    ),
    "ut": _cap(
        "ut",
        "upcoming_trades",
        "0b7e84ec-eded-4458-b111-d29ba4508e85",
        "Upcoming Trades",
        "",
        "2026-10-08T11:30:04",
        "2026-10-08T11:40:28.001597+00:00",
    ),
}
FIRST_OF = {
    "bs_contract_enactment_balancing": "ba1",
    "bs_contract_enactment_balancing_service": "bl",
    "gtma_trades_data": "gu5",
    "gtma_trades_data_pre": "gp",
    "gtma_trades_fy": "gf1",
    "index_linked_contract_volume": "il",
    "ic_requirement_auction": "ic",
    "ssel_contract_enactment": "se_legacy",
    "upcoming_trades": "ut",
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
        prefix="trd", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str, *, crlf: bool | None = None) -> bytes:
    """Fixture ``alias`` with the bronze original's line endings (the uploads' CRLF, the
    datastore captures' LF); a BOM stays."""
    raw = (FIXTURES / CAPTURES[alias].fixture).read_bytes()
    bom = BOM if raw.startswith(BOM) else b""
    text = raw[len(bom) :].replace(b"\r\n", b"\n")
    if crlf if crlf is not None else alias in UPLOADS:
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
        url_type=meta.kind,
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


def _fmt(family: str, source: str) -> str:
    return ZED if family in ("gtma_trades_data", "gtma_trades_data_pre") else NAIVE


def _expected(family: str, spec: Spec, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank string cell is null)."""
    source, _name, kind = spec
    if kind == "f":
        return float(cell) if cell != "" else None
    if kind == "i":
        return int(cell)
    if kind == "d":
        return datetime.strptime(cell, "%Y-%m-%d").date()
    if kind in ("t", "n"):
        if cell == "":
            return None
        return datetime.strptime(cell, _fmt(family, source)).replace(tzinfo=UTC)
    return cell if cell != "" else None


def _assert_cells(family: str, frame: pl.DataFrame, source_rows: list[dict[str, str]]) -> None:
    """Every identifier, label, instant and measure of ``frame`` equals the body cell."""
    for spec in COLS[family]:
        if family == "ic_requirement_auction" and spec[1] == "notes":
            # The vendor writes the empty Notes cell quoted (""), which the CSV reader keeps as an
            # empty string, exactly as published; an unquoted blank elsewhere is null.
            assert frame["notes"].to_list() == [""] * len(source_rows)
            continue
        assert frame[spec[1]].to_list() == [
            _expected(family, spec, r[spec[0]]) for r in source_rows
        ], spec[1]


def _load_clean(data: Path, *aliases: str) -> pl.DataFrame:
    """Capture and run the aliases (one family): every row written, none excluded, the generic
    output columns, one completion per capture."""
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
    return frame


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header, the
    BOM on exactly the three uploads, CRLF rebuilt for them, a `-1` price / cost / volume, a zero
    price and an equal and a reversed window in the GTMA upload cut, the identical duplicated
    ES00104406 pair in the GU6 cut, the nine-id GF1/GF2 overlap shape, the legacy balancing row
    whose end precedes its start, the auction cut's blank price / `null` System Flag / future-start
    lot, both SSEL header epochs, and one upcoming id across two periods."""
    headers = {
        "ba1": H_A,
        "ba2": H_A,
        "bl": H_B,
        "gu5": H_G,
        "gu6": H_G,
        "gp": H_G,
        "gf1": H_F,
        "gf2": H_F,
        "il": H_A,
        "ic": H_C,
        "se_legacy": H_S,
        "se_modern": H_A,
        "ut": H_U,
    }
    assert set(headers) == set(CAPTURES)
    for alias, expected in headers.items():
        assert header(alias) == expected, alias
        raw = (FIXTURES / CAPTURES[alias].fixture).read_bytes()
        assert raw.startswith(BOM) == (alias in UPLOADS), alias
        assert (b"\r\n" in body(alias)) == (alias in UPLOADS), alias
    gu5 = rows("gu5")
    assert any(r["Price"] == "-1" for r in gu5) and any(r["Cost"] == "-1" for r in gu5)
    assert any(r["Volume"] == "-1" for r in gu5) and any(r["Price"] == "0" for r in gu5)
    assert any(r["End Time"] < r["Start Time"] for r in gu5)
    assert any(r["End Time"] == r["Start Time"] for r in gu5)
    gu6 = rows("gu6")
    assert [r["Trade ID"] for r in gu6].count("ES00104406") == 2
    assert gu6[1] == gu6[2]
    shared = {r["ID"] for r in rows("gf1")} & {r["ID"] for r in rows("gf2")}
    assert shared == {"ES00165376", "ES00165377"}
    left = {r["ID"]: r for r in rows("gf1")}
    assert all(left[r["ID"]] == r for r in rows("gf2") if r["ID"] in shared)
    legacy = {r["Record"]: r for r in rows("bl")}
    assert legacy["2021_04_07-1"]["End Time"] < legacy["2021_04_07-1"]["Start Time"]
    assert all(len(r["Start Time"]) == 10 for r in legacy.values())
    ic = rows("ic")
    assert any(r["Clearing Price"] == "" for r in ic) and any(
        r["System Flag"] == "null" for r in ic
    )
    assert max(r["Start Time"] for r in ic) > "2026-10-08T11:08:50"
    assert any(r["Reduction (MW)"] for r in rows("se_legacy")) and "Volume" in header("se_modern")
    ut = rows("ut")
    assert len({r["ID"] for r in ut}) < len(ut)


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_selection_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time, a run, a sibling, a trim or an epoch other than
    the vendor header(s): every record is version 1, utf-8 CSV, whole-capture selection (partitioned
    by resource only where the spec says so), ``capture_fallback`` vintage for the datastore
    families and ``ckan_last_modified`` for the GTMA uploads, the spec's entity key, the vendor
    header(s) as its epoch(s) and ``issue: none`` (an update column is never an issue time)."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    expected_vintage = (
        "ckan_last_modified"
        if key in ("gtma_trades_data", "gtma_trades_data_pre")
        else "capture_fallback"
    )
    assert record.vintage == expected_vintage
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key in PARTITIONED else None)
    assert list(record.entity_key) == KEY_OF[key]
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None
    for epoch in record.epochs:
        assert epoch.issue.kind == "none"
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)
    headers = [list(e.header) for e in record.epochs]
    if key == "ssel_contract_enactment":
        assert headers == [H_S, H_A]
    else:
        assert len(headers) == 1
        assert headers[0] == header(FIRST_OF[key])


@pytest.mark.parametrize("key", [k for k in RECORDED if k != "ssel_contract_enactment"])
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded or relabelled column: identifiers are non-null
    strings (the upcoming period an int64 and its day a date), every measure is a nullable float64
    (a ``-1`` is never a null token), delivery clocks are UTC datetimes in the body's format (the
    GTMA uploads' literal ``Z``), the legacy balancing start / end / update cells and the upcoming
    ``Last_Updated`` stay raw strings, and no column declares ``trim``, ``null_tokens``,
    ``formats_by_filename``, ``min`` or ``max`` (except the settlement period, bounded 1..50: DST
    days run 46 to 50 periods)."""
    columns = _record(key).epochs[0].columns
    assert [c.source for c in columns] == [s[0] for s in COLS[key]]
    assert [c.name for c in columns] == [s[1] for s in COLS[key]]
    for column, (_source, _name, kind) in zip(columns, COLS[key], strict=True):
        assert column.trim is None and column.null_tokens == ()
        assert (column.min, column.max) == ((1, 50) if column.name == "sp" else (None, None))
        assert column.formats_by_filename is None
        if kind == "id":
            assert (column.dtype, column.nullable) == ("string", False), column.name
        elif kind == "s":
            assert (column.dtype, column.nullable, column.format) == ("string", True, None)
        elif kind == "f":
            assert (column.dtype, column.nullable) == ("float64", True), column.name
        elif kind in ("t", "n"):
            assert (column.dtype, column.format, column.zone) == ("datetime", _fmt(key, ""), "UTC")
            assert column.nullable == (kind == "n"), column.name
        elif kind == "i":
            assert (column.dtype, column.nullable) == ("int64", False)
        else:
            assert (column.dtype, column.format, column.nullable) == ("date", "%Y-%m-%d", False)


def test_ssel_has_two_epochs_with_legacy_and_modern_value_columns_kept_apart() -> None:
    """Detects the legacy reduction / rate merged into the modern volume / price, a legacy
    ``Record`` typed as a number, or a legacy / modern start mapped to different outputs: the two
    epochs map ``Start`` / ``StartTime`` to ``starttime`` and ``End`` / ``EndTime`` to ``endtime``
    (UTC), ``Record`` and ``ID`` both to the string ``id``, the legacy epoch owns
    ``reduction_mw`` and ``price_mw_hr`` (float64) and the modern epoch owns ``volume`` /
    ``price`` /
    ``cost``."""
    legacy, modern = _record("ssel_contract_enactment").epochs
    by_source = {c.source: c for c in legacy.columns}
    assert [(c.source, c.name) for c in legacy.columns] == [
        ("Record", "id"),
        ("Unit", "unit"),
        ("Start", "starttime"),
        ("End", "endtime"),
        ("Reduction (MW)", "reduction_mw"),
        ("Price (£/MW/hr)", "price_mw_hr"),
    ]
    assert (by_source["Record"].dtype, by_source["Record"].nullable) == ("string", False)
    for column in (by_source["Start"], by_source["End"]):
        assert (column.dtype, column.format, column.zone) == ("datetime", NAIVE, "UTC")
    for source in ("Reduction (MW)", "Price (£/MW/hr)"):
        assert (by_source[source].dtype, by_source[source].nullable) == ("float64", True)
    assert [(c.source, c.name) for c in modern.columns] == [(s[0], s[1]) for s in A_COLS]
    assert {c.name for c in legacy.columns}.isdisjoint({"volume", "price", "cost"})
    assert {c.name for c in modern.columns}.isdisjoint({"reduction_mw", "price_mw_hr"})
    assert _record("ssel_contract_enactment").temporal.column == "starttime"


@pytest.mark.parametrize(
    ("key", "temporal"),
    [(k, c) for k, c in TEMPORAL_COLUMN.items()],
)
def test_the_temporal_recipe_is_the_utc_start_instant(key: str, temporal: str) -> None:
    """Detects a delivery window anchored on its end, an update column or a capture time: the seven
    window families use ``utc_instant`` on their start column."""
    recipe = _record(key).temporal
    assert (recipe.kind, recipe.column) == ("utc_instant", temporal)


def test_the_legacy_balancing_table_has_no_temporal_recipe_and_raw_clocks() -> None:
    """Detects a ``date_sp1`` anchor on the truncated legacy start (it would fabricate a start
    instant) or a parsed end / update clock whose zone NESO does not document."""
    record = _record("bs_contract_enactment_balancing_service")
    assert record.temporal.kind == "none"
    clocks = {
        c.name: c.dtype for c in record.epochs[0].columns if "time" in c.name or "updated" in c.name
    }
    assert clocks == {"start_time": "string", "end_time": "string", "last_updated": "string"}


def test_upcoming_trades_uses_the_settlement_pair_recipe_and_keeps_empty_allowed() -> None:
    """Detects a UTC date + period composition or a lost empty-capture allowance: the recipe is the
    existing ``sp_pair`` on the body's day and period, the key carries both of them with the trade
    id, and the resource still accepts a marked empty capture."""
    record = _record("upcoming_trades")
    assert (record.temporal.kind, record.temporal.date_column, record.temporal.period_column) == (
        "sp_pair",
        "date",
        "sp",
    )
    family = load_registry().families["upcoming_trades"][1]
    assert family.empty_allowed is True


@pytest.mark.parametrize("key", list(HELD))
def test_seven_records_are_held_with_the_verbatim_question_and_the_package_stays_eligible(
    key: str,
) -> None:
    """Detects a hold lost, reworded or moved to the package: each of the seven records carries its
    verbatim question and this unit, is effectively held, and its package stays eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == (UNIT, HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_two_gtma_upload_records_are_eligible(key: str) -> None:
    """Detects a hold on the clean GTMA families: both uploads carry no eligibility override and
    are effectively eligible."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_gu6_is_a_held_resource_and_the_other_gtma_resources_stay_silver() -> None:
    """Detects GU6 still routed to silver (its ten repeated identifiers would fail the duplicate
    guard or be deduplicated) or another resource changed: GU6 is a HOLD with the verbatim
    question and this unit, GU1-GU5 and GP1 are SILVER of their families, the three GF and the
    non-CSV resources are as before (the two ZIPs HOLD, the PDF DOC)."""
    registry = load_registry()
    held = registry.resources[GU6_ID][1]
    assert held.family == "gtma_trades_data"
    assert held.disposition.model_dump() == {"kind": "HOLD", "reason": GU6_Q, "unit": UNIT}
    gtma = [
        r
        for _p, r in registry.resources.values()
        if r.family in ("gtma_trades_data", "gtma_trades_data_pre")
    ]
    assert len(gtma) == 7
    for resource in gtma:
        if resource.id != GU6_ID:
            assert resource.disposition == SilverDisposition(kind="SILVER", key=resource.family)
    assert len([r for r in gtma if r.family == "gtma_trades_data"]) == 6
    fy = [r for _p, r in registry.resources.values() if r.family == "gtma_trades_fy"]
    assert len(fy) == 6
    assert all(r.disposition == SilverDisposition(kind="SILVER", key=r.family) for r in fy)
    for family_key, kind in (
        ("bs_contract_enactment_files", "HOLD"),
        ("index_linked_contract_volume_files", "HOLD"),
        ("ic_requirement_auction_files", "DOC"),
    ):
        files = [r for _p, r in registry.resources.values() if r.family == family_key]
        assert files and all(r.disposition.kind == kind for r in files), family_key


def test_the_package_files_carry_exactly_the_nine_records() -> None:
    """Detects a record on the wrong family or a family gaining a transformer: the six package
    files hold exactly the nine records between them and no family in them has a bespoke
    transformer."""
    found: set[str] = set()
    for slug in PACKAGES:
        document = _package_doc(slug)
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), slug
    assert found == set(RECORDED)


def test_all_nine_are_reported_as_silver_outputs_seven_held() -> None:
    """Detects a family reported as ingest-only or the wrong eligibility: the eligibility report
    lists each of the nine as ``silver``, two ``eligible`` and seven ``held: TODO:`` with this
    unit, all ``whole_capture``."""
    report = eligibility_module.render(load_registry())
    for key in RECORDED:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver |" in line, key
        if key in HELD:
            assert "| silver | held: TODO:" in line and f"(unit {UNIT})" in line, key
        else:
            assert "| silver | eligible |" in line, key
        assert "| whole_capture" in line, key


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ["ba1", "ba2", "bl", "gu5", "gp", "gf1", "gf2", "il", "ic", "ut"])
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a BOM
    reaching the first silver column name, a duplicate key or an excluded row: the slice loads every
    row with zero exclusions and no failure, every identifier, label, instant and measure equals
    the body cell, and the entity key is unique."""
    family = CAPTURES[alias].family
    frame = _load_clean(data, alias)
    _assert_cells(family, frame, rows(alias))
    key = [c for c in KEY_OF[family]]
    assert not frame.select(key).is_duplicated().any()


@pytest.mark.parametrize(
    ("family", "aliases"),
    [
        ("bs_contract_enactment_balancing", ("ba1", "ba2")),
        ("gtma_trades_fy", ("gf1", "gf2")),
    ],
)
def test_two_resources_of_one_family_load_side_by_side_in_file_order(
    data: Path, family: str, aliases: tuple[str, str]
) -> None:
    """Detects a resource dropped or merged: both resources load all their rows with zero
    exclusions, every cell equal to its body, each row tagged with its own ``resource_id``."""
    frame = _load_clean(data, *aliases)
    expected: list[dict[str, str]] = []
    for alias in aliases:
        expected += rows(alias)
    assert sorted(frame["resource_id"].unique().to_list()) == sorted(
        CAPTURES[a].resource_id for a in aliases
    )
    for spec in COLS[family]:
        got = sorted(frame[spec[1]].to_list(), key=repr)
        want = sorted((_expected(family, spec, r[spec[0]]) for r in expected), key=repr)
        assert got == want, spec[1]


def test_instants_are_utc_tz_aware_and_the_temporal_column_is_the_start(data: Path) -> None:
    """Detects a naive or shifted instant: the GTMA upload start / end parse the literal ``Z`` as
    UTC, every datetime column is UTC time-zone aware, and ``timestamp_utc`` equals the start."""
    frame = _load_clean(data, "gu5")
    for name in ("start_time", "end_time", "timestamp_utc", "event_time"):
        assert frame.schema[name] == pl.Datetime("us", "UTC"), name
    assert frame["timestamp_utc"].to_list() == frame["start_time"].to_list()
    assert frame["start_time"][0] == datetime(2019, 4, 1, 0, 0, tzinfo=UTC)


def test_gtma_keeps_minus_one_zero_and_reversed_or_equal_windows(data: Path) -> None:
    """Detects a ``-1`` read as missing (FACTS g5), a zero price read as null, or a reversed /
    equal window repaired or dropped: the ``-1`` volume, price and cost cells and the zero price /
    cost are values, the window whose end precedes its start is loaded as published and so is the
    equal one."""
    frame = _load_clean(data, "gu5")
    assert frame.filter(pl.col("volume") == -1.0).height == 1
    assert frame.filter(pl.col("price") == -1.0).height == 1
    assert frame.filter(pl.col("cost") == -1.0).height == 1
    zero = frame.filter(pl.col("price") == 0.0)
    assert zero.height == 1 and zero["cost"].to_list() == [0.0]
    for name in ("volume", "price", "cost"):
        assert frame[name].null_count() == 0
    reversed_rows = frame.filter(pl.col("end_time") < pl.col("start_time"))
    equal_rows = frame.filter(pl.col("end_time") == pl.col("start_time"))
    assert reversed_rows["trade_id"].to_list() == ["ES00049438"]
    assert equal_rows["trade_id"].to_list() == ["ES00052927"]
    assert reversed_rows["end_time"][0] == datetime(2020, 2, 23, 8, 0, tzinfo=UTC)


def test_gtma_pre_keeps_minus_one_and_the_zero_price(data: Path) -> None:
    """Detects the same `-1` and zero handling lost in the pre-June upload family."""
    frame = _load_clean(data, "gp")
    assert frame.filter(pl.col("price") == -1.0).height == 2
    assert frame.filter(pl.col("price") == 0.0).height == 1
    assert frame["price"].null_count() == 0


def test_modern_balancing_zero_rows_stay_zero_and_signed_values_stay_signed(data: Path) -> None:
    """Detects a zero volume / price / cost read as missing (a long zero run is a value: services
    without physical power exchange) or a negative volume made positive."""
    frame = _load_clean(data, "ba1", "ba2")
    ba1 = frame.filter(pl.col("resource_id") == CAPTURES["ba1"].resource_id)
    ba2 = frame.filter(pl.col("resource_id") == CAPTURES["ba2"].resource_id)
    assert ba1["volume"].to_list() == [0.0, 0.0, 0.0] == ba1["price"].to_list()
    assert ba1["cost"].to_list() == [0.0, 0.0, 0.0]
    assert ba1["reason"].null_count() == 1
    assert ba2["volume"].to_list() == [float(r["Volume"]) for r in rows("ba2")]
    assert ba2["volume"].to_list()[-2:] == [-4.9, -10.8]
    assert ba2["price"].to_list()[-2:] == [124.0, 124.0]


def test_legacy_balancing_keeps_the_three_clock_cells_as_raw_strings(data: Path) -> None:
    """Detects a parsed legacy clock (the start is a date, the end / update zone is undocumented),
    a date-anchored start instant, or the end-before-start row repaired: ``start_time``,
    ``end_time`` and ``last_updated`` are strings byte-equal to the body, ``2021_04_07-1`` keeps its
    ``2020-04-08T05:00:00`` end, and ``timestamp_utc`` is the capture time (no instant is
    fabricated)."""
    frame = _load_clean(data, "bl")
    source = rows("bl")
    for column, cell in (
        ("start_time", "Start Time"),
        ("end_time", "End Time"),
        ("last_updated", "Last Updated"),
    ):
        assert frame.schema[column] == pl.String
        assert frame[column].to_list() == [r[cell] for r in source]
    reversed_row = frame.filter(pl.col("record") == "2021_04_07-1")
    assert reversed_row["start_time"].to_list() == ["2021-04-07"]
    assert reversed_row["end_time"].to_list() == ["2020-04-08T05:00:00"]
    written = datetime.fromisoformat(CAPTURES["bl"].written)
    assert set(frame["timestamp_utc"].to_list()) == {written}
    assert 150.0 in frame["price"].to_list()


def test_auction_keeps_blank_prices_null_the_null_flag_raw_and_the_future_lot(data: Path) -> None:
    """Detects a blank clearing price read as zero (pending result, never zero), the literal
    ``null`` System Flag cleared or mapped to a null token, a reversed window repaired, or the two
    lots whose delivery starts after the capture dropped: the blank price / volume cells are null,
    ``system_flag`` equals the body cell (``null`` stays the string ``null``, a blank flag is null),
    the future-start lot loads and a zero clearing price stays zero."""
    frame = _load_clean(data, "ic")
    blank = frame.filter(pl.col("auction_lot_id") == "2021_04_01-03_47-1")
    assert blank["clearing_price"].to_list() == [None]
    assert blank["best_price"].to_list() == [None] and blank["vwa_price"].to_list() == [None]
    assert blank["default_price"].to_list() == [69.0]
    flags = dict(
        zip(frame["auction_lot_id"].to_list(), frame["system_flag"].to_list(), strict=True)
    )
    assert flags["2026_05_23-16_43-1"] == "null"
    assert flags["2020_12_16-15_57-1"] is None
    zero = frame.filter(pl.col("auction_lot_id") == "2026_05_23-16_43-1")
    assert zero["clearing_price"].to_list() == [0.0] and zero["cleared_volume"].to_list() == [0.0]
    assert zero["total_bid_volume"].to_list() == [1143.0]
    future = frame.filter(pl.col("auction_lot_id") == "2026_10_08-11_43-1")
    assert future["start_time"].to_list() == [datetime(2026, 10, 8, 13, 0, tzinfo=UTC)]
    assert future["clearing_price"].null_count() == 1
    backwards = frame.filter(pl.col("auction_lot_id") == "2026_09_07-19_42-2")
    assert backwards["end_time"][0] < backwards["start_time"][0]
    assert frame["published_datetime"].null_count() == 0


def test_ssel_loads_both_epochs_and_the_value_columns_are_populated_per_epoch(
    data: Path,
) -> None:
    """Detects the legacy reduction / rate merged into the modern value columns, a legacy numeric
    ``Record`` typed as a number, or a modern column filled on a legacy row: across the legacy and
    modern captures ``reduction_mw`` / ``price_mw_hr`` are populated only on legacy rows,
    ``volume`` / ``price`` / ``cost`` only on modern rows, ``id`` is a string for both ("1" and
    "ES00..."), every instant is UTC and the key is unique."""
    frame = _load_clean(data, "se_legacy", "se_modern")
    legacy_id = CAPTURES["se_legacy"].resource_id
    legacy = frame.filter(pl.col("resource_id") == legacy_id)
    modern = frame.filter(pl.col("resource_id") != legacy_id)
    assert legacy.height == 3 and modern.height == 2
    assert frame.schema["id"] == pl.String
    assert legacy["id"].to_list() == [r["Record"] for r in rows("se_legacy")]
    assert modern["id"].to_list() == [r["ID"] for r in rows("se_modern")]
    assert legacy["reduction_mw"].to_list() == [
        float(r["Reduction (MW)"]) for r in rows("se_legacy")
    ]
    assert legacy["price_mw_hr"].to_list() == [
        float(r["Price (£/MW/hr)"]) for r in rows("se_legacy")
    ]
    for name in ("volume", "price", "cost", "so_flag", "last_updated"):
        assert legacy[name].null_count() == legacy.height, name
    for name in ("reduction_mw", "price_mw_hr"):
        assert modern[name].null_count() == modern.height, name
    assert modern["volume"].to_list() == [-100.0, -100.0]
    assert modern["cost"].to_list() == [-46400.0, -46400.0]
    for name in ("starttime", "endtime", "timestamp_utc"):
        assert frame.schema[name] == pl.Datetime("us", "UTC"), name
    assert legacy["starttime"][0] == datetime(2020, 7, 4, 22, 0, tzinfo=UTC)
    assert not frame.select("resource_id", "id").is_duplicated().any()


def test_upcoming_trades_uses_the_settlement_period_and_keeps_the_raw_update_string(
    data: Path,
) -> None:
    """Detects one id collapsed across its periods, the delivery instant taken from the date alone
    or a UTC composition, and a parsed ``Last_Updated``: ES00380892 appears for periods 29 and 30
    and both rows load, ``timestamp_utc`` is the UK settlement-period start (period 29 of
    2026-10-08 is 13:00 UTC under BST), every delivery is after the capture vintage, and
    ``last_updated`` is the body string."""
    frame = _load_clean(data, "ut")
    pair = frame.filter(pl.col("id") == "ES00380892")
    assert sorted(pair["sp"].to_list()) == [29, 30]
    by_sp = dict(zip(frame["sp"].to_list(), frame["timestamp_utc"].to_list(), strict=True))
    assert by_sp[29] == datetime(2026, 10, 8, 13, 0, tzinfo=UTC)
    assert by_sp[30] == datetime(2026, 10, 8, 13, 30, tzinfo=UTC)
    vintage = datetime.fromisoformat(CAPTURES["ut"].written)
    assert all(t > vintage for t in frame["timestamp_utc"].to_list())
    assert frame.schema["last_updated"] == pl.String
    assert frame["last_updated"].to_list() == [r["Last_Updated"] for r in rows("ut")]
    assert frame.schema["sp"] == pl.Int64 and frame.schema["date"] == pl.Date
    assert frame["note"].null_count() == frame.height


# --------------------------------------------------------------------------- #
# GU6 is held; GF1 / GF2 overlap
# --------------------------------------------------------------------------- #


def test_gu6_feeds_no_silver_and_is_never_deduplicated(data: Path) -> None:
    """Detects GU6 loaded (its repeated ES00104406 would fail the duplicate guard, or be silently
    deduplicated): capturing GU6 alone and running the family writes no silver and no completion,
    and capturing it beside GU5 loads GU5's rows only, the shared transformer neither failing nor
    emitting a row of GU6's resource."""
    gu6_capture = capture(data, "gu6")
    get_transformer(SOURCE, "gtma_trades_data", data).run(DAY, run_id="r")
    assert _no_silver(data, "gtma_trades_data")
    assert read_completion(data, "gtma_trades_data", gu6_capture) is None
    assert read_failure(data, "gtma_trades_data", gu6_capture) is None
    gu5_capture = capture(data, "gu5")
    written = get_transformer(SOURCE, "gtma_trades_data", data).run(DAY, run_id="r2")
    assert written == len(rows("gu5"))
    frame = _silver(data, "gtma_trades_data")
    assert set(frame["resource_id"].to_list()) == {CAPTURES["gu5"].resource_id}
    assert "ES00104406" not in frame["trade_id"].to_list()
    assert read_completion(data, "gtma_trades_data", gu5_capture) is not None
    assert read_completion(data, "gtma_trades_data", gu6_capture) is None


def test_an_eligible_gtma_resource_with_a_repeated_trade_id_still_fails_loud(data: Path) -> None:
    """Detects the duplicate guard silenced on the family: the GU6 duplicate rows served as a
    SILVER resource's body (GU5 here) fail the capture with ``DuplicateEntityKeyError``, load
    nothing and leave no completion, which is why GU6 is held rather than deduplicated."""
    raw = body("gu6")
    assert raw.count(b"ES00104406") == 2
    capture_id = capture(data, "gu5", raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, "gtma_trades_data", data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, "gtma_trades_data", capture_id) is None
    assert _no_silver(data, "gtma_trades_data")


def test_the_gtma_family_reconciles_clean_with_gu6_held(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a held capture reported as a reconcile gap (missing / failed / orphaned): GU5
    transformed and GU6 captured but held reconcile with no gap and no ledger entry."""
    install_generated(
        monkeypatch, data / "_registry", [_package_doc(PACKAGE_OF["gtma_trades_data"])]
    )
    capture(data, "gu5")
    capture(data, "gu6")
    get_transformer(SOURCE, "gtma_trades_data", data).run(DAY, run_id="r")
    code, lines = run_cli("gtma_trades_data", "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert [ln for ln in lines if ln.startswith("GAP")] == []


def _entries(family: str) -> list[dict[str, Any]]:
    path = REGISTRY_DIR / registry_module.RECONCILE_ADJUDICATIONS_FILE
    entries = json.loads(path.read_text(encoding="utf-8"))
    return [e for e in entries if e["family"] == family]


def _tree_bytes(data: Path, top: str) -> dict[str, bytes]:
    return {
        p.relative_to(data / top).as_posix(): p.read_bytes()
        for p in sorted((data / top).rglob("*"))
        if p.is_file()
    }


def test_the_committed_ledger_adjudicates_exactly_the_gf1_gf2_overlap() -> None:
    """Detects the overlap left an open gap (reconcile red forever), an entry that names other
    captures, a wider scope, a dropped ruling, or an entry the registry does not back: one
    ``overlap`` entry (no cause) of ruling 707 for the GF1 (FY 21-22) and GF2 (FY 22-23) captures
    only, a one-line reason naming the nine ids, evidence naming the FACTS and ADR-040, once in the
    ledger; the registry backs it; and no other TRD family has an entry."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    (entry,) = _entries("gtma_trades_fy")
    assert (entry["category"], entry["ruling"]) == ("overlap", "707")
    assert entry.get("cause") is None
    resources = set()
    for path in entry["captures"]:
        assert path.startswith("bronze/neso_data_portal/gtma_trades_fy/2026/10/08/raw_")
        match = registry_module.CAPTURE_ID_PATTERN.fullmatch(path)
        assert match is not None
        resources.add(match["rid"])
    assert len(entry["captures"]) == 2
    assert resources == {CAPTURES["gf1"].resource_id, CAPTURES["gf2"].resource_id}
    for text in ("reason", "question", "evidence"):
        assert "\n" not in entry[text] and entry[text].strip()
    assert "ES00165376" in entry["reason"] and "ES00165384" in entry["reason"]
    assert "nine" in entry["reason"] and "ES00165376" in entry["question"]
    assert "K-TRD-FACTS" in entry["evidence"] and "ADR-040" in entry["evidence"]
    assert [e.family for e in entries].count("gtma_trades_fy") == 1
    assert {e.family for e in entries} & set(RECORDED) == {"gtma_trades_fy"}


def test_the_overlap_is_served_from_both_resources_and_adjudicated_not_open(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects an overlap silently deduplicated (GF1 dropped, a precedence inferred), the gap left
    open, or an adjudication that alters data: both captures load every row (the two shared ids
    twice, with equal values), reconcile without the ledger reports one ``overlap`` gap per capture,
    with the committed entry (capture ids swapped for the fixture captures') every gap is
    adjudicated, none is open or stale, the line names ruling 707 and the silver and state bytes are
    equal before and after."""
    install_generated(monkeypatch, data / "_registry", [_package_doc(PACKAGE_OF["gtma_trades_fy"])])
    ids = {alias: capture(data, alias) for alias in ("gf1", "gf2")}
    get_transformer(SOURCE, "gtma_trades_fy", data).run(DAY, run_id="r")
    frame = _silver(data, "gtma_trades_fy")
    assert frame.height == len(rows("gf1")) + len(rows("gf2"))
    shared = frame.filter(pl.col("id").is_in(["ES00165376", "ES00165377"]))
    assert shared.height == 4
    values = shared.group_by("id").agg(
        pl.col("volume").n_unique().alias("v"), pl.col("last_updated").n_unique().alias("u")
    )
    assert values["v"].to_list() == [1, 1] and values["u"].to_list() == [1, 1]
    before = (_tree_bytes(data, "silver"), _tree_bytes(data, "state"))

    code, lines = run_cli("gtma_trades_fy", "--cutoff", DAY.isoformat())
    assert code == 1, lines
    gaps = [line for line in lines if line.startswith("GAP overlap")]
    assert len(gaps) == 2
    for capture_id in ids.values():
        assert any(capture_id in line for line in gaps), capture_id

    (template,) = _entries("gtma_trades_fy")
    entry = {**template, "captures": [ids["gf1"], ids["gf2"]]}
    (data / "_registry" / registry_module.RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json([entry]), encoding="utf-8"
    )
    code, lines = run_cli("gtma_trades_fy", "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert [line for line in lines if line.startswith("GAP")] == []
    assert "SUMMARY adjudicated 2" in lines and "SUMMARY stale_adjudication 0" in lines
    adjudicated = [line for line in lines if line.startswith("ADJUDICATED overlap")]
    assert len(adjudicated) == 2 and all("ruling 707" in line for line in adjudicated)
    assert (_tree_bytes(data, "silver"), _tree_bytes(data, "state")) == before


# --------------------------------------------------------------------------- #
# Loud failure, vintage and catalogue
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epoch_fails_loud(data: Path, family: str) -> None:
    """Detects a silent header drift (a renamed vendor column loaded under the old typing): a body
    whose first header cell is renamed fails the capture with no completion and no silver."""
    alias = FIRST_OF[family]
    raw = body(alias)
    bom = BOM if raw.startswith(BOM) else b""
    first, newline, rest = raw[len(bom) :].partition(b"\n")
    renamed = bom + first.replace(b",", b"_x,", 1) + newline + rest
    capture_id = capture(data, alias, raw=renamed)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize(
    "family", [k for k in RECORDED if k not in ("gtma_trades_fy", "gtma_trades_data")]
)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, family: str) -> None:
    """Detects a duplicate key loaded silently: the last data row repeated fails the capture with
    ``DuplicateEntityKeyError``, loads nothing and leaves no completion."""
    alias = FIRST_OF[family]
    lines = body(alias).decode("utf-8").splitlines()
    newline = "\r\n" if alias in UPLOADS else "\n"
    raw = (newline.join([*lines, lines[-1]]) + newline).encode("utf-8")
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["ba1", "gu5", "ut"])
def test_the_vintage_is_the_capture_time_or_the_ckan_last_modified_and_as_of_is_bounded(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy from an update column or a catalogue view that cannot carry the
    new columns: ``available_at`` is the sidecar ``written_at`` for the datastore captures and the
    CKAN ``last_modified`` for the GTMA upload; an as-of read before the vintage serves nothing
    and one after serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    if meta.kind == "upload":
        vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    else:
        vintage = datetime.fromisoformat(meta.written)
    assert set(frame["available_at"].to_list()) == {vintage}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(microseconds=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, meta.family, before) == []
    assert set(both_as_of(db, data, meta.family, after)) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}


def test_the_per_resource_selection_serves_both_resources(data: Path) -> None:
    """Detects a whole-capture selection that serves only the newest resource of a partitioned
    family: with the two modern balancing resources both ids are served, and the one resource of an
    unpartitioned family (the legacy table) is served on its own."""
    ids = {a: capture(data, a) for a in ("ba1", "ba2")}
    get_transformer(SOURCE, "bs_contract_enactment_balancing", data).run(DAY, run_id="r")
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    served = set(both_as_of(db, data, "bs_contract_enactment_balancing", None))
    assert served == set(ids.values())


def test_every_family_is_generic_tabular_silver_with_its_own_record() -> None:
    """Detects a family wired to a bespoke transformer instead of the generic engine: every one of
    the nine resolves to a generic transformer reading the family's own record."""
    registry = load_registry()
    for key in RECORDED:
        package, entry = registry.families[key]
        assert entry.record is not None and entry.transformer is None, key
        assert entry.kind == "tabular", key
        assert not isinstance(package.eligibility, Held), key


@pytest.mark.parametrize("slug", list(PACKAGES))
def test_the_skeleton_pages_render_the_new_records(slug: str) -> None:
    """Detects a record the docs generator cannot render (the held questions, the pound-sign
    headers, the two SSEL epochs)."""
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
