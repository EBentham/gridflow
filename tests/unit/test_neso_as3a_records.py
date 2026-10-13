"""The EAC live auction results: eleven frozen records, all eligible, none held (v0.22-K-AS-3a).

The package ``eac-auction-results`` carries eleven families: by-unit results, buy orders, results
summary and sell orders, each as the live (datastore) table, the financial-year archives and (except
the sell archives) a daily table. Every test writes recorded fixture captures (cuts of the
2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/as3a/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation. On master
none of the eleven families has a record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-AS-3-FACTS g1-g6 and section 3, K-AS-3a-SPEC):

- ``issue: none`` everywhere. ``orderEntryTime`` is the bid **submission** instant, not the
  availability of the matched volume and clearing price. The package declares its datetimes UTC
  (the only evidence for ``zone: "UTC"``, no ``zone_evidence``). Endpoint closure is undocumented
  (TODO: INTERVAL-CLOSURE) and the endpoints are kept as given.
- Opaque vendor identifiers (``auctionID``, ``orderID``, ``unitResultID``, ``basketID``,
  ``productID``, ``loopedBasketID``, ``substitutabilityFamily``, ``joinedFamily``,
  ``flexibleGroupID``, ``linkedServiceWindowID``, ``reasonRejected``) are **strings**, never cast.
  Zeros stay zero and negative prices stay negative. ``acceptanceRatio`` has no bound: the current
  buy-order row auction 2758 / order 2405348 = ``1.049999949856768`` is kept (TODO:
  EAC-BUY-ACCEPTANCE-RANGE, never clamped). Whether an order id survives a revision is unmeasured
  (TODO: EAC-ORDER-REVISION-IDENTITY), so no ``key_latest`` is used.
- Vintage follows V-8: a family holding a datastore resource takes ``capture_fallback`` (so the
  mixed sell-order family stamps capture time on its 35 upload archives too); an upload-only family
  takes ``ckan_last_modified``.
- Sell orders take the **raw-time route**: archive ``202607`` writes every time cell with a space
  separator (``2026-06-30 22:00:00``), the other 35 bodies with ``T``; one scalar ``format`` cannot
  read both and ``formats_by_filename`` is ``date`` only, so every sell time column is a string in
  both epochs and the temporal recipe is ``none``. The two header epochs (22 columns, and the 20
  columns of archives ``202411``-``202503``) are selected by exact header, never by date.
- Current, FY and daily tables are separate records. Measured keys are disjoint between current and
  FY; each daily table repeats the latest-auction slice of its current table, so a consumer must
  never sum a daily family with its current counterpart.
- Original publication instants of the auction results are unknown (TODO).
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

import polars as pl
import pytest
from _neso_generic_support import write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings

from gridflow.connectors.neso_data_portal import eligibility as eligibility_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Eligible, load_registry
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as3a"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
PACKAGE = "eac-auction-results"
PACKAGE_ID = "291e3c28-75f2-4a8f-b5f5-008bebaac368"
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

UNIT = "eac_results_by_unit"
UNIT_FY = "eac_results_by_unit_fy"
UNIT_DAILY = "eac_results_daily_by_unit"
BUY = "eac_results_buy_orders"
BUY_FY = "eac_results_buy_orders_fy"
BUY_DAILY = "eac_results_daily_buy_orders"
SUMMARY = "eac_results_summary"
SUMMARY_FY = "eac_results_summary_fy"
SUMMARY_DAILY = "eac_results_daily_summary"
SELL = "eac_results_sell_orders"
SELL_DAILY = "eac_results_daily_sell_orders"
RECORDED = (
    UNIT,
    UNIT_FY,
    UNIT_DAILY,
    BUY,
    BUY_FY,
    BUY_DAILY,
    SUMMARY,
    SUMMARY_FY,
    SUMMARY_DAILY,
    SELL,
    SELL_DAILY,
)
DATASTORE_FAMILIES = (UNIT, BUY, SUMMARY, SELL)

H_U = [
    "registeredAuctionParticipant",
    "auctionUnit",
    "serviceType",
    "auctionProduct",
    "executedQuantity",
    "clearingPrice",
    "deliveryStart",
    "deliveryEnd",
    "technologyType",
    "postCode",
    "unitResultID",
]
H_B = [
    "auctionID",
    "orderID",
    "serviceType",
    "auctionProduct",
    "deliveryStart",
    "deliveryEnd",
    "quantity",
    "price",
    "substitutabilityFamily",
    "paradoxicallyAcceptanceAllowed",
    "orderEntryTime",
    "acceptanceRatio",
    "status",
    "executedQuantity",
    "clearingPrice",
    "reasonRejected",
    "joinedFamily",
]
H_S = [
    "auctionID",
    "auctionProduct",
    "serviceType",
    "deliveryStart",
    "deliveryEnd",
    "clearedVolume",
    "clearingPrice",
    "linkedServiceWindowID",
]
H_O22 = [
    "auctionID",
    "registeredAuctionParticipant",
    "auctionUnit",
    "basketID",
    "serviceType",
    "deliveryStart",
    "deliveryEnd",
    "orderID",
    "orderType",
    "auctionProduct",
    "productID",
    "quantity",
    "priceLimit",
    "loopedBasketID",
    "orderEntryTime",
    "acceptanceRatio",
    "status",
    "executedQuantity",
    "clearingPrice",
    "reasonRejected",
    "flexibleGroupID",
    "minimumConsecutiveWindows",
]
H_O20 = H_O22[:-2]
HEADERS_OF: dict[str, list[list[str]]] = {
    UNIT: [H_U],
    UNIT_FY: [H_U],
    UNIT_DAILY: [H_U],
    BUY: [H_B],
    BUY_FY: [H_B],
    BUY_DAILY: [H_B],
    SUMMARY: [H_S],
    SUMMARY_FY: [H_S],
    SUMMARY_DAILY: [H_S],
    SELL: [H_O22, H_O20],
    SELL_DAILY: [H_O22],
}

SILVER = {
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
FLOATS = {
    "executedQuantity",
    "clearingPrice",
    "quantity",
    "price",
    "priceLimit",
    "acceptanceRatio",
    "clearedVolume",
}
INTS = {"minimumConsecutiveWindows"}
TIMES = {"deliveryStart", "deliveryEnd", "orderEntryTime"}

KEY_OF: dict[str, list[str]] = {
    UNIT: ["unit_result_id"],
    UNIT_FY: ["resource_id", "unit_result_id"],
    UNIT_DAILY: ["unit_result_id"],
    BUY: ["auction_id", "order_id"],
    BUY_FY: ["resource_id", "auction_id", "order_id"],
    BUY_DAILY: ["auction_id", "order_id"],
    SUMMARY: ["auction_id", "auction_product", "delivery_start"],
    SUMMARY_FY: ["resource_id", "auction_id", "auction_product", "delivery_start"],
    SUMMARY_DAILY: ["auction_id", "auction_product", "delivery_start"],
    SELL: ["resource_id", "auction_id", "order_id"],
    SELL_DAILY: ["auction_id", "order_id"],
}
PARTITIONED = {UNIT_FY, BUY_FY, SUMMARY_FY, SELL}
UPLOAD_ONLY = {UNIT_FY, UNIT_DAILY, BUY_FY, BUY_DAILY, SUMMARY_FY, SUMMARY_DAILY, SELL_DAILY}
RAW_TIME = {SELL}

Spec = tuple[str, str, str]


def _kind(source: str, family: str) -> str:
    if source in FLOATS:
        return "f"
    if source in INTS:
        return "i"
    if source in TIMES and family not in RAW_TIME:
        return "t"
    return "s"


def cols_of(family: str, header: list[str]) -> list[Spec]:
    """(source, silver name, kind) for one epoch header: ``f`` float64, ``i`` int64, ``t`` UTC
    datetime, ``s`` string."""
    return [(s, SILVER[s], _kind(s, family)) for s in header]


def non_null_names(family: str) -> set[str]:
    """The silver names that must be ``nullable: false``: the key columns the body carries and the
    temporal input."""
    names = {k for k in KEY_OF[family] if k != "resource_id"}
    if family not in RAW_TIME:
        names.add("delivery_start")
    return names


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    family: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str
    url_type: str


CAPTURES: dict[str, Capture] = {
    "l1": Capture(
        UNIT,
        "a63ab354-7e68-44c2-ad96-c6f920c30e85",
        "NESO Response-Reserve Results By Unit",
        "a63ab354-7e68-44c2-ad96-c6f920c30e85",
        "2026-04-15T11:43:07.338503",
        "2026-10-08T10:31:38.664575+00:00",
        "datastore",
    ),
    "l2": Capture(
        UNIT_FY,
        "7505ec16-e1e7-432e-8abd-a554e03e4f02",
        "NESO Response-Reserve Results By Unit FY2023 (Archive)",
        "neso-response-reserve-results-by-unit-fy2023-archive.csv",
        "2026-04-15T11:14:07.744257",
        "2026-10-08T10:31:42.442089+00:00",
        "upload",
    ),
    "l3": Capture(
        UNIT_FY,
        "2fc9dd7e-5274-40e3-abf4-6d31b30f21c6",
        "NESO Response-Reserve Results By Unit FY2024 (Archive)",
        "neso-response-reserve-results-by-unit-fy2024-archive.csv",
        "2026-04-15T11:18:09.202266",
        "2026-10-08T10:31:46.464819+00:00",
        "upload",
    ),
    "l5": Capture(
        UNIT_DAILY,
        "07f0dd8b-4471-4e2e-9ed6-d21b0ce1f6f5",
        "NESO Response-Reserve Daily Results By Unit",
        "neso-response-reserve-daily-results-by-unit.csv",
        "2026-10-07T13:49:22.409022",
        "2026-10-08T10:31:58.705773+00:00",
        "upload",
    ),
    "l6": Capture(
        BUY,
        "1cf68f59-8eb8-4f1d-bccf-11b5a47b24e5",
        "NESO Response-Reserve Buy Orders",
        "1cf68f59-8eb8-4f1d-bccf-11b5a47b24e5",
        "2026-04-15T11:42:37.727269",
        "2026-10-08T10:28:57.818532+00:00",
        "datastore",
    ),
    "l7": Capture(
        BUY_FY,
        "7a2d64a5-3233-4ee2-92fd-240db03717f1",
        "NESO Response-Reserve Buy Orders FY2023 (Archive)",
        "neso-response-reserve-buy-orders-fy2023-archive.csv",
        "2026-04-15T11:12:53.676038",
        "2026-10-08T10:29:01.256221+00:00",
        "upload",
    ),
    "l10": Capture(
        BUY_DAILY,
        "e638cf3d-bed1-42ae-a984-8fe3c556febf",
        "NESO Response-Reserve Daily Buy Orders",
        "neso-response-reserve-daily-buy-orders.csv",
        "2026-10-07T13:50:00.627160",
        "2026-10-08T10:31:54.753288+00:00",
        "upload",
    ),
    "l11": Capture(
        SUMMARY,
        "596f29ac-0387-4ba4-a6d3-95c243140707",
        "NESO Response-Reserve Results Summary",
        "596f29ac-0387-4ba4-a6d3-95c243140707",
        "2026-04-15T11:41:56.782602",
        "2026-10-08T10:36:10.800537+00:00",
        "datastore",
    ),
    "l12": Capture(
        SUMMARY_FY,
        "be5c6b0d-a335-4859-93f2-389585b4e9a1",
        "NESO Response-Reserve Results Summary FY2023 (Archive)",
        "neso-response-reserve-results-summary-fy2023-archive.csv",
        "2026-04-15T11:11:47.892835",
        "2026-10-08T10:36:14.125280+00:00",
        "upload",
    ),
    "l15": Capture(
        SUMMARY_DAILY,
        "3c51a666-1c33-450e-a6eb-c9b4a0c91584",
        "NESO Response-Reserve Daily Results Summary",
        "neso-response-reserve-daily-results-summary.csv",
        "2026-10-07T13:50:37.548300",
        "2026-10-08T10:32:06.033083+00:00",
        "upload",
    ),
    "l16": Capture(
        SELL,
        "13b511df-d6ec-4143-afb1-0ecc6fd19810",
        "NESO Response-Reserve Sell Orders",
        "13b511df-d6ec-4143-afb1-0ecc6fd19810",
        "2026-10-01T16:01:12.539145",
        "2026-10-08T10:32:59.018985+00:00",
        "datastore",
    ),
    "l32": Capture(
        SELL,
        "5997f602-24f1-4f44-aa36-c7b898229d60",
        "NESO Response-Reserve Sell Orders 202502 (Archive)",
        "neso-response-reserve-sell-orders-202502-archive.csv",
        "2026-03-03T16:06:53.676437",
        "2026-10-08T10:33:50.526131+00:00",
        "upload",
    ),
    "l49": Capture(
        SELL,
        "fc52a1fa-6ce5-4c35-a661-be1b49c1c48f",
        "NESO Response-Reserve Sell Orders 202607 (Archive)",
        "neso-response-reserve-sell-orders-202607-archive.csv",
        "2026-08-12T16:47:23.277278",
        "2026-10-08T10:35:51.129679+00:00",
        "upload",
    ),
    "l52": Capture(
        SELL_DAILY,
        "8ba48f26-d73e-4094-a90d-ba075eb739c1",
        "NESO Response-Reserve Daily Sell Orders",
        "neso-response-reserve-daily-sell-orders.csv",
        "2026-10-07T13:51:36.544135",
        "2026-10-08T10:32:02.592784+00:00",
        "upload",
    ),
}
FIRST_OF = {
    UNIT: "l1",
    UNIT_FY: "l2",
    UNIT_DAILY: "l5",
    BUY: "l6",
    BUY_FY: "l7",
    BUY_DAILY: "l10",
    SUMMARY: "l11",
    SUMMARY_FY: "l12",
    SUMMARY_DAILY: "l15",
    SELL: "l16",
    SELL_DAILY: "l52",
}
ALIASES = list(CAPTURES)
CRLF_ALIASES = {"l2", "l3", "l7", "l12", "l32"}


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
        prefix="as3a", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: the bronze bytes, CRLF or LF)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8")
    return [r for r in csv.DictReader(io.StringIO(text, newline="")) if any(r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8").splitlines()[0]
    return next(csv.reader([first]))


def capture(data: Path, alias: str, *, raw: bytes | None = None) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=PACKAGE,
        package_id=PACKAGE_ID,
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


def _package_doc() -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / f"{PACKAGE}.json").read_text("utf-8"))
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


def _header_specs(family: str, alias: str) -> list[Spec]:
    return cols_of(family, header(alias))


def _assert_cells(family: str, alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, instant and measure of ``frame`` equals the body cell."""
    source_rows = rows(alias)
    for spec in _header_specs(family, alias):
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
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header (the
    two sell epochs included), the bronze row terminators (CRLF on the five FY / archive uploads, LF
    on the rest) and no BOM, a blank technology type and post code
    in the by-unit cut, the BST-season delivery start, the over-one acceptance ratio, the ``-1.0``
    clearing price, two summary rows sharing an auction and product, and the space-separated time
    cells of archive 202607 against the ``T`` separator of every other sell cut."""
    assert set(FIRST_OF.values()) <= set(CAPTURES)
    for alias, meta in CAPTURES.items():
        raw = body(alias)
        assert not raw.startswith(b"\xef\xbb\xbf"), alias
        assert raw.endswith(b"\n"), alias
        assert raw.count(b"\r\n") == (raw.count(b"\n") if alias in CRLF_ALIASES else 0), alias
        assert header(alias) in HEADERS_OF[meta.family], alias
    assert header("l32") == H_O20 and header("l16") == header("l49") == H_O22
    assert any(r["technologyType"] == "" and r["postCode"] == "" for r in rows("l1"))
    assert any(r["deliveryStart"] == "2026-03-31T22:00:00" for r in rows("l1"))
    assert any(r["acceptanceRatio"] == "1.049999949856768" for r in rows("l6"))
    assert any(r["clearingPrice"] == "-1.0" for r in rows("l7"))
    keys = [(r["auctionID"], r["auctionProduct"]) for r in rows("l11")]
    starts = {r["deliveryStart"] for r in rows("l11")}
    assert len(keys) == 2 and len(set(keys)) == 1 and len(starts) == 2
    assert all(" " in r["deliveryStart"] and "T" not in r["deliveryStart"] for r in rows("l49"))
    for alias in ("l16", "l32", "l52"):
        assert all("T" in r["deliveryStart"] and " " not in r["deliveryStart"] for r in rows(alias))


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_partition_vintage_and_no_issue(
    key: str,
) -> None:
    """Detects a record that invents an issue time from the bid submission instant, a ``key_latest``
    selection with no vendor identity, a ``run_type``, a missing resource partition on a multi-file
    family, or the wrong vintage: every record is version 1, utf-8 CSV, whole-capture selection,
    the spec's entity key, partitioned by resource exactly for the FY families and the mixed sell
    family, ``capture_fallback`` for a family holding a datastore resource and
    ``ckan_last_modified`` for an upload-only family, with the vendor header(s) as its epochs and
    ``issue: none``."""
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


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: opaque identifiers,
    free text and codes are non-numeric strings, the volumes, prices and acceptance ratio are
    nullable float64 (zero is a value, the ratio has no upper bound), the window count is int64,
    the three clocks are ``%Y-%m-%dT%H:%M:%S`` UTC datetimes except on the sell orders (strings),
    key columns and the temporal input are ``nullable: false``, and no column declares ``trim``,
    ``null_tokens``, ``formats_by_filename``, a bound or a zone rule other than ``UTC``."""
    record = _record(key)
    for epoch, expected_header in zip(record.epochs, HEADERS_OF[key], strict=True):
        expected = cols_of(key, expected_header)
        assert [c.source for c in epoch.columns] == [s[0] for s in expected]
        assert [c.name for c in epoch.columns] == [s[1] for s in expected]
        for column, (source, name, kind) in zip(epoch.columns, expected, strict=True):
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


@pytest.mark.parametrize("key", RECORDED)
def test_identifiers_are_opaque_strings_never_cast(key: str) -> None:
    """Detects an identifier cast to an integer (the profiler's draft typed ``auctionID`` and the
    order ids ``int64``; ids are opaque vendor strings, ``reasonRejected`` is a code and
    ``unitResultID`` carries ``#||#`` separators): every identifier column of every epoch is a
    string."""
    identifiers = {
        "unit_result_id",
        "auction_id",
        "order_id",
        "basket_id",
        "product_id",
        "looped_basket_id",
        "substitutability_family",
        "joined_family",
        "flexible_group_id",
        "linked_service_window_id",
        "reason_rejected",
    }
    for epoch in _record(key).epochs:
        types = {c.name: c.dtype for c in epoch.columns}
        present = identifiers & set(types)
        assert present and {types[n] for n in present} == {"string"}


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_delivery_start_or_none(key: str) -> None:
    """Detects a delivery interval fabricated for the sell orders (their clock is raw text because
    archive 202607 mixes a space separator, so there is no one-format instant) or a typed family
    anchored on anything but ``delivery_start``: the ten typed families take ``utc_instant`` of
    ``delivery_start`` and the sell orders take ``none``."""
    recipe = _record(key).temporal
    if key in RAW_TIME:
        assert (recipe.kind, recipe.column, recipe.date_column) == ("none", None, None)
    else:
        assert (recipe.kind, recipe.column, recipe.date_column) == (
            "utc_instant",
            "delivery_start",
            None,
        )


def test_the_sell_orders_have_two_exact_header_epochs_and_raw_time_strings() -> None:
    """Detects a sell epoch selected by date or a typed sell clock: epoch 0 is the 22-column
    header, epoch 1 the 20-column header (no ``flexibleGroupID`` or ``minimumConsecutiveWindows``),
    every delivery / submission column is a string in both, and the two epochs share one dtype for
    every silver name they have in common (V-1)."""
    record = _record(SELL)
    assert [len(e.header) for e in record.epochs] == [22, 20]
    by_epoch = [{c.name: c.dtype for c in e.columns} for e in record.epochs]
    assert set(by_epoch[0]) - set(by_epoch[1]) == {
        "flexible_group_id",
        "minimum_consecutive_windows",
    }
    assert all(by_epoch[0][n] == by_epoch[1][n] for n in by_epoch[1])
    for types in by_epoch:
        assert {types[n] for n in ("delivery_start", "delivery_end", "order_entry_time")} == {
            "string"
        }
    assert by_epoch[0]["minimum_consecutive_windows"] == "int64"


def test_the_mixed_sell_family_cannot_take_ckan_last_modified() -> None:
    """Detects the sell record drifting to ``ckan_last_modified`` (V-8): the family holds the
    datastore resource L16 next to 35 upload archives, so a copy of the record with that vintage
    fails the load rule, while the record itself passes it."""
    record = _record(SELL)
    kinds = frozenset({"datastore", "upload"})
    siblings = {key: True for key in RECORDED}

    def check(rec: SchemaRecord) -> None:
        validate_record(
            rec,
            key=SELL,
            kind="tabular",
            legacy=False,
            package_families=siblings,
            family_url_types=kinds,
        )

    check(record)
    with pytest.raises(RecordError, match=r"V-8:"):
        check(record.model_copy(update={"vintage": "ckan_last_modified"}))


@pytest.mark.parametrize("key", RECORDED)
def test_all_eleven_are_eligible_and_the_package_stays_eligible(key: str) -> None:
    """Detects a hold or a per-record override on a clean family: none of the eleven carries an
    eligibility override, each is effectively eligible and so is the package."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.eligibility == Eligible(status="eligible")


def test_the_package_file_carries_exactly_the_eleven_records_and_no_ledger_entry() -> None:
    """Detects a record on the wrong family, a family gaining a transformer, a changed resource
    disposition or a reconcile adjudication this batch does not need: the package file holds
    exactly the eleven records (52 resources, 4 datastore and 48 upload, all ``SILVER``), no family
    has a bespoke transformer, and the reconcile ledger names none of them."""
    document = _package_doc()
    assert {f["key"] for f in document["families"] if "record" in f} == set(RECORDED)
    assert all(f["transformer"] is None for f in document["families"])
    resources = document["resources"]
    assert len(resources) == 52
    assert all(r["disposition"]["kind"] == "SILVER" for r in resources)
    kinds = [r["url_type"] for r in resources]
    assert (kinds.count("datastore"), kinds.count("upload")) == (4, 48)
    assert {r["family"] for r in resources if r["url_type"] == "datastore"} == set(
        DATASTORE_FAMILIES
    )
    entries = registry_module.load_reconcile_adjudications()
    assert {e.family for e in entries} & set(RECORDED) == set()


def test_eleven_are_reported_as_eligible_silver_outputs() -> None:
    """Detects a family reported as ingest-only, held or with the wrong selection or vintage: the
    eligibility report lists each of the eleven as ``silver`` / ``eligible`` / ``whole_capture``,
    the resource-partitioned ones ``per resource_id``, with the capture-time vintage on exactly the
    families that hold a datastore resource."""
    report = eligibility_module.render(load_registry())
    for key in RECORDED:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver | eligible |" in line, key
        assert ("whole_capture per resource_id" in line) == (key in PARTITIONED), key
        assert ("gridflow capture time" in line) == (key in DATASTORE_FAMILIES), key


@pytest.mark.parametrize("key", RECORDED)
def test_every_family_is_generic_tabular_silver_with_its_own_record(key: str) -> None:
    """Detects a family wired to a bespoke transformer instead of the generic engine."""
    package, entry = load_registry().families[key]
    assert entry.record is not None and entry.transformer is None
    assert entry.kind == "tabular"
    assert package.eligibility == Eligible(status="eligible")


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ALIASES)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a duplicate
    key or an excluded row: the slice loads every row with zero exclusions and no failure, every
    identifier, label, instant and measure equals the body cell, the entity key is unique, and the
    temporal column is the delivery start (tz-aware UTC, no shift) or, for the sell orders with no
    recipe, the capture time. A fixture drawn from a typed family that failed the strict
    ``%Y-%m-%dT%H:%M:%S`` parse, or a numeric column that failed its strict cast, would fail
    here."""
    family = CAPTURES[alias].family
    frame = _load_clean(data, alias)
    _assert_cells(family, alias, frame)
    if "resource_id" in frame.columns:
        assert set(frame["resource_id"].to_list()) == {CAPTURES[alias].resource_id}
    assert not frame.select([k for k in KEY_OF[family] if k in frame.columns]).is_duplicated().any()
    if family in RAW_TIME:
        written = datetime.fromisoformat(CAPTURES[alias].written)
        assert set(frame["timestamp_utc"].to_list()) == {written}
        assert {
            frame.schema[n] for n in ("delivery_start", "delivery_end", "order_entry_time")
        } == {pl.String}
    else:
        assert frame["timestamp_utc"].to_list() == frame["delivery_start"].to_list()
        assert frame.schema["delivery_start"] == pl.Datetime("us", "UTC")
        assert frame["delivery_start"].null_count() == 0


def test_by_unit_keeps_blank_technology_and_post_code_null_and_the_bst_start_unshifted(
    data: Path,
) -> None:
    """Detects a blank technology type or post code filled, an identifier trimmed or cast, and a
    UTC clock shifted as if it were UK local time: row 70 keeps both blanks as null,
    ``unit_result_id`` is byte-equal (``#||#`` separators intact), the executed quantity and price
    are floats, and the BST-season ``2026-03-31T22:00:00`` is 22:00 UTC (not 21:00)."""
    frame = _load_clean(data, "l1")
    blank = frame.filter(pl.col("unit_result_id") == "2661#||#3694#||#PQR#||#150393")
    assert blank["technology_type"].to_list() == [None] and blank["post_code"].to_list() == [None]
    assert frame["unit_result_id"].to_list() == [r["unitResultID"] for r in rows("l1")]
    assert frame.schema["unit_result_id"] == pl.String
    assert (frame.schema["executed_quantity"], frame.schema["clearing_price"]) == (
        pl.Float64,
        pl.Float64,
    )
    assert datetime(2026, 3, 31, 22, 0, tzinfo=UTC) in frame["delivery_start"].to_list()
    assert datetime(2026, 3, 31, 21, 0, tzinfo=UTC) not in frame["delivery_start"].to_list()


def test_both_by_unit_fy_resources_are_kept_under_the_resource_partition(data: Path) -> None:
    """Detects one FY resource shadowing the other or an unstamped resource id: L2 (FY2023) and L3
    (FY2024) both load, every row carries its own ``resource_id``, the key is
    ``(resource_id, unit_result_id)``, and the newest complete capture of each resource is served
    (two captures, two resources, neither superseded)."""
    frame = _load_clean(data, "l2", "l3")
    assert frame.height == len(rows("l2")) + len(rows("l3")) == 6
    by_resource = frame.group_by("resource_id").len().sort("resource_id")
    assert by_resource["resource_id"].to_list() == sorted(
        [CAPTURES["l2"].resource_id, CAPTURES["l3"].resource_id]
    )
    assert by_resource["len"].to_list() == [3, 3]
    assert not frame.select(KEY_OF[UNIT_FY]).is_duplicated().any()


def test_buy_orders_keep_the_over_one_acceptance_ratio_and_zero_price(data: Path) -> None:
    """Detects ``acceptanceRatio`` clamped or bounded (the current buy-order row auction 2758 /
    order 2405348 is ``1.049999949856768``, executed quantity 21 on a 20 MW bid; TODO:
    EAC-BUY-ACCEPTANCE-RANGE) and a zero price read as missing: the row is kept, the ratio equals
    the body float exactly, the zero price stays ``0.0``, and the ids are strings."""
    frame = _load_clean(data, "l6")
    anomaly = frame.filter((pl.col("auction_id") == "2758") & (pl.col("order_id") == "2405348"))
    assert anomaly.height == 1
    assert anomaly["acceptance_ratio"].to_list() == [1.049999949856768]
    assert anomaly["executed_quantity"].to_list() == [21.0] and anomaly["price"].to_list() == [0.0]
    assert anomaly["reason_rejected"].to_list() == [None]
    assert frame.schema["auction_id"] == frame.schema["order_id"] == pl.String
    assert frame.schema["acceptance_ratio"] == pl.Float64


def test_a_clearing_price_of_minus_one_stays_minus_one(data: Path) -> None:
    """Detects a negative one read as a missing-value sentinel (FACTS g5: signed documented prices,
    never null): the FY buy order with ``clearingPrice`` ``-1.0`` loads as ``-1.0``, not null, and
    no clearing price in the slice is null."""
    frame = _load_clean(data, "l7")
    assert -1.0 in frame["clearing_price"].to_list()
    assert frame["clearing_price"].null_count() == 0
    assert frame.schema["clearing_price"] == pl.Float64


def test_summary_rows_sharing_auction_and_product_are_both_kept_with_a_unique_key(
    data: Path,
) -> None:
    """Detects the summary key shortened to ``(auction_id, auction_product)`` (which repeats: tens
    of thousands of duplicate excess in the live bodies): the two rows of auction 2661 / product DRH
    at different delivery starts both load, the key including ``delivery_start`` is unique, and the
    cleared volume and clearing price are floats."""
    frame = _load_clean(data, "l11")
    assert frame.height == 2
    assert frame.select(["auction_id", "auction_product"]).is_duplicated().all()
    assert not frame.select(KEY_OF[SUMMARY]).is_duplicated().any()
    assert frame.schema["cleared_volume"] == frame.schema["clearing_price"] == pl.Float64
    assert frame["linked_service_window_id"].to_list() == [None, None]


def test_the_two_sell_epochs_and_the_space_separated_archive_load_in_one_family(
    data: Path,
) -> None:
    """Detects an H-O20 archive refused or its absent columns filled with a default, and the L49
    space-separated times parsed, repaired or rejected: the H-O22 datastore cut, the H-O20 archive
    and the space-separated archive load together (9 rows, zero exclusions); the H-O20 rows have
    null ``flexible_group_id`` and ``minimum_consecutive_windows``; every time cell of every cut is
    byte-equal to its body (the 202607 cells keep the space, ``2026-06-30 22:00:00``) and nothing
    parses them; each row carries its own ``resource_id``; and the capture time is the timestamp."""
    frame = _load_clean(data, "l16", "l32", "l49")
    assert frame.height == 9
    legacy = frame.filter(pl.col("resource_id") == CAPTURES["l32"].resource_id)
    assert legacy.height == 3
    assert legacy["flexible_group_id"].null_count() == 3
    assert legacy["minimum_consecutive_windows"].null_count() == 3
    assert legacy.schema["minimum_consecutive_windows"] == pl.Int64
    spaced = frame.filter(pl.col("resource_id") == CAPTURES["l49"].resource_id)
    assert spaced["delivery_start"].to_list() == [r["deliveryStart"] for r in rows("l49")]
    assert spaced["delivery_start"].to_list()[0] == "2026-06-30 22:00:00"
    assert spaced["order_entry_time"].to_list()[0] == "2026-06-26 13:46:44"
    for alias in ("l16", "l32", "l49"):
        part = frame.filter(pl.col("resource_id") == CAPTURES[alias].resource_id)
        for column, source in (
            ("delivery_start", "deliveryStart"),
            ("delivery_end", "deliveryEnd"),
            ("order_entry_time", "orderEntryTime"),
        ):
            assert part[column].to_list() == [r[source] for r in rows(alias)], (alias, column)
        assert set(part["timestamp_utc"].to_list()) == {
            datetime.fromisoformat(CAPTURES[alias].written)
        }
    assert frame.schema["delivery_start"] == pl.String
    assert not frame.select(KEY_OF[SELL]).is_duplicated().any()
    assert all(isinstance(v, str) for v in frame["order_id"].to_list())


def test_daily_sell_orders_load_typed_under_the_t_separator(data: Path) -> None:
    """Detects the daily sell table (T separator only) following the raw-time route of the monthly
    family: its three clocks are UTC datetimes with no shift, the flexible group and window count
    columns are present (22-column header), the ids are strings and the key is
    ``(auction_id, order_id)``."""
    frame = _load_clean(data, "l52")
    assert frame.schema["delivery_start"] == pl.Datetime("us", "UTC")
    assert frame.schema["order_entry_time"] == pl.Datetime("us", "UTC")
    assert frame["delivery_start"].to_list()[0] == datetime(2026, 10, 8, 10, 0, tzinfo=UTC)
    assert frame.schema["minimum_consecutive_windows"] == pl.Int64
    assert frame.schema["order_id"] == pl.String
    assert not frame.select(KEY_OF[SELL_DAILY]).is_duplicated().any()


def test_a_daily_family_is_not_a_second_copy_to_add(data: Path) -> None:
    """Detects a daily table folded into its current counterpart: the daily by-unit, buy-order and
    summary families are separate silver datasets with their own key, vintage (CKAN last-modified
    against the current table's capture time) and silver directory, so a consumer reading both
    would double-count the latest auction. This pins the separation, not a merge."""
    for current, daily in ((UNIT, UNIT_DAILY), (BUY, BUY_DAILY), (SUMMARY, SUMMARY_DAILY)):
        assert current != daily
        assert _record(current).vintage == "capture_fallback"
        assert _record(daily).vintage == "ckan_last_modified"
        assert list(_record(current).entity_key) == list(_record(daily).entity_key)
    assert _no_silver(data, UNIT_DAILY)


# --------------------------------------------------------------------------- #
# Loud failure, vintage and catalogue
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
    capture_id = capture(data, alias, raw=renamed)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_a_sell_body_that_is_neither_epoch_fails_loud(data: Path) -> None:
    """Detects a third sell header (21 columns: the 22-column header less its last column) loading
    against the nearest epoch: it matches neither the 22- nor the 20-column header, so the capture
    fails and writes nothing."""
    raw = body("l16")
    head, rest = raw.split(b"\n", 1)
    cut = head.rsplit(b",", 1)[0]
    assert cut.count(b",") == 20
    capture_id = capture(data, "l16", raw=cut + b"\n" + rest)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, SELL, data).run(DAY, run_id="r")
    assert read_completion(data, SELL, capture_id) is None
    assert _no_silver(data, SELL)


@pytest.mark.parametrize("family", RECORDED)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, family: str) -> None:
    """Detects a duplicate key loaded silently: the last data row repeated fails the capture with
    ``DuplicateEntityKeyError``, loads nothing and leaves no completion."""
    alias = FIRST_OF[family]
    terminated = body(alias).split(b"\n")
    assert terminated[-1] == b""
    raw = b"\n".join([*terminated[:-1], terminated[-2], b""])
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["l2", "l5", "l7", "l12", "l15", "l52"])
def test_an_upload_only_family_is_stamped_with_the_ckan_last_modified(
    data: Path, alias: str
) -> None:
    """Detects an upload-only family falling back to capture time or a catalogue view that cannot
    carry the new columns: ``available_at`` is the CKAN ``last_modified`` read as UTC; an as-of read
    before it serves nothing and one after serves the capture, in the DuckDB view and in Polars."""
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


@pytest.mark.parametrize("alias", ["l1", "l6", "l11", "l16"])
def test_a_datastore_family_is_stamped_with_the_capture_time(data: Path, alias: str) -> None:
    """Detects a datastore family (whose ``last_modified`` is an administrative instant) taking the
    CKAN vintage: ``available_at`` is the capture's ``written_at`` and an as-of read before it
    serves nothing."""
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


def test_the_upload_archives_of_the_mixed_sell_family_also_take_the_capture_time(
    data: Path,
) -> None:
    """Detects the upload archives of the mixed sell family taking their own CKAN last-modified
    (earlier than the true availability of the capture, which would leak a later capture into an
    earlier as-of read): the H-O20 archive's ``available_at`` is its ``written_at``, not its
    ``ckan_last_modified``."""
    meta = CAPTURES["l32"]
    capture(data, "l32")
    get_transformer(SOURCE, SELL, data).run(DAY, run_id="r")
    frame = _silver(data, SELL)
    written = datetime.fromisoformat(meta.written).astimezone(UTC)
    modified = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {written} != {modified}


@pytest.mark.parametrize("key", RECORDED)
def test_the_skeleton_page_renders_the_new_records(key: str) -> None:
    """Detects a record the docs generator cannot render (the two-epoch sell record, the datastore
    and upload vintages, the resource-partitioned keys)."""
    page = skeleton.render_package(
        load_registry(),
        {
            "name": PACKAGE,
            "title": PACKAGE,
            "organization": {"title": "NESO"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    assert f"`{key}`" in page
