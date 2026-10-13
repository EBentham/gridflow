"""The EAC-BR archive results and the EAC-BR mock results: eight frozen records, four eligible, four
held (v0.22-K-AS-4b).

Two packages carry four families each. ``eac-br-auction-results`` (the 2023-2024 Balancing Reserve
archive: by unit, buy orders, results summary and sell orders) stays eligible. The sell orders have
no bronze body yet (the 2026-10-08 sweep aborted their datastore dump at the 512 MiB cap), so their
record is frozen from the P1 live probe's header and the family cap is raised to 768 MiB; silver for
that family waits for the seat's capture and its checks. ``eac-br-mock-auction-results`` mirrors the
four tables as outcomes of EAC-BR mock runs: its four records are held on the undocumented mock
clock and the package is held as test data. Every test writes recorded fixture captures (cuts of the
2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/as4b/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation. On master
none of the eight families has a record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-AS-4-FACTS sections 7 and 8, K-AS-4b-SPEC):

- Every time cell (``deliveryStart``, ``deliveryEnd``, ``orderEntryTime``) is a raw **string** and
  the temporal recipe is ``none``: neither package states the clock of its naive archive exports
  and the live EAC package's UTC declaration does not extend to them (TODO:
  BR-ARCHIVE-EXPORT-CLOCK, BR-MOCK-EXPORT-CLOCK). No date column is sliced out of a timestamp.
  ``issue: none`` everywhere: ``orderEntryTime`` is bid submission, not the availability of the
  outcome columns.
- Opaque identifiers (``auctionID``, ``orderID``, ``unitResultID``, ``basketID``, ``productID``,
  ``loopedBasketID``, ``joinedFamily``, ``substitutabilityFamily``, ``reasonRejected``) are
  **strings**, byte-equal; volumes, prices and the acceptance ratio are ``float64`` with zeros kept.
- Every family holds one datastore resource, so vintage is ``capture_fallback`` (V-8); there is no
  resource partition and no ``key_latest``.
- The BR sell-orders header is the P1 header, byte-equal to the AS-3a ``eac_results_sell_orders``
  second epoch and to the first line of the mock sell body M03. The sell-order parse tests load
  **mock** bytes (M03) under the live record purely because that is the only real vendor body with
  this header; they are not BR live evidence.
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as4b"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-AS-4b"

BR_PKG = "eac-br-auction-results"
BR_PKG_ID = "94c65383-a108-468f-aad4-1d96380be93f"
MOCK_PKG = "eac-br-mock-auction-results"
MOCK_PKG_ID = "c9843f08-bb12-461d-9741-69729fe887a0"

BR_UNIT = "eac_br_results_by_unit"
BR_BUY = "eac_br_results_buy_orders"
BR_SUMMARY = "eac_br_results_summary"
BR_SELL = "eac_br_results_sell_orders"
MOCK_UNIT = "eac_br_mock_results_by_unit"
MOCK_BUY = "eac_br_mock_results_buy_orders"
MOCK_SELL = "eac_br_mock_results_sell_orders"
MOCK_SUMMARY = "eac_br_mock_results_summary"
ELIGIBLE = (BR_UNIT, BR_BUY, BR_SUMMARY, BR_SELL)
HELD = (MOCK_UNIT, MOCK_BUY, MOCK_SELL, MOCK_SUMMARY)
RECORDED = (*ELIGIBLE, *HELD)
PACKAGE_OF = {**dict.fromkeys(ELIGIBLE, BR_PKG), **dict.fromkeys(HELD, MOCK_PKG)}
BASE_CAP = 536_870_912
SELL_CAP = 805_306_368

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
    "joinedFamily",
    "substitutabilityFamily",
    "paradoxicallyAcceptanceAllowed",
    "orderEntryTime",
    "acceptanceRatio",
    "status",
    "executedQuantity",
    "clearingPrice",
    "reasonRejected",
]
H_S = [
    "auctionID",
    "auctionProduct",
    "serviceType",
    "deliveryStart",
    "deliveryEnd",
    "clearedVolume",
    "clearingPrice",
]
# The P1 header (LIVE-PROBE.md, 2026-10-13), typed here independently of the registry.
H_O = [
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
]
H_O22 = [*H_O, "flexibleGroupID", "minimumConsecutiveWindows"]
HEADER_OF: dict[str, list[str]] = {
    BR_UNIT: H_U,
    BR_BUY: H_B,
    BR_SUMMARY: H_S,
    BR_SELL: H_O,
    MOCK_UNIT: H_U,
    MOCK_BUY: H_B,
    MOCK_SELL: H_O,
    MOCK_SUMMARY: H_S,
}
KEY_OF: dict[str, list[str]] = {
    BR_UNIT: ["unit_result_id"],
    BR_BUY: ["auction_id", "order_id"],
    BR_SUMMARY: ["auction_id", "auction_product", "delivery_start"],
    BR_SELL: ["auction_id", "order_id"],
    MOCK_UNIT: ["unit_result_id"],
    MOCK_BUY: ["auction_id", "order_id"],
    MOCK_SELL: ["auction_id", "order_id"],
    MOCK_SUMMARY: ["auction_id", "auction_product", "delivery_start"],
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
    "joinedFamily": "joined_family",
    "substitutabilityFamily": "substitutability_family",
    "paradoxicallyAcceptanceAllowed": "paradoxically_acceptance_allowed",
    "orderEntryTime": "order_entry_time",
    "acceptanceRatio": "acceptance_ratio",
    "status": "status",
    "reasonRejected": "reason_rejected",
    "clearedVolume": "cleared_volume",
    "basketID": "basket_id",
    "orderType": "order_type",
    "productID": "product_id",
    "priceLimit": "price_limit",
    "loopedBasketID": "looped_basket_id",
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
IDENTIFIER_NAMES = {
    "unit_result_id",
    "auction_id",
    "order_id",
    "basket_id",
    "product_id",
    "looped_basket_id",
    "joined_family",
    "substitutability_family",
    "reason_rejected",
}
TIME_NAMES = {"delivery_start", "delivery_end", "order_entry_time"}

CLOCK_QUESTION = (
    "TODO: BR-MOCK-EXPORT-CLOCK — the EAC-BR mock package and its dictionaries do not state "
    "the clock of the naive deliveryStart / deliveryEnd / orderEntryTime cells, the live EAC "
    "package's UTC declaration does not extend to it, and the captured dates span "
    "February–September 2024 (both GMT and BST), so the clock cannot be inferred; which "
    "clock do the mock exports use? (Mock / test data, not procurement results.)"
)
TEST_DATA_QUESTION = (
    "TODO: EAC-BR-MOCK-TEST-DATA — NESO publishes these resources as outcomes of EAC-BR mock "
    "auction runs, not live Balancing Reserve procurement; publish only under an explicit "
    "mock/test label with separate provenance, and never join them to live EAC or EAC-BR "
    "results on auction id (the measured mock id sets are disjoint from the 596 BR archive ids, "
    "which is not a namespace guarantee)."
)

Spec = tuple[str, str, str]


def cols_of(header: list[str]) -> list[Spec]:
    """(source, silver name, kind) for one header: ``f`` float64, ``s`` string."""
    return [(s, SILVER[s], "f" if s in FLOATS else "s") for s in header]


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    family: str
    package: str
    package_id: str
    resource_id: str
    name: str
    modified: str
    written: str
    file: str


CAPTURES: dict[str, Capture] = {
    "b01": Capture(
        BR_UNIT,
        BR_PKG,
        BR_PKG_ID,
        "5d8e47be-e262-4398-89b0-6f93f636faf6",
        "NESO Balancing-Reserve Results By Unit 2023-2024",
        "2024-02-19T11:31:32.302916",
        "2026-10-08T10:15:51.218565+00:00",
        "b01",
    ),
    "b02": Capture(
        BR_BUY,
        BR_PKG,
        BR_PKG_ID,
        "8a549d29-52cd-4016-a04e-c01a3e31bd3a",
        "NESO Balancing-Reserve Buy Orders 2023-2024",
        "2024-02-19T11:28:38.434406",
        "2026-10-08T10:14:53.687941+00:00",
        "b02",
    ),
    "b03": Capture(
        BR_SUMMARY,
        BR_PKG,
        BR_PKG_ID,
        "1b3f2ee1-74a0-4939-a5a3-f01f19e663e4",
        "NESO Balancing-Reserve Results Summary 2023-2024",
        "2024-02-16T16:40:40.365723",
        "2026-10-08T10:26:23.903043+00:00",
        "b03",
    ),
    "m01": Capture(
        MOCK_UNIT,
        MOCK_PKG,
        MOCK_PKG_ID,
        "7f5a3d87-a3fb-4c5b-88f0-ecba2401834d",
        "NESO Mock Balancing-Reserve Results By Unit 2023-2024",
        "2024-01-24T15:24:05.731718",
        "2026-10-08T10:13:23.458722+00:00",
        "m01",
    ),
    "m02": Capture(
        MOCK_BUY,
        MOCK_PKG,
        MOCK_PKG_ID,
        "e3ad918b-f10b-49c7-93ba-b7e81fe6feea",
        "NESO Mock Balancing-Reserve Buy Orders 2023-2024",
        "2024-01-24T15:40:32.654664",
        "2026-10-08T10:12:50.282950+00:00",
        "m02",
    ),
    "m03": Capture(
        MOCK_SELL,
        MOCK_PKG,
        MOCK_PKG_ID,
        "b73e34ca-28fc-4fb2-9275-d5f546d6f33a",
        "NESO Mock Balancing-Reserve Sell Orders 2023-2024",
        "2024-01-24T15:46:00.991517",
        "2026-10-08T10:13:50.546541+00:00",
        "m03",
    ),
    "m04": Capture(
        MOCK_SUMMARY,
        MOCK_PKG,
        MOCK_PKG_ID,
        "a285483f-d830-4ae2-8b6d-98c05f6d21f3",
        "NESO Mock Balancing-Reserve Results Summary 2023-2024",
        "2024-01-24T15:51:33.611315",
        "2026-10-08T10:14:20.335194+00:00",
        "m04",
    ),
    # Mock bytes (M03) filed as the live BR sell-orders resource: the only real vendor body with
    # the P1 header. Not BR live evidence (PROVENANCE.md). The sidecar times are the test's own.
    "s01": Capture(
        BR_SELL,
        BR_PKG,
        BR_PKG_ID,
        "3f53c38b-d287-41af-b4c8-769abb6e707a",
        "NESO Balancing-Reserve Sell Orders 2023-2024",
        "2024-02-19T11:30:00.000000",
        "2026-10-13T12:00:00.000000+00:00",
        "m03",
    ),
}
FIRST_OF = {
    BR_UNIT: "b01",
    BR_BUY: "b02",
    BR_SUMMARY: "b03",
    BR_SELL: "s01",
    MOCK_UNIT: "m01",
    MOCK_BUY: "m02",
    MOCK_SELL: "m03",
    MOCK_SUMMARY: "m04",
}
ALIASES = list(CAPTURES)
QUOTED_BLANK = {("b01", "technologyType"), ("b01", "postCode")}
"""String cells B01 writes as a quoted empty ``""``: the CSV reader keeps them as the empty string
exactly as published (the K-TRD ``Notes`` and K-AS-4a ``Location`` precedent); an unquoted blank is
null."""


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
        prefix="as4b", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture of ``alias`` exactly as committed (``-text``: the bronze bytes, a lone LF)."""
    return (FIXTURES / f"{CAPTURES[alias].file}.csv").read_bytes()


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8")
    return [r for r in csv.DictReader(io.StringIO(text, newline="")) if any(r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8").splitlines()[0]
    return next(csv.reader([first]))


def capture(data: Path, alias: str, *, raw: bytes | None = None) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=meta.package,
        package_id=meta.package_id,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=meta.resource_id,
        ckan_last_modified=meta.modified,
        url_type="datastore",
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


def _expected(alias: str, spec: Spec, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (an unquoted blank cell is null, a
    quoted empty string stays empty)."""
    source, _name, kind = spec
    if cell == "":
        return "" if (alias, source) in QUOTED_BLANK else None
    return float(cell) if kind == "f" else cell


def _assert_cells(alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, time and measure of ``frame`` equals the body cell."""
    source_rows = rows(alias)
    for spec in cols_of(header(alias)):
        assert frame[spec[1]].to_list() == [
            _expected(alias, spec, r[spec[0]]) for r in source_rows
        ], spec[1]


def _load_clean(data: Path, alias: str) -> pl.DataFrame:
    """Capture and run one fixture: every row written, none excluded, the generic output columns,
    one completion for the capture, the capture time as the timestamp."""
    family = CAPTURES[alias].family
    capture_id = capture(data, alias)
    written = get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert written == len(rows(alias))
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
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header, a
    lone LF terminator and no BOM, B01 rows with the quoted-empty technology type and post code,
    the unquoted blanks of M01, two B03 summary rows sharing an auction and product at different
    delivery starts, M02 rows with a blank and a populated ``joinedFamily``, M03 rows with a blank
    and a populated ``loopedBasketID`` and ``productID`` ``11``, and M03's first line byte-equal to
    the P1 header."""
    for alias in ALIASES:
        raw = body(alias)
        assert not raw.startswith(b"\xef\xbb\xbf"), alias
        assert raw.endswith(b"\n") and b"\r" not in raw, alias
        assert header(alias) == HEADER_OF[CAPTURES[alias].family], alias
    assert body("m03").split(b"\n", 1)[0] == ",".join(H_O).encode()
    assert body("s01") == body("m03")
    b01 = body("b01").decode("utf-8")
    assert ',"",' in b01 and '"",""' in b01
    assert any(r["technologyType"] == "" and r["postCode"] == "" for r in rows("m01"))
    assert b'"' not in body("m01")
    keys = [(r["auctionID"], r["auctionProduct"]) for r in rows("b03")]
    starts = {r["deliveryStart"] for r in rows("b03")}
    assert len(keys) == 2 and len(set(keys)) == 1 and len(starts) == 2
    joined = {r["joinedFamily"] for r in rows("m02")}
    assert "" in joined and joined - {""}
    assert {r["loopedBasketID"] for r in rows("m03")} >= {"", "1", "17"}
    assert "11" in {r["productID"] for r in rows("m03")}


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_vintage_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time from the bid submission instant, a ``key_latest``
    selection with no vendor identity, a ``run_type``, a resource partition on a one-resource
    family, or the wrong vintage: every record is version 1, utf-8 CSV, whole-capture selection,
    the spec's entity key, ``capture_fallback`` (each family holds one datastore resource), one
    epoch whose header is the vendor's in the vendor's column order, and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.latest == "whole_capture" and record.latest_partition is None
    assert record.vintage == "capture_fallback" and record.vintage_evidence is None
    assert list(record.entity_key) == KEY_OF[key]
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None
    assert [list(e.header) for e in record.epochs] == [HEADER_OF[key]]
    (epoch,) = record.epochs
    assert epoch.issue.kind == "none" and epoch.unpivot is None
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: the seven volume,
    price and ratio columns are nullable float64 (zero is a value, the ratio has no upper bound),
    everything else including all three time columns is a plain string, key columns are
    ``nullable: false`` and no column declares ``trim``, ``null_tokens``, ``formats_by_filename``,
    a bound, a format or a zone rule. The silver names follow the vendor header's order."""
    (epoch,) = _record(key).epochs
    expected = cols_of(HEADER_OF[key])
    assert [c.source for c in epoch.columns] == [s[0] for s in expected]
    assert [c.name for c in epoch.columns] == [s[1] for s in expected]
    for column, (source, name, kind) in zip(epoch.columns, expected, strict=True):
        assert column.trim is None and column.null_tokens == ()
        assert (column.min, column.max) == (None, None)
        assert column.formats_by_filename is None
        assert column.nullable == (name not in KEY_OF[key]), name
        assert column.format is None and column.zone is None, source
        assert column.zone_evidence is None and column.ambiguous is None, source
        assert column.dtype == {"f": "float64", "s": "string"}[kind], source


@pytest.mark.parametrize("key", RECORDED)
def test_identifiers_and_time_cells_are_strings_never_cast(key: str) -> None:
    """Detects an identifier cast to an integer (``auctionID`` and the order ids look numeric;
    ``reasonRejected`` is a code; ``unitResultID`` carries ``#||#`` separators) or a clock typed
    without evidence (TODO: BR-ARCHIVE-EXPORT-CLOCK / BR-MOCK-EXPORT-CLOCK): every identifier
    column and every time column the family carries is a string."""
    (epoch,) = _record(key).epochs
    types = {c.name: c.dtype for c in epoch.columns}
    present = (IDENTIFIER_NAMES | TIME_NAMES) & set(types)
    assert {"delivery_start", "delivery_end"} <= present
    assert {types[n] for n in present} == {"string"}


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_none(key: str) -> None:
    """Detects a delivery interval or date fabricated from a naive archive clock: the temporal
    recipe is ``none`` on all eight, so ``timestamp_utc`` is the capture time."""
    recipe = _record(key).temporal
    assert (recipe.kind, recipe.column, recipe.date_column) == ("none", None, None)


# --------------------------------------------------------------------------- #
# Sell orders: header, cap, no bronze
# --------------------------------------------------------------------------- #


def test_the_br_sell_record_is_one_epoch_equal_to_p1_and_to_the_as3a_second_epoch() -> None:
    """Detects a transcription slip in the header frozen without bronze: the BR sell record has one
    epoch, its header equals the P1 probe's 20-field list (typed independently here) and the
    ``eac_results_sell_orders`` second epoch in ``eac-auction-results.json`` (H-O20), is not the
    22-column H-O22 of the newer EAC bodies, and the mock sell record shares it."""
    record = _record(BR_SELL)
    assert [list(e.header) for e in record.epochs] == [H_O]
    as3a = registry_module.load_registry().families["eac_results_sell_orders"][1].record
    assert as3a is not None
    assert list(as3a.epochs[1].header) == H_O != list(as3a.epochs[0].header)
    assert list(as3a.epochs[0].header) == H_O22
    assert [list(e.header) for e in _record(MOCK_SELL).epochs] == [H_O]
    assert list(record.entity_key) == ["auction_id", "order_id"]
    assert (record.temporal.kind, record.vintage, record.latest) == (
        "none",
        "capture_fallback",
        "whole_capture",
    )
    assert record.latest_partition is None


def test_only_the_br_sell_orders_have_the_raised_download_cap() -> None:
    """Detects the sell-order dump still tripping the 512 MiB cap (P1 estimated 561,174,421 B) or a
    cap raised on any other family: through the loaded registry the BR sell orders carry
    805,306,368 B (768 MiB) and every other family of both packages stays 536,870,912 B."""
    registry = registry_module.load_registry()
    for key in RECORDED:
        expected = SELL_CAP if key == BR_SELL else BASE_CAP
        assert registry.families[key][1].max_download_bytes == expected, key
    assert SELL_CAP > 561_174_421 > BASE_CAP


def test_the_br_sell_orders_have_no_bronze_so_no_silver_exists_before_the_capture(
    data: Path,
) -> None:
    """Detects a sell-order test that needs the live capture: the family has a record and a
    transformer, and with no capture written it loads nothing (silver waits for the seat's capture
    and its post-capture checks)."""
    assert get_transformer(SOURCE, BR_SELL, data) is not None
    assert _no_silver(data, BR_SELL)


# --------------------------------------------------------------------------- #
# Eligibility, holds and dispositions
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", ELIGIBLE)
def test_four_br_records_are_eligible_and_the_package_stays_eligible(key: str) -> None:
    """Detects a hold or per-record override on a clean BR family: none of the four carries an
    eligibility override, each is effectively eligible and so is the package."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.package == BR_PKG and package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", HELD)
def test_four_mock_records_are_held_with_the_verbatim_clock_question(key: str) -> None:
    """Detects a hold lost, reworded or moved: each mock record carries the verbatim
    BR-MOCK-EXPORT-CLOCK question and this unit, and is effectively held by the package (the
    test-data hold wins)."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == (UNIT, CLOCK_QUESTION)
    assert effective_eligibility(package, family) == package.eligibility


def test_the_mock_package_is_held_as_test_data_and_the_br_package_stays_eligible() -> None:
    """Detects the mock tables publishable the moment the clock question is answered: the mock
    package is ``held`` with EAC-BR-MOCK-TEST-DATA and this unit, while the BR archive package stays
    ``eligible``."""
    packages = {p.package: p for p in load_registry().packages}
    mock = packages[MOCK_PKG].eligibility
    assert isinstance(mock, Held)
    assert (mock.unit, mock.question) == (UNIT, TEST_DATA_QUESTION)
    assert packages[BR_PKG].eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("package", [BR_PKG, MOCK_PKG])
def test_a_package_file_carries_exactly_its_four_records_and_no_ledger_entry(
    package: str,
) -> None:
    """Detects a record on the wrong family, a bespoke transformer, a changed resource disposition
    or a reconcile adjudication this unit does not need: each file holds its four families, each
    with a record and no transformer, four datastore resources all ``SILVER``, and the reconcile
    ledger names none of the eight."""
    document = _package_doc(package)
    families = [f["key"] for f in document["families"]]
    assert set(families) == {k for k in RECORDED if PACKAGE_OF[k] == package}
    assert all("record" in f and f["transformer"] is None for f in document["families"])
    resources = document["resources"]
    assert len(resources) == 4
    assert all(r["disposition"]["kind"] == "SILVER" for r in resources)
    assert {r["url_type"] for r in resources} == {"datastore"}
    assert {r["family"] for r in resources} == set(families)
    entries = registry_module.load_reconcile_adjudications()
    assert {e.family for e in entries} & set(RECORDED) == set()


def test_eight_are_reported_as_silver_outputs_four_eligible_and_four_held() -> None:
    """Detects a family reported as ingest-only or with the wrong selection or vintage: the
    eligibility report lists each of the eight as ``silver`` / ``whole_capture`` / ``none`` with the
    capture-time vintage, the four BR ones ``eligible`` and the four mock ones ``held: TODO:``."""
    report = eligibility_module.render(load_registry())
    for key in RECORDED:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver |" in line and "| whole_capture | none | gridflow capture time |" in line
        if key in ELIGIBLE:
            assert "| silver | eligible |" in line, key
        else:
            assert "| silver | held: TODO: EAC-BR-MOCK-TEST-DATA" in line, key


@pytest.mark.parametrize("key", RECORDED)
def test_every_family_is_generic_tabular_silver_with_its_own_record(key: str) -> None:
    """Detects a family wired to a bespoke transformer instead of the generic engine."""
    _package, entry = load_registry().families[key]
    assert entry.record is not None and entry.transformer is None
    assert entry.kind == "tabular"


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ALIASES)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a duplicate
    key or an excluded row: the slice loads every row with zero exclusions and no failure, every
    identifier, label, time and measure equals the body cell (a time cell byte-equal, a string),
    the entity key is unique, and ``timestamp_utc`` is the capture's ``written_at`` on every row
    (temporal ``none``)."""
    meta = CAPTURES[alias]
    frame = _load_clean(data, alias)
    _assert_cells(alias, frame)
    assert not frame.select(KEY_OF[meta.family]).is_duplicated().any()
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    for name in TIME_NAMES & set(frame.columns):
        assert frame.schema[name] == pl.String


def test_by_unit_keeps_the_quoted_empty_cells_as_published_and_the_ids_intact(
    data: Path,
) -> None:
    """Detects a quoted-empty technology type or post code filled, token-mapped or silently turned
    into a different value, and an identifier trimmed or cast: the rows with ``""`` keep the empty
    string (the reader's treatment of a quoted empty cell, as for K-TRD ``Notes``), a populated row
    keeps its text, ``unit_result_id`` is byte-equal (``#||#`` separators intact) and unique, the
    executed quantity and price are floats, and the April 2024 time stays the raw text."""
    frame = _load_clean(data, "b01")
    by_id = {r["unit_result_id"]: r for r in frame.iter_rows(named=True)}
    assert by_id["152#||#18#||#PBR#||#4827"]["post_code"] == ""
    assert by_id["152#||#18#||#PBR#||#4827"]["technology_type"] == "Batteries"
    assert by_id["163#||#320#||#PBR#||#5504"]["technology_type"] == ""
    assert by_id["163#||#320#||#PBR#||#5504"]["post_code"] == ""
    assert by_id["152#||#229#||#PBR#||#4835"]["post_code"] == "HP17 8NE"
    assert frame["unit_result_id"].to_list() == [r["unitResultID"] for r in rows("b01")]
    assert frame.schema["unit_result_id"] == pl.String
    assert (frame.schema["executed_quantity"], frame.schema["clearing_price"]) == (
        pl.Float64,
        pl.Float64,
    )
    assert "2024-04-01T16:30:00" in frame["delivery_start"].to_list()


def test_a_mock_by_unit_unquoted_blank_is_null(data: Path) -> None:
    """Detects an unquoted blank technology type or post code turned into an empty string: the mock
    by-unit rows write ``,,`` and load null (the opposite of the B01 quoted-empty cells)."""
    frame = _load_clean(data, "m01")
    assert frame["technology_type"].to_list() == [None] * frame.height
    assert frame["post_code"].to_list() == [None] * frame.height
    assert frame["executed_quantity"].to_list()[0] == 40.0


def test_buy_orders_keep_zeros_blank_reasons_and_the_fractional_ratio(data: Path) -> None:
    """Detects a zero price read as missing, a blank rejection reason filled, a coded reason cast,
    and a fractional acceptance ratio rounded: the zero ``clearingPrice`` and ratio stay ``0.0``,
    ``reasonRejected`` ``31`` is the string, a blank one is null, ``0.502500000006144`` is exact and
    the always-blank ``joinedFamily`` and ``substitutabilityFamily`` are null."""
    frame = _load_clean(data, "b02")
    first, second = frame.iter_rows(named=True)
    assert first["acceptance_ratio"] == 0.502500000006144 and first["reason_rejected"] is None
    assert second["acceptance_ratio"] == 0.0 and second["clearing_price"] == 0.0
    assert second["reason_rejected"] == "31"
    assert frame["joined_family"].to_list() == [None, None]
    assert frame["substitutability_family"].to_list() == [None, None]
    assert frame.schema["auction_id"] == frame.schema["order_id"] == pl.String
    assert frame.schema["reason_rejected"] == pl.String


def test_summary_rows_sharing_auction_and_product_are_both_kept_with_a_unique_key(
    data: Path,
) -> None:
    """Detects the summary key shortened to ``(auction_id, auction_product)`` (which repeats): the
    two rows of auction 152 / product PBR at different delivery starts both load, the key including
    ``delivery_start`` (a raw string) is unique, and the cleared volume and price are floats."""
    frame = _load_clean(data, "b03")
    assert frame.height == 2
    assert frame.select(["auction_id", "auction_product"]).is_duplicated().all()
    assert not frame.select(KEY_OF[BR_SUMMARY]).is_duplicated().any()
    assert frame.schema["delivery_start"] == pl.String
    assert frame.schema["cleared_volume"] == frame.schema["clearing_price"] == pl.Float64


def test_mock_buy_orders_keep_blank_and_populated_joined_family_as_strings(data: Path) -> None:
    """Detects the joined family inferred empty from the live BR table or cast: the blank rows are
    null, the populated ``1`` stays the string ``"1"`` and the column is a nullable string."""
    frame = _load_clean(data, "m02")
    joined = frame["joined_family"].to_list()
    assert joined.count(None) == 3 and joined.count("1") == 1
    assert frame.schema["joined_family"] == pl.String
    assert frame["reason_rejected"].to_list().count(None) == 2


def test_mock_sell_orders_keep_ids_codes_and_blank_looped_baskets(data: Path) -> None:
    """Detects a looped-basket blank filled, a product id or rejection code cast, or an order id
    cast: blank ``loopedBasketID`` is null, ``1`` and ``17`` stay strings, ``productID`` ``11`` is
    the string, ``reasonRejected`` codes are strings, quantities and limits are floats and the three
    order types survive."""
    frame = _load_clean(data, "m03")
    assert frame["looped_basket_id"].to_list() == [None, None, "1", "1", None, "17"]
    assert frame["product_id"].to_list() == ["1", "1", "1", "2", "3", "11"]
    for name in ("order_id", "auction_id", "basket_id", "product_id", "reason_rejected"):
        assert frame.schema[name] == pl.String, name
    assert set(frame["order_type"].to_list()) == {"PARENT", "CHILD", "SUBSTITUTABLECHILD"}
    assert frame.schema["quantity"] == frame.schema["price_limit"] == pl.Float64
    assert frame["reason_rejected"].to_list().count(None) == 1


def test_the_mock_summary_loads_under_its_own_family(data: Path) -> None:
    """Detects the mock summary folded into the BR summary: its own silver directory, key and rows,
    and nothing under the BR family."""
    frame = _load_clean(data, "m04")
    assert frame.height == 2
    assert _no_silver(data, BR_SUMMARY)


def test_the_live_sell_record_parses_mock_bytes_with_the_p1_header(data: Path) -> None:
    """Detects the live BR sell record unable to read a body with the P1 header (mock bytes M03,
    used only because they are the one real vendor body with that header; not BR live evidence):
    six rows load under the live resource with zero exclusions, ids are strings byte-equal to the
    body, quantity, limit, ratio, executed quantity and clearing price are floats, blank
    ``loopedBasketID`` is null, the digit ``reasonRejected`` codes stay strings, every time cell is
    byte-equal to the body, the key is unique and ``timestamp_utc`` is ``capture_written_at``."""
    frame = _load_clean(data, "s01")
    source = rows("s01")
    assert frame.height == len(source) == 6
    for column, key in (("auction_id", "auctionID"), ("order_id", "orderID")):
        assert frame[column].to_list() == [r[key] for r in source]
        assert frame.schema[column] == pl.String
    for column in (
        "quantity",
        "price_limit",
        "acceptance_ratio",
        "executed_quantity",
        "clearing_price",
    ):
        assert frame.schema[column] == pl.Float64, column
    blank_looped = sum(1 for r in source if not r["loopedBasketID"])
    assert frame["looped_basket_id"].null_count() == blank_looped == 3
    assert frame["reason_rejected"].to_list() == [r["reasonRejected"] or None for r in source]
    assert "13" in frame["reason_rejected"].to_list()
    for column, key in (
        ("delivery_start", "deliveryStart"),
        ("delivery_end", "deliveryEnd"),
        ("order_entry_time", "orderEntryTime"),
    ):
        assert frame[column].to_list() == [r[key] for r in source]
    assert not frame.select(KEY_OF[BR_SELL]).is_duplicated().any()
    assert set(frame["timestamp_utc"].to_list()) == {
        datetime.fromisoformat(CAPTURES["s01"].written)
    }


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


def test_the_22_column_eac_sell_header_fails_loud_against_the_br_sell_record(data: Path) -> None:
    """Detects the newer EAC 22-column header (H-O22, ``flexibleGroupID`` and
    ``minimumConsecutiveWindows`` appended) loading against the 20-column BR record, which would
    mean a drifted BR dump is read under a stale freeze: it matches no epoch, so the capture fails
    and writes nothing."""
    head, rest = body("s01").split(b"\n", 1)
    lines = [ln + b",," for ln in rest.split(b"\n") if ln]
    widened = head + b",flexibleGroupID,minimumConsecutiveWindows\n" + b"\n".join(lines) + b"\n"
    assert widened.split(b"\n", 1)[0].decode().split(",") == H_O22
    capture_id = capture(data, "s01", raw=widened)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, BR_SELL, data).run(DAY, run_id="r")
    assert read_completion(data, BR_SELL, capture_id) is None
    assert _no_silver(data, BR_SELL)


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


@pytest.mark.parametrize("alias", ALIASES)
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


@pytest.mark.parametrize("key", RECORDED)
def test_the_skeleton_page_renders_the_new_records(key: str) -> None:
    """Detects a record the docs generator cannot render (the held questions, the em and en dashes,
    the raised cap)."""
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
