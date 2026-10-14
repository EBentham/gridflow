"""FFR post-tender CSV reports and the Phase-2 FFR auction tables: seven frozen records, five
eligible and two held, plus two resource-level holds (v0.22-K-AS-5a).

Two package files carry the records: ``firm-frequency-response-post-tender-reports`` (one record,
``ffr_post_tender_reports_csv_ext``, over the three naming families ``_csv``, ``_csv_ext`` and
``_ext``) and ``phase-2-ffr-auction-results-summary`` (weekly and masterdata order tables,
linear tables, results by unit and results summary). Every test writes recorded fixture captures
(cuts of the 2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/as5a/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation.

Record decisions under test (K-AS-5-FACTS, K-AS-5a-SPEC):

- ``issue: none`` everywhere and ``vintage: ckan_last_modified`` (every resource is an upload).
  ``OrderEntryTime`` is bid submission, not publication.
- Opaque identifiers (tender references, order / period / trade ids including the masters'
  ``1E+14`` spellings, status and flag columns) are strings, byte-equal, never cast. Every clock or
  date-time cell whose export clock is undocumented is a raw string: the FFR ``Start Date`` /
  ``End Date`` (calendar dates and Excel serials mixed) and recurring window labels, and the
  phase-2 ``OrderEntryTime`` / ``DeliveryStart`` / ``DeliveryEnd`` / ``Delivery Start`` /
  ``Delivery End``. Only the result ``EFA Date`` is typed (``date``, one format per filename).
- No trim, token map or bound. Literal ``NA`` / ``N/A`` / ``-`` stay byte-equal in their string
  columns; a single-space cell stays a single space; zeros stay zero; negative block volumes stay
  negative (sell convention).
- The FFR tables are one record under the owner ``_csv_ext`` with the two sibling families; TR147
  (one ``1,366.61`` availability fee) and TR154 (ten rows with no Tender Ref) are resource HOLDs,
  not ADR-040 adjudications.
- The two order tables and the linear table each keep weekly and master exports as separate
  records. Results by unit and results summary consolidate the weekly export with the three
  masters through ``siblings`` and ``formats_by_filename`` and resolve overlap with ``key_latest``.
- EFA is the weekly ordinal 1..42 (``int64``), never composed with a date (RULINGS 572 / 588).
"""

from __future__ import annotations

import csv
import io
import json
import tempfile
from collections import Counter
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as5a"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
LONDON = ZoneInfo("Europe/London")
UNIT = "v0.22-K-AS-5a"

FFR_PACKAGE = "firm-frequency-response-post-tender-reports"
FFR_PACKAGE_ID = "1162a519-b66d-48f6-96ae-1a69ff17951f"
P2_PACKAGE = "phase-2-ffr-auction-results-summary"
P2_PACKAGE_ID = "2d649d03-fb37-46a2-ae82-9e651438b559"

P2 = "ffr_phase2_auction_phase_"
FFR_CSV = "ffr_post_tender_reports_csv"
FFR = "ffr_post_tender_reports_csv_ext"
FFR_EXT = "ffr_post_tender_reports_ext"
BW = P2 + "block_order"
BM = P2 + "blockorders"
LW = P2 + "linear_orders"
LM = P2 + "linearorders"
UN_WEEKLY = P2 + "results_unit"
UN = P2 + "resultsbyunit"
SM_WEEKLY = P2 + "results_sum"
SM = P2 + "resultsum"
RECORDED = (FFR, BW, BM, LW, LM, UN, SM)
ELIGIBLE = (FFR, BW, LW, LM, UN)
HELD = (BM, SM)
SIBLINGS_OF = {FFR: (FFR_CSV, FFR_EXT), UN: (UN_WEEKLY,), SM: (SM_WEEKLY,)}
OWNER_OF = {FFR_CSV: FFR, FFR_EXT: FFR, UN_WEEKLY: UN, SM_WEEKLY: SM}
PACKAGE_OF = {
    **dict.fromkeys((FFR_CSV, FFR, FFR_EXT), FFR_PACKAGE),
    **dict.fromkeys((BW, BM, LW, LM, UN_WEEKLY, UN, SM_WEEKLY, SM), P2_PACKAGE),
}
PACKAGE_ID_OF = {FFR_PACKAGE: FFR_PACKAGE_ID, P2_PACKAGE: P2_PACKAGE_ID}
ALL_FAMILIES = (*RECORDED, *OWNER_OF)

TR147_ID = "c99081fb-0211-4a1a-9eed-53497f64a6ea"
TR154_ID = "7e48c1ce-b4cf-43fa-afe9-4e6730474944"
ARCHIVE = "ffr_phase2_result_summary_archive"
AS5C_RECORDED = (
    "ffr_post_tender_reports_workbook",
    "ffr_phase2_blockorders_archive",
    "ffr_phase2_linearorders_archive",
    "ffr_phase2_results_by_unit_archive",
)

# --------------------------------------------------------------------------- #
# The spec's silver names and types, positional against each table's vendor header. The key is
# a kind code: s string, f float64, i int64, d date.
# --------------------------------------------------------------------------- #

FFR_COLUMNS: list[tuple[str, str]] = [
    ("tender_ref", "s"),
    ("status", "s"),
    ("rejection_codes", "s"),
    ("company_name", "s"),
    ("tendered_unit", "s"),
    ("connection_type", "s"),
    ("generation_type", "s"),
    ("start_date", "s"),
    ("end_date", "s"),
    ("tendered_period", "s"),
    ("capability_data_table", "s"),
    ("frame_from_mon_fri", "s"),
    ("frame_to_mon_fri", "s"),
    ("frame_duration_mon_fri", "f"),
    ("frame_from_sat", "s"),
    ("frame_to_sat", "s"),
    ("frame_duration_sat", "f"),
    ("frame_from_sun_bh", "s"),
    ("frame_to_sun_bh", "s"),
    ("frame_duration_sun_bh", "f"),
    ("availability_fee", "f"),
    ("nomination_fee", "s"),
    ("window_initiation_fee", "s"),
    ("bmus_only", "s"),
    ("max_part_load_point", "s"),
    ("min_part_load_point", "s"),
    ("min_mel", "s"),
    ("max_sel", "s"),
    ("primary_response_0_2hz", "f"),
    ("primary_response_0_5hz", "f"),
    ("primary_response_0_8hz", "f"),
    ("secondary_response_0_2hz", "f"),
    ("secondary_response_0_5hz", "f"),
    ("high_frequency_response_0_2hz", "f"),
    ("high_frequency_response_0_5hz", "f"),
    ("detail_change_volume", "s"),
    ("stacked_onto_tender", "s"),
    ("all_or_nothing_bid", "s"),
    ("mutually_exclusive_tender", "s"),
    ("auto_response_10s", "s"),
    ("auto_response_30s", "s"),
    ("post_code", "s"),
    ("comments", "s"),
]
ORDER_HEAD: list[tuple[str, str]] = [
    ("market_name", "s"),
    ("bidding_level_name", "s"),
    ("member_name", "s"),
    ("order_id", "s"),
    ("portfolio", "s"),
    ("order_entry_time", "s"),
    ("order_entry_user", "s"),
    ("settlement_currency", "s"),
]
B24_COLUMNS = [
    *ORDER_HEAD,
    ("clearing_price", "f"),
    ("price", "f"),
    ("mar", "s"),
    ("status", "s"),
    ("block_code", "s"),
    ("block_code_prm", "s"),
    ("paradoxically", "s"),
    ("order_period_id", "s"),
    ("trade_id", "s"),
    ("efa", "i"),
    ("delivery_start", "s"),
    ("delivery_end", "s"),
    ("executed_volume", "f"),
    ("volume", "f"),
    ("invalid", "s"),
    ("bid_validity", "s"),
]
L22_COLUMNS = [
    *ORDER_HEAD,
    ("order_period_id", "s"),
    ("trade_id", "s"),
    ("efa", "i"),
    ("delivery_start", "s"),
    ("delivery_end", "s"),
    ("executed_volume", "f"),
    ("p1", "f"),
    ("v1", "f"),
    ("p2", "f"),
    ("v2", "f"),
    ("p3", "s"),
    ("v3", "s"),
    ("p4", "s"),
    ("v4", "s"),
]
U10_COLUMNS = [
    ("company", "s"),
    ("unit_name", "s"),
    ("efa_date", "d"),
    ("delivery_start", "s"),
    ("delivery_end", "s"),
    ("efa", "i"),
    ("service", "s"),
    ("cleared_volume", "f"),
    ("clearing_price", "f"),
    ("cancelled", "s"),
]
S7_COLUMNS = [
    ("service", "s"),
    ("efa_date", "d"),
    ("delivery_start", "s"),
    ("delivery_end", "s"),
    ("efa", "i"),
    ("cleared_volume", "f"),
    ("clearing_price", "f"),
]
COLUMNS_OF: dict[str, list[tuple[str, str]]] = {
    FFR: FFR_COLUMNS,
    BW: B24_COLUMNS,
    BM: B24_COLUMNS,
    LW: L22_COLUMNS,
    LM: L22_COLUMNS,
    UN: U10_COLUMNS,
    SM: S7_COLUMNS,
}
B24_NAMES = [n for n, _k in B24_COLUMNS]
KEY_OF: dict[str, list[str]] = {
    FFR: ["resource_id", "tender_ref"],
    BW: ["order_id"],
    BM: ["resource_id", *B24_NAMES],
    LW: ["order_period_id"],
    LM: ["resource_id", "market_name", "delivery_start"],
    UN: ["unit_name", "efa_date", "efa", "service"],
    SM: ["service", "efa_date", "efa"],
}
NON_NULL_OF: dict[str, set[str]] = {
    FFR: {"tender_ref"},
    BW: {"order_id"},
    BM: set(B24_NAMES) - {"bid_validity"},
    LW: {"order_period_id"},
    LM: {"market_name", "delivery_start"},
    UN: {"unit_name", "efa_date", "efa", "service"},
    SM: {"service", "efa_date", "efa"},
}
PARTITIONED = {FFR, BM, LM}
KEY_LATEST = {UN, SM}
DATE_SP1 = {UN, SM}
TIME_NAMES = {"delivery_start", "delivery_end", "order_entry_time"}

BYUNIT_FORMATS = (
    ("ngeso_fra-weekly_2021-11-26_resultsbyunit.csv", "%Y-%m-%d"),
    ("phase-2-ffr-auction-resultsbyunit-masterdata-2019-2020.csv", "%d/%m/%Y"),
    ("phase-2-ffr-auction-resultsbyunit-masterdata-2020-2021.csv", "%d/%m/%Y"),
    ("phase-2-ffr-auction-resultsbyunit-masterdata-2021-2022.csv", "%d/%m/%Y"),
)
SUMMARY_FORMATS = (
    ("ngeso_fra-weekly_2021-11-26_resultsummary.csv", "%Y-%m-%d"),
    ("phase-2-ffr-auction-resultsummary-masterdata-2019-2020.csv", "%d/%m/%Y"),
    ("phase-2-ffr-auction-resultsummary-masterdata-2020-2021.csv", "%d/%m/%Y"),
    ("phase-2-ffr-auction-resultsummary-masterdata-2021-2022.csv", "%d/%m/%Y"),
)
FORMATS_OF = {UN: BYUNIT_FORMATS, SM: SUMMARY_FORMATS}
DATE_FORMAT_OF = {
    "uw": "%Y-%m-%d",
    "um9": "%d/%m/%Y",
    "um0": "%d/%m/%Y",
    "um1": "%d/%m/%Y",
    "sw": "%Y-%m-%d",
    "sm9": "%d/%m/%Y",
    "sm0": "%d/%m/%Y",
    "sm1": "%d/%m/%Y",
}

# The held questions and the two resource-HOLD reasons, verbatim (spec).
QUESTION_OF = {
    BM: (
        "TODO: PHASE2-MASTER-ORDER-ID — the three Phase-2 FFR BlockOrders masterdata exports write "
        "OrderID, OrderPeriodID and TradeID in spreadsheet scientific notation (1E+14, 1.00E+14), "
        "so no row keeps a recoverable order identity and the record's key is the complete row; "
        "can NESO republish the masters with full identifiers? (The 2019-2020 master dictionary "
        "and the trial evaluation describe hourly capacity prices while the newest dictionary "
        "labels prices £/MW; delivery and entry times are naive with an undocumented export "
        "clock.)"
    ),
    SM: (
        "TODO: PHASE2-SUMMARY-PRICE-UNIT — the weekly results-summary dictionary gives Clearing "
        "Price the unit £/MW while describing it as the cleared price per MW per hour, and the "
        'masterdata dictionary gives it no unit ("number"); the by-unit results tables state '
        "£/MW/h. Which rate dimension does the summary export publish? (Delivery start / end "
        "cells are naive with an undocumented export clock.)"
    ),
}
HOLD_REASON_OF = {
    TR147_ID: (
        "TODO: FFR-TR147-THOUSANDS-SEPARATOR — the TR147 (March 2022) CSV EXT writes one "
        "availability fee as `1,366.61`, an internal thousands separator that fails the record's "
        "strict float64 cast and that the lexical trim does not remove (FACTS l.256); "
        "re-disposition only after a declared lexical-number mechanism or a corrected NESO upload "
        "(a new last_modified)"
    ),
    TR154_ID: (
        "TODO: FFR-TR154-ORPHAN-ROWS — the TR154 (October 2022) CSV EXT carries 10 nonblank rows "
        "with no Tender Ref (FACTS l.219, l.259), so the (resource_id, tender_ref) key cannot "
        "hold every row and a non-nullable key would silently exclude them; are they "
        "continuations of another tender or a publication defect NESO can correct?"
    ),
}


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
    "r144": Capture(
        FFR_CSV,
        "ceefff8c-7f61-41dd-b567-42774ca487bc",
        "Post Tender Report TR144 December 2021 CSV",
        "tr144-post-tender-reports_v11.csv",
        "2021-12-16T16:31:59.547797",
        "2026-10-08T10:59:31.598301+00:00",
    ),
    "r139": Capture(
        FFR,
        "9347c45d-bffd-40ee-aa1e-593b41bde9a4",
        "Post Tender Report TR139 July 2021 CSV- EXT ",
        "tr139-post-tender-reports_v1-1.csv",
        "2021-07-22T11:52:10.923126",
        "2026-10-08T10:59:35.183798+00:00",
    ),
    "r142": Capture(
        FFR_EXT,
        "1882b277-6349-4513-9939-b39e1992620b",
        "Post Tender Report TR142 October 2021 EXT ",
        "tr142-post-tender-reports_v1-1.csv",
        "2021-10-18T16:09:22.173955",
        "2026-10-08T11:00:44.668464+00:00",
    ),
    "r146": Capture(
        FFR,
        "66e127dc-5e95-49b5-bc55-915269d09e1a",
        "Post Tender Report TR146 February 2022 CSV- EXT ",
        "tr146-post-tender-reports_v11-csv-corrected-tender-ref.csv",
        "2022-04-26T14:52:49.241569",
        "2026-10-08T10:59:45.501580+00:00",
    ),
    "r147": Capture(
        FFR,
        "c99081fb-0211-4a1a-9eed-53497f64a6ea",
        "Post Tender Report TR147 March 2022 CSV EXT ",
        "tr147-post-tender-reports-ext.csv",
        "2022-03-16T16:39:04.706143",
        "2026-10-08T10:59:47.946404+00:00",
    ),
    "r148": Capture(
        FFR,
        "fc994d78-f7d0-4a29-9ea7-31121931d04c",
        "Post Tender Report TR148 April 2022 CSV EXT ",
        "tr148-post-tender-reports_v11ext.csv",
        "2022-04-20T13:01:32.640805",
        "2026-10-08T10:59:50.576877+00:00",
    ),
    "r163": Capture(
        FFR,
        "fbb1ce86-b2f7-405d-94ac-6cef6f14cceb",
        "Post Tender Report TR163 July 2023 CSV EXT ",
        "tr163-post-tender-reports_v11ext.csv",
        "2023-07-18T09:12:57.073538",
        "2026-10-08T11:00:32.494150+00:00",
    ),
    "r164": Capture(
        FFR,
        "2bfa2ab3-7f4e-44d7-a9ab-7342c4b9cd34",
        "Post Tender Report TR164 August 2023 CSV EXT ",
        "tr164-post-tender-reports_v11ext.csv",
        "2023-08-11T14:16:07.574193",
        "2026-10-08T11:00:34.929954+00:00",
    ),
    "bw": Capture(
        BW,
        "edfb5b8f-4415-45e6-b378-57020c41e96b",
        "Phase 2: FFR Auction Block Order 26.11.2021",
        "ngeso_fra-weekly_2021-11-26_blockorders.csv",
        "2021-11-26T11:15:54.702162",
        "2026-10-08T10:58:37.000878+00:00",
    ),
    "bm9": Capture(
        BM,
        "979329c3-5587-4b08-a900-5520b58b2d8f",
        "Phase 2 FFR Auction BlockOrders Masterdata 2019-2020",
        "phase-2-ffr-auction-blockorders-masterdata-2019-2020.csv",
        "2021-03-31T09:28:52.708950",
        "2026-10-08T10:58:40.780562+00:00",
    ),
    "bm0": Capture(
        BM,
        "c074d827-ad03-4b11-b22e-8bbc96be3bab",
        "Phase 2 FFR Auction BlockOrders Masterdata 2020-2021",
        "phase-2-ffr-auction-blockorders-masterdata-2020-2021.csv",
        "2022-03-18T12:08:37.494234",
        "2026-10-08T10:58:43.329082+00:00",
    ),
    "bm1": Capture(
        BM,
        "89467af0-a577-4bba-9955-f8c380a78923",
        "Phase 2 FFR Auction BlockOrders Masterdata 2021-2022",
        "phase-2-ffr-auction-blockorders-masterdata-2021-2022.csv",
        "2021-11-26T11:18:31.140573",
        "2026-10-08T10:58:46.830914+00:00",
    ),
    "lw": Capture(
        LW,
        "5cce4cb6-c270-488f-96c7-9f5cd7476898",
        "Phase 2: FFR Auction Linear Orders 26.11.2021",
        "ngeso_fra-weekly_2021-11-26_linearorders.csv",
        "2021-11-26T11:16:28.900983",
        "2026-10-08T10:58:50.696009+00:00",
    ),
    "lm9": Capture(
        LM,
        "47ceb316-e284-4e14-bb67-76067a8f9562",
        "Phase 2 FFR Auction LinearOrders Masterdata-2019-2020",
        "phase-2-ffr-auction-linearorders-masterdata-2019-2020.csv",
        "2021-03-31T09:29:46.319416",
        "2026-10-08T10:58:54.907692+00:00",
    ),
    "lm1": Capture(
        LM,
        "eb394ce7-20c6-4434-a263-470b2896fd7b",
        "Phase 2 FFR Auction LinearOrders Masterdata 2021-2022",
        "phase-2-ffr-auction-linearorders-masterdata-2021-2022.csv",
        "2021-11-26T11:19:09.079965",
        "2026-10-08T10:59:00.890809+00:00",
    ),
    "uw": Capture(
        UN_WEEKLY,
        "87ee8fc4-11ae-4575-b734-99548aab2d98",
        "Phase 2: FFR Auction Results by Unit 26.11.2021",
        "ngeso_fra-weekly_2021-11-26_resultsbyunit.csv",
        "2021-11-26T11:17:15.657545",
        "2026-10-08T10:59:09.010145+00:00",
    ),
    "um9": Capture(
        UN,
        "15d7fa42-1c8d-4a79-86f8-890cf9228794",
        "Phase 2 FFR Auction ResultsByUnit Masterdata-2019-2020",
        "phase-2-ffr-auction-resultsbyunit-masterdata-2019-2020.csv",
        "2021-03-31T09:30:53.612085",
        "2026-10-08T10:59:13.120930+00:00",
    ),
    "um0": Capture(
        UN,
        "52faffc9-e804-44d8-b222-dd0821d79e7d",
        "Phase 2 FFR Auction ResultsByUnit Masterdata 2020-2021",
        "phase-2-ffr-auction-resultsbyunit-masterdata-2020-2021.csv",
        "2021-04-01T10:32:40.269524",
        "2026-10-08T10:59:15.916183+00:00",
    ),
    "um1": Capture(
        UN,
        "feafb7ab-7db6-406e-a46f-bbcf6ed92ba1",
        "Phase 2 FFR Auction ResultsByUnit Masterdata 2021-2022",
        "phase-2-ffr-auction-resultsbyunit-masterdata-2021-2022.csv",
        "2021-11-26T11:20:28.830268",
        "2026-10-08T10:59:18.920386+00:00",
    ),
    "sw": Capture(
        SM_WEEKLY,
        "a9659701-f0d4-40d2-91f1-31f4bac0619d",
        "Phase 2: FFR Auction Results Summary 26.11.2021",
        "ngeso_fra-weekly_2021-11-26_resultsummary.csv",
        "2021-11-26T11:17:51.159026",
        "2026-10-08T10:59:04.884807+00:00",
    ),
    "sm9": Capture(
        SM,
        "8b5fa222-f39f-4eea-9475-728ea8365ff3",
        "Phase 2 FFR Auction ResultSummary Masterdata-2019-2020",
        "phase-2-ffr-auction-resultsummary-masterdata-2019-2020.csv",
        "2021-03-31T09:44:47.928290",
        "2026-10-08T10:59:22.589440+00:00",
    ),
    "sm0": Capture(
        SM,
        "279572a9-21c9-4e1e-a7c8-7aed233f8936",
        "Phase 2 FFR Auction ResultSummary Masterdata 2020-2021",
        "phase-2-ffr-auction-resultsummary-masterdata-2020-2021.csv",
        "2021-04-01T10:32:10.734130",
        "2026-10-08T10:59:25.051652+00:00",
    ),
    "sm1": Capture(
        SM,
        "920105e5-a5e3-4bed-83ce-ab6901cc1b84",
        "Phase 2 FFR Auction ResultSummary Masterdata 2021-2022",
        "phase-2-ffr-auction-resultsummary-masterdata-2021-2022.csv",
        "2021-11-26T11:20:55.997220",
        "2026-10-08T10:59:27.850297+00:00",
    ),
}


FIRST_OF = {FFR: "r144", BW: "bw", BM: "bm9", LW: "lw", LM: "lm9", UN: "um9", SM: "sm9"}
LOADING = [a for a in CAPTURES if a != "r147"]
BOM_ALIASES = {a for a in CAPTURES if a.startswith("r")}


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
        prefix="as5a", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: the bronze bytes)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def _text(raw: bytes) -> str:
    return raw.decode("utf-8").removeprefix("﻿")


def _cells(raw: bytes) -> list[list[str]]:
    """Every CSV record of ``raw`` as text, header first (quoted embedded newlines kept)."""
    return list(csv.reader(io.StringIO(_text(raw), newline="")))


def header(alias: str, raw: bytes | None = None) -> list[str]:
    """The fixture's header the way the production reader returns it (BOM removed, edge
    whitespace stripped, inner spacing and embedded newlines kept)."""
    return [c.strip() for c in _cells(raw if raw is not None else body(alias))[0]]


def rows(alias: str, raw: bytes | None = None, *, keep_blank: bool = False) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim); wholly blank rows only on
    request."""
    cells = _cells(raw if raw is not None else body(alias))
    head = [c.strip() for c in cells[0]]
    return [dict(zip(head, r, strict=True)) for r in cells[1:] if keep_blank or any(r)]


def capture(
    data: Path,
    alias: str,
    *,
    raw: bytes | None = None,
    resource_id: str | None = None,
    filename: str | None = None,
) -> str:
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
        resource_id=resource_id or meta.resource_id,
        resource_name=meta.name,
        resource_filename=filename or meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
        ckan_format="CSV",
        extension="csv",
    )
    return capture_id_for(path, data)


def owner(alias: str) -> str:
    """The recorded family that transforms ``alias``'s capture."""
    family = CAPTURES[alias].family
    return OWNER_OF.get(family, family)


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


def _spec_for(alias: str) -> list[tuple[str, str, str]]:
    """``(source, silver name, kind)`` per header cell of ``alias``, from the spec tables."""
    key = owner(alias)
    if key == FFR:
        by_source = dict(zip(header("r144"), FFR_COLUMNS, strict=True))
        return [(s, *by_source[s]) for s in header(alias)]
    return [(s, n, k) for s, (n, k) in zip(header(alias), COLUMNS_OF[key], strict=True)]


HEADERS_OF: dict[str, list[list[str]]] = {
    FFR: [header("r144"), header("r139")],
    BW: [header("bw")],
    BM: [header("bm9")],
    LW: [header("lw")],
    LM: [header("lm9")],
    UN: [header("um9")],
    SM: [header("sm9")],
}


def _expected(alias: str, kind: str, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank cell is null; text is kept
    byte-equal, a lone space included)."""
    if cell == "":
        return None
    if kind == "f":
        return float(cell)
    if kind == "i":
        return int(cell)
    if kind == "d":
        return datetime.strptime(cell, DATE_FORMAT_OF[alias]).date()
    return cell


def _of_capture(alias: str, frame: pl.DataFrame) -> pl.DataFrame:
    written = datetime.fromisoformat(CAPTURES[alias].written).astimezone(UTC)
    return frame.filter(pl.col("capture_written_at") == written)


def _assert_cells(alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, date and measure of the ``alias`` rows equals the body cell."""
    mine = _of_capture(alias, frame)
    source_rows = rows(alias)
    assert mine.height == len(source_rows), alias
    for source, name, kind in _spec_for(alias):
        assert mine[name].to_list() == [_expected(alias, kind, r[source]) for r in source_rows], (
            alias,
            source,
        )


def _anchor(day: date) -> datetime:
    """The UTC instant of local midnight on ``day`` (the label anchor ``date_sp1``)."""
    return datetime(day.year, day.month, day.day, tzinfo=LONDON).astimezone(UTC)


def _load_clean(data: Path, *aliases: str) -> pl.DataFrame:
    """Capture and run the aliases of one record: every row written, none excluded, the generic
    output columns, one completion per capture, the temporal column tz-aware UTC."""
    (key,) = {owner(a) for a in aliases}
    capture_ids = {a: capture(data, a) for a in aliases}
    written = get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    assert written == sum(len(rows(a)) for a in aliases)
    for alias, capture_id in capture_ids.items():
        completion = read_completion(data, key, capture_id)
        assert completion is not None, alias
        assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
        assert read_failure(data, key, capture_id) is None
    frame = _silver(data, key)
    assert frame.height == written
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(key)
    assert frame.schema["timestamp_utc"] == pl.Datetime("us", "UTC")
    return frame


def _fail(
    data: Path, alias: str, *, raw: bytes | None = None, **extra: str
) -> tuple[str, str, str]:
    """Run the record of ``alias`` on a capture that must fail: ``(capture id, error class,
    message)``; nothing is loaded and no completion is written."""
    key = owner(alias)
    capture_id = capture(data, alias, raw=raw, **extra)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    ((got_id, cls, message),) = info.value.failures
    assert got_id == capture_id
    assert read_completion(data, key, capture_id) is None
    assert read_failure(data, key, capture_id) is not None
    assert _no_silver(data, key)
    return got_id, cls, message


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the exact vendor headers
    (CRLF terminators on every body; a UTF-8 BOM and lone LFs inside quoted cells on the FFR bodies
    only, whose header cells hold an embedded newline), the F42 header without ``Post code`` on
    TR139, a TR144 slash date and Excel serial, its embedded-newline post code, its single-space
    automatic
    response cell and ``-`` nomination fee, TR139 prose, TR146's ``1``, TR147's ``1,366.61``,
    the wholly blank rows of TR148 and TR163, ``162.198`` in TR163 and TR164, a ``NA`` block code
    PRM and negative volume, ``1E+14`` ids and the three ``Bid Validity`` rows of the 2019-2020
    master, the weekly linear ``NA`` curve rows, and the keys the weekly and master results
    share."""
    assert set(FIRST_OF.values()) <= set(CAPTURES)
    for alias in CAPTURES:
        raw = body(alias)
        assert raw.startswith(b"\xef\xbb\xbf") == (alias in BOM_ALIASES), alias
        assert raw.endswith(b"\n"), alias
        raw.decode("utf-8")
        lone_lf = raw.count(b"\n") - raw.count(b"\r\n")
        assert (lone_lf > 0) == (alias in BOM_ALIASES), alias
    f43 = header("r144")
    assert len(f43) == 43 == len(FFR_COLUMNS)
    assert "Tendered Unit \n(BMU/Unit ID)" in f43 and "Post code" in f43
    assert "Tendered Prices -  Window Initiation Fee (£/window)" in f43
    assert header("r139") == [c for c in f43 if c != "Post code"]
    for alias in ("r142", "r146", "r147", "r148", "r163", "r164"):
        assert header(alias) == f43, alias
    assert len(header("bw")) == len(header("bm9")) == 24 == len(B24_COLUMNS)
    assert len(header("lw")) == len(header("lm9")) == 22 == len(L22_COLUMNS)
    assert len(header("um9")) == 10 and len(header("sm9")) == 7
    for alias in ("bm0", "bm1"):
        assert header(alias) == header("bm9")
    assert header("lm1") == header("lm9") and header("uw") == header("um1") == header("um9")
    assert header("sw") == header("sm1") == header("sm9")
    r144 = rows("r144")
    assert {"31/12/2021", "44562"} <= {r["Start Date"] for r in r144}
    assert any("\n" in r["Post code"] for r in r144)
    assert any(r["Automatic Response Energy Deliverable by 10s (MW)"] == " " for r in r144)
    assert any(r["Tendered Prices - Nomination Fee (£/h)"] == "-" for r in r144)
    assert any(
        r["Automatic Response Energy Deliverable by 30s (MW)"] == "H for 30 minutes only"
        for r in rows("r139")
    )
    assert [r["Automatic Response Energy Deliverable by 10s (MW)"] for r in rows("r146")] == ["1"]
    assert [r["Tendered Prices - Availability Fee (£/h)"] for r in rows("r147")][-1] == "1,366.61"
    for alias in ("r148", "r163"):
        assert len(rows(alias, keep_blank=True)) == len(rows(alias)) + 1, alias
    assert [r["Tender Ref"] for r in rows("r163")][-1] == "162.198" == rows("r164")[0]["Tender Ref"]
    bw = rows("bw")
    assert {r["BlockCodePRM"] for r in bw} == {"NA"} and all(
        r["Volume"].startswith("-") for r in bw
    )
    bm9 = rows("bm9")
    assert {r["OrderID"] for r in bm9} == {"1E+14"}
    assert [r["Bid Validity"] for r in bm9] == [
        "",
        "",
        "Offered volume is over tested volume",
        "Offered volume is over tested volume",
    ]
    assert {r["3P"] for r in rows("lw")[-2:]} == {"NA"} and rows("lw")[0]["3P"] != "NA"
    assert [r["Cancelled"] for r in rows("um1")][-1].startswith("Executed volume")
    weekly, master = rows("uw"), rows("um1")
    assert [(r["Unit Name"], r["EFA"], r["Service"]) for r in weekly] == [
        (r["Unit Name"], r["EFA"], r["Service"]) for r in master[:2]
    ]
    assert [r["EFA Date"] for r in weekly] == ["2021-11-27"] * 2
    assert [r["EFA Date"] for r in master[:2]] == ["27/11/2021"] * 2


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_selection_vintage_and_no_issue(
    key: str,
) -> None:
    """Detects a record that invents an issue time (neither family carries a publication column and
    ``OrderEntryTime`` is a bid submission), a ``run_type`` (no settlement run), a wrong selection
    (``key_latest`` only for the two results records, whole capture partitioned by resource for
    the FFR record and the two master tables), a missing sibling feed, an extra recipe or the
    wrong vintage: every record is version 1, utf-8 CSV, the spec's entity key, CKAN
    last-modified vintage, the vendor header(s) as its epochs and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.latest == ("key_latest" if key in KEY_LATEST else "whole_capture")
    assert record.latest_partition == ("resource_id" if key in PARTITIONED else None)
    assert record.vintage == "ckan_last_modified" and record.vintage_evidence is None
    assert list(record.entity_key) == KEY_OF[key]
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == SIBLINGS_OF.get(key, ())
    assert record.xlsx is None and record.edition_by_filename is None and record.zip_member is None
    assert [list(e.header) for e in record.epochs] == HEADERS_OF[key]
    for epoch in record.epochs:
        assert epoch.issue.kind == "none" and epoch.unpivot is None
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)


def test_the_ffr_epochs_are_the_exact_reader_headers_f43_and_the_post_code_less_f42() -> None:
    """Detects an epoch retyped rather than read from the bodies (its embedded newlines, double
    spaces and ``£`` signs) or a third epoch: the FFR record has F43 (TR144 and every later
    resource, TR142, TR143) and F42 (TR139: F43 without ``Post code``), and every fixture header
    equals one of them."""
    epochs = [list(e.header) for e in _record(FFR).epochs]
    assert epochs == [header("r144"), header("r139")]
    assert len(epochs[0]) == 43 and len(epochs[1]) == 42
    assert epochs[0][4] == "Tendered Unit \n(BMU/Unit ID)"
    assert epochs[0][9] == "Tendered Period \n(dd.mm.yy - dd.mm.yy)"
    assert epochs[0][21] == "Tendered Prices - Nomination Fee (£/h)"
    assert epochs[0][22] == "Tendered Prices -  Window Initiation Fee (£/window)"
    assert epochs[0][23] == "BMUs  Only"
    assert epochs[0][5] == "TO connection  /  DNO connection"
    for alias in ("r142", "r146", "r147", "r148", "r163", "r164"):
        assert header(alias) in epochs, alias


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: opaque ids, codes,
    flags, raw clocks and every cell that publishes a non-numeric token (``-``, ``NA``, prose, a
    lone space) are strings, measured values are float64, ``EFA`` is int64, only the result
    ``EFA Date`` is a date, key columns and temporal inputs are ``nullable: false`` and nothing
    else is."""
    record = _record(key)
    for epoch, expected_header in zip(record.epochs, HEADERS_OF[key], strict=True):
        assert [c.source for c in epoch.columns] == expected_header
        for column in epoch.columns:
            if key == FFR:
                name, kind = dict(zip(HEADERS_OF[FFR][0], FFR_COLUMNS, strict=True))[column.source]
            else:
                name, kind = dict(zip(expected_header, COLUMNS_OF[key], strict=True))[column.source]
            assert column.name == name
            assert column.trim is None and column.null_tokens == ()
            assert (column.min, column.max) == (None, None), name
            assert column.zone is None and column.zone_evidence is None and column.ambiguous is None
            assert column.nullable == (name not in NON_NULL_OF[key]), name
            if kind == "d":
                assert column.dtype == "date" and column.format is None
                assert column.formats_by_filename == FORMATS_OF[key], name
            else:
                assert column.format is None and column.formats_by_filename is None
                assert column.dtype == {"f": "float64", "i": "int64", "s": "string"}[kind], name


def test_the_typing_rule_keeps_every_raw_clock_and_every_token_column_a_string() -> None:
    """Detects an FFR date or window clock typed (calendar dates and Excel serials are mixed in
    ``Start Date`` / ``End Date``; the ``From`` / ``To`` labels are recurring clocks), the fee and
    load-point columns that publish ``-`` typed as numbers, ``auto_response_10s`` typed (TR144 and
    TR145 publish a lone space, TR146 a ``1``; seat amendment, RULINGS 773), ``auto_response_30s``
    (TR139 prose), the linear ``p3``..``v4`` columns (literal ``NA``), ``mar`` or any phase-2
    clock cell typed."""
    types = {c.name: c.dtype for c in _record(FFR).epochs[0].columns}
    strings = [
        "start_date",
        "end_date",
        "tendered_period",
        "frame_from_mon_fri",
        "frame_to_mon_fri",
        "frame_from_sat",
        "frame_to_sat",
        "frame_from_sun_bh",
        "frame_to_sun_bh",
        "nomination_fee",
        "window_initiation_fee",
        "max_part_load_point",
        "min_part_load_point",
        "min_mel",
        "max_sel",
        "auto_response_10s",
        "auto_response_30s",
        "post_code",
        "comments",
        "tender_ref",
        "rejection_codes",
        "status",
    ]
    assert {types[n] for n in strings} == {"string"}
    floats = [
        "frame_duration_mon_fri",
        "frame_duration_sat",
        "frame_duration_sun_bh",
        "availability_fee",
        "primary_response_0_2hz",
        "primary_response_0_5hz",
        "primary_response_0_8hz",
        "secondary_response_0_2hz",
        "secondary_response_0_5hz",
        "high_frequency_response_0_2hz",
        "high_frequency_response_0_5hz",
    ]
    assert {types[n] for n in floats} == {"float64"}
    for key in (LW, LM):
        linear = {c.name: c.dtype for c in _record(key).epochs[0].columns}
        assert {linear[n] for n in ("p3", "v3", "p4", "v4")} == {"string"}
        assert {linear[n] for n in ("p1", "v1", "p2", "v2", "executed_volume")} == {"float64"}
    for key in (BW, BM):
        block = {c.name: c.dtype for c in _record(key).epochs[0].columns}
        assert block["mar"] == "string" and block["efa"] == "int64"
        assert {block[n] for n in ("clearing_price", "price", "volume")} == {"float64"}
    for key in (BW, BM, LW, LM, UN, SM):
        clocks = {c.name: c.dtype for c in _record(key).epochs[0].columns if c.name in TIME_NAMES}
        assert clocks and set(clocks.values()) == {"string"}, key
    for key in (UN, SM):
        assert {c.name: c.dtype for c in _record(key).epochs[0].columns}["efa_date"] == "date"


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_spec_recipe(key: str) -> None:
    """Detects a composed block start (EFA + date, forbidden by RULINGS 572 / 588), a fabricated
    instant for the FFR and order tables (their dates and clocks are raw vendor strings) or a
    results recipe other than the ``efa_date`` label anchor: results by unit and summary take
    ``date_sp1`` of ``efa_date`` and the other five records ``none``."""
    recipe = _record(key).temporal
    if key in DATE_SP1:
        assert (recipe.kind, recipe.date_column) == ("date_sp1", "efa_date")
    else:
        assert recipe.kind == "none" and recipe.date_column is None
    assert recipe.period_column is None and recipe.column is None


def test_the_two_filename_maps_are_exact_and_exclude_every_other_name() -> None:
    """Detects a filename-format map retyped from the wrong evidence: the weekly files carry
    ``%Y-%m-%d`` and the three masters of each table ``%d/%m/%Y`` (SW-2), keyed by the exact vendor
    filenames, so a resource renamed by NESO fails loud instead of parsing on a guessed format."""
    for key, formats in ((UN, BYUNIT_FORMATS), (SM, SUMMARY_FORMATS)):
        (column,) = [c for c in _record(key).epochs[0].columns if c.name == "efa_date"]
        assert column.formats_by_filename == formats
        names = {name for name, _fmt in formats}
        resources = [
            r
            for r in _package_doc(P2_PACKAGE)["resources"]
            if r["disposition"] == {"kind": "SILVER", "key": key}
        ]
        assert len(resources) == 4, key
        assert {CAPTURES[a].filename for a in CAPTURES if owner(a) == key} <= names
    for alias in ("uw", "um9", "um0", "um1", "sw", "sm9", "sm0", "sm1"):
        assert datetime.strptime(rows(alias)[0]["EFA Date"], DATE_FORMAT_OF[alias])


# --------------------------------------------------------------------------- #
# Eligibility and dispositions
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
    """Detects a hold dropped, reworded or filed under the wrong unit (the master block orders lose
    every order identity to scientific notation; the summary's own dictionary contradicts itself on
    the price unit), or a package-wide hold: the record is ``held`` with the spec's question
    verbatim, unit ``v0.22-K-AS-5a``, effectively held, while its package stays eligible."""
    package, family = registry_module.load_registry().families[key]
    eligibility = _record(key).eligibility
    assert eligibility == Held(status="held", question=QUESTION_OF[key], unit=UNIT)
    assert effective_eligibility(package, family) == eligibility
    assert package.eligibility == Eligible(status="eligible")
    assert eligibility.question.startswith("TODO: PHASE2-")


def test_the_two_packages_stay_eligible_and_the_package_files_hold_the_seven_records() -> None:
    """Detects a package held by accident, a record on the wrong family or a family gaining a
    transformer: the two package files are ``eligible`` and hold the seven records plus the
    existing summary-archive record and AS-5c's workbook and three archive records, and no
    family has a bespoke transformer. The workbook and ZIP-archive dispositions are AS-5c's
    (``test_neso_as5c_records.py``); the summary archive keeps its container and record."""
    found: set[str] = set()
    for package in (FFR_PACKAGE, P2_PACKAGE):
        document = _package_doc(package)
        assert document["eligibility"] == {"status": "eligible"}, package
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), package
    assert found == {*RECORDED, ARCHIVE, *AS5C_RECORDED}
    containers = [
        r
        for r in _package_doc(P2_PACKAGE)["resources"]
        if r["family"] == "ffr_phase2_auction_files"
    ]
    assert [r["disposition"]["kind"] for r in containers].count("HOLD") == 0
    assert ARCHIVE in [
        r["disposition"]["key"] for r in containers if r["disposition"]["kind"] == "SILVER"
    ]
    assert _record(ARCHIVE).siblings == ("ffr_phase2_auction_files",)


def test_the_eligibility_report_lists_five_eligible_two_held_and_four_siblings_ingest_only() -> (
    None
):
    """Detects a family reported with the wrong status: the report lists each eligible family as
    ``silver`` / ``eligible`` with its selection (``whole_capture per resource_id`` for the FFR and
    master linear records, ``key_latest`` for results by unit) and the ``date_sp1`` recipe on the
    results record, each held family as ``silver`` / ``held: TODO: PHASE2-`` under this unit, and
    the four sibling families as ingest-only."""
    lines = eligibility_module.render(load_registry()).splitlines()

    def line_of(key: str) -> str:
        (line,) = [ln for ln in lines if f"| `{key}` |" in ln]
        return line

    for key in ELIGIBLE:
        line = line_of(key)
        assert "| silver | eligible |" in line, key
        assert ("whole_capture per resource_id" in line) == (key in PARTITIONED), key
        assert ("key_latest" in line) == (key in KEY_LATEST), key
        assert ("date_sp1" in line) == (key in DATE_SP1), key
        assert "CKAN last_modified of the captured file" in line, key
    for key in HELD:
        assert "| silver | held: TODO: PHASE2-" in line_of(key), key
        assert f"(unit {UNIT})" in line_of(key), key
    for sibling in OWNER_OF:
        assert "ingest-only (no silver output)" in line_of(sibling), sibling


def test_the_sibling_families_carry_no_record_and_every_resource_is_silver_to_the_owner() -> None:
    """Detects a sibling family gaining its own record or transformer (V-10) or a resource left
    pointing at itself: the four siblings (the two FFR naming families and the two weekly results
    exports) carry no record, and each of their resources is a ``SILVER`` disposition of its owner
    record, as are all the master resources of the owner families."""
    registry = registry_module.load_registry()
    for sibling, key in OWNER_OF.items():
        entry = registry.families[sibling][1]
        assert entry.record is None and entry.transformer is None, sibling
        resources = [
            r for r in _package_doc(PACKAGE_OF[sibling])["resources"] if r["family"] == sibling
        ]
        assert resources, sibling
        assert all(r["disposition"] == {"kind": "SILVER", "key": key} for r in resources), sibling
    for key in (BW, LW, LM, UN, SM, BM):
        resources = [r for r in _package_doc(P2_PACKAGE)["resources"] if r["family"] == key]
        assert all(r["disposition"] == {"kind": "SILVER", "key": key} for r in resources), key


def test_the_two_unfaithful_ffr_files_are_resource_holds_not_adjudications() -> None:
    """Detects TR147 or TR154 silently loaded or excluded, or filed as an ADR-040 failure: TR147
    (one ``1,366.61`` availability fee, an internal thousands separator the strict float64 cast
    rejects) and TR154 (ten nonblank rows with no Tender Ref) are ``HOLD`` resources of this unit
    with the verbatim reasons, and the other 26 resources of the three naming families are
    ``SILVER`` of the owner record."""
    resources = [
        r for r in _package_doc(FFR_PACKAGE)["resources"] if r["family"] in (FFR_CSV, FFR, FFR_EXT)
    ]
    assert len(resources) == 28
    holds = {r["id"]: r for r in resources if r["disposition"]["kind"] == "HOLD"}
    assert set(holds) == set(HOLD_REASON_OF)
    for resource_id, reason in HOLD_REASON_OF.items():
        assert holds[resource_id]["family"] == FFR
        assert holds[resource_id]["disposition"] == {
            "kind": "HOLD",
            "reason": reason,
            "unit": UNIT,
        }
    silver = [r for r in resources if r["id"] not in holds]
    assert len(silver) == 26
    assert all(r["disposition"] == {"kind": "SILVER", "key": FFR} for r in silver)
    ids = {CAPTURES[a].resource_id for a in CAPTURES if owner(a) == FFR}
    assert ids <= {r["id"] for r in silver} | {TR147_ID}


def test_the_ledger_names_only_the_ffr_162_198_overlap_for_the_eleven_families() -> None:
    """Detects an ADR-040 adjudication filed for this unit's own captures, or the post-merge
    ``overlap`` gap left open: every admitted capture loads and the two unfaithful files are
    resource holds, so the only ledger entry for the eleven families is the seat-ruled (780)
    ``overlap`` of tender_ref 162.198, byte-identical in the three consecutive monthly resources
    fd57201c, fbb1ce86 and 2bfa2ab3 of the owner family, and the ledger still validates."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    named = [e for e in entries if e.family in set(ALL_FAMILIES)]
    assert len(named) == 1
    (entry,) = named
    assert (entry.family, entry.category, entry.cause, entry.ruling) == (
        FFR,
        "overlap",
        None,
        "780",
    )
    resources = set()
    for path in entry.captures:
        match = registry_module.CAPTURE_ID_PATTERN.fullmatch(path)
        assert match is not None
        resources.add(match.group("rid"))
    assert len(entry.captures) == 3
    assert {r[:8] for r in resources} == {"fd57201c", "fbb1ce86", "2bfa2ab3"}


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", LOADING)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a duplicate
    key or an excluded row: the slice loads every populated row with zero exclusions and no
    failure, every identifier, label, date and measure equals the body cell (``NA`` / ``N/A`` /
    ``-`` / a lone space / ``1E+14`` byte-equal, zeros zero, blanks null), the entity key is unique
    and its non-nullable columns are populated, ``available_at`` is the CKAN ``last_modified``
    read as UTC, and the temporal column is the capture time (no recipe) or the ``efa_date``
    anchor. A cell failing its strict cast (a ``float64``, ``efa`` or ``efa_date`` under the
    filename's format) would fail here."""
    key = owner(alias)
    frame = _load_clean(data, alias)
    _assert_cells(alias, frame)
    if key in PARTITIONED:
        assert set(frame["resource_id"].to_list()) == {CAPTURES[alias].resource_id}
    else:
        assert "resource_id" not in frame.columns
    assert not frame.select(KEY_OF[key]).is_duplicated().any()
    for name in KEY_OF[key]:
        if name in NON_NULL_OF[key] or name == "resource_id":
            assert frame[name].null_count() == 0, name
    modified = datetime.fromisoformat(CAPTURES[alias].modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {modified}
    if key in DATE_SP1:
        assert frame["timestamp_utc"].to_list() == [_anchor(d) for d in frame["efa_date"]]
        assert frame.schema["efa_date"] == pl.Date
    else:
        written = datetime.fromisoformat(CAPTURES[alias].written)
        assert set(frame["timestamp_utc"].to_list()) == {written}
    for column in TIME_NAMES & set(frame.columns):
        assert frame.schema[column] == pl.String, column
    assert frame["event_time"].to_list() == frame["timestamp_utc"].to_list()


def test_ffr_tr144_keeps_mixed_dates_the_lone_space_dash_fee_and_embedded_newline(
    data: Path,
) -> None:
    """Detects an FFR date or window clock converted (the slash date ``31/12/2021`` and the Excel
    serial ``44562`` both stay the vendor strings, no partial parsing), the lone-space automatic
    response cell trimmed or nulled, the ``-`` nomination fee mapped to null or cast, the embedded
    newline of a multi-postcode cell collapsed, or a zero fee read as missing: TR144 loads its five
    cut rows with every cell equal to the body."""
    frame = _load_clean(data, "r144")
    assert frame.height == 5
    assert frame["start_date"].to_list() == [r["Start Date"] for r in rows("r144")]
    assert {"31/12/2021", "44562"} <= set(frame["start_date"].to_list())
    assert frame.schema["start_date"] == frame.schema["end_date"] == pl.String
    assert " " in frame["auto_response_10s"].to_list()
    assert "-" in frame["nomination_fee"].to_list() and "0" in frame["nomination_fee"].to_list()
    assert any("\n" in (p or "") for p in frame["post_code"].to_list())
    assert frame.schema["availability_fee"] == pl.Float64
    assert frame.schema["frame_duration_mon_fri"] == pl.Float64
    assert frame["status"].to_list() == [r["Status"] for r in rows("r144")]


def test_ffr_tr139_f42_loads_with_a_null_post_code_and_keeps_the_prose_response(
    data: Path,
) -> None:
    """Detects the Post-code-less F42 epoch not mapped by source name (a positional mapping would
    shift every later column) or its prose cell cast: TR139 loads under its own epoch with
    ``post_code`` null for every row, ``comments`` still the last column, and the
    ``H for 30 minutes only`` automatic-response cell a string byte-equal to the body."""
    frame = _load_clean(data, "r139")
    assert frame["post_code"].null_count() == frame.height == 3
    assert "H for 30 minutes only" in frame["auto_response_30s"].to_list()
    assert frame["comments"].to_list() == [r["Comments"] or None for r in rows("r139")]


def test_the_three_naming_families_load_together_and_a_repeated_tender_ref_is_kept(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects one naming family shadowing another, a tender reference treated as a global key or
    an amendment (``162.198`` appears in TR162, TR163 and TR164 and is kept under each
    ``resource_id``), a wholly blank row reaching silver, or a mis-stamped resource: the
    ``_csv`` (TR144), ``_csv_ext`` (TR139, TR163, TR164) and ``_ext`` (TR142) bodies load under one
    record, the two blank rows are dropped before typing and counted (not exclusions), and the
    key is ``(resource_id, tender_ref)``."""
    aliases = ("r144", "r139", "r142", "r148", "r163", "r164", "r146")
    with caplog.at_level("INFO", logger="gridflow.silver.csv_bronze"):
        frame = _load_clean(data, *aliases)
    assert set(frame["resource_id"].to_list()) == {CAPTURES[a].resource_id for a in aliases}
    assert [m for m in (r.getMessage() for r in caplog.records) if "blank row" in m]
    repeated = frame.filter(pl.col("tender_ref") == "162.198")
    assert sorted(repeated["resource_id"].to_list()) == sorted(
        [CAPTURES["r163"].resource_id, CAPTURES["r164"].resource_id]
    )
    assert not frame.select(KEY_OF[FFR]).is_duplicated().any()
    for alias in aliases:
        assert len(rows(alias, keep_blank=True)) >= len(_of_capture(alias, frame))
    tr146 = _of_capture("r146", frame)
    assert tr146["auto_response_10s"].to_list() == ["1"]
    for alias in aliases:
        _assert_cells(alias, frame)


def test_the_tr147_thousands_separator_fails_the_strict_float_cast_and_stays_held(
    data: Path,
) -> None:
    """Detects the strict cast being loosened or TR147 silently loaded: the one ``1,366.61``
    availability fee is not a ``float64`` cell, so the slice filed under an admitted resource id
    fails the capture and loads nothing (the resource itself is a ``HOLD``, not a load path)."""
    assert CAPTURES["r147"].resource_id == TR147_ID
    assert [r["Tendered Prices - Availability Fee (£/h)"] for r in rows("r147")][-1] == "1,366.61"
    _id, cls, message = _fail(data, "r147", resource_id=CAPTURES["r144"].resource_id)
    assert (cls, "1,366.61" in message) == ("InvalidOperationError", True)


def test_the_tr147_resource_is_not_transformed_while_held(data: Path) -> None:
    """Detects a held resource still feeding the record: TR147's capture, filed under its own
    resource id, writes no rows and no failure."""
    capture(data, "r147")
    assert get_transformer(SOURCE, FFR, data).run(DAY, run_id="r") == 0
    assert _no_silver(data, FFR)


def test_weekly_block_orders_keep_the_na_block_code_prm_and_the_negative_volume(
    data: Path,
) -> None:
    """Detects ``NA`` read as missing, a negative (sell) volume made positive or ``Bid Validity``
    blanks given a token: the weekly block rows keep ``block_code_prm`` ``NA`` and negative
    ``volume`` floats, ``invalid`` the vendor text, ``bid_validity`` null, ``efa`` int64."""
    frame = _load_clean(data, "bw")
    assert frame["block_code_prm"].to_list() == ["NA", "NA"]
    assert all(v < 0 for v in frame["volume"].to_list())
    assert frame["bid_validity"].null_count() == frame.height
    assert frame.schema["efa"] == pl.Int64 and frame.schema["volume"] == pl.Float64
    assert frame["order_id"].to_list() == [r["OrderID"] for r in rows("bw")]


def test_master_block_orders_keep_the_scientific_notation_ids_and_the_complete_row_key(
    data: Path,
) -> None:
    """Detects the exports' collapsed identifiers cast to numbers (``1E+14`` would become
    ``100000000000000.0``), a ``Bid Validity`` text row dropped or a blank one given a token, or the
    key losing a column: the three masters load under their own ``resource_id``, ``order_id``,
    ``order_period_id`` and ``trade_id`` are the strings exactly as published, the key is
    ``resource_id`` plus the 24 silver columns and ``bid_validity`` is the one nullable key
    column."""
    frame = _load_clean(data, "bm9", "bm0", "bm1")
    assert set(frame["resource_id"].to_list()) == {
        CAPTURES[a].resource_id for a in ("bm9", "bm0", "bm1")
    }
    assert set(frame["order_id"].to_list()) <= {"1E+14", "1.00E+14"}
    assert "1E+14" in frame["order_id"].to_list()
    nine = _of_capture("bm9", frame)
    assert nine["bid_validity"].to_list() == [
        None,
        None,
        "Offered volume is over tested volume",
        "Offered volume is over tested volume",
    ]
    record = _record(BM)
    assert list(record.entity_key) == ["resource_id", *B24_NAMES]
    nullable_key = [c.name for c in record.epochs[0].columns if c.nullable]
    assert nullable_key == ["bid_validity"]
    assert not frame.select(KEY_OF[BM]).is_duplicated().any()


def test_a_master_block_row_differing_in_one_column_is_a_new_key_but_an_exact_copy_fails(
    data: Path,
) -> None:
    """Detects the complete-row guard dropping a column or deduplicating: a second row that differs
    from the first only in ``volume`` is a different key and both load; an exact copy of a row fails
    the capture as ``DuplicateEntityKeyError`` and loads nothing."""
    cells = _cells(body("bm9"))
    first = cells[1]
    changed = list(first)
    changed[cells[0].index("Volume")] = "-12345"
    out = io.StringIO(newline="")
    csv.writer(out, lineterminator="\r\n").writerows([*cells, changed])
    raw = out.getvalue().encode("utf-8")
    capture(data, "bm9", raw=raw)
    written = get_transformer(SOURCE, BM, data).run(DAY, run_id="r")
    assert written == len(rows("bm9")) + 1
    assert -12345.0 in _silver(data, BM)["volume"].to_list()


def test_weekly_linear_orders_keep_the_na_curve_points_as_strings(data: Path) -> None:
    """Detects the literal ``NA`` of the third and fourth curve points zeroed, nulled or the whole
    column cast: the weekly rows with ``NA`` keep ``p3`` / ``v3`` / ``p4`` / ``v4`` as the string
    ``NA`` while a populated row keeps its price points as text and ``p1``..``v2`` stay float64."""
    frame = _load_clean(data, "lw")
    na = frame.filter(pl.col("p3") == "NA")
    assert na.height == 2
    assert na.select("p3", "v3", "p4", "v4").row(0) == ("NA", "NA", "NA", "NA")
    assert {frame.schema[n] for n in ("p1", "v1", "p2", "v2")} == {pl.Float64}
    assert {frame.schema[n] for n in ("p3", "v3", "p4", "v4")} == {pl.String}
    assert 0.0 in frame["v1"].to_list() or frame["v1"].null_count() == 0


def test_master_linear_orders_load_two_masters_under_distinct_resources(data: Path) -> None:
    """Detects a master shadowing the other or an unstamped resource: the 2019-2020 and 2021-2022
    masters load together under the key ``(resource_id, market_name, delivery_start)`` with the raw
    delivery strings of their own clock format kept verbatim, and the 2021-2022 literal ``NA`` curve
    rows stay strings."""
    frame = _load_clean(data, "lm9", "lm1")
    assert set(frame["resource_id"].to_list()) == {
        CAPTURES["lm9"].resource_id,
        CAPTURES["lm1"].resource_id,
    }
    assert _of_capture("lm9", frame)["delivery_start"][0] == rows("lm9")[0]["DeliveryStart"]
    assert _of_capture("lm1", frame)["delivery_start"][0] == rows("lm1")[0]["DeliveryStart"]
    assert "NA" in frame["p3"].to_list()


def test_weekly_and_master_order_tables_are_separate_records(data: Path) -> None:
    """Detects the weekly and master order exports unioned or summed: the weekly block table and the
    master block table are different families with different keys, and loading both leaves each
    silver family with only its own rows."""
    capture(data, "bw")
    capture(data, "bm9")
    assert get_transformer(SOURCE, BW, data).run(DAY, run_id="r") == len(rows("bw"))
    assert get_transformer(SOURCE, BM, data).run(DAY, run_id="r") == len(rows("bm9"))
    assert _silver(data, BW).height == len(rows("bw"))
    assert _silver(data, BM).height == len(rows("bm9"))
    assert list(_record(BW).entity_key) != list(_record(BM).entity_key)


def test_results_by_unit_keep_the_cancellation_prose_and_each_filename_format(data: Path) -> None:
    """Detects ``Cancelled`` prose dropped or a date parsed on the wrong format: the weekly file's
    ``2021-11-27`` and the masters' ``27/11/2021`` both become the date 27 November 2021, the
    ``Cancelled`` text survives byte-equal and blanks stay null, and the by-unit price (£/MW/h)
    stays float64."""
    frame = _load_clean(data, "uw", "um9", "um0", "um1")
    assert date(2021, 11, 27) in frame["efa_date"].to_list()
    assert date(2019, 12, 7) in frame["efa_date"].to_list()
    cancelled = [c for c in frame["cancelled"].to_list() if c is not None]
    assert cancelled == [rows("um1")[-1]["Cancelled"]]
    assert frame.schema["clearing_price"] == pl.Float64 and frame.schema["efa"] == pl.Int64
    for alias in ("uw", "um9", "um0", "um1"):
        _assert_cells(alias, frame)


def _latest_ids(data: Path, key: str, as_of: datetime | None = None) -> Counter[str]:
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    return Counter(both_as_of(db, data, key, as_of))


@pytest.mark.parametrize(
    ("key", "weekly", "masters", "shared_master", "shared"),
    [
        (UN, "uw", ("um9", "um0", "um1"), "um1", 2),
        (SM, "sw", ("sm9", "sm0", "sm1"), "sm1", 2),
    ],
)
def test_the_results_overlap_resolves_to_the_master_row_and_an_earlier_as_of_to_the_weekly(
    data: Path, key: str, weekly: str, masters: tuple[str, ...], shared_master: str, shared: int
) -> None:
    """Detects ``_latest`` keeping both rows of a shared key, summing them, or resolving to the
    weekly export: the weekly file and the 2021-2022 master share ``shared`` keys (the master
    row differs only in date and time spelling), the base table holds both, the ``_latest`` view
    returns exactly one row per key and it is the master's (its CKAN last-modified is the later
    one), the weekly capture contributes none, and an as-of between the two last-modified instants
    serves the weekly rows instead. The DuckDB view and Polars agree (``both_as_of``)."""
    ids = {a: capture(data, a) for a in (weekly, *masters)}
    written = get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    base = _silver(data, key)
    assert written == base.height == sum(len(rows(a)) for a in ids)
    assert base.select(KEY_OF[key]).is_duplicated().sum() == 2 * shared
    modified = {a: datetime.fromisoformat(CAPTURES[a].modified).replace(tzinfo=UTC) for a in ids}
    assert modified[weekly] < modified[shared_master]
    latest = _latest_ids(data, key)
    assert latest == Counter({ids[a]: len(rows(a)) for a in masters}), (
        "one row per key, the weekly capture contributes none"
    )
    assert sum(latest.values()) == base.height - shared
    between = modified[weekly] + timedelta(seconds=30)
    assert modified[weekly] < between < modified[shared_master]
    earlier = _latest_ids(data, key, between)
    expected = {ids[weekly]: len(rows(weekly))} | {
        ids[a]: len(rows(a)) for a in masters if modified[a] < between
    }
    assert earlier == Counter(expected)


@pytest.mark.parametrize(("alias", "key"), [("um9", UN), ("sm9", SM)])
def test_an_unlisted_filename_fails_the_capture(data: Path, alias: str, key: str) -> None:
    """Detects a resource renamed by NESO parsed on a guessed date format: the master body filed
    under a filename that is not in the record's exact-filename map fails the capture, loads
    nothing and writes no completion."""
    _id, cls, message = _fail(data, alias, filename="phase-2-ffr-auction-renamed-2022-2023.csv")
    assert cls == "UnmappedResourceFormatError"
    assert "phase-2-ffr-auction-renamed-2022-2023.csv" in message
    assert owner(alias) == key


def test_the_summary_keeps_both_dates_and_the_raw_delivery_strings(data: Path) -> None:
    """Detects the summary's delivery cells typed or composed with the EFA ordinal:
    ``delivery_start``
    and ``delivery_end`` stay the vendor strings, ``efa`` is int64 and the temporal anchor is the
    ``efa_date`` label, never a block start."""
    frame = _load_clean(data, "sw", "sm9", "sm0", "sm1")
    assert frame.schema["delivery_start"] == frame.schema["delivery_end"] == pl.String
    assert frame.schema["efa"] == pl.Int64
    block_start = frame["delivery_start"].to_list()
    assert block_start == [
        r["Delivery Start"] for a in ("sw", "sm9", "sm0", "sm1") for r in rows(a)
    ]
    assert all(
        t == _anchor(d) for t, d in zip(frame["timestamp_utc"], frame["efa_date"], strict=True)
    )


# --------------------------------------------------------------------------- #
# Loud failure, vintage and the skeleton pages
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_a_header_outside_the_epochs_fails_loud(data: Path, key: str) -> None:
    """Detects a silent header drift (a renamed vendor column loaded under the old typing): a body
    whose first header cell is renamed fails the capture with no completion and no silver."""
    alias = FIRST_OF[key]
    raw = body(alias)
    first_cell = HEADERS_OF[key][0][0].encode()
    renamed = raw.replace(first_cell, first_cell + b"_x", 1)
    assert renamed != raw
    _fail(data, alias, raw=renamed)


def _byte_records(raw: bytes) -> list[bytes]:
    """Byte-exact CSV records of ``raw`` (header first); quote parity ends a record."""
    out: list[bytes] = []
    current = b""
    for line in raw.split(b"\n")[:-1]:
        current += line + b"\n"
        if current.count(b'"') % 2 == 0:
            out.append(current)
            current = b""
    assert not current
    return out


@pytest.mark.parametrize("key", RECORDED)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, key: str) -> None:
    """Detects a duplicate key loaded silently: the first populated data row repeated at the end of
    the body fails the capture with ``DuplicateEntityKeyError``, loads nothing and leaves no
    completion (the two held records fail the same way)."""
    alias = FIRST_OF[key]
    records = _byte_records(body(alias))
    raw = body(alias) + records[1]
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, key, capture_id) is None
    assert _no_silver(data, key)


@pytest.mark.parametrize(
    "alias", ["r144", "r139", "r142", "bw", "bm9", "lw", "lm9", "uw", "um1", "sw", "sm1"]
)
def test_every_family_is_stamped_with_the_ckan_last_modified(data: Path, alias: str) -> None:
    """Detects the vintage drifting: every resource is an upload, so ``available_at`` is the CKAN
    ``last_modified`` read as UTC (not the capture time). An as-of read just before it serves
    nothing and one after serves the capture, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    key = owner(alias)
    capture_id = capture(data, alias)
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    frame = _silver(data, key)
    modified = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {modified}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, key, modified - timedelta(microseconds=1)) == []
    assert set(both_as_of(db, data, key, modified + timedelta(days=1))) == {capture_id}
    assert set(both_as_of(db, data, key, None)) == {capture_id}


@pytest.mark.parametrize("key", RECORDED)
def test_the_skeleton_page_renders_the_new_records(key: str) -> None:
    """Detects a record the docs generator cannot render (the 43-column FFR epochs with embedded
    newlines, the 24-column complete-row key, the filename-format maps)."""
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
