"""ASDP instructions and window prices, ODFM market information and STOR day-ahead auction results:
five frozen records, three eligible, two held, one no-record family and one resource HOLD
(v0.22-K-AS-5b).

Four package files carry the records:
``non-bm-ancillary-service-dispatch-platform-asdp-instructions``
(``asdp_instructions``), ``...-asdp-window-prices`` (``asdp_window_prices``),
``optional-downward-flexibility-management-odfm-market-information`` (``..._instructed``,
``..._load_factors``; ``..._results`` stays ingest-only with no record, RULINGS 538, and the PDFs
stay
``DOC``) and ``short-term-operating-reserve-stor-day-ahead-auction-results``
(``stor_da_auction_results``). Every test writes recorded fixture captures (cuts of the 2026-10-08
swept bronze under ``tests/fixtures/neso_data_portal/as5b/``, provenance in ``PROVENANCE.md``)
into a
short data root and runs the transformer the **real package registry** generates, so a record that
does not fit its vendor body fails here, not at activation.

Record decisions under test (K-AS-5-FACTS, K-AS-5b-SPEC):

- ``issue: none`` everywhere; ASDP dispatch / cease stamps are send times of separate publication
  stages, ODFM and STOR carry no issue column. ASDP datastore dumps take ``capture_fallback``, the
  ODFM and STOR uploads ``ckan_last_modified``.
- Every ASDP time cell and the ODFM load-factor ``datetime`` are raw strings (export clock
  undocumented: TODO ASDP-EXPORT-CLOCK, ODFM-LOAD-FACTOR-CLOCK-SCALE) at temporal ``none``. ASDP MW
  carries no sign convention (TODO ASDP-SIGN-CONVENTION): ``RDP_NEGATIVE`` rows keep positive MW.
- Window prices are held on ASDP-WINDOW-PRICE-UNIT; load factors on ODFM-LOAD-FACTOR-CLOCK-SCALE
  (values unscaled).
- The ODFM instructed key gains ``resource_id`` (V-6: an ``sp_pair`` key strictly contains the
  pair).
  The V1 upload is a resource HOLD (ODFM-INSTRUCTED-V1), V2 is the loaded version.
- STOR ``Service Delivery From`` / ``To`` carry a literal ``Z`` that agrees with the documented
  05:00 Europe/London service day (23, 24 and 25 hour days), so they are UTC datetimes.
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
from pydantic import ValidationError
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings

from gridflow.connectors.neso_data_portal import eligibility as eligibility_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Eligible, Held, load_registry
from gridflow.connectors.neso_data_portal.registry.record import (
    ColumnSpec,
    HeaderEpoch,
    IssueRecipe,
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "as5b"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-AS-5b"

INST = "asdp_instructions"
WIN = "asdp_window_prices"
ODI = "odfm_market_information_instructed"
ODL = "odfm_market_information_load_factors"
ODR = "odfm_market_information_results"
ODF = "odfm_market_information_files"
STOR = "stor_da_auction_results"
RECORDED = (INST, WIN, ODI, ODL, STOR)
ELIGIBLE = (INST, ODI, STOR)
HELD = (WIN, ODL)

PACKAGE_OF = {
    INST: "non-bm-ancillary-service-dispatch-platform-asdp-instructions",
    WIN: "non-bm-ancillary-service-dispatch-platform-asdp-window-prices",
    ODI: "optional-downward-flexibility-management-odfm-market-information",
    ODL: "optional-downward-flexibility-management-odfm-market-information",
    ODR: "optional-downward-flexibility-management-odfm-market-information",
    ODF: "optional-downward-flexibility-management-odfm-market-information",
    STOR: "short-term-operating-reserve-stor-day-ahead-auction-results",
}
PACKAGE_ID_OF = {
    PACKAGE_OF[INST]: "67c7829e-3d2b-47fe-963a-8846eb1de37b",
    PACKAGE_OF[WIN]: "27ad5028-7df5-43f6-b001-05eb36a5d2e8",
    PACKAGE_OF[ODI]: "23ec7a9f-6220-4d1e-9099-e9195a2ed177",
    PACKAGE_OF[STOR]: "fb4a9900-9438-47ae-9788-06f490353a89",
}

H_D8 = [
    "EntryID",
    "ServiceType",
    "InstructionStartDateTime",
    "MW",
    "IndicativePrice",
    "InstructionCeaseDateTime",
    "DispatchDateTimeStamp",
    "CeaseDateTimeStamp",
]
H_D11 = [*H_D8, "PartyID", "AssetID", "Tendered"]
H_P8 = [
    "EntryID",
    "ServiceType",
    "SPStartDateTime",
    "SPEndDateTime",
    "WindowPrice",
    "CreatedTime",
    "PartyID",
    "AssetID",
]
H_V2 = ["Settlement Period", "ODFM Instructed MW", "Time", "Date"]
H_LF = [
    "Datetime",
    *[
        f"{region} {tech}"
        for tech in ("Wind", "Solar", "Other")
        for region in (
            "Scottish Hydro",
            "Scottish Power",
            "Electricity North West",
            "NPG North East",
            "NPG Yorkshire",
            "SP Manweb",
            "WPD West Midlands",
            "WPD East Midlands",
            "UKPN East Anglia",
            "UKPN London",
            "WPD South Wales",
            "WPD South West",
            "SSE South",
            "UKPN South East",
        )
    ],
]
H_T16 = [
    "Service Name",
    "Tender Round ID",
    "Buy Curve ID",
    "Tender Submission ID",
    "Service Delivery From",
    "Service Delivery To",
    "Unit ID",
    "Company Name",
    "BM/NBM",
    "Fuel Type",
    "Tendered MW",
    "Minimum Acceptable MW",
    "Contracted MW",
    "Tendered Availability Price",
    "Market Clearing Price",
    "Status",
]
HEADERS_OF: dict[str, list[list[str]]] = {
    INST: [H_D8, H_D11],
    WIN: [H_P8],
    ODI: [H_V2],
    ODL: [H_LF],
    STOR: [H_T16],
}

# source header -> (silver name, kind); kind: f float64, i int64, d date, t UTC datetime, s string.
S_INST = {
    "EntryID": ("entry_id", "s"),
    "ServiceType": ("service_type", "s"),
    "InstructionStartDateTime": ("instruction_start_date_time", "s"),
    "MW": ("mw", "f"),
    "IndicativePrice": ("indicative_price", "f"),
    "InstructionCeaseDateTime": ("instruction_cease_date_time", "s"),
    "DispatchDateTimeStamp": ("dispatch_date_time_stamp", "s"),
    "CeaseDateTimeStamp": ("cease_date_time_stamp", "s"),
    "PartyID": ("party_id", "s"),
    "AssetID": ("asset_id", "s"),
    "Tendered": ("tendered", "s"),
}
S_WIN = {
    "EntryID": ("entry_id", "s"),
    "ServiceType": ("service_type", "s"),
    "SPStartDateTime": ("sp_start_date_time", "s"),
    "SPEndDateTime": ("sp_end_date_time", "s"),
    "WindowPrice": ("window_price", "f"),
    "CreatedTime": ("created_time", "s"),
    "PartyID": ("party_id", "s"),
    "AssetID": ("asset_id", "s"),
}
S_ODI = {
    "Settlement Period": ("settlement_period", "i"),
    "ODFM Instructed MW": ("odfm_instructed_mw", "f"),
    "Time": ("time_label", "s"),
    "Date": ("settlement_date", "d"),
}
S_ODL = {h: (h.lower().replace(" ", "_"), "s" if h == "Datetime" else "f") for h in H_LF}
S_STOR = {
    "Service Name": ("service_name", "s"),
    "Tender Round ID": ("tender_round_id", "s"),
    "Buy Curve ID": ("buy_curve_id", "s"),
    "Tender Submission ID": ("tender_submission_id", "s"),
    "Service Delivery From": ("service_delivery_from", "t"),
    "Service Delivery To": ("service_delivery_to", "t"),
    "Unit ID": ("unit_id", "s"),
    "Company Name": ("company_name", "s"),
    "BM/NBM": ("bm_nbm", "s"),
    "Fuel Type": ("fuel_type", "s"),
    "Tendered MW": ("tendered_mw", "f"),
    "Minimum Acceptable MW": ("minimum_acceptable_mw", "f"),
    "Contracted MW": ("contracted_mw", "f"),
    "Tendered Availability Price": ("tendered_availability_price", "f"),
    "Market Clearing Price": ("market_clearing_price", "f"),
    "Status": ("status", "s"),
}
SILVER_OF = {INST: S_INST, WIN: S_WIN, ODI: S_ODI, ODL: S_ODL, STOR: S_STOR}

KEY_OF: dict[str, list[str]] = {
    INST: ["resource_id", "entry_id"],
    WIN: ["resource_id", "created_time", "asset_id"],
    ODI: ["resource_id", "settlement_date", "settlement_period"],
    ODL: ["datetime"],
    STOR: ["tender_submission_id"],
}
TEMPORAL_OF: dict[str, dict[str, str | None]] = {
    INST: {"kind": "none"},
    WIN: {"kind": "none"},
    ODI: {
        "kind": "sp_pair",
        "date_column": "settlement_date",
        "period_column": "settlement_period",
    },
    ODL: {"kind": "none"},
    STOR: {"kind": "utc_instant", "column": "service_delivery_from"},
}
PARTITIONED = {INST, WIN, ODI}
CAPTURE_FALLBACK = {INST, WIN}
STOR_TIME = "%Y-%m-%d %H:%M:%SZ"

V1_ID = "f0a52698-5f80-4fb7-ad64-ede99015195f"
V2_ID = "5c507a16-d2f3-44df-b1bb-98acc0d74e1c"

# The hold questions and the resource-HOLD reason, verbatim (spec).
QUESTION_OF = {
    WIN: (
        "TODO: ASDP-WINDOW-PRICE-UNIT — the ASDP window-price dictionary calls WindowPrice only "
        "an indicative price and gives no currency, energy or time denominator, and the "
        "instruction "
        'table\'s £/MWh cannot be transferred because both are "indicative prices"; which unit '
        "does WindowPrice carry (for example £/MWh or £/MW/h)? (Settlement-period and "
        "creation times are naive with an undocumented export clock.)"
    ),
    ODL: (
        "TODO: ODFM-LOAD-FACTOR-CLOCK-SCALE — the ODFM load-factor upload does not state the "
        "time zone or the period start / end convention of its Datetime labels, nor whether its "
        "values are fractional factors or percentage numbers (the market report discusses load "
        "factors in percentage terms); which clock and which scale does it publish?"
    ),
}
V1_REASON = (
    'TODO: ODFM-INSTRUCTED-V1 — the first "ODFM Instructed Volume for 05-07-2020" upload has 46 '
    "unnamed trailing header cells (a frozen header cannot hold repeated blank names) and every "
    "Settlement Period label is one period behind its Time label (49 of 49 rows); NESO's V2 "
    "republication corrects the periods and changes 36 of the 49 instructed volumes, and V2 is the "
    "loaded version. Is V1 withdrawn, or does it carry a different meaning?"
)


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
    "i1": Capture(
        INST,
        "6f037ebe-110d-4099-ab82-0ac3185e3f18",
        "Non BM Dispatch Cease Instructions_Jun_2020",
        "6f037ebe-110d-4099-ab82-0ac3185e3f18",
        None,
        "2026-10-08T08:16:26.562018+00:00",
        "datastore",
    ),
    "i2": Capture(
        INST,
        "90ba1625-d99b-4ec6-8356-ad69af4e6b20",
        "Non BM Dispatch Cease Instructions_Nov_2021",
        "90ba1625-d99b-4ec6-8356-ad69af4e6b20",
        "2021-12-01T14:22:45.650767",
        "2026-10-08T08:24:27.008835+00:00",
        "datastore",
    ),
    "i3": Capture(
        INST,
        "9d03620d-e526-4c2e-8917-0d990efe71c9",
        "Non BM Dispatch Cease Instructions_Nov_2023",
        "9d03620d-e526-4c2e-8917-0d990efe71c9",
        None,
        "2026-10-08T08:36:27.635245+00:00",
        "datastore",
    ),
    "i4": Capture(
        INST,
        "f352265b-01c4-4f45-b0e4-bef889b74d23",
        "Non BM Dispatch Cease Instructions_Sep_2023",
        "f352265b-01c4-4f45-b0e4-bef889b74d23",
        None,
        "2026-10-08T08:35:27.604477+00:00",
        "datastore",
    ),
    "w1": Capture(
        WIN,
        "91749b29-d98a-4565-b126-bd021253b3af",
        "Non BM Window Price Change_2025",
        "91749b29-d98a-4565-b126-bd021253b3af",
        "2025-01-27T10:19:34.091937",
        "2026-10-08T08:51:28.613802+00:00",
        "datastore",
    ),
    "o1": Capture(
        ODI,
        V1_ID,
        "ODFM Instructed Volume for 05-07-2020",
        "odfm-instructed-volume-for-05-07-2020.csv",
        "2020-07-08T13:42:03.334175",
        "2026-10-08T11:15:29.601535+00:00",
        "upload",
    ),
    "o2": Capture(
        ODI,
        V2_ID,
        "ODFM Instructed Volume for 05-07-2020 V2",
        "odfm-instructed-volume-for-05-07-2020-v2.csv",
        "2020-07-08T13:37:56.620172",
        "2026-10-08T11:15:32.392120+00:00",
        "upload",
    ),
    "l1": Capture(
        ODL,
        "e6146148-c862-49f6-9c7f-52dc2b3e7086",
        "ODFM Load Factors",
        "odfm-loadfactors-2020-2021.csv",
        "2020-07-20T15:17:53.063674",
        "2026-10-08T11:15:36.175565+00:00",
        "upload",
    ),
    "s1": Capture(
        STOR,
        "844a1a65-0a3c-4704-b327-d1685d0c397b",
        "STOR DA Auction Results ",
        "stor-da-auction-results.csv",
        "2026-03-30T04:28:01.703705",
        "2026-10-08T11:27:32.475060+00:00",
        "upload",
    ),
}
FIRST_OF = {INST: "i2", WIN: "w1", ODI: "o2", ODL: "l1", STOR: "s1"}
LOADING = [a for a in CAPTURES if a != "o1"]
LF_ALIASES = {"i1", "i2", "i3", "i4", "w1"}
"""Fixtures cut from LF bodies (the ASDP datastore dumps); the others are CRLF."""


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
        prefix="as5b", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` exactly as committed (``-text``: the bronze bytes)."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed with edge-stripped names (cells verbatim)."""
    text = (raw if raw is not None else body(alias)).decode("utf-8")
    reader = csv.reader(io.StringIO(text, newline=""))
    names = [n.strip() for n in next(reader)]
    return [dict(zip(names, r, strict=False)) for r in reader if any(r)]


def header(alias: str) -> list[str]:
    """The fixture's header as the production reader returns it (edge whitespace stripped)."""
    first = body(alias).decode("utf-8").splitlines()[0]
    return [n.strip() for n in next(csv.reader([first]))]


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


def _expected(spec: tuple[str, str], cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank cell is null)."""
    _name, kind = spec
    if cell == "":
        return None
    if kind == "f":
        return float(cell)
    if kind == "i":
        return int(cell)
    if kind == "d":
        return datetime.strptime(cell, "%d/%m/%Y").date()
    if kind == "t":
        return datetime.strptime(cell, STOR_TIME).replace(tzinfo=UTC)
    return cell


def _assert_cells(alias: str, frame: pl.DataFrame) -> None:
    """Every identifier, label, date, instant and measure of ``frame`` equals the body cell."""
    family = CAPTURES[alias].family
    source_rows = rows(alias)
    for source in header(alias):
        spec = SILVER_OF[family][source]
        assert frame[spec[0]].to_list() == [_expected(spec, r[source]) for r in source_rows], (
            alias,
            source,
        )


def _anchor(day: date) -> datetime:
    """The UTC instant of UK-local midnight on ``day``."""
    return datetime(day.year, day.month, day.day, tzinfo=ZoneInfo("Europe/London")).astimezone(UTC)


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
    (the D8 and D11 ASDP epochs, P8, the V2 header with edge-padded names, the 43-column load
    factors, the 16-column T16), LF terminators on the ASDP datastore dumps and CRLF on the ODFM
    and STOR uploads, the first Jun-2020 body kept whole with its ``... Z`` space-separated start
    label, the D11 slices carrying an ``RDP_NEGATIVE`` row, a blank-MW row, a ``Non-Tendered`` and
    a ``Tendered`` row, the two shared-EntryID window rows, the V1 header with 46 blank trailing
    names, and the STOR slice holding a 23 hour, a 24 hour and a 25 hour service day."""
    for alias in CAPTURES:
        raw = body(alias)
        raw.decode("utf-8")
        assert not raw.startswith(b"\xef\xbb\xbf"), alias
        assert raw.endswith(b"\n"), alias
        crlf = raw.count(b"\r\n")
        assert crlf == (0 if alias in LF_ALIASES else raw.count(b"\n")), alias
    assert header("i1") == H_D8 and header("i2") == header("i3") == header("i4") == H_D11
    assert header("w1") == H_P8 and header("o2") == H_V2 and header("l1") == H_LF
    assert header("s1") == H_T16 and len(H_LF) == 43
    assert b"ODFM Instructed MW ,Time ,Date " in body("o2").split(b"\r\n")[0] + b" "
    assert any("Z" in r["InstructionStartDateTime"] for r in rows("i1"))
    assert any(r["ServiceType"] == "RDP_NEGATIVE" for r in rows("i3"))
    assert any(r["MW"] == "" and r["IndicativePrice"] == "" for r in rows("i2"))
    assert {r["Tendered"] for r in rows("i2")} >= {"", "Tendered"}
    assert {r["Tendered"] for r in rows("i4")} >= {"Non-Tendered"}
    shared = [r["EntryID"] for r in rows("w1")]
    assert len(shared) > len(set(shared))
    v1 = next(csv.reader([body("o1").decode("utf-8").splitlines()[0]]))
    assert [n.strip() for n in v1[:4]] == H_V2 and v1[4:] == [""] * 46
    hours = {
        (
            datetime.strptime(r["Service Delivery To"], STOR_TIME)
            - datetime.strptime(r["Service Delivery From"], STOR_TIME)
        ).total_seconds()
        // 3600
        for r in rows("s1")
    }
    assert {23.0, 24.0, 25.0} <= hours


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_utf8_csv_with_the_spec_key_partition_vintage_and_no_issue(
    key: str,
) -> None:
    """Detects a record that invents an issue time (ASDP stamps are send times, ODFM and STOR have
    no issue column), a ``key_latest`` selection, a ``run_type``, a missing resource partition on
    the multi-resource ASDP and ODFM-instructed families, a partition on a single-resource family
    or the wrong vintage: every record is version 1, utf-8 CSV, whole-capture selection, the spec's
    entity key, ``capture_fallback`` for the two datastore families and ``ckan_last_modified`` for
    the uploads, with the vendor header(s) as its epochs and ``issue: none``."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key in PARTITIONED else None)
    assert record.vintage == (
        "capture_fallback" if key in CAPTURE_FALLBACK else "ckan_last_modified"
    )
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


def test_the_asdp_epochs_are_the_exact_d8_and_d11_headers_read_from_the_bodies() -> None:
    """Detects an epoch whose header was retyped rather than read from the body, or a third ASDP
    header: the instructions record has exactly the 8-column D8 and 11-column D11 headers (D11 is
    D8 plus ``PartyID``, ``AssetID``, ``Tendered``), equal to the first record of the fixtures."""
    assert [list(e.header) for e in _record(INST).epochs] == [H_D8, H_D11]
    assert H_D11[:8] == H_D8
    assert header("i1") == list(_record(INST).epochs[0].header)
    assert header("i2") == list(_record(INST).epochs[1].header)
    assert header("w1") == list(_record(WIN).epochs[0].header)
    assert header("l1") == list(_record(ODL).epochs[0].header)
    assert header("s1") == list(_record(STOR).epochs[0].header)


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded, relabelled or re-typed column: measures are
    nullable float64 (zero is a value), identifiers and codes are strings (``EntryID`` is never
    cast), every ASDP time cell and the load-factor ``Datetime`` are raw strings, the ODFM date is
    ``%d/%m/%Y``, the STOR delivery cells are ``%Y-%m-%d %H:%M:%SZ`` datetimes with zone ``UTC``,
    ``settlement_period`` is the only bounded column (``1..50``), key columns and temporal inputs
    are ``nullable: false`` and nothing else is."""
    record = _record(key)
    non_null = set(KEY_OF[key]) - {"resource_id"}
    if key == STOR:
        non_null.add("service_delivery_from")
    if key == ODI:
        non_null |= {"settlement_date", "settlement_period"}
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
                assert (column.dtype, column.format, column.zone) == ("date", "%d/%m/%Y", None)
            elif kind == "t":
                assert (column.dtype, column.format, column.zone) == (
                    "datetime",
                    STOR_TIME,
                    "UTC",
                )
            else:
                assert column.format is None and column.zone is None
                assert column.dtype == {"f": "float64", "i": "int64", "s": "string"}[kind], name


def test_every_asdp_time_cell_and_the_load_factor_clock_are_raw_strings() -> None:
    """Detects a typed ASDP or load-factor time cell (TODO: ASDP-EXPORT-CLOCK — a literal ``Z`` in
    early cells does not license typing the naive later ones; TODO: ODFM-LOAD-FACTOR-CLOCK-SCALE):
    the four instruction stamps, the two window-period labels, ``created_time`` and the load-factor
    ``datetime`` are ``string`` in every epoch, and the registry types no timestamp for them."""
    wanted = {
        INST: {
            "instruction_start_date_time",
            "instruction_cease_date_time",
            "dispatch_date_time_stamp",
            "cease_date_time_stamp",
        },
        WIN: {"sp_start_date_time", "sp_end_date_time", "created_time"},
        ODL: {"datetime"},
    }
    for key, names in wanted.items():
        for epoch in _record(key).epochs:
            types = {c.name: c.dtype for c in epoch.columns}
            assert names <= set(types) and {types[n] for n in names} == {"string"}, key


def test_the_odfm_load_factor_columns_are_the_forty_two_lowercase_snake_names_unscaled() -> None:
    """Detects a renamed, rescaled or merged load-factor column: 42 distinct ``<region>_<tech>``
    names (14 regions each of Wind, Solar and Other) are the source names lower-cased with spaces
    as underscores, all float64 with no scaling, plus the string ``datetime``."""
    (epoch,) = _record(ODL).epochs
    names = [c.name for c in epoch.columns]
    assert names[0] == "datetime" and len(names) == 43 and len(set(names)) == 43
    assert "scottish_hydro_wind" in names and "ukpn_south_east_other" in names
    assert sum(n.endswith("_wind") for n in names) == 14
    assert sum(n.endswith("_solar") for n in names) == 14
    assert sum(n.endswith("_other") for n in names) == 14
    assert {c.dtype for c in epoch.columns[1:]} == {"float64"}


@pytest.mark.parametrize("key", RECORDED)
def test_the_temporal_recipe_is_the_spec_recipe(key: str) -> None:
    """Detects a fabricated instant for the raw-clock families or a wrong anchor: the ASDP, window
    and load-factor families take ``none`` (the capture time), the ODFM instructed volumes
    ``sp_pair`` on the settlement date and period and STOR ``utc_instant`` of
    ``service_delivery_from``."""
    recipe = _record(key).temporal
    expected = TEMPORAL_OF[key]
    assert recipe.kind == expected["kind"]
    assert recipe.date_column == expected.get("date_column")
    assert recipe.period_column == expected.get("period_column")
    assert recipe.column == expected.get("column")


@pytest.mark.parametrize("key", [INST, WIN])
def test_an_asdp_family_cannot_take_ckan_last_modified(key: str) -> None:
    """Detects an ASDP record drifting to ``ckan_last_modified`` (V-8): the datastore dumps carry a
    blank or administrative CKAN instant, so a copy of the record with that vintage fails the load
    rule while the record itself passes it."""
    record = _record(key)
    kinds = frozenset({"datastore"})

    def check(rec: SchemaRecord) -> None:
        validate_record(
            rec,
            key=key,
            kind="tabular",
            legacy=False,
            package_families={key: True},
            family_url_types=kinds,
        )

    check(record)
    with pytest.raises(RecordError, match=r"V-8:"):
        check(record.model_copy(update={"vintage": "ckan_last_modified"}))


def test_the_resource_url_types_match_the_recorded_vintages() -> None:
    """Detects the V-8 premise drifting: both ASDP packages hold datastore resources only, the
    ODFM CSV families and the STOR table are uploads."""
    for package in (PACKAGE_OF[INST], PACKAGE_OF[WIN]):
        assert {r["url_type"] for r in _package_doc(package)["resources"]} == {"datastore"}
    for package in (PACKAGE_OF[ODI], PACKAGE_OF[STOR]):
        assert {r["url_type"] for r in _package_doc(package)["resources"]} == {"upload"}


# --------------------------------------------------------------------------- #
# Eligibility, the no-record family and the resource HOLD
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_three_eligible_records_carry_no_override(key: str) -> None:
    """Detects a hold or a per-record override on a clean family: each of the three is effectively
    eligible, in an eligible package."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", HELD)
def test_the_two_held_records_carry_their_verbatim_question_and_the_unit(key: str) -> None:
    """Detects a hold dropped, reworded or filed under the wrong unit, or a package-wide hold: the
    record is ``held`` with the spec's question verbatim, unit ``v0.22-K-AS-5b``, effectively held,
    while its package stays eligible."""
    package, family = registry_module.load_registry().families[key]
    eligibility = _record(key).eligibility
    assert eligibility == Held(status="held", question=QUESTION_OF[key], unit=UNIT)
    assert effective_eligibility(package, family) == eligibility
    assert package.eligibility == Eligible(status="eligible")
    assert "\n" not in eligibility.question


def test_the_four_packages_stay_eligible_and_hold_exactly_the_five_records() -> None:
    """Detects a package held by accident, a record on the wrong family or a family gaining a
    transformer: the four package files are ``eligible``, hold exactly the five records, no family
    has a bespoke transformer."""
    found: set[str] = set()
    for package in sorted(set(PACKAGE_OF.values())):
        document = _package_doc(package)
        assert document["eligibility"] == {"status": "eligible"}, package
        found |= {f["key"] for f in document["families"] if "record" in f}
        assert all(f["transformer"] is None for f in document["families"]), package
    assert found == set(RECORDED)


def test_odfm_results_have_no_record_and_the_family_and_resources_are_unchanged() -> None:
    """Detects ``odfm_market_information_results`` gaining a record, a transformer, an occurrence
    index or a changed disposition (RULINGS 538): both bodies repeat complete rows (9 and 170 exact
    duplicates) and ``Unique ID`` collides, so no vendor-column key passes the duplicate guard. The
    family entry is the pre-unit literal, both resources stay ``SILVER`` of the family, the family
    stays ingest-only (``get_transformer`` raises) and the report lists it ingest-only. The eight
    PDFs stay ``DOC``. TODO: ODFM-RESULTS-ROW-ID, the CP1252 decoding candidate and the mixed
    report-period ``Date`` labels (non-holding)."""
    package, entry = registry_module.load_registry().families[ODR]
    assert entry.record is None and entry.transformer is None
    assert package.package == PACKAGE_OF[ODR]
    document = _package_doc(PACKAGE_OF[ODR])
    (family,) = [f for f in document["families"] if f["key"] == ODR]
    assert family == {
        "key": ODR,
        "kind": "tabular",
        "legacy": False,
        "archetype": "EVT",
        "refresh": "frozen",
        "empty_allowed": False,
        "max_download_bytes": 536870912,
        "name_regex": None,
        "transformer": None,
    }
    results = [r for r in document["resources"] if r["family"] == ODR]
    assert len(results) == 2
    assert all(r["disposition"] == {"kind": "SILVER", "key": ODR} for r in results)
    files = [r for r in document["resources"] if r["family"] == ODF]
    assert len(files) == 8 and all(r["disposition"] == {"kind": "DOC"} for r in files)
    with pytest.raises(ValueError, match="No transformer registered"):
        get_transformer(SOURCE, ODR, Path("__test__"))
    line = _report_line(ODR)
    assert "ingest-only (no silver output)" in line


def test_the_odfm_v1_upload_is_a_resource_hold_with_the_verbatim_reason_and_v2_stays_silver() -> (
    None
):
    """Detects the V1 upload loaded, filed as an ADR-040 entry or reworded: the resource carries
    ``HOLD`` with the spec's reason verbatim and unit ``v0.22-K-AS-5b``, V2 stays ``SILVER`` of
    the instructed family, and no other ODFM resource changed disposition."""
    document = _package_doc(PACKAGE_OF[ODI])
    by_id = {r["id"]: r for r in document["resources"]}
    assert by_id[V1_ID]["disposition"] == {"kind": "HOLD", "reason": V1_REASON, "unit": UNIT}
    assert by_id[V2_ID]["disposition"] == {"kind": "SILVER", "key": ODI}
    assert by_id[V1_ID]["family"] == by_id[V2_ID]["family"] == ODI
    holds = [r["id"] for r in document["resources"] if r["disposition"]["kind"] == "HOLD"]
    assert holds == [V1_ID]
    assert "\n" not in V1_REASON


def test_the_odfm_v1_header_cannot_form_a_frozen_epoch() -> None:
    """Detects a V1 epoch being frozen anyway: the V1 header is the four named fields plus 46
    empty names, and ``HeaderEpoch`` rejects a repeated header name, so the upload could only ever
    be a resource HOLD (the first four cells equal V2's)."""
    cells = [n.strip() for n in next(csv.reader([body("o1").decode("utf-8").splitlines()[0]]))]
    assert cells[:4] == H_V2 and cells[4:] == [""] * 46 and len(cells) == 50
    columns = tuple(
        ColumnSpec(source=c, name=f"c{i}", dtype="string", nullable=True)
        for i, c in enumerate(cells)
    )
    with pytest.raises(ValidationError, match="repeats a name"):
        HeaderEpoch(header=tuple(cells), columns=columns, issue=IssueRecipe(kind="none"))


def _report_line(key: str) -> str:
    lines = eligibility_module.render(load_registry()).splitlines()
    (line,) = [ln for ln in lines if f"| `{key}` |" in ln]
    return line


def test_the_eligibility_report_lists_three_eligible_two_held_and_results_ingest_only() -> None:
    """Detects a family reported with the wrong status: the report lists each eligible family as
    ``silver`` / ``eligible`` (``whole_capture per resource_id`` for ASDP instructions and the ODFM
    volumes, the capture-time vintage for ASDP and the CKAN last-modified for the uploads), each
    held family as ``silver`` / ``held: TODO:`` with its unit and the results as ingest-only."""
    for key in ELIGIBLE:
        line = _report_line(key)
        assert "| silver | eligible |" in line, key
        assert ("whole_capture per resource_id" in line) == (key in PARTITIONED), key
        assert ("gridflow capture time" in line) == (key in CAPTURE_FALLBACK), key
    assert "sp_pair" in _report_line(ODI) and "utc_instant" in _report_line(STOR)
    assert "| silver | held: TODO: ASDP-WINDOW-PRICE-UNIT" in _report_line(WIN)
    assert "| silver | held: TODO: ODFM-LOAD-FACTOR-CLOCK-SCALE" in _report_line(ODL)
    for key in HELD:
        assert f"(unit {UNIT})" in _report_line(key), key
    assert "ingest-only (no silver output)" in _report_line(ODR)


def test_the_frozen_ledger_holds_the_five_families_and_no_adjudication_names_them() -> None:
    """Detects a renamed family or an ADR-040 entry filed for this unit: the five families are
    already frozen under their packages and the reconcile ledger names none of the six AS-5b
    families (no admitted capture fails)."""
    frozen = {row.key: row.package for row in registry_module.load_frozen_keys()}
    for key in (*RECORDED, ODR):
        assert frozen[key] == PACKAGE_OF[key], key
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    assert {e.family for e in entries} & {*RECORDED, ODR, ODF} == set()


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", LOADING)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, alias: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled, repaired or null-for-zero cell, a
    duplicate key or an excluded row: the slice loads every populated row with zero exclusions and
    no failure, every identifier, label, date, instant and measure equals the body cell, the entity
    key is unique and non-null, and the temporal column is the settlement-period start, the UTC
    delivery start or, with no recipe, the capture time. A cell failing its strict cast (an ODFM
    period or date, a STOR instant or any ``float64``) would fail here."""
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
        assert frame["timestamp_utc"].to_list() == frame["capture_written_at"].to_list()
    elif recipe == "utc_instant":
        assert frame["timestamp_utc"].to_list() == frame["service_delivery_from"].to_list()
    else:
        assert frame["timestamp_utc"].to_list() == [
            _sp_start(d, p)
            for d, p in zip(frame["settlement_date"], frame["settlement_period"], strict=True)
        ]
    assert frame["event_time"].to_list() == frame["timestamp_utc"].to_list()


def test_the_asdp_epochs_load_in_one_family(data: Path) -> None:
    """The D8 body (i1) and three D11 bodies (i2-i4) load together under their own resources.

    Detects one epoch shadowing another, a typed ASDP time (the ``... Z`` space-separated label of
    the first body and the naive ISO label of the later ones are both kept byte-equal as strings),
    a sign applied to ``RDP_NEGATIVE`` (MW stays positive ``7.0``), the blank MW / price row read
    as zero (both null), ``Non-Tendered`` unified with ``Non-tendered`` (both kept as published,
    as are ``Tendered`` and the blank), a D8 row gaining a party or asset (null), or ``EntryID``
    cast (string, byte-equal)."""
    frame = _load_clean(data, "i1", "i2", "i3", "i4")
    assert set(frame["resource_id"].to_list()) == {
        CAPTURES[a].resource_id for a in ("i1", "i2", "i3", "i4")
    }
    assert not frame.select(KEY_OF[INST]).is_duplicated().any()
    assert frame.schema["entry_id"] == pl.String and frame.schema["mw"] == pl.Float64
    d8 = frame.filter(pl.col("resource_id") == CAPTURES["i1"].resource_id)
    assert d8["instruction_start_date_time"].to_list()[0] == "2020-06-25 01:49:00Z"
    assert set(d8.select("party_id", "asset_id", "tendered").unpivot()["value"].to_list()) == {None}
    d11 = frame.filter(pl.col("resource_id") == CAPTURES["i2"].resource_id)
    assert d11["instruction_start_date_time"].to_list()[0] == "2021-11-01T12:11:00"
    negative = frame.filter(pl.col("service_type") == "RDP_NEGATIVE")
    assert negative["mw"].to_list() == [7.0] and negative["tendered"].to_list() == ["Non-tendered"]
    blank = frame.filter(pl.col("entry_id") == "13737")
    assert blank["mw"].to_list() == [None] and blank["indicative_price"].to_list() == [None]
    assert blank["dispatch_date_time_stamp"].to_list() == [None]
    assert set(frame["tendered"].drop_nulls().to_list()) == {
        "Tendered",
        "Non-tendered",
        "Non-Tendered",
    }
    assert frame["tendered"].null_count() > 0
    assert all(isinstance(e, str) for e in frame["entry_id"].to_list())
    assert sorted(frame["entry_id"].to_list()) == sorted(
        r["EntryID"] for a in ("i1", "i2", "i3", "i4") for r in rows(a)
    )


def test_window_prices_keep_two_rows_sharing_an_entry_id_under_distinct_keys(data: Path) -> None:
    """Detects EntryID used as the identity: in the 2025 resource EntryID ``51279`` is published
    twice (the same asset re-priced), the two rows differ in ``created_time`` and both are kept
    under the key ``(resource_id, created_time, asset_id)``; ``window_price`` is float64 as
    published and the SP labels and ``created_time`` stay byte-equal strings."""
    frame = _load_clean(data, "w1")
    shared = frame.filter(pl.col("entry_id") == "51279")
    assert shared.height == 2
    assert shared["created_time"].n_unique() == 2
    assert shared.select(KEY_OF[WIN]).n_unique() == 2
    assert frame.schema["window_price"] == pl.Float64
    assert frame["created_time"].to_list() == [r["CreatedTime"] for r in rows("w1")]
    assert frame["sp_start_date_time"].to_list()[0] == "2025-01-28T16:00:00"
    assert frame.schema["sp_start_date_time"] == pl.String


def test_odfm_instructed_v2_places_periods_on_the_uk_settlement_day(data: Path) -> None:
    """Detects a settlement period placed on a naive or shifted clock: V2 04/07/2020 periods 47 and
    48 start 2020-07-04T22:00Z and 22:30Z (BST, so 23:00 and 23:30 local), 05/07/2020 periods 1 and
    2 start 2020-07-04T23:00Z and 23:30Z (the next-day anchor), ``time_label`` keeps the
    corroborating ``Time`` cell as a string, the zero MW stays ``0.0`` and the period survives
    its ``1..50`` bound."""
    frame = _load_clean(data, "o2")
    assert frame["settlement_period"].to_list() == [47, 48, 1, 2]
    assert frame["settlement_date"].to_list() == [date(2020, 7, 4)] * 2 + [date(2020, 7, 5)] * 2
    assert frame["timestamp_utc"].to_list() == [
        datetime(2020, 7, 4, 22, 0, tzinfo=UTC),
        datetime(2020, 7, 4, 22, 30, tzinfo=UTC),
        datetime(2020, 7, 4, 23, 0, tzinfo=UTC),
        datetime(2020, 7, 4, 23, 30, tzinfo=UTC),
    ]
    assert frame["time_label"].to_list() == ["23:00", "23:30", "00:00", "00:30"]
    assert frame["odfm_instructed_mw"].to_list()[:2] == [0.0, 0.0]
    assert frame.schema["settlement_period"] == pl.Int64
    assert frame.schema["settlement_date"] == pl.Date


def test_the_held_v1_upload_is_never_transformed(data: Path) -> None:
    """Detects the held V1 resource loading next to V2: with both captures present only V2's four
    rows reach silver (the V1 body is wrong by one period on every row) and V1 gets neither a
    completion nor a failure record."""
    v1_id = capture(data, "o1")
    v2_id = capture(data, "o2")
    written = get_transformer(SOURCE, ODI, data).run(DAY, run_id="r")
    frame = _silver(data, ODI)
    assert written == frame.height == len(rows("o2"))
    assert set(frame["resource_id"].to_list()) == {V2_ID}
    assert read_completion(data, ODI, v1_id) is None and read_failure(data, ODI, v1_id) is None
    completion = read_completion(data, ODI, v2_id)
    assert completion is not None and completion["outcome"] == "populated"


def test_load_factors_keep_zero_solar_and_unit_other_values_unscaled(data: Path) -> None:
    """Detects a percentage rescale, a zero read as missing or a Datetime typed: the first two
    labels carry a Solar ``0`` (kept ``0.0``, never null) and every Other column is ``1`` (kept
    ``1.0``), a nonzero Solar value (``0.000982857``) is unscaled, ``datetime`` stays the raw
    ``%d/%m/%Y %H:%M`` string and ``timestamp_utc`` is the capture time."""
    frame = _load_clean(data, "l1")
    solar = [c for c in frame.columns if c.endswith("_solar")]
    other = [c for c in frame.columns if c.endswith("_other")]
    assert len(solar) == len(other) == 14
    first = frame.row(0, named=True)
    assert {first[c] for c in solar} == {0.0} and {first[c] for c in other} == {1.0}
    assert all(frame[c].null_count() == 0 for c in (*solar, *other))
    assert any(v not in (0.0, 1.0) and 0.0 < v < 1.0 for c in solar for v in frame[c].to_list())
    assert frame["datetime"].to_list() == [r["Datetime"] for r in rows("l1")]
    assert frame.schema["datetime"] == pl.String
    assert max(frame[c].max() for c in frame.columns if c.endswith(("_wind", "_solar"))) < 1.0


def test_stor_keeps_23_24_and_25_hour_service_days_and_never_zeroes_a_rejection(
    data: Path,
) -> None:
    """Detects a delivery instant shifted as if it were UK local time or a rejected row given a
    zero: the literal-``Z`` cells are UTC (the spring 2026-03-28 05:00Z to 2026-03-29 04:00Z service
    day is 23 hours, 2026-03-31 04:00Z to 2026-04-01 04:00Z is 24, the autumn 2025-10-25 04:00Z to
    2025-10-26 05:00Z day is 25), a rejected row keeps ``contracted_mw`` and
    ``market_clearing_price`` null, a blank ``Unit ID`` / ``Buy Curve ID`` is null, the 0 minimum
    acceptable MW stays ``0.0`` and ``timestamp_utc`` is ``service_delivery_from``."""
    frame = _load_clean(data, "s1")
    spans = {
        (a, b): (b - a).total_seconds() / 3600
        for a, b in zip(frame["service_delivery_from"], frame["service_delivery_to"], strict=True)
    }
    assert sorted(set(spans.values())) == [23.0, 24.0, 25.0]
    assert (
        spans[(datetime(2026, 3, 28, 5, 0, tzinfo=UTC), datetime(2026, 3, 29, 4, 0, tzinfo=UTC))]
        == 23.0
    )
    assert (
        spans[(datetime(2025, 10, 25, 4, 0, tzinfo=UTC), datetime(2025, 10, 26, 5, 0, tzinfo=UTC))]
        == 25.0
    )
    assert frame.schema["service_delivery_from"] == pl.Datetime("us", "UTC")
    assert frame.schema["service_delivery_to"] == pl.Datetime("us", "UTC")
    assert frame["timestamp_utc"].to_list() == frame["service_delivery_from"].to_list()
    rejected = frame.filter(pl.col("tender_submission_id") == "TS-127216")
    assert rejected["status"].to_list() == ["Rejected"]
    assert rejected["contracted_mw"].to_list() == [None]
    assert rejected["market_clearing_price"].to_list() == [None]
    assert rejected["tendered_availability_price"].to_list() == [3.4]
    zero = frame.filter(pl.col("tender_submission_id") == "TS-125060")
    assert zero["minimum_acceptable_mw"].to_list() == [0.0]
    assert zero["contracted_mw"].to_list() == [None]
    assert frame.filter(pl.col("tender_submission_id") == "TS-027918")["unit_id"].to_list() == [
        None
    ]
    assert frame.filter(pl.col("tender_submission_id") == "TS-094251")[
        "buy_curve_id"
    ].to_list() == [None]
    assert frame["tender_submission_id"].to_list() == [
        r["Tender Submission ID"] for r in rows("s1")
    ]


# --------------------------------------------------------------------------- #
# Loud failure and vintage
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


@pytest.mark.parametrize("family", RECORDED)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, family: str) -> None:
    """Detects a duplicate key loaded silently or deduplicated: the first populated data row
    repeated at the end of the body fails the capture with ``DuplicateEntityKeyError`` (for ASDP
    instructions the repeated ``(resource_id, entry_id)``), loads nothing and leaves no completion
    (the held records fail the same way)."""
    alias = FIRST_OF[family]
    newline = b"\r\n" if alias not in LF_ALIASES else b"\n"
    lines = body(alias).split(newline)
    assert lines[-1] == b""
    raw = newline.join([*lines[:-1], lines[1], b""])
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_a_period_outside_one_to_fifty_is_excluded_with_a_tally(data: Path) -> None:
    """Detects the ``1..50`` bound missing on the ODFM instructed volumes: a 51st period breaches
    the ``settlement_period`` bound, so the row is excluded with a tally (``rows_excluded`` 1,
    capture still populated) and the other three rows load unchanged."""
    raw = body("o2").replace(b"\r\n47,", b"\r\n51,", 1)
    assert raw != body("o2")
    capture_id = capture(data, "o2", raw=raw)
    written = get_transformer(SOURCE, ODI, data).run(DAY, run_id="r")
    assert written == len(rows("o2")) - 1
    completion = read_completion(data, ODI, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 1)
    assert 51 not in _silver(data, ODI)["settlement_period"].to_list()


@pytest.mark.parametrize("alias", ["i1", "w1", "o2", "l1", "s1"])
def test_the_vintage_is_the_capture_time_for_datastore_and_ckan_last_modified_for_uploads(
    data: Path, alias: str
) -> None:
    """Detects the vintage drifting: ``available_at`` is the capture's ``written_at`` for the two
    ASDP datastore families and the CKAN ``last_modified`` read as UTC for the ODFM and STOR
    uploads. An as-of read before it serves nothing and one after serves the capture, in the
    DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    written = datetime.fromisoformat(meta.written).astimezone(UTC)
    if meta.family in CAPTURE_FALLBACK:
        vintage = written
    else:
        assert meta.modified is not None
        vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert both_as_of(db, data, meta.family, vintage - timedelta(microseconds=1)) == []
    assert set(both_as_of(db, data, meta.family, vintage + timedelta(days=1))) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}
