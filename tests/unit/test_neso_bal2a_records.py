"""The BSUoS monthly forecast summary record and the families that get none (v0.22-K-BAL-2a).

``bsuos_monthly_forecast_fc_summary`` (75 resources, one body each) gets a held, edition-mapped,
resource-partitioned record over its eight valid-UTF-8 reader epochs; its 22 invalid-UTF-8
resources fail the UTF-8 gate and are adjudicated (ADR-040 amendment 1, ruling 667). The seven
other monthly families and the daily forecast get **no record** (RULINGS 538/653 precedent): every
newest body is invalid UTF-8 with no vendor encoding declaration, and the daily body carries no run
column. Every test writes recorded fixture captures (cuts of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/bal2a/``, provenance in ``PROVENANCE.md``) into a short data root
and runs the transformer the **real package registry** generates, so a record that does not fit its
vendor body fails here, not at activation. On master the family has no record, so
``get_transformer`` raises.

Record decisions under test (K-BAL-2-FACTS g1-g5, §4, K-BAL-2a-SPEC):

- The monthly CSVs are long tables, one resource per edition: the key is (``resource_id``,
  ``edition``, ``month_vendor``) and the newest capture is selected per resource. ``edition`` is
  the engine's stamp from the exact-filename map (ADR-042), each pair justified by the resource
  name and filename carrying the same month. The vendor ``Month`` label stays a raw string
  (``%b-%y`` and ``%d/%m/%Y`` spellings both occur) in ``month_vendor`` (``month`` is a reserved
  Hive partition name); temporal ``none`` (TODO BSUOS-MONTH-CLOCK).
- Units only where the header declares them: ``£m`` is ``_gbp_m``, ``(TWh)`` is ``_twh``. The bare
  July 2023 ``Volume`` is its own unit-less column (TODO T127), never merged with the TWh column;
  ``Additional NESO Framework Costs (£m)`` carries no unit suffix (TODO FRAMEWORK-COST-UNIT: the
  header says £m, the dictionary TWh). Trailing anonymous headers stay nullable string columns.
- Held: the issue instant, the forward-target rule and the units are open (the record model has no
  notes field, so the TODOs live in the held question and in ``PROVENANCE.md``).
- Seat ruling 678: March, April and May 2024 carry the accounting token `` -   `` (and padded
  numerals), which the strict cast refuses; those three captures fail with
  ``InvalidOperationError`` rather than load a guess (D-41) and stay open ``failed`` gaps with no
  adjudication entry. The lexical cleanup is the follow-up unit BAL-2aH.

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the bronze
originals' CRLF convention; a BOM stays where the original had one."""

from __future__ import annotations

import csv
import io
import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
from gridflow.connectors.neso_data_portal.registry import (
    RECONCILE_ADJUDICATIONS_FILE,
    Held,
    HoldDisposition,
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "bal2a"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = datetime(2026, 10, 8, tzinfo=UTC).date()
POUND = "£"
FAMILY = "bsuos_monthly_forecast_fc_summary"
PACKAGE = "bsuos-monthly-forecast"
PACKAGE_ID = "c0376ed7-3205-4fe2-9496-28496f1f287a"
UNIT = "v0.22-K-BAL-2a"

HELD_QUESTION = (
    "TODO: the issue instant and availability of each monthly edition (filename month is an "
    "edition label, not an issue time); 50 of the 53 UTF-8 editions load, and March-May 2024 "
    "fail the strict numeric cast until BAL-2aH lands; the loaded editions include targets on or "
    "before their upload (RULINGS 529 forward-target rule fails); the target-month clock "
    "(BSUOS-MONTH-CLOCK); the units of bare `Volume` (July 2023) and `Additional NESO Framework "
    f"Costs ({POUND}m)` (dictionary says TWh); and whether `' - '` in a cost column means zero "
    "(the January and February 2024 editions read `0` for the same target months) or "
    "not-available."
)
FILES_HOLD = (
    "TODO: WORKBOOK-CHILD-RECORDS — 15 workbooks / 151 sheets; table bounds, identifiers, units, "
    "time and issue semantics unsettled per child (FACTS BAL-2a-3); no overlap establishes "
    "COVERED."
)
NO_RECORD = {
    "bsuos_daily_forecast": ("balancing-services-use-of-system-bsuos-daily-forecast", 5),
    "bsuos_monthly_forecast_actual_sum": (PACKAGE, 22),
    "bsuos_monthly_forecast_fc_summary_pct": (PACKAGE, 12),
    "bsuos_monthly_forecast_impact_on_due": (PACKAGE, 1),
    "bsuos_monthly_forecast_including_new": (PACKAGE, 1),
    "bsuos_monthly_forecast_sum_winter": (PACKAGE, 1),
    "bsuos_monthly_forecast_summary": (PACKAGE, 18),
    "bsuos_monthly_forecast_summary_pct": (PACKAGE, 12),
}

B = [f"{n}_{POUND}m" for n in ("Energy_Imbalance", "Positive_Reserve", "Negative_Reserve")] + [
    f"Frequency_Control_{POUND}m",
    f"Other_{POUND}m",
    f"Constraints_{POUND}m",
    f"Restoration_{POUND}m",
]
B_PRIME = [*B[:4], B[5], B[4], B[6]]
C = [f"Balancing Costs (Central) {POUND}m", f"Estimated Internal BSUoS & ESO Incentive {POUND}m"]
WINTER = f"Winter Security of Supply Cost ({POUND}m)"
UNDER = f"21/22 Under-Recovered Costs ({POUND}m)"
TWH = "Estimated BSUoS Volume (TWh)"
J = ["Month", *B_PRIME, *C]
EPOCH_HEADERS: dict[str, list[str]] = {
    "f4": [
        "Month",
        *B,
        *C,
        f"BSUoS Cost Recovery {POUND}m",
        f"ALoMCP {POUND}m",
        f"CMP345/350 Deferred Costs {POUND}m",
        f"CMP381 Deferred Costs {POUND}m",
        TWH,
        "",
    ],
    "f5": [
        "Month",
        *[b for b in B if not b.startswith("Restoration")],
        *C,
        f"BSUoS Cost Recovery {POUND}m",
        f"ALoMCP {POUND}m",
        f"CMP345/350 Deferred Costs {POUND}m",
        f"CMP381 Deferred Costs {POUND}m",
        TWH,
        "",
    ],
    "f7": ["Month", *B, *C, f"ALoMCP {POUND}m", f"CMP381 Deferred Costs {POUND}m", TWH],
    "f8": [
        "Month",
        *B,
        *C,
        f"ALoMCP {POUND}m",
        f"CMP381 Deferred Costs {POUND}m",
        f"Winter Contingency Cost (Central) {POUND}m",
        TWH,
    ],
    "f10": [
        "Month",
        *[*B[:4], B[5], B[4], B[6]],
        *C,
        f"ALoMCP {POUND}m",
        f"CMP381 Deferred Costs {POUND}m",
        f"Winter Contingency Cost (Central) {POUND}m",
        WINTER,
        "Volume",
    ],
    "f11": [*J, WINTER, TWH],
    "f12": [*J, WINTER, UNDER, TWH],
    "f12p": [*J, WINTER, UNDER, TWH],
    "f13": [
        *J,
        WINTER,
        UNDER,
        f"CMP398/412 Claim Costs ({POUND}m)",
        f"Interest Repayment ({POUND}m)",
        f"Additional NESO Framework Costs ({POUND}m)",
        TWH,
    ],
}
FINAL_TAIL = [
    f"CMP398/412 Claim Costs ({POUND}m)",
    f"Interest Repayment ({POUND}m)",
    f"Additional NESO Framework Costs ({POUND}m)",
    TWH,
]
EPOCH_HEADERS["f14"] = [
    *J,
    *(f" {h} " for h in (WINTER, UNDER, *FINAL_TAIL)),
]
EPOCH_HEADERS["f15"] = [
    *J,
    f"{WINTER} ",
    f" {UNDER} ",
    f"{FINAL_TAIL[0]} ",
    f" {FINAL_TAIL[1]} ",
    f"{FINAL_TAIL[2]} ",
    f"{FINAL_TAIL[3]} ",
]
CLEAN = ("f4", "f5", "f7", "f8", "f10", "f11", "f12", "f13", "f14", "f15")
BLANK_ROWS = {"f4": 1, "f5": 1, "f14": 1}


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    fixture: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str


CAPTURES: dict[str, Capture] = {
    "f4": Capture(
        "f4.csv",
        "98fea024-d154-4f4b-840e-5e84e5c649ff",
        "Monthly BSUoS Forecast Summary February 2022",
        "bsuos-forecast-february-2022-csv.csv",
        "2022-02-07T11:59:38.611428",
        "2026-10-08T09:08:57.387457+00:00",
    ),
    "f5": Capture(
        "f5.csv",
        "db352475-1dc7-462a-9f57-cfcf269fa5f2",
        "Monthly BSUoS Forecast Summary March 2022",
        "bsuos-forecast-march-2022-csv.csv",
        "2022-02-15T10:17:32.192803",
        "2026-10-08T09:09:00.380021+00:00",
    ),
    "f7": Capture(
        "f7.csv",
        "aef8c119-d9e3-496a-8185-e1be35f6f71b",
        "Monthly BSUoS Forecast Summary June 2022",
        "bsuos-forecast-june-2022.csv",
        "2022-05-16T08:46:29.064800",
        "2026-10-08T09:09:08.666111+00:00",
    ),
    "f8": Capture(
        "f8.csv",
        "8ddd61ed-1003-479a-bc78-6dfe0f91da6b",
        "Monthly BSUoS Forecast Summary October 2022",
        "bsuos-forecast-october-2022.csv",
        "2022-09-12T13:38:02.188222",
        "2026-10-08T09:09:20.389072+00:00",
    ),
    "f10": Capture(
        "f10.csv",
        "3ed31178-bd64-41ce-8665-85b77db7aaaf",
        "Monthly BSUoS Forecast Summary July 2023",
        "bsuos-forecast-july-2023.csv",
        "2023-06-21T09:48:54.397478",
        "2026-10-08T09:09:45.225423+00:00",
    ),
    "f11": Capture(
        "f11.csv",
        "60ee18d3-b999-4413-aac5-82a30d2a982c",
        "Monthly BSUoS Forecast Summary August 2023",
        "bsuos-forecast-aug-2023.csv",
        "2023-07-14T13:17:07.462570",
        "2026-10-08T09:09:48.332400+00:00",
    ),
    "f12": Capture(
        "f12.csv",
        "963cf73a-138e-4521-986b-12079f45b78d",
        "Monthly BSUoS Forecast Summary February 2024",
        "bsuos-forecast-feb-2024.csv",
        "2024-01-16T10:39:25.887608",
        "2026-10-08T09:10:06.148143+00:00",
    ),
    "f12p": Capture(
        "f12p.csv",
        "9120ae56-f4b5-4512-baaa-e20f8b31a014",
        "Monthly BSUoS Forecast Summary March 2024",
        "bsuos-forecast-mar-2024.csv",
        "2024-02-15T10:22:00.124305",
        "2026-10-08T09:10:08.826909+00:00",
    ),
    "f13": Capture(
        "f13.csv",
        "2d5f0be1-2aab-4ab7-af28-c67a2f38db6b",
        "Monthly BSUoS Forecast Summary August 2024",
        "bsuos-forecast-august-2024.csv",
        "2024-07-16T10:56:16.329068",
        "2026-10-08T09:10:23.252614+00:00",
    ),
    "f14": Capture(
        "f14.csv",
        "495879f8-4605-4ade-b4d1-7b1d37a163fa",
        "Monthly BSUoS Forecast Summary March 2025",
        "bsuos-forecast-march-2025.csv",
        "2025-02-14T14:32:38.559125",
        "2026-10-08T09:10:42.524960+00:00",
    ),
    "f15": Capture(
        "f15.csv",
        "b458c4e5-ab80-43e5-907e-6c7069350390",
        "Monthly BSUoS Forecast Summary - October 2026",
        "bsuos-forecast-october-2026.csv",
        "2026-09-21T10:38:16.472405",
        "2026-10-08T09:11:36.626700+00:00",
    ),
    "bad": Capture(
        "bad.csv",
        "ef004890-679a-4d28-ac00-427ff6e7260d",
        "Monthly BSUoS Forecast Summary Mar_20",
        "monthly-bsuos-forecast-summary-mar_20.csv",
        "2021-01-07T09:58:10.509371",
        "2026-10-08T09:08:05.125065+00:00",
    ),
}
EDITION_OF = {
    "f4": 202202,
    "f5": 202203,
    "f7": 202206,
    "f8": 202210,
    "f10": 202307,
    "f11": 202308,
    "f12": 202402,
    "f13": 202408,
    "f14": 202503,
    "f15": 202610,
}


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings (and so the CLI) point at."""
    with tempfile.TemporaryDirectory(
        prefix="ba2", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's CRLF line endings (a BOM stays)."""
    raw = (FIXTURES / CAPTURES[alias].fixture).read_bytes()
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    text = raw[len(bom) :].replace(b"\r\n", b"\n")
    return bom + text.replace(b"\n", b"\r\n")


def _table(alias: str) -> list[list[str]]:
    reader = csv.reader(io.StringIO(body(alias).decode("utf-8-sig"), newline=""))
    return list(reader)


def header(alias: str) -> list[str]:
    """The fixture's raw header (padding included)."""
    return _table(alias)[0]


def rows(alias: str) -> list[dict[str, str]]:
    """The fixture's non-blank records as text, raw-header-keyed (cells verbatim)."""
    head = header(alias)
    return [dict(zip(head, rec, strict=True)) for rec in _table(alias)[1:] if any(rec)]


def capture(
    data: Path,
    alias: str,
    *,
    raw: bytes | None = None,
    resource_id: str | None = None,
    filename: str | None = None,
    written: str | None = None,
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        FAMILY,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(written or meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=PACKAGE,
        package_id=PACKAGE_ID,
        resource_id=resource_id or meta.resource_id,
        resource_name=meta.name,
        resource_filename=filename or meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
        ckan_format="CSV",
        extension="csv",
    )
    return capture_id_for(path, data)


def _record(key: str = FAMILY) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / FAMILY).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _no_silver(data: Path) -> bool:
    return not list((data / "silver" / SOURCE / FAMILY).rglob("[!.]*.parquet"))


def _package_doc(filename: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / filename).read_text(encoding="utf-8"))
    return document


def _columns() -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record()))
        if c not in ("year", "month")
    ]


def _silver_name(alias: str, source: str, position: int) -> str:
    """The silver column a stripped raw header field maps to, from the record's own epoch."""
    stripped = tuple(h.strip() for h in header(alias))
    record = _record()
    (epoch,) = [e for e in record.epochs if e.header == stripped]
    assert epoch.columns[position].source == source.strip()
    return epoch.columns[position].name


def _assert_clean_load(data: Path, alias: str) -> pl.DataFrame:
    """Capture + run ``alias``: every non-blank row written, none excluded, generic columns."""
    capture_id = capture(data, alias)
    written = get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    assert written == len(rows(alias))
    completion = read_completion(data, FAMILY, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
    frame = _silver(data)
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns()
    return frame


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact raw header
    (incl. the ``£`` spellings, the bare ``Volume``, the trailing anonymous field and the
    F14/F15 padding), CRLF endings, the BOMs, the wholly blank rows, both Month spellings, the
    accounting token and padded numerals of March 2024, and the standalone ``0xA3`` bytes of the
    invalid body."""
    for alias, expected in EPOCH_HEADERS.items():
        assert header(alias) == expected, alias
        assert body(alias).count(b"\r\n") == body(alias).count(b"\n"), alias
    for alias in ("f4", "f5", "f10", "f11", "f12", "f12p", "f13", "f14", "f15"):
        assert body(alias).startswith(b"\xef\xbb\xbf"), alias
    for alias in ("f7", "f8"):
        assert not body(alias).startswith(b"\xef\xbb\xbf"), alias
    for alias, blanks in BLANK_ROWS.items():
        table = _table(alias)[1:]
        assert sum(1 for rec in table if not any(c.strip() for c in rec)) == blanks, alias
    assert {r["Month"][3] for r in rows("f4")} == {"-"}
    assert all(r["Month"].count("/") == 2 for r in rows("f10") + rows("f14") + rows("f15"))
    assert all(len(r["Month"]) == 6 for r in rows("f13"))
    assert any(" -   " in r.values() for r in rows("f12p"))
    assert any(
        v != v.strip() and v.strip() not in ("-", "") for r in rows("f12p") for v in r.values()
    )
    assert body("f12").count(b" -   ") == 0
    assert b"\xa3" in body("bad").split(b"\r\n", 1)[0]
    with pytest.raises(UnicodeDecodeError):
        body("bad").decode("utf-8")


# --------------------------------------------------------------------------- #
# Record shape
# --------------------------------------------------------------------------- #


def test_the_record_is_csv_utf8_partitioned_edition_mapped_and_held() -> None:
    """Detects an issue time invented from the filename, a fallback vintage, a family-scope
    selection (which would keep one edition), a per-resource encoding or a published output:
    version 1, utf-8 CSV, ``ckan_last_modified`` vintage (every capture is an upload),
    ``whole_capture`` per ``resource_id``, key (``resource_id``, ``edition``, ``month_vendor``),
    temporal ``none``, no epoch issue recipe, no siblings, and eligibility *held* with the spec's
    question verbatim and unit ``v0.22-K-BAL-2a``."""
    record = _record()
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert (record.vintage, record.vintage_evidence) == ("ckan_last_modified", None)
    assert (record.latest, record.latest_partition) == ("whole_capture", "resource_id")
    assert record.entity_key == ("resource_id", "edition", "month_vendor")
    assert record.temporal.kind == "none"
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    assert has_issue_time(record) is False and record.siblings == ()
    assert isinstance(record.eligibility, Held)
    assert record.eligibility.unit == UNIT and record.eligibility.question == HELD_QUESTION
    registry = load_registry()
    package, entry = registry.families[FAMILY]
    assert isinstance(package.eligibility, registry_module.Eligible)
    assert eligibility_module.effective_eligibility(package, entry) == record.eligibility


def test_eight_reader_epochs_one_per_valid_shape_in_edition_order() -> None:
    """Detects an epoch for an invalid-UTF-8 raw shape (those 22 captures fail the gate before
    epoch matching), the final shape split by header padding (the reader strips it: F13, F14 and
    F15 are one epoch) or an epoch whose header keeps padding: eight stripped headers, in the
    order of their first edition."""
    record = _record()
    assert len(record.epochs) == 8
    expected = [EPOCH_HEADERS[a] for a in ("f4", "f5", "f7", "f8", "f10", "f11", "f12", "f13")]
    assert [list(e.header) for e in record.epochs] == [[h.strip() for h in x] for x in expected]
    for epoch in record.epochs:
        assert all(h == h.strip() for h in epoch.header)
        assert [c.source for c in epoch.columns] == list(epoch.header)
    for alias in ("f14", "f15"):
        assert tuple(h.strip() for h in header(alias)) == record.epochs[-1].header


def test_units_only_where_the_header_declares_them_and_unknowns_stay_unit_less() -> None:
    """Detects a unit guessed onto a column or two quantities merged: every ``£m`` header is a
    ``_gbp_m`` float64 (nullable), the TWh header ``estimated_bsuos_volume_twh``, the July 2023
    bare ``Volume`` its own unit-less ``volume`` (never merged with the TWh column), the
    framework-cost header unit-less (its dictionary unit conflicts), ``Month`` the non-nullable
    string ``month_vendor`` (``month`` is reserved) and each trailing anonymous header a nullable
    string never dropped."""
    record = _record()
    seen: dict[str, str] = {}
    for epoch in record.epochs:
        for col in epoch.columns:
            seen[col.source] = col.name
            if col.source == "Month":
                assert (col.name, col.dtype, col.nullable) == ("month_vendor", "string", False)
            elif col.source == "":
                assert col.dtype == "string" and col.nullable and col.name.startswith("unnamed_")
            else:
                assert col.dtype == "float64" and col.nullable, col.source
                assert col.min is None and col.max is None and col.null_tokens == ()
            if col.source.endswith(f"{POUND}m") or col.source.endswith(f"{POUND}m)"):
                if "Framework" in col.source:
                    assert col.name == "additional_neso_framework_costs"
                else:
                    assert col.name.endswith("_gbp_m"), col.source
    assert seen[TWH] == "estimated_bsuos_volume_twh"
    assert seen["Volume"] == "volume"
    assert seen["Volume"] != seen[TWH]
    assert "Volume" in {c.source for c in record.epochs[4].columns}
    assert TWH not in {c.source for c in record.epochs[4].columns}
    names = {c.name for e in record.epochs for c in e.columns}
    assert "month" not in names and "additional_neso_framework_costs_gbp_m" not in names


def test_every_edition_pair_is_justified_by_the_resource_name_and_distinct() -> None:
    """Detects an edition not carried by the resource name (ADR-042 P-1), a shared edition, a
    gap or an invalid-UTF-8 resource mapped: the 53 pairs' editions are exactly the months the
    75 resource names carry minus the 22 adjudicated resources (March 2020 ... December 2021, May
    2022, April-June 2023), each distinct, February 2022 ... October 2026, every filename unique
    and carrying its edition month."""
    record = _record()
    pairs = record.edition_by_filename
    assert pairs is not None and len(pairs) == 53
    assert len({e for _f, e in pairs}) == 53
    assert min(e for _f, e in pairs) == 202202 and max(e for _f, e in pairs) == 202610
    registry = load_registry()
    (entry,) = _ledger()
    bad = {registry_module.CAPTURE_ID_PATTERN.fullmatch(p)["rid"] for p in entry["captures"]}  # type: ignore[index]
    months = {
        m: i
        for i, m in enumerate(
            ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
            start=1,
        )
    }
    from_names: dict[str, int] = {}
    for rid, (_pkg, res) in registry.resources.items():
        if res.family != FAMILY or rid in bad:
            continue
        label = res.name.removeprefix("Monthly BSUoS Forecast Summary").lstrip(" -")
        word, year = label.split()
        from_names[rid] = int(year) * 100 + months[word[:3].lower()]
    assert len(from_names) == 53 and sorted(from_names.values()) == sorted(e for _f, e in pairs)
    for filename, edition in pairs:
        year, month = divmod(edition, 100)
        assert 1 <= month <= 12
        stem = filename.lower().removesuffix(".csv")
        assert any(key in stem for key in (name for name, i in months.items() if i == month)), (
            filename,
            edition,
        )
        assert str(year) in stem or str(year % 100) in stem, (filename, edition)


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", CLEAN)
def test_every_loaded_epoch_types_with_zero_failures_and_keeps_every_value(
    data: Path, alias: str
) -> None:
    """Detects a cell re-typed or rounded, the Month label parsed (``%b-%y`` and ``%d/%m/%Y``
    both stay raw), a blank cell turned into zero, the padding of the F14/F15 headers breaking
    the epoch match, a wholly blank row failing the capture, the edition not stamped from the map
    or the resource id dropped: the slice loads every non-blank row with zero exclusions, every
    float equals its cell (blank is null, zero stays zero), the anonymous trailing field is
    null-or-string, ``edition`` is the mapped month and the (resource, edition, month) key is
    unique."""
    source = rows(alias)
    frame = _assert_clean_load(data, alias)
    meta = CAPTURES[alias]
    assert frame["resource_id"].to_list() == [meta.resource_id] * len(source)
    assert frame["edition"].to_list() == [EDITION_OF[alias]] * len(source)
    assert frame.schema["month_vendor"] == pl.String
    assert frame["month_vendor"].to_list() == [r["Month"] for r in source]
    head = header(alias)
    for position, raw_name in enumerate(head):
        if raw_name.strip() == "Month":
            continue
        name = _silver_name(alias, raw_name, position)
        if raw_name.strip() == "":
            assert frame.schema[name] == pl.String
            assert frame[name].to_list() == [r[raw_name] or None for r in source]
            continue
        assert frame.schema[name] == pl.Float64, name
        assert frame[name].to_list() == [
            float(r[raw_name]) if r[raw_name] != "" else None for r in source
        ], name
    assert frame.select(["resource_id", "edition", "month_vendor"]).is_duplicated().sum() == 0


def test_the_wholly_blank_rows_are_dropped_not_loaded_and_not_a_failure(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects a wholly blank row (FACTS g5: 22 of them across the bodies) loaded as an
    observation or failing its capture: the blank row of the February 2022 slice never reaches
    silver, the capture still completes ``populated`` with zero exclusions, and the reader
    accounts the drop in its log."""
    assert len(_table("f4")) - 1 == len(rows("f4")) + 1
    with caplog.at_level("INFO"):
        capture_id = capture(data, "f4")
        get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    frame = _silver(data)
    assert frame.height == len(rows("f4")) == 3
    assert frame["month_vendor"].null_count() == 0 and "" not in frame["month_vendor"].to_list()
    completion = read_completion(data, FAMILY, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    assert any(
        "blank" in r.getMessage().lower() or "dropped" in r.getMessage().lower()
        for r in caplog.records
    )


def test_bare_volume_and_the_twh_column_stay_distinct_and_the_framework_cost_has_no_unit(
    data: Path,
) -> None:
    """Detects the July 2023 bare ``Volume`` merged into the TWh column (or its unit guessed),
    and a unit invented for ``Additional NESO Framework Costs (£m)`` (header £m, dictionary TWh):
    July 2023 fills ``volume`` and leaves ``estimated_bsuos_volume_twh`` null, August 2023 the
    reverse; the August 2024 slice's framework-cost column is ``additional_neso_framework_costs``
    and no ``_gbp_m`` / ``_twh`` twin of it exists."""
    capture(data, "f10")
    capture(data, "f11")
    capture(data, "f13")
    get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    frame = _silver(data)
    july = frame.filter(pl.col("edition") == 202307)
    august = frame.filter(pl.col("edition") == 202308)
    assert (
        july["volume"].null_count() == 0
        and july["estimated_bsuos_volume_twh"].null_count() == july.height
    )
    assert august["volume"].null_count() == august.height
    assert august["estimated_bsuos_volume_twh"].null_count() == 0
    assert [float(r["Volume"]) for r in rows("f10")] == july["volume"].to_list()
    assert "additional_neso_framework_costs" in frame.columns
    assert not [c for c in frame.columns if c.startswith("additional_neso_framework_costs_")]
    twenty = frame.filter(pl.col("edition") == 202408)
    assert twenty["additional_neso_framework_costs"].to_list() == [
        float(r[f"Additional NESO Framework Costs ({POUND}m)"]) for r in rows("f13")
    ]


def test_an_unmapped_filename_fails_the_capture_and_loads_nothing(data: Path) -> None:
    """Detects a fallback edition (a regex on the filename, the resource name or the capture
    date): a capture whose ``resource_filename`` is not in the map fails with
    ``UnmappedResourceEditionError``, leaves no completion and no output."""
    capture_id = capture(data, "f13", filename="bsuos-forecast-august-2024-v2.csv")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "UnmappedResourceEditionError")
    ]
    assert read_completion(data, FAMILY, capture_id) is None
    assert _no_silver(data)


def test_march_to_may_2024_numerals_the_strict_cast_refuses_fail_the_capture_not_a_guess(
    data: Path,
) -> None:
    """Detects a vendor token coerced to null or zero (FACTS g5: no undocumented token becomes
    null) or the padded numerals trimmed by guess. The March 2024 slice (padded ``' 4.0 '`` and the
    accounting token ``' -   '``) fails its capture with the strict-cast ``InvalidOperationError``
    (D-41), writes no row and leaves a failure record, while the same-epoch February 2024 slice
    with plain cells loads. Seat ruling 678: the three captures (March-May 2024) stay open
    ``failed`` gaps with no adjudication entry (``InvalidOperationError`` is not an ADR-040 cause);
    the lexical numeric cleanup is the follow-up unit BAL-2aH."""
    clean = capture(data, "f12")
    padded = capture(data, "f12p")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [(padded, "InvalidOperationError")]
    failure = read_failure(data, FAMILY, padded)
    assert failure is not None and failure["error_class"] == "InvalidOperationError"
    assert read_completion(data, FAMILY, padded) is None
    done = read_completion(data, FAMILY, clean)
    assert done is not None and done["outcome"] == "populated"
    frame = _silver(data)
    assert set(frame["edition"].to_list()) == {EDITION_OF["f12"]}


# --------------------------------------------------------------------------- #
# The invalid-UTF-8 resources: ADR-040 adjudication
# --------------------------------------------------------------------------- #


def test_an_invalid_utf8_body_fails_with_unicode_decode_error_and_is_never_redecoded(
    data: Path,
) -> None:
    """Detects a cp1252 / Latin-1 re-decode, a replacement character or a dropped byte: the
    March 2020 body (standalone ``0xA3`` in its currency headers) fails by itself with
    ``UnicodeDecodeError`` and the strict decoder's own message, leaves no completion and no
    output, and the valid sibling in the same run still completes with every row."""
    bad = capture(data, "bad")
    good = capture(data, "f13")
    with pytest.raises(UnicodeDecodeError) as decoded:
        body("bad").decode("utf-8")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [(bad, "UnicodeDecodeError")]
    failure = read_failure(data, FAMILY, bad)
    assert failure is not None and failure["error_class"] == "UnicodeDecodeError"
    assert failure["message"] == str(decoded.value)
    assert read_completion(data, FAMILY, bad) is None
    completion = read_completion(data, FAMILY, good)
    assert completion is not None and completion["outcome"] == "populated"
    frame = _silver(data)
    assert set(frame["edition"].to_list()) == {EDITION_OF["f13"]}
    assert all("�" not in str(v) for col in frame.columns for v in frame[col].to_list())


def _ledger() -> list[dict[str, Any]]:
    entries = json.loads((REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8"))
    return [e for e in entries if e["family"] == FAMILY]


def test_the_committed_ledger_adjudicates_exactly_the_22_invalid_captures() -> None:
    """Detects the 22 failures left as open gaps (reconcile red forever), a wider or narrower
    scope, a dropped cause or ruling, or an entry the registry does not back: one ``failed``
    entry, cause ``UnicodeDecodeError``, ruling 667, 22 distinct fc_summary captures of
    2026-10-08 (the March 2020 ... December 2021, May 2022 and April-June 2023 resources, none of
    them an edition-mapped one), the misplaced November 2020 actual-summary body named in the
    reason, one-line reason / question / evidence."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    (entry,) = _ledger()
    assert (entry["category"], entry["cause"], entry["ruling"]) == (
        "failed",
        "UnicodeDecodeError",
        "667",
    )
    paths = entry["captures"]
    assert len(paths) == len(set(paths)) == 22
    rids = set()
    for path in paths:
        assert path.startswith(f"bronze/neso_data_portal/{FAMILY}/2026/10/08/raw_")
        match = registry_module.CAPTURE_ID_PATTERN.fullmatch(path)
        assert match is not None
        rids.add(match["rid"])
    assert "b41ca68b-9248-4442-a158-664e13e6f9c8" in rids
    assert CAPTURES["bad"].resource_id in rids
    registry = load_registry()
    names = {registry.resources[rid][1].name for rid in rids}
    assert "Monthly BSUoS Forecast Summary Dec_20" in names
    assert "Monthly BSUoS Forecast Summary December 2021" in names
    assert "Monthly BSUoS Forecast Summary May 2022" in names
    assert "Monthly BSUoS Forecast Summary June 2023" in names
    mapped = _record().edition_by_filename
    assert mapped is not None
    for text in ("reason", "question", "evidence"):
        assert "\n" not in entry[text] and entry[text].strip()
    assert "0xA3" in entry["reason"] and "never re-decoded" in entry["reason"]
    assert "b41ca68b" in entry["reason"] and "42f21a68" in entry["reason"]
    assert "encoding" in entry["question"] and "UTF-8" in entry["question"]
    assert "T130" in entry["evidence"] and "ADR-040" in entry["evidence"]


def test_the_22_failures_are_adjudicated_not_open_and_not_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects the invalid-encoding captures left open, an entry that no longer matches its
    failure records (stale) or an adjudication that alters data: 22 invalid captures plus one
    valid one are run; reconcile without the ledger reports 22 ``failed`` gaps, with the
    committed entry (its capture ids swapped for the fixture captures') every gap is adjudicated
    with ruling 667, none is open or stale, and the silver and state bytes are equal before and
    after."""
    install_generated(monkeypatch, data / "_registry", [_package_doc(f"{PACKAGE}.json")])
    (template,) = _ledger()
    ids: list[str] = []
    for index, path in enumerate(template["captures"]):
        match = registry_module.CAPTURE_ID_PATTERN.fullmatch(path)
        assert match is not None
        written = (
            datetime(2026, 10, 8, 9, 8, 0, tzinfo=UTC) + timedelta(seconds=index)
        ).isoformat()
        ids.append(capture(data, "bad", resource_id=match["rid"], written=written))
    good = capture(data, "f13")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    assert sorted(c for c, _cls, _m in info.value.failures) == sorted(ids)
    assert {cls for _c, cls, _m in info.value.failures} == {"UnicodeDecodeError"}
    assert read_completion(data, FAMILY, good) is not None
    before = (_tree_bytes(data, "silver"), _tree_bytes(data, "state"))

    code, lines = run_cli(FAMILY, "--cutoff", DAY.isoformat())
    assert code == 1, lines
    gaps = [line for line in lines if line.startswith("GAP failed")]
    assert len(gaps) == 22
    entry = {**template, "captures": ids}
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json([entry]), encoding="utf-8"
    )
    code, lines = run_cli(FAMILY, "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert [line for line in lines if line.startswith("GAP")] == []
    assert "SUMMARY adjudicated 22" in lines and "SUMMARY stale_adjudication 0" in lines
    adjudicated = [line for line in lines if line.startswith("ADJUDICATED failed")]
    assert len(adjudicated) == 22
    assert all("UnicodeDecodeError" in line and "ruling 667" in line for line in adjudicated)
    assert (_tree_bytes(data, "silver"), _tree_bytes(data, "state")) == before


def _tree_bytes(data: Path, top: str) -> dict[str, bytes]:
    return {
        p.relative_to(data / top).as_posix(): p.read_bytes()
        for p in sorted((data / top).rglob("*"))
        if p.is_file()
    }


# --------------------------------------------------------------------------- #
# The families that get no record
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", NO_RECORD)
def test_the_eight_families_with_no_valid_utf8_or_no_run_have_no_record(key: str) -> None:
    """Detects one of the eight families gaining a record (RULINGS 538/653 precedent; seat ruling,
    class 3): a UTF-8 record would load zero rows and add failure records, a single-byte
    encoding is unevidenced (no vendor declaration; ``sum_winter`` mixes UTF-8 and a raw 0xA3),
    and the daily body has no run column (a key on (date, period) alone breaks the settlement
    rule). Each stays ingest-only (no transformer, no silver output, no constant-column
    mechanism), its resources SILVER of the record-less family, held by nothing."""
    registry = load_registry()
    package, entry = registry.families[key]
    assert entry.record is None and entry.kind == "tabular"
    assert not isinstance(package.eligibility, Held)
    slug, count = NO_RECORD[key]
    assert package.package == slug
    resources = [r for _p, r in registry.resources.values() if r.family == key]
    assert len(resources) == count
    assert all(r.disposition == SilverDisposition(kind="SILVER", key=key) for r in resources)
    with pytest.raises(ValueError, match="No transformer registered"):
        get_transformer(SOURCE, key, Path("."))


def test_the_no_record_families_are_reported_ingest_only_and_the_record_family_silver_held() -> (
    None
):
    """Detects the no-record holds miscounted or the held record reported eligible: the report
    lists the eight families as ``ingest-only (no silver output)``, the files family as
    ``catalogue only`` and ``fc_summary`` as ``silver`` with the held question and unit."""
    report = eligibility_module.render(load_registry())
    for key in NO_RECORD:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "ingest-only (no silver output)" in line, key
    (line,) = [ln for ln in report.splitlines() if "| `bsuos_monthly_forecast_files` |" in ln]
    assert "catalogue only" in line
    (line,) = [ln for ln in report.splitlines() if f"| `{FAMILY}` |" in ln]
    assert "| silver | held: TODO: the issue instant" in line
    assert f"(unit {UNIT})" in line and "whole_capture per resource_id" in line


def test_the_workbook_archive_stays_hold_with_the_updated_reason_and_the_pdf_stays_doc() -> None:
    """Detects the ZIP-of-XLSX archive promoted or its reason left on the old pending-BAL-2 text,
    or the communications PDF changed: the archive and each of its 151 children stay HOLD with
    ``TODO: WORKBOOK-CHILD-RECORDS``, the PDF stays DOC."""
    document = _package_doc(f"{PACKAGE}.json")
    files = [r for r in document["resources"] if r["family"] == "bsuos_monthly_forecast_files"]
    assert len(files) == 2
    (archive,) = [r for r in files if r["format"] == "XLSX"]
    (pdf,) = [r for r in files if r["format"] == "PDF"]
    assert pdf["disposition"] == {"kind": "DOC"}
    assert archive["disposition"]["kind"] == "HOLD"
    assert archive["disposition"]["reason"] == FILES_HOLD
    assert len(archive["children"]) == 151
    holds = [c for c in archive["children"] if c["disposition"]["kind"] == "HOLD"]
    docs = [c for c in archive["children"] if c["disposition"]["kind"] == "DOC"]
    assert (len(holds), len(docs)) == (136, 15)
    assert all(c["disposition"]["reason"] == FILES_HOLD for c in holds)
    resource = load_registry().resources[archive["id"]][1]
    assert isinstance(resource.disposition, HoldDisposition)
    assert "WORKBOOK-CHILD-RECORDS" in resource.disposition.reason


# --------------------------------------------------------------------------- #
# Vintage, the catalogue and the generated pages
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ["f4", "f10", "f13", "f15"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy (the filename month, RULINGS 529/597) or a catalogue view that
    cannot carry the edition columns: ``available_at`` is the CKAN ``last_modified``,
    ``timestamp_utc`` is the capture's own ``capture_written_at`` (temporal ``none``), an as-of
    read before the vintage serves nothing even though the capture is later and one after serves
    it, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    frame = _silver(data)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
    assert "issue_time" not in frame.columns
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(days=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, FAMILY, before) == []
    assert set(both_as_of(db, data, FAMILY, after)) == {capture_id}
    assert set(both_as_of(db, data, FAMILY, None)) == {capture_id}


def test_the_newest_capture_of_each_resource_is_served_not_one_per_family(data: Path) -> None:
    """Detects family-scope selection (which would keep one edition): two resources (two
    editions) are both served by the latest read, each from its own capture."""
    a = capture(data, "f13")
    b = capture(data, "f15")
    get_transformer(SOURCE, FAMILY, data).run(DAY, run_id="r")
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, FAMILY, None)) == {a, b}


def test_the_skeleton_page_renders_the_new_record() -> None:
    """Detects a record the docs generator cannot render (the held question, the eight header
    epochs, the resource-partitioned edition key)."""
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
    assert f"`{FAMILY}`" in page
