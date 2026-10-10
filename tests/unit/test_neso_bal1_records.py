"""The balancing-costs batch's frozen records (v0.22-K-BAL-1): nine records in six packages.

``current_bsuos_ii``, ``current_bsuos_sf``, ``current_bsuos_rf``, ``current_bsuos_historic_ii``,
``current_bsuos_historic_sf``, ``current_bsuos_historic_rf`` and ``constraint_breakdown`` are
eligible; ``bsuos_fixed_tariffs`` and ``inertia_bid_offer_costs`` are held (E-SEM); the two daily
balancing families have **no record** (RULINGS 538 precedent, 653). Every test writes recorded
fixture captures (cuts of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/bal1/``, provenance in ``PROVENANCE.md``) into a short data root
and runs the transformer the **real package registry** generates, so a record that does not fit its
vendor body fails here, not at activation. On master none of the nine families has a record, so
``get_transformer`` raises for each of them.

Record decisions under test (K-BAL-1-FACTS, K-BAL-1-SPEC, RULINGS 653):

- Every BSUoS body carries its settlement run (``Run Type``: II, SF or RF) and every BSUoS record
  keys on it (``run_type_column``): the three current runs overlap on every settlement pair with
  different values, so a (date, period) key would be wrong.
- ``Settlement Date`` / ``Settlement Day`` is a settlement-date label serialised with midnight text;
  it types as a ``date`` under ``%Y-%m-%dT%H:%M:%S `` (the trailing space also reads the vendor's
  leading and trailing padding), never as an instant.
- Units live in the FACTS g4 citations (the record model has no unit field): tariffs GBP/MWh, volume
  MWh, recovery and actual cost GBP; the old ``BSUoS Price`` GBP/MWh, ``Half-hourly Charge`` GBP
  and ``Total Daily BSUoS Charge`` GBP stay columns of their own, never merged with the post-change
  tariff, recovery or actual-cost columns; both actual-cost spellings are one column.
- Zeros, negatives and the numeric ``-1`` are values, blanks are null (never zero), no undocumented
  token becomes null. Open TODOs (the record model has no notes field, so they live here and in
  ``PROVENANCE.md``): BSUOS-AGGREGATION (the dictionary says whole-day but the values vary within
  every day), BSUOS-FUND-BLANK (what a blank fund tariff means), BSUOS-HI3-LABEL (HI3 is named
  2024-2025 but holds 2025-04-01 to 2025-04-13), CB-WINDOW and CB-RETAG for the constraint
  breakdown.
- Historic RF's two impossible pairs (``2022-03-27`` SP47 and SP48 of a 46-period day) are a row
  exclusion by the settlement-pair check, not an ADR-040 failure.
- The daily balancing costs and volume bodies have no run column and the record model cannot
  generate one: no record is frozen (the families stay ingest-only), counted as no-record holds
  with the reason on the two ``_files`` dispositions.

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the bronze
originals' CRLF convention; a BOM stays where the original had one."""

from __future__ import annotations

import csv
import io
import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

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
    RECONCILE_ADJUDICATIONS_FILE,
    Eligible,
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
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "bal1"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
POUND = "£"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S "

TARIFF_HOLD = (
    "TODO: precise issue clock and availability of each publication at its Published Date, and "
    "whether historical publication rows are immutable — 15 of 17 targets start on or before the "
    "capture vintage, so RULINGS 529's forward-target exception does not apply."
)
INERTIA_HOLD = (
    f"TODO: the unit denominator — every field dictionary says {POUND}/GVA while the package "
    f"description says {POUND} per GVAs; the exact averaging-day window; what a zero method value "
    "means."
)
FILES_HOLD = (
    "missing-period list (60 SETT_DATE/SETT_PERIOD pairs) — no child record while the parent "
    "family has none; the list does not cover all 70 measured gaps."
)
HELD = {"bsuos_fixed_tariffs": TARIFF_HOLD, "inertia_bid_offer_costs": INERTIA_HOLD}
RUN_OF = {
    "current_bsuos_ii": "II",
    "current_bsuos_sf": "SF",
    "current_bsuos_rf": "RF",
    "current_bsuos_historic_ii": "II",
    "current_bsuos_historic_sf": "SF",
    "current_bsuos_historic_rf": "RF",
}
CURRENT = ("current_bsuos_ii", "current_bsuos_sf", "current_bsuos_rf")
HISTORIC = (
    "current_bsuos_historic_ii",
    "current_bsuos_historic_sf",
    "current_bsuos_historic_rf",
)
BSUOS = (*CURRENT, *HISTORIC)
ELIGIBLE = (*BSUOS, "constraint_breakdown")
PARTITIONED = (*HISTORIC, "constraint_breakdown")
DAILY = (
    "daily_balancing_costs",
    "daily_balancing_volume",
    "daily_balancing_costs_files",
    "daily_balancing_volume_files",
)

CUR_PACKAGE = "current-balancing-services-use-of-system-bsuos-data"
CUR_ID = "d6a4bf54-c63f-4014-a716-49fd3878ca52"
CB_PACKAGE = "constraint-breakdown"
CB_ID = "fb56b46e-cef3-4eb8-9294-0ca19769b7eb"
T_PACKAGE = "bsuos-fixed-tariffs"
T_ID = "6c862622-48ff-4025-b998-d4d3db9e984d"
IN_PACKAGE = "gb-system-inertia-bid-and-offer-costs"
IN_ID = "23d66321-9c65-4825-9d62-94b6b24a1207"

CUR_HEADER = [
    "Settlement Date",
    "Settlement Period",
    "BSUoS Tariff_GBP per MWh",
    "Volume_MWh",
    "BSUoS Total Recovery_GBP",
    "Run Type",
    "Actual BSUoS Cost_GBP",
]
OLD_HEADER = [
    "Settlement Day",
    "Settlement Period",
    f"BSUoS Price ({POUND}/MWh Hour)",
    "Half-hourly Charge",
    "Total Daily BSUoS Charge",
    "Run Type",
]


def _new_header(total_unit: str, cost: str) -> list[str]:
    return [
        "Settlement Day",
        "Settlement Period",
        f"BSUoS Tariff ({POUND}/MWh)",
        f"BSUoS Fund Tariff ({POUND}/MWh)",
        "Volume (MWh)",
        f"BSUoS Recovery ({POUND})",
        f"BSUoS Fund Recovery ({POUND})",
        f"BSUoS Total Recovery ({total_unit})",
        "Run Type",
        cost,
    ]


HI2_HEADER = _new_header(POUND, f"Actual BSUoS Cost({POUND})")
HI34_HEADER = _new_header(POUND, f"Actual BSUoS Cost ({POUND})")
HSR_HEADER = _new_header("", f"Actual BSUoS Cost ({POUND})")
CB_HEADER = [
    "Date",
    "Reducing largest loss cost",
    "Increasing system inertia cost",
    "Voltage constraints cost",
    "Thermal constraints cost",
    "Reducing largest loss volume",
    "Increasing system inertia volume",
    "Voltage constraints volume",
    "Thermal constraints volume",
]
T_HEADER = [
    "Publication",
    "Fixed Tariff Title",
    "Published Date",
    "Fixed Tariff Start Date",
    "Fixed Tariff End Date",
    "Fixed Tariff_GBP per MWh",
]
IN_HEADER = [
    "Date",
    "Method A: Average bid price",
    "Method B: Average wind price",
    "Method C: Highest bid price",
]
HEADER_OF = {
    "ii": CUR_HEADER,
    "sf": CUR_HEADER,
    "rf": CUR_HEADER,
    "hi1": OLD_HEADER,
    "hi2": HI2_HEADER,
    "hi3": HI34_HEADER,
    "hi4": HI34_HEADER,
    "hs1": OLD_HEADER,
    "hs4": HSR_HEADER,
    "hr1": OLD_HEADER,
    "hr2": HSR_HEADER,
    "cb1": CB_HEADER,
    "cb9": CB_HEADER,
    "t": T_HEADER,
    "inertia": IN_HEADER,
}
HI3_ID = "ea47c8e4-caa3-49c7-a442-e9644d330f63"
HI4_ID = "45e87b67-ed5d-49ea-a149-2b37f467e542"


@dataclass(frozen=True)
class Capture:
    """One bronze capture (a cut of which is the fixture) and its sidecar provenance."""

    fixture: str
    family: str
    package: str
    package_id: str
    resource_id: str
    name: str
    filename: str
    modified: str
    written: str


def _capture(
    fixture: str,
    family: str,
    resource_id: str,
    name: str,
    filename: str,
    modified: str,
    written: str,
) -> Capture:
    package, package_id = {
        "constraint_breakdown": (CB_PACKAGE, CB_ID),
        "bsuos_fixed_tariffs": (T_PACKAGE, T_ID),
        "inertia_bid_offer_costs": (IN_PACKAGE, IN_ID),
    }.get(family, (CUR_PACKAGE, CUR_ID))
    return Capture(
        fixture, family, package, package_id, resource_id, name, filename, modified, written
    )


CAPTURES: dict[str, Capture] = {
    "ii": _capture(
        "ii.csv",
        "current_bsuos_ii",
        "0eda5e28-1dc6-48da-8663-c00e12f2a1e2",
        "Current II BSUoS Data",
        "current_ii_bsuos_data.csv",
        "2026-10-07T12:23:52.792085",
        "2026-10-08T09:16:48.529603+00:00",
    ),
    "sf": _capture(
        "sf.csv",
        "current_bsuos_sf",
        "f0060fd0-1fc9-4288-a0b3-4af9b592b0cf",
        "Current SF BSUoS Data",
        "current_sf_bsuos_data.csv",
        "2026-10-07T12:25:45.157754",
        "2026-10-08T09:16:55.324157+00:00",
    ),
    "rf": _capture(
        "rf.csv",
        "current_bsuos_rf",
        "26b0f410-27d4-448a-9437-45277818b838",
        "Current RF BSUoS Data",
        "current_rf_bsuos_data.csv",
        "2026-10-07T12:27:00.397791",
        "2026-10-08T09:16:52.162857+00:00",
    ),
    "hi1": _capture(
        "hi1.csv",
        "current_bsuos_historic_ii",
        "3372646d-419f-4599-97a9-6bb4e7e32862",
        "Historic II BSUoS Data",
        "2017-2023-ii.csv",
        "2023-04-19T11:26:14.741540",
        "2026-10-08T09:16:15.711675+00:00",
    ),
    "hi2": _capture(
        "hi2.csv",
        "current_bsuos_historic_ii",
        "d151c80a-f6a8-4b79-9387-1a68cd445af5",
        "Historic II BSUoS Data 2023-2024",
        "2023-2024-ii.csv",
        "2024-04-25T12:12:57.875352",
        "2026-10-08T09:16:18.156356+00:00",
    ),
    "hi3": _capture(
        "hi3.csv",
        "current_bsuos_historic_ii",
        HI3_ID,
        "Historic II BSUoS Data 2024-2025",
        "current_ii_bsuos_data.csv",
        "2025-04-23T12:20:03.734539",
        "2026-10-08T09:16:20.910510+00:00",
    ),
    "hi4": _capture(
        "hi4.csv",
        "current_bsuos_historic_ii",
        HI4_ID,
        "Historic II BSUoS Data 2025-2026",
        "2025-2026-ii.csv",
        "2026-05-11T11:32:20.516878",
        "2026-10-08T09:16:23.404447+00:00",
    ),
    "hs1": _capture(
        "hs1.csv",
        "current_bsuos_historic_sf",
        "241b40c3-1f20-4607-b329-0466d215871d",
        "Historic SF BSUoS Data",
        "2017-2023-sf.csv",
        "2023-04-28T13:00:16.486391",
        "2026-10-08T09:16:36.512794+00:00",
    ),
    "hs4": _capture(
        "hs4.csv",
        "current_bsuos_historic_sf",
        "927aac83-c218-476c-9c20-29cf20cee448",
        "Historic SF BSUoS Data 2025-2026",
        "2025-2026-sf.csv",
        "2026-05-11T13:00:01.647239",
        "2026-10-08T09:16:44.998243+00:00",
    ),
    "hr1": _capture(
        "hr1.csv",
        "current_bsuos_historic_rf",
        "2e8b2ea6-cdb5-4936-8636-4ab5a3f7e350",
        "Historic RF BSUoS Data",
        "2016-2023-rf.csv",
        "2024-07-09T11:11:27.261725",
        "2026-10-08T09:16:27.484757+00:00",
    ),
    "hr2": _capture(
        "hr2.csv",
        "current_bsuos_historic_rf",
        "47642de9-0738-47df-b230-94a097b61ae7",
        "Historic RF BSUoS Data 2023-2024",
        "2023-2024-rf.csv",
        "2025-09-16T10:35:41.968941",
        "2026-10-08T09:16:30.593594+00:00",
    ),
    "cb1": _capture(
        "cb1.csv",
        "constraint_breakdown",
        "3651e9e3-52a8-46b6-a675-3a4c2aedc813",
        "Constraint Breakdown 2017-2018",
        "constraint-breakdown-2017-2018.csv",
        "2021-08-03T13:02:43.115452",
        "2026-10-08T09:15:12.475796+00:00",
    ),
    "cb9": _capture(
        "cb9.csv",
        "constraint_breakdown",
        "6afe1c2b-6d70-4e76-8e74-0952b0a2beab",
        "Constraint Breakdown 2025-2026",
        "constraint-breakdown-2025-2026.csv",
        "2026-04-07T09:49:30.748060",
        "2026-10-08T09:15:34.998942+00:00",
    ),
    "t": _capture(
        "t.csv",
        "bsuos_fixed_tariffs",
        "4dfa533f-bec6-491b-a3f1-7ce92449bc9a",
        " Balancing Services Use of System Charges (BSUoS) Tariffs",
        "bsuos-fixed-tariffs-data-portal-new-header.csv",
        "2026-10-05T15:43:25.730825",
        "2026-10-08T09:06:57.529107+00:00",
    ),
    "inertia": _capture(
        "in.csv",
        "inertia_bid_offer_costs",
        "8da765a1-004f-46a5-8b3f-0e5b1787fcb1",
        "GB System Inertia Costs – combined bids and offers",
        "inertia_costs_methods.csv",
        "2022-10-19T17:16:40.960429",
        "2026-10-08T11:11:57.162696+00:00",
    ),
}
PACKAGE_FILES = {
    **{key: f"{CUR_PACKAGE}.json" for key in BSUOS},
    "constraint_breakdown": "constraint-breakdown.json",
    "bsuos_fixed_tariffs": "bsuos-fixed-tariffs.json",
    "inertia_bid_offer_costs": "gb-system-inertia-bid-and-offer-costs.json",
}
FAMILIES = tuple(PACKAGE_FILES)


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
        prefix="ba1", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's CRLF line endings (a BOM stays)."""
    raw = (FIXTURES / CAPTURES[alias].fixture).read_bytes()
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    text = raw[len(bom) :].replace(b"\r\n", b"\n")
    return bom + text.replace(b"\n", b"\r\n")


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim, padding included)."""
    data_bytes = raw if raw is not None else body(alias)
    reader = csv.DictReader(io.StringIO(data_bytes.decode("utf-8-sig"), newline=""))
    return [r for r in reader if any(v for v in r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8-sig").split("\r\n", 1)[0]
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
        package_slug=meta.package,
        package_id=meta.package_id,
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


def _load(data: Path, *aliases: str) -> tuple[dict[str, str], pl.DataFrame]:
    """Capture every alias (one family), run the family once; capture ids and the silver."""
    (family,) = {CAPTURES[a].family for a in aliases}
    ids = {alias: capture(data, alias) for alias in aliases}
    get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    return ids, _silver(data, family)


def _package_doc(filename: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / filename).read_text(encoding="utf-8"))
    return document


def _columns(key: str) -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record(key)))
        if c not in ("year", "month")
    ]


def _assert_clean_load(data: Path, alias: str) -> pl.DataFrame:
    """Capture + run ``alias``: every row written, none excluded, the generic output columns."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    written = get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert written == len(rows(alias))
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
    frame = _silver(data, meta.family)
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(meta.family)
    return frame


def _day(cell: str) -> date:
    """The settlement date a padded ``2025-07-23T00:00:00`` label names."""
    return date.fromisoformat(cell.strip()[:10])


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header
    (incl. the ``£`` spellings, the unit-less ``BSUoS Total Recovery ()`` of the SF/RF post-change
    files and the two actual-cost spellings), CRLF endings, the BOMs, the run in every body row,
    the midnight-text dates with leading and trailing padding, the blank fund tariff and the zero
    fund recovery, the 47-period 2021-01-14, the 46 valid plus two invalid periods of 2022-03-27,
    HI3's 624 rows equal to HI4's first 624, a thermal ``-1``, the day missing from CB9, the
    tariff stages and the zero inertia rows."""
    for alias, expected in HEADER_OF.items():
        assert header(alias) == expected, alias
        assert body(alias).count(b"\r\n") == body(alias).count(b"\n"), alias
    for alias in ("ii", "sf", "rf", "hi1", "hi2", "hs4", "hr1", "t"):
        assert body(alias).startswith(b"\xef\xbb\xbf"), alias
    for alias in ("cb1", "inertia"):
        assert not body(alias).startswith(b"\xef\xbb\xbf"), alias
    for alias, run in (
        ("ii", "II"),
        ("sf", "SF"),
        ("rf", "RF"),
        ("hi1", "II"),
        ("hi2", "II"),
        ("hi3", "II"),
        ("hi4", "II"),
        ("hs1", "SF"),
        ("hs4", "SF"),
        ("hr1", "RF"),
        ("hr2", "RF"),
    ):
        assert {r["Run Type"] for r in rows(alias)} == {run}, alias
    assert all(r["Settlement Date"].endswith("T00:00:00") for r in rows("ii"))

    assert rows("hi2")[0]["BSUoS Fund Tariff (£/MWh)"] == ""
    assert rows("hi2")[0]["BSUoS Fund Recovery (£)"] == "0"
    assert rows("hi2")[48]["Settlement Day"] == " 2023-11-28T00:00:00"
    assert rows("hs4")[48]["Settlement Day"] == " 2025-07-23T00:00:00"
    assert rows("hr1")[96]["Settlement Day"] == " 2022-10-14T00:00:00"
    assert rows("hr2")[48]["Settlement Day"] == "2023-07-31T00:00:00  "

    day = [r for r in rows("hi1") if r["Settlement Day"].startswith("2021-01-14")]
    assert [int(r["Settlement Period"]) for r in day] == list(range(1, 48))
    march = [r for r in rows("hr1") if r["Settlement Day"].startswith("2022-03-27")]
    assert [int(r["Settlement Period"]) for r in march] == list(range(1, 49))

    assert rows("hi3") == rows("hi4")[: len(rows("hi3"))] and len(rows("hi3")) == 624
    assert len(rows("hi4")) == 672

    assert [r["Thermal constraints volume"] for r in rows("cb1")].count("-1") == 1
    assert "2025-04-08" not in {r["Date"] for r in rows("cb9")}
    assert {"2025-04-07", "2025-04-09"} <= {r["Date"] for r in rows("cb9")}

    assert {r["Publication"] for r in rows("t")} == {"Final", "Draft", "Initial Forecast"}
    assert len(rows("t")) == 17
    zero = [r for r in rows("inertia") if all(v == "0" for k, v in r.items() if k != "Date")]
    assert len(zero) == 17 and len(rows("inertia")) == 20


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", FAMILIES)
def test_every_record_is_csv_utf8_version_1_with_ckan_last_modified_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time or a fallback vintage: every family's record is
    version 1, utf-8 CSV, ``ckan_last_modified`` vintage (every capture is an upload),
    whole-capture selection, per-resource selection exactly for the three historic families and the
    constraint breakdown, and no epoch declares an issue recipe (``issue_time`` is never
    emitted)."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert (record.vintage, record.vintage_evidence) == ("ckan_last_modified", None)
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key in PARTITIONED else None)
    assert ("resource_id" in record.entity_key) == (key in PARTITIONED)
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    assert has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)
    assert record.siblings == () and record.xlsx is None


@pytest.mark.parametrize("key", BSUOS)
def test_every_bsuos_record_keys_on_the_run_and_a_settlement_pair(key: str) -> None:
    """Detects a settlement key without its run (the settlement-data rule: the three runs publish
    different values for the same settlement pair), a nullable or non-string run, a settlement
    period unbounded or capped at 48, or a date left a string: in every epoch ``Run Type`` is the
    non-nullable string ``run_type``, ``run_type_column`` is ``run_type`` and in the key, the
    settlement date is a non-nullable ``date`` under the midnight-text format, the period a
    non-nullable int64 in 1..50, the recipe ``sp_pair`` over exactly those two columns and the key
    holds the pair and the run."""
    record = _record(key)
    date_name = "settlement_date" if key in CURRENT else "settlement_day"
    assert record.run_type_column == "run_type"
    assert "run_type" in record.entity_key
    assert (record.temporal.kind, record.temporal.date_column, record.temporal.period_column) == (
        "sp_pair",
        date_name,
        "settlement_period",
    )
    assert {date_name, "settlement_period", "run_type"} <= set(record.entity_key)
    for epoch in record.epochs:
        by_name = {c.name: c for c in epoch.columns}
        run = by_name["run_type"]
        assert (run.source, run.dtype, run.nullable) == ("Run Type", "string", False)
        assert run.null_tokens == ()
        day = by_name[date_name]
        assert (day.dtype, day.format, day.nullable) == ("date", DATE_FORMAT, False)
        assert day.formats_by_filename is None and day.zone is None
        period = by_name["settlement_period"]
        assert (period.dtype, period.nullable, period.min, period.max) == ("int64", False, 1, 50)


def test_the_three_current_records_are_one_shape() -> None:
    """Detects a current run drifting from the others or a unit lost from a name: II, SF and RF
    share the exact seven-column epoch (the settlement date label, the period, the GBP/MWh tariff,
    the MWh volume, the GBP total recovery, the run and the GBP actual cost, all values nullable
    float64), and the key (date, period, run)."""
    for key in CURRENT:
        record = _record(key)
        (epoch,) = record.epochs
        assert list(epoch.header) == CUR_HEADER
        assert [(c.source, c.name, c.dtype, c.nullable) for c in epoch.columns] == [
            ("Settlement Date", "settlement_date", "date", False),
            ("Settlement Period", "settlement_period", "int64", False),
            ("BSUoS Tariff_GBP per MWh", "bsuos_tariff_gbp_per_mwh", "float64", True),
            ("Volume_MWh", "volume_mwh", "float64", True),
            ("BSUoS Total Recovery_GBP", "bsuos_total_recovery_gbp", "float64", True),
            ("Run Type", "run_type", "string", False),
            ("Actual BSUoS Cost_GBP", "actual_bsuos_cost_gbp", "float64", True),
        ]
        assert not any(c.null_tokens for c in epoch.columns)
        assert record.entity_key == ("settlement_date", "settlement_period", "run_type")
        assert record.latest_partition is None


@pytest.mark.parametrize(
    ("key", "epochs"),
    [
        ("current_bsuos_historic_ii", [OLD_HEADER, HI2_HEADER, HI34_HEADER]),
        ("current_bsuos_historic_sf", [OLD_HEADER, HSR_HEADER]),
        ("current_bsuos_historic_rf", [OLD_HEADER, HSR_HEADER]),
    ],
)
def test_historic_records_have_the_exact_header_epochs_and_one_column_per_quantity(
    key: str, epochs: list[list[str]]
) -> None:
    """Detects a header epoch missing or merged, the two actual-cost spellings kept as two columns,
    the old variable price / half-hourly charge / daily total merged into the post-change tariff,
    recovery or actual-cost columns, or a quantity typed as an integer or string: the epochs are
    exactly the observed headers, in order; the post-change epochs name the same silver columns
    (both ``Cost(£)`` and ``Cost (£)``, and ``BSUoS Total Recovery (£)`` / ``()``, land in
    ``actual_bsuos_cost_gbp`` / ``bsuos_total_recovery_gbp``); the old epoch's three columns are
    their own names; the fund tariff and fund recovery are nullable float64; the key holds the
    resource."""
    record = _record(key)
    assert [list(e.header) for e in record.epochs] == epochs
    old = record.epochs[0]
    assert [c.name for c in old.columns] == [
        "settlement_day",
        "settlement_period",
        "bsuos_price_mwh_hour",
        "half_hourly_charge",
        "total_daily_bsuos_charge",
        "run_type",
    ]
    new_names = [
        "settlement_day",
        "settlement_period",
        "bsuos_tariff_gbp_per_mwh",
        "bsuos_fund_tariff_gbp_per_mwh",
        "volume_mwh",
        "bsuos_recovery_gbp",
        "bsuos_fund_recovery_gbp",
        "bsuos_total_recovery_gbp",
        "run_type",
        "actual_bsuos_cost_gbp",
    ]
    for epoch in record.epochs[1:]:
        assert [c.name for c in epoch.columns] == new_names
    for epoch in record.epochs:
        for column in epoch.columns:
            if column.name not in ("settlement_day", "settlement_period", "run_type"):
                assert (column.dtype, column.nullable) == ("float64", True), column.name
                assert not column.null_tokens and column.min is None and column.max is None
    assert record.entity_key == (
        "resource_id",
        "settlement_day",
        "settlement_period",
        "run_type",
    )
    old_only = {"bsuos_price_mwh_hour", "half_hourly_charge", "total_daily_bsuos_charge"}
    assert old_only.isdisjoint(new_names)


def test_constraint_breakdown_record_is_date_grain_with_unit_named_columns() -> None:
    """Detects a settlement rule wrongly applied to date-grain data, a date typed as a string, a
    category cost or volume dropped or mis-unitted, or the key losing the resource: one epoch, the
    ``%Y-%m-%d`` non-nullable date, four GBP cost and four MWh volume columns as nullable float64,
    ``date_sp1`` over the date, key (resource, date), no run column."""
    record = _record("constraint_breakdown")
    (epoch,) = record.epochs
    assert list(epoch.header) == CB_HEADER
    assert [(c.name, c.dtype, c.format, c.nullable) for c in epoch.columns] == [
        ("date", "date", "%Y-%m-%d", False),
        ("reducing_largest_loss_cost_gbp", "float64", None, True),
        ("increasing_system_inertia_cost_gbp", "float64", None, True),
        ("voltage_constraints_cost_gbp", "float64", None, True),
        ("thermal_constraints_cost_gbp", "float64", None, True),
        ("reducing_largest_loss_volume_mwh", "float64", None, True),
        ("increasing_system_inertia_volume_mwh", "float64", None, True),
        ("voltage_constraints_volume_mwh", "float64", None, True),
        ("thermal_constraints_volume_mwh", "float64", None, True),
    ]
    assert not any(c.null_tokens or c.min is not None or c.max is not None for c in epoch.columns)
    assert (record.temporal.kind, record.temporal.date_column) == ("date_sp1", "date")
    assert record.entity_key == ("resource_id", "date")
    assert record.run_type_column is None


def test_tariff_record_keys_on_the_publication_never_the_value() -> None:
    """Detects the tariff value used as the key (the draft's), a publication date turned into an
    instant, or the end date dropped: three non-nullable ``%Y-%m-%d`` date columns, the stage and
    title non-nullable strings, the GBP/MWh tariff a nullable float64, ``date_sp1`` over the start
    date, the key (publication, title, published date), no issue recipe."""
    record = _record("bsuos_fixed_tariffs")
    (epoch,) = record.epochs
    assert list(epoch.header) == T_HEADER
    assert [(c.name, c.dtype, c.format, c.nullable) for c in epoch.columns] == [
        ("publication", "string", None, False),
        ("fixed_tariff_title", "string", None, False),
        ("published_date", "date", "%Y-%m-%d", False),
        ("fixed_tariff_start_date", "date", "%Y-%m-%d", False),
        ("fixed_tariff_end_date", "date", "%Y-%m-%d", False),
        ("fixed_tariff_gbp_per_mwh", "float64", None, True),
    ]
    assert (record.temporal.kind, record.temporal.date_column) == (
        "date_sp1",
        "fixed_tariff_start_date",
    )
    assert record.entity_key == ("publication", "fixed_tariff_title", "published_date")
    assert "fixed_tariff_gbp_per_mwh" not in record.entity_key


def test_inertia_record_has_no_unit_in_the_method_names() -> None:
    """Detects a unit written into a method column before NESO settles the denominator (GVA or
    GVAs), a method typed as an integer, or the date as a string: the three methods are nullable
    float64 named for the method only, the date a ``%Y-%m-%d`` non-nullable date, ``date_sp1``,
    key (date)."""
    record = _record("inertia_bid_offer_costs")
    (epoch,) = record.epochs
    assert list(epoch.header) == IN_HEADER
    assert [(c.name, c.dtype, c.format, c.nullable) for c in epoch.columns] == [
        ("date", "date", "%Y-%m-%d", False),
        ("method_a_average_bid_price", "float64", None, True),
        ("method_b_average_wind_price", "float64", None, True),
        ("method_c_highest_bid_price", "float64", None, True),
    ]
    assert not any("gva" in c.name or "gbp" in c.name for c in epoch.columns)
    assert (record.temporal.kind, record.temporal.date_column) == ("date_sp1", "date")
    assert record.entity_key == ("date",)


@pytest.mark.parametrize("key", list(HELD))
def test_tariffs_and_inertia_are_held_exactly_as_ruled_and_the_package_stays_eligible(
    key: str,
) -> None:
    """Detects a hold lost, reworded or moved to the package, or an unlisted question: both carry
    the spec's verbatim E-SEM question, are effectively held, and the package itself stays
    eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == ("E-SEM", HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_seven_other_records_are_eligible(key: str) -> None:
    """Detects a hold wrongly put on an eligible family: none carries a record-level eligibility,
    so each is effectively eligible."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_each_family_is_in_the_package_file_the_spec_names() -> None:
    """Detects a record in the wrong package file, a family split, or a resource disposition
    drifting: each file carries exactly its records (the current BSUoS file also carries the
    existing frozen cap-adjustment record); every CSV resource of a record family is SILVER of
    that family; the per-family resource counts are the captured ones."""
    registry = load_registry()
    by_file: dict[str, set[str]] = {}
    for key, filename in PACKAGE_FILES.items():
        by_file.setdefault(filename, set()).add(key)
    by_file[f"{CUR_PACKAGE}.json"].add("current_bsuos_cap_adjustments")
    for filename, keys in by_file.items():
        document = _package_doc(filename)
        assert {f["key"] for f in document["families"] if "record" in f} == keys, filename
        for resource in document["resources"]:
            family = resource["family"]
            if family in keys and family != "current_bsuos_cap_adjustments":
                assert resource["disposition"] == {"kind": "SILVER", "key": family}
    for alias, meta in CAPTURES.items():
        assert registry.resources[meta.resource_id][1].family == meta.family, alias
    counts = {
        key: len([r for _p, r in registry.resources.values() if r.family == key])
        for key in FAMILIES
    }
    assert counts == {
        "current_bsuos_ii": 1,
        "current_bsuos_sf": 1,
        "current_bsuos_rf": 1,
        "current_bsuos_historic_ii": 4,
        "current_bsuos_historic_sf": 4,
        "current_bsuos_historic_rf": 3,
        "constraint_breakdown": 10,
        "bsuos_fixed_tariffs": 1,
        "inertia_bid_offer_costs": 1,
    }


def test_the_existing_cap_adjustment_record_and_files_family_are_unchanged() -> None:
    """Detects BAL-1 touching the frozen workbook child: ``current_bsuos_files`` stays record-less
    (catalogue only), ``current_bsuos_cap_adjustments`` keeps its run-inclusive ``key_latest``
    record, and the four CMP workbooks stay SILVER of it with their ``Prior Comms`` DOC child."""
    registry = load_registry()
    assert registry.families["current_bsuos_files"][1].record is None
    cap = _record("current_bsuos_cap_adjustments")
    assert (cap.latest, cap.run_type_column) == ("key_latest", "run_type")
    assert cap.entity_key == ("settlement_day", "settlement_period", "run_type")
    workbooks = [r for _p, r in registry.resources.values() if r.family == "current_bsuos_files"]
    assert len(workbooks) == 4
    for resource in workbooks:
        assert isinstance(resource.disposition, SilverDisposition)
        assert resource.disposition.key == "current_bsuos_cap_adjustments"
        assert resource.children is not None and len(resource.children) == 2


# --------------------------------------------------------------------------- #
# The two daily balancing families: no record
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", DAILY)
def test_the_daily_balancing_families_have_no_record(key: str) -> None:
    """Detects a daily balancing family gaining a record (RULINGS 538 precedent, 653). The bodies
    are settlement-period data with no run column, the record model has no constant-column
    mechanism and the dictionary's "Initial Settlement Run" phrase does not evidence a run value, so
    a key on (date, period) alone would break the settlement rule: the families stay ingest-only
    (no transformer, no silver output) until NESO names the run each value reflects."""
    entry = registry_module.load_registry().families[key][1]
    assert entry.record is None
    assert not isinstance(entry, Held)


def test_the_daily_families_are_reported_ingest_only_and_the_files_holds_carry_the_reason() -> None:
    """Detects the no-record holds miscounted or the missing-period lists given a child record:
    the eligibility report lists the daily costs and volume as ingest-only (no silver output), the
    two ``_files`` families as catalogue only; each missing-period workbook resource and its child
    stay HOLD with the updated reason (60 pairs, not the 70 measured gaps); the CSV resources stay
    SILVER of the record-less family."""
    registry = load_registry()
    report = eligibility_module.render(registry)
    for key in ("daily_balancing_costs", "daily_balancing_volume"):
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "ingest-only (no silver output)" in line, key
    for key in ("daily_balancing_costs_files", "daily_balancing_volume_files"):
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "catalogue only" in line, key
    for package, csv_family, files_family in (
        ("daily-balancing-costs-balancing-services-use-of-system", *DAILY[0:1], DAILY[2]),
        ("daily-balancing-volume-balancing-services-use-of-system", *DAILY[1:2], DAILY[3]),
    ):
        document = _package_doc(f"{package}.json")
        holds = [r for r in document["resources"] if r["family"] == files_family]
        assert len(holds) == 1
        (workbook,) = holds
        assert workbook["disposition"]["kind"] == "HOLD"
        assert workbook["disposition"]["reason"] == FILES_HOLD
        (child,) = workbook["children"]
        assert (
            child["disposition"]["kind"] == "HOLD" and child["disposition"]["reason"] == FILES_HOLD
        )
        csvs = [r for r in document["resources"] if r["family"] == csv_family]
        assert len(csvs) == 10
        assert all(r["disposition"] == {"kind": "SILVER", "key": csv_family} for r in csvs)
    resource = registry.resources["bfd081bc-9642-4ed6-9f37-60c5cbe26bef"][1]
    assert isinstance(resource.disposition, HoldDisposition)
    assert "60 SETT_DATE/SETT_PERIOD pairs" in resource.disposition.reason
    assert "70 measured gaps" in resource.disposition.reason


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ["ii", "sf", "rf"])
def test_current_runs_type_the_settlement_date_and_keep_the_run_and_every_value(
    data: Path, alias: str
) -> None:
    """Detects the midnight-text label read as an instant, a value rounded or re-typed, the run
    dropped, or the period start off: the slice loads every row with zero exclusions, the date is a
    ``date`` equal to the label, the run is the body's, the four quantities equal the cells as
    float64, ``timestamp_utc`` is the settlement period start, and the (date, period, run) key is
    unique."""
    source = rows(alias)
    frame = _assert_clean_load(data, alias)
    run = RUN_OF[CAPTURES[alias].family]
    assert frame.schema["settlement_date"] == pl.Date
    assert frame["settlement_date"].to_list() == [_day(r["Settlement Date"]) for r in source]
    assert frame["settlement_period"].to_list() == [int(r["Settlement Period"]) for r in source]
    assert set(frame["run_type"].to_list()) == {run}
    for column, vendor in (
        ("bsuos_tariff_gbp_per_mwh", "BSUoS Tariff_GBP per MWh"),
        ("volume_mwh", "Volume_MWh"),
        ("bsuos_total_recovery_gbp", "BSUoS Total Recovery_GBP"),
        ("actual_bsuos_cost_gbp", "Actual BSUoS Cost_GBP"),
    ):
        assert frame.schema[column] == pl.Float64
        assert frame[column].to_list() == [float(r[vendor]) for r in source], column
    assert frame["timestamp_utc"].to_list() == [
        settlement_period_to_utc(d, p)
        for d, p in zip(frame["settlement_date"], frame["settlement_period"], strict=True)
    ]
    assert (
        frame.select(["settlement_date", "settlement_period", "run_type"]).is_duplicated().sum()
        == 0
    )


def test_the_old_epoch_keeps_its_three_columns_and_the_missing_period_stays_missing(
    data: Path,
) -> None:
    """Detects the old variable price, half-hourly charge or repeated daily total merged into the
    post-change columns, a missing period filled, or the run dropped: HI1 loads 95 rows with zero
    exclusions, the three old quantities equal the cells, the post-change columns are null for
    these rows, 2021-01-14 has the 47 periods the body has (SP48 absent), and the run is II."""
    source = rows("hi1")
    frame = _assert_clean_load(data, "hi1")
    for column, vendor in (
        ("bsuos_price_mwh_hour", f"BSUoS Price ({POUND}/MWh Hour)"),
        ("half_hourly_charge", "Half-hourly Charge"),
        ("total_daily_bsuos_charge", "Total Daily BSUoS Charge"),
    ):
        assert frame.schema[column] == pl.Float64
        assert frame[column].to_list() == [float(r[vendor]) for r in source], column
    for column in (
        "bsuos_tariff_gbp_per_mwh",
        "bsuos_fund_tariff_gbp_per_mwh",
        "volume_mwh",
        "bsuos_recovery_gbp",
        "bsuos_fund_recovery_gbp",
        "bsuos_total_recovery_gbp",
        "actual_bsuos_cost_gbp",
    ):
        assert frame[column].null_count() == frame.height, column
    thursday = frame.filter(pl.col("settlement_day") == date(2021, 1, 14))
    assert sorted(thursday["settlement_period"].to_list()) == list(range(1, 48))
    assert set(frame["run_type"].to_list()) == {"II"}
    assert frame["resource_id"].unique().to_list() == [CAPTURES["hi1"].resource_id]


def test_blank_fund_tariffs_stay_null_zero_recovery_stays_zero_and_padding_parses(
    data: Path,
) -> None:
    """Detects a blank fund tariff replaced by zero (BSUOS-FUND-BLANK), a numeric zero fund recovery
    nulled, the ``Cost(£)`` spelling lost, or a leading-space date failing: HI2 loads every row
    with zero exclusions; the fund tariff is null on every blank cell and never zero, the fund
    recovery is the numeric zero of the cell, the actual cost equals the ``Actual BSUoS Cost(£)``
    cell, the old columns are null, and the ``' 2023-11-28T00:00:00'`` rows type as 2023-11-28."""
    source = rows("hi2")
    frame = _assert_clean_load(data, "hi2")
    blanks = [r["BSUoS Fund Tariff (£/MWh)"] == "" for r in source]
    assert all(blanks)
    assert frame["bsuos_fund_tariff_gbp_per_mwh"].null_count() == frame.height
    assert frame["bsuos_fund_recovery_gbp"].to_list() == [
        float(r["BSUoS Fund Recovery (£)"]) for r in source
    ]
    assert set(frame["bsuos_fund_recovery_gbp"].to_list()) == {0.0}
    assert frame["actual_bsuos_cost_gbp"].to_list() == [
        float(r["Actual BSUoS Cost(£)"]) for r in source
    ]
    assert frame["bsuos_total_recovery_gbp"].to_list() == [
        float(r["BSUoS Total Recovery (£)"]) for r in source
    ]
    assert frame["settlement_day"].to_list() == [_day(r["Settlement Day"]) for r in source]
    assert date(2023, 11, 28) in frame["settlement_day"].to_list()
    for column in ("bsuos_price_mwh_hour", "half_hourly_charge", "total_daily_bsuos_charge"):
        assert frame[column].null_count() == frame.height, column


def test_both_actual_cost_spellings_and_the_old_daily_total_land_in_separate_columns(
    data: Path,
) -> None:
    """Detects ``Cost(£)`` and ``Cost (£)`` landing in two columns, the old repeated daily total
    sharing a column with the post-change total recovery, or a resource lost: HI1 (old), HI2
    (``Cost(£)``) and HI4 (``Cost (£)``) load into one family; ``actual_bsuos_cost_gbp`` is filled
    from both new spellings, ``bsuos_total_recovery_gbp`` only by the post-change rows and
    ``total_daily_bsuos_charge`` only by the old ones, never both on a row."""
    ids, frame = _load(data, "hi1", "hi2", "hi4")
    assert frame.height == len(rows("hi1")) + len(rows("hi2")) + len(rows("hi4"))
    assert set(frame["bronze_capture_id"].to_list()) == set(ids.values())
    by_resource = {
        alias: frame.filter(pl.col("resource_id") == CAPTURES[alias].resource_id)
        for alias in ("hi1", "hi2", "hi4")
    }
    assert by_resource["hi2"]["actual_bsuos_cost_gbp"].null_count() == 0
    assert by_resource["hi4"]["actual_bsuos_cost_gbp"].null_count() == 0
    assert by_resource["hi4"]["actual_bsuos_cost_gbp"].to_list() == [
        float(r[HI34_HEADER[-1]]) for r in rows("hi4")
    ]
    assert by_resource["hi1"]["total_daily_bsuos_charge"].null_count() == 0
    assert by_resource["hi1"]["bsuos_total_recovery_gbp"].null_count() == by_resource["hi1"].height
    for alias in ("hi2", "hi4"):
        assert (
            by_resource[alias]["total_daily_bsuos_charge"].null_count() == by_resource[alias].height
        )
        assert by_resource[alias]["bsuos_total_recovery_gbp"].null_count() == 0
    assert (
        frame.select(["resource_id", "settlement_day", "settlement_period", "run_type"])
        .is_duplicated()
        .sum()
        == 0
    )


@pytest.mark.parametrize(("alias", "family"), [("hs1", "sf"), ("hs4", "sf")])
def test_historic_sf_types_both_epochs_and_the_unit_less_total_recovery_header(
    data: Path, alias: str, family: str
) -> None:
    """Detects the ``BSUoS Total Recovery ()`` column (the vendor stripped the currency) dropped or
    mis-mapped, a leading-space date failing, or the old epoch's period typed as a float: both
    slices load every row with zero exclusions, the run is SF, the period is an int64, and in HS4
    the total recovery equals the cell, the actual cost equals the cell, and the
    ``' 2025-07-23T00:00:00'`` rows type as 2025-07-23."""
    assert family == "sf"
    source = rows(alias)
    frame = _assert_clean_load(data, alias)
    assert set(frame["run_type"].to_list()) == {"SF"}
    assert frame.schema["settlement_period"] == pl.Int64
    assert frame["settlement_day"].to_list() == [_day(r["Settlement Day"]) for r in source]
    if alias == "hs4":
        assert frame["bsuos_total_recovery_gbp"].to_list() == [
            float(r["BSUoS Total Recovery ()"]) for r in source
        ]
        assert frame["actual_bsuos_cost_gbp"].to_list() == [
            float(r[HSR_HEADER[-1]]) for r in source
        ]
        assert date(2025, 7, 23) in frame["settlement_day"].to_list()
        assert frame["bsuos_fund_tariff_gbp_per_mwh"].null_count() == frame.height
        assert set(frame["bsuos_fund_recovery_gbp"].to_list()) == {0.0}
    else:
        assert frame["total_daily_bsuos_charge"].null_count() == 0
        assert frame["bsuos_total_recovery_gbp"].null_count() == frame.height


def test_historic_rf_excludes_exactly_the_two_invalid_pairs_and_keeps_the_46_period_day(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects an impossible pair reaching silver, a valid period dropped, or the exclusion turned
    into a capture failure: ``2022-03-27`` SP47 and SP48 (a 46-period day) are excluded by the
    settlement-pair check and surfaced (completion tally, warning naming the rule), the 46 valid
    periods and every other row load, nothing fails, and the ledger needs no entry."""
    capture_id = capture(data, "hr1")
    with caplog.at_level("WARNING"):
        written = get_transformer(SOURCE, "current_bsuos_historic_rf", data).run(DAY, run_id="r")
    source = rows("hr1")
    assert written == len(source) - 2 == 142
    completion = read_completion(data, "current_bsuos_historic_rf", capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 2)
    assert read_failure(data, "current_bsuos_historic_rf", capture_id) is None
    assert any("settlement_period" in r.getMessage() for r in caplog.records)
    frame = _silver(data, "current_bsuos_historic_rf")
    march = frame.filter(pl.col("settlement_day") == date(2022, 3, 27))
    assert sorted(march["settlement_period"].to_list()) == list(range(1, 47))
    assert frame["settlement_period"].max() == 48
    assert not frame.filter(
        (pl.col("settlement_day") == date(2022, 3, 27)) & (pl.col("settlement_period") > 46)
    ).height
    assert set(frame["run_type"].to_list()) == {"RF"}
    assert date(2022, 10, 14) in frame["settlement_day"].to_list()
    assert frame["bsuos_price_mwh_hour"].null_count() == 0
    assert not [
        e
        for e in registry_module.load_reconcile_adjudications()
        if e.family == "current_bsuos_historic_rf"
    ]


def test_historic_rf_post_change_epoch_parses_trailing_padding(data: Path) -> None:
    """Detects a trailing-space date (``'2023-07-31T00:00:00  '``) failing the capture: HR2 loads
    every row with zero exclusions, the padded day types as 2023-07-31, the run is RF and the
    total recovery equals the cell."""
    source = rows("hr2")
    frame = _assert_clean_load(data, "hr2")
    assert date(2023, 7, 31) in frame["settlement_day"].to_list()
    assert frame["settlement_day"].to_list() == [_day(r["Settlement Day"]) for r in source]
    assert set(frame["run_type"].to_list()) == {"RF"}
    assert frame["bsuos_total_recovery_gbp"].to_list() == [
        float(r["BSUoS Total Recovery ()"]) for r in source
    ]


@pytest.mark.parametrize("alias", ["cb1", "cb9"])
def test_constraint_breakdown_types_the_wide_day_rows_and_keeps_the_minus_one(
    data: Path, alias: str
) -> None:
    """Detects a cost or volume re-typed or rounded, the thermal ``-1`` or a zero nulled or
    "repaired", a day invented for CB9's missing 2025-04-08, or the resource lost: every row loads
    with zero exclusions, the eight quantities equal the cells as float64, the date is a ``date``,
    the resource id is stamped, and the (resource, date) key is unique."""
    source = rows(alias)
    frame = _assert_clean_load(data, alias)
    assert frame["date"].to_list() == [date.fromisoformat(r["Date"]) for r in source]
    for vendor in CB_HEADER[1:]:
        name = vendor.lower().replace(" ", "_") + ("_gbp" if vendor.endswith("cost") else "_mwh")
        assert frame.schema[name] == pl.Float64
        assert frame[name].to_list() == [float(r[vendor]) for r in source], name
    assert set(frame["resource_id"].to_list()) == {CAPTURES[alias].resource_id}
    assert frame.select(["resource_id", "date"]).is_duplicated().sum() == 0
    if alias == "cb1":
        assert frame["thermal_constraints_volume_mwh"].to_list().count(-1.0) == 1
        assert 0.0 in frame["increasing_system_inertia_cost_gbp"].to_list()
    else:
        assert date(2025, 4, 8) not in frame["date"].to_list()
        assert date(2025, 4, 7) in frame["date"].to_list()


def test_tariffs_load_with_publication_dates_as_dates_and_no_issue_instant(data: Path) -> None:
    """Detects the publication date synthesised into an instant, a stage or title altered, the
    inclusive end date shifted, or a tariff keyed by its value: all 17 rows load with zero
    exclusions, the three dates equal the cells, the stage and title are verbatim, the tariff is a
    float64, the key is unique, no ``issue_time`` exists and ``timestamp_utc`` is the London
    midnight of the start date."""
    source = rows("t")
    frame = _assert_clean_load(data, "t")
    assert "issue_time" not in frame.columns
    assert frame["publication"].to_list() == [r["Publication"] for r in source]
    assert frame["fixed_tariff_title"].to_list() == [r["Fixed Tariff Title"] for r in source]
    for column, vendor in (
        ("published_date", "Published Date"),
        ("fixed_tariff_start_date", "Fixed Tariff Start Date"),
        ("fixed_tariff_end_date", "Fixed Tariff End Date"),
    ):
        assert frame.schema[column] == pl.Date
        assert frame[column].to_list() == [date.fromisoformat(r[vendor]) for r in source], column
    assert frame["fixed_tariff_gbp_per_mwh"].to_list() == [
        float(r["Fixed Tariff_GBP per MWh"]) for r in source
    ]
    key = ["publication", "fixed_tariff_title", "published_date"]
    assert frame.select(key).is_duplicated().sum() == 0
    london = ZoneInfo("Europe/London")
    assert frame["timestamp_utc"].to_list() == [
        datetime.combine(d, time(0), tzinfo=london).astimezone(UTC)
        for d in frame["fixed_tariff_start_date"].to_list()
    ]


def test_inertia_costs_keep_the_zero_rows_as_numbers(data: Path) -> None:
    """Detects a zero method value nulled or dropped (its meaning is undocumented), a value
    re-typed, or the date as a string: all 20 rows load with zero exclusions, 17 are all-zero
    numbers, the three methods equal the cells as float64 and the date key is unique."""
    source = rows("inertia")
    frame = _assert_clean_load(data, "inertia")
    assert frame["date"].to_list() == [date.fromisoformat(r["Date"]) for r in source]
    for column, vendor in (
        ("method_a_average_bid_price", IN_HEADER[1]),
        ("method_b_average_wind_price", IN_HEADER[2]),
        ("method_c_highest_bid_price", IN_HEADER[3]),
    ):
        assert frame.schema[column] == pl.Float64
        assert frame[column].to_list() == [float(r[vendor]) for r in source], column
        assert frame[column].null_count() == 0
    zero = frame.filter(
        (pl.col("method_a_average_bid_price") == 0)
        & (pl.col("method_b_average_wind_price") == 0)
        & (pl.col("method_c_highest_bid_price") == 0)
    )
    assert zero.height == 17
    assert frame["date"].is_duplicated().sum() == 0


@pytest.mark.parametrize(
    ("alias", "needle", "wrong"),
    [
        ("ii", b"184221.66", b"N/A"),
        ("hi2", b",0,169798.60,", b",N/A,169798.60,"),
        ("hs4", b"2025-04-01T00:00:00,1,10.74,", b"April 1 2025,1,10.74,"),
        ("cb1", b"2017-12-18,0,0,0,178,0,0,0,-1", b"2017-12-18,0,0,0,178,0,0,0,N/A"),
        ("t", b"13.41", b"N/A"),
        ("inertia", b"2019-04-14,0,6462,32391", b"2019-04-14,0,6462,N/A"),
    ],
)
def test_an_undocumented_token_is_never_read_as_null_or_repaired(
    data: Path, alias: str, needle: bytes, wrong: bytes
) -> None:
    """Detects a silent repair (``N/A`` nulled, a date guessed): a cell the strict cast cannot read
    fails the capture loudly, with no completion and no silver."""
    raw = body(alias).replace(needle, wrong, 1)
    assert raw != body(alias)
    capture_id = capture(data, alias, raw=raw)
    meta = CAPTURES[alias]
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    assert read_completion(data, meta.family, capture_id) is None
    assert _no_silver(data, meta.family)


def test_a_header_outside_the_epochs_fails_loud(data: Path) -> None:
    """Detects an unlisted header read by guesswork: the Historic SF body with ``Actual BSUoS
    Cost (£)`` renamed ``Actual Cost`` matches no epoch, so the capture fails with no completion
    and no silver."""
    raw = body("hs4").replace("Actual BSUoS Cost (£)".encode(), b"Actual Cost", 1)
    assert raw != body("hs4")
    capture_id = capture(data, "hs4", raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "current_bsuos_historic_sf", data).run(DAY, run_id="r")
    assert read_completion(data, "current_bsuos_historic_sf", capture_id) is None
    assert _no_silver(data, "current_bsuos_historic_sf")


# --------------------------------------------------------------------------- #
# Reconcile: the HI3 / HI4 overlap is adjudicated
# --------------------------------------------------------------------------- #


def _entries(family: str) -> list[dict[str, Any]]:
    entries = json.loads((REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8"))
    return [e for e in entries if e["family"] == family]


def _tree_bytes(data: Path, top: str) -> dict[str, bytes]:
    return {
        p.relative_to(data / top).as_posix(): p.read_bytes()
        for p in sorted((data / top).rglob("*"))
        if p.is_file()
    }


def test_the_committed_ledger_adjudicates_exactly_the_hi3_hi4_overlap() -> None:
    """Detects the overlap left an open gap (reconcile red forever), an entry that names other
    captures, a wider scope, a dropped cause or ruling, or an entry the registry does not back: one
    ``overlap`` entry (no cause) of ruling 653 for the 2024-2025 and 2025-2026 Historic II captures
    only, a one-line reason and question naming 624 keys, evidence naming the FACTS and this unit
    and ADR-040, last in the ledger; the registry backs it; and no other BAL-1 family has an
    entry."""
    entries = registry_module.load_reconcile_adjudications()
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []
    (entry,) = _entries("current_bsuos_historic_ii")
    assert (entry["category"], entry["ruling"]) == ("overlap", "653")
    assert entry.get("cause") is None
    paths = entry["captures"]
    assert len(paths) == 2
    resources = set()
    for path in paths:
        assert path.startswith("bronze/neso_data_portal/current_bsuos_historic_ii/2026/10/08/raw_")
        match = registry_module.CAPTURE_ID_PATTERN.fullmatch(path)
        assert match is not None
        resources.add(match["rid"])
    assert resources == {HI3_ID, HI4_ID}
    for text in ("reason", "question", "evidence"):
        assert "\n" not in entry[text] and entry[text].strip()
    assert "624" in entry["reason"] and "624" in entry["question"]
    assert "ADR-040" in entry["evidence"] and "v0.22-K-BAL-1" in entry["evidence"]
    assert entries[-1].family == "current_bsuos_historic_ii"
    others = {e.family for e in entries} & (set(FAMILIES) | set(DAILY))
    assert others == {"current_bsuos_historic_ii"}


def test_the_overlap_is_served_from_both_resources_and_adjudicated_not_open_and_not_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects an overlap silently deduplicated (HI3 dropped, a vendor precedence inferred), the
    gap left open, an entry that no longer matches the data (stale) or an adjudication that
    alters data: both captures load all their rows (624 keys twice, with equal values), reconcile
    without the ledger reports one ``overlap`` gap per capture naming the 624 shared keys, with the
    committed entry (its capture ids swapped for the fixture captures') every gap is adjudicated,
    none is open or stale, the line names ruling 653, and the silver and state bytes are equal
    before and after."""
    install_generated(
        monkeypatch,
        data / "_registry",
        [_package_doc(f"{CUR_PACKAGE}.json")],
    )
    ids, frame = _load(data, "hi3", "hi4")
    assert frame.height == 624 + 672
    shared = frame.filter(pl.col("settlement_day") <= date(2025, 4, 13))
    assert shared.height == 1248
    assert (
        shared.group_by(["settlement_day", "settlement_period", "run_type"]).len()["len"].to_list()
        == [2] * 624
    )
    values = shared.group_by(["settlement_day", "settlement_period", "run_type"]).agg(
        pl.col("actual_bsuos_cost_gbp").n_unique().alias("cost"),
        pl.col("bsuos_total_recovery_gbp").n_unique().alias("recovery"),
        pl.col("volume_mwh").n_unique().alias("volume"),
    )
    assert values["cost"].to_list() == [1] * 624 and values["volume"].to_list() == [1] * 624
    assert values["recovery"].to_list() == [1] * 624
    before = (_tree_bytes(data, "silver"), _tree_bytes(data, "state"))

    code, lines = run_cli("current_bsuos_historic_ii", "--cutoff", DAY.isoformat())
    assert code == 1, lines
    gaps = [line for line in lines if line.startswith("GAP overlap")]
    assert len(gaps) == 2
    for capture_id in ids.values():
        assert any(capture_id in line and "624 key(s)" in line for line in gaps), capture_id

    (template,) = _entries("current_bsuos_historic_ii")
    entry = {**template, "captures": [ids["hi3"], ids["hi4"]]}
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json([entry]), encoding="utf-8"
    )
    code, lines = run_cli("current_bsuos_historic_ii", "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert [line for line in lines if line.startswith("GAP")] == []
    assert "SUMMARY adjudicated 2" in lines and "SUMMARY stale_adjudication 0" in lines
    adjudicated = [line for line in lines if line.startswith("ADJUDICATED overlap")]
    assert len(adjudicated) == 2 and all("ruling 653" in line for line in adjudicated)
    assert (_tree_bytes(data, "silver"), _tree_bytes(data, "state")) == before


def test_the_other_historic_families_have_no_overlap_to_adjudicate(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects an unneeded adjudication or a false overlap on the SF and RF families (no shared
    keys across their resources): HS1 with HS4 and HR1 with HR2 each reconcile clean with no ledger
    entry, the HR1 exclusion not being a gap."""
    install_generated(
        monkeypatch,
        data / "_registry",
        [_package_doc(f"{CUR_PACKAGE}.json")],
    )
    for family, aliases in (
        ("current_bsuos_historic_sf", ("hs1", "hs4")),
        ("current_bsuos_historic_rf", ("hr1", "hr2")),
    ):
        _load(data, *aliases)
        code, lines = run_cli(family, "--cutoff", DAY.isoformat())
        assert code == 0, lines
        assert [line for line in lines if line.startswith("GAP")] == []


# --------------------------------------------------------------------------- #
# Vintage, the catalogue and the generated pages
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", ["ii", "sf", "rf", "hi2", "hs4", "hr2", "cb9", "t", "inertia"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy (RULINGS 529/597) or a catalogue view that cannot carry the new
    columns: ``available_at`` is the CKAN ``last_modified``, ``timestamp_utc`` follows the record's
    recipe (the settlement period start for the BSUoS pairs, the London midnight of the day
    otherwise), an as-of read before the vintage serves nothing even though the capture is later
    and one after serves it, in the DuckDB view and in Polars."""
    meta = CAPTURES[alias]
    ids, frame = _load(data, alias)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    recipe = _record(meta.family).temporal
    london = ZoneInfo("Europe/London")
    if recipe.kind == "sp_pair":
        assert recipe.date_column is not None and recipe.period_column is not None
        assert frame["timestamp_utc"].to_list() == [
            settlement_period_to_utc(d, p)
            for d, p in zip(frame[recipe.date_column], frame[recipe.period_column], strict=True)
        ]
    else:
        assert recipe.date_column is not None
        assert set(frame["timestamp_utc"].to_list()) == {
            datetime.combine(d, time(0), tzinfo=london).astimezone(UTC)
            for d in frame[recipe.date_column].to_list()
        }
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(days=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, meta.family, before) == []
    assert set(both_as_of(db, data, meta.family, after)) == {ids[alias]}
    assert set(both_as_of(db, data, meta.family, None)) == {ids[alias]}


def test_the_skeleton_pages_render_the_new_records() -> None:
    """Detects a record the docs generator cannot render (the held questions, the three header
    epochs, the resource-partitioned keys)."""
    registry = load_registry()
    seen = set()
    for key, filename in PACKAGE_FILES.items():
        slug = filename.removesuffix(".json")
        page = skeleton.render_package(
            registry,
            {
                "name": slug,
                "title": slug,
                "organization": {"title": "NESO"},
                "license_title": "NESO Open Data Licence",
                "extras": [],
            },
            None,
        )
        assert f"`{key}`" in page, key
        seen.add(key)
    assert seen == set(FAMILIES)
