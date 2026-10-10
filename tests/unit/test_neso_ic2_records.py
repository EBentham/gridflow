"""BritNed, Nord Pool day-ahead prices and the System Operating Plan (v0.22-K-IC-2): three records.

Every test writes recorded fixture captures (slices of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/ic2/``) into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here,
not at activation. On master none of the three families has a record, so ``get_transformer``
raises for each of them.

Units and meanings (K-IC-2-FACTS): BritNed ``To GB`` is the import limit and ``From GB`` the
export limit, both directional MW maxima, kept as **raw strings** (export cells publish
``"1,056.00"``); the time label is one raw string (``YYYYMMDD HH:MM-HH:MM``, vendor spacing and
``(a)``/``(b)`` fold suffixes untouched). Nord Pool ``Price`` is GBP/MWh and ``Delivery Period``
a GMT hourly interval of the UTC delivery ``Date``. Every SOP physical column is MW.

BritNed epochs (the exact headers are in :data:`HEADERS`): E0 carries From GB before To GB, E4
names its time column ``Operational Period Start Date and Time GMT`` but its cells are the same
labels as the others. Fixture cut (a scratch script, not committed): the header and the first
rows of one body per epoch; the 20221012 body once without and once with its repeated label
(bronze lines 7 and 31); the 20221116 UTF-8 BOM body whole (its last line is a wholly blank row);
the two ``0xA0`` lines of the 20241016 body; both overlapping uploads; and the malformed and
``(a)``/``(b)`` labels. ``git`` normalises a committed fixture's line endings, so :func:`body`
rebuilds the bronze convention (BritNed CRLF, the two datastore dumps LF).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import install_generated, write_capture
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings, run_cli

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import (
    RECONCILE_ADJUDICATIONS_FILE,
    Eligible,
    Held,
)
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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "ic2"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
INSTANT = "%Y-%m-%dT%H:%M:%S"

W = "Operational Date (YYYY-MM-DD) & Time GMT/BST (HH:MM - HH:MM)"
G = "Operational Period Start Date and Time GMT"
HEADERS: dict[str, list[str]] = {
    "E0": [W, "Flow (MW) From GB", "Flow (MW) To GB", "Reason For Restiction"],
    "E1": [W, "Flow (MW) to GB", "Flow (MW) from GB", "Reason For Restriction"],
    "E2": [W, "Flow (MW) To GB", "Flow (MW) From GB", "Reason For Restriction"],
    "E3": [W, "Flow (MW) to GB", "Flow (MW) from GB", "Reason for restriction"],
    "E4": [G, "Flow in MW To GB", "Flow in MW From GB", "Reason for restriction"],
}
EPOCH_OF = {
    "E0": "E0",
    "OLD": "E0",
    "E1": "E1",
    "E2": "E2",
    "E3": "E3",
    "BOM": "E3",
    "ENC": "E3",
    "O1": "E3",
    "O2": "E3",
    "MAL": "E3",
    "FOLD": "E3",
    "E4": "E4",
}
"""The header epoch each fixture capture carries (the BOM body is an E3 header)."""

BRIT_HOLD = (
    "TODO: NESO does not define the operational-date rollover, the GMT/BST mapping or the "
    "`(a)`/`(b)` fold markers of the hourly labels, and the bodies carry no issue time, so no "
    "label can be dated as an issued limit."
)
SOP_HOLD = (
    "TODO: the dump returns each plan's latest version and NESO does not state that its values "
    "are fixed and public at their creation time, and every target precedes the capture, so the "
    "forward-target rule (RULINGS 529) cannot apply."
)

BRIT_PACKAGE = "brit-ned"
BRIT_PACKAGE_ID = "1a9fa49a-dea1-4468-9a8c-7800db9d3ff4"


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture (or a slice of it) and its sidecar provenance."""

    fixture: str
    family: str
    package: str
    package_id: str
    resource_id: str
    name: str
    filename: str
    modified: str
    url_type: str
    written: str


def _brit(
    fixture: str, resource_id: str, week: str, modified: str, written: str, *, bom: bool = False
) -> Capture:
    return Capture(
        fixture,
        "brit_ned",
        BRIT_PACKAGE,
        BRIT_PACKAGE_ID,
        resource_id,
        f"BritNed DA & ID Weekly ITLs {week}",
        f"britned-da-id-weekly-itls-{week}.csv",
        modified,
        "upload",
        written,
    )


CAPTURES: dict[str, Capture] = {
    "E0": _brit(
        "e0_old_clean",
        "10432bfd-2102-4eda-8c6d-bed2a4df676b",
        "20221012",
        "2022-10-19T15:36:13.309223",
        "2026-10-08T08:52:15.482463+00:00",
    ),
    "OLD": _brit(
        "e0_old_collision",
        "10432bfd-2102-4eda-8c6d-bed2a4df676b",
        "20221012",
        "2022-10-19T15:36:13.309223",
        "2026-10-08T08:52:15.482463+00:00",
    ),
    "E1": _brit(
        "e1",
        "c5282013-ee05-4da6-ad46-ff59ff443bfe",
        "20221019",
        "2022-10-26T12:34:38.464816",
        "2026-10-08T08:52:17.969404+00:00",
    ),
    "E2": _brit(
        "e2",
        "2248834b-c822-494a-8f1c-ee3cf04705c7",
        "20221026",
        "2022-11-02T13:07:23.272437",
        "2026-10-08T08:52:20.673550+00:00",
    ),
    "E3": _brit(
        "e3_commas",
        "67fd41a6-3c5a-44e7-934d-83d72d81586f",
        "20230913",
        "2023-09-20T08:52:51.524818",
        "2026-10-08T08:54:28.184847+00:00",
    ),
    "E4": _brit(
        "e4",
        "261e6fb2-ccfa-4718-ba21-66e64e363339",
        "20260916",
        "2026-09-23T08:04:13.828896",
        "2026-10-08T09:01:02.261378+00:00",
    ),
    "BOM": _brit(
        "bom_blank",
        "2a4bc449-649d-4d28-b41e-616fc9fa9215",
        "20221116",
        "2022-11-23T15:28:40.435030",
        "2026-10-08T08:52:28.838160+00:00",
    ),
    "ENC": _brit(
        "enc_0xa0",
        "811bec71-f099-4474-ba5e-2f9932b39cc2",
        "20241016",
        "2024-10-23T08:37:35.939132",
        "2026-10-08T08:56:47.726473+00:00",
    ),
    "O1": _brit(
        "overlap_1",
        "f43260c8-c559-4415-a369-0eb4b9c4e6b2",
        "20251229",
        "2025-12-31T10:08:15.411920",
        "2026-10-08T08:59:16.149393+00:00",
    ),
    "O2": _brit(
        "overlap_2",
        "3e546d9e-32fc-4cbd-bd51-214b106907e6",
        "20251231",
        "2026-01-07T09:20:36.998256",
        "2026-10-08T08:59:18.659127+00:00",
    ),
    "MAL": _brit(
        "malformed",
        "9e78d6a3-1814-4db3-9a27-e604807c92e9",
        "20240515",
        "2024-05-22T09:11:07.008031",
        "2026-10-08T08:56:01.834914+00:00",
    ),
    "FOLD": _brit(
        "fold",
        "7ac2608d-9878-46f2-a075-0b40a87e0a63",
        "20241023",
        "2024-10-30T10:13:58.278979",
        "2026-10-08T08:56:50.448198+00:00",
    ),
    "NP": Capture(
        "nordpool",
        "nordpool_da_prices",
        "day-ahead-power-exchange-prices-nordpool",
        "8e3cc601-1fab-4325-82ca-d8a0daeddf73",
        "4f27eea5-7038-4f73-9740-e3e4ad47c26a",
        " N2EX GB Day-Ahead Price",
        "4f27eea5-7038-4f73-9740-e3e4ad47c26a",
        "2026-09-21T12:38:33.467657",
        "datastore",
        "2026-10-08T11:14:21.282381+00:00",
    ),
    "SOP": Capture(
        "sop",
        "system_operating_plan",
        "system-operating-plan-sop",
        "99442aab-b184-44f5-8495-fc0b82869d46",
        "e51f2721-00ab-4182-9cae-3c973e854aa8",
        "System Operating Plan - Data Table",
        "e51f2721-00ab-4182-9cae-3c973e854aa8",
        "2023-01-24T00:30:04.725185",
        "datastore",
        "2026-10-08T11:38:18.294948+00:00",
    ),
}
BRIT_ALIASES = tuple(a for a, c in CAPTURES.items() if c.family == "brit_ned")
LOADABLE = tuple(a for a in BRIT_ALIASES if a not in ("OLD", "ENC"))
"""Every BritNed fixture whose capture completes (OLD repeats a label, ENC is not UTF-8)."""

SOP_FLOATS = [
    *[
        "customer_demand_forcast",
        "station_transformer",
        "dsbr",
        "total_sop_demand",
        "standing_reserve_requirement",
        "standing_reserve_availability",
        "standing_reserve_shortfall",
        "standing_reserve_excess",
        "standing_res_wind_adj",
        "net_positive_regulating_reserve",
        "positive_reg_res_wind_adj",
        "reserve_for_response",
        "total_positive_reserve",
        "percentage_of_standing_reserve_excess",
        "net_negative_regulating_reserve",
        "negative_reg_res_wind_adj",
        "negative_response_reserve",
        "total_negative_reserve",
        "maximum_loss_generation",
        "maximum_loss_demand",
        "positive_residual",
        "imbalance",
        "negative_residual",
        "contingency_requirement",
        "operating_margin_surplus",
        "trigger_level",
    ],
    *(
        f"{stem}_{suffix}"
        for stem in [
            "no1",
            "nw1",
            "so1",
            "sw1",
            "britned",
            "ewic",
            "france",
            "intifa2",
            "moyle",
            "nemo",
            "ps",
            "sto",
            "sb",
            "total",
            "intnsl",
            "intelec",
            "bat",
            "slr",
            "intgrnl",
            "intvkl",
        ]
        for suffix in ("temx", "teol", "temi")
    ),
]
"""Every physical SOP column (K-IC-2-FACTS §4 g4), all MW, by silver name."""


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings (and so the CLI) point at."""
    with tempfile.TemporaryDirectory(
        prefix="ic", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with its bronze original's line ending (BritNed CRLF, dumps LF)."""
    raw = (FIXTURES / f"{CAPTURES[alias].fixture}.csv").read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n") if CAPTURES[alias].family == "brit_ned" else raw


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (the ENC body is read as Latin-1)."""
    data_bytes = raw if raw is not None else body(alias)
    text = data_bytes.decode("latin-1" if alias == "ENC" else "utf-8-sig")
    return list(csv.DictReader(io.StringIO(text, newline="")))


def populated(alias: str) -> list[dict[str, str]]:
    """:func:`rows` without the wholly blank lines the reader drops."""
    return [r for r in rows(alias) if any(r.values())]


def capture(
    data: Path, alias: str, *, raw: bytes | None = None, written: datetime | None = None
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=written or datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=meta.package,
        package_id=meta.package_id,
        resource_id=meta.resource_id,
        resource_name=meta.name,
        resource_filename=meta.filename,
        ckan_last_modified=meta.modified or None,
        url_type=meta.url_type,
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _load(data: Path, *aliases: str) -> dict[str, str]:
    """Capture ``aliases`` of one family and transform them; the capture id of each."""
    ids = {alias: capture(data, alias) for alias in aliases}
    (family,) = {CAPTURES[alias].family for alias in aliases}
    get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    return ids


def _of(frame: pl.DataFrame, alias: str) -> pl.DataFrame:
    return frame.filter(pl.col("resource_id") == CAPTURES[alias].resource_id)


def _instant(value: str) -> datetime:
    return datetime.strptime(value, INSTANT).replace(tzinfo=UTC)


def _package_doc(filename: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / filename).read_text(encoding="utf-8"))
    return document


def _fresh_interpreter(code: str) -> Any:
    """Run ``code`` in a fresh interpreter (nothing collection imported can mask it); the
    JSON of its last stdout line."""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    return json.loads(result.stdout.strip().splitlines()[-1])


# --------------------------------------------------------------------------- #
# Fixtures and record shapes
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact BritNed
    header (incl. the vendor misspelling ``Restiction``), the BOM, the wholly blank ``,,,``
    line, a comma-thousands export cell, the malformed and ``(a)``/``(b)`` labels, the two
    standalone ``0xA0`` bytes, the 24 shared labels of the overlapping uploads with equal value
    pairs, the repeated 20221012 04:00 label with two From GB values, and the Nord Pool and SOP
    rows (a DST day, negative prices, two ``-1`` prices, SOP ``-1``/blank/``0`` cells)."""
    for alias in BRIT_ALIASES:
        assert list(rows(alias)[0]) == HEADERS[EPOCH_OF[alias]], alias
    assert body("BOM").startswith(b"\xef\xbb\xbf") and b",,,\r\n" in body("BOM")
    assert not any(body(a).startswith(b"\xef\xbb\xbf") for a in BRIT_ALIASES if a != "BOM")
    assert any(r["Flow (MW) from GB"] == "1,056.00" for r in rows("E3"))
    assert "202405210 17:00-18:00" in {r[W] for r in rows("MAL")}
    assert {"20241027 01:00-02:00 (a)", "20241027 01:00-02:00 (b)"} <= {r[W] for r in rows("FOLD")}
    assert body("ENC").count(b"\xa0") == 2
    assert body("ENC").count(b",\xa00,\r\n") == 2
    with pytest.raises(UnicodeDecodeError):
        body("ENC").decode("utf-8")
    shared = {r[W]: (r[HEADERS["E3"][1]], r[HEADERS["E3"][2]]) for r in rows("O2")}
    first = {r[W]: (r[HEADERS["E3"][1]], r[HEADERS["E3"][2]]) for r in rows("O1") if r[W] in shared}
    assert len(shared) == len(first) == 24 and first == shared
    label = "20221012 04:00 - 05:00"
    clash = [r for r in rows("OLD") if r[HEADERS["E0"][0]] == label]
    assert [r["Flow (MW) From GB"] for r in clash] == ["1050", "1,007"]
    assert sum(r[HEADERS["E0"][0]] == label for r in rows("E0")) == 1
    np_rows = rows("NP")
    assert {r["Price"] for r in np_rows} >= {"-1.000000"}
    assert sum(r["Price"] == "-1.000000" for r in np_rows) == 2
    assert any(r["Price"].startswith("-") and r["Price"] != "-1.000000" for r in np_rows)
    assert sum(r["Date"] == "2024-10-27" for r in np_rows) == 24
    sop_rows = rows("SOP")
    assert len(list(sop_rows[0])) == 93
    assert any(r["imbalance"] == "-1" for r in sop_rows)
    assert any(r["dsbr"] == "" for r in sop_rows)
    assert any(r["britned_temx"] == "0" for r in sop_rows)
    assert any(r["report_date"][:10] != r["sop_report_creation_time_gmt"][:10] for r in sop_rows)


def test_brit_ned_record_shape() -> None:
    """Detects a BritNed record drifting from the spec: csv/utf-8, exactly the five observed
    header epochs in order, no issue recipe, temporal ``none``, the per-resource key (resource
    + label), per-resource whole-capture selection, the upload ``ckan_last_modified`` vintage,
    one silver name per meaning across epochs (E0's From-before-To included), every column a
    nullable raw string (no numeric cast, no null token, no bound) and the E-SEM hold."""
    record = _record("brit_ned")
    assert (record.reader, record.encoding, record.version) == ("csv", "utf-8", "1")
    assert [list(epoch.header) for epoch in record.epochs] == list(HEADERS.values())
    assert [epoch.issue.kind for epoch in record.epochs] == ["none"] * 5
    assert record.temporal.kind == "none"
    assert record.entity_key == ("resource_id", "operational_date_and_hour")
    assert (record.latest, record.latest_partition) == ("whole_capture", "resource_id")
    assert (record.vintage, record.siblings) == ("ckan_last_modified", ())
    meaning = {
        "flow_to_gb_mw_raw": {"Flow (MW) To GB", "Flow (MW) to GB", "Flow in MW To GB"},
        "flow_from_gb_mw_raw": {"Flow (MW) From GB", "Flow (MW) from GB", "Flow in MW From GB"},
        "operational_date_and_hour": {W, G},
        "reason_for_restriction": {
            "Reason For Restiction",
            "Reason For Restriction",
            "Reason for restriction",
        },
    }
    seen: dict[str, set[str]] = {}
    for epoch in record.epochs:
        assert len(epoch.columns) == 4
        for column in epoch.columns:
            assert column.dtype == "string" and column.nullable, column.name
            assert not column.null_tokens and column.format is None, column.name
            assert column.min is None and column.max is None, column.name
            seen.setdefault(column.name, set()).add(column.source)
    assert seen == meaning
    assert [c.name for c in record.epochs[0].columns] == [
        "operational_date_and_hour",
        "flow_from_gb_mw_raw",
        "flow_to_gb_mw_raw",
        "reason_for_restriction",
    ]
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == ("E-SEM", BRIT_HOLD)


def test_nordpool_record_shape() -> None:
    """Detects a Nord Pool record that invents a clock or a hold: one ``Date,Delivery Period,
    Price`` epoch, ``Date`` a date (``%Y-%m-%d``), the interval a raw string, ``Price`` a
    float64, temporal ``none`` (no recipe composes a UTC date with an interval label and
    ``date_sp1`` would apply UK settlement rules to a UTC label), no issue, the dump vintage
    ``capture_fallback``, key ``(date, delivery_period)`` and an eligible family (no hold)."""
    record = _record("nordpool_da_prices")
    assert (record.reader, record.encoding, record.version) == ("csv", "utf-8", "1")
    (epoch,) = record.epochs
    assert list(epoch.header) == ["Date", "Delivery Period", "Price"]
    assert [(c.name, c.dtype, c.format, c.nullable) for c in epoch.columns] == [
        ("date", "date", "%Y-%m-%d", True),
        ("delivery_period", "string", None, True),
        ("price", "float64", None, True),
    ]
    assert all(not c.null_tokens and c.min is None and c.max is None for c in epoch.columns)
    assert epoch.issue.kind == "none"
    assert record.temporal.kind == "none"
    assert record.entity_key == ("date", "delivery_period")
    assert (record.latest, record.latest_partition) == ("whole_capture", None)
    assert record.vintage == "capture_fallback"
    package, family = registry_module.load_registry().families["nordpool_da_prices"]
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_sop_record_shape() -> None:
    """Detects an SOP record drifting from the spec: the 93 columns exactly as the capture
    header, the four datetimes UTC (``%Y-%m-%dT%H:%M:%S``, only ``sop_datetime`` non-nullable),
    ``report_date`` a date parsed with the same format, ``latest_version`` int64, status and
    cardinal point raw strings, all 86 physical columns float64 (the MW list of FACTS §4 g4,
    lower-cased), temporal ``utc_instant(sop_datetime)``, issue from the creation time, key
    ``(issue_time, sop_datetime)``, dump vintage and the E-SEM hold."""
    record = _record("system_operating_plan")
    assert (record.reader, record.encoding, record.version) == ("csv", "utf-8", "1")
    (epoch,) = record.epochs
    assert list(epoch.header) == list(rows("SOP")[0])
    assert len(epoch.columns) == 93 and len(SOP_FLOATS) == 86
    by_name = {c.name: c for c in epoch.columns}
    assert [c.source.lower() for c in epoch.columns] == [c.name for c in epoch.columns]
    for name in ("sop_datetime", "sop_report_creation_time_gmt", "sop_d_and_c_time_gmt"):
        column = by_name[name]
        assert (column.dtype, column.format, column.zone) == ("datetime", INSTANT, "UTC"), name
        assert column.nullable is (name != "sop_datetime"), name
    assert (by_name["report_date"].dtype, by_name["report_date"].format) == ("date", INSTANT)
    assert by_name["latest_version"].dtype == "int64"
    assert by_name["latest_status"].dtype == by_name["cardinal_point"].dtype == "string"
    assert {n for n, c in by_name.items() if c.dtype == "float64"} == set(SOP_FLOATS)
    assert all(not c.null_tokens and c.min is None and c.max is None for c in epoch.columns)
    assert all(c.format is None for c in epoch.columns if c.dtype in ("float64", "string", "int64"))
    assert (record.temporal.kind, record.temporal.column) == ("utc_instant", "sop_datetime")
    assert (epoch.issue.kind, epoch.issue.column) == ("data_column", "sop_report_creation_time_gmt")
    assert record.entity_key == ("issue_time", "sop_datetime")
    assert (record.latest, record.latest_partition) == ("whole_capture", None)
    assert record.vintage == "capture_fallback"
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == ("E-SEM", SOP_HOLD)


@pytest.mark.parametrize("key", ["brit_ned", "system_operating_plan"])
def test_held_families_are_effectively_held_and_the_packages_stay_eligible(key: str) -> None:
    """Detects a held family published, or the whole package held: the record's hold is the
    family's effective eligibility and the package itself stays eligible."""
    package, family = registry_module.load_registry().families[key]
    assert package.eligibility == Eligible(status="eligible")
    assert isinstance(effective_eligibility(package, family), Held)


def test_each_family_is_in_the_package_file_the_spec_names() -> None:
    """Detects a record in the wrong package file, a family split, or the SOP's two PDFs
    changing disposition: each file carries exactly its record(s); every tabular resource keeps
    its SILVER disposition and ``system_operating_plan_files`` stays DOC as registered."""
    expected = {
        "brit-ned.json": {"brit_ned"},
        "day-ahead-power-exchange-prices-nordpool.json": {"nordpool_da_prices"},
        "system-operating-plan-sop.json": {"system_operating_plan"},
    }
    for filename, keys in expected.items():
        document = _package_doc(filename)
        assert {f["key"] for f in document["families"] if "record" in f} == keys, filename
        for resource in document["resources"]:
            if resource["family"] == "system_operating_plan_files":
                assert resource["disposition"] == {"kind": "DOC"}
            else:
                assert resource["disposition"] == {"kind": "SILVER", "key": resource["family"]}
    registry = registry_module.load_registry()
    assert len([r for p, r in registry.resources.values() if r.family == "brit_ned"]) == 187
    for alias, meta in CAPTURES.items():
        assert registry.resources[meta.resource_id][1].family == meta.family, alias


# --------------------------------------------------------------------------- #
# BritNed
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", LOADABLE)
def test_brit_fixture_types_with_no_exclusion(
    data: Path, alias: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the BOM
    prefix or the E0 order breaking the exact-header match), any cast the raw-string record
    should not need, a row excluded, and a silent blank-row filter: the capture completes
    with every populated row, zero exclusions, and the generic output columns; the wholly
    blank ``,,,`` line is dropped by the reader's logged path, not counted as data."""
    meta = CAPTURES[alias]
    transformer = get_transformer(SOURCE, meta.family, data)
    capture_id = capture(data, alias)
    with caplog.at_level(logging.INFO, logger="gridflow.silver.csv_bronze"):
        written = transformer.run(DAY, run_id="r")
    expected = len(populated(alias))
    assert written == expected
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["row_count"], completion["rows_excluded"]) == (
        "populated",
        expected,
        0,
    )
    blank = len(rows(alias)) - expected
    logged = [r.getMessage() for r in caplog.records if "blank row" in r.getMessage()]
    assert len(logged) == (1 if blank else 0)
    assert all(f"dropped {blank} blank row(s)" in message for message in logged)
    frame = _silver(data, meta.family)
    expected_columns = [name for name, _type in generic.output_columns(_record(meta.family))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected_columns if c not in ("year", "month")
    ]
    assert frame.select(["resource_id", "operational_date_and_hour"]).is_duplicated().sum() == 0
    assert set(frame["resource_id"].to_list()) == {meta.resource_id}


def test_the_blank_line_is_dropped_by_the_logged_path_and_the_bom_is_stripped(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects a UTF-8 BOM left on the first header (the exact-header match then fails) or on
    the first label, and a blank row kept as data or dropped without a record: the BOM body
    (a 73-line body whose last line is ``,,,``) loads 72 rows, the first label equals its cell
    without the BOM, and one INFO record names the single dropped blank row."""
    assert body("BOM").startswith(b"\xef\xbb\xbf")
    with caplog.at_level(logging.INFO, logger="gridflow.silver.csv_bronze"):
        _load(data, "BOM")
    frame = _silver(data, "brit_ned")
    source = rows("BOM")
    assert len(source) == 73 and frame.height == 72
    assert frame["operational_date_and_hour"].to_list() == [r[W] for r in source[:72]]
    assert not any("﻿" in value for value in frame["operational_date_and_hour"].to_list())
    assert [r.getMessage() for r in caplog.records if "dropped 1 blank row(s)" in r.getMessage()]


@pytest.mark.parametrize("alias", LOADABLE)
def test_brit_labels_and_values_survive_byte_identical(data: Path, alias: str) -> None:
    """Detects a normalised label, a parsed or corrected date, a stripped fold suffix, a
    number cast, a dropped zero or an invented reason: every column equals its CSV cell in
    order, so ``20221012 23:00 - 00:00`` and ``20230914 00:00-01:00`` keep their different
    spacing, ``(a)``/``(b)`` stay on the label, the malformed ``202405210 17:00-18:00`` is not
    corrected, ``"1,056.00"`` stays text, a zero limit stays ``"0"`` and a blank cell (limit or
    reason) is null, never zero or a filled reason."""
    _load(data, alias)
    frame = _silver(data, "brit_ned")
    source = populated(alias)
    header = HEADERS[EPOCH_OF[alias]]
    names = {
        "operational_date_and_hour": header[0],
        "flow_to_gb_mw_raw": next(h for h in header if h.lower().endswith("to gb")),
        "flow_from_gb_mw_raw": next(h for h in header if h.lower().endswith("from gb")),
        "reason_for_restriction": header[3],
    }
    for name, source_header in names.items():
        assert frame.schema[name] == pl.Utf8, name
        assert frame[name].to_list() == [r[source_header] or None for r in source], (alias, name)


def test_cells_with_commas_decimals_and_zeros_are_kept_as_text(data: Path) -> None:
    """Detects an export limit cast to a number (``"1,056.00"`` is not a float, a lone ``0`` is
    not a null): the E3 fixture's comma cells arrive exact, a zero limit arrives as ``"0"`` (E1
    carries them) and a blank limit is null, never ``"0"``."""
    _load(data, "E3", "E1")
    frame = _silver(data, "brit_ned")
    e3, e1 = _of(frame, "E3"), _of(frame, "E1")
    assert {"1,056.00", "1,076.00", "1,016.00"} <= set(e3["flow_from_gb_mw_raw"].to_list())
    assert frame.schema["flow_from_gb_mw_raw"] == pl.Utf8
    zeros = sum(r["Flow (MW) from GB"] == "0" for r in populated("E1"))
    assert zeros > 0 and e1["flow_from_gb_mw_raw"].to_list().count("0") == zeros
    blank = sum(r["Flow (MW) to GB"] == "" for r in populated("E3"))
    assert e3["flow_to_gb_mw_raw"].null_count() == blank


def test_e0_maps_from_and_to_gb_by_name_not_by_position(data: Path) -> None:
    """Detects a positional mapping: E0 puts ``From GB`` before ``To GB`` (every other epoch
    the reverse), so a position-based record would swap import and export. The E0 body's To GB
    column is 1060 on every row while its From GB column varies; each silver column equals
    the cell of the header it is named for, and E0 and E2 (same data direction, opposite
    column order) agree on which column is the import."""
    _load(data, "E0")
    frame = _silver(data, "brit_ned")
    to_cells = [r["Flow (MW) To GB"] for r in populated("E0")]
    from_cells = [r["Flow (MW) From GB"] for r in populated("E0")]
    assert to_cells != from_cells
    assert frame["flow_to_gb_mw_raw"].to_list() == to_cells
    assert frame["flow_from_gb_mw_raw"].to_list() == from_cells
    assert set(to_cells) == {"1060"}


def test_brit_issue_and_instant_are_never_derived(data: Path) -> None:
    """Detects an issue time, target instant, date or week invented from a label, a filename
    token or the CKAN ``last_modified``: ``timestamp_utc`` is the capture time, ``issue_time``
    is absent or null and no date or datetime column exists beyond the engine's."""
    _load(data, "E1", "E4")
    frame = _silver(data, "brit_ned")
    for alias in ("E1", "E4"):
        written = datetime.fromisoformat(CAPTURES[alias].written).astimezone(UTC)
        assert set(_of(frame, alias)["timestamp_utc"].to_list()) == {written}
    assert "issue_time" not in frame.columns or frame["issue_time"].null_count() == frame.height
    forbidden = [c for c in frame.columns if "week" in c or c in ("date", "settlement_date")]
    assert forbidden == []


def test_e4_time_column_is_the_same_label_not_a_datetime(data: Path) -> None:
    """Detects the E4 header ``Operational Period Start Date and Time GMT`` typed as a
    datetime on its name alone: its cells are ``YYYYMMDD HH:MM-HH:MM`` labels, stored raw under
    the same silver name as the other epochs."""
    _load(data, "E4")
    frame = _silver(data, "brit_ned")
    assert frame.schema["operational_date_and_hour"] == pl.Utf8
    assert frame["operational_date_and_hour"].to_list() == [r[G] for r in rows("E4")]
    assert frame["operational_date_and_hour"][0] == "20260920 23:00-00:00"


def test_two_uploads_load_and_latest_serves_each(data: Path) -> None:
    """Detects one upload's capture displacing another's (a family-wide newest-capture
    selection) or an unstamped ``resource_id``: two resources of the family both complete,
    every row carries its own resource, and ``_latest`` (the catalogue view and Polars agree)
    serves both, then serves a later capture of one with the other still present."""
    ids = _load(data, "E1", "E2")
    frame = _silver(data, "brit_ned")
    assert (_of(frame, "E1").height, _of(frame, "E2").height) == (
        len(populated("E1")),
        len(populated("E2")),
    )
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, "brit_ned", None)) == set(ids.values())
    later = capture(data, "E1", written=datetime(2026, 10, 8, 18, 0, tzinfo=UTC))
    get_transformer(SOURCE, "brit_ned", data).run(DAY, run_id="r2")
    assert set(both_as_of(db, data, "brit_ned", None)) == {later, ids["E2"]}


def test_overlapping_uploads_are_both_served(data: Path) -> None:
    """Detects an overlap silently deduplicated: the 24 hourly labels of 2025-12-31 that two
    uploads both publish stay one row per upload (48 rows for the 24 shared labels), the value
    pairs are equal and the key is unique once the resource is part of it."""
    _load(data, "O1", "O2")
    frame = _silver(data, "brit_ned")
    shared = [r[W] for r in rows("O2")]
    both = frame.filter(pl.col("operational_date_and_hour").is_in(shared))
    assert both.height == 48
    assert both.group_by("operational_date_and_hour").len()["len"].to_list() == [2] * 24
    pairs = both.group_by("operational_date_and_hour").agg(
        pl.col("flow_to_gb_mw_raw").n_unique().alias("to"),
        pl.col("flow_from_gb_mw_raw").n_unique().alias("from"),
    )
    assert pairs["to"].to_list() == [1] * 24 and pairs["from"].to_list() == [1] * 24
    assert frame.select(["resource_id", "operational_date_and_hour"]).is_duplicated().sum() == 0


def test_the_repeated_label_fails_the_guard_and_the_clean_part_loads(data: Path) -> None:
    """Detects a repeated hourly label deduplicated, absorbed into the key by its value, or
    taking a sibling down: the 20221012 body (04:00 - 05:00 twice with From GB ``1050`` and
    ``"1,007"``) fails with ``DuplicateEntityKeyError``, leaves no completion and no output,
    while a clean upload of the same family completes; the same body without the repeat
    loads."""
    old, clean = capture(data, "OLD"), capture(data, "E1")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, "brit_ned", data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [(old, "DuplicateEntityKeyError")]
    failure = read_failure(data, "brit_ned", old)
    assert failure is not None and failure["error_class"] == DuplicateEntityKeyError.__name__
    assert read_completion(data, "brit_ned", old) is None
    completion = read_completion(data, "brit_ned", clean)
    assert completion is not None and completion["rows_excluded"] == 0
    silver = _silver(data, "brit_ned")
    assert set(silver["bronze_capture_id"].to_list()) == {clean}


def test_the_0xa0_body_fails_alone_and_nothing_is_altered(data: Path) -> None:
    """Detects a non-UTF-8 body decoded with replacement or a dropped byte, a generic failure
    class no ledger entry may name (the pre-parse's ``ComputeError``), or one bad capture
    taking the family down: the 20241016 body (two standalone ``0xA0`` bytes) fails by itself
    with ``UnicodeDecodeError`` and ``bytes.decode``'s message, leaves no completion and no
    output, and the sibling upload completes with every row and no replacement character
    anywhere in the silver output. The fixture bytes are not changed (bronze repair is out of
    scope)."""
    enc, clean = capture(data, "ENC"), capture(data, "E1")
    assert body("ENC").count(b"\xa0") == 2
    with pytest.raises(UnicodeDecodeError) as decoded:
        body("ENC").decode("utf-8")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, "brit_ned", data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [(enc, "UnicodeDecodeError")]
    failure = read_failure(data, "brit_ned", enc)
    assert failure is not None and failure["error_class"] == "UnicodeDecodeError"
    assert failure["message"] == str(decoded.value)
    assert read_completion(data, "brit_ned", enc) is None
    completion = read_completion(data, "brit_ned", clean)
    assert completion is not None and completion["row_count"] == len(populated("E1"))
    silver = _silver(data, "brit_ned")
    assert set(silver["bronze_capture_id"].to_list()) == {clean}
    text = "".join(str(v) for column in silver.columns for v in silver[column].to_list())
    assert "�" not in text and "\xa0" not in text


def test_the_0xa0_body_without_the_byte_loads(data: Path) -> None:
    """A1 positive control. Detects a gate that rejects the 20241016 body for anything but its
    two ``0xA0`` bytes: with them removed the same capture completes with every populated row
    and leaves no failure record."""
    raw = body("ENC").replace(b"\xa00", b"0")
    raw.decode("utf-8")
    enc = capture(data, "ENC", raw=raw)
    written = get_transformer(SOURCE, "brit_ned", data).run(DAY, run_id="r")
    assert written == len(populated("ENC"))
    assert read_completion(data, "brit_ned", enc) is not None
    assert read_failure(data, "brit_ned", enc) is None


def _large_enc_body() -> bytes:
    """Over 6 MiB of unique hourly labels under the ENC header (beyond the pre-parse's reach,
    K-IC-2H PLAN E4), CRLF, the last row carrying ``,\\xa00,``."""
    lines = [body("ENC").split(b"\r\n")[0]]
    start = date(2000, 1, 1).toordinal()
    size, day = 0, 0
    while size < 6 * (1 << 20) + 1024:
        label = date.fromordinal(start + day).strftime("%Y%m%d")
        for hour in range(24):
            line = f"{label} {hour:02d}:00-{hour + 1:02d}:00,1060,1050,".encode()
            lines.append(line)
            size += len(line) + 2
        day += 1
    lines[-1] = lines[-1].replace(b",1050,", b",\xa00,")
    return b"\r\n".join(lines) + b"\r\n"


@pytest.mark.parametrize("where", ["header", "large"])
def test_the_0xa0_class_does_not_depend_on_where_the_byte_is(data: Path, where: str) -> None:
    """A2. Detects a failure class that depends on the bad byte's position: a ``0xA0`` inside
    the header name ``Reason for restriction``, or in the last row of a body larger than the
    pre-parse reads, fails the capture with ``UnicodeDecodeError`` just as a data-row byte
    does (before the gate both were ``NotCsvBodyError``)."""
    if where == "header":
        raw = (
            body("ENC")
            .replace(b"\xa00", b"0")
            .replace(b"Reason for restriction", b"Reason for\xa0restriction")
        )
    else:
        raw = _large_enc_body()
        assert len(raw) >= 6 * (1 << 20)
    assert raw.count(b"\xa0") == 1
    enc = capture(data, "ENC", raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, "brit_ned", data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [(enc, "UnicodeDecodeError")]
    failure = read_failure(data, "brit_ned", enc)
    assert failure is not None and failure["error_class"] == "UnicodeDecodeError"
    assert read_completion(data, "brit_ned", enc) is None


def _entry_template(position: int) -> dict[str, Any]:
    entries = json.loads((REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8"))
    brit = [e for e in entries if e["family"] == "brit_ned"]
    assert isinstance(brit[position], dict)
    template: dict[str, Any] = brit[position]
    return template


def test_the_committed_brit_ned_entries_adjudicate_the_overlap_and_the_collision(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects an expected vendor-caused gap left open (reconcile exits 1 forever), an entry
    that no longer matches the data (stale) or an adjudication that alters data: without the
    entries reconcile reports the repeated-label failure and the overlap; with the committed
    entries (their capture ids swapped for the fixture captures') every gap is adjudicated, none
    is open or stale, and the silver, state and ``_latest`` bytes are equal before and after."""
    install_generated(monkeypatch, data / "_registry", [_package_doc("brit-ned.json")])
    old, o1, o2, clean = (capture(data, a) for a in ("OLD", "O1", "O2", "E1"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "brit_ned", data).run(DAY, run_id="r")
    assert read_completion(data, "brit_ned", clean) is not None

    def snapshot() -> tuple[dict[str, bytes], dict[str, bytes]]:
        return (
            {
                p.relative_to(data / "silver").as_posix(): p.read_bytes()
                for p in sorted((data / "silver").rglob("*"))
                if p.is_file()
            },
            {
                p.relative_to(data / "state").as_posix(): p.read_bytes()
                for p in sorted((data / "state").rglob("*"))
                if p.is_file()
            },
        )

    before = snapshot()
    code, lines = run_cli("brit_ned", "--cutoff", DAY.isoformat())
    assert code == 1, lines
    assert any(line.startswith("GAP failed") and old in line for line in lines)
    assert any(line.startswith("GAP overlap") and o1 in line for line in lines)

    overlap, failed = _entry_template(0), _entry_template(1)
    entries = [{**overlap, "captures": [o1, o2]}, {**failed, "captures": [old]}]
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json(entries), encoding="utf-8"
    )
    code, lines = run_cli("brit_ned", "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert "SUMMARY adjudicated 3" in lines
    assert "SUMMARY stale_adjudication 0" in lines
    assert [line for line in lines if line.startswith("GAP")] == []
    assert snapshot() == before


def _tree_bytes(data: Path, top: str) -> dict[str, bytes]:
    return {
        p.relative_to(data / top).as_posix(): p.read_bytes()
        for p in sorted((data / top).rglob("*"))
        if p.is_file()
    }


def _adjudicate_all_three(data: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Capture ENC, OLD, O1, O2 and E1, run the family (ENC and OLD fail) and install the
    three committed BritNed entries with their capture ids swapped for the fixture captures'.
    The capture id of each alias."""
    install_generated(monkeypatch, data / "_registry", [_package_doc("brit-ned.json")])
    ids = {a: capture(data, a) for a in ("ENC", "OLD", "O1", "O2", "E1")}
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "brit_ned", data).run(DAY, run_id="r")
    entries = [
        {**_entry_template(0), "captures": [ids["O1"], ids["O2"]]},
        {**_entry_template(1), "captures": [ids["OLD"]]},
        {**_entry_template(2), "captures": [ids["ENC"]]},
    ]
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json(entries), encoding="utf-8"
    )
    return ids


def test_the_0xa0_capture_is_adjudicated(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A4. Detects the invalid-encoding capture left as an open gap (reconcile red forever),
    an entry that does not match its failure record (stale), or an adjudication that alters
    data: with the three committed BritNed entries every gap is adjudicated, none is open or
    stale, the ENC line names ``UnicodeDecodeError`` and ruling 575, and the silver and state
    bytes are equal before and after."""
    ids = _adjudicate_all_three(data, monkeypatch)
    before = (_tree_bytes(data, "silver"), _tree_bytes(data, "state"))
    code, lines = run_cli("brit_ned", "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert [line for line in lines if line.startswith("GAP")] == []
    assert "SUMMARY adjudicated 4" in lines
    assert "SUMMARY stale_adjudication 0" in lines
    encoded = [line for line in lines if ids["ENC"] in line]
    assert len(encoded) == 1
    assert encoded[0].startswith("ADJUDICATED failed brit_ned")
    assert "UnicodeDecodeError" in encoded[0] and "ruling 575" in encoded[0]
    assert (_tree_bytes(data, "silver"), _tree_bytes(data, "state")) == before


def test_a_pre_gate_compute_error_record_is_drained_into_the_ruled_class(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-MIG (FM-9, FM-11). Detects a pre-gate ``ComputeError`` failure record silently
    covered by the ``UnicodeDecodeError`` entry, or a drain that re-runs anything but the open
    gap: with the old record, reconcile exits 1 with the ENC gap open and its entry stale; one
    drain re-runs only the ENC capture, rewrites its record into ``UnicodeDecodeError`` and
    passes with all four gaps adjudicated, and the OLD record is byte-unchanged."""
    from gridflow.silver.neso_data_portal.completion import failure_path, write_failure
    from gridflow.silver.neso_data_portal.reconcile import drain

    ids = _adjudicate_all_three(data, monkeypatch)
    enc, old = ids["ENC"], ids["OLD"]
    write_failure(data, "brit_ned", enc, DAY, pl.exceptions.ComputeError("invalid utf-8 sequence"))
    old_bytes = failure_path(data, "brit_ned", old).read_bytes()
    code, lines = run_cli("brit_ned", "--cutoff", DAY.isoformat())
    assert code == 1, lines
    gaps = [line for line in lines if line.startswith("GAP")]
    assert len(gaps) == 2, gaps
    assert any(g.startswith("GAP failed") and enc in g and "ComputeError" in g for g in gaps), gaps
    assert any(g.startswith("GAP stale_adjudication") and enc in g for g in gaps), gaps
    report = drain(data, registry_module.load_registry(), ["brit_ned"], DAY, lambda: None)
    assert report.drained == (("brit_ned", DAY, 1),)
    assert report.passed, report.lines()
    assert len(report.adjudicated) == 4
    failure = read_failure(data, "brit_ned", enc)
    assert failure is not None and failure["error_class"] == "UnicodeDecodeError"
    with pytest.raises(UnicodeDecodeError) as decoded:
        body("ENC").decode("utf-8")
    assert failure["message"] == str(decoded.value)
    assert failure_path(data, "brit_ned", old).read_bytes() == old_bytes


ENCODING_ENTRY: dict[str, Any] = {
    "family": "brit_ned",
    "category": "failed",
    "cause": "UnicodeDecodeError",
    "captures": [
        "bronze/neso_data_portal/brit_ned/2026/10/08/"
        "raw_20261008T085647Z_811bec71-f099-4474-ba5e-2f9932b39cc2_24bb3d9b.csv"
    ],
    "reason": (
        "the 20241016 weekly upload is ASCII except two standalone 0xA0 bytes (a cp1252 or "
        "Latin-1 no-break space) in its 20241018 21:00-22:00 and 22:00-23:00 rows, so it is not "
        "valid in the record's declared UTF-8 and the capture fails with UnicodeDecodeError; it "
        "is never re-decoded and its 144 rows stay unloaded with their failure record"
    ),
    "question": (
        "Which text encoding does resource 811bec71-f099-4474-ba5e-2f9932b39cc2 (BritNed DA & ID "
        "Weekly ITLs 20241016) use, and can NESO republish it as UTF-8?"
    ),
    "evidence": "K-IC-2H-SPEC §Problem",
    "ruling": "575",
}
"""The ruled IC-2H entry (RULINGS 575), field for field."""


def test_the_committed_ledger_carries_the_three_brit_ned_entries() -> None:
    """Detects the committed entries drifting from the ruled text (another capture, a wider
    scope, an edited reason or question), a ledger the registry rejects, an entry naming a
    resource that is not a BritNed weekly upload, or a stray entry for another family; in a
    fresh interpreter. The whole ledger is the ruled families: GEN-2H's two (547), NSL (565),
    BritNed's two (571), BritNed's invalid-encoding capture (575), the 2022 regional FES
    GSP lookup capture (607), the three 2020-2022 building block captures (608) and the
    day-ahead constraint archive (632), the six Pathfinder captures (642) and the Historic II
    overlap (653), appended last."""
    loaded = _fresh_interpreter(
        """
        import json
        from gridflow.connectors.neso_data_portal.registry import (
            CAPTURE_ID_PATTERN, load_reconcile_adjudications, load_registry,
            reconcile_adjudication_problems,
        )
        registry = load_registry()
        entries = load_reconcile_adjudications()
        assert reconcile_adjudication_problems(registry, entries) == []
        brit = [e for e in entries if e.family == "brit_ned"]
        names = [
            sorted(
                registry.resources[CAPTURE_ID_PATTERN.fullmatch(c)["rid"]][1].name
                for c in e.captures
            )
            for e in brit
        ]
        print(json.dumps({
            "entries": [e.model_dump(mode="json") for e in brit],
            "names": names,
            "families": [e.family for e in entries],
        }))
        """
    )
    assert loaded["families"] == [
        "metered_wind_output_monthly",
        "wind_bmu_boa_volumes",
        "nsl",
        "brit_ned",
        "brit_ned",
        "brit_ned",
        "fes_regional_gsp_info",
        "fes_building_blocks_main",
        "fes_building_blocks_main",
        "fes_building_blocks_main",
        "da_constraint_flows_limits",
        "stability_pathfinder_utilisation_report",
        "stability_pathfinder_utilisation_report",
        "stability_pathfinder_availability_report",
        "stability_pathfinder_availability_report",
        "stability_pathfinder_availability_report",
        "stability_pathfinder_availability_report",
        "current_bsuos_historic_ii",
    ]
    directory = "bronze/neso_data_portal/brit_ned/2026/10/08/"
    overlap, failed, encoding = loaded["entries"]
    assert (overlap["category"], overlap["cause"], overlap["ruling"]) == ("overlap", None, "571")
    assert overlap["captures"] == [
        directory + "raw_20261008T085916Z_f43260c8-c559-4415-a369-0eb4b9c4e6b2_87ed618c.csv",
        directory + "raw_20261008T085918Z_3e546d9e-32fc-4cbd-bd51-214b106907e6_c654d837.csv",
    ]
    assert (failed["category"], failed["cause"], failed["ruling"]) == (
        "failed",
        "DuplicateEntityKeyError",
        "571",
    )
    assert failed["captures"] == [
        directory + "raw_20261008T085215Z_10432bfd-2102-4eda-8c6d-bed2a4df676b_a7f5cb18.csv"
    ]
    for entry in (overlap, failed):
        assert entry["evidence"].startswith("K-IC-2-FACTS §2")
    assert encoding == ENCODING_ENTRY
    assert loaded["names"] == [
        ["BritNed DA & ID Weekly ITLs 20251229", "BritNed DA & ID Weekly ITLs 20251231"],
        ["BritNed DA & ID Weekly ITLs 20221012"],
        ["BritNed DA & ID Weekly ITLs 20241016"],
    ]


# --------------------------------------------------------------------------- #
# Nord Pool day-ahead prices
# --------------------------------------------------------------------------- #


def test_nordpool_types_with_no_exclusion_and_keeps_every_price(data: Path) -> None:
    """Detects a cast the vendor body does not satisfy, a sentinel invented for ``-1``, a
    dropped negative price or a rounded price: the dump completes with every row and zero
    exclusions, ``Date`` is a date, the interval a byte-identical string, ``Price`` a float64
    equal to its cell, the two ``-1.000000`` prices and the negatives stay real prices and
    the key ``(date, delivery_period)`` is unique."""
    meta = CAPTURES["NP"]
    transformer = get_transformer(SOURCE, "nordpool_da_prices", data)
    capture_id = capture(data, "NP")
    assert transformer.run(DAY, run_id="r") == len(rows("NP"))
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, "nordpool_da_prices")
    source = rows("NP")
    assert frame.schema["date"] == pl.Date and frame.schema["price"] == pl.Float64
    assert frame.schema["delivery_period"] == pl.Utf8
    assert frame["date"].to_list() == [date.fromisoformat(r["Date"]) for r in source]
    assert frame["delivery_period"].to_list() == [r["Delivery Period"] for r in source]
    assert frame["price"].to_list() == [float(r["Price"]) for r in source]
    assert frame.filter(pl.col("price") == -1.0).height == 2
    assert frame.filter(pl.col("price") < 0).height > 2
    assert frame["price"].null_count() == 0
    assert frame.select(["date", "delivery_period"]).is_duplicated().sum() == 0


def test_nordpool_dst_day_keeps_24_gmt_hours_and_the_clock_is_the_capture_time(
    data: Path,
) -> None:
    """Detects a UK-local-day model applied to the UTC delivery date, or a delivery instant
    invented for ``timestamp_utc``: 2024-10-27 (the UK fall-back day) keeps 24 distinct GMT
    intervals with no fold marker, and ``timestamp_utc`` is the **capture time** on every row
    (the record's temporal recipe is ``none`` until a date-plus-interval recipe exists), so the
    delivery instant is only ``date`` plus the interval's start hour, UTC, for a consumer."""
    _load(data, "NP")
    frame = _silver(data, "nordpool_da_prices")
    day = frame.filter(pl.col("date") == date(2024, 10, 27))
    assert day.height == 24 and day["delivery_period"].n_unique() == 24
    assert not any("(" in v for v in day["delivery_period"].to_list())
    written = datetime.fromisoformat(CAPTURES["NP"].written).astimezone(UTC)
    assert set(frame["timestamp_utc"].to_list()) == {written}
    assert "issue_time" not in frame.columns or frame["issue_time"].null_count() == frame.height
    assert set(frame["available_at"].to_list()) == {written}
    assert frame.schema["timestamp_utc"] == pl.Datetime("us", "UTC")


# --------------------------------------------------------------------------- #
# System Operating Plan
# --------------------------------------------------------------------------- #


def test_sop_types_with_no_exclusion_and_keeps_blanks_zeros_and_minus_ones(data: Path) -> None:
    """Detects a cast the vendor body does not satisfy (the 93-column header, ``report_date``
    serialised as a midnight datetime, an integer version), a blank filled with zero, a ``-1``
    or ``0`` treated as a sentinel, or a lost column: the dump completes with every row and
    zero exclusions, every physical column is a float64 equal to its cell (blank -> null, ``-1``
    and ``0`` kept), ``latest_version`` an int64, the status and cardinal point raw strings."""
    meta = CAPTURES["SOP"]
    transformer = get_transformer(SOURCE, "system_operating_plan", data)
    capture_id = capture(data, "SOP")
    assert transformer.run(DAY, run_id="r") == len(rows("SOP"))
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, meta.family, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    frame = _silver(data, "system_operating_plan")
    source = rows("SOP")
    for name in SOP_FLOATS:
        header = next(h for h in source[0] if h.lower() == name)
        assert frame.schema[name] == pl.Float64, name
        assert frame[name].to_list() == [float(r[header]) if r[header] else None for r in source]
    assert frame.schema["latest_version"] == pl.Int64
    assert frame["latest_version"].to_list() == [int(r["latest_version"]) for r in source]
    for name in ("latest_status", "cardinal_point"):
        assert frame[name].to_list() == [r[name] for r in source]
    assert frame["imbalance"].to_list().count(-1.0) == sum(r["imbalance"] == "-1" for r in source)
    assert frame["imbalance"].to_list().count(-1.0) > 0
    assert frame["britned_temx"].to_list().count(0.0) > 0
    assert frame["dsbr"].null_count() == sum(r["dsbr"] == "" for r in source) > 0
    assert frame.select(["issue_time", "sop_datetime"]).is_duplicated().sum() == 0


def test_sop_clocks_are_utc_the_issue_is_the_creation_time_and_report_date_is_a_date(
    data: Path,
) -> None:
    """Detects a naive or shifted instant, an issue time taken from anywhere but the creation
    column, a target that is not ``sop_datetime`` or ``report_date`` read as an instant: the
    three datetimes are tz-aware UTC and equal their cells, ``issue_time`` equals the creation
    time, ``timestamp_utc`` is the target, and ``report_date`` is a date equal to the first ten
    characters of its midnight serialisation (it differs from the creation date on some rows
    and is never an issue or a target)."""
    _load(data, "SOP")
    frame = _silver(data, "system_operating_plan")
    source = rows("SOP")
    utc = pl.Datetime("us", "UTC")
    for name in ("sop_datetime", "sop_report_creation_time_gmt", "sop_d_and_c_time_gmt"):
        assert frame.schema[name] == utc, name
        assert frame[name].to_list() == [_instant(r[name]) for r in source], name
    assert frame.schema["issue_time"] == utc and frame.schema["timestamp_utc"] == utc
    assert frame["issue_time"].to_list() == frame["sop_report_creation_time_gmt"].to_list()
    assert frame["timestamp_utc"].to_list() == frame["sop_datetime"].to_list()
    assert frame.schema["report_date"] == pl.Date
    assert frame["report_date"].to_list() == [
        date.fromisoformat(r["report_date"][:10]) for r in source
    ]
    assert any(
        d != c.date()
        for d, c in zip(
            frame["report_date"].to_list(),
            frame["sop_report_creation_time_gmt"].to_list(),
            strict=True,
        )
    )
