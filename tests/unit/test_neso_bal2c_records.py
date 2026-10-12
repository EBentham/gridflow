"""The tariff batch's nine frozen records, all held (v0.22-K-BAL-2c).

``aahedc_tariffs`` and the eight ``tnuos_tariffs_*`` families get records whose eligibility is
**held** (`TARIFF-ISSUE-AVAILABILITY`): every body mixes Forecast / Draft / Final rows with no
publication instant, so what was available when is unknown and a publication date alone does not
establish availability at midnight. Every test writes recorded fixture captures (cuts of the
2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/bal2c/``, provenance in
``PROVENANCE.md``) into a short data root and runs the transformer the **real package registry**
generates, so a record that does not fit its vendor body fails here, not at activation. On master
none of the nine families has a record, so ``get_transformer`` raises for each of them.

Record decisions under test (K-BAL-2-FACTS g1-g5 and section 6, K-BAL-2c-SPEC):

- Temporal ``none``, issue ``none``, ``whole_capture``, ``ckan_last_modified`` vintage: the
  financial year and the publication date are labels, not instants (g1), and no body has an issue
  column (g2). The stage labels ``Forecast`` / ``Draft`` / ``Final`` are kept as supplied; a Final
  row is never relabelled a forecast.
- ``Year_FY`` / ``Year FY`` is an int64 identifier (the ending year of an April-March financial
  year, no unit). ``Published_Date`` is a ``date`` (``%d/%m/%Y``) in eight families and the **raw
  string** in ``tnuos_tariffs_onshore_local_circuit`` (one resource holds two spellings, so no
  single format and no ``formats_by_filename`` entry can apply; TODO:
  CIRCUIT-PUBLICATION-DATE-CAST).
- Zone numbers are int64 identifiers; zone names, generator names, substation names, voltage,
  TEC band, connection type and TDR band stay strings as supplied: no trim (V-19 forbids it on a
  key anyway), no normalisation. The 78 local-circuit substation names with a trailing space or tab
  are kept, and the question whether they are meaningful is part of that record's hold.
- Units live in FACTS g4 / section 6 (the record model has no unit field) and in the silver names:
  AAHEDC and NHH demand pence/kWh (``p_per_kwh``), the other tariff columns GBP/kW
  (``gbp_per_kw``, never per year), the local substation ``Tariff`` GBP/kW on the vendor tariff
  statement's authority (FACTS T02 / W09, the header carries no unit). The TDR value column has no
  unit suffix: ordinary bands are GBP/(site day) but the unmetered band's unit conflicts
  (TODO: TDR-UNMETERED-UNIT), so the silver name is ``tdr_tariff`` and the conflict is in the hold.
- The TDR header is literally ``TDR Tariff in <A-circumflex><pound>/(Site Day)`` (UTF-8 bytes
  ``c3 82 c2 a3``, source mojibake): kept as the vendor column source, the silver name is clean.
- Zeros (``PhasedResidual``, ``SGD`` adjustments, ``SmallGenDiscount``, ``NHH_Zonal``) are values,
  never null. The wholly blank rows of the offshore generator (113) and local substation (48)
  bodies are dropped by the shared CSV reader before typing (counted in an INFO log, not in the
  completion tally); a partly blank row is excluded by the non-nullable key columns and tallied.
- No ADR-034 amendment-1 trim: the seat's full-body check (RULINGS 692) found no padded and no
  non-numeric cell in any measure column.

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the bronze
originals' CRLF; a BOM stays where the original had one."""

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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "bal2c"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-BAL-2c"
BOM = b"\xef\xbb\xbf"
AAHEDC_PACKAGE = ("aahedc-tariffs", "43d94335-c97f-4939-a8b9-607d2cfb3734")
TNUOS_PACKAGE = (
    "transmission-network-use-of-system-tnuos-tariffs",
    "eaef4708-1100-4ad9-98d5-d892f3c9a56c",
)

BASE_Q = (
    "TODO: TARIFF-ISSUE-AVAILABILITY — each body mixes Forecast/Draft/Final rows with no "
    "publication instant; what was available when (publication-level evidence), and how should "
    "mixed stages be treated? A publication date alone does not establish availability at midnight."
)
TDR_Q = (
    f"{BASE_Q} TODO: TDR-UNMETERED-UNIT — ordinary bands are £/(site day), but the unmetered "
    "band's unit conflicts (the dictionary says p/kWh per day, the final tariff statement "
    "p/kWh/annum in its summary and p/kWh in its detailed table); which unit does the Unmetered "
    "band carry in this column?"
)
CIRCUIT_Q = (
    f"{BASE_Q} Are trailing spaces / tab in `Substation_Name` meaningful, or should NESO "
    "republish them trimmed?"
)
HELD = {
    "aahedc_tariffs": BASE_Q,
    "tnuos_tariffs_embedded_export": BASE_Q,
    "tnuos_tariffs_half_hourly_hh_demand": BASE_Q,
    "tnuos_tariffs_non_half_hourly_nhh_demand": BASE_Q,
    "tnuos_tariffs_onshore_generator": BASE_Q,
    "tnuos_tariffs_local_offshore_generator": BASE_Q,
    "tnuos_tariffs_local_substation_tariff": BASE_Q,
    "tnuos_tariffs_transmission_demand": TDR_Q,
    "tnuos_tariffs_onshore_local_circuit": CIRCUIT_Q,
}
RECORDED = tuple(HELD)
PACKAGE_OF = {
    key: (AAHEDC_PACKAGE if key == "aahedc_tariffs" else TNUOS_PACKAGE) for key in RECORDED
}

POUND = "£"
P_HEADER = ["Publication", "Year_FY", "Published_Date"]
ZONE_HEADER = ["Zone_No", "Zone_Name"]
HEADER_OF: dict[str, list[str]] = {
    "aahedc_tariffs": [
        "Publication Type",
        "Year FY",
        "Published Date",
        "Total Scheme Tariff in p/kwh",
        "Shetland Tariff in p/kwh",
        "AAHEDC tariff excluding the Shetland Assistance Amount in p/kwh",
    ],
    "tnuos_tariffs_embedded_export": [
        *P_HEADER,
        *ZONE_HEADER,
        f"Locational_{POUND}/kW",
        f"AGIC_{POUND}/kW",
        f"PhasedResidual_{POUND}/kW",
        f"EET(Floored)_{POUND}/kW",
    ],
    "tnuos_tariffs_half_hourly_hh_demand": [
        *P_HEADER,
        *ZONE_HEADER,
        f"Peak_{POUND}/kW",
        f"YearRound_{POUND}/kW",
        f"HH_SGD_Adjustment_{POUND}/kW",
        f"HHTariff(Floored)_{POUND}/kW",
    ],
    "tnuos_tariffs_non_half_hourly_nhh_demand": [
        *P_HEADER,
        *ZONE_HEADER,
        "NHH_Zonal_p/kWh",
        "NHH_SGD_Adjustment_p/kWh",
        "NHHTariff(Floored)_p/kWh",
    ],
    "tnuos_tariffs_onshore_generator": [
        *P_HEADER,
        *ZONE_HEADER,
        f"SystemPeak_{POUND}/kW",
        f"SharedYearRound_{POUND}/kW",
        f"NotSharedYearRound_{POUND}/kW",
        f"Residual_{POUND}/kW",
        f"SmallGenDiscount_{POUND}/kW",
    ],
    "tnuos_tariffs_local_offshore_generator": [
        *P_HEADER,
        "OffshoreGenerator",
        f"Substation_{POUND}/kW",
        f"Circuit_{POUND}/kW",
        f"ETUoS_{POUND}/kW",
    ],
    "tnuos_tariffs_local_substation_tariff": [
        *P_HEADER,
        "Voltage",
        "Sum of TEC at Connecting Substation",
        "Connection Type",
        "Tariff",
    ],
    "tnuos_tariffs_transmission_demand": [
        *P_HEADER,
        "TDR Band",
        "TDR Tariff in Â£/(Site Day)",
        "Notes",
    ],
    "tnuos_tariffs_onshore_local_circuit": [
        *P_HEADER,
        "Substation_Name",
        f"Tariff_{POUND}/kW",
    ],
}
NAMES_OF: dict[str, list[str]] = {
    "aahedc_tariffs": [
        "publication_type",
        "year_fy",
        "published_date",
        "total_scheme_tariff_p_per_kwh",
        "shetland_tariff_p_per_kwh",
        "tariff_excluding_shetland_assistance_amount_p_per_kwh",
    ],
    "tnuos_tariffs_embedded_export": [
        "publication",
        "year_fy",
        "published_date",
        "zone_no",
        "zone_name",
        "locational_gbp_per_kw",
        "agic_gbp_per_kw",
        "phased_residual_gbp_per_kw",
        "eet_floored_gbp_per_kw",
    ],
    "tnuos_tariffs_half_hourly_hh_demand": [
        "publication",
        "year_fy",
        "published_date",
        "zone_no",
        "zone_name",
        "peak_gbp_per_kw",
        "year_round_gbp_per_kw",
        "hh_sgd_adjustment_gbp_per_kw",
        "hh_tariff_floored_gbp_per_kw",
    ],
    "tnuos_tariffs_non_half_hourly_nhh_demand": [
        "publication",
        "year_fy",
        "published_date",
        "zone_no",
        "zone_name",
        "nhh_zonal_p_per_kwh",
        "nhh_sgd_adjustment_p_per_kwh",
        "nhh_tariff_floored_p_per_kwh",
    ],
    "tnuos_tariffs_onshore_generator": [
        "publication",
        "year_fy",
        "published_date",
        "zone_no",
        "zone_name",
        "system_peak_gbp_per_kw",
        "shared_year_round_gbp_per_kw",
        "not_shared_year_round_gbp_per_kw",
        "residual_gbp_per_kw",
        "small_gen_discount_gbp_per_kw",
    ],
    "tnuos_tariffs_local_offshore_generator": [
        "publication",
        "year_fy",
        "published_date",
        "offshore_generator",
        "substation_gbp_per_kw",
        "circuit_gbp_per_kw",
        "etuos_gbp_per_kw",
    ],
    "tnuos_tariffs_local_substation_tariff": [
        "publication",
        "year_fy",
        "published_date",
        "voltage",
        "sum_of_tec_at_connecting_substation",
        "connection_type",
        "tariff_gbp_per_kw",
    ],
    "tnuos_tariffs_transmission_demand": [
        "publication",
        "year_fy",
        "published_date",
        "tdr_band",
        "tdr_tariff",
        "notes",
    ],
    "tnuos_tariffs_onshore_local_circuit": [
        "publication",
        "year_fy",
        "published_date",
        "substation_name",
        "tariff_gbp_per_kw",
    ],
}
KEY_OF: dict[str, list[str]] = {
    "aahedc_tariffs": ["publication_type", "year_fy", "published_date"],
    "tnuos_tariffs_embedded_export": ["publication", "year_fy", "published_date", "zone_no"],
    "tnuos_tariffs_half_hourly_hh_demand": ["publication", "year_fy", "published_date", "zone_no"],
    "tnuos_tariffs_non_half_hourly_nhh_demand": [
        "publication",
        "year_fy",
        "published_date",
        "zone_no",
    ],
    "tnuos_tariffs_onshore_generator": ["publication", "year_fy", "published_date", "zone_no"],
    "tnuos_tariffs_local_offshore_generator": [
        "publication",
        "year_fy",
        "published_date",
        "offshore_generator",
    ],
    "tnuos_tariffs_local_substation_tariff": [
        "publication",
        "year_fy",
        "published_date",
        "voltage",
        "sum_of_tec_at_connecting_substation",
        "connection_type",
    ],
    "tnuos_tariffs_transmission_demand": ["publication", "year_fy", "published_date", "tdr_band"],
    "tnuos_tariffs_onshore_local_circuit": [
        "publication",
        "year_fy",
        "published_date",
        "substation_name",
    ],
}
INT_COLUMNS = {"year_fy", "zone_no"}
NULLABLE_STRINGS = {"zone_name", "notes"}
STRING_COLUMNS = {
    "publication",
    "publication_type",
    "zone_name",
    "offshore_generator",
    "voltage",
    "sum_of_tec_at_connecting_substation",
    "connection_type",
    "tdr_band",
    "notes",
    "substation_name",
}
BLANK_ROWS = {
    "tnuos_tariffs_local_offshore_generator": 2,
    "tnuos_tariffs_local_substation_tariff": 2,
}
"""The families whose fixture keeps wholly blank rows, and how many (113 and 48 in the bodies)."""
CLEAN = tuple(key for key in RECORDED if key not in BLANK_ROWS)
STAGES = {"Forecast", "Draft", "Final"}


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


CAPTURES: dict[str, Capture] = {
    "aa": Capture(
        "aa.csv",
        "aahedc_tariffs",
        "ffd29cc8-3c55-4e83-aa0e-73212d4fedba",
        "Assistance for Areas of High Electricity Distribution Costs (AAHEDC) Tariffs",
        "aahedc-tariffs-for-dataportal-2026-07.csv",
        "2026-07-15T09:16:36.703704",
        "2026-10-08T08:13:00.098981+00:00",
    ),
    "ee": Capture(
        "ee.csv",
        "tnuos_tariffs_embedded_export",
        "af4501a0-6b96-4088-926c-7c7b0c499b08",
        "Embedded Export Tariffs",
        "tnuos_embeddedexport.csv",
        "2026-05-28T16:24:12.610430",
        "2026-10-08T11:39:04.071264+00:00",
    ),
    "hh": Capture(
        "hh.csv",
        "tnuos_tariffs_half_hourly_hh_demand",
        "6abe8416-149c-4c78-b65b-059eea19957a",
        "Half-Hourly (HH) Demand Tariffs",
        "tnuos_demand_hh.csv",
        "2026-05-12T13:12:39.529022",
        "2026-10-08T11:39:07.650463+00:00",
    ),
    "of": Capture(
        "of.csv",
        "tnuos_tariffs_local_offshore_generator",
        "98569a29-940d-4bb7-8dc4-01e46187039a",
        "Local Offshore Generator Tariffs",
        "tnuos_generation_offshore.csv",
        "2026-05-12T13:07:57.900619",
        "2026-10-08T11:39:11.619050+00:00",
    ),
    "ls": Capture(
        "ls.csv",
        "tnuos_tariffs_local_substation_tariff",
        "e697e9be-c068-482d-ae35-01f60bcb6700",
        "Local Substation TNUoS Tariff",
        "local-substation-tnuos-tariff.csv",
        "2026-05-12T13:05:48.317924",
        "2026-10-08T11:39:15.061170+00:00",
    ),
    "nh": Capture(
        "nh.csv",
        "tnuos_tariffs_non_half_hourly_nhh_demand",
        "4e8dd130-5283-4f22-8c05-84e4d3280eb3",
        "Non Half-Hourly (NHH) Demand Tariffs",
        "tnuos_demand_nhh.csv",
        "2026-05-12T13:13:48.215441",
        "2026-10-08T11:39:18.819254+00:00",
    ),
    "og": Capture(
        "og.csv",
        "tnuos_tariffs_onshore_generator",
        "ffc0adb8-5426-474e-94a3-a5f3f01a0cdb",
        "Onshore Generator Tariffs",
        "tnuos_generation_onshore.csv",
        "2026-05-12T13:09:05.384311",
        "2026-10-08T11:39:22.460136+00:00",
    ),
    "lc": Capture(
        "lc.csv",
        "tnuos_tariffs_onshore_local_circuit",
        "01f36b32-4ad3-47fa-a645-7f7a47f1b0ae",
        "Onshore Local Circuit Tariffs",
        "tnuos_onshore_local_circuit.csv",
        "2026-05-12T13:10:10.034982",
        "2026-10-08T11:39:26.347590+00:00",
    ),
    "td": Capture(
        "td.csv",
        "tnuos_tariffs_transmission_demand",
        "dcca94fd-343e-4d4e-8c5d-66009dec4ad3",
        "Transmission Demand Residual (TDR) Tariffs",
        "tnuos_tdr-tariffs.csv",
        "2026-05-12T13:06:47.185973",
        "2026-10-08T11:39:30.082617+00:00",
    ),
}
ALIAS_OF = {meta.family: alias for alias, meta in CAPTURES.items()}


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
        prefix="ba2c", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's CRLF line endings (a BOM stays)."""
    raw = (FIXTURES / CAPTURES[alias].fixture).read_bytes()
    bom = BOM if raw.startswith(BOM) else b""
    return bom + raw[len(bom) :].replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")


def _reader(raw: bytes) -> csv.DictReader[str]:
    return csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))


def rows(alias: str, raw: bytes | None = None, *, keep_blank: bool = False) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim); wholly blank rows only on
    request."""
    reader = _reader(raw if raw is not None else body(alias))
    return [r for r in reader if keep_blank or any(v for v in r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8-sig").splitlines()[0]
    return next(csv.reader([first]))


def capture(data: Path, alias: str, *, raw: bytes | None = None) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    slug, package_id = PACKAGE_OF[meta.family]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=slug,
        package_id=package_id,
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


def _package_doc(slug: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / f"{slug}.json").read_text("utf-8"))
    return document


def _columns(key: str) -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record(key)))
        if c not in ("year", "month")
    ]


def _load_clean(data: Path, family: str) -> pl.DataFrame:
    """Capture and run ``family``'s fixture: every row written, none excluded, the generic output
    columns, no BOM in any column name."""
    alias = ALIAS_OF[family]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    completion = read_completion(data, family, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
    assert read_failure(data, family, capture_id) is None
    frame = _silver(data, family)
    assert frame.height == len(rows(alias))
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(family)
    assert not [c for c in frame.columns if "﻿" in c]
    return frame


def _expected(family: str, source: str, name: str, cell: str) -> Any:
    """What the silver cell must be for body cell ``cell`` (a blank string cell is null)."""
    if name in INT_COLUMNS:
        return int(cell)
    if name == "published_date" and family != "tnuos_tariffs_onshore_local_circuit":
        return datetime.strptime(cell, "%d/%m/%Y").date()
    if name in STRING_COLUMNS or name == "published_date":
        return cell if cell != "" else None
    return float(cell) if cell != "" else None


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header, the
    BOM on exactly the eight TNUoS bodies (AAHEDC has none), the CRLF originals, all three stages in
    every TNUoS cut (AAHEDC holds Draft and Final only), a wholly blank row in the offshore
    generator and local substation cuts, both publication-date spellings plus a space-ended and
    a tab-ended substation name in the local circuit cut, the mojibake TDR header bytes, the
    unmetered TDR band, and a zero in the zero-run columns."""
    for alias, meta in CAPTURES.items():
        assert header(alias) == HEADER_OF[meta.family], alias
        raw = (FIXTURES / meta.fixture).read_bytes()
        assert raw.startswith(BOM) == (alias != "aa"), alias
        assert b"\r\n" in body(alias), alias
        stages = {r[HEADER_OF[meta.family][0]] for r in rows(alias)}
        assert stages == ({"Draft", "Final"} if alias == "aa" else STAGES), alias
    for alias, count in ((ALIAS_OF[f], n) for f, n in BLANK_ROWS.items()):
        blanks = rows(alias, keep_blank=True)
        assert len([r for r in blanks if not any(r.values())]) == count, alias
    circuit = rows("lc")
    dates = {r["Published_Date"] for r in circuit}
    assert "2025/09/12" in dates and "11/05/2026" in dates
    names = {r["Substation_Name"] for r in circuit}
    assert "CHLEANSAID " in names and "Fasque\t" in names
    assert "TDR Tariff in Â£/(Site Day)" in header("td")
    assert b"TDR Tariff in \xc3\x82\xc2\xa3/(Site Day)" in body("td")
    assert "Unmetered" in {r["TDR Band"] for r in rows("td")}
    for alias, column in (
        ("ee", f"PhasedResidual_{POUND}/kW"),
        ("hh", f"HH_SGD_Adjustment_{POUND}/kW"),
        ("nh", "NHH_Zonal_p/kWh"),
        ("og", f"SmallGenDiscount_{POUND}/kW"),
        ("of", f"ETUoS_{POUND}/kW"),
        ("lc", f"Tariff_{POUND}/kW"),
    ):
        assert any(float(r[column]) == 0 for r in rows(alias)), alias


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_one_epoch_csv_utf8_with_ckan_last_modified_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time, a publication instant, a run, a sibling or a
    per-resource selection, or an epoch other than the one vendor header: every record is version
    1, utf-8 CSV, ``ckan_last_modified`` vintage (every capture is an upload), whole-capture
    selection with no partition, temporal ``none`` (the financial year and publication date are
    labels, not instants), one epoch whose header is the body's, no issue recipe, no run column and
    the spec's entity key."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert (record.vintage, record.vintage_evidence) == ("ckan_last_modified", None)
    assert (record.latest, record.latest_partition) == ("whole_capture", None)
    assert record.temporal.kind == "none"
    assert list(record.entity_key) == KEY_OF[key]
    assert len(record.epochs) == 1
    assert list(record.epochs[0].header) == HEADER_OF[key]
    assert record.epochs[0].issue.kind == "none" and has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None
    assert record.zip_member is None


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a trimmed, token-mapped, bounded or relabelled column, a financial year given a
    unit or a string type, a publication date left a string (or parsed) in the wrong family, or a
    lost nullability: ``year_fy`` and ``zone_no`` are non-nullable int64 identifiers with no
    min/max, ``published_date`` is a non-nullable ``date`` in ``%d/%m/%Y`` everywhere except the
    local circuit (a non-nullable raw string), every label column is a string with no format (only
    the zone name and the TDR notes may be null), every measure is a nullable float64, and no
    column declares ``trim``, ``null_tokens`` or ``formats_by_filename``."""
    columns = _record(key).epochs[0].columns
    assert [c.name for c in columns] == NAMES_OF[key]
    for column in columns:
        assert column.trim is None and column.null_tokens == ()
        assert (column.min, column.max) == (None, None)
        assert column.formats_by_filename is None
        if column.name in INT_COLUMNS:
            assert (column.dtype, column.nullable, column.format) == ("int64", False, None)
        elif column.name == "published_date":
            if key == "tnuos_tariffs_onshore_local_circuit":
                assert (column.dtype, column.nullable, column.format) == ("string", False, None)
            else:
                assert (column.dtype, column.nullable, column.format) == (
                    "date",
                    False,
                    "%d/%m/%Y",
                )
        elif column.name in STRING_COLUMNS:
            assert column.dtype == "string" and column.format is None, column.name
            assert column.nullable == (column.name in NULLABLE_STRINGS), column.name
        else:
            assert (column.dtype, column.nullable) == ("float64", True), column.name
    key_columns = set(KEY_OF[key])
    assert all(not c.nullable for c in columns if c.name in key_columns)


def test_the_unit_is_in_the_silver_name_and_never_rescaled() -> None:
    """Detects a unit dropped, converted or made annual: AAHEDC and the NHH demand columns are
    pence/kWh (``p_per_kwh``), the embedded export, HH demand, onshore generator, offshore
    generator, local circuit and local substation columns are GBP/kW (``gbp_per_kw``, never
    ``per_year``), and the TDR value column carries no unit suffix because the unmetered band's
    unit conflicts (TODO: TDR-UNMETERED-UNIT)."""
    for key in RECORDED:
        measures = [c.name for c in _record(key).epochs[0].columns if c.dtype == "float64"]
        assert measures, key
        if key == "tnuos_tariffs_transmission_demand":
            assert measures == ["tdr_tariff"]
            continue
        suffix = (
            "_p_per_kwh"
            if key in ("aahedc_tariffs", "tnuos_tariffs_non_half_hourly_nhh_demand")
            else "_gbp_per_kw"
        )
        assert all(m.endswith(suffix) for m in measures), key
        assert not [m for m in measures if m.endswith("_per_year")]
    assert len(_record("tnuos_tariffs_embedded_export").epochs[0].columns) == 9


@pytest.mark.parametrize("key", list(HELD))
def test_every_record_is_held_with_the_ruled_question_and_the_package_stays_eligible(
    key: str,
) -> None:
    """Detects a hold lost, reworded or moved to the package, or a family-specific addition
    missing: each of the nine records carries its verbatim question and this unit, is effectively
    held, and the package itself stays eligible (the TDR record adds the unmetered-unit conflict,
    the local circuit record the trailing-whitespace question)."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == (UNIT, HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")
    assert "TARIFF-ISSUE-AVAILABILITY" in record.eligibility.question
    assert ("TDR-UNMETERED-UNIT" in record.eligibility.question) == (
        key == "tnuos_tariffs_transmission_demand"
    )
    assert ("trailing spaces / tab" in record.eligibility.question) == (
        key == "tnuos_tariffs_onshore_local_circuit"
    )


def test_the_package_files_carry_exactly_the_nine_records_and_their_resources() -> None:
    """Detects a record on the wrong family or a resource disposition drifting: aahedc-tariffs.json
    holds one record, the TNUoS file eight, every CSV resource is SILVER of its family, each
    capture's resource id belongs to the family the fixture is for, and no family gained a
    transformer."""
    aahedc = _package_doc(AAHEDC_PACKAGE[0])
    tnuos = _package_doc(TNUOS_PACKAGE[0])
    assert {f["key"] for f in aahedc["families"] if "record" in f} == {"aahedc_tariffs"}
    assert {f["key"] for f in tnuos["families"] if "record" in f} == set(RECORDED) - {
        "aahedc_tariffs"
    }
    assert len(tnuos["families"]) == 8 and len(tnuos["resources"]) == 8
    for document in (aahedc, tnuos):
        assert all(f["transformer"] is None for f in document["families"])
        for resource in document["resources"]:
            assert resource["disposition"] == {"kind": "SILVER", "key": resource["family"]}
    registry = load_registry()
    for alias, meta in CAPTURES.items():
        assert registry.resources[meta.resource_id][1].family == meta.family, alias


def test_all_nine_are_reported_as_held_silver_outputs() -> None:
    """Detects a family reported as ingest-only or eligible: the eligibility report lists each of
    the nine as ``silver`` with ``held: TODO:`` and this unit, ``whole_capture``, temporal
    ``none``."""
    report = eligibility_module.render(load_registry())
    for key in RECORDED:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver | held: TODO: TARIFF-ISSUE-AVAILABILITY" in line, key
        assert f"(unit {UNIT})" in line, key
        assert "| whole_capture | none |" in line, key


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("family", CLEAN)
def test_a_fixture_loads_with_zero_exclusions_and_every_cell_equal_to_the_body(
    data: Path, family: str
) -> None:
    """Detects a rescaled, trimmed, rounded, relabelled or null-for-zero cell, a BOM reaching the
    first silver column name, or a stage label rewritten: the slice loads every row with zero
    exclusions and no failure, ``year_fy`` is int64, ``published_date`` a ``date`` (the raw string
    for the local circuit), every identifier, label, date and measure equals the body cell, and the
    Forecast / Draft / Final labels are exactly the body's."""
    frame = _load_clean(data, family)
    alias = ALIAS_OF[family]
    source = rows(alias)
    assert frame.schema["year_fy"] == pl.Int64
    expected_date = pl.String if family == "tnuos_tariffs_onshore_local_circuit" else pl.Date
    assert frame.schema["published_date"] == expected_date
    for src, name in zip(HEADER_OF[family], NAMES_OF[family], strict=True):
        assert frame[name].to_list() == [_expected(family, src, name, r[src]) for r in source], name
    stage_name = "publication_type" if family == "aahedc_tariffs" else "publication"
    assert set(frame[stage_name].to_list()) == {r[HEADER_OF[family][0]] for r in source}
    assert set(frame[stage_name].to_list()) <= STAGES
    assert not frame.select(_record(family).entity_key).is_duplicated().any()


@pytest.mark.parametrize(
    ("family", "column"),
    [
        ("tnuos_tariffs_embedded_export", "phased_residual_gbp_per_kw"),
        ("tnuos_tariffs_half_hourly_hh_demand", "hh_sgd_adjustment_gbp_per_kw"),
        ("tnuos_tariffs_non_half_hourly_nhh_demand", "nhh_zonal_p_per_kwh"),
        ("tnuos_tariffs_onshore_generator", "small_gen_discount_gbp_per_kw"),
        ("tnuos_tariffs_onshore_local_circuit", "tariff_gbp_per_kw"),
    ],
)
def test_zero_runs_stay_zero_and_are_never_null(data: Path, family: str, column: str) -> None:
    """Detects a zero tariff (SGD adjustment, PhasedResidual, SmallGenDiscount, NHH zonal, a zero
    circuit tariff) read as missing (FACTS g5: a zero run is a value): the column has no nulls and
    holds 0.0 on exactly the body's zero rows."""
    frame = _load_clean(data, family)
    cells = [r[HEADER_OF[family][NAMES_OF[family].index(column)]] for r in rows(ALIAS_OF[family])]
    assert frame[column].null_count() == 0
    assert 0.0 in frame[column].to_list()
    assert [v == 0.0 for v in frame[column].to_list()] == [float(c) == 0 for c in cells]


def test_aahedc_keeps_draft_and_final_apart_and_the_three_pence_columns(data: Path) -> None:
    """Detects a Final row collapsed into its Draft row or relabelled a forecast, or a pence/kWh
    column swapped or rescaled: the nine rows are six Final and three Draft with the same financial
    year allowed under both labels (2025, 2026 and 2027 appear twice), and the three tariff
    columns equal the body's cells (the vendor's last row is loaded as published, with its
    Shetland and excluding-Shetland cells as supplied)."""
    frame = _load_clean(data, "aahedc_tariffs")
    assert frame["publication_type"].value_counts().sort("publication_type").rows() == [
        ("Draft", 3),
        ("Final", 6),
    ]
    assert frame.group_by("year_fy").len().filter(pl.col("len") == 2)[
        "year_fy"
    ].sort().to_list() == [
        2025,
        2026,
        2027,
    ]
    assert not frame.select("publication_type", "year_fy", "published_date").is_duplicated().any()


def test_wholly_blank_rows_are_dropped_by_the_reader_before_typing_and_logged(
    data: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects a wholly blank row (``,,,,,,``, 113 in the offshore generator body and 48 in the
    local substation body) reaching silver as a null-key row or failing the capture. The shared
    CSV reader drops such rows before typing and logs the count at INFO (``csv_bronze``); that is
    not an exclusion tally, so ``rows_excluded`` is 0 (the spec's "excluded by the non-nullable key
    columns with a tally" does not describe the existing mechanism, see PROVENANCE.md). Each
    fixture's two blank rows are dropped and named in the log, the other rows load in full and
    equal to the body, the key columns have no null and the key is unique."""
    for family in BLANK_ROWS:
        alias = ALIAS_OF[family]
        capture_id = capture(data, alias)
        with caplog.at_level("INFO", logger="gridflow.silver.csv_bronze"):
            written = get_transformer(SOURCE, family, data).run(DAY, run_id="r")
        source = rows(alias)
        assert written == len(source)
        assert len(rows(alias, keep_blank=True)) == len(source) + BLANK_ROWS[family]
        completion = read_completion(data, family, capture_id)
        assert completion is not None
        assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0)
        assert read_failure(data, family, capture_id) is None
        frame = _silver(data, family)
        assert frame.height == len(source)
        for column in KEY_OF[family]:
            assert frame[column].null_count() == 0, (family, column)
        assert not frame.select(KEY_OF[family]).is_duplicated().any()
        for src, name in zip(HEADER_OF[family], NAMES_OF[family], strict=True):
            assert frame[name].to_list() == [
                _expected(family, src, name, r[src]) for r in source
            ], name
    dropped = [r.getMessage() for r in caplog.records if "blank row" in r.getMessage()]
    assert len(dropped) == len(BLANK_ROWS)
    assert all(m.startswith("dropped 2 blank row(s)") for m in dropped)


@pytest.mark.parametrize("family", list(BLANK_ROWS))
def test_a_partly_blank_row_is_excluded_by_its_non_nullable_key_column_with_a_tally(
    data: Path, family: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects a row with a blank key cell but other content loading with a null key or failing
    the capture: unlike a wholly blank row it survives the reader, so the non-nullable key column
    excludes it, the completion tallies it (``rows_excluded`` 1, outcome populated, no failure) and
    the warning names the ``null`` rule; the other rows load unchanged."""
    alias = ALIAS_OF[family]
    key_column = 3
    lines = body(alias).decode("utf-8").splitlines()
    cells = next(csv.reader([lines[1]]))
    cells[key_column] = ""
    broken = [lines[0], ",".join(cells), *lines[2:]]
    raw = chr(10).join(broken).encode("utf-8")
    capture_id = capture(data, alias, raw=raw)
    with caplog.at_level("WARNING"):
        written = get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    source = rows(alias)
    assert written == len(source) - 1
    completion = read_completion(data, family, capture_id)
    assert completion is not None
    assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 1)
    assert read_failure(data, family, capture_id) is None
    assert any("null" in r.getMessage() for r in caplog.records)
    frame = _silver(data, family)
    assert frame.height == len(source) - 1
    assert frame[KEY_OF[family][-1]].null_count() == 0


def test_local_circuit_keeps_the_publication_date_string_and_the_padded_names(data: Path) -> None:
    """Detects the two publication-date spellings parsed, normalised or one of them failing the
    capture (TODO: CIRCUIT-PUBLICATION-DATE-CAST), and a substation name trimmed (a trailing space
    or tab is part of the identifier, V-19): ``published_date`` is a String equal to the body byte
    for byte (``11/05/2026`` and the literal ``2025/09/12`` both), the names ``CHLEANSAID ``,
    ``Millennium Wind `` and ``Fasque<TAB>`` keep their trailing character, and the key stays
    unique."""
    frame = _load_clean(data, "tnuos_tariffs_onshore_local_circuit")
    assert frame.schema["published_date"] == pl.String
    assert {"11/05/2026", "30/01/2026", "19/12/2025", "2025/09/12"} <= set(
        frame["published_date"].to_list()
    )
    names = frame["substation_name"].to_list()
    assert names == [r["Substation_Name"] for r in rows("lc")]
    assert {"CHLEANSAID ", "Millennium Wind ", "Fasque\t"} <= set(names)
    assert [n for n in names if n != n.strip()] == [
        r["Substation_Name"]
        for r in rows("lc")
        if r["Substation_Name"] != r["Substation_Name"].strip()
    ]
    assert len([n for n in names if n != n.strip()]) == 4


def test_tdr_keeps_the_mojibake_source_header_with_a_clean_unitless_silver_name(
    data: Path,
) -> None:
    """Detects the source header repaired (so a body carrying the vendor's mojibake fails) or the
    silver name carrying a unit or the mojibake: the record's source is the literal mojibake
    header (UTF-8 bytes ``c3 82 c2 a3`` before ``/(Site Day)``), the silver column is ``tdr_tariff``
    with every value as published (the unmetered band's ``2.568147`` kept, no daily or annual
    conversion)."""
    record = _record("tnuos_tariffs_transmission_demand")
    (value,) = [c for c in record.epochs[0].columns if c.name == "tdr_tariff"]
    assert value.source.encode("utf-8") == b"TDR Tariff in \xc3\x82\xc2\xa3/(Site Day)"
    assert "Â" not in value.name and "£" not in value.name
    frame = _load_clean(data, "tnuos_tariffs_transmission_demand")
    assert frame["tdr_tariff"].to_list() == [float(r[value.source]) for r in rows("td")]
    unmetered = frame.filter(pl.col("tdr_band") == "Unmetered")
    assert unmetered["tdr_tariff"].to_list() == [2.568147]
    assert frame["notes"].to_list() == [r["Notes"] for r in rows("td")]


def test_tdr_header_with_the_repaired_pound_sign_fails_loud(data: Path) -> None:
    """Detects the source header silently accepted in a repaired spelling: the record's epoch is
    the vendor's literal mojibake header, so a body whose header says the single pound sign instead
    fails the capture, writes nothing and leaves no completion (a vendor repair is a new epoch,
    not a silent match)."""
    repaired = body("td").replace(bytes.fromhex("c382c2a3"), bytes.fromhex("c2a3"))
    assert repaired != body("td")
    capture_id = capture(data, "td", raw=repaired)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, "tnuos_tariffs_transmission_demand", data).run(DAY, run_id="r")
    assert read_completion(data, "tnuos_tariffs_transmission_demand", capture_id) is None
    assert _no_silver(data, "tnuos_tariffs_transmission_demand")


def test_local_substation_dimensions_are_strings_as_supplied_and_blank_rows_have_no_key(
    data: Path,
) -> None:
    """Detects a TEC band or voltage normalised (``<1320 MW`` rewritten) or the three-string key
    collapsed: voltage, TEC band and connection type equal the body, both connection types appear
    for each stage, and the tariff is a float64 equal to the cell."""
    family = "tnuos_tariffs_local_substation_tariff"
    capture(data, "ls")
    get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    frame = _silver(data, family)
    source = rows("ls")
    assert frame["sum_of_tec_at_connecting_substation"].to_list() == [
        r["Sum of TEC at Connecting Substation"] for r in source
    ]
    assert set(frame["sum_of_tec_at_connecting_substation"].to_list()) == {"<1320 MW"}
    assert set(frame["connection_type"].to_list()) == {"No redundancy", "Redundancy"}
    assert frame.schema["tariff_gbp_per_kw"] == pl.Float64
    assert frame["tariff_gbp_per_kw"].to_list() == [float(r["Tariff"]) for r in source]


@pytest.mark.parametrize(
    "family",
    [
        "tnuos_tariffs_local_substation_tariff",
        "tnuos_tariffs_onshore_local_circuit",
        "aahedc_tariffs",
    ],
)
def test_a_repeated_key_fails_the_capture_as_a_duplicate(data: Path, family: str) -> None:
    """Detects a duplicate key loaded silently (the key is the spec's, with the raw date string for
    the local circuit): the last data row repeated fails the capture with
    ``DuplicateEntityKeyError``, loads nothing and leaves no completion."""
    alias = ALIAS_OF[family]
    lines = body(alias).decode("utf-8").splitlines()
    data_lines = [ln for ln in lines[1:] if ln.strip(",")]
    raw = ("\r\n".join([*lines, data_lines[-1]]) + "\r\n").encode("utf-8")
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epoch_fails_loud(data: Path, family: str) -> None:
    """Detects a silent header drift (a renamed or reordered vendor column loaded under the old
    typing): a body whose first header cell is renamed fails the capture with no completion and
    no silver."""
    alias = ALIAS_OF[family]
    raw = body(alias)
    bom = BOM if raw.startswith(BOM) else b""
    first, newline, rest = raw[len(bom) :].partition(b"\n")
    renamed = bom + first.replace(b",", b"_x,", 1) + newline + rest
    capture_id = capture(data, alias, raw=renamed)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["aa", "ee", "of", "lc"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy (RULINGS 529/597) from the publication date or a catalogue view
    that cannot carry the new columns: ``available_at`` is the CKAN ``last_modified``; an as-of read
    before the vintage serves nothing even though the capture is later and one after serves it, in
    the DuckDB view and in Polars (AAHEDC, a zone family, a blank-row family and the local
    circuit)."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    assert vintage < datetime.fromisoformat(meta.written)
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(days=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, meta.family, before) == []
    assert set(both_as_of(db, data, meta.family, after)) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}


def test_every_family_is_generic_tabular_silver_with_its_own_record() -> None:
    """Detects a family wired to a bespoke transformer instead of the generic engine: every one of
    the nine resolves to a generic transformer reading the family's own record, and none shares
    another family's."""
    registry = load_registry()
    for key in RECORDED:
        package, entry = registry.families[key]
        assert entry.record is not None and entry.transformer is None, key
        assert entry.kind == "tabular", key
        assert not isinstance(package.eligibility, Held), key
    resources = [r for _p, r in registry.resources.values() if r.family in RECORDED]
    assert len(resources) == 9
    assert all(r.disposition == SilverDisposition(kind="SILVER", key=r.family) for r in resources)


@pytest.mark.parametrize("slug", [AAHEDC_PACKAGE[0], TNUOS_PACKAGE[0]])
def test_the_skeleton_pages_render_the_new_records(slug: str) -> None:
    """Detects a record the docs generator cannot render (the held questions, the non-ASCII
    pound-sign headers, the mojibake TDR header)."""
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
    keys = (
        ["aahedc_tariffs"]
        if slug == AAHEDC_PACKAGE[0]
        else [k for k in RECORDED if k != "aahedc_tariffs"]
    )
    for key in keys:
        assert f"`{key}`" in page, key
