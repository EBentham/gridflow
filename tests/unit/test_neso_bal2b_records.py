"""The skip-rate batch's nine frozen records and the two families that get none (v0.22-K-BAL-2b).

``skip_rates_summary`` and ``skip_rates_stage5_psa_technology`` are eligible; the two
behind-constraints families and the five five-minute families are held;
``skip_rates_in_merit_psa_dsf_unit`` and ``skip_rates_stage5_dsf_technology`` get **no record**
(RULINGS 538 precedent): their newest bodies repeat whole rows (7,830 and 192 excess), so no key
can pass and no row ordinal or dedup may be invented. Every test writes recorded fixture captures
(cuts of the 2026-10-08 swept bronze under ``tests/fixtures/neso_data_portal/bal2b/``, provenance
in ``PROVENANCE.md``) into a short data root and runs the transformer the **real package
registry** generates, so a record that does not fit its vendor body fails here, not at
activation. On master none of the nine families has a record, so ``get_transformer`` raises for
each of them.

Record decisions under test (K-BAL-2-FACTS g1-g5 and section 5, K-BAL-2b-SPEC):

- No skip-rate body has a ``(date, period)`` pair or a settlement run: ``stage`` and
  ``exclusion_stage`` are methodology stages (dimensionless int64 identifiers), not ``run_type``;
  ``pair_id = -1`` is a signed identifier, not a sentinel. No record carries ``run_type_column`` or
  an ``sp_pair`` recipe.
- ``settlement_period_start`` (summary, stage 5 PSA technology) is a UTC instant with the **start**
  of the half-hour: ``utc_instant``, ``zone: "UTC"``.
- The five-minute ``date`` and the behind-constraints ``month`` stay raw strings (``date_vendor``,
  ``month_vendor``: ``date`` is a designated DATE name and ``month`` a reserved Hive partition
  name, so a string column may not reuse either); temporal ``none`` (TODO SKIP-DATE-ENDPOINT, and no
  month-boundary recipe for a UTC month).
- Units live in FACTS g4 (the record model has no unit field): volumes MWh and average prices
  GBP/MWh in the header names, the skip-rate columns Percentage, the published value kept and never
  rescaled. Blanks are null (never zero), zeros stay zero, no token becomes null.
- No ADR-034 amendment-1 trim: the seat's full-body check (RULINGS 690) found no padded and no
  non-numeric measure cell; the four BOM bodies are stripped by the reader.
- Open TODOs the record model has no notes field for live in the held questions and in
  ``PROVENANCE.md``: the summary carries no stage-6 rows; the stage-6 acceptance dictionary gives
  ``pair_id`` the unit MWh while describing a signed identifier (kept an int64 identifier, no unit).

``git`` normalises a committed CSV fixture's line endings, so :func:`body` rebuilds the bronze
originals' CRLF convention where they had one; a BOM stays where the original had one."""

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
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "bal2b"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = datetime(2026, 10, 8, tzinfo=UTC).date()
PACKAGE = "skip-rates"
PACKAGE_ID = "c7f21d91-4e93-4162-9caa-ad9165db9c5e"
UNIT = "v0.22-K-BAL-2b"
BOM = b"\xef\xbb\xbf"

DATE_Q = "TODO: SKIP-DATE-ENDPOINT — does the five-minute `date` stamp mark the slice start or end?"
BEHIND_Q = (
    "TODO: CONSTRAINT-RATE-DENOMINATOR — the dictionary says percentage of in-merit volume "
    "skipped, the methodology presentation uses accepted volume; which denominator is the "
    "published rate? Also no month-boundary recipe for a UTC month."
)
ALL_BM_Q = (
    f"{DATE_Q} TODO: ALL-BM-SEPTEMBER-COVERAGE — resource ca3587a2-ae0d-47db-b0a1-4d681af8bced "
    "holds 30 September 2026 only (387,189 rows)."
)
PSA_Q = (
    f"{DATE_Q} TODO: PSA-MARCH/SEPTEMBER-COVERAGE — resource a04c4c6e-0526-42b4-99d0-eb114beba8a2 "
    "stops at exactly 3,200,000 rows on 2026-03-24T15:45:00, and resource "
    "38421f3d-8949-4dcf-81cf-0295a8688086 holds 30 September 2026 only (273,888 rows)."
)
HELD = {
    "skip_rates_behind_constraints_bmu": BEHIND_Q,
    "skip_rates_behind_constraints_cons": BEHIND_Q,
    "skip_rates_in_merit_all_bm": ALL_BM_Q,
    "skip_rates_in_merit_psa": PSA_Q,
    "skip_rates_exclusion_reasons": DATE_Q,
    "skip_rates_stage6_acceptance": DATE_Q,
    "skip_rates_stage6_psa": DATE_Q,
}
ELIGIBLE = ("skip_rates_summary", "skip_rates_stage5_psa_technology")
RECORDED = (*ELIGIBLE, *HELD)
PARTITIONED = (
    "skip_rates_in_merit_all_bm",
    "skip_rates_in_merit_psa",
    "skip_rates_exclusion_reasons",
)
NO_RECORD = {
    "skip_rates_in_merit_psa_dsf_unit": 7830,
    "skip_rates_stage5_dsf_technology": 192,
}
"""The families with no record and the excess whole-row duplicates in their newest body (g3)."""
RESOURCES = {
    "skip_rates_summary": 1,
    "skip_rates_stage5_psa_technology": 1,
    "skip_rates_behind_constraints_bmu": 1,
    "skip_rates_behind_constraints_cons": 1,
    "skip_rates_in_merit_all_bm": 23,
    "skip_rates_in_merit_psa": 23,
    "skip_rates_exclusion_reasons": 23,
    "skip_rates_stage6_acceptance": 1,
    "skip_rates_stage6_psa": 1,
    "skip_rates_in_merit_psa_dsf_unit": 1,
    "skip_rates_stage5_dsf_technology": 1,
}

SUMMARY_HEADER = [
    "settlement_period_start",
    "stage",
    "bm_skip_rate_offers_percent",
    "bm_total_volume_skipped_offers_MWh",
    "post_system_action_skip_rate_offers_percent",
    "post_system_action_total_volume_skipped_offers_MWh",
    "bm_skip_rate_bids_percent",
    "bm_total_volume_skipped_bids_MWh",
    "post_system_action_skip_rate_bids_percent",
    "post_system_action_total_volume_skipped_bids_MWh",
]
TECH_HEADER = [
    "settlement_period_start",
    "bid_offer",
    "fuel",
    "relative_technology_skip_rate",
    "technology_specific_skip_rate",
    "fuel_in_merit_volume_MWh",
    "fuel_skipped_volume_MWh",
]
BMU_HEADER = [
    "month",
    "bm_unit",
    "bid_offer",
    "fuel_type",
    "skipped_volume_mwh",
    "in_merit_volume_mwh",
    "skip_rate",
]
CONS_HEADER = ["month", "constraint", "skipped_volume_mwh", "in_merit_volume_mwh", "skip_rate"]
FIVE_HEADER = [
    "date",
    "bm_unit",
    "fuel",
    "bid_offer",
    "stage",
    "available_volume_MWh",
    "average_price_per_MWh",
    "pair_id",
    "accepted_volume_MWh",
    "in_merit_volume_MWh",
    "skipped_volume_MWh",
]
EXCL_HEADER = [
    "date",
    "bm_unit",
    "fuel",
    "bid_offer",
    "excluded_from_accepted_or_feasible_merit_stack",
    "exclusion_stage",
    "pair_id",
    "average_price_per_MWh",
    "excluded_volume_MWh",
    "exclusion_reason",
]
S6A_HEADER = [
    "date",
    "stage",
    "bm_unit",
    "fuel",
    "bid_offer",
    "pair_id",
    "average_price_per_MWh",
    "accepted_volume_MWh",
]
HEADER_OF = {
    "skip_rates_summary": SUMMARY_HEADER,
    "skip_rates_stage5_psa_technology": TECH_HEADER,
    "skip_rates_behind_constraints_bmu": BMU_HEADER,
    "skip_rates_behind_constraints_cons": CONS_HEADER,
    "skip_rates_in_merit_all_bm": FIVE_HEADER,
    "skip_rates_in_merit_psa": FIVE_HEADER,
    "skip_rates_exclusion_reasons": EXCL_HEADER,
    "skip_rates_stage6_acceptance": S6A_HEADER,
    "skip_rates_stage6_psa": FIVE_HEADER,
}
KEY_OF = {
    "skip_rates_summary": ["settlement_period_start", "stage"],
    "skip_rates_stage5_psa_technology": ["settlement_period_start", "bid_offer", "fuel"],
    "skip_rates_behind_constraints_bmu": ["month_vendor", "bm_unit", "bid_offer"],
    "skip_rates_behind_constraints_cons": ["month_vendor", "constraint"],
    "skip_rates_in_merit_all_bm": [
        "resource_id",
        "date_vendor",
        "bm_unit",
        "bid_offer",
        "stage",
        "pair_id",
    ],
    "skip_rates_in_merit_psa": [
        "resource_id",
        "date_vendor",
        "bm_unit",
        "bid_offer",
        "stage",
        "pair_id",
    ],
    "skip_rates_exclusion_reasons": [
        "resource_id",
        "date_vendor",
        "bm_unit",
        "bid_offer",
        "excluded_from_accepted_or_feasible_merit_stack",
        "exclusion_stage",
        "pair_id",
        "exclusion_reason",
    ],
    "skip_rates_stage6_acceptance": ["date_vendor", "bm_unit", "bid_offer", "stage", "pair_id"],
    "skip_rates_stage6_psa": ["date_vendor", "bm_unit", "bid_offer", "stage", "pair_id"],
}
INT_COLUMNS = {"stage", "exclusion_stage", "pair_id"}
STRING_COLUMNS = {
    "bid_offer",
    "bm_unit",
    "constraint",
    "date_vendor",
    "exclusion_reason",
    "excluded_from_accepted_or_feasible_merit_stack",
    "fuel",
    "fuel_type",
    "month_vendor",
}
NULLABLE_STRINGS = {"fuel", "fuel_type"}


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
    crlf: bool = False


CAPTURES: dict[str, Capture] = {
    "su": Capture(
        "su.csv",
        "skip_rates_summary",
        "e4129210-ea22-4a36-aa2f-ff7bd6147675",
        "Skip Rate - Summary",
        "1_skip_rates_summary.csv",
        "2026-10-08T08:51:21.367851",
        "2026-10-08T11:24:18.216520+00:00",
    ),
    "tp": Capture(
        "tp.csv",
        "skip_rates_stage5_psa_technology",
        "007c5786-bd34-4a09-8bdf-be5d63eda16a",
        "Stage 5 PSA Skip Rate by Technology",
        "5_fuel_type_skip_rate.csv",
        "2026-10-08T08:58:11.847497",
        "2026-10-08T11:23:52.769492+00:00",
    ),
    "bm": Capture(
        "bm.csv",
        "skip_rates_behind_constraints_bmu",
        "26f3a3de-e5fd-43e5-9d8a-d89a9c786812",
        "Skips Behind Constraints - In Merit BMU-Level",
        "skips_behind_constraints_in_merit_bmu_level.csv",
        "2026-09-28T12:04:03.117796",
        "2026-10-08T11:17:44.829058+00:00",
        crlf=True,
    ),
    "co": Capture(
        "co.csv",
        "skip_rates_behind_constraints_cons",
        "a88adc4c-d3a6-440d-946b-4e5a3e7ea1c5",
        "Skips Behind Constraints - In Merit Constraint-Level",
        "skips_behind_constraints_in_merit_constraint_level.csv",
        "2026-09-28T12:08:43.108251",
        "2026-10-08T11:17:48.366851+00:00",
        crlf=True,
    ),
    "s6a": Capture(
        "s6a.csv",
        "skip_rates_stage6_acceptance",
        "235e5cf1-745a-46e3-9e64-f7c8532abdc1",
        "Skip Rate - Stage 6 Acceptance",
        "9_s6_acceptances.csv",
        "2026-02-04T08:47:52.060966",
        "2026-10-08T11:23:59.962482+00:00",
        crlf=True,
    ),
    "s6p": Capture(
        "s6p.csv",
        "skip_rates_stage6_psa",
        "32072115-34bd-4838-92ab-96803e1b692d",
        "Stage 6 PSA Skip Rate",
        "8_in_merit_s6_data_post_system_actions_skips.csv",
        "2026-09-21T13:39:18.302571",
        "2026-10-08T11:24:14.609576+00:00",
        crlf=True,
    ),
    "abz": Capture(
        "abz.csv",
        "skip_rates_in_merit_all_bm",
        "ca3587a2-ae0d-47db-b0a1-4d681af8bced",
        "Skip Rate - In Merit All Balancing Mechanism - September 2026",
        "2_in_merit_data_bm_skips_09_2026.csv",
        "2026-10-01T08:53:23.891952",
        "2026-10-08T11:21:12.571276+00:00",
    ),
    "abn": Capture(
        "abn.csv",
        "skip_rates_in_merit_all_bm",
        "fb15911f-ddda-4b87-8334-471430c0e70f",
        "Skip Rate - In Merit All Balancing Mechanism - October 2026",
        "2_in_merit_data_bm_skips_10_2026.csv",
        "2026-10-08T08:53:59.338371",
        "2026-10-08T11:21:15.631624+00:00",
    ),
    "pa": Capture(
        "pa.csv",
        "skip_rates_in_merit_psa",
        "3a4863d1-220e-43e5-9049-94db1045d92c",
        "Skip Rate - In Merit Post System Action - January 2025",
        "3_in_merit_data_post_system_actions_skips_01_2025.csv",
        "2025-11-18T18:16:21.298512",
        "2026-10-08T11:21:20.318912+00:00",
    ),
    "ex": Capture(
        "ex.csv",
        "skip_rates_exclusion_reasons",
        "f8110eb8-f4ad-4288-87d5-37ab434b55b5",
        "Skip Rate - Exclusion Reasons - October 2026",
        "4_exclusions_10_2026.csv",
        "2026-10-08T08:57:27.401427",
        "2026-10-08T11:19:00.524166+00:00",
    ),
}
FAMILY_ALIASES: dict[str, tuple[str, ...]] = {}
for _alias, _meta in CAPTURES.items():
    FAMILY_ALIASES[_meta.family] = (*FAMILY_ALIASES.get(_meta.family, ()), _alias)
FIVE_MINUTE = (
    "skip_rates_in_merit_all_bm",
    "skip_rates_in_merit_psa",
    "skip_rates_exclusion_reasons",
    "skip_rates_stage6_acceptance",
    "skip_rates_stage6_psa",
)


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
        prefix="ba2b", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's line endings (a BOM stays)."""
    raw = (FIXTURES / CAPTURES[alias].fixture).read_bytes()
    bom = BOM if raw.startswith(BOM) else b""
    text = raw[len(bom) :].replace(b"\r\n", b"\n")
    return bom + (text.replace(b"\n", b"\r\n") if CAPTURES[alias].crlf else text)


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed (cells verbatim)."""
    data_bytes = raw if raw is not None else body(alias)
    reader = csv.DictReader(io.StringIO(data_bytes.decode("utf-8-sig"), newline=""))
    return [r for r in reader if any(v for v in r.values())]


def header(alias: str) -> list[str]:
    """The fixture's header."""
    first = body(alias).decode("utf-8-sig").splitlines()[0]
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


def _package_doc() -> dict[str, Any]:
    document: dict[str, Any] = json.loads(
        (REGISTRY_DIR / f"{PACKAGE}.json").read_text(encoding="utf-8")
    )
    return document


def _columns(key: str) -> list[str]:
    return [
        c
        for c in (name for name, _t in generic.output_columns(_record(key)))
        if c not in ("year", "month")
    ]


def _load(data: Path, family: str) -> tuple[dict[str, str], pl.DataFrame]:
    """Capture every fixture of ``family``, run it once; the capture ids and the silver."""
    ids = {alias: capture(data, alias) for alias in FAMILY_ALIASES[family]}
    get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    return ids, _silver(data, family)


def _assert_clean_load(data: Path, family: str) -> pl.DataFrame:
    """Load ``family``: every row written, none excluded, the generic output columns, no BOM."""
    ids, frame = _load(data, family)
    for alias, capture_id in ids.items():
        completion = read_completion(data, family, capture_id)
        assert completion is not None, alias
        assert (completion["outcome"], completion["rows_excluded"]) == ("populated", 0), alias
        assert read_failure(data, family, capture_id) is None, alias
    assert frame.height == sum(len(rows(a)) for a in FAMILY_ALIASES[family])
    assert [c for c in frame.columns if c not in ("year", "month")] == _columns(family)
    assert not [c for c in frame.columns if "﻿" in c]
    return frame


def _of(frame: pl.DataFrame, alias: str) -> pl.DataFrame:
    """The rows one capture wrote, in body order (the resource column when partitioned)."""
    if "resource_id" in frame.columns:
        return frame.filter(pl.col("resource_id") == CAPTURES[alias].resource_id)
    return frame


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: each exact vendor header, the
    BOM on exactly the four bodies that carry one (behind-constraints x2, stage-6 x2), the CRLF
    originals, a naive and a ``Z`` slice of one five-minute family, a blank fuel row in every
    family with a fuel column, a ``pair_id = -1`` row and a stage-0 row in the five-minute
    families, the summary's wholly blank measure row and a zero-rate behind-constraints row."""
    for alias, meta in CAPTURES.items():
        assert header(alias) == HEADER_OF[meta.family], alias
        raw = (FIXTURES / meta.fixture).read_bytes()
        assert (BOM in raw[:3]) == (alias in ("bm", "co", "s6a", "s6p")), alias
        assert (b"\r\n" in body(alias)) == meta.crlf, alias
    assert {r["date"].endswith("Z") for r in rows("abz")} == {True}
    assert {r["date"].endswith("Z") for r in rows("abn")} == {False}
    for alias in ("tp", "s6a", "s6p", "abz", "abn", "pa", "ex"):
        col = "fuel"
        assert "" in {r[col] for r in rows(alias)}, alias
    assert "" in {r["fuel_type"] for r in rows("bm")}
    for alias in ("s6a", "s6p", "abz", "abn", "pa", "ex"):
        assert "-1" in {r["pair_id"] for r in rows(alias)}, alias
    for alias in ("abz", "abn", "pa"):
        assert "0" in {r["stage"] for r in rows(alias)}, alias
    assert "0" in {r["stage"] for r in rows("su")}
    blank = [r for r in rows("su") if r["bm_skip_rate_offers_percent"] == ""]
    assert len(blank) == 1
    assert {k for k, v in blank[0].items() if v == ""} == set(SUMMARY_HEADER[2:])
    assert "0" in {r["skip_rate"] for r in rows("bm")}
    assert "0" in {r["skip_rate"] for r in rows("co")}


# --------------------------------------------------------------------------- #
# Record shapes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", RECORDED)
def test_every_record_is_one_epoch_csv_utf8_with_ckan_last_modified_and_no_issue(key: str) -> None:
    """Detects a record that invents an issue time, a run or a settlement pair, or an epoch other
    than the one vendor header: every record is version 1, utf-8 CSV, ``ckan_last_modified``
    vintage (every capture is an upload), whole-capture selection, per-resource selection exactly
    for the three 23-resource families, one epoch whose header is the body's, no issue recipe,
    ``run_type_column`` unset and no ``sp_pair`` recipe (the stage is a methodology stage, not a
    settlement run), no sibling and the spec's entity key."""
    record = _record(key)
    assert (record.version, record.encoding, record.reader) == ("1", "utf-8", "csv")
    assert (record.vintage, record.vintage_evidence) == ("ckan_last_modified", None)
    assert record.latest == "whole_capture"
    assert record.latest_partition == ("resource_id" if key in PARTITIONED else None)
    assert ("resource_id" in record.entity_key) == (key in PARTITIONED)
    assert list(record.entity_key) == KEY_OF[key]
    assert len(record.epochs) == 1
    assert list(record.epochs[0].header) == HEADER_OF[key]
    assert record.epochs[0].issue.kind == "none" and has_issue_time(record) is False
    assert "issue_time" not in record.entity_key and "issue_time" not in _columns(key)
    assert record.run_type_column is None and "run_type" not in _columns(key)
    assert record.temporal.kind != "sp_pair"
    assert record.siblings == () and record.xlsx is None and record.edition_by_filename is None


@pytest.mark.parametrize("key", RECORDED)
def test_columns_are_typed_from_the_vendor_names_without_trim_tokens_or_bounds(key: str) -> None:
    """Detects a rescaled, trimmed, token-mapped or bounded column, a stage or pair identifier
    given a unit, range or sentinel, a raw ``date``/``month`` label typed or renamed to a reserved
    name, or a lost nullability: identifiers are non-nullable int64 with no min, max or null
    token, the raw labels non-nullable strings with no format, only ``fuel`` / ``fuel_type`` are
    nullable strings, every measure is a nullable float64, no column declares ``trim`` (RULINGS 690:
    no padded and no non-numeric cell), and every silver name is the lower-cased vendor header (the
    label columns carry the ``_vendor`` suffix)."""
    record = _record(key)
    for column in record.epochs[0].columns:
        expected = {"date": "date_vendor", "month": "month_vendor"}.get(
            column.source, column.source.lower()
        )
        assert column.name == expected, column.source
        assert column.trim is None and column.null_tokens == ()
        assert (column.min, column.max) == (None, None)
        assert column.formats_by_filename is None
        if column.name in INT_COLUMNS:
            assert (column.dtype, column.nullable) == ("int64", False), column.name
        elif column.name in STRING_COLUMNS:
            assert column.dtype == "string" and column.format is None, column.name
            assert column.nullable == (column.name in NULLABLE_STRINGS), column.name
        elif column.name == "settlement_period_start":
            assert (column.dtype, column.nullable) == ("datetime", False)
            assert (column.format, column.zone) == ("%Y-%m-%dT%H:%M:%S", "UTC")
        else:
            assert (column.dtype, column.nullable) == ("float64", True), column.name
    names = [c.name for c in record.epochs[0].columns]
    assert "date" not in names and "month" not in names


def test_the_unit_is_in_the_header_name_and_never_rescaled() -> None:
    """Detects a unit dropped from a column name or a rate renamed: the volumes keep ``_mwh``,
    the average prices ``average_price_per_mwh`` (GBP/MWh), the summary rates ``_percent``, and the
    unit-less vendor rate names (``skip_rate``, ``relative_technology_skip_rate``,
    ``technology_specific_skip_rate``) are kept as published, Percentage per FACTS g4."""
    summary = [c.name for c in _record("skip_rates_summary").epochs[0].columns]
    assert [n for n in summary if n.endswith("_percent")] == [
        "bm_skip_rate_offers_percent",
        "post_system_action_skip_rate_offers_percent",
        "bm_skip_rate_bids_percent",
        "post_system_action_skip_rate_bids_percent",
    ]
    assert len([n for n in summary if n.endswith("_mwh")]) == 4
    tech = [c.name for c in _record("skip_rates_stage5_psa_technology").epochs[0].columns]
    assert tech[3:] == [
        "relative_technology_skip_rate",
        "technology_specific_skip_rate",
        "fuel_in_merit_volume_mwh",
        "fuel_skipped_volume_mwh",
    ]
    bmu = [c.name for c in _record("skip_rates_behind_constraints_bmu").epochs[0].columns]
    assert bmu[-3:] == ["skipped_volume_mwh", "in_merit_volume_mwh", "skip_rate"]
    for key in FIVE_MINUTE:
        names = {c.name for c in _record(key).epochs[0].columns}
        assert "average_price_per_mwh" in names
        assert {n for n in names if n.endswith("_mwh")} <= {
            "available_volume_mwh",
            "accepted_volume_mwh",
            "in_merit_volume_mwh",
            "skipped_volume_mwh",
            "excluded_volume_mwh",
            "average_price_per_mwh",
        }


def test_the_two_utc_instant_records_attach_utc_to_the_period_start() -> None:
    """Detects a London clock or a date-only anchor on the half-hour start (FACTS g1: the
    dictionary says UTC and the support guide says period start): both records use
    ``utc_instant`` over the non-nullable ``settlement_period_start``, formatted
    ``%Y-%m-%dT%H:%M:%S`` with zone UTC; every other record is temporal ``none``."""
    for key in ELIGIBLE:
        recipe = _record(key).temporal
        assert (recipe.kind, recipe.column) == ("utc_instant", "settlement_period_start"), key
    for key in HELD:
        assert _record(key).temporal.kind == "none", key


@pytest.mark.parametrize("key", list(HELD))
def test_held_records_carry_the_ruled_question_and_the_package_stays_eligible(key: str) -> None:
    """Detects a hold lost, reworded or moved to the package, or the five-minute families given
    a different question: each held record carries the spec's verbatim question and this unit,
    is effectively held, and the package itself stays eligible."""
    package, family = registry_module.load_registry().families[key]
    record = _record(key)
    assert isinstance(record.eligibility, Held)
    assert (record.eligibility.unit, record.eligibility.question) == (UNIT, HELD[key])
    assert effective_eligibility(package, family) == record.eligibility
    assert package.eligibility == Eligible(status="eligible")


@pytest.mark.parametrize("key", ELIGIBLE)
def test_the_two_eligible_records_carry_no_hold(key: str) -> None:
    """Detects a hold wrongly put on an eligible family: neither carries a record-level
    eligibility, so each is effectively eligible."""
    package, family = registry_module.load_registry().families[key]
    assert _record(key).eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")


def test_the_package_file_carries_exactly_the_nine_records_and_their_resources() -> None:
    """Detects a record on the wrong family, a resource disposition drifting or a resource count
    moving: skip-rates.json holds a record on exactly the nine families, every CSV resource is
    SILVER of its family, and the per-family resource counts are the captured ones (77 in all,
    the three monthly families 23 each)."""
    document = _package_doc()
    assert {f["key"] for f in document["families"] if "record" in f} == set(RECORDED)
    assert {f["key"] for f in document["families"]} == set(RESOURCES)
    for resource in document["resources"]:
        assert resource["disposition"] == {"kind": "SILVER", "key": resource["family"]}
    registry = load_registry()
    counts = {
        key: len([r for _p, r in registry.resources.values() if r.family == key])
        for key in RESOURCES
    }
    assert counts == RESOURCES and sum(counts.values()) == 77
    for alias, meta in CAPTURES.items():
        assert registry.resources[meta.resource_id][1].family == meta.family, alias


# --------------------------------------------------------------------------- #
# The two families that get no record
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("key", NO_RECORD)
def test_the_two_dsf_families_have_no_record(key: str) -> None:
    """Detects a DSF family gaining a record (RULINGS 538 precedent). The newest body repeats
    whole rows (7,830 excess for ``skip_rates_in_merit_psa_dsf_unit``, 192 for
    ``skip_rates_stage5_dsf_technology``, FACTS g3): no key passes, and a row ordinal or a dedup
    would invent data, so the family stays ingest-only (no transformer, no silver output) with
    the vendor question "are the repeated rows intended?". No ADR-040 adjudication is written:
    no record means no capture is attempted, so there is no gap."""
    registry = load_registry()
    package, entry = registry.families[key]
    assert entry.record is None and entry.kind == "tabular" and entry.transformer is None
    assert not isinstance(package.eligibility, Held)
    resources = [r for _p, r in registry.resources.values() if r.family == key]
    assert len(resources) == 1
    assert resources[0].disposition == SilverDisposition(kind="SILVER", key=key)
    with pytest.raises(ValueError, match="No transformer registered"):
        get_transformer(SOURCE, key, Path("."))
    ledger = registry_module.load_reconcile_adjudications()
    assert key not in {e.family for e in ledger}


def test_eligible_held_and_no_record_are_reported_exactly_as_ruled() -> None:
    """Detects a family reported in the wrong class: the eligibility report lists the two eligible
    records as plain ``silver`` (no hold text), the seven held ones as ``silver`` with
    ``held: TODO`` and this unit, the five-minute 23-resource families ``whole_capture per
    resource_id``, and the two DSF families as ``ingest-only (no silver output)``."""
    report = eligibility_module.render(load_registry())
    for key in ELIGIBLE:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver | eligible |" in line, key
        assert "utc_instant" in line
    for key in HELD:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "| silver | held: TODO:" in line and f"(unit {UNIT})" in line, key
        assert ("whole_capture per resource_id" in line) == (key in PARTITIONED), key
    for key in NO_RECORD:
        (line,) = [ln for ln in report.splitlines() if f"| `{key}` |" in ln]
        assert "ingest-only (no silver output)" in line, key


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


def test_summary_types_the_utc_period_start_and_keeps_blank_measures_null(data: Path) -> None:
    """Detects a local-time or naive period start, a blank measure read as zero, a zero read as
    null or a stage dropped: every summary row loads with zero exclusions,
    ``settlement_period_start`` is tz-aware UTC and equals the naive body literal read as UTC
    (and so does ``timestamp_utc``), stage 0 is kept as an int64 identifier, the wholly
    blank-measure row has eight null measures (never zero), the recorded zero rate stays 0.0 and
    every other cell equals the body's."""
    frame = _assert_clean_load(data, "skip_rates_summary")
    source = rows("su")
    assert frame.schema["settlement_period_start"] == pl.Datetime("us", "UTC")
    assert frame.schema["stage"] == pl.Int64
    expected = [
        datetime.fromisoformat(r["settlement_period_start"]).replace(tzinfo=UTC) for r in source
    ]
    assert frame["settlement_period_start"].to_list() == expected
    assert frame["timestamp_utc"].to_list() == expected
    assert frame["stage"].to_list() == [int(r["stage"]) for r in source]
    assert 0 in frame["stage"].to_list()
    for column in SUMMARY_HEADER[2:]:
        silver = frame[column.lower()].to_list()
        assert silver == [float(r[column]) if r[column] else None for r in source], column
    blank = frame.filter(pl.col("bm_skip_rate_offers_percent").is_null())
    assert blank.height == 1
    assert all(blank[c.lower()][0] is None for c in SUMMARY_HEADER[2:])
    assert 0.0 in frame["bm_skip_rate_bids_percent"].to_list()


def test_stage5_psa_technology_keeps_the_blank_fuel_null_inside_the_key(data: Path) -> None:
    """Detects a made-up technology code for a blank fuel, a blank fuel dropped from the load, or
    a nullable key column the uniqueness check cannot see: the two blank-fuel rows load with a
    null ``fuel`` (never ``""`` or a code), the key (period, bid_offer, fuel) is unique across the
    slice, the period start is tz-aware UTC and every float equals its cell."""
    frame = _assert_clean_load(data, "skip_rates_stage5_psa_technology")
    source = rows("tp")
    assert frame["fuel"].to_list() == [r["fuel"] or None for r in source]
    assert frame["fuel"].null_count() == 2
    assert (
        not frame.select(_record("skip_rates_stage5_psa_technology").entity_key)
        .is_duplicated()
        .any()
    )
    assert frame["settlement_period_start"].to_list() == [
        datetime.fromisoformat(r["settlement_period_start"]).replace(tzinfo=UTC) for r in source
    ]
    for column in TECH_HEADER[3:]:
        assert frame[column.lower()].to_list() == [float(r[column]) for r in source], column


def test_a_repeated_blank_fuel_row_is_a_duplicate_key_not_a_distinct_group(data: Path) -> None:
    """Detects null fuel values being treated as distinct keys (so a repeated blank-fuel group
    would load silently): the same (period, bid_offer, blank fuel) twice fails the capture with
    ``DuplicateEntityKeyError``, loads nothing and leaves no completion."""
    lines = body("tp").decode("utf-8").splitlines()
    blank = next(ln for ln in lines[1:] if ",," in ln)
    raw = ("\n".join([*lines, blank]) + "\n").encode("utf-8")
    capture_id = capture(data, "tp", raw=raw)
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, "skip_rates_stage5_psa_technology", data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, "DuplicateEntityKeyError")
    ]
    assert read_completion(data, "skip_rates_stage5_psa_technology", capture_id) is None
    assert _no_silver(data, "skip_rates_stage5_psa_technology")


@pytest.mark.parametrize(
    "family", ["skip_rates_behind_constraints_bmu", "skip_rates_behind_constraints_cons"]
)
def test_behind_constraints_keep_the_utc_month_label_raw_and_the_bom_out_of_the_header(
    data: Path, family: str
) -> None:
    """Detects the UTC month anchored as a London midnight (the ``month`` recipe), a BOM reaching
    the first silver column name, a blank fuel type read as a value, a zero rate read as null or
    a rate rescaled: ``month_vendor`` is the raw ``YYYY-MM`` string of the body, there is no
    ``month`` data column, the temporal stamp is the capture's own write time, and every volume
    and rate equals its cell (the zero rate stays 0.0)."""
    (alias,) = FAMILY_ALIASES[family]
    frame = _assert_clean_load(data, family)
    source = rows(alias)
    assert frame["month_vendor"].to_list() == [r["month"] for r in source]
    assert frame.schema["month_vendor"] == pl.String
    assert "month" not in _record(family).entity_key
    for column in ("skipped_volume_mwh", "in_merit_volume_mwh", "skip_rate"):
        assert frame[column].to_list() == [float(r[column]) for r in source], column
    assert 0.0 in frame["skip_rate"].to_list()
    if family.endswith("bmu"):
        assert frame["fuel_type"].to_list() == [r["fuel_type"] or None for r in source]
        assert frame["fuel_type"].null_count() == 1
        assert frame["bm_unit"].to_list() == [r["bm_unit"] for r in source]
    else:
        assert frame["constraint"].to_list() == [r["constraint"] for r in source]
    meta = CAPTURES[alias]
    assert set(frame["timestamp_utc"].to_list()) == {
        datetime.fromisoformat(meta.written).astimezone(UTC)
    }


@pytest.mark.parametrize("family", FIVE_MINUTE)
def test_five_minute_families_keep_the_raw_date_string_and_the_signed_identifiers(
    data: Path, family: str
) -> None:
    """Detects the raw ``date`` normalised, parsed or UTC-anchored (the endpoint is an open
    question), a negative pair id dropped, abs()ed or read as a sentinel, stage 0 dropped, a blank
    fuel made a code, a zero volume read as null, or a BOM reaching a column name: ``date_vendor``
    equals the body string byte for byte (naive and ``Z`` spellings both), ``pair_id`` and the
    stages are int64 with the body's negative values, blank fuels are null, every measure equals
    its cell and zeros stay zero."""
    frame = _assert_clean_load(data, family)
    for alias in FAMILY_ALIASES[family]:
        part = _of(frame, alias)
        source = rows(alias)
        assert part.height == len(source), alias
        assert part["date_vendor"].to_list() == [r["date"] for r in source], alias
        assert part.schema["date_vendor"] == pl.String
        assert part["bm_unit"].to_list() == [r["bm_unit"] for r in source]
        assert part["bid_offer"].to_list() == [r["bid_offer"] for r in source]
        assert part["fuel"].to_list() == [r["fuel"] or None for r in source]
        assert part.schema["pair_id"] == pl.Int64
        assert part["pair_id"].to_list() == [int(r["pair_id"]) for r in source]
        stage_name = "exclusion_stage" if family.endswith("exclusion_reasons") else "stage"
        assert part.schema[stage_name] == pl.Int64
        assert part[stage_name].to_list() == [int(r[stage_name]) for r in source]
        for column in HEADER_OF[family]:
            if column.endswith("MWh"):
                assert part[column.lower()].to_list() == [float(r[column]) for r in source], column
    assert (frame["pair_id"] == -1).any()
    assert frame["fuel"].null_count() >= 1


def test_the_naive_and_the_z_spelling_of_one_family_both_load_untouched(data: Path) -> None:
    """Detects a per-spelling parse or a ``Z`` stripped: the all-BM family's September 2026 body
    (``Z``) and October 2026 body (naive) both load into one table, each resource keeping its own
    literal, and the two resources are told apart by the stamped ``resource_id`` (ADR-039)."""
    frame = _assert_clean_load(data, "skip_rates_in_merit_all_bm")
    assert set(_of(frame, "abz")["date_vendor"].to_list()) == {"2026-09-30T00:00:00Z"}
    assert set(_of(frame, "abn")["date_vendor"].to_list()) == {"2026-10-01T00:00:00"}
    assert set(frame["resource_id"].to_list()) == {
        CAPTURES["abz"].resource_id,
        CAPTURES["abn"].resource_id,
    }


def test_exclusion_reasons_keep_every_label_and_the_zero_price(data: Path) -> None:
    """Detects a stack, reason or stage label altered or a zero average price read as null: the
    key columns (stack, stage, pair, reason) equal the body's, the zero price stays 0.0 and the
    excluded volumes equal their cells."""
    frame = _assert_clean_load(data, "skip_rates_exclusion_reasons")
    source = rows("ex")
    assert frame["excluded_from_accepted_or_feasible_merit_stack"].to_list() == [
        r["excluded_from_accepted_or_feasible_merit_stack"] for r in source
    ]
    assert frame["exclusion_reason"].to_list() == [r["exclusion_reason"] for r in source]
    assert frame["average_price_per_mwh"].to_list() == [
        float(r["average_price_per_MWh"]) for r in source
    ]
    assert 0.0 in frame["average_price_per_mwh"].to_list()
    assert {1, 2, 5} <= set(frame["exclusion_stage"].to_list())


def test_stage6_pair_id_stays_an_identifier_not_an_energy_quantity(data: Path) -> None:
    """Detects ``pair_id`` of the stage-6 families given a unit or float type (the stage-6
    acceptance dictionary labels it MWh while describing a signed identifier, conflict noted in
    PROVENANCE.md): int64 with no unit in the name, the signed values kept."""
    for family in ("skip_rates_stage6_acceptance", "skip_rates_stage6_psa"):
        spec = {c.name: c for c in _record(family).epochs[0].columns}["pair_id"]
        assert (spec.dtype, spec.nullable) == ("int64", False), family
        assert "mwh" not in spec.name
    frame = _assert_clean_load(data, "skip_rates_stage6_acceptance")
    assert -1 in frame["pair_id"].to_list() and set(frame["stage"].to_list()) == {6}


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epoch_fails_loud(data: Path, family: str) -> None:
    """Detects a silent header drift (a renamed or reordered vendor column loaded under the old
    typing): a body whose first header cell is renamed fails the capture with no completion and
    no silver."""
    alias = FAMILY_ALIASES[family][0]
    raw = body(alias)
    bom = BOM if raw.startswith(BOM) else b""
    first, newline, rest = raw[len(bom) :].partition(b"\n")
    renamed = bom + first.replace(b",", b"_x,", 1) + newline + rest
    capture_id = capture(data, alias, raw=renamed)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


@pytest.mark.parametrize("alias", ["su", "bm", "abz", "s6p"])
def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(
    data: Path, alias: str
) -> None:
    """Detects an issue-time proxy (RULINGS 529/597) or a catalogue view that cannot carry the new
    columns: ``available_at`` is the CKAN ``last_modified``; an as-of read before the vintage
    serves nothing even though the capture is later and one after serves it, in the DuckDB view
    and in Polars (a summary, a behind-constraints, an all-BM and a stage-6 family)."""
    meta = CAPTURES[alias]
    capture_id = capture(data, alias)
    get_transformer(SOURCE, meta.family, data).run(DAY, run_id="r")
    frame = _silver(data, meta.family)
    vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
    assert set(frame["available_at"].to_list()) == {vintage}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    before = vintage - timedelta(days=1)
    after = vintage + timedelta(days=1)
    assert both_as_of(db, data, meta.family, before) == []
    assert set(both_as_of(db, data, meta.family, after)) == {capture_id}
    assert set(both_as_of(db, data, meta.family, None)) == {capture_id}


def test_the_skeleton_pages_render_the_new_records() -> None:
    """Detects a record the docs generator cannot render (the held questions, the nullable key
    column, the resource-partitioned keys)."""
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
    for key in RESOURCES:
        assert f"`{key}`" in page, key
