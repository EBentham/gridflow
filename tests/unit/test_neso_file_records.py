"""The GSP / Gnode / Direct Connect / Region lookup frozen record (v0.22-K-FILE).

``gis_grid_supply_points`` (package ``gis-boundaries-for-gb-grid-supply-points``) gets record
version 1: one standalone CSV resource, a reference table keyed on ``ggd_id`` with family-scope
whole-capture selection (no resource partition, no edition map, temporal ``none``), inheriting
the package's ``eligible`` status. The embedded child ``view_gsp_gnode_all_20181031.csv`` keeps
its ``HOLD``, the GSP changelog stays ``DOC``, every GIS resource stays ``GIS`` and the DNO
licence-area package stays held.

Every transform test writes a recorded fixture capture (a cut of the 2026-10-08 swept bronze,
``tests/fixtures/neso_data_portal/file/``, provenance in ``PROVENANCE.md``) into a short data
root and runs the transformer the **real package registry** generates, so a record that does not
fit its vendor body fails here, not at activation. On master the family has no record, so
``get_transformer`` raises.
"""

from __future__ import annotations

import csv
import io
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import (
    DocDisposition,
    GisDisposition,
    HoldDisposition,
    SilverDisposition,
    load_registry,
)
from gridflow.silver.neso_data_portal.casting import DuplicateEntityKeyError
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
)
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "file"
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 11, 4, 7, 596378, tzinfo=UTC)
PUBLISHED = datetime(2020, 5, 5, 7, 54, 7, 800829, tzinfo=UTC)

KEY = "gis_grid_supply_points"
PACKAGE = "gis-boundaries-for-gb-grid-supply-points"
PACKAGE_ID = "2810092e-d4b2-472f-b955-d8bea01f9ec0"
RESOURCE_ID = "bbe2cc72-a6c6-46e6-8f4e-48b879467368"
RESOURCE_NAME = "GSP - Gnode - Direct Connect - Region Lookup 20181031"
FILENAME = "gsp_gnode_directconnect_region_lookup.csv"
CKAN_LAST_MODIFIED = "2020-05-05T07:54:07.800829"
EMBEDDED_CHILD = "view_gsp_gnode_all_20181031.csv"
EMBEDDED_HOLD_REASON = (
    "proposed family gis_gsp_lookup_archive: COVERED nomination by "
    "bbe2cc72-a6c6-46e6-8f4e-48b879467368 — byte-identical but covering published 129.559 s later"
)
CHANGELOG_RESOURCE_ID = "857f8af8-e41b-4f1b-9962-2b441cd31eea"

HEADER = [
    "ng_id",
    "ggd_id",
    "gnode_id",
    "gnode_name",
    "gnode_lat",
    "gnode_lon",
    "gsp_id",
    "gsp_name",
    "gsp_lat",
    "gsp_lon",
    "dc_id",
    "dc_name",
    "dc_lat",
    "dc_lon",
    "region_id",
    "region_name",
    "has_pv",
    "pes_id",
    "pes_name",
]
INT_COLUMNS = ("ng_id", "ggd_id", "gnode_id", "gsp_id", "dc_id", "region_id", "has_pv", "pes_id")
FLOAT_COLUMNS = ("gnode_lat", "gnode_lon", "gsp_lat", "gsp_lon", "dc_lat", "dc_lon")
STRING_COLUMNS = ("gnode_name", "gsp_name", "dc_name", "region_name", "pes_name")
NON_NULLABLE = ("ng_id", "ggd_id", "has_pv")
# column -> number of exact empty cells in the 13-record fixture
FIXTURE_EMPTY = {
    "gnode_id": 1,
    "gsp_id": 4,
    "dc_id": 10,
    "region_id": 4,
    "pes_id": 4,
}


def _short_base() -> str:
    """The drive root on Windows (ADR-036: engine names pass MAX_PATH there); tmp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root; gold views are out of scope for this catalogue."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="f1", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def body() -> bytes:
    """The fixture re-terminated with the bronze original's CRLF (git may normalise it)."""
    raw = (FIXTURES / "gsp_lookup.csv").read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n")


def table(raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's records as the production reader sees them (BOM removed)."""
    text = (raw if raw is not None else body()).decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text, newline="")))


def capture(data: Path, raw: bytes | None = None, *, filename: str = FILENAME) -> str:
    """Write the recorded capture (real sidecar identity); return its capture id."""
    path, _sidecar = write_capture(
        data,
        KEY,
        body=raw if raw is not None else body(),
        written_at=WRITTEN,
        partition=DAY,
        package_slug=PACKAGE,
        package_id=PACKAGE_ID,
        resource_id=RESOURCE_ID,
        resource_name=RESOURCE_NAME,
        resource_filename=filename,
        ckan_last_modified=CKAN_LAST_MODIFIED,
    )
    return capture_id_for(path, data)


def _record() -> SchemaRecord:
    record = registry_module.load_registry().families[KEY][1].record
    assert record is not None
    return record


def _silver(data: Path) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / KEY).rglob("[!.]*.parquet"))
    assert files
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _run(data: Path) -> tuple[str, pl.DataFrame]:
    capture_id = capture(data)
    get_transformer(SOURCE, KEY, data).run(DAY, run_id="r")
    return capture_id, _silver(data)


def _no_silver(data: Path) -> bool:
    return not list((data / "silver" / SOURCE / KEY).rglob("[!.]*.parquet"))


def test_record_shape_matches_the_unit_spec() -> None:
    """Detects a record drifting from the spec: csv reader, strict utf-8, one header epoch
    with no issue recipe, ``temporal: none`` (no invented instant), key ``ggd_id`` (not the
    proposal's ``ng_id``), family-scope whole-capture selection (no resource partition, no
    edition map), CKAN vintage, no per-output eligibility override, no null token or bound."""
    record = _record()
    assert record.reader == "csv"
    assert record.encoding == "utf-8"
    assert len(record.epochs) == 1
    epoch = record.epochs[0]
    assert epoch.issue.kind == "none"
    assert list(epoch.header) == HEADER
    assert record.temporal.kind == "none"
    assert record.entity_key == ("ggd_id",)
    assert record.latest == "whole_capture"
    assert record.latest_partition is None
    assert record.edition_by_filename is None
    assert record.vintage == "ckan_last_modified"
    assert record.eligibility is None
    by_name = {c.name: c for c in epoch.columns}
    assert list(by_name) == HEADER
    for column in epoch.columns:
        assert column.source == column.name
        assert not column.null_tokens, column.name
        assert column.min is None and column.max is None, column.name
        assert column.nullable is (column.name not in NON_NULLABLE), column.name
        expected = (
            "int64"
            if column.name in INT_COLUMNS
            else "float64"
            if column.name in FLOAT_COLUMNS
            else "string"
        )
        assert column.dtype == expected, column.name


def test_the_family_is_eligible_and_its_only_resource_is_silver_to_it() -> None:
    """Detects a hold added, the package eligibility changed or the lookup resource routed
    elsewhere: the family inherits the package's ``eligible`` status and its single CSV is
    ``SILVER`` to it."""
    registry = load_registry()
    package, entry = registry.families[KEY]
    assert package.package == PACKAGE
    assert package.eligibility.status == "eligible"
    assert effective_eligibility(package, entry).status == "eligible"
    resource = registry.resources[RESOURCE_ID][1]
    assert resource.family == KEY
    assert isinstance(resource.disposition, SilverDisposition)
    assert resource.disposition.key == KEY
    silver = [
        r
        for r in registry.resources.values()
        if r[1].family == KEY and isinstance(r[1].disposition, SilverDisposition)
    ]
    assert [r[1].id for r in silver] == [RESOURCE_ID]


def test_the_fixture_types_with_no_exclusion(data: Path) -> None:
    """Detects a header that matches no epoch, a cast the vendor body does not satisfy and any
    excluded row: the capture completes with all its rows and none excluded."""
    capture_id = capture(data)
    transformer = get_transformer(SOURCE, KEY, data)
    written = transformer.run(DAY, run_id="r")
    assert written == len(table()) == 13
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, KEY, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == 13
    assert completion["rows_excluded"] == 0


def test_every_cell_equals_the_body_and_empties_are_null(data: Path) -> None:
    """Detects a cast that changes a value, zero-filled or ``""`` empties, a coordinate that is
    not the published decimal text and a trimmed string: every output cell equals the body's
    cell, every exact empty cell is null (never ``0`` or ``""``) in every typed column."""
    _capture_id, silver = _run(data)
    source = table()
    assert silver.height == len(source)
    for name in HEADER:
        cells = [row[name] for row in source]
        values = silver[name].to_list()
        for cell, value in zip(cells, values, strict=True):
            if cell == "":
                assert value is None, (name, value)
            elif name in INT_COLUMNS:
                assert value == int(cell), name
            elif name in FLOAT_COLUMNS:
                assert value == float(cell), name
            else:
                assert value == cell, name
    for name in HEADER:
        assert silver[name].null_count() == sum(row[name] == "" for row in source), name
    for name, count in FIXTURE_EMPTY.items():
        assert silver[name].null_count() == count, name
    for name in NON_NULLABLE:
        assert silver[name].null_count() == 0, name
    for name in STRING_COLUMNS:
        assert "" not in silver[name].drop_nulls().to_list(), name


def test_output_dtypes_and_no_resource_or_edition_column(data: Path) -> None:
    """Detects a boolean recode of ``has_pv``, a string coordinate and a resource / edition
    column on a family-scope reference record: identifiers and ``has_pv`` are Int64, coordinates
    Float64, names String, and the output carries neither ``resource_id`` nor ``edition``."""
    _capture_id, silver = _run(data)
    for name in INT_COLUMNS:
        assert silver.schema[name] == pl.Int64, name
    for name in FLOAT_COLUMNS:
        assert silver.schema[name] == pl.Float64, name
    for name in STRING_COLUMNS:
        assert silver.schema[name] == pl.Utf8, name
    assert "resource_id" not in silver.columns
    assert "edition" not in silver.columns
    assert set(HEADER) <= set(silver.columns)
    assert set(silver["has_pv"].to_list()) == {0, 1}


def test_the_key_is_ggd_id_not_ng_id(data: Path) -> None:
    """Detects the proposal's ``ng_id`` returning as the key or a renumbering of either id: in
    the ``MELK`` row ``ng_id=387`` and ``ggd_id=372`` differ, and ``ggd_id`` is unique across the
    output."""
    _capture_id, silver = _run(data)
    melk = silver.filter(pl.col("gsp_name") == "MELK_1")
    assert melk.height == 1
    row = melk.row(0, named=True)
    assert (row["ng_id"], row["ggd_id"], row["region_id"]) == (387, 372, 18)
    assert row["ng_id"] != row["ggd_id"]
    assert silver["ggd_id"].is_unique().all()


def test_repeated_non_key_identifiers_survive_without_dedup(data: Path) -> None:
    """Detects a deduplication on a non-key id: both ``dc_id=12`` rows, the three ``gsp_id=289``
    rows and the two ``region_id=16`` rows all reach the output."""
    _capture_id, silver = _run(data)
    assert silver.filter(pl.col("dc_id") == 12).height == 2
    assert silver.filter(pl.col("gsp_id") == 289).height == 3
    assert silver.filter(pl.col("region_id") == 16).height == 2
    assert silver.height == len(table())


def test_the_published_apostrophe_survives_byte_equal(data: Path) -> None:
    """Detects a mis-decode or normalisation of the vendor's U+2019: ``Clyde’s Mill`` is kept
    exactly (RULINGS: names stored as published)."""
    _capture_id, silver = _run(data)
    assert "Clyde’s Mill" in silver["region_name"].to_list()


def test_timestamps_come_from_the_capture_and_the_sidecar(data: Path) -> None:
    """Detects an invented observation instant (``temporal: none``): ``timestamp_utc`` is the
    capture's ``written_at`` and ``published_at`` is the sidecar's CKAN ``last_modified``, both
    tz-aware UTC."""
    _capture_id, silver = _run(data)
    assert set(silver["timestamp_utc"].to_list()) == {WRITTEN}
    assert set(silver["published_at"].to_list()) == {PUBLISHED}
    assert silver.schema["timestamp_utc"] == pl.Datetime("us", "UTC")
    assert silver.schema["published_at"] == pl.Datetime("us", "UTC")


def test_a_shared_ggd_id_fails_the_capture(data: Path) -> None:
    """Detects a key too weak to guard the body (RULINGS 538): two rows sharing one ``ggd_id``
    with different other cells fail with ``DuplicateEntityKeyError``, leave no completion and
    write no silver (never deduplicated)."""
    lines = body().decode("utf-8").split("\r\n")
    fields = next(csv.reader([lines[2]]))
    fields[HEADER.index("gnode_name")] = "CHANGED"
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    duplicate = out.getvalue()
    assert duplicate != lines[2]
    capture_id = capture(data, body() + duplicate.encode("utf-8") + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, KEY, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert "2 row(s) sharing an entity key" in info.value.failures[0][2]
    assert read_completion(data, KEY, capture_id) is None
    assert _no_silver(data)


def test_an_altered_header_name_fails_loud(data: Path) -> None:
    """Detects a header that matches the frozen epoch loosely: one renamed vendor column fails
    the capture, leaves no completion and writes nothing."""
    raw = body().replace(b"region_name", b"region_label", 1)
    assert raw != body()
    capture_id = capture(data, raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, KEY, data).run(DAY, run_id="r")
    assert read_completion(data, KEY, capture_id) is None
    assert _no_silver(data)


@pytest.mark.parametrize("column", ["ng_id", "ggd_id", "has_pv"])
def test_an_empty_non_nullable_cell_is_excluded_not_zero_filled(data: Path, column: str) -> None:
    """Detects a non-nullable column silently filled: a record whose ``ng_id`` / ``ggd_id`` /
    ``has_pv`` is empty is surfaced as an exclusion (or a failed capture), never written as
    ``0``."""
    lines = body().decode("utf-8").split("\r\n")
    fields = next(csv.reader([lines[1]]))
    fields[HEADER.index(column)] = ""
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    lines[1] = out.getvalue()
    capture_id = capture(data, "\r\n".join(lines).encode("utf-8"))
    transformer = get_transformer(SOURCE, KEY, data)
    try:
        transformer.run(DAY, run_id="r")
    except NesoCaptureFailedError:
        assert read_completion(data, KEY, capture_id) is None
        assert _no_silver(data)
        return
    assert transformer.last_excluded_row_count == 1
    assert _silver(data).height == len(table()) - 1
    assert _silver(data)[column].null_count() == 0


def test_the_embedded_child_stays_held_and_the_gis_files_stay_gis() -> None:
    """Detects a COVERED grant, a reworded reason or a changed unit on the embedded child, a
    changelog promoted out of ``DOC`` and a GIS resource routed anywhere else: the 2018 ZIP's
    ``view_gsp_gnode_all_20181031.csv`` keeps its ``HOLD`` (unit ``FILE``) verbatim, the
    changelog is ``DOC`` and every other resource of the GIS family is ``GIS``."""
    registry = load_registry()
    zip_resource = registry.resources["506d99a3-d4f5-4dd8-9586-652047682ccc"][1]
    child = next(c for c in zip_resource.children if c.child == EMBEDDED_CHILD)
    assert isinstance(child.disposition, HoldDisposition)
    assert child.disposition.unit == "FILE"
    assert child.disposition.reason == EMBEDDED_HOLD_REASON
    others = [c for c in zip_resource.children if c.child != EMBEDDED_CHILD]
    assert others
    assert all(isinstance(c.disposition, GisDisposition) for c in others)
    changelog = registry.resources[CHANGELOG_RESOURCE_ID][1]
    assert isinstance(changelog.disposition, DocDisposition)
    for resource_id, (package, resource) in registry.resources.items():
        if package.package != PACKAGE or resource_id in {RESOURCE_ID, CHANGELOG_RESOURCE_ID}:
            continue
        assert isinstance(resource.disposition, GisDisposition), resource.name
        assert resource.family == "gis_grid_supply_points_files", resource.name
    files_entry = registry.families["gis_grid_supply_points_files"][1]
    assert files_entry.record is None


def test_the_dno_package_stays_held() -> None:
    """Detects the DNO licence-area package leaving HOLD(N) (RULINGS 477, third-party licence
    carve-out): the package eligibility is ``held`` unit ``N`` and its files family has no
    record."""
    registry = load_registry()
    package, entry = registry.families["gis_dno_license_areas_files"]
    assert package.eligibility.status == "held"
    assert package.eligibility.unit == "N"  # type: ignore[union-attr]
    assert entry.record is None


def test_the_skeleton_page_renders_the_new_record() -> None:
    """Detects a record the docs generator cannot render."""
    page = skeleton.render_package(
        load_registry(),
        {
            "name": PACKAGE,
            "title": "GIS Boundaries for GB Grid Supply Points",
            "organization": {"title": "System"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    for needle in ("`ggd_id`", "`has_pv`", "`region_name`", KEY):
        assert needle in page, needle


def test_the_provenance_names_the_fixture_and_the_todos() -> None:
    """Detects a fixture left out of the provenance, the sha256 prefix, the resource id or a
    non-holding TODO dropped or reworded."""
    provenance = (FIXTURES / "PROVENANCE.md").read_text(encoding="utf-8")
    assert "`gsp_lookup.csv`" in provenance
    assert RESOURCE_ID in provenance
    assert "e6048a53" in provenance
    assert {p.name for p in FIXTURES.glob("*.csv")} == {"gsp_lookup.csv"}
    for needle in (
        "VENDOR-UNIQUENESS",
        "GSP-ID-JOIN",
        "CHANGELOG-GAP",
        "TODO: direct GSP-ID-to-feature equality is unavailable in this 2018 GeoJSON. "
        "A vendor-issued feature property or mapping identifying GSPs would settle it.",
        "TODO: a vendor changelog for the 20181031 → 20220314 revisions would settle their "
        "detailed changes.",
    ):
        assert needle in provenance, needle
