"""The metered wind output and wind BOA volume frozen records (v0.22-GEN-2H, ADR-040).

Every test writes recorded fixture captures (slices of the 2026-10-08 swept bronze under
``tests/fixtures/neso_data_portal/gen2h/``) into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here,
not at activation. On master neither family has a record, so ``get_transformer`` raises.

Units (K-GEN-2-FACTS g4; a ``ColumnSpec`` has no unit field, so they are recorded here): the
metered ``Scottish Wind Output``, ``England/Wales Wind Output`` and ``Total`` are MW; BOA
``BOA_Volume`` is MWh per settlement period, signed as supplied (curtailment negative; the
2026/27 file also carries positive volumes, which is part of the family's hold).

Fixture cut (a scratch script, not committed). Metered (CRLF in bronze): M01 = 2018-04-01
P1-P4 plus the whole 2018-10-28 long day (50 periods); M08 = the whole 2026-03-29 short day
(46 periods), 2026-04-01 P1-P4 and 2026-04-05 P1; M09 = 2026-04-01 P1-P4 and 2026-04-05 P1-P3,
so M08 and M09 share 5 settlement keys. BOA (LF in bronze): B07 = the identical
``2024-10-20,46,GORDW-2,...,-0.634`` pair and 20 neighbouring rows; B08 = one repeated
(date, period, generator) group with distinct volumes, 30 rows and one ``-1`` row; B09 = CSV
record line 351 (positive ``7.333``), two ``-1.000`` rows and 20 rows. ``git`` normalises a
committed fixture's line endings, so :func:`body` rebuilds each bronze original's convention.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
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
METERED = "metered_wind_output_monthly"
BOA = "wind_bmu_boa_volumes"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "gen2h"
REGISTRY_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gridflow"
    / "connectors"
    / "neso_data_portal"
    / "registry"
)
DAY = date(2026, 10, 8)
METERED_HEADER = [
    "Sett_Date",
    "Sett_Period",
    "Scottish Wind Output",
    "England/Wales Wind Output",
    "Total",
]
BOA_HEADER = ["Date", "Settlement_Period", "Generator_Name", "Generator_Full_Name", "BOA_Volume"]
BOA_QUESTION = (
    "TODO: NESO does not explain whole repeated rows (the 2018/19, 2019/20 and 2024/25 files "
    "repeat 21 rows on every column; 2,396 rows repeat date, period and generator), so a row's "
    "identity (one acceptance, one contribution or a duplicate) is unknown and the key carries "
    "boa_volume; the 2026/27 file also carries 1,044 positive volumes against the dictionary's "
    "negative-curtailment definition."
)

# Sidecar provenance copied from the real 2026-10-08 sidecars.
SIDECARS: dict[str, dict[str, str]] = {
    "metered_m01": {
        "family": METERED,
        "package": "monthly-operational-metered-wind-output",
        "package_id": "7f7fa642-6eff-4b8f-9e8b-7ede1ba50e20",
        "resource_id": "bf03c648-98d8-40f3-b5d9-e174cb2c1f81",
        "name": "Monthly Operational Metered Wind Output 2018-2019",
        "filename": "monthly-operational-metered-wind-output-2018-2019.csv",
        "modified": "2021-05-12T10:42:16.491017",
        "written": "2026-10-08T11:12:24.543560+00:00",
    },
    "metered_m08": {
        "family": METERED,
        "package": "monthly-operational-metered-wind-output",
        "package_id": "7f7fa642-6eff-4b8f-9e8b-7ede1ba50e20",
        "resource_id": "7622b040-977a-45a6-924e-f158df6c29f0",
        "name": "Monthly Operational Metered Wind Output 2025-2026",
        "filename": "monthly-operational-metered-wind-output-2025-2026.csv",
        "modified": "2026-04-07T10:00:37.054177",
        "written": "2026-10-08T11:12:43.591189+00:00",
    },
    "metered_m09": {
        "family": METERED,
        "package": "monthly-operational-metered-wind-output",
        "package_id": "7f7fa642-6eff-4b8f-9e8b-7ede1ba50e20",
        "resource_id": "c9bc94c4-6d8c-49ff-afff-67c0030bec05",
        "name": "Monthly Operational Metered Wind Output 2026-2027",
        "filename": "monthly-operational-metered-wind-output-2026-2027.csv",
        "modified": "2026-10-03T11:17:29.313427",
        "written": "2026-10-08T11:12:46.191152+00:00",
    },
    "boa_b07_dup": {
        "family": BOA,
        "package": "wind-bmu-boa-volumes",
        "package_id": "102638b9-6214-4db6-9115-86b5e879d1ce",
        "resource_id": "d3fbf6c1-7688-4486-8716-b5af0c895a5a",
        "name": "Wind BOA Volumes 2024/25",
        "filename": "boa_data_2024_25.csv",
        "modified": "2026-08-10T14:58:05.288334",
        "written": "2026-10-08T11:41:33.003242+00:00",
    },
    "boa_b08": {
        "family": BOA,
        "package": "wind-bmu-boa-volumes",
        "package_id": "102638b9-6214-4db6-9115-86b5e879d1ce",
        "resource_id": "7bf83942-3590-4fe5-9980-49a9546e30a9",
        "name": "Wind BOA Volumes 2025/26",
        "filename": "boa_data_2025_26.csv",
        "modified": "2026-05-28T09:03:25.895718",
        "written": "2026-10-08T11:41:52.749162+00:00",
    },
    "boa_b09": {
        "family": BOA,
        "package": "wind-bmu-boa-volumes",
        "package_id": "102638b9-6214-4db6-9115-86b5e879d1ce",
        "resource_id": "45598dcd-ea9c-4911-95f2-7946c5f3b034",
        "name": "Wind BOA Volumes 2026/27",
        "filename": "boa_data_2026_27.csv",
        "modified": "2026-10-08T09:00:15.469647",
        "written": "2026-10-08T11:41:49.905864+00:00",
    },
}


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings (and so the CLI) point at."""
    with tempfile.TemporaryDirectory(
        prefix="gh", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(name: str) -> bytes:
    """Fixture ``name`` with its bronze original's line ending (metered CRLF, BOA LF)."""
    raw = (FIXTURES / f"{name}.csv").read_bytes().replace(b"\r\n", b"\n")
    return raw.replace(b"\n", b"\r\n") if name.startswith("metered") else raw


def rows(name: str) -> list[dict[str, str]]:
    """The fixture's records as text, header-keyed."""
    return list(csv.DictReader(io.StringIO(body(name).decode("utf-8"), newline="")))


def capture(data: Path, name: str) -> str:
    """Write fixture ``name`` as a committed capture with its real sidecar values."""
    meta = SIDECARS[name]
    path, _sidecar = write_capture(
        data,
        meta["family"],
        body=body(name),
        written_at=datetime.fromisoformat(meta["written"]).astimezone(UTC),
        partition=DAY,
        package_slug=meta["package"],
        package_id=meta["package_id"],
        resource_id=meta["resource_id"],
        resource_name=meta["name"],
        resource_filename=meta["filename"],
        ckan_last_modified=meta["modified"],
        url_type="upload",
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _bytes_under(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _latest(db: Path, key: str) -> list[dict[str, Any]]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        frame = con.execute(f'SELECT * FROM "silver_{SOURCE}_{key}_latest"').pl()
    finally:
        con.close()
    return frame.sort(frame.columns).to_dicts()


# --------------------------------------------------------------------------- #
# Fixtures and record shapes (T-G2H-1..6)
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: the headers, the long
    and short days, the 5 shared metered keys, the identical BOA pair, a repeated BOA group
    with distinct volumes, the positive ``7.333`` and the ``-1`` volumes."""
    assert list(rows("metered_m01")[0]) == METERED_HEADER
    assert list(rows("boa_b08")[0]) == BOA_HEADER
    assert body("metered_m08").count(b"\r\n") == len(rows("metered_m08")) + 1
    assert b"\r" not in body("boa_b09")
    long_day = [r for r in rows("metered_m01") if r["Sett_Date"] == "2018-10-28"]
    short_day = [r for r in rows("metered_m08") if r["Sett_Date"] == "2026-03-29"]
    assert sorted(int(r["Sett_Period"]) for r in long_day) == list(range(1, 51))
    assert sorted(int(r["Sett_Period"]) for r in short_day) == list(range(1, 47))
    keys = {
        name: {(r["Sett_Date"], r["Sett_Period"]) for r in rows(name)}
        for name in ("metered_m08", "metered_m09")
    }
    assert len(keys["metered_m08"] & keys["metered_m09"]) == 5
    dup = rows("boa_b07_dup")
    assert len(dup) - len({tuple(r.values()) for r in dup}) == 1
    b08 = rows("boa_b08")
    groups = [(r["Date"], r["Settlement_Period"], r["Generator_Name"]) for r in b08]
    assert len(groups) - len(set(groups)) == 1
    assert len({tuple(r.values()) for r in b08}) == len(b08)
    assert any(r["BOA_Volume"] == "-1" for r in b08)
    volumes = [r["BOA_Volume"] for r in rows("boa_b09")]
    assert "7.333" in volumes and volumes.count("-1.000") == 2


def test_t_g2h_1_metered_record_shape() -> None:
    """Detects the metered record drifting from P-7: csv/utf-8, one epoch with the vendor
    header, typed columns (date, int64 1-50, three float64 MW columns), sp_pair on
    (sett_date, sett_period), key (resource_id, sett_date, sett_period), whole-capture
    selection per resource, ``ckan_last_modified`` vintage, no invented null token."""
    record = _record(METERED)
    assert (record.reader, record.encoding, len(record.epochs)) == ("csv", "utf-8", 1)
    epoch = record.epochs[0]
    assert list(epoch.header) == METERED_HEADER
    assert [(c.source, c.name, c.dtype, c.nullable) for c in epoch.columns] == [
        ("Sett_Date", "sett_date", "date", False),
        ("Sett_Period", "sett_period", "int64", False),
        ("Scottish Wind Output", "scottish_wind_output", "float64", True),
        ("England/Wales Wind Output", "england_wales_wind_output", "float64", True),
        ("Total", "total", "float64", True),
    ]
    assert epoch.columns[0].format == "%Y-%m-%d"
    assert (epoch.columns[1].min, epoch.columns[1].max) == (1, 50)
    assert all(not c.null_tokens for c in epoch.columns)
    assert epoch.issue.kind == "none"
    assert record.temporal.kind == "sp_pair"
    assert record.entity_key == ("resource_id", "sett_date", "sett_period")
    assert (record.latest, record.latest_partition) == ("whole_capture", "resource_id")
    assert record.vintage == "ckan_last_modified"
    assert record.siblings == ()


def test_t_g2h_1_boa_record_shape() -> None:
    """Detects the BOA record drifting from P-8: the vendor header, typed columns, sp_pair
    on (date, settlement_period), the value-bearing key C-5 chose, per-resource
    whole-capture selection, no null token (``-1`` stays numeric)."""
    record = _record(BOA)
    epoch = record.epochs[0]
    assert list(epoch.header) == BOA_HEADER
    assert [(c.source, c.name, c.dtype, c.nullable) for c in epoch.columns] == [
        ("Date", "date", "date", False),
        ("Settlement_Period", "settlement_period", "int64", False),
        ("Generator_Name", "generator_name", "string", True),
        ("Generator_Full_Name", "generator_full_name", "string", True),
        ("BOA_Volume", "boa_volume", "float64", True),
    ]
    assert (epoch.columns[1].min, epoch.columns[1].max) == (1, 50)
    assert all(not c.null_tokens for c in epoch.columns)
    assert record.temporal.kind == "sp_pair"
    assert record.entity_key == (
        "resource_id",
        "date",
        "settlement_period",
        "generator_name",
        "boa_volume",
    )
    assert (record.latest, record.latest_partition) == ("whole_capture", "resource_id")
    assert record.vintage == "ckan_last_modified"


def test_t_g2h_2_metered_fixtures_type_with_no_exclusion(data: Path) -> None:
    """Detects a cast the vendor bodies do not satisfy, or a DST day losing periods: all
    three captures complete with every row, P49/P50 of the long day and the short day's 46
    periods survive."""
    ids = [capture(data, name) for name in ("metered_m01", "metered_m08", "metered_m09")]
    transformer = get_transformer(SOURCE, METERED, data)
    written = transformer.run(DAY, run_id="r")
    expected = sum(len(rows(n)) for n in ("metered_m01", "metered_m08", "metered_m09"))
    assert written == expected
    assert transformer.last_excluded_row_count == 0
    for capture_id in ids:
        completion = read_completion(data, METERED, capture_id)
        assert completion is not None and completion["rows_excluded"] == 0
    silver = _silver(data, METERED)
    long_day = silver.filter(pl.col("sett_date") == date(2018, 10, 28))
    short_day = silver.filter(pl.col("sett_date") == date(2026, 3, 29))
    assert sorted(long_day["sett_period"].to_list()) == list(range(1, 51))
    assert sorted(short_day["sett_period"].to_list()) == list(range(1, 47))
    assert silver["timestamp_utc"].null_count() == 0


def test_t_g2h_3_boa_is_held_on_the_row_grain_question() -> None:
    """Detects the BOA family published without its hold (repeated rows unexplained,
    positive volumes against the definition): unit E-SEM, the P-8 question verbatim."""
    record = _record(BOA)
    assert isinstance(record.eligibility, Held)
    assert record.eligibility.unit == "E-SEM"
    assert record.eligibility.question == BOA_QUESTION


def test_t_g2h_4_effective_eligibility() -> None:
    """Detects a wrong publication status: metered is eligible, BOA is held."""
    registry = registry_module.load_registry()
    package, family = registry.families[METERED]
    assert isinstance(effective_eligibility(package, family), Eligible)
    package, family = registry.families[BOA]
    assert isinstance(effective_eligibility(package, family), Held)


def test_t_g2h_5_metered_total_is_the_sum_of_the_regions(data: Path) -> None:
    """Detects a column mapped to the wrong vendor header: on the fixture ``total`` equals
    Scottish plus England/Wales to the vendor's 3 decimals, and every value equals its CSV
    cell."""
    for name in ("metered_m01", "metered_m08"):
        capture(data, name)
    get_transformer(SOURCE, METERED, data).run(DAY, run_id="r")
    silver = _silver(data, METERED)
    gap = (
        silver["scottish_wind_output"] + silver["england_wales_wind_output"] - silver["total"]
    ).abs()
    assert gap.max() is not None and gap.max() < 5e-3  # type: ignore[operator]
    cells = sorted(float(r["Total"]) for n in ("metered_m01", "metered_m08") for r in rows(n))
    assert sorted(silver["total"].to_list()) == cells


def test_t_g2h_6_bmu_names_survive_byte_identical(data: Path) -> None:
    """Detects a normalised BM unit id or name (GEN2-3): both columns equal the CSV text."""
    capture(data, "boa_b09")
    get_transformer(SOURCE, BOA, data).run(DAY, run_id="r")
    silver = _silver(data, BOA)
    source = rows("boa_b09")
    assert sorted(silver["generator_name"].to_list()) == sorted(r["Generator_Name"] for r in source)
    assert sorted(silver["generator_full_name"].to_list()) == sorted(
        r["Generator_Full_Name"] for r in source
    )
    assert "AG-GEDF03" in set(silver["generator_name"].to_list())


# --------------------------------------------------------------------------- #
# A5, A6
# --------------------------------------------------------------------------- #


def _package_doc(filename: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((REGISTRY_DIR / filename).read_text(encoding="utf-8"))
    return document


def test_a5_metered_latest_keeps_both_resources_on_shared_keys(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A5: detects adjudication altering data, or a precedence invented between the two
    archives: ``_latest`` (catalogue, both as-of modes, and Polars) serves M08's and M09's rows
    on every one of the 5 shared keys, and the silver, state and ``_latest`` bytes are equal
    across reconcile without the entry (exit 1, two overlap gaps) and with it (exit 0)."""
    install_generated(
        monkeypatch,
        data / "_registry",
        [_package_doc("monthly-operational-metered-wind-output.json")],
    )
    m01, m08, m09 = (capture(data, n) for n in ("metered_m01", "metered_m08", "metered_m09"))
    get_transformer(SOURCE, METERED, data).run(DAY, run_id="r")
    db = data / "cat.duckdb"
    init_catalogue(db, data)

    latest = both_as_of(db, data, METERED, None)
    assert set(latest) == {m01, m08, m09}
    early = datetime(2026, 4, 8, tzinfo=UTC)
    assert set(both_as_of(db, data, METERED, early)) == {m01, m08}
    frame = pl.DataFrame(_latest(db, METERED))
    shared = frame.filter(pl.struct("sett_date", "sett_period").is_duplicated()).select(
        "sett_date", "sett_period", "bronze_capture_id"
    )
    assert shared.height == 10
    assert (
        shared.group_by("sett_date", "sett_period")
        .agg(pl.col("bronze_capture_id").n_unique())["bronze_capture_id"]
        .to_list()
        == [2] * 5
    )

    before = (_bytes_under(data / "silver"), _bytes_under(data / "state"), _latest(db, METERED))
    code, lines = run_cli(METERED, "--cutoff", DAY.isoformat())
    assert code == 1
    assert sorted(line.split()[4] for line in lines if line.startswith("GAP overlap")) == sorted(
        [m08, m09]
    )
    entry = {
        "family": METERED,
        "category": "overlap",
        "captures": [m08, m09],
        "reason": "synthetic",
        "question": "synthetic?",
        "evidence": "synthetic",
        "ruling": "547",
    }
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json([entry]), encoding="utf-8"
    )
    code, lines = run_cli(METERED, "--cutoff", DAY.isoformat())
    assert code == 0, lines
    assert "SUMMARY adjudicated 2" in lines
    after = (_bytes_under(data / "silver"), _bytes_under(data / "state"), _latest(db, METERED))
    assert after == before


def test_a6_boa_duplicate_fails_clean_loads_latest_serves_two(data: Path) -> None:
    """A6: detects a BOA key that dedups, sums or silently drops the repeated row, or a
    clean archive failing: B07's identical pair fails the capture with a
    ``DuplicateEntityKeyError`` failure record (no completion, no output); B08 and B09 load
    with zero excluded rows, ``_latest`` serves both resources, and the positive and ``-1``
    volumes and the names equal the CSV."""
    dup = capture(data, "boa_b07_dup")
    b08, b09 = capture(data, "boa_b08"), capture(data, "boa_b09")
    transformer = get_transformer(SOURCE, BOA, data)
    with pytest.raises(NesoCaptureFailedError) as info:
        transformer.run(DAY, run_id="r")
    assert [capture_id for capture_id, _cls, _message in info.value.failures] == [dup]
    failure = read_failure(data, BOA, dup)
    assert failure is not None
    assert failure["error_class"] == DuplicateEntityKeyError.__name__
    assert read_completion(data, BOA, dup) is None
    for capture_id in (b08, b09):
        completion = read_completion(data, BOA, capture_id)
        assert completion is not None and completion["rows_excluded"] == 0
    silver = _silver(data, BOA)
    assert dup not in set(silver["bronze_capture_id"].to_list())

    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, BOA, None)) == {b08, b09}
    frame = pl.DataFrame(_latest(db, BOA))
    assert set(frame["resource_id"].to_list()) == {
        SIDECARS["boa_b08"]["resource_id"],
        SIDECARS["boa_b09"]["resource_id"],
    }
    source = rows("boa_b08") + rows("boa_b09")
    assert frame.height == len(source)
    assert sorted(frame["boa_volume"].to_list()) == sorted(float(r["BOA_Volume"]) for r in source)
    assert 7.333 in frame["boa_volume"].to_list()
    assert frame["boa_volume"].to_list().count(-1.0) == 3
    assert sorted(frame["generator_name"].to_list()) == sorted(r["Generator_Name"] for r in source)
