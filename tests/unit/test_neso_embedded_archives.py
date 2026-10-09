"""The embedded wind and solar forecast archives (v0.22-EF, ADR-038).

The archive family ``embedded_wind_solar_forecast_archive`` stays the recordless
bronze home; two sibling-fed owners carry the records: the yearly uploads
(``embedded_forecast_archive_upload``, two header epochs) and the 2026 datastore
dump (``embedded_forecast_archive_dump``). The truncated 2019 upload is held.

Registry and wiring assertions run out of process (the
``test_neso_registry.py`` ``_run``/``_assert_ok`` idiom), because pytest
collection has already imported the connector and the transformers. Engine
tests install the COMMITTED package document through P-15's seam
(``install_generated``), so the records under test are the committed ones, and
write captures under a short tmp data root; nothing touches ``C:/gridflow-data``.

Fixtures (``tests/fixtures/neso_data_portal/embedded_archive/``, T-EF0), cut
byte-exact from the real bronze bodies of 2026-10-08 by a scratch script that is
not committed; bodies are read here with CRLF folded to LF, because a Windows
checkout with ``core.autocrlf`` rewrites line endings:

- ``upload_2024.csv``: the H9 header, then the first 3 lines of each issue
  ``2024-01-01T00:12:00Z``, ``2024-03-31T00:12:00Z``, ``2024-03-31T02:12:00Z``,
  ``2024-07-01T12:12:00Z``, ``2024-10-27T00:12:00Z``, ``2024-10-27T01:12:00Z``,
  ``2024-10-27T02:12:00Z``; plus, from issue ``2024-03-31T00:12:00Z``, the
  target 2024-03-31 SP46 line and, from issue ``2024-10-27T00:12:00Z``, the
  target 2024-10-27 SP49 and SP50 lines. Body order kept.
- ``upload_2025_head.csv``: the first 5 lines of the seat's 8,192-byte ranged
  probe of the 2025 upload (LIVE-PROBE, RULINGS 514), the H10 header; 656 bytes,
  sha256 ``fda1890b1686eeb61981dff53d78b254ec3b5cd0b13fe23d58100c2e48995040`` in
  its LF form.
- ``dump_2026.csv``: the H9 header, then the first 3 lines of each issue
  ``2026-01-01T00:12:00``, ``2026-03-29T00:12:10``, ``2026-03-29T02:12:11``,
  ``2026-06-12T11:54:02``, ``2026-10-08T06:52:26``; plus the first 3 null-solar
  lines of issue ``2026-08-07T22:53:25``. Body order kept.
- ``upload_2019_tail.csv``: the H9 header plus the 2019 body's last 2 lines,
  with no trailing newline (the vendor truncation, E14).
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest
from _neso_generic_support import assert_same_output, install_generated, snapshot, write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.captures import scan_dataset
from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, latest_select_sql, select_latest_vintage
from gridflow.silver.neso_data_portal import embedded_wind_solar_forecast as bespoke_module
from gridflow.silver.neso_data_portal._bronze import _ISSUE_TOKEN_PATTERN
from gridflow.silver.neso_data_portal.casting import TypedChild, type_child
from gridflow.silver.neso_data_portal.completion import (
    CaptureContextError,
    NesoCaptureFailedError,
    capture_context,
    read_completion,
    read_failure,
    scan_completions,
)
from gridflow.silver.neso_data_portal.embedded_wind_solar_forecast import (
    EmbeddedWindSolarForecastTransformer,
)
from gridflow.silver.neso_data_portal.generic import families_of, output_columns
from gridflow.silver.neso_data_portal.readers import read_csv_body
from gridflow.silver.neso_data_portal.reconcile import drain, reconcile
from gridflow.storage.duckdb import init_catalogue
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry import Registry, SchemaRecord

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "neso_data_portal" / "embedded_archive"
LIVE_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "neso_data_portal" / "embedded_forecast.csv"
REGISTRY_DIR = Path(registry_module.__file__).parent

SOURCE = "neso_data_portal"
ARCHIVE = "embedded_wind_solar_forecast_archive"
LIVE = "embedded_wind_solar_forecast"
UPLOAD_OWNER = "embedded_forecast_archive_upload"
DUMP_OWNER = "embedded_forecast_archive_dump"
PKG_SLUG = "embedded-wind-and-solar-forecasts"
PKG_ID = "91c0c70e-0ef5-4116-b6fa-7ad084b5e0e8"
DAY = date(2026, 10, 8)
ENTITY_KEY = ("settlement_date", "settlement_period", "issue_time")

H9 = (
    "DATE_GMT",
    "TIME_GMT",
    "SETTLEMENT_DATE",
    "SETTLEMENT_PERIOD",
    "EMBEDDED_WIND_FORECAST",
    "EMBEDDED_WIND_CAPACITY",
    "EMBEDDED_SOLAR_FORECAST",
    "EMBEDDED_SOLAR_CAPACITY",
    "Forecast_Datetime",
)
H10 = (*H9, "source_file")

R19 = (
    "vendor body truncated: declared Content-Length equals the 260,472,832 B received, "
    "the final row ends mid-value (Forecast_Datetime '2019-12-2') and issues stop at "
    "2019-12-21T03:12; a strict cast fails the whole capture (D-41); re-disposition only "
    "after NESO re-uploads (a new last_modified)"
)
Q_EF1 = (
    "TODO: Forecast_Datetime ends in 'Z' but measures as UK local time (no 01:xx issue on "
    "spring-forward nights; one 01:12 issue on fall-back nights, the BST occurrence); typed "
    "Europe/London, fold earliest (ADR-038); NESO does not document the zone"
)
Q_EF2 = (
    "TODO: Forecast_Datetime is naive; rows before 2026-06-12T11:54:02 measure as UK local "
    "time (no 01:xx issue on 2026-03-29); rows from the 2026-06-12 forecast-system migration "
    "have crossed no DST transition, so their zone is unmeasured; typed Europe/London, fold "
    "earliest (ADR-038); NESO does not document the zone"
)

LIVE_ID = "db6c038f-98af-4570-ab60-24d71ebd0ae5"
ID_2019 = "bc4d1093-ecf2-46d8-b207-7a0e3e8fb957"
UPLOAD_IDS = (
    "8a7249d4-ee67-45aa-9641-a3e063f54dba",
    "794107f5-8567-4b32-ae3a-6817fec73e5c",
    "07b0b42c-5152-4fc5-bcd1-e10ec8ec07de",
    "26c9ef64-ce43-4e22-b984-ef013636aacb",
    "06abd00a-ef6b-488b-9b6d-5e08fdc0c890",
    "fc13df13-2dad-4a1c-b9e3-4569efba4955",
)
ID_2020 = UPLOAD_IDS[0]
ID_2024 = UPLOAD_IDS[4]
ID_2025 = UPLOAD_IDS[5]
DUMP_ID = "31861619-0b86-47ba-bac2-d008a760af54"

_YEARS = {ID_2019: 2019, **{rid: 2020 + i for i, rid in enumerate(UPLOAD_IDS)}, DUMP_ID: 2026}
NAMES = {rid: f"Embedded Solar and Wind Forecast Archive {year}" for rid, year in _YEARS.items()}
FILENAMES = {
    **{rid: f"embedded_archive_{year}.csv" for rid, year in _YEARS.items()},
    DUMP_ID: DUMP_ID,
}

# The real 2026-10-08 sidecar clocks (E2/E25): CKAN last_modified, written_at.
LM_2019 = "2025-05-23T09:32:48.819542"
W_2019 = datetime(2026, 10, 8, 10, 36, 54, tzinfo=UTC)
LM_2024 = "2025-05-23T14:30:16.994795"
W_2024 = datetime(2026, 10, 8, 10, 37, 37, tzinfo=UTC)
LM_2025 = "2026-01-17T07:00:51"
W_2025 = datetime(2026, 10, 8, 11, 0, 0, tzinfo=UTC)
W_DUMP = datetime(2026, 10, 8, 10, 52, 46, tzinfo=UTC)


def _run(code: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run ``code`` in a fresh interpreter with the project on its path."""
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code), *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
    )


def _assert_ok(result: subprocess.CompletedProcess[str]) -> str:
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "OK" in result.stdout, result.stdout
    return result.stdout


_CONSTANTS = f"""
ARCHIVE = {ARCHIVE!r}
LIVE = {LIVE!r}
UPLOAD_OWNER = {UPLOAD_OWNER!r}
DUMP_OWNER = {DUMP_OWNER!r}
H9 = {H9!r}
H10 = {H10!r}
R19 = {R19!r}
Q_EF1 = {Q_EF1!r}
Q_EF2 = {Q_EF2!r}
LIVE_ID = {LIVE_ID!r}
ID_2019 = {ID_2019!r}
UPLOAD_IDS = {UPLOAD_IDS!r}
DUMP_ID = {DUMP_ID!r}
"""


def test_ef1_every_csv_resource_routes_to_one_owner_or_a_hold() -> None:
    """Detects an archive CSV resource left unrouted, routed to two owners, routed into
    the bespoke family, or a record drifting from P-3/P-4 (criterion 1)."""
    code = (
        _CONSTANTS
        + """
from gridflow.connectors.neso_data_portal.registry import load_registry
from gridflow.silver.neso_data_portal.generic import (
    INGEST_ONLY_REASON,
    generated_registrations,
)

registry = load_registry()
(package,) = [p for p in registry.packages if p.package == "embedded-wind-and-solar-forecasts"]
csv = {r.id: r for r in package.resources if r.format == "CSV"}
assert set(csv) == {LIVE_ID, ID_2019, DUMP_ID, *UPLOAD_IDS}, sorted(csv)
assert len(csv) == 9

def route(r):
    d = r.disposition
    return (d.kind, getattr(d, "key", None))

assert route(csv[LIVE_ID]) == ("SILVER", LIVE)
for rid in UPLOAD_IDS:
    assert route(csv[rid]) == ("SILVER", UPLOAD_OWNER), rid
assert route(csv[DUMP_ID]) == ("SILVER", DUMP_OWNER)
hold = csv[ID_2019].disposition
assert hold.kind == "HOLD" and hold.unit == "E-SEM" and hold.reason == R19, hold
assert csv[LIVE_ID].family == LIVE
for rid in (ID_2019, DUMP_ID, *UPLOAD_IDS):
    assert csv[rid].family == ARCHIVE, rid
assert all(not r.children for r in csv.values())

families = {f.key: f for f in package.families}
assert families[ARCHIVE].record is None
for owner in (UPLOAD_OWNER, DUMP_OWNER):
    rec = families[owner].record
    assert rec is not None, owner
    assert rec.siblings == (ARCHIVE,), rec.siblings
    assert not [r for r in package.resources if r.family == owner], owner
upload = families[UPLOAD_OWNER].record
dump = families[DUMP_OWNER].record
assert upload.eligibility.status == "held" and upload.eligibility.unit == "E-SEM"
assert upload.eligibility.question == Q_EF1
assert dump.eligibility.status == "held" and dump.eligibility.unit == "E-SEM"
assert dump.eligibility.question == Q_EF2
assert upload.vintage == "ckan_last_modified" and dump.vintage == "capture_fallback"
assert tuple(e.header for e in upload.epochs) == (H9, H10)
assert tuple(e.header for e in dump.epochs) == (H9,)

live = families[LIVE].model_dump(mode="json")
assert live == {
    "key": LIVE, "kind": "tabular", "legacy": True, "archetype": "FC",
    "refresh": "intraday", "empty_allowed": False, "max_download_bytes": 8388608,
    "name_regex": None, "transformer": "bespoke", "record": None,
}, live
live_res = csv[LIVE_ID].model_dump(mode="json")
assert live_res == {
    "id": LIVE_ID, "name": "Embedded Solar and Wind Forecast", "format": "CSV",
    "url_type": "upload", "family": LIVE,
    "disposition": {"kind": "SILVER", "key": LIVE}, "children": [],
}, live_res

generated = generated_registrations(registry)
for owner in (UPLOAD_OWNER, DUMP_OWNER):
    assert owner in generated.transformers, owner
    spec = generated.specs[("neso_data_portal", owner)]
    assert spec.key_columns == ("settlement_date", "settlement_period"), spec
    assert spec.order_columns == ("issue_time", "available_at"), spec
    assert spec.mode == "key_latest", spec
assert generated.ingest_only[("neso_data_portal", ARCHIVE)][0] == INGEST_ONLY_REASON
assert ARCHIVE not in generated.transformers
print("OK")
"""
    )
    _assert_ok(_run(code))


def test_ef8_the_raised_cap_is_this_family_only() -> None:
    """Detects the 2025 upload (645,655,728 B) still refused at the archive's cap, or a
    cap raised on any other family (P-2)."""
    code = (
        _CONSTANTS
        + """
from gridflow.connectors.neso_data_portal.endpoints import build_families
from gridflow.connectors.neso_data_portal.registry import load_registry

families = build_families(load_registry())
cap = families[ARCHIVE].max_download_bytes
assert cap == 805306368 and cap > 645655728, cap
above = sorted(k for k, f in families.items() if f.max_download_bytes > 536870912)
assert above == [ARCHIVE], above
assert families[LIVE].max_download_bytes == 8388608
assert families["embedded_wind_solar_forecast_files"].max_download_bytes == 536870912
print("OK")
"""
    )
    _assert_ok(_run(code))


# --------------------------------------------------------------------------- #
# Engine tests over the committed package document (T-EF2..T-EF7, T-EF9, T-EF10)
# --------------------------------------------------------------------------- #


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root (ADR-036 MAX_PATH margin); gold views out of scope."""
    pipeline_runner.import_transformers()
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(prefix="ef", ignore_cleanup_errors=True) as root:
        yield Path(root)


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes().replace(b"\r\n", b"\n")


def _package_doc() -> dict[str, Any]:
    document: dict[str, Any] = json.loads(
        (REGISTRY_DIR / f"{PKG_SLUG}.json").read_text(encoding="utf-8")
    )
    return document


def _install(monkeypatch: pytest.MonkeyPatch, data: Path) -> tuple[Registry, Any]:
    """Install the committed package document and its generated set (P-15)."""
    return install_generated(monkeypatch, data / "_reg", [_package_doc()])  # type: ignore[no-any-return]


def _record(registry: Registry, key: str) -> SchemaRecord:
    record = registry.families[key][1].record
    assert record is not None, key
    return record


def _capture(
    data: Path,
    resource_id: str,
    body: bytes,
    *,
    written: datetime,
    last_modified: str | None,
    url_type: str = "upload",
) -> str:
    """Write one archive capture under the archive key; return its capture id."""
    path, _sidecar = write_capture(
        data,
        ARCHIVE,
        package_slug=PKG_SLUG,
        package_id=PKG_ID,
        resource_id=resource_id,
        resource_name=NAMES[resource_id],
        body=body,
        written_at=written,
        ckan_last_modified=last_modified,
        resource_filename=FILENAMES[resource_id],
        url_type=url_type,
        partition=DAY,
    )
    return path.relative_to(data).as_posix()


def _dump_capture(data: Path, body: bytes, written: datetime = W_DUMP) -> str:
    return _capture(data, DUMP_ID, body, written=written, last_modified=None, url_type="datastore")


def _outputs(data: Path, key: str) -> list[Path]:
    return sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))


def _silver(data: Path, key: str) -> pl.DataFrame:
    return pl.concat(
        [pl.read_parquet(path, hive_partitioning=False) for path in _outputs(data, key)],
        how="vertical",
    )


def _scan_capture(data: Path, registry: Registry, capture_id: str) -> Capture:
    scan = scan_dataset(
        PathBuilder(data).bronze_dir(SOURCE, ARCHIVE),
        registry,
        partition=DAY,
        require_provenance=False,
    )
    (capture,) = [c for c in scan.captures if c.body.relative_to(data).as_posix() == capture_id]
    return capture


def _type_directly(data: Path, registry: Registry, key: str, capture_id: str) -> TypedChild:
    """``read_csv_body`` then ``type_child`` under ``key``'s committed record, in memory."""
    record = _record(registry, key)
    capture = _scan_capture(data, registry, capture_id)
    ctx = capture_context(capture, record, data)
    (table,) = read_csv_body(capture.body, record, ())
    return type_child(table, record, ctx)


def _assert_failed(data: Path, key: str, capture_id: str, error_class: str) -> None:
    assert _outputs(data, key) == []
    assert read_completion(data, key, capture_id) is None
    failure = read_failure(data, key, capture_id)
    assert failure is not None and failure["error_class"] == error_class, failure


def _utc(*parts: int) -> datetime:
    return datetime(*parts, tzinfo=UTC)  # type: ignore[misc]


def _naive_utc(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def _instants(frame: pl.DataFrame, column: str) -> list[datetime]:
    """The column's values as UTC instants (zone-agnostic comparison)."""
    return frame.get_column(column).dt.convert_time_zone("UTC").to_list()


def _rows(body: bytes) -> int:
    return len([line for line in body.split(b"\n")[1:] if line.strip()])


def _targets(body: bytes) -> set[tuple[str, str]]:
    """The distinct ``(SETTLEMENT_DATE date part, SETTLEMENT_PERIOD)`` of a body."""
    out = set()
    for line in body.split(b"\n")[1:]:
        if line.strip():
            cells = line.decode().split(",")
            out.add((cells[2][:10], cells[3]))
    return out


class TestEf2UploadRecordTypesBothEpochs:
    """T-EF2: the upload owner types H9 (2020-2024) and H10 (2025) and refuses others."""

    def test_a_h9_2024_types_every_row_in_london_time(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects an H9 body refused or rows excluded, ``Forecast_Datetime`` read as
        UTC (as labelled) rather than UK local time with fold ``earliest``, a lost DST
        period (spring SP46, autumn SP49/50), or a clock other than ``last_modified``."""
        _registry, generated = _install(monkeypatch, data)
        body = _fixture("upload_2024.csv")
        capture_id = _capture(data, ID_2024, body, written=W_2024, last_modified=LM_2024)
        transformer = generated.transformers[UPLOAD_OWNER](data)
        assert transformer.run(DAY, run_id="r") == _rows(body) == 24
        assert transformer.last_excluded_row_count == 0
        completion = read_completion(data, UPLOAD_OWNER, capture_id)
        assert completion is not None and completion["rows_excluded"] == 0

        frame = _silver(data, UPLOAD_OWNER)
        assert frame.height == 24
        assert frame["source_file"].null_count() == 24
        assert sorted(set(_instants(frame, "issue_time"))) == [
            _utc(2024, 1, 1, 0, 12),  # GMT: equal to the label
            _utc(2024, 3, 31, 0, 12),  # before the 01:00 UTC spring change
            _utc(2024, 3, 31, 1, 12),  # 2024-03-31T02:12:00Z read as BST
            _utc(2024, 7, 1, 11, 12),  # BST
            _utc(2024, 10, 26, 23, 12),  # 2024-10-27T00:12:00Z read as BST
            _utc(2024, 10, 27, 0, 12),  # 01:12 ambiguous: earliest, the BST occurrence
            _utc(2024, 10, 27, 2, 12),  # GMT after the fall-back
        ]
        spring_02 = frame.filter(pl.col("issue_time") == _utc(2024, 3, 31, 1, 12))
        assert spring_02["settlement_period"].sort().to_list() == [3, 4, 5]

        dst = frame.filter(
            ((pl.col("settlement_date") == date(2024, 3, 31)) & (pl.col("settlement_period") == 46))
            | (
                (pl.col("settlement_date") == date(2024, 10, 27))
                & pl.col("settlement_period").is_in([49, 50])
            )
        ).sort("settlement_date", "settlement_period")
        assert dst.select("settlement_date", "settlement_period").rows() == [
            (date(2024, 3, 31), 46),
            (date(2024, 10, 27), 49),
            (date(2024, 10, 27), 50),
        ]
        expected = [
            _utc(2024, 3, 31, 22, 30),
            _utc(2024, 10, 27, 23, 0),
            _utc(2024, 10, 27, 23, 30),
        ]
        assert _instants(dst, "timestamp_utc") == expected
        assert _instants(dst, "event_time") == expected

        published = _naive_utc(LM_2024)
        assert set(_instants(frame, "published_at")) == {published}
        assert set(_instants(frame, "available_at")) == {published}

    def test_b_h10_2025_keeps_source_file(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the 2025 H10 header refused (no second epoch) or its ``source_file``
        column dropped, or a clock other than the 2025 ``last_modified``."""
        _registry, generated = _install(monkeypatch, data)
        body = _fixture("upload_2025_head.csv")
        capture_id = _capture(data, ID_2025, body, written=W_2025, last_modified=LM_2025)
        transformer = generated.transformers[UPLOAD_OWNER](data)
        assert transformer.run(DAY, run_id="r") == 4
        assert transformer.last_excluded_row_count == 0
        assert read_completion(data, UPLOAD_OWNER, capture_id) is not None

        frame = _silver(data, UPLOAD_OWNER).sort("settlement_period")
        assert frame["settlement_date"].unique().to_list() == [date(2025, 1, 1)]
        assert frame["settlement_period"].to_list() == [1, 2, 3, 4]
        assert frame["source_file"].to_list() == ["Embedded_Archive_2025_1.csv"] * 4
        assert set(_instants(frame, "issue_time")) == {_utc(2025, 1, 1, 0, 12)}
        published = _utc(2026, 1, 17, 7, 0, 51)
        assert set(_instants(frame, "published_at")) == {published}
        assert set(_instants(frame, "available_at")) == {published}

    def test_c_an_unsupported_header_fails_the_capture_loud(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-3: detects a header that is neither epoch being typed by position or
        skipped silently instead of failing the capture with ``HeaderEpochError``."""
        _registry, generated = _install(monkeypatch, data)
        header, rest = _fixture("upload_2025_head.csv").split(b"\n", 1)
        cells = header.split(b",")
        reordered = [*cells[:8], b"source_file", b"Forecast_Datetime"]
        assert set(reordered) == set(cells) and reordered != cells
        capture_id = _capture(
            data,
            ID_2025,
            b",".join(reordered) + b"\n" + rest,
            written=W_2025,
            last_modified=LM_2025,
        )
        with pytest.raises(NesoCaptureFailedError, match="HeaderEpochError"):
            generated.transformers[UPLOAD_OWNER](data).run(DAY, run_id="r")
        _assert_failed(data, UPLOAD_OWNER, capture_id, "HeaderEpochError")


class TestEf3DumpRecordTypesRealRows:
    """T-EF3: the dump owner types the real dump rows, and its residuals fail loud."""

    def test_dump_rows(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Detects a dump row lost to the migration's non-midnight ``SETTLEMENT_DATE``,
        a vendor null dropped, ``Forecast_Datetime`` read as UTC, or a dump dated by a
        ``last_modified`` it does not have (published_at must be null)."""
        _registry, generated = _install(monkeypatch, data)
        body = _fixture("dump_2026.csv")
        capture_id = _dump_capture(data, body)
        transformer = generated.transformers[DUMP_OWNER](data)
        assert transformer.run(DAY, run_id="r") == _rows(body) == 18
        assert transformer.last_excluded_row_count == 0
        assert read_completion(data, DUMP_OWNER, capture_id) is not None

        frame = _silver(data, DUMP_OWNER)
        assert "source_file" not in frame.columns
        assert sorted(set(_instants(frame, "issue_time"))) == [
            _utc(2026, 1, 1, 0, 12),
            _utc(2026, 3, 29, 0, 12, 10),
            _utc(2026, 3, 29, 1, 12, 11),  # 2026-03-29T02:12:11 read as BST
            _utc(2026, 6, 12, 10, 54, 2),
            _utc(2026, 8, 7, 21, 53, 25),
            _utc(2026, 10, 8, 5, 52, 26),  # 2026-10-08T06:52:26 read as BST
        ]
        migration = frame.filter(pl.col("issue_time") == _utc(2026, 6, 12, 10, 54, 2))
        assert migration.select("settlement_date", "settlement_period").sort(
            "settlement_period"
        ).rows() == [(date(2026, 6, 12), 27), (date(2026, 6, 12), 28), (date(2026, 6, 12), 29)]
        nulls = frame.filter(pl.col("embedded_solar_forecast").is_null())
        assert nulls.height == 3
        assert nulls["embedded_solar_capacity"].null_count() == 3
        assert set(_instants(nulls, "issue_time")) == {_utc(2026, 8, 7, 21, 53, 25)}
        assert frame["published_at"].null_count() == 18
        assert set(_instants(frame, "available_at")) == {W_DUMP}
        assert set(_instants(frame, "capture_written_at")) == {W_DUMP}

    def test_fm5_a_repeated_local_issue_fails_the_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-5 (accepted residual): detects a post-migration fall-back night's repeated
        local ``Forecast_Datetime`` being merged or dropped instead of failing loud."""
        _registry, generated = _install(monkeypatch, data)
        row = "2026-10-25T00:00:00,01:30,2026-10-25T00:00:00,3,{v},6417,0,23963,2026-10-25T01:52:30"
        body = ",".join(H9) + "\n" + row.format(v=1000) + "\n" + row.format(v=1001) + "\n"
        capture_id = _dump_capture(data, body.encode())
        with pytest.raises(NesoCaptureFailedError, match="DuplicateEntityKeyError"):
            generated.transformers[DUMP_OWNER](data).run(DAY, run_id="r")
        _assert_failed(data, DUMP_OWNER, capture_id, "DuplicateEntityKeyError")

    def test_fm6_a_non_existent_local_issue_raises(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-6 (accepted residual): detects a 01:xx issue on the 2027-03-28 spring
        gap being shifted silently instead of failing the cast."""
        registry, _generated = _install(monkeypatch, data)
        row = (
            "2027-03-28T00:00:00,01:30,2027-03-28T00:00:00,3,1000,6417,0,23963,2027-03-28T01:30:00"
        )
        capture_id = _dump_capture(data, (",".join(H9) + "\n" + row + "\n").encode())
        with pytest.raises(pl.exceptions.PolarsError, match="non-existent"):
            _type_directly(data, registry, DUMP_OWNER, capture_id)


class TestEf4The2019ArchiveIsHeld:
    """T-EF4: the truncated 2019 body cannot type, and its HOLD keeps it out of silver."""

    def test_a_the_2019_body_cannot_type(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Detects the truncation being typed (a partial value coerced or dropped):
        the strict cast must fail the whole capture, the reason for P-5's HOLD."""
        registry, _generated = _install(monkeypatch, data)
        body = _fixture("upload_2019_tail.csv")
        assert not body.endswith(b"\n")
        capture_id = _capture(data, ID_2019, body, written=W_2019, last_modified=LM_2019)
        with pytest.raises(pl.exceptions.InvalidOperationError, match="2019-12-2"):
            _type_directly(data, registry, UPLOAD_OWNER, capture_id)

    def test_b_the_held_capture_is_never_expected_or_transformed(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the 2019 capture routed to an owner (a failed capture and a
        reconcile gap) or expected by reconcile despite its HOLD."""
        registry, generated = _install(monkeypatch, data)
        held = _capture(
            data, ID_2019, _fixture("upload_2019_tail.csv"), written=W_2019, last_modified=LM_2019
        )
        _capture(data, ID_2024, _fixture("upload_2024.csv"), written=W_2024, last_modified=LM_2024)
        assert families_of(_scan_capture(data, registry, held), ARCHIVE, registry) == {}
        assert generated.transformers[UPLOAD_OWNER](data).run(DAY, run_id="r") == 24
        assert generated.transformers[DUMP_OWNER](data).run(DAY, run_id="r") == 0
        report = reconcile(data, registry, [UPLOAD_OWNER, DUMP_OWNER], DAY)
        assert report.gaps == (), report.lines()
        for owner in (UPLOAD_OWNER, DUMP_OWNER):
            assert read_completion(data, owner, held) is None
            assert read_failure(data, owner, held) is None
        assert held not in scan_completions(data).collect()["bronze_capture_id"].to_list()


class TestEf5ArchiveBodiesNeverReachTheBespoke:
    """T-EF5 (criterion 3): the bespoke live transformer reads its own directory only."""

    LIVE_FILENAME = "202610080725_embedded_forecast.csv"

    def _live(self, data: Path) -> None:
        write_capture(
            data,
            LIVE,
            package_slug=PKG_SLUG,
            package_id=PKG_ID,
            resource_id=LIVE_ID,
            resource_name="Embedded Solar and Wind Forecast",
            body=LIVE_FIXTURE.read_bytes(),
            written_at=_utc(2026, 10, 8, 7, 30),
            ckan_last_modified="2026-10-08T07:25:04",
            resource_filename=self.LIVE_FILENAME,
            partition=DAY,
        )

    def _archives(self, data: Path) -> None:
        _capture(
            data, ID_2019, _fixture("upload_2019_tail.csv"), written=W_2019, last_modified=LM_2019
        )
        _capture(data, ID_2024, _fixture("upload_2024.csv"), written=W_2024, last_modified=LM_2024)
        _capture(
            data, ID_2025, _fixture("upload_2025_head.csv"), written=W_2025, last_modified=LM_2025
        )
        _dump_capture(data, _fixture("dump_2026.csv"))

    def test_a_functional_isolation(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Detects an archive body reaching the bespoke reader or its provenance
        site, or changing the bespoke output, when both live on one partition date."""
        seen: list[Path] = []
        real_read = bespoke_module.read_csv_bronze_body
        real_provenance = bespoke_module.provenance_for

        def spy_read(raw: bytes, **kwargs: Any) -> pl.DataFrame:
            seen.append(Path(kwargs["source_label"]))
            return real_read(raw, **kwargs)

        def spy_provenance(raw_path: Path) -> Any:
            seen.append(Path(raw_path))
            return real_provenance(raw_path)

        monkeypatch.setattr(bespoke_module, "read_csv_bronze_body", spy_read)
        monkeypatch.setattr(bespoke_module, "provenance_for", spy_provenance)

        self._live(data)
        self._archives(data)
        EmbeddedWindSolarForecastTransformer(data).run(DAY, run_id="r")
        assert seen
        live_dir = PathBuilder(data).bronze_dir(SOURCE, LIVE)
        assert all(path.is_relative_to(live_dir) for path in seen), seen
        mixed = snapshot(data, LIVE)
        assert mixed["outputs"]

        with tempfile.TemporaryDirectory(prefix="ef", ignore_cleanup_errors=True) as other:
            alone = Path(other)
            self._live(alone)
            EmbeddedWindSolarForecastTransformer(alone).run(DAY, run_id="r")
            assert_same_output(
                mixed,
                snapshot(alone, LIVE),
                EmbeddedWindSolarForecastTransformer.ENTITY_KEY_COLUMNS,
            )

    def test_b_the_bespoke_reads_no_sibling(self) -> None:
        """Detects a sibling bronze directory added to the bespoke's read surface."""
        assert EmbeddedWindSolarForecastTransformer.BRONZE_SIBLING_DATASETS == ()

    def test_c_registry_routes_only_the_live_resource_to_the_bespoke(self) -> None:
        """Detects an archive resource disposed into the bespoke family, a record
        reading the bespoke's directory, or the legacy selector widened."""
        code = (
            _CONSTANTS
            + """
from gridflow.connectors.neso_data_portal.registry import load_registry

registry = load_registry()
into_live = sorted(
    rid for rid, (_p, r) in registry.resources.items()
    if r.disposition.kind == "SILVER" and r.disposition.key == LIVE
)
assert into_live == [LIVE_ID], into_live
readers = sorted(
    key for key, (_p, f) in registry.families.items()
    if f.record is not None and LIVE in f.record.siblings
)
assert readers == [], readers
names = registry.family_names(LIVE)
assert names == frozenset({("Embedded Solar and Wind Forecast", "CSV")}), names
print("OK")
"""
        )
        _assert_ok(_run(code))

    def test_d_the_issue_token_matches_live_files_only(self) -> None:
        """16b: detects the live token pattern matching an archive filename, or no
        longer matching the current and newer live filenames."""
        assert _ISSUE_TOKEN_PATTERN.match("202610061925_embedded_forecast.csv")
        assert _ISSUE_TOKEN_PATTERN.match("202610080725_embedded_forecast.csv")
        assert not _ISSUE_TOKEN_PATTERN.match("embedded_archive_2025.csv")


def test_ef6_vintage_guards_hold_under_the_owners(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FM-4: detects a datastore capture dated by the upload record's
    ``last_modified`` vintage, or an upload sidecar's ``last_modified`` leaking
    into the dump record's clock."""
    registry, _generated = _install(monkeypatch, data)
    dump_id = _dump_capture(data, _fixture("dump_2026.csv"))
    upload_id = _capture(
        data, ID_2024, _fixture("upload_2024.csv"), written=W_2024, last_modified=LM_2024
    )
    with pytest.raises(CaptureContextError, match="datastore"):
        capture_context(
            _scan_capture(data, registry, dump_id), _record(registry, UPLOAD_OWNER), data
        )
    context = capture_context(
        _scan_capture(data, registry, upload_id), _record(registry, DUMP_OWNER), data
    )
    assert context.published_at is None and context.url_type == "upload"


def _query(db: Path, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, params).pl() if params else con.execute(sql).pl()
    finally:
        con.close()


def _ids(frame: pl.DataFrame) -> list[str]:
    return sorted(frame["bronze_capture_id"].to_list())


def _sql_as_of(db: Path, key: str, as_of: datetime | None) -> list[str]:
    view = f"silver_{SOURCE}_{key}"
    if as_of is None:
        return _ids(_query(db, f'SELECT * FROM "{view}_latest"'))
    columns = set(_query(db, f'SELECT * FROM "{view}" LIMIT 0').columns)
    select = latest_select_sql(view, LATEST_VIEW_SPECS[(SOURCE, key)], columns, as_of_param=True)
    assert select is not None
    return _ids(_query(db, select, {"as_of": as_of.isoformat()}))


def _polars_as_of(data: Path, key: str, as_of: datetime | None) -> list[str]:
    files = _outputs(data, key)
    if not files:
        return []
    lf = pl.scan_parquet(files, hive_partitioning=False)
    return _ids(select_latest_vintage(lf, LATEST_VIEW_SPECS[(SOURCE, key)], as_of).collect())


def _both_as_of(data: Path, key: str, as_of: datetime | None) -> list[str]:
    """Both renderers (catalogue SQL and Polars) must agree; return the winning ids."""
    db = data / "cat.duckdb"
    if not db.exists():
        init_catalogue(db, data)
    sql = _sql_as_of(db, key, as_of)
    assert sql == _polars_as_of(data, key, as_of), (key, as_of)
    return sql


def _one_row(value: int, *, zulu: bool) -> bytes:
    z = "Z" if zulu else ""
    time_gmt = "00:30:00" if zulu else "00:30"
    row = (
        f"2026-10-07T00:00:00{z},{time_gmt},2026-10-07T00:00:00{z},1,{value},6417,0,23963,"
        f"2026-10-06T23:12:00{z}"
    )
    return (",".join(H9) + "\n" + row + "\n").encode()


def _t7(hour: int, minute: int = 0) -> datetime:
    return _utc(2026, 10, 7, hour, minute)


class TestEf7LeakageMatrixOnBothOwners:
    """T-EF7 (criterion 4): B's leakage matrix (T-B3-1/3-3) on both owners."""

    def test_upload_owner_correction_never_leaks(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the upload owner's 12:00 correction visible to an as-of 09:00 read,
        or the unparameterised ``_latest`` not returning the correction."""
        _registry, generated = _install(monkeypatch, data)
        original = _capture(
            data,
            ID_2024,
            _one_row(1, zulu=True),
            written=_t7(8, 30),
            last_modified="2026-10-07T08:25:00",
        )
        correction = _capture(
            data,
            ID_2024,
            _one_row(9, zulu=True),
            written=_t7(12),
            last_modified="2026-10-07T11:55:00",
        )
        generated.transformers[UPLOAD_OWNER](data).run(DAY, run_id="r")
        assert scan_completions(data, UPLOAD_OWNER).collect()["published_at"].null_count() == 0
        assert _both_as_of(data, UPLOAD_OWNER, _t7(9)) == [original]
        assert _both_as_of(data, UPLOAD_OWNER, None) == [correction]

    def test_upload_owner_equal_available_at_resolves_by_written_at(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B3-3: detects equal ``available_at`` (one ``last_modified``) resolved by
        anything but the later ``capture_written_at``, or the renderers disagreeing."""
        _registry, generated = _install(monkeypatch, data)
        _capture(
            data,
            ID_2024,
            _one_row(1, zulu=True),
            written=_t7(8, 30),
            last_modified="2026-10-07T08:25:00",
        )
        later = _capture(
            data,
            ID_2024,
            _one_row(2, zulu=True),
            written=_t7(8, 40),
            last_modified="2026-10-07T08:25:00",
        )
        generated.transformers[UPLOAD_OWNER](data).run(DAY, run_id="r")
        assert _both_as_of(data, UPLOAD_OWNER, None) == [later]
        assert _both_as_of(data, UPLOAD_OWNER, _t7(9)) == [later]

    def test_dump_owner_correction_never_leaks(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the dump owner's 12:00 capture visible to an as-of 09:00 read (null
        ``published_at``; ``available_at`` is the written instant)."""
        _registry, generated = _install(monkeypatch, data)
        original = _dump_capture(data, _one_row(1, zulu=False), written=_t7(8, 30))
        correction = _dump_capture(data, _one_row(9, zulu=False), written=_t7(12))
        generated.transformers[DUMP_OWNER](data).run(DAY, run_id="r")
        completions = scan_completions(data, DUMP_OWNER).collect()
        assert completions["published_at"].null_count() == 2
        assert _both_as_of(data, DUMP_OWNER, _t7(9)) == [original]
        assert _both_as_of(data, DUMP_OWNER, None) == [correction]

    def test_upload_publication_boundary(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Detects an as-of read before an archive's publication seeing its rows
        (the vault sentence: none of its rows before its ``last_modified``)."""
        _registry, generated = _install(monkeypatch, data)
        capture_id = _capture(
            data,
            ID_2020,
            _one_row(1, zulu=True),
            written=_utc(2026, 10, 8, 10, 36, 54),
            last_modified="2025-05-23T09:32:48.819542",
        )
        generated.transformers[UPLOAD_OWNER](data).run(DAY, run_id="r")
        assert _both_as_of(data, UPLOAD_OWNER, _utc(2025, 5, 23, 9, 32)) == []
        assert _both_as_of(data, UPLOAD_OWNER, _utc(2025, 5, 23, 9, 33)) == [capture_id]

    def test_dump_ignores_last_modified(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Detects the dump dated by its sidecar's (metadata) ``last_modified``, which
        would leak it into an as-of read before it was captured."""
        _registry, generated = _install(monkeypatch, data)
        capture_id = _capture(
            data,
            DUMP_ID,
            _one_row(1, zulu=False),
            written=_utc(2026, 10, 8, 10, 52, 46),
            last_modified="2026-07-28T16:19:45.553879",
            url_type="datastore",
        )
        generated.transformers[DUMP_OWNER](data).run(DAY, run_id="r")
        assert _both_as_of(data, DUMP_OWNER, _utc(2026, 8, 1)) == []
        assert _both_as_of(data, DUMP_OWNER, _utc(2026, 10, 8, 11)) == [capture_id]


def test_ef9_reconcile_and_drain_close_the_archive(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FM-1: detects an archive capture reconcile cannot see as missing, a drain that
    leaves a gap, a lost output not reported or not restored equal (B7), or a repeat
    drain that changes anything; the held 2019 capture is never a gap."""
    registry, _generated = _install(monkeypatch, data)
    owners = [UPLOAD_OWNER, DUMP_OWNER]
    c2024 = _capture(
        data, ID_2024, _fixture("upload_2024.csv"), written=W_2024, last_modified=LM_2024
    )
    c2025 = _capture(
        data, ID_2025, _fixture("upload_2025_head.csv"), written=W_2025, last_modified=LM_2025
    )
    cdump = _dump_capture(data, _fixture("dump_2026.csv"))
    _capture(data, ID_2019, _fixture("upload_2019_tail.csv"), written=W_2019, last_modified=LM_2019)

    before = reconcile(data, registry, owners, DAY)
    assert sorted((g.category, g.capture_id) for g in before.gaps) == sorted(
        [("missing", c2024), ("missing", c2025), ("missing", cdump)]
    ), before.lines()

    first = drain(data, registry, owners, DAY, lambda: None)
    assert first.clean, first.lines()
    drained = {key: snapshot(data, key) for key in owners}
    assert len(drained[UPLOAD_OWNER]["outputs"]) == 2 and len(drained[DUMP_OWNER]["outputs"]) == 1

    completion = read_completion(data, UPLOAD_OWNER, c2024)
    assert completion is not None
    (data / completion["output_path"]).unlink()
    lost = reconcile(data, registry, owners, DAY)
    assert [(g.category, g.capture_id) for g in lost.gaps] == [
        ("missing_or_invalid_output", c2024)
    ], lost.lines()
    restored = drain(data, registry, owners, DAY, lambda: None)
    assert restored.clean, restored.lines()
    assert_same_output(drained[UPLOAD_OWNER], snapshot(data, UPLOAD_OWNER), ENTITY_KEY)

    again = drain(data, registry, owners, DAY, lambda: None)
    assert again.clean and again.drained == ()
    for key in owners:
        assert_same_output(drained[key], snapshot(data, key), ENTITY_KEY)


_POLARS_TYPES: dict[str, Any] = {
    "VARCHAR": pl.Utf8,
    "DATE": pl.Date,
    "BIGINT": pl.Int64,
    "DOUBLE": pl.Float64,
}


def test_ef10_owner_outputs_are_typed_and_manifested(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects an owner output whose columns or types drift from its record's
    ``output_columns`` (``source_file`` on the upload owner only), a missing base or
    ``_latest`` view, or a ``_latest`` that is not one row per target."""
    registry, generated = _install(monkeypatch, data)
    b2024 = _fixture("upload_2024.csv")
    b2025 = _fixture("upload_2025_head.csv")
    _capture(data, ID_2024, b2024, written=W_2024, last_modified=LM_2024)
    _capture(data, ID_2025, b2025, written=W_2025, last_modified=LM_2025)
    _dump_capture(data, _fixture("dump_2026.csv"))
    for key in (UPLOAD_OWNER, DUMP_OWNER):
        generated.transformers[key](data).run(DAY, run_id="r")

    for key in (UPLOAD_OWNER, DUMP_OWNER):
        expected = [
            (name, kind)
            for name, kind in output_columns(_record(registry, key))
            if name not in ("year", "month")
        ]
        has_source_file = ("source_file", "VARCHAR") in expected
        assert has_source_file is (key == UPLOAD_OWNER), key
        for path in _outputs(data, key):
            frame = pl.read_parquet(path, hive_partitioning=False)
            assert [c for c in frame.columns if c not in ("year", "month")] == [
                name for name, _kind in expected
            ], (key, path)
            for name, kind in expected:
                dtype = frame.schema[name]
                if kind == "TIMESTAMPTZ":
                    assert isinstance(dtype, pl.Datetime) and dtype.time_zone is not None, name
                else:
                    assert dtype == _POLARS_TYPES[kind], (key, name, dtype)
            assert frame.schema["settlement_date"] == pl.Date

    db = data / "cat.duckdb"
    init_catalogue(db, data)
    for key in (UPLOAD_OWNER, DUMP_OWNER):
        for suffix in ("", "_latest"):
            (count,) = _query(db, f'SELECT count(*) AS n FROM "silver_{SOURCE}_{key}{suffix}"').row(
                0
            )
            assert count > 0, (key, suffix)
    latest = _query(db, f'SELECT * FROM "silver_{SOURCE}_{UPLOAD_OWNER}_latest"')
    targets = _targets(b2024) | _targets(b2025)
    assert latest.height == len(targets)
    assert latest.select("settlement_date", "settlement_period").unique().height == latest.height
    with_source = latest.filter(pl.col("source_file").is_not_null())
    assert set(with_source["settlement_date"].to_list()) == {date(2025, 1, 1)}
    assert with_source.height == len(_targets(b2025))
    assert latest.filter(pl.col("settlement_date") == date(2025, 1, 1)).height == with_source.height
