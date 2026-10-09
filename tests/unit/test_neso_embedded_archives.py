"""The embedded wind and solar forecast archives (v0.22-EF, ADR-038).

The archive family ``embedded_wind_solar_forecast_archive`` stays the recordless
bronze home; two sibling-fed owners carry the records: the yearly uploads
(``embedded_forecast_archive_upload``, two header epochs) and the 2026 datastore
dump (``embedded_forecast_archive_dump``). The truncated 2019 upload is held.

Registry and wiring assertions run out of process (the
``test_neso_registry.py`` ``_run``/``_assert_ok`` idiom), because pytest
collection has already imported the connector and the transformers.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ARCHIVE = "embedded_wind_solar_forecast_archive"
LIVE = "embedded_wind_solar_forecast"
UPLOAD_OWNER = "embedded_forecast_archive_upload"
DUMP_OWNER = "embedded_forecast_archive_dump"

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
DUMP_ID = "31861619-0b86-47ba-bac2-d008a760af54"


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
