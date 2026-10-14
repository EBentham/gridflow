"""I-6: the five existing container records' outputs are byte-unchanged by AS-5c (A-4).

Each existing ``xlsx`` / ``zip_member`` record's committed fixture is written as a capture under
a short tmp data root with the sidecar values of that fixture's PROVENANCE, then composed in
memory exactly as the engine composes one capture (``scan_dataset`` -> ``newest_by_resource`` ->
``families_of`` -> ``capture_context`` -> ``read_children`` -> ``type_child`` ->
``finish_capture``). The digest of each output (rows, schema, SHA-256 of the frame without
``source_run_id``, sorted by the entity key, as CSV) is compared with the golden
``as5c/container_pin.json``, which was computed on the untouched base (master ``493f9a6``) before
any ``src/`` change of the unit. Nothing touches ``C:/gridflow-data``.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from _container_support import forbid_zipfile_reads  # noqa: F401 - a fixture
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal.captures import newest_by_resource, scan_dataset
from gridflow.connectors.neso_data_portal.registry import load_registry
from gridflow.silver.neso_data_portal.casting import ExclusionTally, finish_capture, type_child
from gridflow.silver.neso_data_portal.completion import capture_context
from gridflow.silver.neso_data_portal.generic import families_of
from gridflow.silver.neso_data_portal.readers import read_children
from gridflow.storage.paths import PathBuilder

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "neso_data_portal"
GOLDEN = FIXTURES / "as5c" / "container_pin.json"
SOURCE = "neso_data_portal"
DAY = date(2026, 10, 8)


@dataclass(frozen=True)
class _Body:
    """One committed container fixture and its PROVENANCE sidecar values."""

    fixture: str
    dir_key: str
    resource_id: str
    filename: str
    modified: str
    written: datetime
    ckan_format: str
    extension: str


_CMP = _Body(
    "containers/cmp381_ii.xlsx",
    "current_bsuos_files",
    "88dcf101-1358-4d78-934f-527484d8789d",
    "cmp381-current_ii_bsuos_110422.xlsx",
    "2022-04-11T16:08:00.098987",
    datetime(2026, 10, 8, 9, 16, 2, tzinfo=UTC),
    "XLSX",
    "xlsx",
)
_RS = _Body(
    "containers/result_summary.zip",
    "ffr_phase2_auction_files",
    "3928f192-97a5-4b4f-9234-0dcc9ad071a0",
    "resultsummary-2019-11-22-to-2019-11-29.zip",
    "2021-03-31T09:22:26.071477",
    datetime(2026, 10, 8, 10, 58, 33, tzinfo=UTC),
    "ZIP",
    "zip",
)
_X1 = _Body(
    "constraint_mgmt/x1.xlsx",
    "thermal_constraint_costs_files",
    "d195f1d8-7d9e-46f1-96a6-4251e75e9bd0",
    "map-of-outturn-system-costs-19-20.xlsx",
    "2020-06-12T14:41:30.987252",
    datetime(2026, 10, 8, 11, 38, 49, 949763, tzinfo=UTC),
    "XLSX",
    "xlsx",
)
_TRESP = _Body(
    "scn1a/tresp_lists.xlsx",
    "tresp_demand_pathways_files",
    "11c36b60-eee5-45e9-b0da-6240e6e60d1c",
    "lists-of-tresp-pathways-building-blocks-and-of-tresp-gsp-areas-with-names.xlsx",
    "2026-01-30T08:15:29.726307",
    datetime(2026, 10, 8, 11, 39, 46, 157281, tzinfo=UTC),
    "XLSX",
    "xlsx",
)

FAMILIES: dict[str, _Body] = {
    "current_bsuos_cap_adjustments": _CMP,
    "ffr_phase2_result_summary_archive": _RS,
    "thermal_constraint_costs_xlsx": _X1,
    "tresp_building_block_definitions": _TRESP,
    "tresp_gsp_area_names": _TRESP,
}


def digest(family: str) -> dict[str, Any]:
    """The output digest of ``family`` over its committed container fixture.

    Args:
        family: One of the five existing container record keys.

    Returns:
        ``{"rows", "schema", "sha256"}`` of the composed capture output.
    """
    body = FAMILIES[family]
    registry = load_registry()
    package, resource = registry.resources[body.resource_id]
    _package, entry = registry.families[family]
    record = entry.record
    assert record is not None
    with tempfile.TemporaryDirectory(prefix="p", ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        write_capture(
            root,
            body.dir_key,
            package_slug=package.package,
            package_id=package.package_id,
            resource_id=body.resource_id,
            resource_name=resource.name,
            body=(FIXTURES / body.fixture).read_bytes(),
            written_at=body.written,
            ckan_last_modified=body.modified,
            resource_filename=body.filename,
            ckan_format=body.ckan_format,
            extension=body.extension,
            partition=DAY,
        )
        scan = scan_dataset(
            PathBuilder(root).bronze_dir(SOURCE, body.dir_key),
            registry,
            partition=DAY,
            require_provenance=False,
        )
        (capture,) = newest_by_resource(scan.captures).values()
        children = families_of(capture, body.dir_key, registry)[family]
        ctx = capture_context(capture, record, root)
        typed = [type_child(t, record, ctx) for t in read_children(ctx.body, record, children)]
        tally = ExclusionTally()
        for child in typed:
            tally.merge(child.tally)
        frame = finish_capture([child.frame for child in typed], tally, record, ctx, family)
    if "source_run_id" in frame.columns:
        frame = frame.drop("source_run_id")
    frame = frame.sort(list(record.entity_key))
    return {
        "rows": frame.height,
        "schema": [[name, str(dtype)] for name, dtype in frame.schema.items()],
        "sha256": hashlib.sha256(frame.write_csv().encode("utf-8")).hexdigest(),
    }


@pytest.mark.parametrize("family", sorted(FAMILIES))
def test_i6_existing_container_output_is_byte_unchanged(family: str) -> None:
    """Detects any change to an existing container record's silver output (I-6): rows, schema
    and the content hash equal the golden computed on master before AS-5c."""
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert digest(family) == golden[family]
