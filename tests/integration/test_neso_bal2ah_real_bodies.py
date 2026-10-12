"""A5-real: the BAL-2aH edge trim over every real fc_summary body, in memory (v0.22-K-BAL-2aH).

Runs the engine's per-capture composition (``capture_context`` -> ``read_children`` ->
``type_child`` -> ``finish_capture``) over the 75 newest captures of the 2026-10-08 sweep under
``GRIDFLOW_REAL_DATA_DIR`` (default ``C:/gridflow-data``), twice: with the committed record and
with the same record minus the two F12 cost columns' ``trim`` and ``null_tokens``. Nothing is
written: no silver, no completion, no failure record.

Skipped unless the 2026-10-08 bronze date directory exists. Marked ``slow``: excluded from the
local fast gate.
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from gridflow.connectors.neso_data_portal.captures import newest_by_resource, scan_dataset
from gridflow.connectors.neso_data_portal.registry import SchemaRecord, load_registry
from gridflow.silver.neso_data_portal.casting import ExclusionTally, finish_capture, type_child
from gridflow.silver.neso_data_portal.completion import capture_context
from gridflow.silver.neso_data_portal.generic import families_of
from gridflow.silver.neso_data_portal.readers import read_children
from gridflow.storage.paths import PathBuilder

pytestmark = pytest.mark.slow

SOURCE = "neso_data_portal"
FAMILY = "bsuos_monthly_forecast_fc_summary"
DAY = date(2026, 10, 8)
POUND = "£"
WINTER = f"Winter Security of Supply Cost ({POUND}m)"
UNDER = f"21/22 Under-Recovered Costs ({POUND}m)"
COST_COLUMNS = ("winter_security_of_supply_cost_gbp_m", "under_recovered_costs_21_22_gbp_m")
MARCH_TO_MAY = {
    "9120ae56-f4b5-4512-baaa-e20f8b31a014",
    "0bef1780-082d-46ec-90af-62b3d0586061",
    "ebdd84ac-02cb-4552-a427-9b0dc56f5482",
}


def _root() -> Path:
    return Path(os.environ.get("GRIDFLOW_REAL_DATA_DIR", "C:/gridflow-data"))


def _reverted(record: SchemaRecord) -> SchemaRecord:
    """The committed record with the two F12 cost columns' ``trim`` and tokens removed."""
    document: dict[str, Any] = record.model_dump(mode="json")
    for column in document["epochs"][6]["columns"]:
        if column["source"] in (WINTER, UNDER):
            column["trim"] = None
            column["null_tokens"] = []
    return SchemaRecord.model_validate(document)


def _run_all(root: Path, record: SchemaRecord) -> tuple[Counter[str], dict[str, pl.DataFrame]]:
    """Type every newest capture in memory: failures by class and frames by resource id."""
    registry = load_registry()
    scan = scan_dataset(
        PathBuilder(root).bronze_dir(SOURCE, FAMILY),
        registry,
        partition=DAY,
        require_provenance=False,
    )
    assert (len(scan.unusable), len(scan.orphans)) == (0, 0)
    newest = newest_by_resource(scan.captures)
    assert len(newest) == 75
    failures: Counter[str] = Counter()
    frames: dict[str, pl.DataFrame] = {}
    for resource_id, capture in sorted(newest.items()):
        children = families_of(capture, FAMILY, registry)[FAMILY]
        try:
            ctx = capture_context(capture, record, root)
            typed = [
                type_child(table, record, ctx)
                for table in read_children(ctx.body, record, children)
            ]
            tally = ExclusionTally()
            for child in typed:
                tally.merge(child.tally)
            frames[resource_id] = finish_capture(
                [child.frame for child in typed], tally, record, ctx, FAMILY
            )
        except Exception as exc:  # noqa: BLE001 - classify every capture failure, as the engine does
            failures[type(exc).__name__] += 1
    return failures, frames


def test_a5_real_the_trim_loads_march_to_may_2024_and_changes_no_loaded_frame() -> None:
    """Detects the amended record failing a real March-May 2024 body, a dash loaded as anything
    but null, or any of the 50 captures that load today typing differently (I-1e, I-3): with the
    committed record 53 load (1,272 rows, 53 editions, 105 null dash cells) and only the 22
    adjudicated ``UnicodeDecodeError`` captures fail; without the declaration the three fail with
    ``InvalidOperationError`` and the other 50 frames are equal."""
    root = _root()
    bronze_day = PathBuilder(root).bronze_dir(SOURCE, FAMILY) / "2026" / "10" / "08"
    if not bronze_day.is_dir():
        pytest.skip(f"real fc_summary bronze absent: {bronze_day}")
    _package, entry = load_registry().families[FAMILY]
    committed = entry.record
    assert committed is not None

    failures, frames = _run_all(root, committed)
    print(f"committed: failures={dict(failures)} loaded={len(frames)}")
    assert dict(failures) == {"UnicodeDecodeError": 22}
    assert len(frames) == 53
    combined = pl.concat(list(frames.values()), how="diagonal_relaxed")
    assert combined.height == 1272
    assert combined["edition"].n_unique() == 53
    dash_nulls = 0
    for resource_id in MARCH_TO_MAY:
        part = frames[resource_id]
        dash_nulls += sum(part[name].null_count() for name in COST_COLUMNS)
    print(f"committed: rows={combined.height} dash nulls={dash_nulls}")
    assert dash_nulls == 105

    base_failures, base_frames = _run_all(root, _reverted(committed))
    print(f"reverted: failures={dict(base_failures)} loaded={len(base_frames)}")
    assert dict(base_failures) == {"UnicodeDecodeError": 22, "InvalidOperationError": 3}
    assert len(base_frames) == 50
    assert set(base_frames) == set(frames) - MARCH_TO_MAY
    unequal = [rid for rid, frame in base_frames.items() if not frame.equals(frames[rid])]
    assert unequal == []
    print("reverted: 50 of 50 frames equal")
