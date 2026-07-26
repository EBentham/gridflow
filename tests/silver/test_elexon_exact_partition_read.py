"""G-1 gate: every Elexon ``read_bronze`` is exact-partition-only (R2-A Task 2).

Audited in the plan (S1.6): zero Elexon call sites for
``_bronze_path_for_date`` / ``_find_covering_bronze_partition`` /
``_bronze_date_dirs`` — every registered transformer (except
``bmunits_reference``, static reference data with no date dimension) builds
its exact bronze path literally in ``read_bronze()``. This test turns a
future covering-fallback regression into a CI failure (R-9) and is expected
to be GREEN on ``master`` (it pins a pre-existing property, not a R2-A fix).
"""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING

import gridflow.silver.elexon  # noqa: F401 -- registers every elexon transformer
from gridflow.silver.elexon._publication_window import (
    PUBLICATION_WINDOW_EXEMPT,
    publication_window_params,
)
from gridflow.silver.registry import get_transformer, list_transformers
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

_COVERING_FALLBACK_EXEMPT = frozenset({"bmunits_reference"})
"""Static reference data with no date dimension -- reads across all dates by
design (``bmunits.py``'s ``rglob`` over the whole bronze tree), not a G-1
violation."""


def _seed_minimal_partition(bronze_dir: Path) -> None:
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_20260710T000000Z_aaaa1111.json").write_text(json.dumps({"data": []}))


def test_no_elexon_transformer_reads_outside_its_exact_partition(tmp_path: Path) -> None:
    """G-1: seeding bronze at D-1 only must never leak into ``run(D)``."""
    registered = list_transformers("elexon")
    assert registered, "elexon transformers must be registered before this test runs"

    target_date = date(2026, 7, 11)
    prior_date = date(2026, 7, 10)

    checked = 0
    for source, dataset in registered:
        if dataset in _COVERING_FALLBACK_EXEMPT:
            continue
        case_dir = tmp_path / source / dataset
        _seed_minimal_partition(PathBuilder(case_dir).bronze_date_dir(source, dataset, prior_date))
        transformer = get_transformer(source, dataset, case_dir)
        rows = transformer.run(target_date, run_id="g1-gate")
        assert rows == 0, f"{source}/{dataset} leaked rows from a prior-day partition"
        assert not PathBuilder(case_dir).silver_file(source, dataset, target_date).exists()
        checked += 1

    assert checked == len(registered) - len(_COVERING_FALLBACK_EXEMPT)


def test_every_elexon_dataset_is_filtered_or_exempt_with_a_reason() -> None:
    """A-11: 33/33 registered datasets classify as either filtered or exempt.

    ``bod`` is excluded from this loop: it is UNREGISTERED by design
    (decommissioned by Elexon, ``EXCLUDED_ENDPOINTS``) in isolation, but
    other test modules import ``gridflow.silver.elexon.bod`` directly for
    its schema (not through ``elexon.__init__``), which triggers its
    module-level ``register_transformer`` side effect and leaks "bod" into
    the shared registry for the rest of a full-suite run. That leak is a
    pre-existing test-isolation gap unrelated to R2-A (out of scope here) --
    tolerated, not silently masked, by naming it explicitly.
    """
    registered = list_transformers("elexon")
    assert registered

    for _source, dataset in registered:
        if dataset == "bod":
            continue
        in_scope = publication_window_params(dataset) is not None
        exempt = dataset in PUBLICATION_WINDOW_EXEMPT
        assert in_scope or exempt, f"{dataset} is neither filtered nor exempt with a reason"
        assert not (in_scope and exempt), f"{dataset} is both in scope and exempt"

    assert len(PUBLICATION_WINDOW_EXEMPT) == 11
