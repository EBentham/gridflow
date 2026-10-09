"""Peak-RSS probe for the memory-bounded NESO overlap check (v0.22-GEN-2H, H7).

Run in a FRESH interpreter by ``test_neso_overlap_memory.py``; never collected by
pytest (no ``test_`` prefix)::

    python tests/integration/_neso_overlap_rss_probe.py overlap <data root>
    python tests/integration/_neso_overlap_rss_probe.py reconcile <data root>

``overlap`` runs the check over ``da_wind_forecast_historic_day_ahead_bmu`` (21.9M
rows on 2026-10-09, the family whose whole-family window segfaulted) at cutoff
2026-10-08 and prints ``GAPS <n>``; ``reconcile`` runs ``reconcile`` over every
family and prints ``GAPS <open> ADJUDICATED <n>``. Both then print
``PEAK <peak resident set size in bytes>``.

Read-only: ``reconcile`` without ``drain`` writes no silver, state, completion or
failure byte (PLAN E16); this probe never calls ``drain``.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _neso_rss_probe import peak_rss_bytes  # noqa: E402

FAMILY = "da_wind_forecast_historic_day_ahead_bmu"
CUTOFF = date(2026, 10, 8)


def main(argv: list[str]) -> int:
    """Run one probe mode; print its result and the peak RSS."""
    mode, root = argv[0], Path(argv[1])
    from gridflow.connectors.neso_data_portal.registry import load_registry
    from gridflow.pipeline.runner import import_transformers
    from gridflow.silver.neso_data_portal import reconcile as reconcile_module

    import_transformers()
    registry = load_registry()
    if mode == "overlap":
        record = registry.families[FAMILY][1].record
        assert record is not None, FAMILY
        gaps = reconcile_module._overlaps(FAMILY, record, root, CUTOFF)
        for gap in gaps:
            print(gap.line())
        print(f"GAPS {len(gaps)}")
    elif mode == "reconcile":
        report = reconcile_module.reconcile(root, registry, None, CUTOFF)
        print(f"GAPS {len(report.gaps)} ADJUDICATED {len(report.adjudicated)}")
    else:
        print(f"unknown mode {mode!r}", file=sys.stderr)
        return 2
    print(f"PEAK {peak_rss_bytes()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
