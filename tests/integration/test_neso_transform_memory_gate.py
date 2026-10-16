"""B8 memory gate: a NESO generic transform is bounded by one capture (ADR-034 P-16).

Each N runs in a FRESH interpreter (``_neso_transform_rss_probe.py``) so the
peaks are independent. The probe writes N ~75 MiB CSV captures into one bronze
date partition and runs the generic engine over that date; 20 captures must
cost no more than 10 % over 5, and every capture must land as its own output
with every generated row, so the gate measures retention through COMPLETED
transforms, not a run that stopped early.

Both runs first transform the same WARMUP captures of another date. Without
it the gate measured allocator warm-up, not retention: the working set after
each capture is flat (~1.0-1.2 GB from capture 1 to 20), but the process peak
climbs over the first ~6-9 captures and then plateaus, so two no-warm-up runs
of this very engine gave ratios 1.078 and 1.114. The ratio, the two N and the
outputs/rows checks are P-16's; only where the peaks are sampled changed.

Red controls (not committed): the ``perfile`` engine at ``d7cf513``, whose
per-file branch retained each body's frame, peaked at 1.16 GB for 2 captures
and 2.16 GB for 5.

The ZIP test (v0.22-H, ADR-043) runs the same gate through the ``zip_member``
reader: each capture is a deflated one-member ZIP of the same rows.

Marked ``slow``: excluded from the local fast gate, run by CI (``-m "not live"``).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

PROBE = Path(__file__).resolve().parent / "_neso_transform_rss_probe.py"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
WARMUP = 8


def _probe(members: int, data_dir: Path, reader: str = "csv") -> dict[str, Any]:
    result = subprocess.run(
        [
            sys.executable,
            str(PROBE),
            "--members",
            str(members),
            "--engine",
            "generic",
            "--data-dir",
            str(data_dir),
            "--warmup",
            str(WARMUP),
            "--reader",
            reader,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    outcome: dict[str, Any] = json.loads(result.stdout.strip().splitlines()[-1])
    return outcome


@pytest.mark.slow
def test_peak_rss_does_not_grow_with_captures_per_date(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    small_dir = tmp_path_factory.mktemp("m5")
    large_dir = tmp_path_factory.mktemp("m20")
    try:
        small = _probe(5, small_dir)
        large = _probe(20, large_dir)
    finally:
        # ~1.9 GB of synthetic bronze; do not leave it in the retained basetemps.
        shutil.rmtree(small_dir, ignore_errors=True)
        shutil.rmtree(large_dir, ignore_errors=True)
    print(f"generic transform peak RSS: 5 captures {small}, 20 captures {large}")
    for members, outcome in ((5, small), (20, large)):
        assert outcome["outputs"] == members, outcome
        assert outcome["rows"] == outcome["rows_written_to_bronze"], outcome
    assert large["peak_rss"] <= 1.10 * small["peak_rss"], (small, large)


@pytest.mark.slow
def test_peak_rss_flat_for_zip_member_captures(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Detects the ``zip_member`` path retaining a capture's member bytes or frame (H7):
    the same 5-vs-20 gate, every capture a deflated one-member ZIP of the same rows."""
    small_dir = tmp_path_factory.mktemp("z5")
    large_dir = tmp_path_factory.mktemp("z20")
    try:
        small = _probe(5, small_dir, "zip_member")
        large = _probe(20, large_dir, "zip_member")
    finally:
        shutil.rmtree(small_dir, ignore_errors=True)
        shutil.rmtree(large_dir, ignore_errors=True)
    print(f"zip_member transform peak RSS: 5 captures {small}, 20 captures {large}")
    for members, outcome in ((5, small), (20, large)):
        assert outcome["outputs"] == members, outcome
        assert outcome["rows"] == outcome["rows_written_to_bronze"], outcome
    assert large["peak_rss"] <= 1.10 * small["peak_rss"], (small, large)
