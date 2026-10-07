"""A9 memory gate: NESO member ingest is bounded by one body (ADR-033 P-13, G-1).

Each N runs in a FRESH interpreter (``_neso_rss_probe.py``) so the peaks are
independent. On master, ``fetch()`` returned the whole family as a list, so the
peak grew with N (E15); the member branch publishes each capture before the
next download, so 20 members must cost no more than 10 % over 5.

Marked ``slow``: excluded from the local fast gate, run by CI (``-m "not live"``).
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PROBE = Path(__file__).resolve().parent / "_neso_rss_probe.py"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BODY_BYTES = 75 * 1024 * 1024


def _probe(members: int, data_dir: Path) -> int:
    result = subprocess.run(
        [sys.executable, str(PROBE), "--members", str(members), "--data-dir", str(data_dir)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    lines = dict(
        line.split(" ", 1) for line in result.stdout.splitlines() if line[:5] in ("CAPTU", "PEAK ")
    )
    count, sizes = lines["CAPTURES"].split(" ", 1)
    assert int(count) == members, lines
    assert [int(size) for size in sizes.split(",")] == [BODY_BYTES] * members
    return int(lines["PEAK"])


@pytest.mark.slow
def test_peak_rss_does_not_grow_with_family_size(tmp_path_factory: pytest.TempPathFactory) -> None:
    small_dir = tmp_path_factory.mktemp("g5")
    large_dir = tmp_path_factory.mktemp("g20")
    try:
        peak_small = _probe(5, small_dir)
        peak_large = _probe(20, large_dir)
    finally:
        # 1.9 GB of synthetic bronze; do not leave it in the retained basetemps.
        shutil.rmtree(small_dir, ignore_errors=True)
        shutil.rmtree(large_dir, ignore_errors=True)
    print(f"peak RSS: 5 members {peak_small} B, 20 members {peak_large} B")
    assert peak_large <= 1.10 * peak_small, (peak_small, peak_large)
