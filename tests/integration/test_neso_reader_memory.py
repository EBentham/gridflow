"""A-MEM (I-2b): the strict UTF-8 gate adds no material peak to the largest NESO CSV read.

The largest NESO CSV body (645,655,728 bytes, 5,369,698 rows on 2026-10-09) is read
through ``read_csv_body`` twice, each in a FRESH interpreter
(``_neso_reader_rss_probe.py``): once with the gate's validator stubbed out, once
with the gate. The gated peak may exceed the stubbed one by at most 2% of the
stubbed peak (a whole-body decode would add the body's size, about 616 MiB).

Skipped unless the body exists under ``GRIDFLOW_REAL_DATA_DIR`` (default
``C:/gridflow-data``) and at least 5 GiB of physical memory is free; the guard is
never lowered. Read-only.

Marked ``slow``: excluded from the local fast gate.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_neso_overlap_memory import _available_physical

PROBE = Path(__file__).resolve().parent / "_neso_reader_rss_probe.py"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BODY = (
    "bronze/neso_data_portal/embedded_wind_solar_forecast_archive/2026/10/09/"
    "raw_20261009T032911Z_fc13df13-2dad-4a1c-b9e3-4569efba4955_8a9e75da.csv"
)
ROWS = 5_369_698
GIB = 1024**3
MIB = 1024**2
FREE_GUARD = 5 * GIB
BOUND = 0.02


def _body() -> Path:
    path = Path(os.environ.get("GRIDFLOW_REAL_DATA_DIR", "C:/gridflow-data")) / BODY
    if not path.is_file():
        pytest.skip(f"no body at {path}")
    return path


def _probe(mode: str, path: Path) -> int:
    free = _available_physical()
    if free < FREE_GUARD:
        pytest.skip(f"only {free} B of physical memory free (guard {FREE_GUARD} B)")
    result = subprocess.run(
        [sys.executable, str(PROBE), mode, str(path)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=3600,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    lines = result.stdout.splitlines()
    assert f"ROWS {ROWS}" in lines, lines[-20:]
    peaks = [int(line.split()[1]) for line in lines if line.startswith("PEAK ")]
    assert len(peaks) == 1, lines[-20:]
    print(f"{mode}: PEAK {peaks[0] / MIB:.1f} MiB")
    return peaks[0]


@pytest.mark.slow
def test_a_mem_the_gate_adds_no_material_peak_on_the_largest_body() -> None:
    """Detects the gate holding a whole-body copy or decode: on the largest real NESO CSV
    both reads return every row, and the gated peak is within 2% of the stubbed one."""
    path = _body()
    stubbed = _probe("stubbed", path)
    gated = _probe("gate", path)
    print(f"difference: {(gated - stubbed) / MIB:+.1f} MiB (bound {stubbed * BOUND / MIB:.1f} MiB)")
    assert gated - stubbed <= stubbed * BOUND, (stubbed, gated)
