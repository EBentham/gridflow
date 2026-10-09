"""A8b memory gate: the NESO overlap check completes over the real historic BMU silver (H7).

On 2026-10-09 ``reconcile --all`` segfaulted four times in the overlap check's
whole-family window over ``da_wind_forecast_historic_day_ahead_bmu`` (21,906,814
rows). Each probe mode runs in a FRESH interpreter (``_neso_overlap_rss_probe.py``)
over the real data root, read-only, and must exit 0 under ``PEAK < 3 GiB``.

Skipped unless the family's silver exists under ``GRIDFLOW_REAL_DATA_DIR`` (default
``C:/gridflow-data``) and at least 4 GiB of physical memory is free: a crash still
opens a Windows error dialog, so the guard is never lowered.

Marked ``slow``: excluded from the local fast gate.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

PROBE = Path(__file__).resolve().parent / "_neso_overlap_rss_probe.py"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
FAMILY = "da_wind_forecast_historic_day_ahead_bmu"
GIB = 1024**3
PEAK_BOUND = 3 * GIB
FREE_GUARD = 4 * GIB


def _root() -> Path:
    return Path(os.environ.get("GRIDFLOW_REAL_DATA_DIR", "C:/gridflow-data"))


def _available_physical() -> int:
    """Free physical memory in bytes (``GlobalMemoryStatusEx().ullAvailPhys``)."""
    if sys.platform != "win32":
        return int(os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    import ctypes
    from ctypes import wintypes

    class _Status(ctypes.Structure):
        _fields_ = [
            ("dwLength", wintypes.DWORD),
            ("dwMemoryLoad", wintypes.DWORD),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    status = _Status()
    status.dwLength = ctypes.sizeof(_Status)
    if not ctypes.WinDLL("kernel32").GlobalMemoryStatusEx(ctypes.byref(status)):
        raise OSError("GlobalMemoryStatusEx failed")
    return int(status.ullAvailPhys)


def _probe(mode: str, root: Path) -> tuple[list[str], int]:
    free = _available_physical()
    if free < FREE_GUARD:
        pytest.skip(f"only {free} B of physical memory free (guard {FREE_GUARD} B)")
    result = subprocess.run(
        [sys.executable, str(PROBE), mode, str(root)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=3600,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    lines = result.stdout.splitlines()
    peaks = [int(line.split()[1]) for line in lines if line.startswith("PEAK ")]
    assert len(peaks) == 1, lines[-20:]
    print(f"{mode}: {[line for line in lines if line.startswith('GAPS')]} PEAK {peaks[0]} B")
    return lines, peaks[0]


@pytest.fixture
def root() -> Path:
    """The real data root, or a skip when the historic BMU silver is absent."""
    path = _root()
    if not (path / "silver" / "neso_data_portal" / FAMILY).is_dir():
        pytest.skip(f"no {FAMILY} silver under {path}")
    return path


@pytest.mark.slow
def test_a8b_the_overlap_check_over_the_historic_bmu_family_is_bounded(root: Path) -> None:
    """Detects the whole-family materialisation coming back: the check over 21.9M rows
    exits 0, reports no overlap (one resource) and peaks under 3 GiB."""
    lines, peak = _probe("overlap", root)
    assert "GAPS 0" in lines, lines[-20:]
    assert peak < PEAK_BOUND, peak


@pytest.mark.slow
def test_a8b_reconcile_over_every_family_completes(root: Path) -> None:
    """Detects ``reconcile --all --cutoff 2026-10-08`` failing to complete over the real
    data (the 2026-10-09 segfault); records its peak. Gaps and the peak are reported,
    not asserted: the probe exiting 0 is the claim."""
    lines, _peak = _probe("reconcile", root)
    assert any(line.startswith("GAPS ") for line in lines), lines[-20:]
