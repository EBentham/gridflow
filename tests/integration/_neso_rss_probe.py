"""Peak-RSS probe for the bounded NESO member ingest (ADR-033 P-13, A9).

Run in a FRESH interpreter by ``test_neso_ingest_memory_gate.py``; never
collected by pytest (no ``test_`` prefix)::

    python tests/integration/_neso_rss_probe.py --members N --data-dir <tmp>

It installs a synthetic registry holding one upload family of N CSV members,
serves each member as a lazily generated 75 MiB body in 1 MiB chunks, runs the
REAL ``run_ingest`` against a tmp DuckDB catalogue, and prints::

    CAPTURES <n> <bytes of each, comma-separated>
    PEAK <peak resident set size in bytes>

Nothing leaves the machine: HTTP is respx, the resolver is stubbed.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import respx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

MIB = 1024 * 1024
BODY_MIB = 75
PKG = "eeeeeeee-0000-4000-8000-000000000000"
BASE_URL = "https://api.neso.energy"
SOURCE = "neso_data_portal"
FAMILY = "rss_probe_series"


def _rid(n: int) -> str:
    return f"eeeeeeee-0000-4000-8000-{n:012d}"


def peak_rss_bytes() -> int:
    """This process's peak resident set size."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = _Counters()
        counters.cb = ctypes.sizeof(_Counters)
        psapi = ctypes.WinDLL("psapi")
        kernel32 = ctypes.WinDLL("kernel32")
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_Counters),
            wintypes.DWORD,
        ]
        ok = psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        )
        if not ok:
            raise OSError("GetProcessMemoryInfo failed")
        return int(counters.PeakWorkingSetSize)
    import resource

    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


class _LazyCsv(httpx.AsyncByteStream):
    """A 75 MiB CSV body generated chunk by chunk; never held whole."""

    def __init__(self, seed: int) -> None:
        line = f"{seed},2026-10-07T00:00:00Z,123.456,abcdefghijklmnopqrstuvwxyz\n".encode()
        self._chunk = b"ID,TIME,VALUE,TEXT\n" + line * ((MIB - 19) // len(line))
        self._chunk = self._chunk.ljust(MIB, b"\n")

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(BODY_MIB):
            yield bytes(bytearray(self._chunk))

    async def aclose(self) -> None:
        return None


def _install_registry(data_dir: Path, members: int) -> None:
    from gridflow.connectors.neso_data_portal import endpoints
    from gridflow.connectors.neso_data_portal import registry as registry_module

    directory = data_dir / "registry"
    directory.mkdir(parents=True, exist_ok=True)
    document = {
        "package": "rss-probe",
        "package_id": PKG,
        "group": "synthetic",
        "archetype": "SER",
        "refresh": "daily",
        "eligibility": {"status": "eligible"},
        "families": [
            {
                "key": FAMILY,
                "kind": "tabular",
                "legacy": False,
                "archetype": "SER",
                "refresh": "daily",
                "empty_allowed": False,
                "max_download_bytes": 128 * MIB,
                "name_regex": None,
                "transformer": None,
            }
        ],
        "resources": [
            {
                "id": _rid(n),
                "name": f"RSS Probe Member {n}",
                "format": "CSV",
                "url_type": "upload",
                "family": FAMILY,
                "disposition": {"kind": "SILVER", "key": FAMILY},
            }
            for n in range(1, members + 1)
        ],
    }
    (directory / "rss-probe.json").write_text(json.dumps(document), encoding="utf-8")
    (directory / "_frozen_keys.json").write_text("[]", encoding="utf-8")
    (directory / "_adjudications.json").write_text("[]", encoding="utf-8")
    real_load = registry_module.load_registry
    loaded = real_load(directory)
    registry_module.load_registry = lambda path=None: (  # type: ignore[assignment]
        loaded if path is None else real_load(path)
    )
    endpoints.FAMILIES = endpoints.build_families(loaded)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--members", type=int, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()
    data_dir: Path = args.data_dir
    for layer in ("bronze", "silver", "gold"):
        (data_dir / layer).mkdir(parents=True, exist_ok=True)
    os.environ["GRIDFLOW_DATA_DIR"] = str(data_dir)
    os.environ["GRIDFLOW_DUCKDB_PATH"] = str(data_dir / "gridflow.duckdb")
    os.environ["GRIDFLOW_LOG_DIR"] = str(data_dir / "logs")

    from gridflow.config.settings import load_settings
    from gridflow.connectors.neso_data_portal import client as client_module
    from gridflow.connectors.neso_data_portal import pacer as pacer_module
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage import duckdb as duckdb_module
    from gridflow.storage.duckdb import get_connection, init_catalogue

    _install_registry(data_dir, args.members)

    async def _stub_resolver(host: str, port: int) -> list[Any]:
        return [ipaddress.ip_address("93.184.216.34")]

    client_module._resolve_host_addresses = _stub_resolver  # type: ignore[assignment]
    # Zero-interval pacing, through the real seam. The config rate stays at 1
    # because the connector's Semaphore(rate) needs >= 1.
    fast = pacer_module.RunPacer(0.0, 0.0)

    def _shared(config: Any, state_dir: Path | None = None) -> pacer_module.RunPacer:
        if state_dir is not None:
            fast.bind(state_dir)
        return fast

    pacer_module.shared_pacer = _shared  # type: ignore[assignment]
    duckdb_module._register_gold_views = lambda con: None  # type: ignore[assignment]

    live = [
        {
            "id": _rid(n),
            "name": f"RSS Probe Member {n}",
            "format": "CSV",
            "url_type": "upload",
            "last_modified": "2026-10-07T00:00:00.000001",
            "url": f"{BASE_URL}/dataset/{PKG}/resource/{_rid(n)}/download/m{n}.csv",
        }
        for n in range(1, args.members + 1)
    ]

    def _handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "package_show" in url:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "result": {"id": PKG, "name": "rss-probe", "resources": live},
                },
            )
        for n, item in enumerate(live, start=1):
            if url == item["url"]:
                return httpx.Response(
                    200,
                    headers={"content-length": str(BODY_MIB * MIB)},
                    stream=_LazyCsv(n),
                )
        return httpx.Response(404)

    settings = load_settings()
    init_catalogue(data_dir / "gridflow.duckdb", data_dir)
    con = get_connection(data_dir / "gridflow.duckdb")
    end = datetime.now(UTC) - timedelta(minutes=1)
    try:
        with respx.mock(assert_all_called=False) as router:
            router.route(url__regex=r".*").mock(side_effect=_handler)
            ctx = pipeline_runner.PipelineContext(con=con, settings=settings)
            (result,) = pipeline_runner.run_ingest(
                ctx, SOURCE, [FAMILY], end - timedelta(hours=1), end
            )
    finally:
        con.close()
    if result.status != "success":
        print(f"STATUS {result.status} {result.error}", flush=True)
        return 1
    bodies = sorted(p for p in (data_dir / "bronze" / SOURCE / FAMILY).rglob("raw_*.csv"))
    print(f"CAPTURES {len(bodies)} {','.join(str(p.stat().st_size) for p in bodies)}", flush=True)
    print(f"PEAK {peak_rss_bytes()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
