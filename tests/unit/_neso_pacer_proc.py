"""A NESO connector process for the cross-process pacing tests (ADR-033 Q-3, Q-6).

Run as a script, never imported by pytest collection (no ``test_`` prefix)::

    python _neso_pacer_proc.py A <data_dir>      # three sends, stalls inside the third
    python _neso_pacer_proc.py B <data_dir>      # waits for "go" on stdin, binds, sends once
    python _neso_pacer_proc.py busy <data_dir>   # binds while another process holds the lock

With ``NESO_PACER_LANE=datastore`` (ADR-035 T-D2-6) both roles pace the
datastore lane on a short 2 s interval: A sends three dumps, B one
``datastore_search`` call.

Every line it prints is ``<TAG> <value>``. HTTP is mocked with respx and the
resolver is stubbed, so nothing leaves the machine.
"""

from __future__ import annotations

import asyncio
import ipaddress
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import respx

from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.neso_data_portal import client as client_module
from gridflow.connectors.neso_data_portal import pacer as pacer_module
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector
from gridflow.connectors.neso_data_portal.pacer import NesoPacerBusyError, RunPacer

BASE_URL = "https://api.neso.energy"
DUMP_RID = "aaaaaaaa-0000-4000-8000-000000000010"
DATASTORE_INTERVAL = 2.0


def emit(tag: str, value: object) -> None:
    print(f"{tag} {value}", flush=True)


async def _stub_resolver(host: str, port: int) -> list[Any]:
    return [ipaddress.ip_address("93.184.216.34")]


def _config() -> SourceConfig:
    return SourceConfig(
        base_url=BASE_URL,
        rate_limit_per_second=1,
        datasets={"daily_wind_availability": DatasetConfig(endpoint="/api/3/action/package_show")},
    )


def _payload() -> dict[str, Any]:
    return {
        "success": True,
        "result": {"id": "p", "name": "daily-wind-availability", "records": [], "limit": 0},
    }


def main() -> int:
    role, data_dir = sys.argv[1], Path(sys.argv[2])
    datastore = os.environ.get("NESO_PACER_LANE") == "datastore"
    client_module._resolve_host_addresses = _stub_resolver  # type: ignore[assignment]
    sends: list[float] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        sends.append(time.time())
        emit("SEND", repr(sends[-1]))
        if role == "A" and len(sends) == 3:
            time.sleep(5)  # killed in here, mid-send, still holding the lock
        return httpx.Response(200, json=_payload())

    if role == "A":
        resolution = time.get_clock_info("monotonic").resolution

        async def _oversleep(seconds: float) -> None:
            await asyncio.sleep(seconds + 0.5)

        def _shared(config: SourceConfig, state_dir: Path | None = None) -> RunPacer:
            pacer = RunPacer(1.0, DATASTORE_INTERVAL, sleep=_oversleep, clock_resolution=resolution)
            if state_dir is not None:
                pacer.bind(state_dir)
            return pacer

        pacer_module.shared_pacer = _shared  # type: ignore[assignment]
    elif role == "B":
        if datastore:
            b_resolution = time.get_clock_info("monotonic").resolution

            def _shared_b(config: SourceConfig, state_dir: Path | None = None) -> RunPacer:
                pacer = RunPacer(1.0, DATASTORE_INTERVAL, clock_resolution=b_resolution)
                if state_dir is not None:
                    pacer.bind(state_dir)
                return pacer

            pacer_module.shared_pacer = _shared_b  # type: ignore[assignment]
        emit("READY", 1)
        if sys.stdin.readline().strip() != "go":
            return 2
        real_try_lock = pacer_module._try_lock

        def _recording_try_lock(handle: Any) -> bool:
            locked = real_try_lock(handle)
            if locked:
                emit("LOCK", repr(time.monotonic()))
            return locked

        pacer_module._try_lock = _recording_try_lock  # type: ignore[assignment]
        real_acquire = RunPacer.acquire

        async def _recording_acquire(self: RunPacer, lane: pacer_module.Lane) -> None:
            await real_acquire(self, lane)
            emit("ADMIT", repr(time.monotonic()))

        RunPacer.acquire = _recording_acquire  # type: ignore[method-assign]
        if os.environ.get("NESO_PACER_NO_ANCHOR") == "1":
            RunPacer._anchor_after_lock = lambda self: None  # type: ignore[method-assign]

    async def _run() -> None:
        connector = NesoDataPortalConnector(_config())
        try:
            connector.bind_data_dir(data_dir)
        except NesoPacerBusyError as exc:
            emit("BUSY", type(exc).__name__)
            return
        emit("BOUND", 1)
        async with connector:
            for _ in range(3 if role == "A" else 1):
                if not datastore:
                    await connector._package_show("daily-wind-availability")
                elif role == "A":
                    await connector._download_dump(DUMP_RID, 1 << 20, "daily_wind_availability")
                else:
                    await connector.datastore_fields(DUMP_RID)

    with respx.mock(assert_all_called=False) as router:
        router.route(url__regex=r".*").mock(side_effect=_handler)
        asyncio.run(_run())
        emit("CALLS", len(router.calls))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
