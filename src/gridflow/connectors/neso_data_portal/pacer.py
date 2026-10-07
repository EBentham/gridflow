"""Run-scoped NESO request pacing: two lanes, one process lock (ADR-033 P-12, A8).

Before ADR-033 each connector instance paced only itself and ``run_ingest``
builds one instance per dataset, so the 1 req/s interval reset at every
dataset handoff. :class:`RunPacer` is shared process-wide instead, and a
bound pacer holds an OS lock for the life of the process.

**Invariant I-P.**

- *(a) in process:* consecutive admissions on a lane are at least its interval
  apart in monotonic time, however late a wake is: the admission instant is
  read after the wait, the check-and-set is under a ``threading.Lock`` (each
  dataset runs its own ``asyncio.run``), and nothing runs between the clock
  read and the return.
- *(b) across processes:* a bound process takes the OS lock in :meth:`bind`,
  before its first send, and holds it until it exits, after its last send. The
  next process can take the lock only after that exit, and its first admission
  on each lane is at least one interval after its own lock acquisition. No
  clock value crosses the process boundary and nothing is persisted, so there
  is no state file to be stale, absent or half-written.
- *(c) admission is the send:* the connector awaits the transport directly
  after :meth:`RunPacer.acquire`.

An unbound pacer (direct construction in a test or a notebook) takes no lock
and paces within its process only (accepted residual FM-15).
"""

from __future__ import annotations

import asyncio
import math
import sys
import threading
import time
from enum import StrEnum
from pathlib import Path
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from gridflow.config.settings import SourceConfig

__all__ = [
    "DATASTORE_INTERVAL_S",
    "LOCK_FILENAME",
    "Lane",
    "NesoPacerBusyError",
    "RunPacer",
    "shared_pacer",
]

DATASTORE_INTERVAL_S = 30.0
"""NESO's datastore guidance is 2 requests/minute. Unit D applies this lane."""

LOCK_FILENAME = "pacer.lock"


class Lane(StrEnum):
    """An independently paced request class."""

    CKAN = "ckan"
    DATASTORE = "datastore"


class NesoPacerBusyError(RuntimeError):
    """Another bound NESO process holds the pacer lock; nothing was sent."""


def _try_lock(handle: IO[bytes]) -> bool:
    """Take a non-blocking exclusive lock on ``handle``; ``False`` if it is held."""
    handle.seek(0)
    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock(handle: IO[bytes]) -> None:
    handle.seek(0)
    if sys.platform == "win32":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class RunPacer:
    """Paces every NESO send in this process, per lane.

    Args:
        ckan_interval: Minimum seconds between CKAN-lane admissions. ``0``
            admits immediately.
        datastore_interval: Minimum seconds between datastore-lane admissions.
        monotonic: The clock (injectable for tests).
        sleep: The async sleep (injectable for tests).
        clock_resolution: The clock's tick. Two readings ``interval`` apart on
            a quantised clock can be up to one tick less than ``interval``
            apart in real time, so every non-zero interval is widened by one
            tick. :func:`shared_pacer` passes the real clock's resolution
            (15.6 ms on Windows, 1 ns on Linux); an injected fake clock is exact.
    """

    def __init__(
        self,
        ckan_interval: float,
        datastore_interval: float = DATASTORE_INTERVAL_S,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock_resolution: float = 0.0,
    ) -> None:
        self._interval = {Lane.CKAN: ckan_interval, Lane.DATASTORE: datastore_interval}
        self._gap = {
            lane: interval + clock_resolution if interval > 0 else 0.0
            for lane, interval in self._interval.items()
        }
        self._last_admit = {Lane.CKAN: -math.inf, Lane.DATASTORE: -math.inf}
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._state_dir: Path | None = None
        # Owned for the process lifetime: closing or collecting it drops the lock.
        self._lock_handle: IO[bytes] | None = None

    @property
    def state_dir(self) -> Path | None:
        """The directory whose ``pacer.lock`` this pacer holds, or ``None`` if unbound."""
        return self._state_dir

    def interval(self, lane: Lane) -> float:
        """The lane's minimum admission interval in seconds."""
        return self._interval[lane]

    async def acquire(self, lane: Lane) -> None:
        """Wait until ``lane`` may send, then record the admission. No file I/O."""
        while True:
            with self._lock:
                now = self._monotonic()
                due = self._last_admit[lane] + self._gap[lane]
                if now >= due:
                    self._last_admit[lane] = now
                    return
                wait = due - now
            await self._sleep(wait)

    def bind(self, state_dir: Path) -> None:
        """Take the cross-process lock on ``state_dir/pacer.lock`` (first call only).

        Every lane's next admission is anchored at least one interval after the
        lock is held, so this process's first send follows the previous bound
        process's last send by at least an interval (I-P(b)).

        Raises:
            NesoPacerBusyError: Another bound process holds the lock.
            RuntimeError: This pacer is already bound to a different directory.
        """
        state_dir = Path(state_dir)
        if self._state_dir is not None:
            if self._state_dir == state_dir:
                return
            raise RuntimeError(
                f"pacer already bound to {self._state_dir}; refusing to rebind to {state_dir}"
            )
        state_dir.mkdir(parents=True, exist_ok=True)
        handle = open(state_dir / LOCK_FILENAME, "a+b")  # noqa: SIM115 — held for the process
        if not _try_lock(handle):
            handle.close()
            raise NesoPacerBusyError(
                f"another NESO process holds {state_dir / LOCK_FILENAME}; refusing to send "
                "concurrently with it (ADR-033 I-P)"
            )
        self._lock_handle = handle
        self._state_dir = state_dir
        self._anchor_after_lock()

    def _anchor_after_lock(self) -> None:
        """Start every lane's interval at the lock acquisition (clock read after it)."""
        with self._lock:
            anchor = self._monotonic()
            for lane in self._last_admit:
                self._last_admit[lane] = max(self._last_admit[lane], anchor)

    def close(self) -> None:
        """Release the OS lock. **Tests only**: production holds it until exit."""
        self._state_dir = None
        handle, self._lock_handle = self._lock_handle, None
        if handle is not None:
            try:
                _unlock(handle)
            finally:
                handle.close()


_SHARED: dict[tuple[str, float, Path | None], RunPacer] = {}
_SHARED_LOCK = threading.Lock()


def shared_pacer(config: SourceConfig, state_dir: Path | None = None) -> RunPacer:
    """Return the process-wide pacer for ``config``; bound when ``state_dir`` is given.

    One object per ``(base_url, ckan_interval, state_dir)``, so every connector
    instance in the process — one per dataset under ``run_ingest`` — shares it.

    Raises:
        ValueError: ``rate_limit_per_second <= 0`` (it would never admit a send).
        NesoPacerBusyError: Binding found the lock held by another process.
    """
    rate = config.rate_limit_per_second
    if rate <= 0:
        raise ValueError(
            f"neso_data_portal rate_limit_per_second must be > 0, got {rate}; a "
            "non-positive rate can never admit a send"
        )
    interval = 1.0 / rate
    key = (config.base_url, interval, None if state_dir is None else Path(state_dir))
    with _SHARED_LOCK:
        pacer = _SHARED.get(key)
        if pacer is None:
            pacer = RunPacer(interval, clock_resolution=time.get_clock_info("monotonic").resolution)
            _SHARED[key] = pacer
    if state_dir is not None:
        pacer.bind(Path(state_dir))
    return pacer


def reset_shared_pacers() -> None:
    """Release every cached pacer's lock and forget them. **Tests only.**"""
    with _SHARED_LOCK:
        pacers = list(_SHARED.values())
        _SHARED.clear()
    for pacer in pacers:
        pacer.close()
