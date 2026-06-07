"""Retry, backoff, and circuit breaker helpers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)

RETRY_POLICY = retry(
    stop=stop_after_attempt(5),
    # Jittered backoff (random in [0, exp]) de-syncs concurrent retries: CH3-03
    # made connector fetches concurrent, so on a 429/5xx storm N units fail at
    # the same instant. A fixed wait_exponential would retry them in lockstep —
    # a thundering herd amplifying load on the struggling API. The random spread
    # staggers the retries instead.
    wait=wait_random_exponential(multiplier=1, max=60),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


class CircuitBreaker:
    """Simple circuit breaker: closed -> open -> half_open.

    After `failure_threshold` consecutive failures, enters open state
    for `cooldown_seconds`. Prevents hammering a down API.
    """

    def __init__(self, failure_threshold: int = 10, cooldown_seconds: int = 300):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._failures = 0
        self._state = "closed"  # closed | open | half_open
        self._opened_at: datetime | None = None

    @property
    def state(self) -> str:
        return self._state

    def record_success(self) -> None:
        self._failures = 0
        self._state = "closed"

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._state = "open"
            self._opened_at = datetime.now(UTC)

    def can_execute(self) -> bool:
        if self._state == "closed":
            return True
        if self._state == "open":
            if self._opened_at is None:
                return True
            elapsed = (datetime.now(UTC) - self._opened_at).total_seconds()
            if elapsed >= self.cooldown_seconds:
                self._state = "half_open"
                return True
            return False
        return True  # half_open: allow one attempt
