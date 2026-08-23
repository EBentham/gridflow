"""Retry predicate + error-description pins (v1.7 P0-1).

The Open-Meteo ERA5 archive backfill lost ~half its chunks to
``httpx.ConnectError('')``: the retry predicate matched timeouts only, so a
dropped TLS handshake failed the dataset on its first attempt, and the empty
``str(exc)`` meant the logged failure named neither the cause nor its class.
"""

from __future__ import annotations

import httpx
import pytest

from gridflow.pipeline.runner import describe_exception
from gridflow.utils.retry import RETRY_POLICY


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError(""),
        httpx.ConnectTimeout(""),
        httpx.ReadError(""),
        httpx.ReadTimeout(""),
        httpx.WriteError(""),
        httpx.HTTPStatusError(
            "429", request=httpx.Request("GET", "http://x"), response=httpx.Response(429)
        ),
    ],
)
def test_transient_transport_failures_are_retried(exc: httpx.HTTPError) -> None:
    attempts = 0

    @RETRY_POLICY
    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise exc
        return "ok"

    assert flaky() == "ok"
    assert attempts == 2


@pytest.mark.parametrize(
    "exc",
    [httpx.DecodingError("bad body"), httpx.InvalidURL("nope"), ValueError("deterministic")],
)
def test_deterministic_failures_are_not_retried(exc: Exception) -> None:
    attempts = 0

    @RETRY_POLICY
    def always_bad() -> str:
        nonlocal attempts
        attempts += 1
        raise exc

    with pytest.raises(type(exc)):
        always_bad()
    assert attempts == 1


def test_describe_exception_falls_back_to_the_class_name_when_str_is_empty() -> None:
    assert describe_exception(httpx.ConnectError("")) == "ConnectError"


def test_describe_exception_keeps_a_real_message_and_still_redacts() -> None:
    described = describe_exception(RuntimeError("boom at ?securityToken=hunter2&x=1"))
    assert "hunter2" not in described
    assert described.startswith("boom at ")
