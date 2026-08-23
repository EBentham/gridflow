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
    [
        httpx.DecodingError("bad body"),
        httpx.InvalidURL("nope"),
        # Both live UNDER httpx.TransportError, which is why the predicate names
        # TimeoutException + NetworkError rather than their common parent: a bad
        # URL scheme and a request we malformed ourselves are deterministic and
        # must not sleep through five attempts (Sol review, 2026-08-23).
        httpx.UnsupportedProtocol("ftp:// is not a thing here"),
        httpx.LocalProtocolError("we built a bad request"),
        ValueError("deterministic"),
    ],
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


@pytest.mark.parametrize("blank", ["", "   ", "\n", " \t\n "])
def test_describe_exception_treats_whitespace_as_empty(blank: str) -> None:
    """A message of "   " is as useless as "" and must reach the fallback."""
    assert describe_exception(httpx.ConnectError(blank)) == "ConnectError"


def test_describe_exception_collapses_a_multiline_message_to_one_line() -> None:
    described = describe_exception(RuntimeError("line one\nline two"))
    assert described == "line one line two"


def test_describe_exception_keeps_a_real_message_and_still_redacts() -> None:
    """Pins the exact result: the secret goes, the surrounding prose stays."""
    described = describe_exception(RuntimeError("boom at ?securityToken=hunter2&x=1"))
    assert "hunter2" not in described
    assert described == "boom at ?securityToken=<redacted>&x=1"


def test_describe_exception_redacts_the_class_name_fallback_too() -> None:
    """The fallback goes through the SAME redaction path as the message.

    A type name is normally a safe constant, but it is not guaranteed to be:
    a dynamically built exception class can carry vendor text, and this value
    is stored in ``pipeline_runs.error_message``. Routing the fallback through
    ``safe_error_message`` rather than trusting it costs nothing (Sol review,
    2026-08-23). The redaction itself is key-anchored on a word boundary —
    that is ``sanitize_url``'s documented contract, not this helper's.
    """
    leaky = type("Boom.securityToken=hunter2", (Exception,), {})
    described = describe_exception(leaky(""))
    assert "hunter2" not in described
    assert described == "Boom.securityToken=<redacted>"
