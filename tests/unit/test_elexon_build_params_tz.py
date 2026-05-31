"""issue-19 site D: ``build_params`` must emit Elexon ``...Z`` params at the
UTC instant, CONVERTING a tz-aware non-UTC ``start``/``end`` rather than
stamping its local wall clock with a false ``Z``.

The existing endpoint-URL tests only ever pass already-UTC reference datetimes,
so every assertion passes whether or not conversion happens (test-efficacy
note). These pin the conversion behaviour under non-UTC input.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from gridflow.connectors.elexon.endpoints import (
    ENDPOINTS,
    ElexonEndpoint,
    ParamStyle,
    build_params,
)


def _publish_datetime_endpoint() -> ElexonEndpoint:
    for endpoint in ENDPOINTS.values():
        if endpoint.param_style == ParamStyle.PUBLISH_DATETIME:
            return endpoint
    raise AssertionError("no PUBLISH_DATETIME endpoint registered")


def test_build_params_converts_non_utc_offset_to_utc_z() -> None:
    """A ``+02:00`` start is emitted as its UTC instant + Z (12:00+02:00 ->
    10:00:00Z), not the local digits with a false Z.

    FAILS against the pre-fix literal-Z strftime, which emitted ``12:00:00Z``.
    """
    endpoint = _publish_datetime_endpoint()
    plus_two = timezone(timedelta(hours=2))
    start = datetime(2024, 1, 15, 12, 0, 0, tzinfo=plus_two)
    end = datetime(2024, 1, 15, 14, 0, 0, tzinfo=plus_two)

    params = build_params(endpoint, start=start, end=end)

    assert params[endpoint.from_param] == "2024-01-15T10:00:00Z"
    assert params[endpoint.to_param] == "2024-01-15T12:00:00Z"


def test_build_params_utc_input_unchanged() -> None:
    """A tz-aware UTC start is emitted unchanged (regression guard)."""
    endpoint = _publish_datetime_endpoint()
    start = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)

    params = build_params(endpoint, start=start)

    assert params[endpoint.from_param] == "2024-01-15T09:30:00Z"
