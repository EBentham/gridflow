"""NESO Data Portal connector test helpers: a legacy capture and a ``_send`` spy.

Not a test module (no ``test_`` prefix).

``run_ingest`` captures every ``neso_data_portal`` dataset through
:meth:`NesoDataPortalConnector.iter_members`, which turns a member's failure into a
``failed`` event carrying only text. The transport, redirect and admission tests
assert the typed error, so :func:`capture_legacy` runs the steps ``iter_members``
takes for a legacy key's one upload member (window screen, ``package_show``,
exact-name selection, then ``_capture_member``) without the bronze skip or the
event wrapper.

:func:`record_sends` and :func:`unsent` prove the single-send-primitive invariant
(D-39 §1b) at the transport: every request respx records must carry a tag that
``_send`` stamped, each tag once.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector
from gridflow.connectors.neso_data_portal.endpoints import DATASETS, FAMILIES
from gridflow.utils.time import window_dates

if TYPE_CHECKING:
    from datetime import datetime

    import httpx
    import pytest

    from gridflow.connectors.base import RawResponse

_SENT = "test_sent_by_send"


async def capture_legacy(
    connector: NesoDataPortalConnector, dataset: str, start: datetime, end: datetime
) -> list[RawResponse]:
    """Capture ``dataset``'s one member on an entered connector; a one-element list."""
    family = FAMILIES[dataset]
    spec = DATASETS[dataset]
    connector._assert_window_admissible(dataset, start, end)
    payload = await connector._package_show(family.package)
    resource, _redirector = connector._select_resource(payload, spec, dataset)
    response, _extension = await connector._capture_member(
        payload, resource, family, spec, dataset, day=window_dates(start, end)[-1]
    )
    return [response]


def record_sends(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Stamp a fresh tag on every attempt ``_send`` makes; the tags, in order.

    The tag rides ``request.extensions``, which httpx carries to the transport but
    never puts on the wire. respx records a copy of each request, so object
    identity cannot tell which requests came through ``_send``. The stamp sits
    inside ``_send``'s own retry policy (the same ``Retrying`` object), so each
    retried attempt carries its own tag.
    """
    sent: list[int] = []
    retrying = NesoDataPortalConnector._send.retry  # type: ignore[attr-defined]
    attempt = NesoDataPortalConnector._send.__wrapped__  # type: ignore[attr-defined]

    async def _recording(
        self: NesoDataPortalConnector, request: httpx.Request, *args: Any, **kwargs: Any
    ) -> httpx.Response:
        request.extensions[_SENT] = len(sent)
        sent.append(len(sent))
        response: httpx.Response = await attempt(self, request, *args, **kwargs)
        return response

    monkeypatch.setattr(NesoDataPortalConnector, "_send", retrying.wraps(_recording))
    return sent


def unsent(recorded: list[httpx.Request], sent: list[int]) -> list[httpx.Request]:
    """Transport requests not matched one-for-one by a tag ``_send`` stamped."""
    pending = set(sent)
    out = []
    for request in recorded:
        tag = request.extensions.get(_SENT)
        if tag in pending:
            pending.remove(tag)
        else:
            out.append(request)
    return out
