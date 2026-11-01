"""Every connector verifies vendors against the OS trust store when it can."""

from __future__ import annotations

import ssl
import sys

import certifi
import pytest

from gridflow.connectors.base import _make_ssl_context


@pytest.mark.parametrize("truststore_importable", [True, False], ids=["truststore", "hidden"])
def test_ssl_context_prefers_os_trust_store(
    truststore_importable: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A proxy root that only the OS store holds must verify, as it does in notebooks.

    Without truststore the context falls back to the certifi bundle. The strict
    X.509 flag stays cleared either way, for the ENTSO-E and GIE chains.
    """
    if truststore_importable:
        import truststore

        ctx = _make_ssl_context()

        assert isinstance(ctx, truststore.SSLContext)
    else:
        # None in sys.modules makes `import truststore` raise ImportError.
        monkeypatch.setitem(sys.modules, "truststore", None)

        ctx = _make_ssl_context()

        certifi_only = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        certifi_only.load_verify_locations(certifi.where())
        assert type(ctx) is ssl.SSLContext
        assert set(certifi_only.get_ca_certs(True)) <= set(ctx.get_ca_certs(True))
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname
    assert not ctx.verify_flags & ssl.VERIFY_X509_STRICT
