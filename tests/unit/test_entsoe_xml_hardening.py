"""XXE-hardening tests for ENTSO-E XML parsing (CH1-02 / CH-SEC-02).

These tests guard against XML External Entity (XXE) resolution. lxml's default
``resolve_entities=True`` would inline the contents of an external file referenced
by a ``<!ENTITY ... SYSTEM ...>`` declaration; the hardened parser must not.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gridflow.connectors.entsoe.client import _extract_acknowledgement_reason
from gridflow.connectors.entsoe.parsers import _hardened_parser

if TYPE_CHECKING:
    from pathlib import Path

_SENTINEL = "XXE-SENTINEL-d41d8cd98f00b204"


def test_external_entity_not_resolved(tmp_path: Path) -> None:
    """An external SYSTEM entity must not be resolved by either parse path.

    Covers the hardened-parser factory directly AND the migrated stdlib site
    ``_extract_acknowledgement_reason`` (which parses untrusted HTTP-error bodies).
    """
    from lxml import etree

    sentinel_file = tmp_path / "sentinel.txt"
    sentinel_file.write_text(_SENTINEL, encoding="utf-8")
    # Path.as_uri() yields a valid file:///C:/... URL on Windows; a raw
    # "file://C:\..." string is malformed and would not resolve even unhardened,
    # making the RED check vacuous.
    file_uri = sentinel_file.as_uri()

    # Direct factory path: entity referenced in element text.
    doc_xml = (
        f'<?xml version="1.0"?><!DOCTYPE foo [<!ENTITY xxe SYSTEM "{file_uri}">]><doc>&xxe;</doc>'
    ).encode()
    root = etree.fromstring(doc_xml, parser=_hardened_parser())
    all_text = "".join(t for t in root.itertext())
    assert _SENTINEL not in all_text

    # Migrated _extract_acknowledgement_reason path: the entity is embedded inside
    # the ack-namespace <text> node so a resolved entity would surface in the
    # returned reason string. With hardening, the text is empty and the sentinel
    # is absent.
    ack_xml = (
        '<?xml version="1.0"?>'
        "<!DOCTYPE Acknowledgement_MarketDocument "
        f'[<!ENTITY xxe SYSTEM "{file_uri}">]>'
        "<Acknowledgement_MarketDocument "
        'xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">'
        "<Reason><code>999</code><text>&xxe;</text></Reason>"
        "</Acknowledgement_MarketDocument>"
    ).encode()
    reason = _extract_acknowledgement_reason(ack_xml)
    assert _SENTINEL not in reason
