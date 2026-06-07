"""Secret sanitization for bronze provenance metadata.

The bronze sidecar (``.meta.json``) is the irreproducible system-of-record: any
API credential written there is a durable on-disk leak. This module masks secret
*values* while preserving the key's presence, satisfying CLAUDE.md's
"validation surfaced, never dropped" rule — the recorded metadata still shows a
``securityToken`` was sent, only its value becomes ``<redacted>``.

This is the single canonical secret-key list; the ENTSO-E client and the CLI
error-message redactors delegate their URL masking to :func:`sanitize_url`.
"""

from __future__ import annotations

import re
from typing import Any

REDACTED = "<redacted>"

# Case-folded EXACT key names (not substrings — substring matching over-masks
# benign params like "tokenType"). Covers ENTSO-E's query token and every
# configured header-auth key (GIE's "x-key"); GIE/header masking is defensive
# since those never reach the request url/params today.
SECRET_KEYS: frozenset[str] = frozenset(
    {
        "securitytoken",
        "api_key",
        "apikey",
        "api-key",
        "x-key",
        "x_key",
    }
)


def sanitize_params(
    obj: Any,
    secret_keys: frozenset[str] = SECRET_KEYS,
) -> Any:
    """Return a copy of ``obj`` with any secret-keyed value replaced by ``<redacted>``.

    Recurses into nested dicts, lists, tuples, and sets, returning the same
    container type. The input is never mutated (``RawResponse`` is frozen), so a
    new structure is always returned. Dict insertion order is preserved, so a
    payload with no secret keys round-trips byte-identically.

    httpx accepts query params as a ``list[tuple[str, str]]``, so a ``(key,
    value)`` pair tuple whose key is secret is masked positionally — the secret
    sits in element ``[1]`` and would otherwise never be seen as a dict key.

    Args:
        obj: An arbitrary JSON-like value (dict, list, tuple, set, or scalar).
        secret_keys: Case-folded exact key names whose values are masked.

    Returns:
        A new structure mirroring ``obj`` with secret values masked.
    """
    if isinstance(obj, dict):
        return {
            key: REDACTED
            if isinstance(key, str) and key.casefold() in secret_keys
            else sanitize_params(value, secret_keys)
            for key, value in obj.items()
        }
    if isinstance(obj, list):
        return [sanitize_params(item, secret_keys) for item in obj]
    if isinstance(obj, tuple):
        # A 2-tuple is the httpx (key, value) param shape: mask positionally when
        # the key is secret, since the value is not reachable as a dict key.
        if len(obj) == 2 and isinstance(obj[0], str) and obj[0].casefold() in secret_keys:
            return (obj[0], REDACTED)
        return tuple(sanitize_params(item, secret_keys) for item in obj)
    if isinstance(obj, (set, frozenset)):
        # Members route through the tuple/scalar branches above; the result is
        # the same set type. Members stay hashable (a redacted pair is a tuple).
        return type(obj)(sanitize_params(item, secret_keys) for item in obj)
    return obj


def sanitize_url(
    url: str,
    secret_keys: frozenset[str] = SECRET_KEYS,
    *,
    value_chars: str = r"[^&]",
) -> str:
    """Mask secret query-parameter values in a URL, preserving the rest verbatim.

    A key-anchored substitution replaces ``<key>=<value>`` with
    ``<key>=<redacted>`` for each secret key, leaving every other character of
    the string untouched (preferred over a ``parse_qsl``/``urlencode`` round-trip
    that would normalise encoding and ordering).

    Args:
        url: The URL (or free text containing one) to redact.
        secret_keys: Case-folded exact key names whose values are masked.
        value_chars: Character class matching one value character. The default
            ``[^&]`` is greedy to the next param separator — correct for a clean
            URL. Callers redacting a URL embedded in free text pass a stricter
            class (e.g. ``[^&\\s)]``) so masking stops at the URL boundary.

    Returns:
        The URL with secret values replaced by ``<redacted>``.
    """
    for key in secret_keys:
        url = re.sub(
            rf"(?i)(\b{re.escape(key)}=){value_chars}+",
            r"\1" + REDACTED,
            url,
        )
    return url
