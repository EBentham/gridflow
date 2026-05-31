"""Shared enum-mapping constants for ENTSO-E silver transformers.

ENTSO-E enum codes (``flow_direction``, ``business_type``) cross the API ->
silver boundary untrusted-by-value. The parser's empty-string default and
legitimate-but-unlisted codes (e.g. ``A03`` "up and down") are expected, not
garbage. Mapping such a code with ``replace_strict`` and no ``default=`` raises
``InvalidOperationError`` and zeroes the whole date (ADR-022 finding H2).

Every ENTSO-E ``replace_strict`` enum site supplies ``default=UNMAPPED_SENTINEL``
so an unmapped code maps to an explicit, recoverable, counted label instead of
crashing the transform. See ``docs/DECISION_LOG/ADR-022-unmapped-enum-code-policy.md``.
"""

from __future__ import annotations

UNMAPPED_SENTINEL = "unmapped"
"""Sentinel label for an ENTSO-E enum code absent from a transformer's map."""
