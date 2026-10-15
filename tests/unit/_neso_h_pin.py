"""The byte-unchanged base pin for v0.22-H (I-1 / H8).

Not a test module (no ``test_`` prefix). :func:`write_golden` wrote
``tests/fixtures/neso_data_portal/h/base_pin.json`` once, on the untouched base
(master ``4c4b8ec``), before any ``src/`` edit of the unit, from
:func:`_neso_dem1h_pin.generated_pin`. T-F1 compares the current tree's pin
against its keys and asserts the only added keys are the five system frequency
rendering families. Only APIs that exist on that base are imported here.
"""

from __future__ import annotations

from pathlib import Path

import _neso_dem1h_pin

PIN_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "h" / "base_pin.json"
)


def write_golden() -> Path:
    """Write :data:`PIN_PATH` from the current tree (run once, on the base)."""
    PIN_PATH.parent.mkdir(parents=True, exist_ok=True)
    PIN_PATH.write_bytes(_neso_dem1h_pin.dump(_neso_dem1h_pin.generated_pin()).encode("utf-8"))
    return PIN_PATH
