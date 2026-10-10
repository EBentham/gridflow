"""Peak-RSS probe for the NESO CSV reader's strict UTF-8 gate (v0.22-K-IC-2H, I-2b).

Run in a FRESH interpreter by ``test_neso_reader_memory.py``; never collected by
pytest (no ``test_`` prefix)::

    python tests/integration/_neso_reader_rss_probe.py gate <body path>
    python tests/integration/_neso_reader_rss_probe.py stubbed <body path>

Both modes read the body through ``read_csv_body`` with a record stand-in whose
``encoding`` is ``utf-8`` (the one field the reader reads); ``stubbed`` first
replaces the gate's validator with a no-op, so the two peaks differ by the gate
alone. Each prints ``ROWS <n>`` then ``PEAK <peak resident set size in bytes>``.

Read-only: the reader opens the body and writes nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _neso_rss_probe import peak_rss_bytes  # noqa: E402


def main(argv: list[str]) -> int:
    """Run one probe mode; print the row count and the peak RSS."""
    mode, path = argv[0], Path(argv[1])
    from gridflow.silver.neso_data_portal import readers

    if mode == "stubbed":

        def _no_gate(raw: bytes) -> None:
            return None

        readers._validate_utf8 = _no_gate
    elif mode != "gate":
        print(f"unknown mode {mode!r}", file=sys.stderr)
        return 2
    record: Any = SimpleNamespace(encoding="utf-8")
    (table,) = list(readers.read_csv_body(path, record, ()))
    print(f"ROWS {table.frame.height}")
    print(f"PEAK {peak_rss_bytes()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
