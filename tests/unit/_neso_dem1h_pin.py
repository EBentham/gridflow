"""The byte-unchanged pin of every generated NESO family (v0.22-DEM-1H, I-1 / H4).

Not a test module (no ``test_`` prefix). :func:`generated_pin` renders, for the
committed registry:

- ``sql``: every generated ``_latest`` select, in the registered (``latest``)
  and the parameterised (``as_of``) form;
- ``records``: every record's ``model_dump(mode="json", exclude_none=True)``;
- ``columns``: every record's ``output_columns``;
- ``engine``: a sha256 of every DEM-1 fixture's engine output (``source_run_id``
  excluded, B7), sorted by every column and written as CSV.

:func:`write_golden` wrote ``tests/fixtures/neso_data_portal/dem1h/base_pin.json``
once, on the untouched base (master ``73fde80``), before any ``src/`` edit of the
unit; the T-H1 test compares the current tree's pin against it. Only APIs that
exist on that base are imported here.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

PIN_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "neso_data_portal"
    / "dem1h"
    / "base_pin.json"
)


def _engine_digests() -> dict[str, str]:
    from test_neso_dem1_records import DAY, SIDECARS, _capture, _short_base, _silver, dem1_body

    from gridflow.silver.registry import get_transformer

    out: dict[str, str] = {}
    for key in sorted(SIDECARS):
        with tempfile.TemporaryDirectory(
            prefix="dh", dir=_short_base(), ignore_cleanup_errors=True
        ) as root:
            data = Path(root)
            _capture(data, key, dem1_body(key))
            get_transformer("neso_data_portal", key, data).run(DAY, run_id="pin")
            frame = _silver(data, key).drop("source_run_id")
            frame = frame.sort(frame.columns)
            out[key] = hashlib.sha256(frame.write_csv().encode("utf-8")).hexdigest()
    return out


def generated_pin() -> dict[str, Any]:
    """The pin of every generated family of the committed registry (see the module doc)."""
    from gridflow.connectors.neso_data_portal.registry import load_registry
    from gridflow.silver.latest_views import latest_select_sql
    from gridflow.silver.neso_data_portal.generic import generated_registrations, output_columns

    registry = load_registry()
    generated = generated_registrations(registry)
    sql: dict[str, dict[str, str | None]] = {}
    records: dict[str, Any] = {}
    columns: dict[str, list[list[str]]] = {}
    for (source, key), spec in sorted(generated.specs.items()):
        record = registry.families[key][1].record
        assert record is not None, key
        names = {name for name, _type in output_columns(record)}
        base = f"silver_{source}_{key}"
        sql[key] = {
            "latest": latest_select_sql(base, spec, names, as_of_param=False),
            "as_of": latest_select_sql(base, spec, names, as_of_param=True),
        }
        records[key] = record.model_dump(mode="json", exclude_none=True)
        columns[key] = [[name, kind] for name, kind in output_columns(record)]
    return {"sql": sql, "records": records, "columns": columns, "engine": _engine_digests()}


def dump(pin: dict[str, Any]) -> str:
    """The golden's text: sorted keys, indent 2, LF, separator-normalised (RULINGS 490)."""
    return json.dumps(pin, sort_keys=True, indent=2).replace("\\\\", "/") + "\n"


def write_golden() -> Path:
    """Write :data:`PIN_PATH` from the current tree (run once, on the base)."""
    PIN_PATH.parent.mkdir(parents=True, exist_ok=True)
    PIN_PATH.write_bytes(dump(generated_pin()).encode("utf-8"))
    return PIN_PATH
