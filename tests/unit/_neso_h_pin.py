"""The byte-unchanged pin of every generated NESO family (I-1 / H8).

Not a test module (no ``test_`` prefix). :func:`generated_pin` renders, for the
committed registry:

- ``sql``: every generated ``_latest`` select, in the registered (``latest``)
  and the parameterised (``as_of``) form;
- ``records``: every record's ``model_dump(mode="json", exclude_none=True)``;
- ``columns``: every record's ``output_columns``;
- ``engine``: a sha256 of every DEM-1 fixture's engine output (``source_run_id``
  excluded, B7), sorted by every column and written as CSV.

:func:`write_golden` wrote ``tests/fixtures/neso_data_portal/h/base_pin.json``
once, on the untouched base of v0.22-H (master ``4c4b8ec``), before any ``src/``
edit of the unit. T-F1 asserts every family in it is unchanged in the current
tree. It holds every family of the older DEM-1H and SC goldens with equal values,
which is why those two were deleted.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

PIN_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "h" / "base_pin.json"
)


def _engine_digests(short_base: Path) -> dict[str, str]:
    from _neso_generic_support import silver_frame
    from test_neso_dem1_records import DAY, SIDECARS, _capture, dem1_body

    from gridflow.silver.registry import get_transformer

    out: dict[str, str] = {}
    for key in sorted(SIDECARS):
        with tempfile.TemporaryDirectory(dir=short_base) as root:
            data = Path(root)
            _capture(data, key, dem1_body(key))
            get_transformer("neso_data_portal", key, data).run(DAY, run_id="pin")
            frame = silver_frame(data, key).drop("source_run_id")
            frame = frame.sort(frame.columns)
            out[key] = hashlib.sha256(frame.write_csv().encode("utf-8")).hexdigest()
    return out


def generated_pin(short_base: Path) -> dict[str, Any]:
    """The pin of every generated family of the committed registry (see the module doc).

    ``short_base`` holds the engine runs' data roots (``tests/conftest.py``).
    """
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
    return {
        "sql": sql,
        "records": records,
        "columns": columns,
        "engine": _engine_digests(short_base),
    }


def dump(pin: dict[str, Any]) -> str:
    """The golden's text: sorted keys, indent 2, LF, separator-normalised (RULINGS 490)."""
    return json.dumps(pin, sort_keys=True, indent=2).replace("\\\\", "/") + "\n"


def write_golden() -> Path:
    """Write :data:`PIN_PATH` from the current tree (run once, on the base)."""
    PIN_PATH.parent.mkdir(parents=True, exist_ok=True)
    PIN_PATH.write_bytes(dump(generated_pin()).encode("utf-8"))
    return PIN_PATH
