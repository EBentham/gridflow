"""P0-a-1 / D-2: cold-process proof of NESO's class-scoped exact-read coverage.

D-2 buys automatic exact-read coverage for every current AND FUTURE NESO
transformer by scoping the fix to ``GenericNesoJsonTransformer`` (the
generated-class base every NESO dataset inherits from), rather than a
source-scoped frozenset. That promise is only as good as production bootstrap
actually wiring every ``source == "neso"`` transformer through that base --
and an in-process registry check is not a real test of that, because pytest's
collection-time imports populate the process-global
``gridflow.silver.registry._REGISTRY`` ambiently. An assertion made against
that ambient state can pass even when the production import path
(``gridflow.pipeline.runner.import_transformers``) is broken, since Python
caches already-imported modules and re-running ``import_transformers()`` in
the SAME process would not re-execute the transformer modules' top-level
``register_transformer()`` calls. That exact false-negative shape has shipped
in this repo before (see ``tests/integration/test_lockstep_optin_registry.py``,
which this module is modelled on).

SUBPROCESS-DRIVEN, deliberately, for the same reason.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

from gridflow.connectors.neso.endpoints import ENDPOINTS

_THIS_FILE = Path(__file__)

# Emitted by the child; each entry is one registered source == "neso" transformer.
_PROBE = """
import json
from pathlib import Path

from gridflow.pipeline.runner import import_transformers
from gridflow.silver.registry import get_transformer, list_transformers

# Deliberately NOT imported before import_transformers(): the neso module calls
# register_neso_transformers() at import time, so importing it here would
# populate the registry itself and the probe would stay green even if the
# production bootstrap stopped importing gridflow.silver.neso -- the exact
# false positive this cold-process test exists to prevent (Sol diff major 1).
import_transformers()

from gridflow.silver.neso.carbon_intensity import GenericNesoJsonTransformer

rows = []
for source, dataset in sorted(list_transformers()):
    if source != "neso":
        continue
    cls = type(get_transformer(source, dataset, Path(".")))
    rows.append(
        {
            "dataset": dataset,
            "class_name": cls.__name__,
            "is_generic_neso_subclass": issubclass(cls, GenericNesoJsonTransformer),
        }
    )
print("PROBE_JSON:" + json.dumps(rows))
"""


def _probe_registry_in_a_fresh_process(tmp_path: Path) -> list[dict[str, object]]:
    """Run the probe in a genuinely separate interpreter and return its rows."""
    # Inherit the full parent environment (Windows subprocess creation needs
    # more than PATH/SYSTEMROOT) and override only the GRIDFLOW_* settings.
    env = dict(os.environ)
    env["GRIDFLOW_DATA_DIR"] = str(tmp_path / "data")
    env["GRIDFLOW_DUCKDB_PATH"] = str(tmp_path / "gridflow.duckdb")
    env["GRIDFLOW_LOG_DIR"] = str(tmp_path / "logs")
    env["ELEXON_API_KEY"] = "test-key"

    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )
    assert result.returncode == 0, (
        f"registry probe failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    marker = next(
        (line for line in result.stdout.splitlines() if line.startswith("PROBE_JSON:")),
        None,
    )
    assert marker is not None, f"probe emitted no result line:\n{result.stdout}"
    rows: list[dict[str, object]] = json.loads(marker.removeprefix("PROBE_JSON:"))
    assert rows, "no NESO transformers registered in the fresh process -- the probe proved nothing"
    return rows


def test_every_neso_transformer_is_a_generic_neso_json_transformer_subclass(
    tmp_path: Path,
) -> None:
    """D-2's coverage promise: every registered ``source == 'neso'`` transformer
    is a subclass of the base the exact-read fix lives on.

    A future bespoke NESO transformer that bypasses
    ``GenericNesoJsonTransformer`` (and its exact-partition ``_bronze_files``)
    would silently reintroduce the covering-fallback duplication -- this
    fails loudly instead.
    """
    rows = _probe_registry_in_a_fresh_process(tmp_path)

    non_subclass = [r["dataset"] for r in rows if not r["is_generic_neso_subclass"]]
    assert non_subclass == [], (
        f"NESO datasets registered outside GenericNesoJsonTransformer: {non_subclass}"
    )


def test_registered_neso_dataset_set_equals_the_endpoint_catalog(tmp_path: Path) -> None:
    """Every catalog endpoint is wired, and nothing extra is registered."""
    rows = _probe_registry_in_a_fresh_process(tmp_path)

    registered = {str(r["dataset"]) for r in rows}
    assert registered == set(ENDPOINTS)


def test_this_module_contains_no_in_process_registry_assertion() -> None:
    """The subprocess discipline is the point of this file, so pin it.

    An in-process version of either test above would pass on ambient pytest
    collection imports and could ship as a permanent no-op.
    """
    tree = ast.parse(_THIS_FILE.read_text())
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    # `_PROBE` is a string literal, so the child's own registry calls are
    # invisible to this parse -- which is exactly the distinction being pinned.
    forbidden = called & {"import_transformers", "list_transformers", "get_transformer"}
    assert forbidden == set(), (
        f"{sorted(forbidden)} must only ever run inside the CHILD probe: an "
        "in-process registry assertion passes on ambient pytest imports and "
        "could ship as a permanent no-op"
    )
    assert "subprocess.run" in _THIS_FILE.read_text()
