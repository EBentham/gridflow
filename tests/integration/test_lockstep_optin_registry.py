"""R2-g / ADR-028: the LOCKSTEP_BRONZE_READ opt-in set, proven in a COLD process.

``LOCKSTEP_BRONZE_READ`` changes which bronze bodies a transformer reads. Its
blast radius is therefore exactly the set of transformers that opt in, and the
whole "no behaviour change reaches any other source" argument rests on that set
being what this unit says it is.

SUBPROCESS-DRIVEN, deliberately. Pytest's collection-time imports populate the
process-global ``gridflow.silver.registry._REGISTRY`` ambiently, so an
in-process assertion about the registry can pass while the production code path
that populates it is broken -- and Python caches already-imported modules, so
re-running ``import_transformers()`` in the SAME process would not re-execute
the transformer modules' top-level ``register_transformer()`` calls. That exact
false-negative shape has shipped in this repo before
(``test_quality_command_registry_import.py``). These tests drive a genuinely
fresh interpreter instead, so they cannot ride on any ambient import.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

_THIS_FILE = Path(__file__)

# Emitted by the child; each entry is (class_name, lockstep, per_file_vintage).
_PROBE = """
import json
from gridflow.pipeline.runner import import_transformers
from gridflow.silver.registry import list_transformers, get_transformer

import_transformers()

rows = []
for source, dataset in sorted(list_transformers()):
    cls = type(get_transformer(source, dataset, __import__("pathlib").Path(".")))
    rows.append(
        {
            "source": source,
            "dataset": dataset,
            "class_name": cls.__name__,
            "lockstep": bool(getattr(cls, "LOCKSTEP_BRONZE_READ", False)),
            "per_file_vintage": bool(getattr(cls, "VINTAGE_PER_BRONZE_FILE", False)),
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
    assert rows, "the registry was empty in the fresh process -- the probe proved nothing"
    return rows


def test_the_lockstep_optin_set_is_exactly_the_two_entsog_families(tmp_path: Path) -> None:
    """T3-e (I-9).

    Asserted as an EXACT set, so both a new opt-in and a dropped one fail. A
    transformer outside ENTSO-G opting in would be a data-semantics change on a
    source that has bronze on disk -- which is N-15's unit, not this one.
    """
    rows = _probe_registry_in_a_fresh_process(tmp_path)

    opted_in = {(str(r["source"]), str(r["dataset"])) for r in rows if r["lockstep"]}
    entsog = {(str(r["source"]), str(r["dataset"])) for r in rows if r["source"] == "entsog"}

    assert opted_in == entsog, (
        "the opt-in set must be exactly the ENTSO-G generic family plus "
        f"PhysicalFlowsTransformer.\n  unexpected opt-ins: {sorted(opted_in - entsog)}\n"
        f"  missing opt-ins:    {sorted(entsog - opted_in)}"
    )
    assert ("entsog", "physical_flows") in opted_in
    assert len(opted_in) > 1, "the generic family must be opted in too, not just physical_flows"


def test_no_transformer_sets_both_vintage_strategies(tmp_path: Path) -> None:
    """T3-f (I-9).

    ``VINTAGE_PER_BRONZE_FILE`` and ``LOCKSTEP_BRONZE_READ`` are different
    vintage strategies over the same partition, and ``run()`` dispatches over
    three mutually exclusive branches. A transformer setting both is a bug: the
    per-file branch is checked first, so the lockstep opt-in would be silently
    ignored.
    """
    rows = _probe_registry_in_a_fresh_process(tmp_path)

    both = [(r["source"], r["dataset"]) for r in rows if r["lockstep"] and r["per_file_vintage"]]

    assert both == [], f"transformers set BOTH vintage strategies: {both}"


def test_this_module_contains_no_in_process_registry_assertion() -> None:
    """The subprocess discipline is the point of this file, so pin it.

    An in-process version of either test above would pass on ambient pytest
    imports and could ship as a permanent no-op.
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
