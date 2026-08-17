"""T-02 (D-37): registration is proven OUT OF PROCESS.

An in-process assertion is worthless here. pytest collection imports the
connector package directly, so the registry is already populated and
``get_connector("neso_data_portal", ...)`` succeeds **even when the
``_CONNECTOR_MODULES`` entry is missing** — a production no-op behind a green
test. The bootstrap lists are exactly where that bites: ``import_connectors``
log-and-continues on an ``ImportError``, so even a broken module is silent.

Every assertion below therefore runs in a fresh interpreter with no prior
imports, calls the **production** bootstrap function, and each positive proof
carries a negative control so it cannot pass vacuously on a registry something
else happened to populate.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_SOURCE_CONFIG = (
    "from gridflow.config.settings import SourceConfig; "
    "cfg = SourceConfig(base_url='https://api.neso.energy', rate_limit_per_second=1)"
)

_WITH_BOOTSTRAP = f"""
from gridflow.pipeline import runner
from gridflow.connectors.registry import get_connector
{_SOURCE_CONFIG}
runner.import_connectors()
connector = get_connector('neso_data_portal', cfg)
assert connector.source_name == 'neso_data_portal', connector.source_name
print('RESOLVED')
"""

_WITHOUT_BOOTSTRAP = f"""
from gridflow.connectors.registry import get_connector
{_SOURCE_CONFIG}
get_connector('neso_data_portal', cfg)
print('RESOLVED')
"""

# import_transformers() logs and continues on a real ImportError, so its whole
# hazard is that it SUCCEEDS while swallowing the failure. Checking the exit
# code would prove nothing; the log is the only signal.
_TRANSFORMER_BOOTSTRAP = """
import logging

records = []


class _Capture(logging.Handler):
    def emit(self, record):
        records.append(record)


logging.getLogger().addHandler(_Capture())
logging.getLogger().setLevel(logging.DEBUG)

from gridflow.pipeline import runner

runner.import_transformers()

offending = [
    r.getMessage()
    for r in records
    if 'gridflow.silver.neso_data_portal' in r.getMessage()
]
assert not offending, offending
print('IMPORTED_CLEANLY')
"""


def _run_in_fresh_interpreter(script: str) -> subprocess.CompletedProcess[str]:
    """Run ``script`` in a brand-new interpreter with no prior gridflow imports."""
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_connector_resolves_in_a_fresh_interpreter_via_the_production_bootstrap() -> None:
    """The positive proof: ``import_connectors()`` reaches this source."""
    result = _run_in_fresh_interpreter(_WITH_BOOTSTRAP)

    assert result.returncode == 0, (
        "get_connector('neso_data_portal') failed in a fresh interpreter after "
        f"import_connectors() — is the _CONNECTOR_MODULES entry missing?\n{result.stderr}"
    )
    assert "RESOLVED" in result.stdout


def test_the_negative_control_fails_without_the_bootstrap() -> None:
    """Without ``import_connectors()`` the same call must FAIL.

    This is what makes the positive test meaningful: if resolution succeeded
    here too, something other than the bootstrap would be populating the
    registry and the proof above would be vacuous.
    """
    result = _run_in_fresh_interpreter(_WITHOUT_BOOTSTRAP)

    assert result.returncode != 0, (
        "the connector resolved WITHOUT import_connectors() — the positive proof "
        "above is therefore vacuous, since something else populates the registry"
    )
    assert "RESOLVED" not in result.stdout
    assert "Unknown source" in result.stderr


def test_transformer_bootstrap_logs_no_import_error_for_this_package() -> None:
    """``_TRANSFORMER_MODULES`` must not reference a package that fails to import.

    The silver package is created empty in this same task precisely so this
    holds from the moment the list is extended, rather than leaving the
    production bootstrap in a known-broken state across two units.
    """
    result = _run_in_fresh_interpreter(_TRANSFORMER_BOOTSTRAP)

    assert result.returncode == 0, (
        "import_transformers() logged a failure for gridflow.silver.neso_data_portal "
        f"(it swallows the ImportError, so the exit code alone would not show it)\n"
        f"{result.stdout}\n{result.stderr}"
    )
    assert "IMPORTED_CLEANLY" in result.stdout
