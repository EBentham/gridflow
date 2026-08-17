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
# code would prove nothing; the log is one signal.
#
# But an absence-of-error check is only HALF the proof, and the weaker half:
# an entry that is missing from _TRANSFORMER_MODULES logs nothing at all, so a
# log-only assertion passes exactly as loudly for "imported fine" as for "never
# attempted". That is the vacuity class this module exists to prevent, and the
# first version of this test had it. The observation must be POSITIVE — the
# module is in sys.modules only because the bootstrap put it there.
_TRANSFORMER_BOOTSTRAP = """
import logging
import sys

records = []


class _Capture(logging.Handler):
    def emit(self, record):
        records.append(record)


logging.getLogger().addHandler(_Capture())
logging.getLogger().setLevel(logging.DEBUG)

from gridflow.pipeline import runner

assert 'gridflow.silver.neso_data_portal' not in sys.modules, (
    'precondition failed: the package was already imported before the bootstrap ran, '
    'so this test cannot attribute the import to import_transformers()'
)

runner.import_transformers()

assert 'gridflow.silver.neso_data_portal' in sys.modules, (
    'import_transformers() did NOT import gridflow.silver.neso_data_portal -- the '
    '_TRANSFORMER_MODULES entry is missing, so every transformer in that package '
    'would be silently unregistered'
)

offending = [
    r.getMessage()
    for r in records
    if 'gridflow.silver.neso_data_portal' in r.getMessage()
]
assert not offending, offending
print('IMPORTED_CLEANLY')
"""

# The negative control for the assertion above: with the entry stripped from the
# bootstrap list, the positive observation MUST fail. Without this, "the module
# is in sys.modules" could be true for some unrelated reason -- a transitive
# import from gridflow.silver's own __init__, say -- and the check would be
# green while proving nothing.
_TRANSFORMER_BOOTSTRAP_WITHOUT_ENTRY = """
import sys

from gridflow.pipeline import runner

runner._TRANSFORMER_MODULES = [
    m for m in runner._TRANSFORMER_MODULES if m != 'gridflow.silver.neso_data_portal'
]
runner.import_transformers()

if 'gridflow.silver.neso_data_portal' in sys.modules:
    print('IMPORTED_ANYWAY')
else:
    print('NOT_IMPORTED')
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


def test_transformer_bootstrap_actually_imports_this_package() -> None:
    """``import_transformers()`` must POSITIVELY import this package.

    Two distinct failures are covered, and the first is the one a log-only
    assertion misses entirely: an entry absent from ``_TRANSFORMER_MODULES``
    logs nothing, so "no ImportError was logged" is equally true of a package
    imported cleanly and a package never attempted. The module's presence in
    ``sys.modules`` after the bootstrap — and its absence before — is what
    distinguishes them.

    The silver package is created empty in the task that adds the entry
    precisely so this holds from that moment, rather than leaving the
    production bootstrap in a known-broken state across two units.
    """
    result = _run_in_fresh_interpreter(_TRANSFORMER_BOOTSTRAP)

    assert result.returncode == 0, (
        "import_transformers() did not import gridflow.silver.neso_data_portal, or "
        "logged a failure for it (it swallows the ImportError, so the exit code alone "
        f"would not show that)\n{result.stdout}\n{result.stderr}"
    )
    assert "IMPORTED_CLEANLY" in result.stdout


def test_the_negative_control_shows_the_import_depends_on_the_bootstrap_entry() -> None:
    """Strip the entry from ``_TRANSFORMER_MODULES`` and the package must NOT
    be imported.

    This is what makes the positive observation above meaningful. If the module
    still appeared in ``sys.modules`` here — pulled in transitively by
    ``gridflow.silver``'s own ``__init__``, for instance — then the positive
    check would be measuring that transitive import rather than the bootstrap,
    and removing the entry would leave every transformer in the package
    unregistered behind a green test.
    """
    result = _run_in_fresh_interpreter(_TRANSFORMER_BOOTSTRAP_WITHOUT_ENTRY)

    assert result.returncode == 0, result.stderr
    assert "NOT_IMPORTED" in result.stdout, (
        "the package was imported even with its bootstrap entry removed, so the "
        "positive assertion above does not actually depend on _TRANSFORMER_MODULES"
    )
