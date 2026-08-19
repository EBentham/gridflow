"""NESO Open Data Portal silver transformers.

**This file is deliberately bare right now, and there is a rule for growing it
(D-38): the task that CREATES a transformer module adds its own import line
here**, as::

    from gridflow.silver.neso_data_portal import daily_wind_availability  # noqa: F401

That import is what fires ``register_transformer(...)``, so a transformer whose
line is missing is silently unregistered — ``import_transformers()``
log-and-continues, so the failure is a WARNING nobody reads rather than an
error. The out-of-process registration proof
(``tests/unit/test_neso_data_portal_registration.py``) is the backstop that
catches a missed line.

The package is created empty, in the same task that adds
``"gridflow.silver.neso_data_portal"`` to ``runner._TRANSFORMER_MODULES``, so
the bootstrap list never references a package that does not exist. Importing
all three modules here up front would fail immediately; importing none while no
later task edits the file would leave every transformer unregistered. Both
directions are unexecutable, which is why the rule is stated rather than left
to judgement.

Distinct from ``gridflow.silver.neso``, which is the Carbon Intensity source
(D-01).
"""

from __future__ import annotations

from gridflow.silver.neso_data_portal import (
    daily_wind_availability,  # noqa: F401
    historic_generation_mix,  # noqa: F401
)
