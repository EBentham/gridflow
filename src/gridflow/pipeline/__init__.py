"""Shared, UI-agnostic pipeline orchestration.

``gridflow.pipeline.runner`` is the single source of truth for the bronze /
silver / gold step loops, date/dataset resolution, run-tracker lifecycle, error
redaction, view refresh, and watermark advancement. It deliberately imports no
``typer``/``argparse`` and never prints or raises ``typer.Exit`` — it returns
structured :class:`~gridflow.pipeline.runner.RunReport` results. The CLI
(``gridflow.cli``) is the thin adapter that translates those results into
console output and exit codes.
"""

from __future__ import annotations
