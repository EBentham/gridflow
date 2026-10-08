"""CLI: reconcile NESO bronze against silver completion, optionally draining (ADR-034 P-14).

``python -m gridflow.connectors.neso_data_portal.reconcile (<key> [<key> ...] | --all)
--cutoff YYYY-MM-DD [--drain]``

The data root and the DuckDB catalogue come from ``load_settings()``, so
``GRIDFLOW_DATA_DIR`` / ``GRIDFLOW_DUCKDB_PATH`` select them. Prints one
``GAP <category> <family> <partition_date> <capture_id> <detail>`` line per gap,
then ``SUMMARY`` lines. Exit 0 no gaps, 1 gaps, 2 usage error. The logic lives
in :mod:`gridflow.silver.neso_data_portal.reconcile`.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

__all__ = ["main"]


def _cutoff(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--cutoff must be YYYY-MM-DD, got {value!r}") from exc


def main(argv: list[str] | None = None) -> int:
    """Entry point. Exit 0 clean, 1 gaps, 2 usage."""
    parser = argparse.ArgumentParser(
        prog="python -m gridflow.connectors.neso_data_portal.reconcile"
    )
    parser.add_argument("keys", nargs="*", help="registry family keys")
    parser.add_argument("--all", action="store_true", dest="all_keys", help="every family")
    parser.add_argument("--cutoff", type=_cutoff, required=True, help="last partition date")
    parser.add_argument("--drain", action="store_true", help="recover drainable gaps")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code) if isinstance(exc.code, int) else 2
    if bool(args.keys) == bool(args.all_keys):
        print("name one or more family keys, or --all (not both)", file=sys.stderr)
        return 2

    from gridflow.config.settings import load_settings
    from gridflow.connectors.neso_data_portal import registry as registry_module
    from gridflow.pipeline import runner
    from gridflow.silver.neso_data_portal.reconcile import (
        UnknownFamilyError,
        drain,
        reconcile,
    )

    settings = load_settings()
    runner.import_transformers()
    registry = registry_module.load_registry()
    keys = None if args.all_keys else list(args.keys)
    data_dir = settings.pipeline.data_dir
    try:
        if args.drain:
            report = drain(
                data_dir, registry, keys, args.cutoff, lambda: runner.refresh_views(settings)
            )
        else:
            report = reconcile(data_dir, registry, keys, args.cutoff)
    except UnknownFamilyError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    for line in report.lines():
        print(line)
    return 0 if report.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
