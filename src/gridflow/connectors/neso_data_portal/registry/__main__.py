"""Registry tooling: keep ``config/sources.yaml`` in agreement with the registry (P-3).

Usage::

    python -m gridflow.connectors.neso_data_portal.registry yaml --check
    python -m gridflow.connectors.neso_data_portal.registry yaml --write [--path <sources.yaml>]

Only the lines between the two marker comments are rewritten, as text: a
pyyaml round-trip would drop every comment in the file. The three legacy
entries keep their block text byte-for-byte and come first; every other key
follows, sorted, one per line.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from gridflow.connectors.neso_data_portal import registry as registry_module

if TYPE_CHECKING:
    from collections.abc import Iterable

BEGIN_MARKER = "      # >>> generated: neso_data_portal datasets"
END_MARKER = "      # <<< generated"

# Master's three entries, verbatim and in master's order.
_LEGACY_ORDER = (
    "daily_wind_availability",
    "historic_generation_mix",
    "embedded_wind_solar_forecast",
)
_LEGACY_ENTRY = (
    "      {key}:",
    '        endpoint: "/api/3/action/package_show"',
    '        schedule: "daily"',
    "        max_query_days: 1",
)
_GENERATED_ENTRY = (
    '      {key}: {{endpoint: "/api/3/action/package_show", schedule: "daily", max_query_days: 1}}'
)


class YamlDriftError(Exception):
    """``sources.yaml`` cannot be regenerated (markers missing or malformed)."""


def render_dataset_lines(keys: Iterable[str]) -> list[str]:
    """Return the generated block's lines (no markers) for the registry ``keys``."""
    keyset = set(keys)
    missing = [key for key in _LEGACY_ORDER if key not in keyset]
    if missing:
        raise YamlDriftError(f"the registry lacks the legacy keys {missing}")
    lines: list[str] = []
    for key in _LEGACY_ORDER:
        lines.extend(line.format(key=key) for line in _LEGACY_ENTRY)
    for key in sorted(keyset - set(_LEGACY_ORDER)):
        lines.append(_GENERATED_ENTRY.format(key=key))
    return lines


def regenerate(text: str, keys: Iterable[str]) -> str:
    """Return ``text`` with the marker-delimited block regenerated from ``keys``.

    Raises:
        YamlDriftError: The markers are absent, repeated or out of order.
    """
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.split(newline)
    begins = [i for i, line in enumerate(lines) if line == BEGIN_MARKER]
    ends = [i for i, line in enumerate(lines) if line == END_MARKER]
    if len(begins) != 1 or len(ends) != 1 or ends[0] <= begins[0]:
        raise YamlDriftError(
            f"expected exactly one {BEGIN_MARKER.strip()!r} followed by one "
            f"{END_MARKER.strip()!r}; found {len(begins)} and {len(ends)}"
        )
    return newline.join([*lines[: begins[0] + 1], *render_dataset_lines(keys), *lines[ends[0] :]])


def _default_sources_path() -> Path:
    from gridflow.config.settings import _find_config_dir

    return _find_config_dir() / "sources.yaml"


def _replace_atomically(path: Path, data: bytes) -> None:
    """Publish ``data`` at ``path`` so a failed write leaves the original intact.

    ``sources.yaml`` configures every source, so it is never truncated in
    place: the bytes go to a sibling temp file (same volume, so ``os.replace``
    is atomic on Windows too) and only a complete file replaces the target.
    Bytes, not text, so the file's CRLF/LF convention survives unchanged.
    """
    tmp = path.with_name(f".{path.name}.tmp_{uuid4().hex[:16]}")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _yaml_command(path: Path, *, write: bool) -> int:
    keys = registry_module.load_registry().families
    current = path.read_bytes().decode("utf-8")
    try:
        expected = regenerate(current, keys)
    except YamlDriftError as exc:
        print(f"{path}: {exc}", file=sys.stderr)
        return 2
    if expected == current:
        print(f"{path}: neso_data_portal datasets agree with the registry ({len(keys)} keys)")
        return 0
    if write:
        _replace_atomically(path, expected.encode("utf-8"))
        print(f"{path}: regenerated {len(keys)} neso_data_portal datasets")
        return 0
    print(
        f"{path}: neso_data_portal datasets drift from the registry; run "
        "`python -m gridflow.connectors.neso_data_portal.registry yaml --write`",
        file=sys.stderr,
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    """Entry point. Exit 0 clean or written, 1 drift (``--check``), 2 usage."""
    parser = argparse.ArgumentParser(prog="python -m gridflow.connectors.neso_data_portal.registry")
    sub = parser.add_subparsers(dest="command", required=True)
    yaml_parser = sub.add_parser("yaml", help="check or regenerate the sources.yaml datasets")
    mode = yaml_parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    yaml_parser.add_argument("--path", type=Path, default=None)
    args = parser.parse_args(argv)

    path: Path = args.path if args.path is not None else _default_sources_path()
    return _yaml_command(path, write=bool(args.write))


if __name__ == "__main__":
    raise SystemExit(main())
