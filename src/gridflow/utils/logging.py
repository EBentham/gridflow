"""Structured logging setup for gridflow."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pythonjsonlogger.json import JsonFormatter

if TYPE_CHECKING:
    from pathlib import Path


def _resolve_level(level_name: str) -> int:
    """Resolve a logging level name to its numeric value."""
    level = getattr(logging, level_name.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Unknown log level: {level_name}")
    return level


def setup_logging(
    log_dir: Path,
    level: str = "INFO",
    console_level: str = "WARNING",
) -> None:
    """Configure detailed file logs and quieter human-readable console logs."""
    log_dir.mkdir(parents=True, exist_ok=True)
    file_log_level = _resolve_level(level)
    console_log_level = _resolve_level(console_level)

    json_formatter = JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    )
    console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(log_dir / f"gridflow_{today}.log")
    file_handler.setLevel(file_log_level)
    file_handler.setFormatter(json_formatter)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(console_log_level)
    console_handler.setFormatter(console_formatter)

    root = logging.getLogger("gridflow")
    root.setLevel(min(file_log_level, console_log_level))
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)
