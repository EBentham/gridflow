"""Structured logging setup for gridflow."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from pythonjsonlogger import jsonlogger


def setup_logging(log_dir: Path, level: str = "INFO") -> None:
    """Configure structured logging with JSON file output and human-readable console output."""
    log_dir.mkdir(parents=True, exist_ok=True)

    # JSON formatter for file output
    json_formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    )

    # Human-readable for console
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # File handler — one log file per day
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(log_dir / f"gridflow_{today}.log")
    file_handler.setFormatter(json_formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(console_formatter)

    # Root logger for gridflow
    root = logging.getLogger("gridflow")
    root.setLevel(getattr(logging, level.upper()))
    # Avoid duplicate handlers on repeated calls
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)
