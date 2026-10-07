"""Whole-file publication for the NESO registry and coverage tools."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["replace_atomically"]


def replace_atomically(path: Path, data: bytes) -> None:
    """Publish ``data`` at ``path`` so a failed write leaves the original intact.

    The bytes go to a sibling temp file (same volume, so ``os.replace`` is
    atomic on Windows too) and only a complete file replaces the target; an
    interrupted or disk-full write never truncates what was there. Bytes, not
    text, so the caller owns the newline convention.

    Args:
        path: The file to create or replace.
        data: The complete new contents.

    Raises:
        OSError: The temp write or the replace failed. ``path`` is unchanged
            and the temp file is removed.
    """
    tmp = path.with_name(f".{path.name}.tmp_{uuid4().hex[:16]}")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
