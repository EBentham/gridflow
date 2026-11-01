"""DATA-SOURCES.md must cover every configured source.

A vendor added to ``sources.yaml`` without a section there would ship with no record of its
terms, its attribution or whether its fixtures are real responses.
"""

from __future__ import annotations

import re
from pathlib import Path

from gridflow.config.settings import load_settings

PAGE = Path(__file__).resolve().parents[2] / "DATA-SOURCES.md"


def test_every_source_has_a_data_sources_section() -> None:
    assert PAGE.is_file(), f"{PAGE.name} is missing"
    headings = [
        line for line in PAGE.read_text(encoding="utf-8").splitlines() if line.startswith("## ")
    ]
    named = {key for line in headings for key in re.findall(r"`([a-z0-9_]+)`", line)}
    missing = sorted(set(load_settings().sources) - named)
    assert not missing, f"{PAGE.name} has no section heading naming: {missing}"
