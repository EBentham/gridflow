"""The pyproject version must equal the newest released CHANGELOG heading.

The release workflow publishes a ``v*`` tag only when it equals the pyproject
version, and takes the release notes from that tag's CHANGELOG section, so a
version bump without its CHANGELOG entry (or the reverse) fails here first.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RELEASE_HEADING = re.compile(r"^## \[v(?P<version>[^\]]+)\]", re.MULTILINE)


def test_version_matches_top_changelog_release() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    changelog = (PROJECT_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")

    top = _RELEASE_HEADING.search(changelog)

    assert top is not None, "CHANGELOG.md has no released version heading"
    assert pyproject["project"]["version"] == top["version"]
