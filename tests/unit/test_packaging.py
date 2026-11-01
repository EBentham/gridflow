"""The wheel must carry every runtime file under ``src/gridflow``, not just the Python.

setuptools ships only ``.py`` files unless ``[tool.setuptools.package-data]``
names the rest, and a missing file fails silently at runtime: with no gold SQL
in the wheel, ``init`` registers no gold views and reports success.
"""

from __future__ import annotations

import tomllib
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "gridflow"


def _non_python_files() -> list[PurePosixPath]:
    """Every non-Python file under the package, relative to ``src/``."""
    return sorted(
        PurePosixPath(path.relative_to(PACKAGE_ROOT.parent).as_posix())
        for path in PACKAGE_ROOT.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".py", ".pyc"}
    )


def _is_shipped(file: PurePosixPath, package_data: dict[str, list[str]]) -> bool:
    """Whether a ``package-data`` pattern matches ``file``, with glob semantics.

    A ``*`` stays inside one path segment, as it does for setuptools, so
    ``views/*.sql`` does not cover ``views/held/x.sql``.
    """
    for package, patterns in package_data.items():
        package_dir = PurePosixPath(*package.split("."))
        if not file.is_relative_to(package_dir):
            continue
        relative = file.relative_to(package_dir).parts
        for pattern in patterns:
            segments = PurePosixPath(pattern).parts
            if len(segments) == len(relative) and all(
                fnmatchcase(part, segment) for part, segment in zip(relative, segments, strict=True)
            ):
                return True
    return False


def test_package_data_covers_every_non_python_file() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    package_data = pyproject["tool"]["setuptools"]["package-data"]

    files = _non_python_files()
    unshipped = [str(file) for file in files if not _is_shipped(file, package_data)]

    assert files, "found no non-Python files under src/gridflow"
    assert unshipped == [], f"not matched by [tool.setuptools.package-data]: {unshipped}"
