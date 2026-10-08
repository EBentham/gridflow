"""The NESO eligibility report is generated from the registry (v0.22-E P-8).

T-EL1..T-EL3. The committed report is checked out of process, the way
``registry yaml --check`` is (``TestAgreement``): pytest collection has
already imported the registry, so only a fresh interpreter proves the module's
own CLI agrees with the committed file.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from _neso_registry_support import family, package, record, resource, write_registry

from gridflow.connectors.neso_data_portal import eligibility
from gridflow.connectors.neso_data_portal import registry as registry_module

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT = PROJECT_ROOT / "docs" / "neso_data_portal" / "eligibility.md"


def _check(*extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "gridflow.connectors.neso_data_portal.eligibility",
            "--check",
            *extra,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
    )


def _drifted_copy(tmp_path: Path) -> tuple[Path, bytes]:
    lines = REPORT.read_bytes().decode("utf-8").replace("\r\n", "\n").split("\n")
    dropped = next(i for i, line in enumerate(lines) if "| `tec_register` |" in line)
    drifted = "\n".join(lines[:dropped] + lines[dropped + 1 :]).encode("utf-8")
    copy = tmp_path / "eligibility.md"
    copy.write_bytes(drifted)
    return copy, drifted


class TestCommittedReport:
    def test_t_el1_the_committed_report_agrees_with_the_registry(self) -> None:
        """T-EL1: detects a registry change committed without regenerating the report."""
        result = _check()
        assert result.returncode == 0, result.stdout + result.stderr

    def test_t_el2_a_dropped_line_is_drift(self, tmp_path: Path) -> None:
        """T-EL2: detects ``--check`` passing a report that lost a family row."""
        copy, _drifted = _drifted_copy(tmp_path)
        result = _check("--path", str(copy))
        assert result.returncode == 1, result.stdout + result.stderr

    def test_t_el2_write_regenerates_the_render(self, tmp_path: Path) -> None:
        """T-EL2: detects ``--write`` producing anything but :func:`render`, or a stray temp."""
        copy, _drifted = _drifted_copy(tmp_path)
        assert eligibility.main(["--write", "--path", str(copy)]) == 0
        expected = eligibility.render(registry_module.load_registry()).encode("utf-8")
        assert copy.read_bytes() == expected
        assert [p.name for p in tmp_path.iterdir()] == ["eligibility.md"]

    def test_t_el2_a_failed_write_leaves_the_copy_intact(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-EL2: detects a disk-full write truncating the committed report."""
        copy, drifted = _drifted_copy(tmp_path)
        real_write = Path.write_bytes

        def failing_write(self: Path, data: Any) -> int:
            if self.parent == tmp_path and self.name.startswith("."):
                real_write(self, bytes(data)[:64])
                raise OSError(28, "No space left on device")
            return real_write(self, data)

        monkeypatch.setattr(Path, "write_bytes", failing_write)
        with pytest.raises(OSError, match="No space"):
            eligibility.main(["--write", "--path", str(copy)])
        assert copy.read_bytes() == drifted
        assert [p.name for p in tmp_path.iterdir()] == ["eligibility.md"]

    def test_usage_without_a_mode_exits_2(self) -> None:
        """Detects the CLI accepting neither ``--check`` nor ``--write``."""
        with pytest.raises(SystemExit) as raised:
            eligibility.main([])
        assert raised.value.code == 2


def _held(question: str, unit: str) -> dict[str, str]:
    return {"status": "held", "question": question, "unit": unit}


def _row(text: str, key: str) -> str:
    return next(line for line in text.splitlines() if f"| `{key}` |" in line)


class TestRule:
    """T-EL3: the P-8 effective-eligibility rule over a synthetic registry."""

    @pytest.fixture
    def rendered(self, tmp_path: Path) -> str:
        eligible_record = record()
        held_record = dict(record(), eligibility=_held("Q-record", "U-R"))
        held_package = package(
            "held-package",
            "00000000-0000-4000-8000-0000000000a1",
            [family("held_pkg_rec", record=eligible_record)],
            [resource("00000000-0000-4000-8000-0000000000b1", "Held", "held_pkg_rec")],
        )
        held_package["eligibility"] = _held("Q-package", "U-P")
        open_package = package(
            "open-package",
            "00000000-0000-4000-8000-0000000000a2",
            [
                family("open_held_rec", record=held_record),
                family("open_plain"),
                family("open_files", kind="files"),
                family("open_dump", record=record(vintage="capture_fallback")),
            ],
            [
                resource("00000000-0000-4000-8000-0000000000b2", "A", "open_held_rec"),
                resource("00000000-0000-4000-8000-0000000000b3", "B", "open_plain"),
                resource("00000000-0000-4000-8000-0000000000b4", "C", "open_files", fmt="PDF"),
                resource(
                    "00000000-0000-4000-8000-0000000000b5", "D", "open_dump", url_type="datastore"
                ),
            ],
        )
        directory = write_registry(tmp_path / "registry", [held_package, open_package])
        return eligibility.render(registry_module.load_registry(directory))

    def test_a_held_package_holds_an_eligible_record(self, rendered: str) -> None:
        """Detects a package hold being overridden by its record's eligibility."""
        assert "held: Q-package (unit U-P)" in _row(rendered, "held_pkg_rec")

    def test_an_eligible_package_takes_the_records_hold(self, rendered: str) -> None:
        """Detects a record's per-output hold being dropped."""
        assert "held: Q-record (unit U-R)" in _row(rendered, "open_held_rec")

    def test_no_record_is_ingest_only_and_files_are_catalogue_only(self, rendered: str) -> None:
        """Detects a recordless family reported as a silver output."""
        assert "ingest-only (no silver output)" in _row(rendered, "open_plain")
        assert "catalogue only" in _row(rendered, "open_files")

    def test_a_dump_family_is_labelled_capture_time(self, rendered: str) -> None:
        """Detects a dump family's clock labelled as a vendor clock (ADR-035)."""
        row = _row(rendered, "open_dump")
        assert "gridflow capture time" in row
        assert "| eligible |" in row

    def test_render_is_deterministic(self, rendered: str, tmp_path: Path) -> None:
        """Detects ordering that depends on anything but the registry."""
        again = eligibility.render(registry_module.load_registry(tmp_path / "registry"))
        assert again == rendered
        assert rendered.endswith("\n") and "\r" not in rendered
