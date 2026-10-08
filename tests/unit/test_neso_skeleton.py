"""Vault page skeletons are generated deterministically from the registry (v0.22-E P-9).

T-SK1..T-SK4. ``render_package`` is pure; the CLI is exercised over a
verifiable snapshot directory built by the snapshot module's own writers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from _neso_profile_support import (
    PILOT_DIR,
    pilot_field_info,
    pilot_snapshot_packages,
    write_field_info,
    write_snapshot,
)
from _neso_registry_support import family, package, record, resource, write_registry

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton

TEC = "transmission-entry-capacity-tec-register"
GOLDEN = PILOT_DIR / "skeleton_tec_register.md"
PILOT_KEYS = {
    TEC: "tec_register",
    "interconnector-register": "interconnector_register",
    "embedded-register": "embedded_register",
    "long-term-2-52-weeks-ahead-national-demand-forecast": "demand_forecast_2_52w",
    "day-ahead-half-hourly-demand-forecast-performance": "da_demand_fc_performance",
    "24-months-ahead-constraint-cost-forecast": "constraint_cost_fc_24m",
}


def _snapshot_package(slug: str) -> dict[str, Any]:
    return next(p for p in pilot_snapshot_packages() if p["name"] == slug)


def _pilot_render(slug: str) -> str:
    key = PILOT_KEYS[slug]
    return skeleton.render_package(
        registry_module.load_registry(), _snapshot_package(slug), {key: pilot_field_info(key)}
    )


class TestGolden:
    def test_t_sk1_tec_register_equals_the_golden(self) -> None:
        """T-SK1: detects any unreviewed change to a rendered page.

        The golden is compared after CRLF->LF (git normalises committed text).
        """
        rendered = _pilot_render(TEC)
        assert rendered == GOLDEN.read_bytes().decode("utf-8").replace("\r\n", "\n")
        front = rendered.split("\n---\n", 1)[0]
        assert 'dataset_key: "tec_register"' in front
        assert "skeleton: true" in front
        assert "chart:" not in rendered and "page:" not in front
        assert skeleton.ATTRIBUTION in rendered
        assert "Twice weekly" in rendered
        assert (
            "| `published_at` | CKAN `last_modified` of the captured file (ADR-030) |" in rendered
        )
        assert "| MW Connected | `mw_connected` | float64 | — | yes | — | MW |" in rendered


@pytest.fixture
def synthetic(tmp_path: Path) -> tuple[Any, dict[str, Any]]:
    """A held package with every disposition, a child inventory and a dump family."""
    ids = [f"00000000-0000-4000-8000-0000000001{n:02d}" for n in range(10)]
    evidence = {"fingerprint": "0" * 64, "components": {"harness": "0" * 64}}
    document = package(
        "every-disposition",
        "00000000-0000-4000-8000-000000000001",
        [
            family("syn_series", refresh="intraday"),
            family("syn_dump", record=record(vintage="capture_fallback")),
            family("syn_files", kind="files"),
        ],
        [
            resource(ids[0], "Series | 2026", "syn_series"),
            resource(
                ids[1],
                "Old Series",
                "syn_dump",
                disposition={
                    "kind": "COVERED",
                    "by": ids[5],
                    "key": "syn_dump",
                    "evidence": evidence,
                },
            ),
            resource(ids[2], "Guide", "syn_files", fmt="PDF"),
            resource(ids[3], "Map", "syn_files", fmt="GEOJSON", disposition={"kind": "GIS"}),
            resource(
                ids[4],
                "Workbook",
                "syn_series",
                fmt="XLSX",
                disposition={"kind": "HOLD", "reason": "Excel reader", "unit": "X"},
                children=[
                    {"child": "Data", "disposition": {"kind": "SILVER", "key": "syn_series"}},
                    {"child": "Notes", "disposition": {"kind": "DOC"}},
                ],
            ),
            resource(ids[5], "Dump", "syn_dump", url_type="datastore"),
        ],
    )
    document["eligibility"] = {"status": "held", "question": "Q-pkg", "unit": "U-1"}
    loaded = registry_module.load_registry(write_registry(tmp_path / "registry", [document]))
    snapshot = {
        "name": "every-disposition",
        "title": "Every Disposition",
        "organization": {"title": "Synthetic"},
        "license_title": "NESO Open Data Licence",
        "extras": [{"key": "Update Frequency", "value": "Every 30 minutes"}],
    }
    return loaded, snapshot


class TestDispositions:
    def test_t_sk2_every_section_renders(self, synthetic: tuple[Any, dict[str, Any]]) -> None:
        """T-SK2: detects a disposition, child, clock or hold missing from the page."""
        loaded, snapshot = synthetic
        page = skeleton.render_package(loaded, snapshot, None)
        for heading in ("### SILVER", "### COVERED", "### DOC", "### GIS", "### HOLD"):
            assert heading in page
        assert page.index("### SILVER") < page.index("### COVERED") < page.index("### HOLD")
        assert "Series \\| 2026" in page
        assert "| Old Series | CSV | upload |" in page
        assert "Excel reader (X)" in page
        assert "↳ Data" in page and "↳ Notes" in page
        assert "| Dump | CSV | dump |" in page
        assert "| `published_at` | null |" in page
        assert "gridflow capture time" in page
        assert "held: Q-pkg (unit U-1)" in page
        assert 'dataset_keys: ["syn_dump", "syn_series"]' in page
        assert "sampled, not complete" in page
        assert "Every 30 minutes" in page
        assert "- ingest-only: no silver yet" in page
        assert "- files: catalogue only" in page

    def test_render_is_deterministic(self, synthetic: tuple[Any, dict[str, Any]]) -> None:
        """Detects output that depends on anything but its inputs."""
        loaded, snapshot = synthetic
        assert skeleton.render_package(loaded, snapshot, None) == skeleton.render_package(
            loaded, snapshot, None
        )


def _cli(tmp_path: Path, out: Path, *slugs: str) -> int:
    snapshot_dir = tmp_path / "snapshots"
    if not snapshot_dir.exists():
        write_snapshot(snapshot_dir, pilot_snapshot_packages())
        write_field_info(
            tmp_path / "field-info", {key: pilot_field_info(key) for key in PILOT_KEYS.values()}
        )
    argv = [
        "--snapshot",
        str(snapshot_dir / "20261006T195819Z"),
        "--out",
        str(out),
        "--field-info",
        str(tmp_path / "field-info" / "20261008T114228Z"),
    ]
    for slug in slugs:
        argv += ["--package", slug]
    return skeleton.main(argv)


class TestCli:
    def test_t_sk3_a_hand_written_page_is_never_overwritten(self, tmp_path: Path) -> None:
        """T-SK3 (I-3): detects the generator clobbering a page without ``skeleton: true``."""
        out = tmp_path / "out"
        out.mkdir()
        page = out / f"{TEC}.md"
        hand = b"---\nsource: neso_data_portal\n---\n# Hand-written\n"
        page.write_bytes(hand)
        assert _cli(tmp_path, out, TEC, "embedded-register") == 1
        assert page.read_bytes() == hand
        assert sorted(p.name for p in out.iterdir()) == [f"{TEC}.md"]

    def test_t_sk3_a_skeleton_is_regenerated(self, tmp_path: Path) -> None:
        """T-SK3: detects a stale skeleton being refused rather than regenerated."""
        out = tmp_path / "out"
        out.mkdir()
        page = out / f"{TEC}.md"
        page.write_bytes(b"---\nskeleton: true\n---\nstale\n")
        assert _cli(tmp_path, out, TEC) == 0
        assert page.read_bytes().decode("utf-8") == _pilot_render(TEC)

    def test_t_sk3_runs_are_identical_and_write_only_pages(self, tmp_path: Path) -> None:
        """T-SK3: detects nondeterminism or a write outside ``<out>/<slug>.md``."""
        first, second = tmp_path / "a", tmp_path / "b"
        slugs = sorted(PILOT_KEYS)
        assert _cli(tmp_path, first, *slugs) == 0
        assert _cli(tmp_path, second, *slugs) == 0
        names = sorted(p.name for p in first.iterdir())
        assert names == sorted(f"{slug}.md" for slug in slugs)
        assert names == sorted(p.name for p in second.iterdir())
        for name in names:
            assert (first / name).read_bytes() == (second / name).read_bytes()

    def test_t_sk3_a_failed_write_leaves_the_prior_page(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-SK3: detects a failed write truncating the existing skeleton."""
        out = tmp_path / "out"
        out.mkdir()
        page = out / f"{TEC}.md"
        prior = b"---\nskeleton: true\n---\nprior\n"
        page.write_bytes(prior)
        real_write = Path.write_bytes

        def failing_write(self: Path, data: Any) -> int:
            if self.parent == out and self.name.startswith("."):
                real_write(self, bytes(data)[:16])
                raise OSError(28, "No space left on device")
            return real_write(self, data)

        monkeypatch.setattr(Path, "write_bytes", failing_write)
        with pytest.raises(OSError, match="No space"):
            _cli(tmp_path, out, TEC)
        assert page.read_bytes() == prior
        assert sorted(p.name for p in out.iterdir()) == [f"{TEC}.md"]

    def test_a_tampered_field_info_file_is_refused(self, tmp_path: Path) -> None:
        """Detects field-info evidence used after its checksum stopped matching."""
        out = tmp_path / "out"
        _cli(tmp_path, out, TEC)
        tampered = tmp_path / "field-info" / "20261008T114228Z" / "tec_register.json"
        tampered.write_bytes(tampered.read_bytes() + b" ")
        assert _cli(tmp_path, tmp_path / "out2", TEC) == 1
        assert not (tmp_path / "out2").exists()


class TestPilot:
    @pytest.mark.parametrize("slug", sorted(PILOT_KEYS))
    def test_t_sk4_every_pilot_package_renders(self, slug: str) -> None:
        """T-SK4: detects a pilot package the generator cannot render."""
        page = _pilot_render(slug)
        assert f'dataset_key: "{PILOT_KEYS[slug]}"' in page
        assert 'layer_coverage: "bronze, silver"' in page

    @pytest.mark.parametrize(
        ("slug", "fragment"),
        [
            ("long-term-2-52-weeks-ahead-national-demand-forecast", "TODO: ESI week definition"),
            ("day-ahead-half-hourly-demand-forecast-performance", "end in 'Z' but"),
            ("24-months-ahead-constraint-cost-forecast", "TODO: currency unit of Constraint"),
        ],
    )
    def test_t_sk4_held_pilot_pages_show_their_question(self, slug: str, fragment: str) -> None:
        """T-SK4: detects a held output's question missing from its page."""
        holds = _pilot_render(slug).split("## Holds", 1)[1]
        assert fragment in holds and "(unit E-SEM)" in holds
