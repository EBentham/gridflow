"""Generate CANONICAL_SCHEMA.yaml from the registered silver transformer registry.

Run from the gridflow repo root:
    python scripts/seed_canonical_schema.py
    python scripts/seed_canonical_schema.py --output /tmp/test.yaml

The script walks list_transformers() and emits a YAML skeleton.  Six
Open-Meteo entries are authored by hand (canonical post-F15-B names); all
others get a TODO_HUMAN_FILL_COLUMNS: true marker for the curation pass.

Cadence values are read from config/sources.yaml.  Atomic write (temp +
os.replace) per CLAUDE.md hard rule.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import tempfile
from pathlib import Path
from typing import Any

import yaml

# -- registry side-effects (must import before list_transformers) --
import gridflow.silver.elexon  # noqa: F401
import gridflow.silver.entsoe  # noqa: F401
import gridflow.silver.entsog  # noqa: F401
import gridflow.silver.gie  # noqa: F401
import gridflow.silver.neso  # noqa: F401
import gridflow.silver.openmeteo  # noqa: F401
from gridflow.silver.registry import list_transformers

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SOURCES_YAML = _REPO_ROOT / "config" / "sources.yaml"

# ---------------------------------------------------------------------------
# Canonical Open-Meteo overrides — post-F15-B state (canonical names/units)
# ---------------------------------------------------------------------------

_OPEN_METEO_OVERRIDES: dict[tuple[str, str], dict[str, Any]] = {
    ("open_meteo", "historical_demand"): {
        "bitemporal_columns": {
            "event_time": {"type": "Datetime(us, UTC)", "semantics": "ERA5 archive hour start"},
            "available_at": {"type": "Datetime(us, UTC)", "semantics": "silver write time"},
        },
        "business_columns": {
            "timestamp_utc": {"type": "Datetime(us, UTC)", "semantics": "ERA5 hour start"},
            "location": {"type": "Utf8", "semantics": "population centre name"},
            "latitude": {"type": "Float64", "unit": "degrees_north"},
            "longitude": {"type": "Float64", "unit": "degrees_east"},
            "temperature_2m_c": {"type": "Float64", "unit": "C"},
            "wind_speed_10m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_direction_10m_deg": {"type": "Float64", "unit": "deg"},
            "relative_humidity_2m_pct": {"type": "Float64", "unit": "%"},
            "precipitation_mm": {"type": "Float64", "unit": "mm"},
            "shortwave_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "surface_pressure_hpa": {"type": "Float64", "unit": "hPa"},
            "snowfall_cm": {"type": "Float64", "unit": "cm"},
            "snow_depth_m": {"type": "Float64", "unit": "m"},
            "hdd_k": {"type": "Float64", "unit": "K-degree-day", "derived": True},
            "cdd_k": {"type": "Float64", "unit": "K-degree-day", "derived": True},
            "air_density_kg_m3": {"type": "Float64", "unit": "kg/m^3", "derived": True},
            "data_provider": {"type": "Utf8"},
            "ingested_at": {"type": "Datetime(us, UTC)"},
        },
        "metadata_columns": {
            "source_run_id": {"type": "Utf8"},
            "dataset_version": {"type": "Utf8"},
        },
        "cadence": "daily",
        "notes": (
            "ERA5 archive. 7 UK population centres. "
            "HDD base 15.5°C, CDD base 22°C. "
            "Unit renames applied by F15-B."
        ),
    },
    ("open_meteo", "historical_wind"): {
        "bitemporal_columns": {
            "event_time": {"type": "Datetime(us, UTC)", "semantics": "ERA5 archive hour start"},
            "available_at": {"type": "Datetime(us, UTC)", "semantics": "silver write time"},
        },
        "business_columns": {
            "timestamp_utc": {"type": "Datetime(us, UTC)", "semantics": "ERA5 hour start"},
            "location": {"type": "Utf8", "semantics": "capacity-weighted GB wind site"},
            "latitude": {"type": "Float64", "unit": "degrees_north"},
            "longitude": {"type": "Float64", "unit": "degrees_east"},
            "temperature_2m_c": {"type": "Float64", "unit": "C"},
            "surface_pressure_hpa": {"type": "Float64", "unit": "hPa"},
            "wind_speed_10m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_speed_100m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_direction_10m_deg": {"type": "Float64", "unit": "deg"},
            "wind_direction_100m_deg": {"type": "Float64", "unit": "deg"},
            "wind_gusts_10m_mps": {"type": "Float64", "unit": "m/s"},
            "cloud_cover_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_low_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_mid_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_high_pct": {"type": "Float64", "unit": "%"},
            "dew_point_2m_c": {"type": "Float64", "unit": "C"},
            "precipitation_mm": {"type": "Float64", "unit": "mm"},
            "air_density_kg_m3": {"type": "Float64", "unit": "kg/m^3", "derived": True},
            "data_provider": {"type": "Utf8"},
            "ingested_at": {"type": "Datetime(us, UTC)"},
        },
        "metadata_columns": {
            "source_run_id": {"type": "Utf8"},
            "dataset_version": {"type": "Utf8"},
        },
        "cadence": "daily",
        "notes": (
            "ERA5 archive. 12 capacity-weighted GB wind sites. "
            "80m/120m/180m hub heights omitted — all-null on ERA5 archive (see forecast_wind). "
            "Unit renames applied by F15-B."
        ),
    },
    ("open_meteo", "historical_solar"): {
        "bitemporal_columns": {
            "event_time": {"type": "Datetime(us, UTC)", "semantics": "ERA5 archive hour start"},
            "available_at": {"type": "Datetime(us, UTC)", "semantics": "silver write time"},
        },
        "business_columns": {
            "timestamp_utc": {"type": "Datetime(us, UTC)", "semantics": "ERA5 hour start"},
            "location": {"type": "Utf8", "semantics": "capacity-weighted GB solar site"},
            "latitude": {"type": "Float64", "unit": "degrees_north"},
            "longitude": {"type": "Float64", "unit": "degrees_east"},
            "temperature_2m_c": {"type": "Float64", "unit": "C"},
            "shortwave_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "direct_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "direct_normal_irradiance_wm2": {"type": "Float64", "unit": "W/m²"},
            "diffuse_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "global_tilted_irradiance_wm2": {"type": "Float64", "unit": "W/m²"},
            "cloud_cover_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_low_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_mid_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_high_pct": {"type": "Float64", "unit": "%"},
            "snowfall_cm": {"type": "Float64", "unit": "cm"},
            "snow_depth_m": {"type": "Float64", "unit": "m"},
            "data_provider": {"type": "Utf8"},
            "ingested_at": {"type": "Datetime(us, UTC)"},
        },
        "metadata_columns": {
            "source_run_id": {"type": "Utf8"},
            "dataset_version": {"type": "Utf8"},
        },
        "cadence": "daily",
        "notes": (
            "ERA5 archive. 6 capacity-weighted GB solar sites. "
            "GTI request adds tilt=35, azimuth=0 (UK fixed-tilt, due south). "
            "Unit renames applied by F15-B."
        ),
    },
    ("open_meteo", "forecast_demand"): {
        "bitemporal_columns": {
            "event_time": {"type": "Datetime(us, UTC)", "semantics": "forecast hour start"},
            "available_at": {"type": "Datetime(us, UTC)", "semantics": "silver write time"},
        },
        "business_columns": {
            "timestamp_utc": {"type": "Datetime(us, UTC)", "semantics": "forecast hour start"},
            "location": {"type": "Utf8", "semantics": "population centre name"},
            "latitude": {"type": "Float64", "unit": "degrees_north"},
            "longitude": {"type": "Float64", "unit": "degrees_east"},
            "temperature_2m_c": {"type": "Float64", "unit": "C"},
            "wind_speed_10m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_direction_10m_deg": {"type": "Float64", "unit": "deg"},
            "relative_humidity_2m_pct": {"type": "Float64", "unit": "%"},
            "precipitation_mm": {"type": "Float64", "unit": "mm"},
            "shortwave_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "surface_pressure_hpa": {"type": "Float64", "unit": "hPa"},
            "snowfall_cm": {"type": "Float64", "unit": "cm"},
            "snow_depth_m": {"type": "Float64", "unit": "m"},
            "hdd_k": {"type": "Float64", "unit": "K-degree-day", "derived": True},
            "cdd_k": {"type": "Float64", "unit": "K-degree-day", "derived": True},
            "air_density_kg_m3": {"type": "Float64", "unit": "kg/m^3", "derived": True},
            "data_provider": {"type": "Utf8"},
            "ingested_at": {"type": "Datetime(us, UTC)"},
        },
        "metadata_columns": {
            "source_run_id": {"type": "Utf8"},
            "dataset_version": {"type": "Utf8"},
        },
        "cadence": "hourly",
        "notes": (
            "UKMO/ECMWF NWP forecast. 7 UK population centres. "
            "HDD base 15.5°C, CDD base 22°C. "
            "Unit renames applied by F15-B."
        ),
    },
    ("open_meteo", "forecast_wind"): {
        "bitemporal_columns": {
            "event_time": {"type": "Datetime(us, UTC)", "semantics": "forecast hour start"},
            "available_at": {"type": "Datetime(us, UTC)", "semantics": "silver write time"},
        },
        "business_columns": {
            "timestamp_utc": {"type": "Datetime(us, UTC)", "semantics": "forecast hour start"},
            "location": {"type": "Utf8", "semantics": "capacity-weighted GB wind site"},
            "latitude": {"type": "Float64", "unit": "degrees_north"},
            "longitude": {"type": "Float64", "unit": "degrees_east"},
            "temperature_2m_c": {"type": "Float64", "unit": "C"},
            "surface_pressure_hpa": {"type": "Float64", "unit": "hPa"},
            "wind_speed_10m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_speed_100m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_direction_10m_deg": {"type": "Float64", "unit": "deg"},
            "wind_direction_100m_deg": {"type": "Float64", "unit": "deg"},
            "wind_gusts_10m_mps": {"type": "Float64", "unit": "m/s"},
            "cloud_cover_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_low_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_mid_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_high_pct": {"type": "Float64", "unit": "%"},
            "dew_point_2m_c": {"type": "Float64", "unit": "C"},
            "precipitation_mm": {"type": "Float64", "unit": "mm"},
            "wind_speed_80m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_speed_120m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_speed_180m_mps": {"type": "Float64", "unit": "m/s"},
            "wind_direction_80m_deg": {"type": "Float64", "unit": "deg"},
            "wind_direction_120m_deg": {"type": "Float64", "unit": "deg"},
            "wind_direction_180m_deg": {"type": "Float64", "unit": "deg"},
            "air_density_kg_m3": {"type": "Float64", "unit": "kg/m^3", "derived": True},
            "data_provider": {"type": "Utf8"},
            "ingested_at": {"type": "Datetime(us, UTC)"},
        },
        "metadata_columns": {
            "source_run_id": {"type": "Utf8"},
            "dataset_version": {"type": "Utf8"},
        },
        "cadence": "hourly",
        "notes": (
            "UKMO/ECMWF NWP forecast. 12 capacity-weighted GB wind sites. "
            "Includes 80m/120m/180m hub heights (nulled by models that don't carry them). "
            "Unit renames applied by F15-B."
        ),
    },
    ("open_meteo", "forecast_solar"): {
        "bitemporal_columns": {
            "event_time": {"type": "Datetime(us, UTC)", "semantics": "forecast hour start"},
            "available_at": {"type": "Datetime(us, UTC)", "semantics": "silver write time"},
        },
        "business_columns": {
            "timestamp_utc": {"type": "Datetime(us, UTC)", "semantics": "forecast hour start"},
            "location": {"type": "Utf8", "semantics": "capacity-weighted GB solar site"},
            "latitude": {"type": "Float64", "unit": "degrees_north"},
            "longitude": {"type": "Float64", "unit": "degrees_east"},
            "temperature_2m_c": {"type": "Float64", "unit": "C"},
            "shortwave_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "direct_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "direct_normal_irradiance_wm2": {"type": "Float64", "unit": "W/m²"},
            "diffuse_radiation_wm2": {"type": "Float64", "unit": "W/m²"},
            "global_tilted_irradiance_wm2": {"type": "Float64", "unit": "W/m²"},
            "cloud_cover_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_low_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_mid_pct": {"type": "Float64", "unit": "%"},
            "cloud_cover_high_pct": {"type": "Float64", "unit": "%"},
            "snowfall_cm": {"type": "Float64", "unit": "cm"},
            "snow_depth_m": {"type": "Float64", "unit": "m"},
            "data_provider": {"type": "Utf8"},
            "ingested_at": {"type": "Datetime(us, UTC)"},
        },
        "metadata_columns": {
            "source_run_id": {"type": "Utf8"},
            "dataset_version": {"type": "Utf8"},
        },
        "cadence": "hourly",
        "notes": (
            "UKMO/ECMWF NWP forecast. 6 capacity-weighted GB solar sites. "
            "GTI request adds tilt=35, azimuth=0 (UK fixed-tilt, due south). "
            "Unit renames applied by F15-B."
        ),
    },
}


def _load_cadence_map() -> dict[tuple[str, str], str]:
    """Build a (source, dataset) -> schedule map from sources.yaml."""
    raw = yaml.safe_load(_SOURCES_YAML.read_text(encoding="utf-8"))
    result: dict[tuple[str, str], str] = {}
    for source, source_cfg in raw.get("sources", {}).items():
        for dataset, dataset_cfg in source_cfg.get("datasets", {}).items():
            schedule = dataset_cfg.get("schedule", "unknown")
            result[(source, dataset)] = schedule
    return result


def _skeleton_entry(source: str, dataset: str, cadence: str) -> dict[str, Any]:
    """Return a minimal skeleton entry for non-Open-Meteo transformers."""
    return {
        "bitemporal_columns": {
            "event_time": {
                "type": "Datetime(us, UTC)",
                "semantics": "delivery period or target date midnight",
            },
            "available_at": {
                "type": "Datetime(us, UTC)",
                "semantics": "silver write time or sidecar-derived under --reingest",
            },
        },
        "business_columns": {
            "TODO_HUMAN_FILL_COLUMNS": True,
        },
        "metadata_columns": {
            "source_run_id": {"type": "Utf8"},
            "dataset_version": {"type": "Utf8"},
        },
        "cadence": cadence,
        "notes": "Schema not yet curated. Run seed_canonical_schema.py after curation.",
    }


def build_canonical(output: Path) -> None:
    cadence_map = _load_cadence_map()
    pairs = sorted(list_transformers())

    datasets: dict[str, Any] = {}
    for source, dataset in pairs:
        key = f"{source}/{dataset}"
        override = _OPEN_METEO_OVERRIDES.get((source, dataset))
        if override is not None:
            datasets[key] = override
        else:
            cadence = cadence_map.get((source, dataset), "unknown")
            datasets[key] = _skeleton_entry(source, dataset, cadence)

    sha = _repo_head_sha()
    doc: dict[str, Any] = {
        "__meta__": {
            "description": "Canonical silver schema for every registered (source, dataset) pair.",
            "pinned_at": sha,
            "generated_by": "scripts/seed_canonical_schema.py",
            "dataset_count": len(datasets),
        },
        "datasets": datasets,
    }

    tmp_fd, tmp_path_str = tempfile.mkstemp(
        dir=output.parent, prefix=".canonical_schema_", suffix=".yaml.tmp"
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            fh.write(
                "# CANONICAL_SCHEMA.yaml — source of truth for F15 silver contracts.\n"
                f"# Pinned at gridflow HEAD {sha}.\n"
                "# Generated by scripts/seed_canonical_schema.py.\n"
                "# Open-Meteo entries reflect post-F15-B canonical names.\n"
                "# Entries with TODO_HUMAN_FILL_COLUMNS: true need curation.\n\n"
            )
            yaml.dump(
                doc,
                fh,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
                width=120,
            )
        os.replace(tmp_path_str, output)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path_str)
        raise

    print(f"Written {len(datasets)} datasets to {output}")


def _repo_head_sha() -> str:
    try:
        import subprocess

        result = subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed CANONICAL_SCHEMA.yaml from registry")
    parser.add_argument(
        "--output",
        type=Path,
        default=_REPO_ROOT / "docs" / "CANONICAL_SCHEMA.yaml",
        help="Output path (default: docs/CANONICAL_SCHEMA.yaml)",
    )
    args = parser.parse_args()
    build_canonical(args.output)


if __name__ == "__main__":
    main()
