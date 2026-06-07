"""Pydantic Settings model and configuration loading for gridflow.

Path resolution: relative `data_dir`, `duckdb_path`, and `log_dir` values
are anchored to the project root (the main git worktree), not to CWD.
This means running a gridflow CLI from a Jupyter kernel whose CWD is
`notebooks/` writes data to `<gridflow_root>/data/`, not
`notebooks/data/`. See ADR-001 (project-root resolution) — ported
verbatim from gridflow_models D-WORKTREE-PATH (commit 3e9b7e8).
"""

from __future__ import annotations

import os
import subprocess
from functools import cache
from pathlib import Path
from typing import Any

# PyYAML ships no type stubs; types-PyYAML is a dev-only stub, not a runtime dependency.
import yaml  # type: ignore[import-untyped]
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings


@cache
def _project_root() -> Path:
    """Resolve the project root via `git rev-parse --git-common-dir`.

    Worktree-safe: from a worktree, `--git-common-dir` returns the
    canonical .git directory; the project root is its parent.

    Falls back to walk-up-for-pyproject.toml then Path.cwd() if git
    is unavailable (CI minimal containers, sdist installs).

    Ported verbatim from gridflow_models D-WORKTREE-PATH (commit 3e9b7e8).
    """
    here = Path(__file__).resolve().parent
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            cwd=here,
            capture_output=True,
            text=True,
            check=True,
            timeout=2,
        )
        return Path(result.stdout.strip()).parent
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        for parent in [here, *here.parents]:
            if (parent / "pyproject.toml").exists():
                return parent
        return Path.cwd()


class DatasetConfig(BaseModel):
    """Configuration for a single dataset within a source."""

    endpoint: str = ""
    schedule: str = "daily"
    max_query_days: int = 1
    document_type: str | None = None
    process_type: str | None = None


class SourceConfig(BaseModel):
    """Configuration for a data source (Elexon, ENTSO-E, etc.)."""

    base_url: str
    api_key_env: str = ""
    api_key_header: str | None = None
    api_key: str = ""
    rate_limit_per_second: int = 5
    timeout: int = 30
    max_retries: int = 5
    datasets: dict[str, DatasetConfig] = Field(default_factory=dict)


class QualityConfig(BaseModel):
    """Quality check configuration."""

    null_rate_threshold: float = 0.05
    enable_outlier_detection: bool = True
    expected_freq_minutes: int = 30


class PipelineSettings(BaseSettings):
    """Main pipeline settings, loaded from env vars and .env file.

    Relative `data_dir`, `duckdb_path`, and `log_dir` are anchored to the
    project root (the main git worktree) by `_resolve_paths`, not to CWD.
    See ADR-001.
    """

    model_config = {"env_prefix": "GRIDFLOW_", "env_file": ".env", "extra": "ignore"}

    data_dir: Path = Path("./data")
    log_dir: Path = Path("./logs")
    duckdb_path: Path = Path("./data/gridflow.duckdb")
    default_lookback_hours: int = 24
    # Incremental ingest re-fetches from `watermark - incremental_overlap_hours`
    # to recover late/revised publications (run_type II->SF->R1). Default 0 keeps
    # `--incremental` behaviour-preserving (start == watermark). Raising it is safe
    # because bronze is immutable and silver dedups on (date, period, run_type), so
    # re-fetching the recent past adds bronze bytes without corrupting silver.
    #
    # WARNING (revision-settlement lag): with the default 0, `--incremental`
    # advances each dataset's frontier past the requested window and NEVER
    # re-fetches it. A revision-bearing dataset (Elexon settlement data
    # republished II->SF->R1 under a new run_type for an already-watermarked
    # date/period) will silently miss those late revisions on the incremental
    # path. Raise this to cover the publisher's revision lag (settlement runs
    # can revise for weeks) — or run a periodic backfill — before adopting
    # `--incremental` for settlement data. CLAUDE.md treats settlement revisions
    # as first-class, so a zero overlap on that path is a latent data-loss trap.
    incremental_overlap_hours: int = 0
    max_concurrent_requests: int = 5
    log_level: str = "INFO"
    console_log_level: str = "WARNING"
    # Per-date silver CSV sidecar (CH3-02 / CH-PERF-02 / C4-1). Default OFF:
    # Parquet is the canonical silver format. The old always-on write emitted an
    # unpartitioned per-date `.csv` alongside every Parquet partition on EVERY
    # run, doubling the silver write surface for a sidecar nothing in the
    # read/gold/quality path consumes. On-demand CSV is covered by the
    # `export_csv` CLI command, so the per-run write is pure redundant I/O.
    # Flip to True only when a downstream tool genuinely needs the live sidecar.
    write_silver_csv: bool = False

    # Secrets (loaded from .env)
    elexon_api_key: str = Field(default="")
    entsoe_api_key: str = Field(default="")
    entsog_api_key: str = Field(default="")
    gie_api_key: str = Field(default="")

    @model_validator(mode="after")
    def _resolve_paths(self) -> PipelineSettings:
        if not self.data_dir.is_absolute():
            object.__setattr__(self, "data_dir", (_project_root() / self.data_dir).resolve())
        if not self.duckdb_path.is_absolute():
            object.__setattr__(self, "duckdb_path", (_project_root() / self.duckdb_path).resolve())
        if not self.log_dir.is_absolute():
            object.__setattr__(self, "log_dir", (_project_root() / self.log_dir).resolve())
        return self


class GridflowConfig(BaseModel):
    """Complete gridflow configuration combining pipeline settings and source configs."""

    pipeline: PipelineSettings
    quality: QualityConfig = Field(default_factory=QualityConfig)
    sources: dict[str, SourceConfig] = Field(default_factory=dict)

    def get_source_config(self, source_name: str) -> SourceConfig:
        """Get configuration for a named source, with API key resolved."""
        if source_name not in self.sources:
            raise ValueError(f"Unknown source: {source_name}. Available: {list(self.sources)}")
        config = self.sources[source_name]
        # Resolve API key from environment if not already set
        if not config.api_key and config.api_key_env:
            config.api_key = os.environ.get(config.api_key_env, "")
        return config


def _find_config_dir() -> Path:
    """Find the config directory, searching up from cwd."""
    candidates = [
        Path.cwd() / "config",
        Path(__file__).resolve().parent.parent.parent.parent / "config",
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return Path.cwd() / "config"


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file, returning empty dict if not found."""
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def load_settings() -> GridflowConfig:
    """Load the complete gridflow configuration.

    Merges settings.yaml, sources.yaml, environment variables, and .env file.
    """
    config_dir = _find_config_dir()
    load_dotenv(config_dir.parent / ".env", override=False)

    # Load YAML configs
    settings_data = _load_yaml(config_dir / "settings.yaml")
    sources_data = _load_yaml(config_dir / "sources.yaml")

    # Build pipeline settings (env vars override YAML)
    pipeline_yaml = settings_data.get("pipeline", {})
    pipeline_values = dict(pipeline_yaml)
    env_prefix = PipelineSettings.model_config.get("env_prefix", "")
    for field_name in PipelineSettings.model_fields:
        env_name = f"{env_prefix}{field_name}".upper()
        if env_name in os.environ:
            pipeline_values[field_name] = os.environ[env_name]
    pipeline = PipelineSettings(**pipeline_values)

    # Build quality config
    quality_yaml = settings_data.get("quality", {})
    quality = QualityConfig(**quality_yaml)

    # Build source configs
    sources: dict[str, SourceConfig] = {}
    for name, src_data in sources_data.get("sources", {}).items():
        sources[name] = SourceConfig(**src_data)

    # Resolve API keys from pipeline settings into source configs.
    # AGSI+ and ALSI are two GIE endpoints sharing one credential, so both
    # source configs map to the single gie_api_key field.
    key_map = {
        "elexon": pipeline.elexon_api_key,
        "entsoe": pipeline.entsoe_api_key,
        "entsog": pipeline.entsog_api_key,
        "gie_agsi": pipeline.gie_api_key,
        "gie_alsi": pipeline.gie_api_key,
    }
    for name, key in key_map.items():
        if name in sources and key:
            sources[name].api_key = key

    return GridflowConfig(pipeline=pipeline, quality=quality, sources=sources)
