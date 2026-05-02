"""Pydantic Settings model and configuration loading for gridflow."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


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


class PipelineSettings(BaseSettings):
    """Main pipeline settings, loaded from env vars and .env file."""

    model_config = {"env_prefix": "GRIDFLOW_", "env_file": ".env", "extra": "ignore"}

    data_dir: Path = Path("./data")
    log_dir: Path = Path("./logs")
    duckdb_path: Path = Path("./data/gridflow.duckdb")
    default_lookback_hours: int = 24
    max_concurrent_requests: int = 5
    log_level: str = "INFO"
    console_log_level: str = "WARNING"

    # Secrets (loaded from .env)
    elexon_api_key: str = Field(default="")
    entsoe_api_key: str = Field(default="")
    entsog_api_key: str = Field(default="")


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

    # Load YAML configs
    settings_data = _load_yaml(config_dir / "settings.yaml")
    sources_data = _load_yaml(config_dir / "sources.yaml")

    # Build pipeline settings (env vars override YAML)
    pipeline_yaml = settings_data.get("pipeline", {})
    pipeline = PipelineSettings(**pipeline_yaml)

    # Build quality config
    quality_yaml = settings_data.get("quality", {})
    quality = QualityConfig(**quality_yaml)

    # Build source configs
    sources: dict[str, SourceConfig] = {}
    for name, src_data in sources_data.get("sources", {}).items():
        sources[name] = SourceConfig(**src_data)

    # Resolve API keys from pipeline settings into source configs
    key_map = {
        "elexon": pipeline.elexon_api_key,
        "entsoe": pipeline.entsoe_api_key,
        "entsog": pipeline.entsog_api_key,
    }
    for name, key in key_map.items():
        if name in sources and key:
            sources[name].api_key = key

    return GridflowConfig(pipeline=pipeline, quality=quality, sources=sources)
