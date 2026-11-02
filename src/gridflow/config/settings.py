"""Pydantic Settings model and configuration loading for gridflow.

Path resolution: the data root is ``GRIDFLOW_DATA_DIR``, defaulting to
``~/gridflow-data`` on every OS, so a checkout, an installed package, a
notebook kernel and a downstream repo all find the same data from any folder.
The catalogue and the logs sit under it unless set on their own. Relative
values resolve against the current folder.
"""

from __future__ import annotations

import os
import subprocess
from functools import cache
from importlib.resources import files
from pathlib import Path, PureWindowsPath
from typing import Any

# PyYAML ships no type stubs; types-PyYAML is a dev-only stub, not a runtime dependency.
import yaml  # type: ignore[import-untyped]
from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings


@cache
def _project_root() -> Path:
    """Resolve the project root via `git rev-parse --git-common-dir`.

    Used only by the ``reset``/``prune`` containment guard, which refuses to
    delete an ancestor of the checkout; data paths do not depend on it.

    Worktree-safe: from a worktree, `--git-common-dir` returns the
    canonical .git directory; the project root is its parent.

    Falls back to walk-up-for-pyproject.toml then Path.cwd() if git
    is unavailable (CI minimal containers, installed packages).
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
    # DuckDB's memory cap for the checks; past it a large aggregation spills to
    # a temporary directory. DuckDB fills the cap before spilling and the process
    # peaks some 0.7 GB above it: `quality --all` on the live catalogue peaked at
    # 2.7 GB under 2GB, which a 16 GB machine shared with other jobs can spare.
    duckdb_memory_limit: str = "2GB"


class PipelineSettings(BaseSettings):
    """Main pipeline settings, read from ``GRIDFLOW_*`` environment variables.

    ``load_settings`` loads the nearest ``.env`` first. `_resolve_paths` puts
    an unset `duckdb_path` and `log_dir` under `data_dir` and resolves relative
    values against the current folder. A drive-relative Windows path such as
    `C:x` is refused.
    """

    model_config = {"env_prefix": "GRIDFLOW_", "extra": "ignore"}

    # A factory, so the home folder is read when settings load, not at import.
    data_dir: Path = Field(default_factory=lambda: Path.home() / "gridflow-data")
    # Unless set, these two are relative to data_dir (see _resolve_paths).
    log_dir: Path = Path("logs")
    duckdb_path: Path = Path("gridflow.duckdb")
    # This generic default is unchanged; the runner separately warns when the
    # effective transformed dates are insufficient for a dataset's declared
    # positive partition-source offsets.
    default_lookback_hours: int = 24
    # Incremental ingest re-fetches from `watermark - incremental_overlap_hours`
    # to recover late/revised publications (run_type II->SF->R1) and any window
    # that was empty at fetch time (publication lag). Re-fetching is safe: bronze
    # is immutable and silver dedups on (date, period, run_type), so re-fetching
    # the recent past adds bronze bytes without corrupting silver.
    #
    # Default 72h (3 days): covers weekend publication lag + the initial Elexon
    # II->SF->R1 revision burst. It is the backstop for the R3-F04 fix — the
    # watermark no longer advances past empty/partial evidence — AND, since C-8,
    # for a 200 response carrying a parsed, empty record array from a connector
    # that stamps `record_count` (detected directly at the ingest boundary, no
    # longer only self-healed by re-fetching). For a connector that cannot
    # supply a count without a new parse (D-9), or on a parse failure, the
    # overlap re-fetch remains the only defence. It does NOT cover weeks-long
    # settlement revision tails: complete revision capture still needs a
    # periodic backfill. CLAUDE.md treats settlement revisions as first-class,
    # so do not lower this to 0 without a backfill schedule in place.
    incremental_overlap_hours: int = 72
    # F-09: the incremental fetch window can never span more than this many
    # hours, however long the frontier has been frozen. Two derived trip
    # points (D-1): the clamp fires (quiet, window still advances) at
    # staleness > max_incremental_lookback_hours - incremental_overlap_hours
    # = 96h; the coverage predicate denies the advance (alarming, R-8) at
    # staleness > max_incremental_lookback_hours = 168h. NOT applied to
    # backfill or explicit --start/--end windows (D-3) -- those are never
    # clamped, so the gap-repair command can never be clamped shut.
    max_incremental_lookback_hours: int = 168
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

    @model_validator(mode="after")
    def _resolve_paths(self) -> PipelineSettings:
        # data_dir first: the other two may be placed under it.
        for name in ("data_dir", "duckdb_path", "log_dir"):
            path: Path = getattr(self, name)
            # `C:x` has a drive but no root, so it is relative and would be joined
            # to the current folder with no error; an unquoted `C:\x` loses its
            # backslash in a POSIX shell and arrives this way. The text is read as
            # a Windows path on every OS so the Linux CI runner exercises the
            # refusal (Python 3.11 drops the separator when converting a PosixPath).
            windows = PureWindowsPath(str(path))
            if windows.drive and not windows.root:
                raise ValueError(
                    f"{name} {str(path)!r} is drive-relative (a drive with no root); "
                    "write it as an absolute path with forward slashes, e.g. D:/gridflow-data"
                )
            if name != "data_dir" and name not in self.model_fields_set:
                path = self.data_dir / path
            if not path.is_absolute():
                path = (Path.cwd() / path).resolve()
            object.__setattr__(self, name, path)
        return self

    @model_validator(mode="after")
    def _validate_incremental_bounds(self) -> PipelineSettings:
        """Guard the F-09 clamp/coverage constants against config foot-guns (R-7).

        Three clauses, each naming the offending values:

        Clause 1 -- ``incremental_overlap_hours <= max_incremental_lookback_hours``.
        Otherwise the clamp trip point (``max_lookback - overlap``) goes
        negative, so every incremental run would be denied regardless of
        staleness.

        Clause 2 (D-11) -- ``default_lookback_hours <= max_incremental_lookback_hours``.
        Makes a first run's span ``<= max_lookback``, so "a first run never
        clamps" is a *derived* property rather than an assumption.

        Clause 3 (D-15) -- non-negativity / per-constant floor:
        ``incremental_overlap_hours >= 0`` (zero is documented
        behaviour-preserving); ``default_lookback_hours >= 1`` (zero yields a
        zero-width first-run window that silently ingests nothing);
        ``max_incremental_lookback_hours >= 1`` (asserted so each constant
        carries its own floor, even though clause 1/2 imply it when the other
        constants are positive).
        """
        if self.incremental_overlap_hours < 0:
            raise ValueError(
                f"incremental_overlap_hours ({self.incremental_overlap_hours}) must be >= 0."
            )
        if self.default_lookback_hours < 1:
            raise ValueError(
                f"default_lookback_hours ({self.default_lookback_hours}) must be >= 1 "
                "(zero yields a zero-width first-run window that silently ingests nothing)."
            )
        if self.max_incremental_lookback_hours < 1:
            raise ValueError(
                "max_incremental_lookback_hours "
                f"({self.max_incremental_lookback_hours}) must be >= 1."
            )
        if self.incremental_overlap_hours > self.max_incremental_lookback_hours:
            raise ValueError(
                f"incremental_overlap_hours ({self.incremental_overlap_hours}) must be "
                f"<= max_incremental_lookback_hours ({self.max_incremental_lookback_hours}); "
                "otherwise the clamp trip point (max_lookback - overlap) goes negative."
            )
        if self.default_lookback_hours > self.max_incremental_lookback_hours:
            raise ValueError(
                f"default_lookback_hours ({self.default_lookback_hours}) must be <= "
                f"max_incremental_lookback_hours ({self.max_incremental_lookback_hours}); "
                "otherwise a first run's own span could exceed the clamp (D-11)."
            )
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
    """The folder of the optional ``settings.yaml`` override: ``config/`` in the current folder."""
    return Path.cwd() / "config"


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file, returning empty dict if not found."""
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _neso_portal_datasets() -> dict[str, DatasetConfig]:
    """One dataset per NESO portal registry family, for a sources.yaml that lists none.

    The legacy keys come first, in their historical order, then the rest
    sorted: the order ``ingest --all`` has always walked. ``max_query_days`` is
    0 because a window never selects anything for this source (one whole-file
    capture per run), so the client's span notice stays quiet.
    """
    # Imported here: the connector package is heavy and imports nothing from settings.
    from gridflow.connectors.neso_data_portal import registry

    families = registry.load_registry().families
    legacy = [key for key in registry.LEGACY_KEYS if key in families]
    rest = sorted(key for key in families if key not in registry.LEGACY_KEYS)
    return {
        key: DatasetConfig(
            endpoint="/api/3/action/package_show", schedule="daily", max_query_days=0
        )
        for key in (*legacy, *rest)
    }


def load_settings() -> GridflowConfig:
    """Load the complete gridflow configuration.

    Merges the optional ``config/settings.yaml`` in the current folder, the
    packaged sources.yaml, environment variables, and the nearest ``.env``.
    Pipeline settings take the environment first, then settings.yaml, then the
    code defaults.
    The ``neso_data_portal`` datasets come from its registry unless
    sources.yaml lists some.
    """
    config_dir = _find_config_dir()
    # The nearest .env at or above the current folder, as for any tool run from
    # a project; an installed gridflow has no checkout of its own to look in.
    load_dotenv(find_dotenv(usecwd=True), override=False)

    # Load YAML configs
    settings_data = _load_yaml(config_dir / "settings.yaml")
    # Sources are code (endpoints, pacing) and ship inside the package.
    sources_data = yaml.safe_load(
        files("gridflow.config").joinpath("sources.yaml").read_text(encoding="utf-8")
    )

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
        if name == "neso_data_portal" and not src_data.get("datasets"):
            src_data = {**src_data, "datasets": _neso_portal_datasets()}
        sources[name] = SourceConfig(**src_data)

    return GridflowConfig(pipeline=pipeline, quality=quality, sources=sources)
