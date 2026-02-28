"""Initialise DuckDB catalogue + views."""

from __future__ import annotations

from pathlib import Path

from gridflow.config.settings import load_settings
from gridflow.storage.duckdb import init_catalogue

if __name__ == "__main__":
    settings = load_settings()
    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    print(f"DuckDB catalogue initialised at {settings.pipeline.duckdb_path}")
