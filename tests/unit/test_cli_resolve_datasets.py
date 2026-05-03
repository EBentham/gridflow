"""Unit tests for _resolve_datasets CLI helper."""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

from gridflow.cli import _resolve_datasets
from gridflow.config.settings import load_settings


class TestAllPositionalAlias:
    def test_lowercase_all_treated_as_flag(self, sample_config):
        """Positional 'all' expands to all datasets for the source."""
        result = _resolve_datasets("elexon", "all", False, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())

    def test_uppercase_all_treated_as_flag(self, sample_config):
        """Positional 'ALL' (case-insensitive) also expands to all datasets."""
        result = _resolve_datasets("elexon", "ALL", False, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())

    def test_mixed_case_all_treated_as_flag(self, sample_config):
        """Positional 'All' (mixed case) also expands to all datasets."""
        result = _resolve_datasets("elexon", "All", False, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())


class TestEntsoeDatasetExpansion:
    def test_entsoe_all_expands_to_all_configured_datasets(self):
        """ENTSO-E positional 'all' expands to all configured datasets."""
        settings = load_settings()
        result = _resolve_datasets("entsoe", "all", False, settings)
        expected = list(settings.get_source_config("entsoe").datasets.keys())

        assert result == expected


class TestAllFlagBehaviourUnchanged:
    def test_all_flag_true_expands_datasets(self, sample_config):
        """Existing --all flag behaviour is preserved."""
        result = _resolve_datasets("elexon", None, True, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())

    def test_all_flag_true_with_dataset_string_expands(self, sample_config):
        """When all_flag=True, dataset string is irrelevant - all datasets returned."""
        result = _resolve_datasets("elexon", "system_prices", True, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())


class TestSpecificDataset:
    def test_specific_dataset_returned_as_single_item_list(self, sample_config):
        """A named dataset returns a one-element list."""
        result = _resolve_datasets("elexon", "system_prices", False, sample_config)
        assert result == ["system_prices"]


class TestErrorPaths:
    def test_no_dataset_no_flag_raises_bad_parameter(self, sample_config):
        """No dataset and no --all flag raises typer.BadParameter."""
        with pytest.raises(typer.BadParameter):
            _resolve_datasets("elexon", None, False, sample_config)

    def test_invalid_settings_type_raises_type_error(self):
        """Non-GridflowConfig settings object raises TypeError."""
        with pytest.raises(TypeError):
            _resolve_datasets("elexon", "system_prices", False, object())


class TestSettingsEnvOverrides:
    def test_gridflow_pipeline_env_vars_override_yaml_paths(self, monkeypatch, tmp_path):
        """Runtime GRIDFLOW_* paths should isolate CLI runs from config YAML defaults."""
        data_dir = tmp_path / "data"
        duckdb_path = tmp_path / "catalogue" / "gridflow.duckdb"
        log_dir = tmp_path / "logs"

        monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
        monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(duckdb_path))
        monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(log_dir))

        settings = load_settings()

        assert settings.pipeline.data_dir == Path(data_dir)
        assert settings.pipeline.duckdb_path == Path(duckdb_path)
        assert settings.pipeline.log_dir == Path(log_dir)
