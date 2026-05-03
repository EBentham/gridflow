"""Tests for the ENTSO-E endpoint catalog gap matrix."""

from __future__ import annotations

from pathlib import Path

import yaml

from gridflow.connectors.entsoe.endpoints import DOC_TYPES

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = PROJECT_ROOT / "docs" / "entsoe_endpoint_catalog.yaml"
VALID_STATUSES = {"implemented", "planned", "deferred", "excluded"}
REQUIRED_FIELDS = {
    "area_params",
    "batch",
    "business_type",
    "dataset",
    "document_type",
    "domain",
    "max_window",
    "name",
    "optional_filters",
    "parser_family",
    "process_type",
    "reason",
    "status",
}


def _catalog_entries() -> list[dict[str, object]]:
    data = yaml.safe_load(CATALOG_PATH.read_text())
    return data["entries"]


def test_catalog_classifies_official_collection_entries() -> None:
    entries = _catalog_entries()

    assert len(entries) >= 60
    for entry in entries:
        assert REQUIRED_FIELDS.issubset(entry)
        assert entry["status"] in VALID_STATUSES
        if entry["status"] in {"planned", "deferred", "excluded"}:
            assert entry["reason"]


def test_implemented_catalog_entries_match_active_doc_types() -> None:
    entries = _catalog_entries()
    implemented = {
        str(entry["dataset"])
        for entry in entries
        if entry["status"] == "implemented"
    }

    assert implemented == set(DOC_TYPES)


def test_active_doc_types_have_catalog_metadata() -> None:
    entries = _catalog_entries()
    by_dataset = {
        str(entry["dataset"]): entry
        for entry in entries
        if entry["status"] == "implemented"
    }

    for dataset, doc_type in DOC_TYPES.items():
        entry = by_dataset[dataset]
        assert entry["document_type"] == doc_type.document_type
        assert entry["process_type"] == doc_type.process_type

