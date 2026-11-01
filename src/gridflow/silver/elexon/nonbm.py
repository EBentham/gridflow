"""Silver transformer for Elexon Non-BM STOR Generation (NONBM)."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.schemas.elexon import ElexonNonBM
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# NONBM ignores publishDateTimeFrom/To and returns its latest record whatever
# the window (connectors/elexon/endpoints.py), so only a body whose sidecar
# shows it was fetched with from/to is a response for its partition. Fail
# closed: a body with no readable proof is excluded, because the stale record
# looks like valid data. Excluded bodies stay in bronze (immutable).
_HONOURED_WINDOW_PARAMS = frozenset({"from", "to"})
_IGNORED_WINDOW_PARAMS = frozenset({"publishDateTimeFrom", "publishDateTimeTo"})


def _window_exclusion_reason(raw_path: Path) -> str | None:
    """Say why a NONBM bronze body cannot be trusted to match its window.

    Args:
        raw_path: Path of the ``raw_*.json`` body; its sidecar sits beside it.

    Returns:
        None when the sidecar records ``from``/``to`` and none of the ignored
        names, else a short reason for the exclusion warning.
    """
    try:
        meta = json.loads(raw_path.with_suffix(".meta.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        return f"its sidecar cannot be read ({e})"
    params = meta.get("request_params") if isinstance(meta, dict) else None
    if not isinstance(params, dict):
        return "its sidecar records no request_params"
    if not _IGNORED_WINDOW_PARAMS.isdisjoint(params):
        return "it was fetched with publishDateTimeFrom/To, which the vendor ignores"
    if not _HONOURED_WINDOW_PARAMS.issubset(params):
        return "its sidecar records no from/to window"
    return None


class NONBMTransformer(ElexonTransformer):
    """Transform Elexon NONBM data from bronze to silver."""

    source = "elexon"
    dataset = "nonbm"
    schema_cls = ElexonNonBM
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "generation": "generation_mw",
    }
    REQUIRED = ("settlement_date", "settlement_period", "generation_mw")
    CASTS = {"generation_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "generation_mw",
        "published_at",
    )

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        records = super()._parse_bronze_body(path)
        reason = _window_exclusion_reason(path)
        if reason is not None:
            self.last_excluded_row_count += len(records)
            logger.warning("NONBM: excluded %d row(s) from %s: %s", len(records), path, reason)
            return []
        return records


register_transformer("elexon", "nonbm", NONBMTransformer)
