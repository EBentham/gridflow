"""Unit tests for REMIT revision preservation.

Pre-F7 the silver transformer collapsed multiple revisions of the same `mrid`
to a single row. F7 removes that deduplication so every revision is preserved
in silver. Latest-revision selection becomes a read-time concern.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from gridflow.silver.elexon.remit import REMITTransformer


def _make_transformer() -> REMITTransformer:
    """Construct a REMIT transformer without touching the filesystem."""
    t = REMITTransformer.__new__(REMITTransformer)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/elexon/remit")
    t.silver_dir = Path("/tmp/test/silver/elexon/remit")
    return t


def _three_revision_raw() -> pl.DataFrame:
    """Three rows: MSG-001 rev 1, MSG-001 rev 2, MSG-002 rev 1."""
    rows = [
        {
            "mrid": "MSG-001",
            "revisionNumber": 1,
            "publishTime": "2024-01-15T08:00:00Z",
            "messageType": "Production unavailability",
            "fuelType": "Gas",
            "normalCapacity": 500.0,
            "availableCapacity": 0.0,
            "unavailableCapacity": 500.0,
            "eventStartTime": "2024-01-16T00:00:00Z",
            "eventEndTime": "2024-01-20T00:00:00Z",
        },
        {
            "mrid": "MSG-001",
            "revisionNumber": 2,
            "publishTime": "2024-01-15T12:00:00Z",
            "messageType": "Production unavailability",
            "fuelType": "Gas",
            "normalCapacity": 500.0,
            "availableCapacity": 0.0,
            "unavailableCapacity": 500.0,
            "eventStartTime": "2024-01-16T00:00:00Z",
            "eventEndTime": "2024-01-21T00:00:00Z",
        },
        {
            "mrid": "MSG-002",
            "revisionNumber": 1,
            "publishTime": "2024-01-15T10:00:00Z",
            "messageType": "Production unavailability",
            "fuelType": "Coal",
            "normalCapacity": 700.0,
            "availableCapacity": 0.0,
            "unavailableCapacity": 700.0,
            "eventStartTime": "2024-01-17T00:00:00Z",
            "eventEndTime": "2024-01-19T00:00:00Z",
        },
    ]
    return pl.DataFrame(rows)


def test_remit_transform_keeps_all_revisions() -> None:
    transformer = _make_transformer()
    out = transformer.transform(_three_revision_raw())

    assert len(out) == 3
    msg_001 = out.filter(pl.col("mrid") == "MSG-001")
    assert sorted(msg_001["revision_number"].to_list()) == [1, 2]


def test_remit_transform_does_not_collapse_duplicate_mrid() -> None:
    """The pre-F7 line `df.unique(subset=["mrid"], keep="last")` is gone."""
    transformer = _make_transformer()
    raw = _three_revision_raw()
    out = transformer.transform(raw)

    # Same mrid across multiple revisions is a feature now, not a duplicate.
    mrid_counts = out.group_by("mrid").len().sort("mrid")
    counts = {
        row["mrid"]: row["len"] for row in mrid_counts.iter_rows(named=True)
    }
    assert counts["MSG-001"] == 2
    assert counts["MSG-002"] == 1


def test_remit_transformer_declares_append_only_and_version() -> None:
    """Class attributes encode the F7 contract."""
    assert REMITTransformer.APPEND_ONLY is True
    assert REMITTransformer.DATASET_VERSION == "2.0.0"
