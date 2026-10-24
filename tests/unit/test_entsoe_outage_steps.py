# ruff: noqa: E501  (vendor XML element names are long)
"""Outage silver: PT1M steps, document identity, revisions and day ownership.

Regression tests for backlog 13ai (PT1M unmapped, stamps reaching 2069),
13aj (identity, revision, status, end, generation unit and nominal power
dropped), 13ak (different documents collapsed in parse order) and 13al (each
outage repeated in every daily file it overlaps). The XML follows the shape of
real A80 bronze (outages_generation/2026/09/08), trimmed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.entsoe._outages import clip_to_day, parse_outage_xml, read_outage_partition
from gridflow.silver.entsoe.outages_generation import OutagesGenerationTransformer
from gridflow.silver.entsoe.outages_h7 import OutagesProductionTransformer

if TYPE_CHECKING:
    from pathlib import Path

_NS = "urn:iec62325.351:tc57wg16:451-6:outagedocument:3:0"


def _a80(
    mrid: str,
    revision: int,
    *,
    start: str = "2026-08-28T22:00Z",
    end: str = "2026-12-03T23:00Z",
    points: tuple[tuple[int, float], ...] = ((1, 0.0),),
    cancelled: bool = False,
    unit: str = "17W100P100P0233M",
    generation_unit: str = "17W100P100P0152M",
) -> bytes:
    status = "<docStatus><value>A09</value></docStatus>" if cancelled else ""
    pts = "".join(
        f"<Point><position>{p}</position><quantity>{q}</quantity></Point>" for p, q in points
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Unavailability_MarketDocument xmlns="{_NS}">
  <mRID>{mrid}</mRID>
  <revisionNumber>{revision}</revisionNumber>
  <type>A80</type>
  <createdDateTime>2026-01-29T10:45:{revision:02d}Z</createdDateTime>
  <unavailability_Time_Period.timeInterval><start>{start}</start><end>{end}</end>
  </unavailability_Time_Period.timeInterval>
  {status}
  <TimeSeries>
    <mRID>1</mRID>
    <businessType>A53</businessType>
    <biddingZone_Domain.mRID codingScheme="A01">10YFR-RTE------C</biddingZone_Domain.mRID>
    <curveType>A03</curveType>
    <production_RegisteredResource.mRID codingScheme="A01">{unit}</production_RegisteredResource.mRID>
    <production_RegisteredResource.name>TRICASTIN 3</production_RegisteredResource.name>
    <production_RegisteredResource.pSRType.psrType>B14</production_RegisteredResource.pSRType.psrType>
    <production_RegisteredResource.pSRType.powerSystemResources.mRID codingScheme="A01">{generation_unit}</production_RegisteredResource.pSRType.powerSystemResources.mRID>
    <production_RegisteredResource.pSRType.powerSystemResources.name>TRICASTIN 3</production_RegisteredResource.pSRType.powerSystemResources.name>
    <production_RegisteredResource.pSRType.powerSystemResources.nominalP unit="MAW">915.0</production_RegisteredResource.pSRType.powerSystemResources.nominalP>
    <Available_Period>
      <timeInterval><start>{start}</start><end>{end}</end></timeInterval>
      <resolution>PT1M</resolution>
      {pts}
    </Available_Period>
  </TimeSeries>
</Unavailability_MarketDocument>""".encode()


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def _transform(*bodies: bytes) -> pl.DataFrame:
    rows = [row for body in bodies for row in parse_outage_xml(body)]
    transformer = OutagesGenerationTransformer.__new__(OutagesGenerationTransformer)
    return transformer.transform(pl.DataFrame(rows, infer_schema_length=None))


class TestPt1mResolution:
    def test_forward_fill_steps_by_minutes_not_hours(self) -> None:
        """13l/13ai: PT1M was unmapped, so position 16 landed 15 hours in."""
        body = _a80(
            "doc",
            1,
            start="2026-08-01T00:00Z",
            end="2026-08-01T00:45Z",
            points=((1, 328.16), (16, 195.4), (31, 164.36)),
        )
        records = parse_timeseries_xml(body, value_tag="quantity")
        stamps = sorted(r["timestamp_utc"] for r in records)
        assert len(stamps) == 45
        assert stamps[-1] == _utc("2026-08-01T00:44")

    def test_step_mode_emits_one_row_per_declared_point_with_its_end(self) -> None:
        body = _a80(
            "doc",
            1,
            start="2026-09-01T00:00Z",
            end="2026-11-01T00:00Z",
            points=((1, 0.0), (20161, 450.0)),  # second step two weeks in
        )
        steps = parse_outage_xml(body)
        assert [(s["timestamp_utc"], s["step_end_utc"]) for s in steps] == [
            (_utc("2026-09-01T00:00"), _utc("2026-09-15T00:00")),
            (_utc("2026-09-15T00:00"), _utc("2026-11-01T00:00")),
        ]
        assert max(s["step_end_utc"] for s in steps) < _utc("2027-01-01T00:00")


class TestOutageIdentity:
    def test_identity_revision_status_and_unit_columns_survive(self) -> None:
        """13aj: silver kept none of these."""
        out = _transform(_a80("doc-a", 8, cancelled=True))
        row = out.row(0, named=True)
        assert row["document_mrid"] == "doc-a"
        assert row["revision_number"] == 8
        assert row["document_status"] == "A09"
        assert row["production_type"] == "B14"
        assert row["unit_mrid"] == "17W100P100P0233M"
        assert row["generation_unit_mrid"] == "17W100P100P0152M"
        assert row["nominal_mw"] == 915.0
        assert row["outage_start_utc"] == _utc("2026-08-28T22:00")
        assert row["outage_end_utc"] == _utc("2026-12-03T23:00")
        assert row["step_end_utc"] == _utc("2026-12-03T23:00")

    def test_two_documents_for_one_unit_both_survive(self) -> None:
        """13ak: the old (timestamp_utc, unit_mrid) key kept whichever parsed last."""
        out = _transform(_a80("cancelled-doc", 2, cancelled=True), _a80("active-doc", 1))
        assert sorted(out["document_mrid"].to_list()) == ["active-doc", "cancelled-doc"]

    def test_only_the_latest_revision_of_a_document_is_kept(self) -> None:
        older = _a80("doc", 3, points=((1, 100.0),))
        newer = _a80("doc", 4, points=((1, 0.0), (60, 200.0)))
        out = _transform(newer, older)
        assert out["revision_number"].unique().to_list() == [4]
        assert out.height == 2

    def test_repeated_fetches_of_one_revision_collapse(self) -> None:
        body = _a80("doc", 1)
        assert _transform(body, body).height == 1


class TestDayOwnership:
    def test_clip_keeps_only_the_partition_day(self) -> None:
        steps = pl.DataFrame(parse_outage_xml(_a80("doc", 1)))
        clipped = clip_to_day(steps, date(2026, 9, 10))
        assert clipped.select("timestamp_utc", "step_end_utc").rows() == [
            (_utc("2026-09-10T00:00"), _utc("2026-09-11T00:00"))
        ]

    def test_query_over_two_days_returns_each_outage_minute_once(self, tmp_path: Path) -> None:
        """13al: both daily files held the whole outage, so a two-day read doubled it."""
        body = _a80("doc", 1)
        frames = []
        for day in (date(2026, 9, 10), date(2026, 9, 11)):
            part = tmp_path / f"{day:%Y%m%d}"
            part.mkdir()
            (part / "raw_x.xml").write_bytes(body)
            frames.append(read_outage_partition(part, day))
        both = pl.concat(frames, how="diagonal")
        assert both.height == 2
        assert both["timestamp_utc"].n_unique() == 2
        assert (both["step_end_utc"] - both["timestamp_utc"]).dt.total_hours().to_list() == [24, 24]

    def test_production_table_keys_on_document(self) -> None:
        """13ak: production's timeseries_mrid is always "1", so two documents collided."""
        rows = [
            r
            for body in (_a80("doc-a", 1), _a80("doc-b", 1))
            for r in parse_outage_xml(body.replace(b">A80<", b">A77<"))
        ]
        transformer = OutagesProductionTransformer.__new__(OutagesProductionTransformer)
        out = transformer.transform(pl.DataFrame(rows, infer_schema_length=None))
        assert out.height == 2
        assert out["nominal_mw"].to_list() == [915.0, 915.0]
