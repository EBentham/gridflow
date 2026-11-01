"""Balancing silver: document-level area, direction, product, price and fetch choice.

Regression tests for backlog 13l (current balancing state: 1-minute steps,
zone and direction), 13o (procured capacity: area and agreement type empty,
direction and price dropped, arbitrary last point kept), 13q (bids: product
missing from the key, price dropped) and 13r (aggregated bids: area empty).
The XML follows the shape of real bronze from 2026-08-01 and 2026-09-22.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.entsoe.h8_balancing import (
    AggregatedBalancingEnergyBidsTransformer,
    BalancingEnergyBidsTransformer,
    CurrentBalancingStateTransformer,
    ProcuredBalancingCapacityTransformer,
    _latest_fetch_files,
)

if TYPE_CHECKING:
    from pathlib import Path

_BAL_NS = "urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:1"
_BID_NS = "urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2"


def _balancing_doc(doc_type: str, area: str, series: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Balancing_MarketDocument xmlns="{_BAL_NS}">
  <mRID>7e108462ea41463ea334192d12d0c223</mRID>
  <revisionNumber>1</revisionNumber>
  <type>{doc_type}</type>
  <createdDateTime>2026-08-16T14:10:54Z</createdDateTime>
  <area_Domain.mRID codingScheme="A01">{area}</area_Domain.mRID>
  <period.timeInterval><start>2026-08-01T00:00Z</start><end>2026-08-02T00:00Z</end></period.timeInterval>
  {series}
</Balancing_MarketDocument>""".encode()


def _transform(cls: type, body: bytes) -> pl.DataFrame:
    transformer = cls.__new__(cls)
    records = parse_timeseries_xml(body, value_tag="quantity")
    return transformer.transform(pl.DataFrame(records, infer_schema_length=None))


def _cbs_series(mrid: int, direction: str, start: str, end: str, points: str) -> str:
    return f"""<TimeSeries>
    <mRID>{mrid}</mRID><businessType>B33</businessType>
    <flowDirection.direction>{direction}</flowDirection.direction>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name><curveType>A03</curveType>
    <Period><timeInterval><start>{start}</start><end>{end}</end></timeInterval>
      <resolution>PT1M</resolution>{points}</Period>
  </TimeSeries>"""


def _point(position: int, quantity: float, price: str = "") -> str:
    return f"<Point><position>{position}</position><quantity>{quantity}</quantity>{price}</Point>"


class TestCurrentBalancingState:
    def test_minutes_zone_and_direction(self) -> None:
        body = _balancing_doc(
            "A86",
            "10YFR-RTE------C",
            _cbs_series(
                1,
                "A02",
                "2026-08-01T00:00Z",
                "2026-08-01T00:45Z",
                _point(1, 328.16) + _point(16, 195.4) + _point(31, 164.36),
            )
            + _cbs_series(2, "A01", "2026-08-01T00:45Z", "2026-08-01T01:00Z", _point(1, 93.4)),
        )
        out = _transform(CurrentBalancingStateTransformer, body)
        assert out.height == 60
        assert out["area_code"].unique().to_list() == ["10YFR-RTE------C"]
        assert dict(out["direction"].value_counts().rows()) == {"A02": 45, "A01": 15}
        at = out.filter(pl.col("timestamp_utc").dt.strftime("%H:%M") == "00:15")
        assert at["quantity_mw"].to_list() == [195.4]


def _procured_series(mrid: int, quantity: float, price: float) -> str:
    return f"""<TimeSeries>
    <mRID>{mrid}</mRID><businessType>B95</businessType>
    <type_MarketAgreement.type>A01</type_MarketAgreement.type>
    <standard_MarketProduct.marketProductType>A01</standard_MarketProduct.marketProductType>
    <mktPSRType.psrType>A03</mktPSRType.psrType>
    <flowDirection.direction>A02</flowDirection.direction>
    <currency_Unit.name>EUR</currency_Unit.name>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name><curveType>A01</curveType>
    <Period><timeInterval><start>2026-08-01T00:00Z</start><end>2026-08-01T00:15Z</end></timeInterval>
      <resolution>PT15M</resolution>
      {_point(1, quantity, f"<procurement_Price.amount>{price}</procurement_Price.amount>")}
    </Period>
  </TimeSeries>"""


class TestProcuredBalancingCapacity:
    def test_area_agreement_direction_price_and_every_offer(self) -> None:
        body = _balancing_doc(
            "A15",
            "10YNL----------L",
            _procured_series(1, 3.0, 0.0) + _procured_series(2, 5.0, 13.75),
        )
        out = _transform(ProcuredBalancingCapacityTransformer, body).sort("series_mrid")
        assert out.height == 2  # the old key kept one arbitrary offer
        assert out["area_code"].unique().to_list() == ["10YNL----------L"]
        assert out["market_agreement_type"].unique().to_list() == ["A01"]
        assert out["direction"].unique().to_list() == ["A02"]
        assert out["psr_type"].unique().to_list() == ["A03"]
        assert out["procurement_price"].to_list() == [0.0, 13.75]
        assert out["quantity_mw"].sum() == 8.0


class TestAggregatedBids:
    def test_area_from_document_and_both_directions(self) -> None:
        series = "".join(
            f"""<TimeSeries><mRID>{i}</mRID><businessType>A14</businessType>
            <standard_MarketProduct.marketProductType>A01</standard_MarketProduct.marketProductType>
            <flowDirection.direction>{d}</flowDirection.direction><curveType>A03</curveType>
            <Period><timeInterval><start>2026-08-01T00:00Z</start><end>2026-08-01T00:15Z</end>
            </timeInterval><resolution>PT15M</resolution>{_point(1, q)}</Period></TimeSeries>"""
            for i, d, q in ((1, "A02", 390.0), (2, "A01", 379.0))
        )
        out = _transform(
            AggregatedBalancingEnergyBidsTransformer,
            _balancing_doc("A24", "10YBE----------2", series),
        )
        assert out.height == 2
        assert out["area_code"].unique().to_list() == ["10YBE----------2"]


class TestBalancingEnergyBids:
    def test_one_bid_with_two_products_keeps_both_with_prices(self) -> None:
        def bid(product: str, quantity: float, price: float) -> str:
            return f"""<Bid_TimeSeries>
      <mRID>58cc097a8d283cbaa694d92c3aafdc0c1</mRID><businessType>B74</businessType>
      <connecting_Domain.mRID codingScheme="A01">10YBE----------2</connecting_Domain.mRID>
      <currency_Unit.name>EUR</currency_Unit.name>
      <flowDirection.direction>A01</flowDirection.direction>
      <standard_MarketProduct.marketProductType>{product}</standard_MarketProduct.marketProductType>
      <Period><timeInterval><start>2026-08-01T00:00Z</start><end>2026-08-01T00:15Z</end></timeInterval>
        <resolution>PT15M</resolution>
        <Point><position>1</position><quantity.quantity>{quantity}</quantity.quantity>
          <energy_Price.amount>{price}</energy_Price.amount></Point>
      </Period>
    </Bid_TimeSeries>"""

        body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="{_BID_NS}">
  <mRID>741a74838d1d4cad8fa4aa9282d79da1</mRID><revisionNumber>1</revisionNumber><type>A37</type>
  <createdDateTime>2026-08-16T14:12:08Z</createdDateTime>
  {bid("A05", 9.0, -300.0)}{bid("A07", 19.0, 120.5)}
</ReserveBid_MarketDocument>""".encode()
        out = _transform(BalancingEnergyBidsTransformer, body).sort("standard_market_product")
        assert out.height == 2  # the old key dropped one product
        assert out["energy_price"].to_list() == [-300.0, 120.5]
        assert out["currency_unit"].unique().to_list() == ["EUR"]


class TestLatestFetchPerRequest:
    @staticmethod
    def _body(
        tmp_path: Path,
        name: str,
        fetched_at: str,
        params: dict[str, str] | None,
    ) -> Path:
        meta: dict[str, object] = {"fetched_at": fetched_at}
        if params is not None:
            meta["request_params"] = params
        (tmp_path / f"{name}.meta.json").write_text(json.dumps(meta))
        path = tmp_path / f"{name}.xml"
        path.write_bytes(b"<x/>")
        return path

    def test_keeps_every_zip_entry_and_page_of_the_latest_fetch_only(self, tmp_path: Path) -> None:
        def req(area: str, offset: str, entry: str | None = None) -> dict[str, str]:
            params = {"documentType": "A37", "connecting_Domain": area, "offset": offset}
            if entry:
                params["zip_entry"] = entry
            return params

        b = self._body
        old = b(tmp_path, "raw_1", "2026-08-16T14:12:07.979160+00:00", req("FR", "0", "001"))
        new_a = b(tmp_path, "raw_2", "2026-08-20T09:00:00.000010+00:00", req("FR", "0", "001"))
        new_b = b(tmp_path, "raw_3", "2026-08-20T09:00:00.000002+00:00", req("FR", "0", "002"))
        new_p2 = b(tmp_path, "raw_4", "2026-08-20T09:02:30+00:00", req("FR", "100"))
        other = b(tmp_path, "raw_5", "2026-08-16T14:12:11.401764+00:00", req("NL", "0"))
        files = [old, new_a, new_b, new_p2, other]
        assert _latest_fetch_files(files) == [new_a, new_b, new_p2, other]

    def test_a_shorter_refetch_drops_the_older_trailing_pages(self, tmp_path: Path) -> None:
        """Review finding: keying on offset kept page three of a 250-offer fetch
        after a 150-offer re-fetch, so silver held offers that no longer exist."""

        def req(offset: str) -> dict[str, str]:
            return {"documentType": "A15", "area_Domain": "FR", "offset": offset}

        old = [
            self._body(tmp_path, f"raw_old{o}", f"2026-08-16T14:00:0{i}+00:00", req(o))
            for i, o in enumerate(("0", "100", "200"))
        ]
        new = [
            self._body(tmp_path, f"raw_new{o}", f"2026-08-20T09:00:0{i}+00:00", req(o))
            for i, o in enumerate(("0", "100"))
        ]
        assert _latest_fetch_files([*old, *new]) == new

    def test_bodies_without_request_identity_or_aware_time_are_kept(self, tmp_path: Path) -> None:
        """Review findings: empty params merged unrelated bodies, and a naive
        fetched_at crashed the comparison."""
        b = self._body
        fr = b(tmp_path, "raw_1", "2026-08-16T14:00:00+00:00", {})
        nl = b(tmp_path, "raw_2", "2026-08-16T14:02:00+00:00", None)
        aware = b(tmp_path, "raw_3", "2026-08-16T14:03:00+00:00", {"area_Domain": "BE"})
        naive = b(tmp_path, "raw_4", "2026-08-16T14:04:00", {"area_Domain": "BE"})
        assert _latest_fetch_files([fr, nl, aware, naive]) == [fr, nl, aware, naive]
