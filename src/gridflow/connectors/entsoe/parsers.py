"""XML parsers for ENTSO-E API responses."""

from __future__ import annotations

import contextlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Resolution code -> timedelta
_RESOLUTION_MAP: dict[str, timedelta] = {
    "PT15M": timedelta(minutes=15),
    "PT30M": timedelta(minutes=30),
    "PT60M": timedelta(hours=1),
    "P1D": timedelta(days=1),
    "P7D": timedelta(days=7),
    "P1M": timedelta(days=30),
    "P1Y": timedelta(days=365),
}


def _resolve_resolution(code: str) -> timedelta:
    return _RESOLUTION_MAP.get(code, timedelta(hours=1))


def _parse_utc(dt_str: str) -> datetime:
    """Parse ENTSO-E ISO datetime string to UTC datetime."""
    dt_str = dt_str.strip().rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse ENTSO-E datetime: {dt_str!r}")


def _strip_ns(tag: str) -> str:
    """Strip XML namespace from a tag name."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _first_child_text(el: Any, names: set[str]) -> str:
    for child in el:
        if _strip_ns(child.tag) in names:
            return (child.text or "").strip()
    return ""


def _root_document_metadata(root: Any) -> dict[str, str]:
    metadata = {
        "document_mrid": "",
        "revision_number": "",
        "document_status": "",
    }
    for child in root:
        tag = _strip_ns(child.tag)
        text = (child.text or "").strip()
        if tag == "mRID":
            metadata["document_mrid"] = text
        elif tag == "revisionNumber":
            metadata["revision_number"] = text
        elif tag in {"docStatus", "docStatus.value"}:
            metadata["document_status"] = text or _first_child_text(
                child, {"value"}
            )
    return metadata


def _matches_value_tag(tag: str, value_tag: str) -> bool:
    if tag == value_tag:
        return True
    if value_tag == "price.amount":
        return tag.endswith("_Price.amount")
    return False


def parse_timeseries_xml(
    xml_bytes: bytes,
    value_tag: str = "price.amount",
) -> list[dict[str, Any]]:
    """Parse a generic ENTSO-E TimeSeries XML response into records.

    Args:
        xml_bytes: Raw XML response body.
        value_tag: The tag name containing the numeric value (e.g.
            ``"price.amount"`` for day-ahead prices, ``"quantity"`` for load /
            generation / flows).

    Returns:
        List of dicts with keys: ``timestamp_utc``, ``value``,
        ``in_domain``, ``out_domain``, ``production_type``,
        ``control_area_domain``, ``business_type``, ``flow_direction``,
        ``resolution``, ``unit_mrid``, ``unit_name``. Fields absent from a
        given TimeSeries element are returned as empty strings
        (backward-compatible).
    """
    try:
        from lxml import etree  # type: ignore[import-untyped]
    except ImportError:
        logger.error("lxml not installed; cannot parse ENTSO-E XML")
        return []

    try:
        root = etree.fromstring(xml_bytes)  # noqa: S320 (trusted internal data)
    except etree.XMLSyntaxError as exc:
        logger.error("XML parse error: %s", exc)
        return []

    records: list[dict[str, Any]] = []
    document_metadata = _root_document_metadata(root)

    # Collect all TimeSeries elements (namespace-agnostic)
    for ts_el in root.iter():
        if _strip_ns(ts_el.tag) != "TimeSeries":
            continue

        # Extract domain codes and metadata fields
        in_domain = out_domain = production_type = ""
        control_area_domain = business_type = flow_direction = ""
        area_domain = connecting_domain = acquiring_domain = ""
        market_agreement_type = original_market_product = standard_market_product = ""
        unit_mrid = unit_name = ""
        timeseries_mrid = asset_mrid = asset_name = document_status = ""
        for child in ts_el:
            tag = _strip_ns(child.tag)
            text = (child.text or "").strip()
            if tag == "mRID":
                timeseries_mrid = text
            elif tag in (
                "in_Domain.mRID",
                "In_Domain.mRID",
                "inBiddingZone_Domain.mRID",
                "outBiddingZone_Domain.mRID",
                "BiddingZone_Domain.mRID",
                "biddingZone_Domain.mRID",
            ):
                in_domain = text
            elif tag in {"out_Domain.mRID", "Out_Domain.mRID"}:
                out_domain = text
            elif tag == "controlArea_Domain.mRID":
                control_area_domain = text
            elif tag in {"area_Domain.mRID", "Area_Domain.mRID", "area_domain.mRID"}:
                area_domain = text
            elif tag in {"connecting_Domain.mRID", "Connecting_Domain.mRID"}:
                connecting_domain = text
            elif tag in {"acquiring_Domain.mRID", "Acquiring_Domain.mRID"}:
                acquiring_domain = text
            elif tag == "businessType":
                business_type = text
            elif tag in {"docStatus", "docStatus.value"}:
                document_status = text or _first_child_text(child, {"value"})
            elif tag == "flowDirection.direction":
                flow_direction = text
            elif tag in {"Direction", "direction", "flowDirection"}:
                flow_direction = text or _first_child_text(child, {"direction", "value"})
            elif tag in {
                "Type_MarketAgreement.Type",
                "type_MarketAgreement.Type",
                "MarketAgreement.Type",
                "marketAgreement.Type",
            }:
                market_agreement_type = text
            elif tag in {"Type_MarketAgreement", "type_MarketAgreement"}:
                market_agreement_type = _first_child_text(child, {"type", "Type"})
            elif tag in {
                "Original_MarketProduct",
                "original_MarketProduct",
                "original_MarketProduct.marketProductType",
            }:
                original_market_product = text or _first_child_text(
                    child, {"marketProductType", "type", "Type"}
                )
            elif tag in {
                "Standard_MarketProduct",
                "standard_MarketProduct",
                "standard_MarketProduct.marketProductType",
            }:
                standard_market_product = text or _first_child_text(
                    child, {"marketProductType", "type", "Type"}
                )
            elif tag == "MktPSRType":
                for sub in child:
                    if _strip_ns(sub.tag) == "psrType":
                        production_type = (sub.text or "").strip()
            elif tag == "generatingUnit_PSRType.psrType":
                production_type = (child.text or "").strip()
            elif tag == "registeredResource.mRID":
                unit_mrid = (child.text or "").strip()
            elif tag == "registeredResource.name":
                unit_name = (child.text or "").strip()
            elif tag == "RegisteredResource":
                for sub in child:
                    sub_tag = _strip_ns(sub.tag)
                    if sub_tag == "mRID":
                        unit_mrid = (sub.text or "").strip()
                        asset_mrid = unit_mrid
                    elif sub_tag == "name":
                        unit_name = (sub.text or "").strip()
                        asset_name = unit_name
            elif tag in {"Asset_RegisteredResource", "asset_RegisteredResource"}:
                for sub in child:
                    sub_tag = _strip_ns(sub.tag)
                    if sub_tag == "mRID":
                        asset_mrid = (sub.text or "").strip()
                    elif sub_tag == "name":
                        asset_name = (sub.text or "").strip()
            elif tag in {
                "Asset_RegisteredResource.mRID",
                "asset_RegisteredResource.mRID",
            }:
                asset_mrid = (child.text or "").strip()
            elif tag in {
                "Asset_RegisteredResource.name",
                "asset_RegisteredResource.name",
            }:
                asset_name = (child.text or "").strip()
            elif tag == "production_RegisteredResource.mRID":
                unit_mrid = (child.text or "").strip()
                asset_mrid = unit_mrid
            elif tag == "production_RegisteredResource.name":
                unit_name = (child.text or "").strip()
                asset_name = unit_name
            elif tag == "production_RegisteredResource.pSRType.psrType":
                production_type = (child.text or "").strip()

        # Parse each Period
        # WindPowerFeedin_Period appears in Unavailability_MarketDocument (H7 outages);
        # it has the same timeInterval/resolution/Point structure as Period.
        for period_el in ts_el.iter():
            if _strip_ns(period_el.tag) not in {"Period", "Available_Period", "WindPowerFeedin_Period"}:
                continue

            start_dt: datetime | None = None
            resolution: timedelta = timedelta(hours=1)

            for child in period_el:
                tag = _strip_ns(child.tag)
                if tag == "timeInterval":
                    for sub in child:
                        if _strip_ns(sub.tag) == "start":
                            with contextlib.suppress(ValueError):
                                start_dt = _parse_utc(sub.text or "")
                elif tag == "timeInterval.start":
                    with contextlib.suppress(ValueError):
                        start_dt = _parse_utc(child.text or "")
                elif tag == "resolution":
                    resolution = _resolve_resolution((child.text or "").strip())

            if start_dt is None:
                continue

            for point_el in period_el.iter():
                if _strip_ns(point_el.tag) != "Point":
                    continue
                position: int | None = None
                value: float | None = None
                for child in point_el:
                    tag = _strip_ns(child.tag)
                    if tag == "position":
                        with contextlib.suppress(ValueError):
                            position = int(child.text or "0")
                    elif _matches_value_tag(tag, value_tag):
                        with contextlib.suppress(ValueError):
                            value = float(child.text or "nan")
                if position is None or value is None:
                    continue

                timestamp = start_dt + (position - 1) * resolution
                records.append({
                    **document_metadata,
                    "timestamp_utc": timestamp,
                    "value": value,
                    "in_domain": in_domain,
                    "out_domain": out_domain,
                    "production_type": production_type,
                    "control_area_domain": control_area_domain,
                    "area_domain": area_domain,
                    "connecting_domain": connecting_domain,
                    "acquiring_domain": acquiring_domain,
                    "business_type": business_type,
                    "flow_direction": flow_direction,
                    "market_agreement_type": market_agreement_type,
                    "original_market_product": original_market_product,
                    "standard_market_product": standard_market_product,
                    "resolution": str(resolution),
                    "unit_mrid": unit_mrid,
                    "unit_name": unit_name,
                    "timeseries_mrid": timeseries_mrid,
                    "asset_mrid": asset_mrid,
                    "asset_name": asset_name,
                    "document_status": document_status
                    or document_metadata["document_status"],
                })

    return records


def parse_generation_units_master_data_xml(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Parse ENTSO-E production/generation unit master-data XML.

    The Transparency Platform's master-data documents are reference data rather
    than point time series. This parser stays namespace-agnostic and extracts the
    fields the silver layer needs while tolerating small document-shape changes.
    """
    try:
        from lxml import etree  # type: ignore[import-untyped]
    except ImportError:
        logger.error("lxml not installed; cannot parse ENTSO-E XML")
        return []

    try:
        root = etree.fromstring(xml_bytes)  # noqa: S320 (trusted internal data)
    except etree.XMLSyntaxError as exc:
        logger.error("XML parse error: %s", exc)
        return []

    area_code = ""
    implementation_datetime = None
    for el in root.iter():
        tag = _strip_ns(el.tag)
        text = (el.text or "").strip()
        if tag in {"BiddingZone_Domain.mRID", "biddingZone_Domain.mRID"} and text:
            area_code = text
        elif tag in {
            "Implementation_DateAndOrTime",
            "implementation_DateAndOrTime",
            "implementation_DateAndOrTime.date",
        } and text:
            with contextlib.suppress(ValueError):
                implementation_datetime = _parse_utc(text)

    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for ts_el in root.iter():
        if _strip_ns(ts_el.tag) != "TimeSeries":
            continue

        ts_area_code = area_code
        ts_implementation_datetime = implementation_datetime
        unit_mrid = unit_name = production_type = ""
        for child in ts_el.iter():
            tag = _strip_ns(child.tag)
            text = (child.text or "").strip()
            if not text:
                continue
            if tag in {"BiddingZone_Domain.mRID", "biddingZone_Domain.mRID"}:
                ts_area_code = text
            elif tag in {
                "Implementation_DateAndOrTime",
                "implementation_DateAndOrTime",
                "implementation_DateAndOrTime.date",
            }:
                with contextlib.suppress(ValueError):
                    ts_implementation_datetime = _parse_utc(text)
            elif tag == "registeredResource.mRID":
                unit_mrid = text
            elif tag == "registeredResource.name":
                unit_name = text
            elif tag in {"psrType", "generatingUnit_PSRType.psrType"} and not production_type:
                production_type = text

        if unit_mrid:
            key = (ts_area_code, unit_mrid)
            seen.add(key)
            records.append({
                "area_code": ts_area_code,
                "unit_mrid": unit_mrid,
                "unit_name": unit_name,
                "production_type": production_type,
                "implementation_datetime_utc": ts_implementation_datetime,
            })

    unit_tags = {
        "MktGeneratingUnit",
        "MktGenerationUnit",
        "GeneratingUnit",
        "ProductionUnit",
    }
    for unit_el in root.iter():
        if _strip_ns(unit_el.tag) not in unit_tags:
            continue

        unit_mrid = unit_name = production_type = ""
        for child in unit_el.iter():
            tag = _strip_ns(child.tag)
            text = (child.text or "").strip()
            if not text:
                continue
            if tag == "mRID" and not unit_mrid:
                unit_mrid = text
            elif tag == "name" and not unit_name:
                unit_name = text
            elif tag == "psrType" and not production_type:
                production_type = text

        if not unit_mrid:
            continue
        key = (area_code, unit_mrid)
        if key in seen:
            continue
        seen.add(key)

        records.append({
            "area_code": area_code,
            "unit_mrid": unit_mrid,
            "unit_name": unit_name,
            "production_type": production_type,
            "implementation_datetime_utc": implementation_datetime,
        })

    return records
