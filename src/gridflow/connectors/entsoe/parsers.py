"""XML parsers for ENTSO-E API responses."""

from __future__ import annotations

import calendar
import contextlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def _hardened_parser() -> Any:
    """Return a fresh lxml parser configured to refuse external entities.

    A new parser is returned on every call because lxml parsers are stateful and
    must not be shared across parses.

    Returns:
        A configured ``lxml.etree.XMLParser``.
    """
    from lxml import etree  # type: ignore[import-untyped]

    # resolve_entities=False is the load-bearing flag: it blocks XXE external-entity
    # resolution and billion-laughs entity expansion. no_network and huge_tree are
    # already lxml defaults; set explicitly as belt-and-suspenders.
    return etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=False)


# Resolution code -> timedelta. Used as a fallback / human-readable string in
# silver `resolution` columns. For P1M and P1Y the timedelta is an approximation
# only (30d / 365d); the actual point timestamps for those codes are computed
# via _advance_calendar() which uses real calendar arithmetic.
_RESOLUTION_MAP: dict[str, timedelta] = {
    "PT15M": timedelta(minutes=15),
    "PT30M": timedelta(minutes=30),
    "PT60M": timedelta(hours=1),
    "P1D": timedelta(days=1),
    "P7D": timedelta(days=7),
    "P1M": timedelta(days=30),
    "P1Y": timedelta(days=365),
}

# Resolution codes that need calendar-correct advancement (variable-length
# units). PT/P1D/P7D are fixed-length and use plain timedelta multiplication.
_CALENDAR_RESOLUTIONS: frozenset[str] = frozenset({"P1M", "P1Y"})


def _resolve_resolution(code: str) -> timedelta:
    return _RESOLUTION_MAP.get(code, timedelta(hours=1))


def _add_months(dt: datetime, n: int) -> datetime:
    """Add n calendar months to dt. Clamps day to end-of-month when needed."""
    if n == 0:
        return dt
    month_index = dt.month - 1 + n
    year = dt.year + month_index // 12
    month = month_index % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(dt.day, last_day)
    return dt.replace(year=year, month=month, day=day)


def _add_years(dt: datetime, n: int) -> datetime:
    """Add n calendar years to dt. Handles Feb 29 by clamping to Feb 28."""
    if n == 0:
        return dt
    year = dt.year + n
    last_day = calendar.monthrange(year, dt.month)[1]
    day = min(dt.day, last_day)
    return dt.replace(year=year, day=day)


def _advance_calendar(start: datetime, position: int, code: str) -> datetime:
    """Compute the timestamp of the Nth point under a calendar resolution.

    G9 ENTSOE-04: ENTSO-E TimeSeries with `<resolution>P1M</resolution>` or
    `P1Y` use calendar units. Treating them as fixed 30d / 365d timedeltas
    drifts by up to 11 days per year (or 1+ day per leap year), making
    `load_forecast_monthly` and `load_forecast_yearly` point timestamps
    misaligned with the vendor's documented monthly/yearly buckets.

    Position is the 1-based ENTSO-E Point/position value.
    """
    n = position - 1
    if code == "P1M":
        return _add_months(start, n)
    if code == "P1Y":
        return _add_years(start, n)
    # Should not reach here when called only with calendar codes
    return start + n * _resolve_resolution(code)


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
        # Issue 04 / ENTSOE: the document-level <createdDateTime> is the
        # vendor's genuine, leak-proof forecast issue time (publication
        # vintage). Kept as string-or-empty at the parser boundary (matching
        # the existing metadata style); the silver forecast transformers cast
        # it to a tz-aware `published_at` datetime. Empty string when absent so
        # downstream emits a deterministic typed-null.
        "document_created_at": "",
    }
    for child in root:
        tag = _strip_ns(child.tag)
        text = (child.text or "").strip()
        if tag == "mRID":
            metadata["document_mrid"] = text
        elif tag == "revisionNumber":
            metadata["revision_number"] = text
        elif tag == "createdDateTime":
            metadata["document_created_at"] = text
        elif tag in {"docStatus", "docStatus.value"}:
            metadata["document_status"] = text or _first_child_text(child, {"value"})
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
        ``resolution`` (the ENTSO-E ISO code, e.g. ``"PT60M"``),
        ``currency_unit`` (e.g. ``"EUR"``/``"GBP"`` for price documents),
        ``document_created_at`` (the document ``<createdDateTime>`` publication
        vintage, string-or-empty), ``unit_mrid``, ``unit_name``. Fields absent
        from a given TimeSeries element are returned as empty strings
        (backward-compatible).
    """
    try:
        from lxml import etree
    except ImportError:
        logger.error("lxml not installed; cannot parse ENTSO-E XML")
        return []

    try:
        root = etree.fromstring(xml_bytes, parser=_hardened_parser())
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
        # Issue 05 #1: ENTSO-E carries the price currency in
        # <currency_Unit.name> (e.g. EUR for continental zones, GBP for GB).
        # Capture it so a GBP price is never silently labelled EUR downstream.
        currency_unit = ""
        timeseries_mrid = asset_mrid = asset_name = document_status = ""
        # G9 ENTSOE-02: TimeSeries-level Reason.code (e.g. A87 financial
        # documents carry per-series reason classifiers). Extracted below
        # via a dedicated walk so this loop's elif chain stays linear.
        reason_code = ""
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
            elif tag in {"currency_Unit.name", "Currency_Unit.name"}:
                currency_unit = text
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

        # G9 ENTSOE-02: capture the first Reason.code descendant of the
        # TimeSeries (A87 financial documents carry per-series Reason
        # blocks; other doc types may or may not). Done as a second walk
        # to keep the main elif chain readable.
        if not reason_code:
            for descendant in ts_el.iter():
                if _strip_ns(descendant.tag) != "Reason":
                    continue
                for sub in descendant:
                    if _strip_ns(sub.tag) == "code":
                        reason_code = (sub.text or "").strip()
                        break
                if reason_code:
                    break

        # Parse each Period
        # WindPowerFeedin_Period appears in Unavailability_MarketDocument (H7 outages);
        # it has the same timeInterval/resolution/Point structure as Period.
        for period_el in ts_el.iter():
            if _strip_ns(period_el.tag) not in {
                "Period",
                "Available_Period",
                "WindPowerFeedin_Period",
            }:
                continue

            start_dt: datetime | None = None
            resolution: timedelta = timedelta(hours=1)
            resolution_code: str = ""

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
                    resolution_code = (child.text or "").strip()
                    resolution = _resolve_resolution(resolution_code)

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

                # G9 ENTSOE-04: P1M / P1Y are calendar units (variable days
                # per month, leap years for years). Use calendar arithmetic
                # rather than timedelta multiplication so monthly/yearly
                # bucket alignment matches the vendor's documented buckets.
                if resolution_code in _CALENDAR_RESOLUTIONS:
                    timestamp = _advance_calendar(start_dt, position, resolution_code)
                else:
                    timestamp = start_dt + (position - 1) * resolution
                records.append(
                    {
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
                        # Issue 05 #1: carry the parsed currency through to silver.
                        "currency_unit": currency_unit,
                        # Issue 05 #4: emit the ENTSO-E ISO resolution code
                        # (PT60M / PT15M / P1M / P1Y) verbatim, not str(timedelta)
                        # ("1:00:00"). Empty string when the Period had no
                        # <resolution> element (honest absence, not a fake code).
                        "resolution": resolution_code,
                        "unit_mrid": unit_mrid,
                        "unit_name": unit_name,
                        "timeseries_mrid": timeseries_mrid,
                        "asset_mrid": asset_mrid,
                        "asset_name": asset_name,
                        "document_status": document_status or document_metadata["document_status"],
                        # G9 ENTSOE-02: TimeSeries-level Reason.code
                        # (populated for A87 financial documents).
                        "reason_code": reason_code,
                    }
                )

    return records


def parse_generation_units_master_data_xml(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Parse ENTSO-E production/generation unit master-data XML.

    The Transparency Platform's master-data documents are reference data rather
    than point time series. This parser stays namespace-agnostic and extracts the
    fields the silver layer needs while tolerating small document-shape changes.
    """
    try:
        from lxml import etree
    except ImportError:
        logger.error("lxml not installed; cannot parse ENTSO-E XML")
        return []

    try:
        root = etree.fromstring(xml_bytes, parser=_hardened_parser())
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
        elif (
            tag
            in {
                "Implementation_DateAndOrTime",
                "implementation_DateAndOrTime",
                "implementation_DateAndOrTime.date",
            }
            and text
        ):
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
            records.append(
                {
                    "area_code": ts_area_code,
                    "unit_mrid": unit_mrid,
                    "unit_name": unit_name,
                    "production_type": production_type,
                    "implementation_datetime_utc": ts_implementation_datetime,
                }
            )

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

        records.append(
            {
                "area_code": area_code,
                "unit_mrid": unit_mrid,
                "unit_name": unit_name,
                "production_type": production_type,
                "implementation_datetime_utc": implementation_datetime,
            }
        )

    return records
