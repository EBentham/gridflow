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
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M"):
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
        ``resolution``. Fields absent from a given TimeSeries element are
        returned as empty strings (backward-compatible).
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

    # Collect all TimeSeries elements (namespace-agnostic)
    for ts_el in root.iter():
        if _strip_ns(ts_el.tag) != "TimeSeries":
            continue

        # Extract domain codes and metadata fields
        in_domain = out_domain = production_type = ""
        control_area_domain = business_type = flow_direction = ""
        for child in ts_el:
            tag = _strip_ns(child.tag)
            if tag in ("in_Domain.mRID", "outBiddingZone_Domain.mRID"):
                in_domain = (child.text or "").strip()
            elif tag == "out_Domain.mRID":
                out_domain = (child.text or "").strip()
            elif tag == "controlArea_Domain.mRID":
                control_area_domain = (child.text or "").strip()
            elif tag == "businessType":
                business_type = (child.text or "").strip()
            elif tag == "flowDirection.direction":
                flow_direction = (child.text or "").strip()
            elif tag == "MktPSRType":
                for sub in child:
                    if _strip_ns(sub.tag) == "psrType":
                        production_type = (sub.text or "").strip()

        # Parse each Period
        for period_el in ts_el.iter():
            if _strip_ns(period_el.tag) != "Period":
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
                    elif tag == value_tag:
                        with contextlib.suppress(ValueError):
                            value = float(child.text or "nan")
                if position is None or value is None:
                    continue

                timestamp = start_dt + (position - 1) * resolution
                records.append({
                    "timestamp_utc": timestamp,
                    "value": value,
                    "in_domain": in_domain,
                    "out_domain": out_domain,
                    "production_type": production_type,
                    "control_area_domain": control_area_domain,
                    "business_type": business_type,
                    "flow_direction": flow_direction,
                    "resolution": str(resolution),
                })

    return records
