"""The bronze body parser every ENTSO-E XML transformer shares."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.base import BaseSilverTransformer

if TYPE_CHECKING:
    from pathlib import Path


class EntsoeXmlTransformer(BaseSilverTransformer):
    """An ENTSO-E transformer: each body is one XML market document."""

    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.xml"
    value_tag: ClassVar[str] = "quantity"
    """The ``<Point>`` child holding the value (``"quantity"`` or ``"price.amount"``)."""

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        return parse_timeseries_xml(path.read_bytes(), value_tag=self.value_tag)
