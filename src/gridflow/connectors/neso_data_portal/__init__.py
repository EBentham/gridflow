"""NESO Open Data Portal (CKAN) connector package.

Importing this package imports ``client``, which is what fires
``register_connector("neso_data_portal", ...)``. ``runner.import_connectors()``
relies on that side effect.

Distinct from ``gridflow.connectors.neso``, which is the Carbon Intensity API
and keeps its own scope (D-01).
"""

from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector

__all__ = ["NesoDataPortalConnector"]
