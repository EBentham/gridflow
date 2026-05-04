"""ENTSO-G silver transformers: import all modules to trigger registration."""

from gridflow.silver.entsog.generic import register_generic_entsog_transformers
from gridflow.silver.entsog.physical_flows import PhysicalFlowsTransformer

register_generic_entsog_transformers()

__all__ = ["PhysicalFlowsTransformer"]
