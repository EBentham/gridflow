"""ENTSO-E silver transformers — import all modules to trigger registration."""

from gridflow.silver.entsoe.actual_generation import ActualGenerationTransformer
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.entsoe.cross_border_flows import CrossBorderFlowsTransformer
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer
from gridflow.silver.entsoe.installed_capacity import InstalledCapacityTransformer
from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.entsoe.outages_generation import OutagesGenerationTransformer
from gridflow.silver.entsoe.wind_solar_forecast import WindSolarForecastTransformer

__all__ = [
    "DayAheadPricesTransformer",
    "ActualLoadTransformer",
    "ActualGenerationTransformer",
    "CrossBorderFlowsTransformer",
    "LoadForecastTransformer",
    "WindSolarForecastTransformer",
    "OutagesGenerationTransformer",
    "InstalledCapacityTransformer",
]
