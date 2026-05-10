"""Elexon silver transformers — import all modules to trigger registration."""

from gridflow.silver.elexon.bmunits import BMUnitsTransformer
from gridflow.silver.elexon.boal import BOALTransformer
# BOD endpoint decommissioned by Elexon — transformer retained but not registered
from gridflow.silver.elexon.demand_forecast import DemandForecastTransformer, NDFDTransformer
from gridflow.silver.elexon.disbsad import DISBSADTransformer
from gridflow.silver.elexon.freq import FreqTransformer
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.pn import PNTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.elexon.wind_forecast import WindForecastTransformer
from gridflow.silver.elexon.generic import (
    AGPTTransformer,
    AGWSTransformer,
    ATLTransformer,
    FOU2T14DTransformer,
    FuelInstTransformer,
    ImbalNGCTransformer,
    INDDEMTransformer,
    INDGENTransformer,
    INDODTransformer,
    INDOTransformer,
    ITSDOTransformer,
    LOLPDRMTransformer,
    MarketDepthTransformer,
    MelNGCTransformer,
    NETBSADTransformer,
    NONBMTransformer,
    REMITTransformer,
    SOSOTransformer,
    TSDFDTransformer,
    TSDFTransformer,
    TempTransformer,
    UOU2T14DTransformer,
)
# generation_by_fuel removed — was a duplicate of fuelhh (both used /datasets/FUELHH)

__all__ = [
    "AGPTTransformer",
    "AGWSTransformer",
    "ATLTransformer",
    "BMUnitsTransformer",
    "BOALTransformer",
    "DemandForecastTransformer",
    "NDFDTransformer",
    "DISBSADTransformer",
    "FOU2T14DTransformer",
    "FreqTransformer",
    "FuelInstTransformer",
    "FuelHHTransformer",
    "ImbalNGCTransformer",
    "INDDEMTransformer",
    "INDGENTransformer",
    "INDOTransformer",
    "INDODTransformer",
    "ITSDOTransformer",
    "LOLPDRMTransformer",
    "MarketDepthTransformer",
    "MelNGCTransformer",
    "MIDTransformer",
    "NETBSADTransformer",
    "NONBMTransformer",
    "PNTransformer",
    "REMITTransformer",
    "SOSOTransformer",
    "SystemPriceTransformer",
    "TempTransformer",
    "TSDFTransformer",
    "TSDFDTransformer",
    "UOU2T14DTransformer",
    "WindForecastTransformer",
]
