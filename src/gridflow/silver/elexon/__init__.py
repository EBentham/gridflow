"""Elexon silver transformers — import all modules to trigger registration."""

from gridflow.silver.elexon.agpt import AGPTTransformer
from gridflow.silver.elexon.agws import AGWSTransformer
from gridflow.silver.elexon.atl import ATLTransformer
from gridflow.silver.elexon.bmunits import BMUnitsTransformer
from gridflow.silver.elexon.boal import BOALTransformer
# BOD endpoint decommissioned by Elexon — transformer retained but not registered
from gridflow.silver.elexon.demand_forecast import DemandForecastTransformer, NDFDTransformer
from gridflow.silver.elexon.disbsad import DISBSADTransformer
from gridflow.silver.elexon.fou2t14d import FOU2T14DTransformer
from gridflow.silver.elexon.freq import FreqTransformer
from gridflow.silver.elexon.fuelinst import FuelInstTransformer
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.imbalngc import ImbalNGCTransformer
from gridflow.silver.elexon.inddem import INDDEMTransformer
from gridflow.silver.elexon.indgen import INDGENTransformer
from gridflow.silver.elexon.indo import INDOTransformer
from gridflow.silver.elexon.indod import INDODTransformer
from gridflow.silver.elexon.itsdo import ITSDOTransformer
from gridflow.silver.elexon.lolpdrm import LOLPDRMTransformer
from gridflow.silver.elexon.market_depth import MarketDepthTransformer
from gridflow.silver.elexon.melngc import MelNGCTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.netbsad import NETBSADTransformer
from gridflow.silver.elexon.nonbm import NONBMTransformer
from gridflow.silver.elexon.pn import PNTransformer
from gridflow.silver.elexon.remit import REMITTransformer
from gridflow.silver.elexon.soso import SOSOTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.elexon.temp import TempTransformer
from gridflow.silver.elexon.tsdf import TSDFTransformer
from gridflow.silver.elexon.tsdfd import TSDFDTransformer
from gridflow.silver.elexon.uou2t14d import UOU2T14DTransformer
from gridflow.silver.elexon.wind_forecast import WindForecastTransformer
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
