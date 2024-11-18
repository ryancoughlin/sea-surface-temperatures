from pathlib import Path
import json
import os

SERVER_URL = "http://157.245.10.94"

ROOT_DIR = Path(__file__).parent.parent

with open(ROOT_DIR / 'color_scale.json', 'r') as f:  # Update color scale path
    color_scale = json.load(f)

IMAGE_SETTINGS = {
    'colors': color_scale['colors'],
    "colormap": "RdYlBu_r",
    "dpi": 300,
}

SOURCES = {
    "CMEMS_Global_Currents_Daily": {
        "source_type": "cmems",
        "name": "CMEMS Global Daily Mean Ocean Currents",
        "base_url": "https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
        "dataset_id": "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
        "variables": ["uo", "vo"],
        "color_scale": "viridis",
        "type": "currents",
        "lag_days": 0,
        "supportedLayers": ["image", "data"],
        "metadata": {
            "cloud-free": "Yes",
            "frequency": "Daily",
            "resolution": "5 miles",
            "description": "Ocean surface currents calculated from model outputs.",
            "Dataset URL": "https://nrt.cmems-du.eu"
        }
    },
    "CMEMS_Global_Temperature_Daily": {
        "source_type": "cmems",
        "name": "CMEMS Global Daily Mean Sea Water Temperature",
        "base_url": "https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
        "dataset_id": "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
        "variables": ["thetao"],
        "color_scale": "RdYlBu_r",
        "type": "sst",
        "lag_days": 0,
        "supportedLayers": ["image", "data", "contours"],
        "metadata": {
            "cloud-free": "No",
            "frequency": "Daily",
            "resolution": "5 miles",
            "description": "Sea surface temperature from global model data.",
            "Dataset URL": "https://nrt.cmems-du.eu"
        }
    },
    "LEOACSPOSSTL3SnrtCDaily": {
        "source_type": "erddap",
        "type": "sst",
        "name": "LEO ACSPO SST L3S NRT C Daily",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwLEOACSPOSSTL3SnrtCDaily",
        "variables": ["sea_surface_temperature", "sst_gradient_magnitude"],
        "lag_days": 2,
        "color_scale": "RdYlBu_r",
        "stride": None,
        "supportedLayers": ["image", "data", "contours"],
        "metadata": {
            "cloud-free": "No",
            "frequency": "Daily",
            "resolution": "1 mile",
            "capture": "blends day and night",
            "description": "Sea surface temperature from NOAA's ACSPO L3S product.",
            "Dataset URL": "https://coastwatch.noaa.gov"
        }
    },
    "chlorophyll_oci": {
        "source_type": "erddap",
        "name": "Chlorophyll OCI VIIRS Daily (Gap-filled)",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwNPPN20VIIRSDINEOFDaily",
        "variables": ["chlor_a"],
        "lag_days": 2,
        "color_scale": "YlGnBu",
        "altitude": "[0:1:0]",
        "type": "chlorophyll",
        "supportedLayers": ["image", "data"],
        "metadata": {
            "cloud-free": "No",
            "frequency": "Daily",
            "resolution": "2.5 miles",
            "description": "Chlorophyll-a concentration derived from VIIRS.",
            "Dataset URL": "https://coastwatch.noaa.gov"
        }
    },
    "BLENDEDsstDNDaily": {
        "source_type": "erddap",
        "type": "sst",
        "name": "NOAA Geo-polar Blended SST Analysis Day+Night",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwBLENDEDsstDNDaily",
        "variables": ["analysed_sst"],
        "lag_days": 2,
        "color_scale": "RdYlBu_r",
        "stride": None,
        "supportedLayers": ["image", "data", "contours"],
        "metadata": {
            "cloud-free": "Yes",
            "frequency": "Daily",
            "resolution": "~3.1 miles (0.05°)",
            "description": "Blended sea surface temperature analysis from multiple satellites.",
            "dataset url": "https://coastwatch.noaa.gov",
            "capture": "Blended day and night"
        }
    }
}