from pathlib import Path
import json
import os

# Define the root directory
ROOT_DIR = Path(__file__).parent.parent

# Define main directories
OUTPUT_DIR = ROOT_DIR / "output"
DATA_DIR = ROOT_DIR / "downloaded_data"
REGIONS_DIR = OUTPUT_DIR / "regions"

# Ensure critical directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
REGIONS_DIR.mkdir(parents=True, exist_ok=True)

# Directory structure configuration
DIR_STRUCTURE = {
    "base": REGIONS_DIR,
    "paths": {
        "image": "{region}/datasets/{dataset}/{timestamp}/image.png",
        "geojson": "{region}/datasets/{dataset}/{timestamp}/data.geojson",
        "metadata": "{region}/datasets/{dataset}/{timestamp}/data.json",
        "tiles": "{region}/datasets/{dataset}/{timestamp}/tiles",
        "index": {
            "main": "index.json",
            "region": "{region}/index.json",
            "dataset": "{region}/datasets/{dataset}/index.json"
        }
    }
}

# Load color scale relative to ROOT_DIR
with open(ROOT_DIR / 'color_scale.json', 'r') as f:  # Update color scale path
    color_scale = json.load(f)

# Image generation settings
IMAGE_SETTINGS = {
    'colors': color_scale['colors'],
    "colormap": "RdYlBu_r",
    "dpi": 300,
}

# Tile generation settings
TILE_SETTINGS = {
    "zoom_levels": [5, 8, 10],
    "tile_size": 256,
}

# Add Mapbox configuration
MAPBOX_CONFIG = {
    'access_token': 'pk.eyJ1Ijoic25vd2Nhc3QiLCJhIjoiY2plYXNjdTRoMDhsbDJ4bGFjOWN0YjdzeCJ9.fM2s4NZq_LUiTXJxsl2HbQ',
    'username': 'snowcast',
}

# Data source configurations
SOURCES = {
    "LEOACSPOSSTL3SnrtCDaily": {
        "source_type": "erddap",
        "name": "LEO ACSPO SST L3S NRT C Daily",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwLEOACSPOSSTL3SnrtCDaily",
        "variables": ["sea_surface_temperature"],
        "time_format": "%Y-%m-%dT00:00:00Z",
        "lag_days": 2,
        "color_scale": "RdYlBu_r",
        "category": "sst",
        "stride": None,
   },
    # "BLENDEDNRTcurrentsDaily": {
    #     "source_type": "erddap",
    #     "name": "NOAA Blended NRT Currents Daily",
    #     "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
    #     "dataset_id": "noaacwBLENDEDNRTcurrentsDaily",
    #     "variables": ["u_current", "v_current"],
    #     "time_format": "%Y-%m-%dT00:00:00Z",
    #     "lag_days": 1,
    #     "color_scale": "viridis",
    #     "category": "currents",
  
    # },
    # "chlorophyll_oci": {
    #     "source_type": "erddap",
    #     "name": "Chlorophyll OCI",
    #     "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
    #     "dataset_id": "noaacwNPPN20VIIRSDINEOFDaily",
    #     "variables": ["chlor_a"],
    #     "time_format": "%Y-%m-%dT00:00:00Z",
    #     "lag_days": 2,
    #     "color_scale": "YlGnBu",
    #     "altitude": "[0.0:1:0.0]",
    #     "category": "chlorophyll",
    # },
    # 'oscar_currents': {
    #     'name': 'OSCAR Ocean Currents',
    #     'source_type': 'podaac',
    #     'category': 'currents',
    #     'variables': ['u', 'v'],  # Now needs both components
    #     'color_scale': 'viridis',  # Or another appropriate colormap
    #     'dataset_id': 'OSCAR_L4_OC_NRT_V2.0',
    #     'collection_shortname': 'OSCAR_L4_OC_NRT_V2.0',
    #     'provider': 'POCLOUD',
    #     'lag_days': 3
    # }
}

