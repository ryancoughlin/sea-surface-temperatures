from pathlib import Path
import json
import os

# Define the root directory
ROOT_DIR = Path(__file__).parent.parent

# Define main directories
OUTPUT_DIR = ROOT_DIR / "output"
DATA_DIR = ROOT_DIR / "downloaded_data"
REGIONS_DIR = OUTPUT_DIR / "regions"

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

# Data source configurations
SOURCES = {
    "LEOACSPOSSTL3SnrtCDaily": {
        "source_type": "erddap",
        "category": "sst",
        "name": "LEO ACSPO SST L3S NRT C Daily",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwLEOACSPOSSTL3SnrtCDaily",
        "variables": ["sea_surface_temperature"],
        "lag_days": 2,
        "color_scale": "RdYlBu_r",
        "stride": None,
        "layers": ["image", "geojson", "contours"]
   },
    "BLENDEDNRTcurrentsDaily": {
        "source_type": "erddap",
        "name": "NOAA Blended NRT Currents Daily",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwBLENDEDNRTcurrentsDaily",
        "variables": ["u_current", "v_current"],
        "lag_days": 2,
        "color_scale": "viridis",
        "category": "currents",
        "layers": ["image", "geojson"]
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
        "category": "chlorophyll",
        "layers": ["image", "geojson"]
    }
}

# Assuming we have a function to generate metadata for each dataset
def generate_metadata(dataset_id, dataset_name, category, date, processing_time):
    return {
        "dataset_info": {
            "id": dataset_id,
            "name": dataset_name,
            "category": category
        },
        "dates": [
            {
                "date": date,
                "processing_time": processing_time,
                "paths": {
                    "image": f"cape_cod/datasets/{dataset_id}/{date}/image.png",
                    "geojson": f"cape_cod/datasets/{dataset_id}/{date}/data.geojson",
                    "tiles": f"cape_cod/datasets/{dataset_id}/{date}/tiles"
                }
            }
        ],
        "last_updated": processing_time
    }

# Example usage for LEOACSPOSSTL3SnrtCDaily
metadata = generate_metadata(
    dataset_id="LEOACSPOSSTL3SnrtCDaily",
    dataset_name="LEO ACSPO SST L3S NRT C Daily",
    category="sst",
    date="20241028",
    processing_time="2024-10-28T10:38:35.683293"
)

# Output the modified metadata
print(metadata)
