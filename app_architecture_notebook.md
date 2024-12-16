# Below is the architecture of the app

## Who uses this data?

- Offshore fisherman across the world.
- These are not scientists and except data to be in a format that is easy to understand and use. E.g "Eddy here" vs seeing raw data.

## What needs to happen?

- Fetch data from either CMEMS or ERDDAP

## Configuration and Responsibilites

- DataProcessor takes care of preprocessing, process once and use
- DataAssembler responsible for creating the JSON for the front-end API endpoint. Generated to output/metadata.json
- Dataset config lives in config/settings.py.
- Region config lives in config/regions.py.

## Data Fetching

- Fetched from CMEMS and ERDDAP from NOAA
- Data returned is per-region, no masking required

## Processing

## Extra Datasets

````
 "CMEMS_Global_Currents_Daily": {
        "source_type": "cmems",
        "name": "CMEMS Global Daily Mean Ocean Currents",
        "dataset_id": "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
        "variables": {
            "uo": {
                "type": "current",
                "unit": "m/s",
            },
            "vo": {
                "type": "current",
                "unit": "m/s",
            },
        },
        "type": "currents",
        "lag_days": 1,
        "supportedLayers": ["image", "data"],
        "color_scale": ['#B1C2D8', '#89CFF0', '#4682B4', '#0047AB', '#00008B', '#000033'],
        "metadata": {
            "cloud-free": "Yes",
            "frequency": "Daily",
            "resolution": "5 miles",
            "description": "Ocean surface currents calculated from model outputs."
        }
    },
    "CMEMS_Global_Altimetry_Hourly": {
        "source_type": "cmems",
        "type": "altimetry",
        "name": "CMEMS Global Sea Surface Height Analysis",
        "dataset_id": "cmems_mod_glo_phy_anfc_merged-sl_PT1H-i",
        "variables": {
            "sea_surface_height": {
                "type": "height",
                "unit": "m"
            }
        },
        "lag_days": 1,
        "time_selection": {
            "hour": 12,  # Noon UTC
            "window_hours": 1  # Only fetch 1 hour of data
        },
        "supportedLayers": ["image", "data", "contours"],
        "color_scale": [
            '#053061', '#2166ac', '#4393c3', '#92c5de', '#d1e5f0',
            '#f7f7f7', '#fddbc7', '#f4a582', '#d6604d', '#b2182b'
        ],
        "metadata": {
            "cloud-free": "Yes",
            "frequency": "Daily at 12:00 UTC",
            "resolution": "~9 km",
            "description": "Global sea surface height analysis from merged satellite altimetry.",
            "related_datasets": ["CMEMS_Global_Currents_Daily", "BLENDEDsstDNDaily"]
        }
    },
    ```
````
