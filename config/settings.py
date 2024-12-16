from pathlib import Path
import json

SERVER_URL = "http://157.245.10.94"

ROOT_DIR = Path(__file__).parent.parent

with open(ROOT_DIR / 'color_scale.json', 'r') as f:  # Update color scale path
    color_scale = json.load(f)

IMAGE_SETTINGS = {
    'colors': color_scale['colors'],
    "colormap": "RdYlBu_r",
    "dpi": 150,
}

UNIT_TRANSFORMS = {
    "K_to_F": lambda x: (x - 273.15) * 9/5 + 32,
    "C_to_F": lambda x: x * 9/5 + 32,
    "m/s_to_knots": lambda x: x * 1.944,
    "m_to_ft": lambda x: x * 3.28084
}

SOURCES = {
    "Ocean_Dynamics_Combined": {
        "source_type": "combined_view",
        "type": "water_movement",
        "name": "Ocean Dynamics (Currents and Eddies)",
        "source_datasets": {
            "altimetry": {
                "source_type": "cmems",
                "dataset_id": "cmems_mod_glo_phy_anfc_merged-sl_PT1H-i",
                "variables": {
                    "sea_surface_height": {
                        "type": "height",
                        "unit": "m"
                    }
                }
            },
            "currents": {
                "source_type": "cmems",
                "dataset_id": "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
                "variables": {
                    "uo": {
                        "type": "current",
                        "unit": "m/s"
                    },
                    "vo": {
                        "type": "current",
                        "unit": "m/s"
                    }
                }
            }
        },
        "supportedLayers": ["image", "data", "contours"],
        "metadata": {
            "cloud-free": "Yes",
            "frequency": "Daily",
            "resolution": "~9 km",
            "description": "Combined visualization of currents and detected eddies using altimetry and current data.",
            "processing_note": "Processes noon UTC altimetry data with daily currents."
        }
    },
    "LEOACSPOSSTL3SnrtCDaily": {
        "source_type": "erddap",
        "type": "sst",
        "name": "LEO ACSPO SST L3S NRT C Daily",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwLEOACSPOSSTL3SnrtCDaily",
        "variables": {
            "sea_surface_temperature": {
                "type": "temperature",
                "source_unit": "C",
                "target_unit": "fahrenheit",
            },
            "sst_gradient_magnitude": {
                "type": "gradient",
                "unit": "C/km",
            }
        },
        "lag_days": 2,
        "color_scale": [
            '#081d58', '#0d2167', '#122b76', '#173584', '#1c3f93',
            '#2149a1', '#2653b0', '#2b5dbe', '#3067cd', '#3571db',
            '#3a7bea', '#4185f8', '#41b6c4', '#46c0cd', '#4bcad6',
            '#50d4df', '#55dde8', '#5ae7f1', '#7fcdbb', '#8ed7c4',
            '#9de1cd', '#acebd6', '#bbf5df', '#c7e9b4', '#d6edb8',
            '#e5f1bc', '#f4f5c0', '#fef396', '#fec44f', '#fdb347',
            '#fca23f', '#fb9137', '#fa802f', '#f96f27', '#f85e1f',
            '#f74d17'
        ],
        "stride": None,
        "supportedLayers": ["image", "data", "contours", "features"],
        "metadata": {
            "cloud-free": "No",
            "frequency": "Daily",
            "resolution": "1 mile",
            "capture": "blends day and night",
            "description": "Sea surface temperature from NOAA's ACSPO L3S product."
        }
    },
    "BLENDEDsstDNDaily": {
        "source_type": "erddap",
        "type": "sst",
        "name": "NOAA Geo-polar Blended SST Analysis Day+Night",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwBLENDEDsstDNDaily",
        "variables": {
            "analysed_sst": {
                "type": "temperature",
                "source_unit": "C",
                "target_unit": "fahrenheit",
            }
        },
        "lag_days": 2,
        "supportedLayers": ["image", "data", "contours"],
        "color_scale": [
            '#081d58', '#0d2167', '#122b76', '#173584', '#1c3f93',
            '#2149a1', '#2653b0', '#2b5dbe', '#3067cd', '#3571db',
            '#3a7bea', '#4185f8', '#41b6c4', '#46c0cd', '#4bcad6',
            '#50d4df', '#55dde8', '#5ae7f1', '#7fcdbb', '#8ed7c4',
            '#9de1cd', '#acebd6', '#bbf5df', '#c7e9b4', '#d6edb8',
            '#e5f1bc', '#f4f5c0', '#fef396', '#fec44f', '#fdb347',
            '#fca23f', '#fb9137', '#fa802f', '#f96f27', '#f85e1f',
            '#f74d17'
        ],
        "metadata": {
            "cloud-free": "Yes",
            "frequency": "Daily",
            "resolution": "~3.1 miles (0.05°)",
            "description": "Blended sea surface temperature analysis from multiple satellites.",
            "capture": "Blends day and night"
        }
    },
    "chlorophyll_oci": {
        "source_type": "erddap",
        "name": "Chlorophyll OCI VIIRS Daily (Gap-filled)",
        "base_url": "https://coastwatch.noaa.gov/erddap/griddap",
        "dataset_id": "noaacwNPPN20VIIRSDINEOFDaily",
        "variables": {
            "chlor_a": {
                "type": "chlorophyll",
                "unit": "mg/m³",
            }
        },
        "lag_days": 2,
        "altitude": "[0:1:0]",
        "type": "chlorophyll",
        "supportedLayers": ["image", "data", "contours"],
        "color_scale": [
            '#B1C2D8', '#A3B9D3', '#96B1CF', '#88A8CA', '#7AA0C5',
            '#6C98C0', '#5F8FBB', '#5187B6', '#437FB0', '#3577AB',
            '#2EAB87', '#37B993', '#40C79F', '#49D5AB', '#52E3B7',
            '#63E8B8', '#75EDB9', '#86F3BA', '#98F8BB', '#A9FDBB',
            '#C1F5A3', '#DAFD8B', '#F2FF73', '#FFF75B', '#FFE742',
            '#FFD629', '#FFC611', '#FFB600', '#FFA500', '#FF9400',
            '#FF8300', '#FF7200', '#FF6100', '#FF5000', '#FF3F00'
        ],
        "metadata": {
            "cloud-free": "No",
            "frequency": "Daily",
            "resolution": "2.5 miles",
            "description": "Chlorophyll-a concentration derived from VIIRS."
        }
    },
    "CMEMS_Global_Waves_Daily": {
        "source_type": "cmems",
        "type": "waves",
        "name": "CMEMS Global Wave Analysis and Forecast",
        "dataset_id": "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
        "variables": {
            "VHM0": {
                "type": "wave_height",
                "unit": "m",
            },
            "VMDR": {
                "type": "wave_direction",
                "unit": "degrees",
            },
        },
        "lag_days": 1,
        "supportedLayers": ["image", "data", "extrude", "vectors"],
        "color_scale": [
            '#053061', '#0a3666', '#0f3d6c', '#164270', '#1c4785', '#234d91',
            '#2c5ea0', '#3165a6', '#366dad', '#3d77bb', '#417fc0', '#4687c4',
            '#4b8bc2', '#5293c7', '#599bcc', '#5EA1CF', '#67aad3', '#70b2d7',
            '#73B3D8', '#7cbbdd', '#85c3e1', '#88C4E2', '#91cce6', '#9ad4ea',
            '#9DD6EC', '#a6def0', '#afe5f4', '#B2E5F4', '#bae7f3', '#c1e9f2',
            '#c6dbef', '#cdddf0', '#d3dff1', '#d9e6f2', '#e0e9f3', '#e7ecf4',
            '#e5eef4', '#edf1f6', '#f0f5f7', '#f2f2f1', '#f3efeb', '#f5ebe6',
            '#f4e7df', '#f3e3d9', '#f3e0d4', '#f2d9c8', '#f1d1bc', '#f0c5ac',
            '#ecb399', '#e8a086', '#e48d73', '#dd7960', '#d66552', '#d15043',
            '#cb3e36', '#c52828', '#bf1f1f', '#b81717', '#b01010', '#a80808'
        ],
        "metadata": {
            "cloud-free": "Yes",
            "frequency": "3-hourly",
            "resolution": "5 miles",
            "description": "Global wave analysis including height, period, and direction."
        }
    }
}