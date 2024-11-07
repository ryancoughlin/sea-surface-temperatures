from typing import Dict, TypedDict

class Region(TypedDict):
    name: str
    description: str
    bounds: list  # [[west, south], [east, north]] in GeoJSON format

REGIONS: Dict[str, Region] = {
    # "gulf_of_mexico": {
    #     "name": "Gulf of Mexico",
    #     "description": "Gulf of Mexico coastal waters", 
    #     "bounds": [
    #         [-98.0, 18.0],  # [west, south]
    #         [-80.0, 31.0]   # [east, north]
    #     ]
    # },
    # "gulf_of_maine": {
    #     "name": "Gulf of Maine",
    #     "description": "Gulf of Maine region",
    #     "bounds": [
    #         [-71.0, 41.5],  # [west, south] 
    #         [-66.0, 45.0]   # [east, north]
    #     ]
    # },
    "cape_cod": {
        "name": "Cape Cod and Georges Bank",  # Changed from "Cape Cod / Georges Bank"
        "description": "Cape Cod and Georges Bank region",
        "bounds": [
            [-71.25, 39.5],  # [west, south]
            [-65.25, 43.5]   # [east, north]
        ]
    },
    # "canyons_overview": {
    #     "name": "NE Canyons Overview",
    #     "description": "Satfish overview of Northeast US submarine canyons",
    #     "bounds": [
    #         [-77.0, 36.0],  # [west, south]
    #         [-65.0, 42.0]   # [east, north]
    #     ]
    # },
    # "canyons_north": {
    #     "name": "NE Canyons North",
    #     "description": "Satfish northern section of Northeast US submarine canyons",
    #     "bounds": [
    #         [-74.25, 38.0],  # [west, south]
    #         [-67.0, 42.0]    # [east, north]
    #     ]
    # },
    # "canyons_south": {
    #     "name": "NE Canyons South",
    #     "description": "Satfish southern section of Northeast US submarine canyons",
    #     "bounds": [
    #         [-77.0, 36.0],  # [west, south]
    #         [-71.0, 40.5]   # [east, north]
    #     ]
    # },
}
