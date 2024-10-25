from typing import Dict, TypedDict

class Region(TypedDict):
    name: str
    description: str
    bounds: list  # [[west, south], [east, north]] in GeoJSON format

REGIONS: Dict[str, Region] = {
    "gulf_of_mexico": {
        "name": "Gulf of Mexico",
        "description": "Gulf of Mexico coastal waters",
        "bounds": [
            [-98.0, 18.0],  # [west, south]
            [-80.0, 31.0]   # [east, north]
        ]
    },
    # "northeast": {
    #     "name": "Northeast",
    #     "description": "Northeast US coastal waters",
    #     "bounds": [
    #         [-76.0, 35.0],  # [west, south]
    #         [-65.0, 45.0]   # [east, north]
    #     ]
    # },
    # "mid_atlantic": {
    #     "name": "Mid-Atlantic",
    #     "description": "Mid-Atlantic coastal waters",
    #     "bounds": [
    #         [-77.0, 35.0],  # [west, south]
    #         [-70.0, 41.0]   # [east, north]
    #     ]
    # },
    # "gulf_of_maine": {
    #     "name": "Gulf of Maine",
    #     "description": "Gulf of Maine coastal waters",
    #     "bounds": [
    #         [-71.1565, 41.5091],  # [west, south]
    #         [-63.2934, 46.0109]   # [east, north]
    #     ]
    # }
}
