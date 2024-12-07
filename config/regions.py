from typing import Dict, TypedDict

class Region(TypedDict):
    name: str
    description: str
    bounds: list

REGIONS: Dict[str, Region] = {
    "gulf_of_maine": {
        "name": "Gulf of Maine",
        "bounds": [
            [-71.0, 41.5], 
            [-66.0, 45.0]
        ]
    },
    "cape_cod": {
        "name": "Cape Cod and Georges Bank",
        "bounds": [
            [-71.25, 39.5],
            [-65.25, 43.5]
        ]
    },
    "ne_canyons": {
        "name": "NE Canyons Overview",
        "bounds": [
            [-77.0, 36.0],
            [-65.0, 42.0]
        ]
    },
    "carolinas": {
        "name": "Carolinas",
        "bounds": [
            [-79.0, 33.0],
            [-72.0, 37.0]
        ]
    },
    "sc_ga": {
        "name": "South Carolina and Georgia",
        "bounds": [
            [-81.75, 30.5],
            [-75.0, 34.25]
        ]
    },
    "florida_overview": {
        "name": "Florida Overview",
        "bounds": [
            [-88.0, 23.0],
            [-77.0, 31.0]
        ]
    },
    "bahamas": {
        "name": "Bahamas",
        "bounds": [
            [-80.0, 21.5],
            [-74.0, 28.0]
        ]
    },
   "gulf_of_mexico": {
        "name": "Gulf of Mexico",
        "bounds": [
            [-98.0, 18.0],
            [-80.0, 31.0]
        ]
    },
    "us_complete": {
        "name": "United States Complete",
        "bounds": [
            [-125.0, 24.0],  # Southwest corner (covers West Coast + Hawaii area)
            [-66.0, 49.0]    # Northeast corner (covers East Coast + Alaska southern tip)
        ]
    },
    "united_states": {
        "name": "United States Waters",
        "bounds": [
            [-180.0, 15.0],  # Southwest corner (includes Hawaii, Guam, and Caribbean)
            [-64.0, 72.0]    # Northeast corner (includes Alaska and Maine)
        ]
    },
}