from typing import Dict, TypedDict

class Region(TypedDict):
    name: str
    description: str
    bounds: list

REGIONS: Dict[str, Region] = {
    "gulf_of_maine": {
        "name": "Gulf of Maine",
        "bounds": [
            [-71.0, 41.5],  # Southwest corner
            [-65.5, 45.0]   # Northeast corner
        ]
    },
    "cape_cod": {
        "name": "Cape Cod and Georges Bank",
        "bounds": [
            [-71.0, 40.5],  # Southwest corner
            [-66.0, 42.5]   # Northeast corner
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
    "west_coast": {
        "name": "U.S. West Coast",
        "bounds": [
            [-126.0, 32.0],  # Southwest (San Diego)
            [-116.0, 49.0]   # Northeast (Canadian border)
        ]
    },
    "hawaii": {
        "name": "Hawaiian Islands",
        "bounds": [
            [-161.0, 18.0],  # Southwest
            [-154.0, 23.0]   # Northeast
        ]
    },
    "baja": {
        "name": "Baja California",
        "bounds": [
            [-118.0, 22.5],  # Southwest (Cabo San Lucas)
            [-109.0, 32.5]   # Northeast (Includes San Diego)
        ]
    },
    "pnw": {
        "name": "Pacific Northwest",
        "bounds": [
            [-126.0, 42.0],  # Southwest (Oregon-California border)
            [-122.0, 49.0]   # Northeast (Canadian border)
        ]
    },
    "socal": {
        "name": "Southern California",
        "bounds": [
            [-121.0, 32.0],  # Southwest (San Diego)
            [-117.0, 35.0]   # Northeast (Point Conception)
        ]
    },
    "central_cal": {
        "name": "Central California",
        "bounds": [
            [-125.0, 35.0],  # Southwest (Point Conception)
            [-121.0, 39.0]   # Northeast (Cape Mendocino)
        ]
    }
}