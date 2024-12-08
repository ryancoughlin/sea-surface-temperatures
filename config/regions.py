from typing import Dict, TypedDict, List

class Bounds(TypedDict):
    north: float
    south: float
    east: float
    west: float

class Region(TypedDict):
    name: str
    group: str
    bounds: Bounds
    center: List[float]

REGIONS: Dict[str, Region] = {
    # East Coast
    "gulf_of_maine": {
        "name": "Gulf of Maine",
        "group": "East Coast",
        "bounds": {
            "north": 45.0,
            "south": 41.5,
            "east": -65.5,
            "west": -71.0
        },
        "center": [43.25, -68.25]
    },
    "cape_cod": {
        "name": "Cape Cod and Georges Bank",
        "group": "East Coast",
        "bounds": {
            "north": 42.5,
            "south": 40.5,
            "east": -66.0,
            "west": -71.0
        },
        "center": [41.5, -68.5]
    },
    "ne_canyons": {
        "name": "NE Canyons Overview",
        "group": "East Coast",
        "bounds": {
            "north": 42.0,
            "south": 36.0,
            "east": -65.0,
            "west": -77.0
        },
        "center": [39.0, -71.0]
    },
    "carolinas": {
        "name": "Carolinas",
        "group": "East Coast",
        "bounds": {
            "north": 37.0,
            "south": 33.0,
            "east": -72.0,
            "west": -79.0
        },
        "center": [35.0, -75.5]
    },
    "sc_ga": {
        "name": "South Carolina and Georgia",
        "group": "East Coast",
        "bounds": {
            "north": 34.25,
            "south": 30.5,
            "east": -75.0,
            "west": -81.75
        },
        "center": [32.375, -78.375]
    },
    
    # Southeast & Caribbean
    "florida_overview": {
        "name": "Florida Overview",
        "group": "Southeast & Caribbean",
        "bounds": {
            "north": 31.0,
            "south": 23.0,
            "east": -77.0,
            "west": -88.0
        },
        "center": [27.0, -82.5]
    },
    "bahamas": {
        "name": "Bahamas",
        "group": "Southeast & Caribbean",
        "bounds": {
            "north": 28.0,
            "south": 21.5,
            "east": -74.0,
            "west": -80.0
        },
        "center": [24.75, -77.0]
    },
    
    # Gulf Coast
    "gulf_of_mexico": {
        "name": "Gulf of Mexico",
        "group": "Gulf Coast",
        "bounds": {
            "north": 31.0,
            "south": 18.0,
            "east": -80.0,
            "west": -98.0
        },
        "center": [24.5, -89.0]
    },
    
    # Pacific Coast
    "west_coast": {
        "name": "U.S. West Coast",
        "group": "Pacific Coast",
        "bounds": {
            "north": 49.0,
            "south": 32.0,
            "east": -116.0,
            "west": -126.0
        },
        "center": [40.5, -121.0]
    },
    "pnw": {
        "name": "Pacific Northwest",
        "group": "Pacific Coast",
        "bounds": {
            "north": 49.0,
            "south": 42.0,
            "east": -122.0,
            "west": -126.0
        },
        "center": [45.5, -124.0]
    },
    "central_cal": {
        "name": "Central California",
        "group": "Pacific Coast",
        "bounds": {
            "north": 39.0,
            "south": 35.0,
            "east": -121.0,
            "west": -125.0
        },
        "center": [37.0, -123.0]
    },
    "socal": {
        "name": "Southern California",
        "group": "Pacific Coast",
        "bounds": {
            "north": 35.0,
            "south": 32.0,
            "east": -117.0,
            "west": -121.0
        },
        "center": [33.5, -119.0]
    },
    
    # Mexico
    "baja": {
        "name": "Baja California",
        "group": "Mexico",
        "bounds": {
            "north": 32.5,
            "south": 22.5,
            "east": -109.0,
            "west": -118.0
        },
        "center": [27.5, -113.5]
    },
    
    # Pacific Islands
    "hawaii": {
        "name": "Hawaiian Islands",
        "group": "Pacific Islands",
        "bounds": {
            "north": 23.0,
            "south": 18.0,
            "east": -154.0,
            "west": -161.0
        },
        "center": [20.5, -157.5]
    }
}