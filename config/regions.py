from typing import Dict, TypedDict, List

class Region(TypedDict):
    name: str
    group: str
    bounds: List[List[float]]
    center: List[float]

REGIONS: Dict[str, Region] = {
    # Atlantic Ocean
    # "gulf_of_maine": {
    #     "name": "Gulf of Maine",
    #     "group": "Atlantic Ocean",
    #     "bounds": [
    #         [-71.0, 41.5],  # Southwest corner
    #         [-65.5, 45.0]   # Northeast corner
    #     ],
    #     "center": [43.25, -68.25]
    # },
    # "cape_cod": {
    #     "name": "Cape Cod and Georges Bank",
    #     "group": "Atlantic Ocean",
    #     "bounds": [
    #         [-71.0, 40.5],  # Southwest corner
    #         [-66.0, 42.5]   # Northeast corner
    #     ],
    #     "center": [41.5, -68.5]
    # },
    # "ne_canyons": {
    #     "name": "Northeast Canyons",
    #     "group": "Atlantic Ocean",
    #     "bounds": [
    #         [-77.0, 36.0],  # Southwest corner
    #         [-65.0, 42.0]   # Northeast corner
    #     ],
    #     "center": [39.0, -71.0]
    # },
    # "carolinas": {
    #     "name": "Carolinas",
    #     "group": "Atlantic Ocean",
    #     "bounds": [
    #         [-79.0, 33.0],  # Southwest corner
    #         [-72.0, 37.0]   # Northeast corner
    #     ],
    #     "center": [35.0, -75.5]
    # },
    # "sc_ga": {
    #     "name": "South Carolina and Georgia",
    #     "group": "Atlantic Ocean",
    #     "bounds": [
    #         [-81.75, 30.5],  # Southwest corner
    #         [-75.0, 34.25]   # Northeast corner
    #     ],
    #     "center": [32.375, -78.375]
    # },
    
    # # Southeast & Caribbean
    # "florida_overview": {
    #     "name": "Florida Overview",
    #     "group": "Southeast & Caribbean",
    #     "bounds": [
    #         [-88.0, 23.0],  # Southwest corner
    #         [-77.0, 31.0]   # Northeast corner
    #     ],
    #     "center": [27.0, -82.5]
    # },
    # "bahamas": {
    #     "name": "Bahamas",
    #     "group": "Southeast & Caribbean",
    #     "bounds": [
    #         [-80.0, 21.5],  # Southwest corner
    #         [-74.0, 28.0]   # Northeast corner
    #     ],
    #     "center": [24.75, -77.0]
    # },
    
    # Gulf Coast
    "gulf_of_mexico": {
        "name": "Gulf of Mexico",
        "group": "Gulf Coast",
        "bounds": [
            [-98.0, 18.0],  # Southwest corner
            [-80.0, 31.0]   # Northeast corner
        ],
        "center": [24.5, -89.0]
    },
    
    # # Pacific Ocean
    # "southern_alaska": {
    #     "name": "Southern Alaska",
    #     "group": "Pacific Ocean",
    #     "bounds": [
    #         [-150.0, 54.0],  # Southwest corner
    #         [-130.0, 61.0]   # Northeast corner
    #     ],
    #     "center": [57.5, -140.0]
    # },
    # "british_columbia": {
    #     "name": "British Columbia",
    #     "group": "Pacific Ocean",
    #     "bounds": [
    #         [-132.0, 48.0],  # Southwest corner
    #         [-122.0, 54.0]   # Northeast corner
    #     ],
    #     "center": [51.0, -127.0]
    # },
    # "west_coast": {
    #     "name": "U.S. West Coast",
    #     "group": "Pacific Ocean",
    #     "bounds": [
    #         [-127.0, 32.0],  # Southwest corner (San Diego)
    #         [-116.0, 49.0]   # Northeast corner (Canadian border)
    #     ],
    #     "center": [40.5, -121.5]
    # },
    # "pnw": {
    #     "name": "Pacific Northwest",
    #     "group": "Pacific Ocean",
    #     "bounds": [
    #         [-127.0, 42.0],  # Southwest corner (Oregon-California border)
    #         [-122.0, 49.0]   # Northeast corner (Canadian border)
    #     ],
    #     "center": [45.5, -124.5]
    # },
    # "central_cal": {
    #     "name": "Central California",
    #     "group": "Pacific Ocean",
    #     "bounds": [
    #         [-127.0, 35.0],  # Southwest corner (Point Conception)
    #         [-121.0, 39.0]   # Northeast corner (Cape Mendocino)
    #     ],
    #     "center": [37.0, -124.0]
    # },
    # "socal": {
    #     "name": "Southern California",
    #     "group": "Pacific Ocean",
    #     "bounds": [
    #         [-127.0, 32.0],  # Southwest corner (San Diego)
    #         [-117.0, 35.0]   # Northeast corner (Point Conception)
    #     ],
    #     "center": [33.5, -122.0]
    # },
    
    # # Mexico
    # "baja": {
    #     "name": "Baja California",
    #     "group": "Mexico",
    #     "bounds": [
    #         [-118.0, 22.5],  # Southwest corner (Cabo San Lucas)
    #         [-109.0, 32.5]   # Northeast corner (Includes San Diego)
    #     ],
    #     "center": [27.5, -113.5]
    # },
    
    # # Pacific Islands
    # "hawaii": {
    #     "name": "Hawaiian Islands",
    #     "group": "Pacific Ocean",
    #     "bounds": [
    #         [-161.0, 18.0],  # Southwest corner
    #         [-154.0, 23.0]   # Northeast corner
    #     ],
    #     "center": [20.5, -157.5]
    # }
}