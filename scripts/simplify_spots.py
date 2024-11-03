import json
import sys
from pathlib import Path

def convert_to_geojson(input_file):
    """Convert spots.json to GeoJSON format."""
    
    # Read input file
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
            # Get the lab_lay array
            spots = data['lab_lay']
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"Error reading file: {e}")
        sys.exit(1)
    
    # Create GeoJSON structure
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    # Convert each spot to GeoJSON feature
    for item in spots:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(item["lng"]), float(item["lat"])]
            },
            "properties": {
                "id": item["id"],
                "name": item["name"]
            }
        }
        geojson["features"].append(feature)
    
    # Create output filename
    output_file = Path(input_file).stem + '_geo.json'
    
    # Write GeoJSON file
    with open(output_file, 'w') as f:
        json.dump(geojson, f, indent=2)
    
    print(f"Converted {len(geojson['features'])} features")
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py spots.json")
        sys.exit(1)
        
    input_file = sys.argv[1]
    convert_to_geojson(input_file)