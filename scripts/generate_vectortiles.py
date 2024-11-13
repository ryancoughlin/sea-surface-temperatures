import geopandas as gpd
import os
import subprocess
import fiona
from fiona.drvsupport import supported_drivers
from pathlib import Path
import shutil
import json
from datetime import datetime

# Enable FileGDB driver
supported_drivers['FileGDB'] = 'raw'

# Consolidated paths
INPUT_GDB = "/Users/ryan/Downloads/BathymetricContour/BathymetryContours.gdb"
OUTPUT_DIR = "output/bathymetry"  # Changed to use output/bathymetry
TILES_DIR = os.path.join(OUTPUT_DIR, "tiles")
GEOJSON_FILE = os.path.join(TILES_DIR, "bathy.geojson")
MBTILES_FILE = os.path.join(TILES_DIR, "bathy.mbtiles")
STATIC_TILES_DIR = os.path.join(TILES_DIR, "static")

def generate_vector_tiles():
    """Generate vector tiles from GDB file."""
    # Create output directories
    Path(TILES_DIR).mkdir(parents=True, exist_ok=True)
    
    # Step 1: List available layers
    try:
        layers = fiona.listlayers(INPUT_GDB)
        print("Available layers:", layers)
    except Exception as e:
        print(f"Error listing layers: {str(e)}")
        raise

    # Step 2: Convert to GeoJSON with reprojection
    layer_name = layers[0]
    gdf = gpd.read_file(INPUT_GDB, layer=layer_name)
    gdf = gdf.to_crs(epsg=4326)
    gdf.to_file(GEOJSON_FILE, driver="GeoJSON")
    print(f"Exported {layer_name} to GeoJSON: {GEOJSON_FILE}")

    # Step 3: Convert GeoJSON to MBTiles
    subprocess.run([
        "tippecanoe",
        "-o", MBTILES_FILE,
        "-zg",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--force",
        GEOJSON_FILE
    ], check=True)
    print(f"Generated MBTiles: {MBTILES_FILE}")

def convert_to_static():
    """Convert MBTiles to static PBF files."""
    # Remove the static directory if it exists
    if os.path.exists(STATIC_TILES_DIR):
        shutil.rmtree(STATIC_TILES_DIR)
    
    try:
        # Extract tiles
        print(f"Converting {MBTILES_FILE} to static tiles...")
        subprocess.run([
            "mb-util",
            MBTILES_FILE,
            STATIC_TILES_DIR,
            "--image_format=pbf"
        ], check=True)
        
        print(f"Successfully extracted tiles to {STATIC_TILES_DIR}")
        
        # Create metadata.json in the bathymetry root directory
        metadata_file = os.path.join(OUTPUT_DIR, "metadata.json")
        if not os.path.exists(metadata_file):
            print("Creating metadata.json...")
            metadata = {
                "type": "bathymetry",
                "tilesets": {
                    "vector": {
                        "format": "pbf",
                        "url": "tiles/static/{z}/{x}/{y}.pbf",
                        "mimeType": "application/x-protobuf"
                    }
                },
                "attribution": "NOAA Bathymetric Contours",
                "description": "Bathymetric contour lines showing ocean depth",
                "lastUpdated": datetime.now().isoformat()
            }
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
    except subprocess.CalledProcessError as e:
        print(f"Error converting tiles: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise

def cleanup():
    """Clean up intermediate files."""
    if os.path.exists(GEOJSON_FILE):
        os.remove(GEOJSON_FILE)
        print("Cleaned up intermediate GeoJSON file.")

if __name__ == "__main__":
    generate_vector_tiles()
    convert_to_static()
    cleanup()
    
    print("\nNext steps:")
    print("1. Upload the 'output/bathymetry' directory to your hosting service")
    print("2. Make sure your hosting service is configured to serve PBF files with the correct MIME type:")
    print("   application/x-protobuf for .pbf files")
    print("3. Enable CORS if needed")
    print("\nClient-side usage:")
    print("URL pattern: ${SERVER_URL}/bathymetry/tiles/static/{z}/{x}/{y}.pbf")
