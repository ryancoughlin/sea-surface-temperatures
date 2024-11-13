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
# INPUT_GDB = "downloaded_data/BathymetricContour/BathymetryContours.gdb"
INPUT_GDB = "/Users/ryan/Downloads/BathymetricContour/BathymetryContours.gdb"
OUTPUT_DIR = "output/bathymetry"
TILES_DIR = os.path.join(OUTPUT_DIR, "tiles")
GEOJSON_FILE = os.path.join(TILES_DIR, "bathy.geojson")
MBTILES_FILE = os.path.join(TILES_DIR, "bathy.mbtiles")
STATIC_TILES_DIR = os.path.join(TILES_DIR, "static")

def generate_vector_tiles():
    """Generate vector tiles from GDB file."""
    # Create output directories
    Path(TILES_DIR).mkdir(parents=True, exist_ok=True)
    
    # Step 1: Convert to GeoJSON with reprojection
    try:
        gdf = gpd.read_file(INPUT_GDB)
        # Ensure we're working with LineString geometry
        gdf = gdf[gdf.geometry.type.isin(['LineString', 'MultiLineString'])]
        # Reproject to WGS84
        gdf = gdf.to_crs("EPSG:4326")
        gdf.to_file(GEOJSON_FILE, driver="GeoJSON")
        
        # Step 2: Generate vector tiles using tippecanoe
        subprocess.run([
            "tippecanoe",
            "-o", MBTILES_FILE,
            "-l", "bathymetry",              # Layer name
            "--minimum-zoom", "0",           # Min zoom level
            "--maximum-zoom", "15",          # Max zoom level
            "--no-line-simplification",      # Preserve line detail
            "--no-tiny-polygon-reduction",
            "--no-feature-limit",            # Don't limit features per tile
            "--no-tile-size-limit",          # Don't limit tile sizes
            "--no-tile-compression",         # Important: Don't compress tiles
            "--force",                       # Overwrite existing files
            "--drop-densest-as-needed",      # Drop features if tiles too large
            "--extend-zooms-if-still-dropping", # Add zoom levels if needed
            GEOJSON_FILE
        ], check=True)
        
        # Step 3: Extract tiles to static directory
        Path(STATIC_TILES_DIR).mkdir(parents=True, exist_ok=True)
        subprocess.run([
            "mb-util",
            "--image_format=pbf",
            MBTILES_FILE,
            STATIC_TILES_DIR
        ], check=True)
        
        # Step 4: Generate metadata.json
        metadata_file = os.path.join(OUTPUT_DIR, "metadata.json")
        metadata = {
            "version": "1.0.0",
            "name": "Bathymetric Contours",
            "format": "pbf",
            "minzoom": 0,
            "maxzoom": 15,
            "bounds": gdf.total_bounds.tolist(),
            "type": "line",
            "generator": "tippecanoe",
            "vector_layers": {
                "bathymetry": {
                    "description": "Bathymetric contour lines",
                    "minzoom": 0,
                    "maxzoom": 15,
                    "fields": {
                        "depth": "number",
                        "contour_type": "string"
                    }
                }
            },
            "attribution": "NOAA Bathymetric Contours",
            "description": "Bathymetric contour lines showing ocean depth",
            "lastUpdated": datetime.now().isoformat()
        }
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

def cleanup():
    """Clean up intermediate files."""
    if os.path.exists(GEOJSON_FILE):
        os.remove(GEOJSON_FILE)
        print("Cleaned up intermediate GeoJSON file.")

if __name__ == "__main__":
    generate_vector_tiles()
    cleanup()
    
    print("\nNext steps:")
    print("1. Upload the 'output/bathymetry' directory to your hosting service")
    print("2. Make sure your hosting service is configured to serve PBF files with the correct MIME type:")
    print("   application/x-protobuf for .pbf files")
    print("3. Enable CORS if needed")
