import os
import subprocess
import shutil
from pathlib import Path
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PATHS = {
    'input_gdb': os.path.join(PROJECT_ROOT, "downloaded_data/BathymetricContour/BathymetryContours.gdb"),
    'output_dir': os.path.join(PROJECT_ROOT, "output/bathymetry"),
    'tiles_dir': os.path.join(PROJECT_ROOT, "output/bathymetry/tiles"),
}

def generate_vector_tiles():
    """Generate vector tiles from GDB file."""
    # Create output directories
    Path(PATHS['output_dir']).mkdir(parents=True, exist_ok=True)
    Path(PATHS['tiles_dir']).mkdir(parents=True, exist_ok=True)
    
    geojson_file = os.path.join(PATHS['tiles_dir'], "bathy.geojson")
    mbtiles_file = os.path.join(PATHS['tiles_dir'], "bathy.mbtiles")
    static_tiles_dir = os.path.join(PATHS['tiles_dir'], "static")

    try:
        # First convert GDB to GeoJSON with explicit projection
        subprocess.run([
            "ogr2ogr",
            "-f", "GeoJSON",
            "-t_srs", "EPSG:4326",  # Explicitly set output projection to WGS84
            geojson_file,
            PATHS['input_gdb']
        ], check=True)

        # Remove existing mbtiles file if it exists
        if os.path.exists(mbtiles_file):
            os.remove(mbtiles_file)

        # Generate vector tiles
        subprocess.run([
            "tippecanoe",
            "--output", mbtiles_file,
            "--layer", "bathymetry",
            "--minimum-zoom", "0",
            "--maximum-zoom", "15",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--force",
            "--projection", "EPSG:4326",  # Explicitly set projection
            geojson_file
        ], check=True)

        # Remove and recreate static tiles directory
        if os.path.exists(static_tiles_dir):
            shutil.rmtree(static_tiles_dir)
        
        # Create a new static tiles directory with a timestamp to ensure uniqueness
        static_tiles_dir = os.path.join(PATHS['tiles_dir'], f"static_{int(time.time())}")
        os.makedirs(static_tiles_dir)
        
        # Extract tiles
        subprocess.run([
            "mb-util",
            "--image_format=pbf",
            mbtiles_file,
            static_tiles_dir
        ], check=True)
        
        print(f"Successfully generated vector tiles in: {static_tiles_dir}")
        
    except Exception as e:
        print(f"Error generating vector tiles: {str(e)}")
        raise

if __name__ == "__main__":
    generate_vector_tiles()
