import os
import subprocess
import shutil
from pathlib import Path
import geopandas as gpd

def get_project_paths():
    """Get standardized project paths."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return {
        'input_gdb': os.path.join(project_root, "downloaded_data/BathymetricContour/BathymetryContours.gdb"),
        'output_dir': os.path.join(project_root, "output/bathymetry"),
        'tiles_dir': os.path.join(project_root, "output/bathymetry/tiles")
    }

def ensure_clean_directory(directory):
    """Ensure directory exists and is empty."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    Path(directory).mkdir(parents=True, exist_ok=True)

def generate_vector_tiles():
    """Generate vector tiles from GDB file."""
    paths = get_project_paths()
    
    # Setup file paths
    geojson_file = os.path.join(paths['tiles_dir'], "bathy.geojson")
    mbtiles_file = os.path.join(paths['tiles_dir'], "bathy.mbtiles")
    static_tiles_dir = os.path.join(paths['tiles_dir'], "static")

    try:
        # Create clean directories
        ensure_clean_directory(paths['output_dir'])
        ensure_clean_directory(paths['tiles_dir'])
        
        # Convert GDB to GeoJSON using geopandas
        gdf = gpd.read_file(paths['input_gdb'])
        gdf = gdf.to_crs("EPSG:4326")  # Reproject to WGS84
        gdf.to_file(geojson_file, driver="GeoJSON")

        # Generate vector tiles
        subprocess.run([
            "tippecanoe",
            "--output", mbtiles_file,
            "--layer", "bathymetry",
            "--minimum-zoom", "6",
            "--maximum-zoom", "22",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--force",
            geojson_file
        ], check=True)

        # Extract tiles to static directory
        ensure_clean_directory(static_tiles_dir)
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
