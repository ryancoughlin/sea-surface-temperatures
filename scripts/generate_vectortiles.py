import os
import subprocess
import shutil
from pathlib import Path

def get_project_paths():
    """Get standardized project paths."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return {
        'input_gdb': os.path.join(project_root, "downloaded_data/BathymetricContour/BathymetryContours.gdb"),
        'output_dir': os.path.join(project_root, "output/bathymetry"),
        'tiles_dir': os.path.join(project_root, "output/bathymetry/tiles"),
        'temp_geojson': os.path.join(project_root, "output/bathymetry/temp_bathy.geojson"),
        'mbtiles': os.path.join(project_root, "output/bathymetry/tiles/bathymetry.mbtiles"),
        'static_tiles': os.path.join(project_root, "output/bathymetry/tiles/static")
    }

def remove_tiles_and_mbtiles():
    """Remove existing tiles and mbtiles."""
    paths = get_project_paths()
    
    # Clean up existing files
    if os.path.exists(paths['static_tiles']):
        shutil.rmtree(paths['static_tiles'])
    if os.path.exists(paths['mbtiles']):
        os.remove(paths['mbtiles'])
    if os.path.exists(paths['temp_geojson']):
        os.remove(paths['temp_geojson'])
    
    # Ensure output directory exists
    Path(paths['output_dir']).mkdir(parents=True, exist_ok=True)
    Path(paths['tiles_dir']).mkdir(parents=True, exist_ok=True)

def check_dependencies():
    """Check if required command line tools are installed."""
    required_tools = ['ogr2ogr', 'tippecanoe', 'mb-util']
    missing_tools = []
    
    for tool in required_tools:
        if shutil.which(tool) is None:
            missing_tools.append(tool)
    
    if missing_tools:
        print("Missing required dependencies. Please install the following tools:")
        print("\nFor Ubuntu/Debian systems, run:")
        if 'ogr2ogr' in missing_tools:
            print("sudo apt-get install gdal-bin")
        if 'tippecanoe' in missing_tools:
            print("sudo apt-get install tippecanoe")
        if 'mb-util' in missing_tools:
            print("sudo apt-get install mb-util")
        raise SystemExit(1)

def generate_vector_tiles():
    """Generate vector tiles optimized for bathymetric visualization."""
    paths = get_project_paths()

    try:
        check_dependencies()
        remove_tiles_and_mbtiles()

        # Convert GDB to GeoJSON with reprojection and field type mapping
        subprocess.run([
            "ogr2ogr",
            "-f", "GeoJSON",
            "-t_srs", "EPSG:4326",
            "-mapFieldType", "Binary=String",  # Handle binary field conversion
            paths['temp_geojson'],
            paths['input_gdb']
        ], check=True)

        # Generate vector tiles with optimized parameters
        subprocess.run([
            "tippecanoe",
            "-o", paths['mbtiles'],
            "--minimum-zoom=6",      # Regional level
            "--maximum-zoom=15",     # Local detail
            "--layer=bathymetry",
            "--name=bathymetry",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--simplification=10",   # Smooth lines
            "--force",
            "--no-tile-compression", # Faster tile serving
            "--no-feature-limit",    # Preserve all features
            "--no-tile-size-limit",  # Allow larger tiles for detail
            paths['temp_geojson']
        ], check=True)

        # Extract tiles to static directory
        subprocess.run([
            "mb-util",
            "--image_format=pbf",
            paths['mbtiles'],
            paths['static_tiles']
        ], check=True)

        print(f"Successfully generated vector tiles in: {paths['static_tiles']}")

    except Exception as e:
        print(f"Error generating vector tiles: {str(e)}")
        raise
    finally:
        # Clean up temporary files
        if os.path.exists(paths['temp_geojson']):
            os.remove(paths['temp_geojson'])

if __name__ == "__main__":
    generate_vector_tiles()
