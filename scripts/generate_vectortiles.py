#!/usr/bin/env python3

import os
import json
import subprocess
import shutil
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Optional
import sys

# Add project root to Python path
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from config.settings import PATHS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class VectorTileGenerator:
    """Handles the generation of vector tiles from geospatial data."""
    
    def __init__(self, input_path: str, layer_name: str):
        """Initialize the vector tile generator.
        
        Args:
            input_path: Path to input data (GDB directory or shapefile)
            layer_name: Name for the data layer
        """
        self.input_path = input_path
        self.layer_name = layer_name
        self.output_dir = PATHS['VECTOR_TILES_DIR'] / layer_name
        self.paths = self._setup_paths()
        self.required_tools = ['ogr2ogr', 'tippecanoe', 'mb-util']

    def _setup_paths(self) -> Dict[str, str]:
        """Set up standardized project paths."""
        return {
            'input_path': self.input_path,
            'output_dir': str(self.output_dir),
            'tiles_dir': str(self.output_dir / 'tiles'),
            'temp_geojson': str(self.output_dir / 'temp.geojson'),
            'mbtiles': str(self.output_dir / 'tiles' / f'{self.layer_name}.mbtiles'),
            'static_tiles': str(self.output_dir / 'tiles' / 'static')
        }

    def _check_dependencies(self) -> None:
        """Verify required command line tools are installed."""
        missing_tools = [tool for tool in self.required_tools if shutil.which(tool) is None]
        if missing_tools:
            msg = "Missing required dependencies. Please install:\n"
            msg += "\nFor macOS (using Homebrew):\n"
            for tool in missing_tools:
                msg += f"brew install {tool}\n"
            raise SystemError(msg)

    def _validate_input(self) -> None:
        """Validate input file exists and is of supported format."""
        if not os.path.exists(self.paths['input_path']):
            raise ValueError(f"Input path does not exist: {self.paths['input_path']}")
            
        input_path = Path(self.paths['input_path'])
        if input_path.suffix.lower() == '.gdb':
            if not input_path.is_dir():
                raise ValueError("GDB input must be a directory")
        elif input_path.suffix.lower() == '.shp':
            # Check for required shapefile components
            required_files = {
                '.shx': 'Index file',
                '.dbf': 'Attribute database file',
                '.prj': 'Projection definition file'
            }
            missing = []
            for ext, desc in required_files.items():
                if not input_path.with_suffix(ext).exists():
                    missing.append(f"{ext} ({desc})")
            if missing:
                raise ValueError(f"Missing required shapefile components:\n" + 
                              "\n".join(f"- {f}" for f in missing))
        else:
            raise ValueError("Input must be either a .gdb directory or .shp file")

    def _clean_existing_files(self) -> None:
        """Remove existing output files and create directories."""
        try:
            if os.path.exists(self.output_dir):
                shutil.rmtree(self.output_dir)
            os.makedirs(self.paths['tiles_dir'])
            logger.info("Cleaned existing files and created directories")
        except Exception as e:
            logger.error(f"Error cleaning files: {e}")
            raise

    def _convert_to_geojson(self) -> None:
        """Convert input data to GeoJSON format."""
        try:
            subprocess.run([
                "ogr2ogr",
                "-f", "GeoJSON",
                "-t_srs", "EPSG:4326",
                "-mapFieldType", "Binary=String",
                self.paths['temp_geojson'],
                self.paths['input_path']
            ], check=True)
            logger.info("Converted to GeoJSON successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error converting to GeoJSON: {e}")
            raise

    def _generate_mbtiles(self, min_zoom: int = 4, max_zoom: int = 14) -> None:
        """Generate vector tiles using tippecanoe."""
        try:
            subprocess.run([
                "tippecanoe",
                "-o", self.paths['mbtiles'],
                "--drop-densest-as-needed",
                "--extend-zooms-if-still-dropping",
                f"--minimum-zoom={min_zoom}",
                f"--maximum-zoom={max_zoom}",
                "--force",
                f"--layer={self.layer_name}",
                f"--name={self.layer_name}",
                self.paths['temp_geojson']
            ], check=True)
            logger.info("Generated MBTiles successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating MBTiles: {e}")
            raise

    def _export_static_tiles(self) -> None:
        """Export MBTiles to static tile directory."""
        try:
            subprocess.run([
                "mb-util",
                "--image_format=pbf",
                self.paths['mbtiles'],
                self.paths['static_tiles']
            ], check=True)
            logger.info("Exported static tiles successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error exporting static tiles: {e}")
            raise

    def generate_tiles(self, min_zoom: int = 0, max_zoom: int = 14) -> None:
        """Generate vector tiles from input data."""
        try:
            self._check_dependencies()
            self._validate_input()
            self._clean_existing_files()
            self._convert_to_geojson()
            self._generate_mbtiles(min_zoom, max_zoom)
            self._export_static_tiles()
            
            # Create layer info file
            layer_info = {
                "id": self.layer_name,
                "name": self.layer_name,
                "type": "vector",
                "tiles": [f"/tiles/{self.layer_name}/tiles/static/{{z}}/{{x}}/{{y}}.pbf"],
                "minzoom": min_zoom,
                "maxzoom": max_zoom
            }
            
            with open(self.output_dir / 'layer.json', 'w') as f:
                json.dump(layer_info, f, indent=2)
                
            logger.info(f"Vector tiles generated successfully in {self.output_dir}")
        except Exception as e:
            logger.error(f"Error generating vector tiles: {e}")
            raise

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate vector tiles from GDB or Shapefile input"
    )
    parser.add_argument(
        "input_path",
        help="Path to input GDB directory or Shapefile"
    )
    parser.add_argument(
        "-n", "--name",
        required=True,
        help="Name for the vector tile layer"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="layers",
        help="Output directory (default: layers)"
    )
    parser.add_argument(
        "--min-zoom",
        type=int,
        default=0,
        help="Minimum zoom level"
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=14,
        help="Maximum zoom level"
    )
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    try:
        args = parse_args()
        generator = VectorTileGenerator(args.input_path, args.name)
        generator.generate_tiles(args.min_zoom, args.max_zoom)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
