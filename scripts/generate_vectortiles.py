#!/usr/bin/env python3

import os
import subprocess
import shutil
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Union
import geopandas as gpd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class VectorTileGenerator:
    """Handles the generation of vector tiles from geospatial data.
    
    Supports various input formats including GDB and Shapefiles.
    """
    
    def __init__(self, input_path: str, output_dir: str, layer_name: Optional[str] = None):
        """Initialize the vector tile generator.
        
        Args:
            input_path: Path to input data (GDB directory or shapefile)
            output_dir: Directory for output files
            layer_name: Name for the data layer (defaults to filename without extension)
        """
        self.input_path = input_path
        self.output_dir = output_dir
        self.layer_name = layer_name or Path(input_path).stem
        self.paths = self._setup_paths()
        self.required_tools = ['ogr2ogr', 'tippecanoe', 'mb-util']

    def _setup_paths(self) -> Dict[str, str]:
        """Set up standardized project paths.
        
        Returns:
            Dictionary containing all required paths
        """
        return {
            'input_path': self.input_path,
            'output_dir': self.output_dir,
            'tiles_dir': os.path.join(self.output_dir, "tiles"),
            'temp_geojson': os.path.join(self.output_dir, "temp.geojson"),
            'mbtiles': os.path.join(self.output_dir, "tiles/output.mbtiles"),
            'static_tiles': os.path.join(self.output_dir, "tiles/static")
        }

    def _check_dependencies(self) -> None:
        """Verify required command line tools are installed.
        
        Raises:
            SystemError: If any required tool is missing
        """
        missing_tools = [tool for tool in self.required_tools if shutil.which(tool) is None]
        
        if missing_tools:
            msg = "Missing required dependencies. Please install:\n"
            msg += "\nFor macOS (using Homebrew):\n"
            for tool in missing_tools:
                msg += f"brew install {tool}\n"
            msg += "\nFor Ubuntu/Debian systems:\n"
            for tool in missing_tools:
                msg += f"sudo apt-get install {tool}\n"
            raise SystemError(msg)

    def _validate_input(self) -> None:
        """Validate input file exists and is of supported format.
        
        For shapefiles, checks for required supporting files:
        - .shp: The main file containing geometry data
        - .shx: The index file for geometry data
        - .dbf: The database file containing attribute data
        - .prj: The projection file defining coordinate system
        Optional files that may be present:
        - .sbn, .sbx: Spatial index files
        - .cpg: Code page file for character encoding
        - .qix: Quadtree spatial index
        
        Raises:
            ValueError: If input file is invalid or unsupported, or if required
                      supporting files are missing for shapefiles
        """
        if not os.path.exists(self.paths['input_path']):
            raise ValueError(f"Input path does not exist: {self.paths['input_path']}")
            
        input_path = Path(self.paths['input_path'])
        if input_path.suffix.lower() == '.gdb':
            if not input_path.is_dir():
                raise ValueError("GDB input must be a directory")
        elif input_path.suffix.lower() == '.shp':
            # Required supporting files for a valid shapefile
            required_extensions = {
                '.shx': 'Index file',
                '.dbf': 'Attribute database file',
                '.prj': 'Projection definition file'
            }
            # Optional supporting files that may enhance functionality
            optional_extensions = {
                '.sbn': 'Spatial index file',
                '.sbx': 'Spatial index file',
                '.cpg': 'Character encoding file',
                '.qix': 'Quadtree spatial index'
            }
            
            missing_files = []
            for ext, description in required_extensions.items():
                related_file = input_path.with_suffix(ext)
                if not related_file.exists():
                    missing_files.append(f"{ext} ({description})")
            
            if missing_files:
                raise ValueError(
                    f"Missing required shapefile components:\n" + 
                    "\n".join(f"- {f}" for f in missing_files) +
                    "\nA valid shapefile requires .shp, .shx, .dbf, and .prj files."
                )
                
            # Log info about optional files
            present_optional = []
            for ext, description in optional_extensions.items():
                if input_path.with_suffix(ext).exists():
                    present_optional.append(f"{ext} ({description})")
            
            if present_optional:
                logger.info("Found optional shapefile components:\n" + 
                          "\n".join(f"- {f}" for f in present_optional))
        else:
            raise ValueError("Input must be either a .gdb directory or .shp file")

    def _clean_existing_files(self) -> None:
        """Remove existing output files and create directories."""
        try:
            for path in [self.paths['static_tiles'], self.paths['mbtiles'], 
                        f"{self.paths['mbtiles']}-journal", self.paths['temp_geojson']]:
                if os.path.exists(path):
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)

            # Ensure output directories exist
            Path(self.paths['output_dir']).mkdir(parents=True, exist_ok=True)
            Path(self.paths['tiles_dir']).mkdir(parents=True, exist_ok=True)
            
            logger.info("Cleaned existing files and created directories")
        except Exception as e:
            logger.error(f"Error cleaning files: {e}")
            raise

    def _convert_to_geojson(self) -> None:
        """Convert input data to GeoJSON format.
        
        For shapefiles, this process automatically includes all the supporting files
        (.shx, .dbf, .prj) that are present in the same directory as the .shp file.
        OGR2OGR will use these files to properly convert the geometry, attributes,
        and coordinate system.
        """
        try:
            cmd = [
                "ogr2ogr",
                "-f", "GeoJSON",
                "-t_srs", "EPSG:4326",  # Convert to WGS84
                "-mapFieldType", "Binary=String",
            ]
            
            # Add spatial filtering if input is large
            if Path(self.input_path).suffix.lower() == '.shp':
                cmd.extend([
                    "-skipfailures",  # Skip features that cause errors
                    "-lco", "RFC7946=YES",  # Follow RFC 7946 GeoJSON standard
                ])
            
            cmd.extend([self.paths['temp_geojson'], self.paths['input_path']])
            
            subprocess.run(cmd, check=True)
            logger.info("Converted input to GeoJSON successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error converting to GeoJSON: {e}")
            raise

    def _generate_mbtiles(self, min_zoom: int = 4, max_zoom: int = 14) -> None:
        """Generate vector tiles using tippecanoe.
        
        Args:
            min_zoom: Minimum zoom level
            max_zoom: Maximum zoom level
        """
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
                "--description=Vector tiles generated by VectorTileGenerator",
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
        """Generate vector tiles from input data.
        
        Args:
            min_zoom: Minimum zoom level
            max_zoom: Maximum zoom level
            
        Raises:
            Various exceptions with detailed error messages
        """
        try:
            self._check_dependencies()
            self._validate_input()
            self._clean_existing_files()
            self._convert_to_geojson()
            self._generate_mbtiles(min_zoom, max_zoom)
            self._export_static_tiles()
            logger.info("Vector tile generation completed successfully")
        except Exception as e:
            logger.error(f"Error generating vector tiles: {e}")
            raise

def parse_args() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Generate vector tiles from GDB or Shapefile input"
    )
    parser.add_argument(
        "input_path",
        help="Path to input GDB directory or Shapefile"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="output",
        help="Output directory (default: ./output)"
    )
    parser.add_argument(
        "-n", "--name",
        help="Name for the data layer (defaults to input filename)"
    )
    parser.add_argument(
        "--min-zoom",
        type=int,
        default=0,
        help="Minimum zoom level (default: 0)"
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=14,
        help="Maximum zoom level (default: 14)"
    )
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    try:
        args = parse_args()
        generator = VectorTileGenerator(args.input_path, args.output_dir, args.name)
        generator.generate_tiles(args.min_zoom, args.max_zoom)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
