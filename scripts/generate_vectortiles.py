import os
import subprocess
import shutil
import logging
from pathlib import Path
import geopandas as gpd
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class VectorTileGenerator:
    """Handles the generation of vector tiles from bathymetry data."""
    
    def __init__(self, project_root: Optional[str] = None):
        """Initialize with project paths."""
        self.paths = self._get_project_paths(project_root)
        self.required_tools = ['ogr2ogr', 'tippecanoe', 'mb-util']

    def _get_project_paths(self, project_root: Optional[str] = None) -> Dict[str, str]:
        """Get standardized project paths."""
        if project_root is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        return {
            'input_gdb': os.path.join(project_root, "downloaded_data/BathymetricContour/BathymetryContours.gdb"),
            'output_dir': os.path.join(project_root, "output/bathymetry"),
            'tiles_dir': os.path.join(project_root, "output/bathymetry/tiles"),
            'temp_geojson': os.path.join(project_root, "output/bathymetry/temp_bathy.geojson"),
            'mbtiles': os.path.join(project_root, "output/bathymetry/tiles/bathymetry.mbtiles"),
            'static_tiles': os.path.join(project_root, "output/bathymetry/tiles/static")
        }

    def _check_dependencies(self) -> None:
        """Verify required command line tools are installed."""
        missing_tools = [tool for tool in self.required_tools if shutil.which(tool) is None]
        
        if missing_tools:
            msg = "Missing required dependencies. Please install:\n"
            msg += "\nFor Ubuntu/Debian systems:\n"
            for tool in missing_tools:
                msg += f"sudo apt-get install {tool}\n"
            raise SystemError(msg)

    def _clean_existing_files(self) -> None:
        """Remove existing output files and create directories."""
        try:
            if os.path.exists(self.paths['static_tiles']):
                shutil.rmtree(self.paths['static_tiles'])
            if os.path.exists(self.paths['mbtiles']):
                os.remove(self.paths['mbtiles'])
            if os.path.exists(f"{self.paths['mbtiles']}-journal"):
                os.remove(f"{self.paths['mbtiles']}-journal")
            if os.path.exists(self.paths['temp_geojson']):
                os.remove(self.paths['temp_geojson'])

            # Ensure output directories exist
            Path(self.paths['output_dir']).mkdir(parents=True, exist_ok=True)
            Path(self.paths['tiles_dir']).mkdir(parents=True, exist_ok=True)
            
            logger.info("Cleaned existing files and created directories")
        except Exception as e:
            logger.error(f"Error cleaning files: {str(e)}")
            raise

    def _convert_to_geojson(self) -> None:
        """Convert GDB to GeoJSON format."""
        try:
            subprocess.run([
                "ogr2ogr",
                "-f", "GeoJSON",
                "-t_srs", "EPSG:4326",
                "-mapFieldType", "Binary=String",
                self.paths['temp_geojson'],
                self.paths['input_gdb']
            ], check=True)
            logger.info("Converted GDB to GeoJSON successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error converting to GeoJSON: {str(e)}")
            raise

    def _process_bathymetry(self) -> None:
        """Process bathymetry data to convert negative depths to positive."""
        try:
            gdf = gpd.read_file(self.paths['temp_geojson'])
            gdf['Contour'] = gdf['Contour'].abs()
            gdf.to_file(self.paths['temp_geojson'], driver='GeoJSON')
            logger.info("Processed bathymetry data successfully")
        except Exception as e:
            logger.error(f"Error processing bathymetry: {str(e)}")
            raise

    def _generate_mbtiles(self) -> None:
        """Generate vector tiles using tippecanoe."""
        try:
            subprocess.run([
                "tippecanoe",
                "-o", self.paths['mbtiles'],
                "--minimum-zoom=6",
                "--maximum-zoom=14",
                "--drop-densest-as-needed",
                "--extend-zooms-if-still-dropping",
                "--simplification=10",
                "--minimum-detail=12",
                "--no-tile-compression",
                "--clip-bounding-box=-125.0,24.396308,-66.934570,49.384358",
                "--layer=bathymetry",
                "--name=US Bathymetry",
                self.paths['temp_geojson']
            ], check=True)
            logger.info("Generated MBTiles successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating MBTiles: {str(e)}")
            raise

    def _export_vector_tiles(self) -> None:
        """Export MBTiles to PBF vector tiles."""
        try:
            subprocess.run([
                "mb-util",
                "--image_format=pbf",
                self.paths['mbtiles'],
                self.paths['static_tiles']
            ], check=True)
            logger.info("Exported vector tiles successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error exporting vector tiles: {str(e)}")
            raise

    def generate(self) -> None:
        """Main method to generate vector tiles."""
        try:
            logger.info("Starting vector tile generation")
            self._check_dependencies()
            self._clean_existing_files()
            self._convert_to_geojson()
            self._process_bathymetry()
            self._generate_mbtiles()
            self._export_vector_tiles()
            logger.info(f"Successfully generated vector tiles in: {self.paths['static_tiles']}")
        except Exception as e:
            logger.error(f"Vector tile generation failed: {str(e)}")
            raise
        finally:
            if os.path.exists(self.paths['temp_geojson']):
                os.remove(self.paths['temp_geojson'])
                logger.info("Cleaned up temporary files")

def main():
    """Main entry point."""
    try:
        generator = VectorTileGenerator()
        generator.generate()
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
