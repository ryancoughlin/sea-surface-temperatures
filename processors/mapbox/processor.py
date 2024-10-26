import subprocess
import logging
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from functools import partial

logger = logging.getLogger(__name__)

class MapboxProcessor:
    def __init__(self):
        self.username = "snowcast"  # Your Mapbox username
        
    async def process(self, data_path: Path, region: str, dataset: str) -> Optional[str]:
        """Process and upload data to Mapbox Tilesets"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d')
            
            # Create consistent IDs for organization
            source_id = self._get_source_id(dataset, region, timestamp)
            tileset_id = self._get_tileset_id(dataset, region)
            
            logger.info(f"Processing Mapbox tileset for {dataset} in {region}")
            
            # Run commands in executor
            loop = asyncio.get_running_loop()
            
            # 1. Upload source
            await loop.run_in_executor(
                None,
                partial(self._run_command, [
                    'tilesets', 
                    'upload-raster-source',
                    self.username,
                    source_id,
                    str(data_path)
                ])
            )
            
            # 2. Create recipe
            recipe = self._create_recipe(source_id, dataset)
            recipe_path = data_path.parent / "recipe.json"
            with open(recipe_path, 'w') as f:
                json.dump(recipe, f, indent=2)
            
            # 3. Create tileset
            await loop.run_in_executor(
                None,
                partial(self._run_command, [
                    'tilesets',
                    'create',
                    tileset_id,
                    '--recipe',
                    str(recipe_path),
                    '--name',
                    f"{dataset} - {region}"
                ])
            )
            
            # 4. Publish tileset
            await loop.run_in_executor(
                None,
                partial(self._run_command, [
                    'tilesets',
                    'publish',
                    tileset_id
                ])
            )
            
            # Return the Mapbox URL for the tileset
            mapbox_url = f"mapbox://{tileset_id}"
            logger.info(f"Successfully published tileset: {mapbox_url}")
            return mapbox_url
            
        except Exception as e:
            logger.error(f"Error processing Mapbox tileset: {str(e)}")
            return None

    def _run_command(self, cmd: list) -> None:
        """Run a command and handle its output"""
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"Command succeeded: {' '.join(cmd)}")
            logger.info(result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {' '.join(cmd)}")
            logger.error(e.stderr)
            raise

    def _get_source_id(self, dataset: str, region: str, timestamp: str) -> str:
        """Generate consistent source ID format"""
        return f"{dataset}_{region}_{timestamp}"

    def _get_tileset_id(self, dataset: str, region: str) -> str:
        """Generate consistent tileset ID format"""
        return f"{self.username}.{dataset}_{region}"

    def _create_recipe(self, source_id: str, dataset: str) -> Dict:
        """Create appropriate recipe based on dataset type"""
        recipe = {
            "version": 1,
            "layers": {
                dataset: {
                    "source": f"mapbox://tileset-source/{self.username}/{source_id}",
                    "minzoom": 0,
                    "maxzoom": 6
                }
            }
        }
        
        # Dataset-specific configurations
        if dataset == "BLENDEDNRTcurrentsDaily":
            recipe["layers"][dataset].update({
                "maxzoom": 6,
                "raster_band": "u_current,v_current"
            })
        elif dataset == "blended_sst":
            recipe["layers"][dataset].update({
                "maxzoom": 8,
                "raster_band": "analysed_sst"
            })
            
        return recipe
