from pathlib import Path
from datetime import datetime
import logging
import json
from config.settings import SOURCES
from config.regions import REGIONS
from typing import Optional, Dict
from utils.path_manager import PathManager, AssetPaths

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager

    def assemble_metadata(self, region: str, dataset: str, date: datetime) -> Path:
        """Assemble metadata for processed assets."""
        # Get all paths from PathManager
        asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
        
        metadata = {
            "region": region,
            "dataset": dataset,
            "date": date.strftime('%Y%m%d'),
            "paths": {
                "image": str(asset_paths.image.relative_to(self.path_manager.base_dir)),
                "metadata": str(asset_paths.metadata.relative_to(self.path_manager.base_dir))
            }
        }
        
        if asset_paths.contours:
            metadata["paths"]["contours"] = str(asset_paths.contours.relative_to(self.path_manager.base_dir))
        
        # Ensure directory exists
        asset_paths.metadata.parent.mkdir(parents=True, exist_ok=True)
        
        # Write metadata
        with open(asset_paths.metadata, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        return asset_paths.metadata
