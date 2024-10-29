import json
from datetime import datetime
from config.settings import SOURCES

class MetadataAssembler:
    def __init__(self, path_manager):
        """Initialize with path manager"""
        self.path_manager = path_manager

    def assemble_metadata(self, date: datetime, dataset: str, region: str, asset_paths) -> dict:
        """Assemble metadata in the new format and save to disk"""
        now = datetime.now()
        
        # Build layers paths
        layers = {}
        if asset_paths.image.exists():
            layers["image"] = str(asset_paths.image.relative_to(self.path_manager.base_dir))
        if asset_paths.contours and asset_paths.contours.exists():
            layers["contours"] = str(asset_paths.contours.relative_to(self.path_manager.base_dir))
            
        metadata = {
            "id": dataset,
            "name": SOURCES[dataset]["name"],
            "type": SOURCES[dataset]["type"],
            "supportedLayers": SOURCES[dataset]["supportedLayers"],
            "dates": [
                {
                    "date": date.strftime('%Y%m%d'),
                    "processing_time": now.isoformat(),
                    "layers": layers
                }
            ]
        }
        
        # Save metadata to file
        with open(asset_paths.metadata, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return metadata
