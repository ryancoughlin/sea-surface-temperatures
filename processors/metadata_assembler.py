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
        
        # Save dataset-level metadata
        with open(asset_paths.metadata, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        # Update region-level metadata
        self.update_region_metadata(region, dataset, date, asset_paths)
        
        return metadata

    def update_region_metadata(self, region: str, dataset: str, date: datetime, asset_paths) -> None:
        """Update or create the region-level metadata file that aggregates all datasets"""
        region_metadata_path = self.path_manager.output_dir / region / "metadata.json"
        
        # Load existing metadata if it exists, or create new
        if region_metadata_path.exists():
            with open(region_metadata_path) as f:
                region_metadata = json.load(f)
        else:
            region_metadata = {}
            
        # Initialize dataset entry if it doesn't exist
        if dataset not in region_metadata:
            region_metadata[dataset] = {
                "id": dataset,
                "name": SOURCES[dataset]["name"],
                "type": SOURCES[dataset]["type"],
                "supportedLayers": SOURCES[dataset]["supportedLayers"],
                "dates": []
            }
            
        # Add or update the date entry
        date_str = date.strftime('%Y%m%d')
        layers = {}
        if asset_paths.image.exists():
            layers["image"] = str(asset_paths.image.relative_to(self.path_manager.base_dir))
        if asset_paths.contours and asset_paths.contours.exists():
            layers["contours"] = str(asset_paths.contours.relative_to(self.path_manager.base_dir))
            
        # Remove existing entry for this date if it exists
        region_metadata[dataset]["dates"] = [
            d for d in region_metadata[dataset]["dates"] 
            if d["date"] != date_str
        ]
        
        # Add new date entry
        region_metadata[dataset]["dates"].append({
            "date": date_str,
            "processing_time": datetime.now().isoformat(),
            "layers": layers
        })
        
        # Save updated metadata
        with open(region_metadata_path, 'w') as f:
            json.dump(region_metadata, f, indent=2)