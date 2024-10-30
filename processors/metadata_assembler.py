import json
from datetime import datetime
from config.settings import SOURCES

class MetadataAssembler:
    def __init__(self, path_manager):
        """Initialize with path manager"""
        self.path_manager = path_manager

    def assemble_metadata(self, date: datetime, dataset: str, region: str, asset_paths) -> dict:
        """Assemble metadata for a single dataset and update the global metadata file"""
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
            
        # Update global metadata file
        self.update_global_metadata(region, dataset, date, asset_paths)
        
        return metadata

    def update_global_metadata(self, region: str, dataset: str, date: datetime, asset_paths) -> None:
        """Update or create the global metadata file that contains all regions and datasets"""
        global_metadata_path = self.path_manager.output_dir / "metadata.json"
        
        # Load existing metadata if it exists, or create new
        if global_metadata_path.exists():
            with open(global_metadata_path) as f:
                metadata = json.load(f)
        else:
            metadata = {"regions": [], "lastUpdated": datetime.now().isoformat()}
        
        # Find or create region entry
        region_entry = next((r for r in metadata["regions"] if r["id"] == region), None)
        if not region_entry:
            from config.regions import REGIONS
            region_entry = {
                "id": region,
                "name": REGIONS[region]["name"],
                "bounds": REGIONS[region]["bounds"],
                "datasets": []
            }
            metadata["regions"].append(region_entry)
        
        # Find or create dataset entry
        dataset_entry = next((d for d in region_entry["datasets"] if d["id"] == dataset), None)
        if not dataset_entry:
            dataset_entry = {
                "id": dataset,
                "category": SOURCES[dataset]["type"],
                "name": SOURCES[dataset]["name"],
                "supportedLayers": SOURCES[dataset]["supportedLayers"],
                "dates": []
            }
            region_entry["datasets"].append(dataset_entry)
        
        # Build layers paths
        layers = {}
        if asset_paths.image.exists():
            layers["image"] = str(asset_paths.image.relative_to(self.path_manager.base_dir))
        if asset_paths.contours and asset_paths.contours.exists():
            layers["contours"] = str(asset_paths.contours.relative_to(self.path_manager.base_dir))
            
        # Remove existing entry for this date if it exists
        date_str = date.strftime('%Y%m%d')
        dataset_entry["dates"] = [d for d in dataset_entry["dates"] if d["date"] != date_str]
        
        # Add new date entry
        dataset_entry["dates"].append({
            "date": date_str,
            "layers": layers
        })
        
        # Update lastUpdated timestamp
        metadata["lastUpdated"] = datetime.now().isoformat()
        
        # Save updated metadata
        with open(global_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)