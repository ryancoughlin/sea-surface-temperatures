import json
from datetime import datetime
from config.settings import SOURCES, SERVER_URL

class MetadataAssembler:
    def __init__(self, path_manager):
        self.path_manager = path_manager

    def get_full_url(self, relative_path: str) -> str:
        return f"{SERVER_URL}/{str(relative_path).replace('output/', '')}"

    def assemble_metadata(self, date: datetime, dataset: str, region: str, asset_paths) -> dict:
        now = datetime.now()
        
        layers = {}
        if asset_paths.image.exists():
            layers["image"] = self.get_full_url(asset_paths.image.relative_to(self.path_manager.base_dir))
        if asset_paths.data.exists():
            layers["data"] = self.get_full_url(asset_paths.data.relative_to(self.path_manager.base_dir))
        if asset_paths.contours and asset_paths.contours.exists():
            layers["contours"] = self.get_full_url(asset_paths.contours.relative_to(self.path_manager.base_dir))
            
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
        
        with open(asset_paths.metadata, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        self.update_global_metadata(region, dataset, date, asset_paths)
        
        return metadata

    def update_global_metadata(self, region: str, dataset: str, date: datetime, asset_paths) -> None:
        global_metadata_path = self.path_manager.output_dir / "metadata.json"
        
        if global_metadata_path.exists():
            with open(global_metadata_path) as f:
                metadata = json.load(f)
        else:
            metadata = {"regions": [], "lastUpdated": datetime.now().isoformat()}
        
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
        
        layers = {}
        if asset_paths.image.exists():
            layers["image"] = self.get_full_url(asset_paths.image.relative_to(self.path_manager.base_dir))
        if asset_paths.data.exists():
            layers["data"] = self.get_full_url(asset_paths.data.relative_to(self.path_manager.base_dir))
        if asset_paths.contours and asset_paths.contours.exists():
            layers["contours"] = self.get_full_url(asset_paths.contours.relative_to(self.path_manager.base_dir))
            
        date_str = date.strftime('%Y%m%d')
        dataset_entry["dates"] = [d for d in dataset_entry["dates"] if d["date"] != date_str]
        
        dataset_entry["dates"].append({
            "date": date_str,
            "layers": layers
        })
        
        metadata["lastUpdated"] = datetime.now().isoformat()
        
        with open(global_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)