import json
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict

from config.settings import SOURCES, SERVER_URL, LAYER_TYPES, FILE_EXTENSIONS, PATHS

logger = logging.getLogger(__name__)

class DataAssembler:
    """Manages asset paths and metadata for the front-end API endpoint."""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.data_dir = PATHS['DATA_DIR']
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> Dict[str, Path]:
        """Get paths for all assets for a given date, dataset, and region."""
        base_dir = self.data_dir / region / dataset / date.strftime('%Y%m%d')
        base_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            LAYER_TYPES['DATA']: base_dir / f"{LAYER_TYPES['DATA']}.{FILE_EXTENSIONS[LAYER_TYPES['DATA']]}",
            LAYER_TYPES['IMAGE']: base_dir / f"{LAYER_TYPES['IMAGE']}.{FILE_EXTENSIONS[LAYER_TYPES['IMAGE']]}",
            LAYER_TYPES['CONTOURS']: base_dir / f"{LAYER_TYPES['CONTOURS']}.{FILE_EXTENSIONS[LAYER_TYPES['CONTOURS']]}",
            LAYER_TYPES['FEATURES']: base_dir / f"{LAYER_TYPES['FEATURES']}.{FILE_EXTENSIONS[LAYER_TYPES['FEATURES']]}"
        }

    def update_metadata(self, dataset: str, region: str, date: datetime, paths: Dict[str, str], ranges: Dict = None):
        """Update metadata JSON with new dataset information."""
        try:
            # Create date entry with paths and ranges
            date_entry = {
                "date": date.strftime('%Y%m%d'),
                "layers": self._get_layer_urls(paths)
            }
            
            # Add ranges if provided
            if ranges:
                date_entry["ranges"] = ranges
            
            # Load or create metadata file
            metadata_path = PATHS['API_DIR'] / "metadata.json"
            if metadata_path.exists():
                with open(metadata_path) as f:
                    metadata = json.load(f)
            else:
                metadata = {"regions": [], "lastUpdated": datetime.now().isoformat()}

            # Find or create region entry
            region_entry = next((r for r in metadata["regions"] if r["id"] == region), None)
            if not region_entry:
                region_entry = {
                    "id": region,
                    "datasets": []
                }
                metadata["regions"].append(region_entry)

            # Find or create dataset entry
            dataset_entry = next((d for d in region_entry["datasets"] if d["id"] == dataset), None)
            if not dataset_entry:
                dataset_config = SOURCES[dataset]
                dataset_entry = {
                    "id": dataset,
                    "name": dataset_config["name"],
                    "type": dataset_config["type"],
                    "supportedLayers": dataset_config["supportedLayers"],
                    "metadata": dataset_config["metadata"],
                    "dates": []
                }
                
                # Add source datasets info for combined views
                if dataset_config.get('source_type') == 'combined_view':
                    dataset_entry["source_datasets"] = {
                        name: {
                            "id": info["dataset_id"],
                            "variables": list(info["variables"].keys())
                        }
                        for name, info in dataset_config["source_datasets"].items()
                    }
                
                region_entry["datasets"].append(dataset_entry)

            # Update dates
            dataset_entry["dates"] = [d for d in dataset_entry["dates"] if d["date"] != date_entry["date"]]
            dataset_entry["dates"].append(date_entry)
            dataset_entry["dates"].sort(key=lambda x: x["date"], reverse=True)

            # Save metadata
            metadata["lastUpdated"] = datetime.now().isoformat()
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            raise

    def _get_layer_urls(self, paths: Dict[str, str]) -> Dict[str, str]:
        """Convert local paths to front-end URLs."""
        urls = {}
        for layer_type, path in paths.items():
            if not path:  
                continue
            if isinstance(path, (str, Path)):
                relative_path = str(path).replace(str(self.data_dir) + '/', '')
                urls[layer_type] = f"{SERVER_URL}/{PATHS['DATA_DIR'].name}/{relative_path}"
            else:
                logger.error(f"Invalid path type for {layer_type}: expected string or Path, got {type(path)}")
        return urls