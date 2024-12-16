import json
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict

from config.settings import SOURCES, SERVER_URL

logger = logging.getLogger(__name__)

class DataAssembler:
    """Manages asset paths and metadata for the front-end API endpoint."""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.output_dir = base_dir / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> Dict[str, Path]:
        """Get paths for all assets for a given date, dataset, and region."""
        base_dir = self.output_dir / region / date.strftime('%Y%m%d') / dataset
        base_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            'data': base_dir / 'data.json',
            'image': base_dir / 'image.png',
            'contours': base_dir / 'contours.json',
            'features': base_dir / 'features.json'
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
            metadata_path = self.output_dir / "metadata.json"
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
            if path:  # Only include paths that exist
                urls[layer_type] = f"{SERVER_URL}/{path.replace('output/', '')}"
        return urls