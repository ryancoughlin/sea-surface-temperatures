from pathlib import Path
from datetime import datetime
import json
import logging
from config.settings import SOURCES, REGIONS_DIR
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self):
        self.base_dir = REGIONS_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_index_files()

    def _ensure_index_files(self):
        """Create or update the main index files."""
        # Main regions index
        regions_index = {
            "regions": [
                {
                    "id": region_id,
                    "name": region_data.get('name', region_id),
                    "bounds": region_data['bounds']
                }
                for region_id, region_data in REGIONS.items()
            ]
        }
        
        with open(self.base_dir.parent / "index.json", 'w') as f:
            json.dump(regions_index, f, indent=2)

    def assemble_metadata(self, region: str, dataset: str, timestamp: str,
                         image_path: Path, geojson_path: Path, mapbox_url: str = None) -> Path:
        """Assemble and save metadata for a processed dataset."""
        try:
            # Prepare paths
            region_dir = self.base_dir / region
            dataset_dir = region_dir / "datasets" / dataset
            date_dir = dataset_dir / timestamp

            # Ensure directories exist
            date_dir.mkdir(parents=True, exist_ok=True)

            # Create data.json for this specific date/dataset
            metadata = {
                "dataset": {
                    "id": dataset,
                    "name": SOURCES[dataset].get('name', dataset),
                    "category": SOURCES[dataset].get('category', 'unknown'),
                },
                "timestamp": timestamp,
                "processing_time": datetime.utcnow().isoformat(),
                "paths": {
                    "image": str(image_path.relative_to(self.base_dir)),
                    "geojson": str(geojson_path.relative_to(self.base_dir)),
                    "tiles": f"{region}/datasets/{dataset}/{timestamp}/tiles",
                    "mapbox_url": mapbox_url  # Add Mapbox URL
                }
            }

            # Save date-specific metadata
            data_json_path = date_dir / "data.json"
            with open(data_json_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            # Update dataset index
            self._update_dataset_index(dataset_dir, timestamp)
            # Update region index
            self._update_region_index(region_dir, dataset)

            return data_json_path

        except Exception as e:
            logger.error(f"Error assembling metadata: {str(e)}")
            raise

    def _update_dataset_index(self, dataset_dir: Path, timestamp: str):
        """Update the index of available dates for a dataset."""
        index_path = dataset_dir / "index.json"
        # Filter out .DS_Store and only include directories with data.json
        dates = [
            d.name for d in dataset_dir.glob("*/data.json") 
            if d.parent.name != '.DS_Store' and d.parent.is_dir()
        ]
        
        index_data = {
            "dates": sorted(dates, reverse=True),
            "last_updated": datetime.utcnow().isoformat()
        }
        
        with open(index_path, 'w') as f:
            json.dump(index_data, f, indent=2)

    def _update_region_index(self, region_dir: Path, dataset: str):
        """Update the index of available datasets for a region."""
        index_path = region_dir / "index.json"
        # Filter out .DS_Store and only include actual dataset directories
        datasets = [
            d.name for d in (region_dir / "datasets").glob("*")
            if d.name != '.DS_Store' and d.is_dir()
        ]
        
        index_data = {
            "datasets": [
                {
                    "id": ds,
                    "name": SOURCES[ds].get('name', ds),
                    "category": SOURCES[ds].get('category', 'unknown')
                }
                for ds in datasets
            ],
            "last_updated": datetime.utcnow().isoformat()
        }
        
        with open(index_path, 'w') as f:
            json.dump(index_data, f, indent=2)
