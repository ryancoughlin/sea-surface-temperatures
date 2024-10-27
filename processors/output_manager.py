from pathlib import Path
from datetime import datetime
import json
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)

@dataclass
class DatasetMetadata:
    id: str
    name: str
    category: str
    timestamp: str
    image_path: Path
    geojson_path: Path
    mapbox_url: Optional[str] = None

class OutputManager:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
    def get_dataset_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Get standardized path for dataset files."""
        return self.base_dir / region / "datasets" / dataset / timestamp

    @lru_cache
    def get_region_config(self, region: str) -> Dict:
        """Get cached region configuration."""
        from config.regions import REGIONS
        return REGIONS[region]

    @lru_cache
    def get_source_config(self, dataset: str) -> Dict:
        """Get cached source configuration."""
        from config.settings import SOURCES
        return SOURCES.get(dataset, {})

    def save_dataset(self, metadata: DatasetMetadata, region: str) -> Path:
        """Save dataset metadata and update indices."""
        dataset_dir = self.get_dataset_path(region, metadata.id, metadata.timestamp)
        dataset_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Saving dataset with paths - Image: {metadata.image_path}, GeoJSON: {metadata.geojson_path}")
        logger.info(f"Base directory: {self.base_dir}")

        try:
            # Verify paths exist before processing
            if not metadata.image_path.exists():
                raise FileNotFoundError(f"Image file not found: {metadata.image_path}")
            if not metadata.geojson_path.exists():
                raise FileNotFoundError(f"GeoJSON file not found: {metadata.geojson_path}")

            # Save dataset-specific metadata
            data = {
                "dataset_info": {
                    "id": metadata.id,
                    "name": metadata.name,
                    "category": metadata.category,
                },
                "timestamp": metadata.timestamp,
                "processing_time": datetime.utcnow().isoformat(),
                "paths": {
                    "image": str(metadata.image_path.relative_to(self.base_dir)),
                    "geojson": str(metadata.geojson_path.relative_to(self.base_dir)),
                    "tiles": f"{region}/datasets/{metadata.id}/{metadata.timestamp}/tiles",
                    "mapbox_url": metadata.mapbox_url
                }
            }

            data_path = dataset_dir / "data.json"
            with open(data_path, 'w') as f:
                json.dump(data, f, indent=2)

            # Update indices
            self._update_dataset_index(region, metadata.id)
            self._update_region_index(region)

            return data_path

        except Exception as e:
            logger.error(f"Error saving dataset {metadata.id} for region {region}: {str(e)}")
            logger.error(f"Full metadata: {vars(metadata)}")
            raise

    def _update_dataset_index(self, region: str, dataset: str) -> None:
        """Update dataset-specific index."""
        dataset_dir = self.base_dir / region / "datasets" / dataset
        available_dates = []

        # Add debug logging
        logger.debug(f"Updating dataset index for {dataset} in {region}")

        for data_file in dataset_dir.glob("*/data.json"):
            if data_file.parent.name == '.DS_Store':
                continue

            try:
                with open(data_file, 'r') as f:
                    data = json.load(f)
                    date_entry = {
                        "date": data_file.parent.name,
                        "processing_time": data.get("processing_time", datetime.utcnow().isoformat()),
                        "paths": data.get("paths", {})
                    }
                    available_dates.append(date_entry)
            except Exception as e:
                logger.error(f"Error reading {data_file}: {str(e)}")
                continue

        # Create dataset index with safe defaults
        source_info = self.get_source_config(dataset)
        index = {
            "dataset_info": {  # Changed from "dataset" to "dataset_info" to be more explicit
                "id": dataset,
                "name": source_info.get('name', dataset),
                "category": source_info.get('category', 'unknown'),
            },
            "dates": sorted(available_dates, key=lambda x: x["date"], reverse=True),
            "last_updated": datetime.utcnow().isoformat()
        }

        index_path = dataset_dir / "index.json"
        with open(index_path, 'w') as f:
            json.dump(index, f, indent=2)

    def _update_region_index(self, region: str) -> None:
        """Update region-specific index."""
        region_dir = self.base_dir / region
        datasets = []

        for dataset_dir in (region_dir / "datasets").glob("*"):
            if not dataset_dir.is_dir() or dataset_dir.name == '.DS_Store':
                continue

            index_path = dataset_dir / "index.json"
            if index_path.exists():
                try:
                    with open(index_path, 'r') as f:
                        dataset_index = json.load(f)
                        latest_date = (dataset_index.get("dates", []) or [{}])[0].get("date")
                        
                        # Use dataset_info instead of dataset
                        dataset_info = dataset_index.get("dataset_info", {})
                        datasets.append({
                            "id": dataset_info.get("id", dataset_dir.name),
                            "name": dataset_info.get("name", dataset_dir.name),
                            "category": dataset_info.get("category", "unknown"),
                            "latest_date": latest_date,
                            "url": f"datasets/{dataset_dir.name}/index.json"
                        })
                except Exception as e:
                    logger.error(f"Error reading {index_path}: {str(e)}")
                    continue

        region_config = self.get_region_config(region)
        index = {
            "id": region,
            "name": region_config.get('name', region),
            "bounds": region_config.get('bounds', []),
            "datasets": sorted(datasets, key=lambda x: x["id"]),
            "last_updated": datetime.utcnow().isoformat()
        }

        with open(region_dir / "index.json", 'w') as f:
            json.dump(index, f, indent=2)
