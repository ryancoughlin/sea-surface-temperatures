import json
from datetime import datetime
from pathlib import Path
import logging
import xarray as xr
import numpy as np
from typing import Dict, Any

from config.settings import SOURCES, SERVER_URL, UNIT_TRANSFORMS
from utils.data_utils import extract_variables

logger = logging.getLogger(__name__)

class DataAssembler:
    """Assembles metadata JSON for the front-end API endpoint."""
    
    def __init__(self, path_manager):
        self.path_manager = path_manager

    def get_full_url(self, relative_path: str) -> str:
        """Convert relative path to full URL for front-end access."""
        return f"{SERVER_URL}/{str(relative_path).replace('output/', '')}"

    @staticmethod
    def get_dataset_config(dataset: str) -> Dict:
        """Get dataset configuration with validation."""
        if dataset not in SOURCES:
            raise ValueError(f"Dataset {dataset} not found in SOURCES")
        return SOURCES[dataset]

    def assemble_metadata(self, data: xr.DataArray | xr.Dataset, dataset: str, region: str, date: datetime):
        """Update metadata JSON with new dataset information."""
        try:
            # Calculate ranges for the dataset
            ranges = self._calculate_ranges(data, dataset)
            logger.info(f"Calculated ranges for {dataset}")

            # Get asset paths and create layer URLs
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            layers = self._get_layer_urls(asset_paths)
            
            # Update the metadata JSON
            self._update_metadata_file(
                region=region,
                dataset=dataset,
                date=date,
                layers=layers,
                ranges=ranges
            )
            
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            raise

    def _calculate_ranges(self, data: xr.DataArray | xr.Dataset, dataset: str) -> Dict[str, Dict[str, Any]]:
        """Calculate ranges for all variables in a dataset with unit conversions."""
        try:
            dataset_config = self.get_dataset_config(dataset)
            ranges = {}

            for var_name, var_config in dataset_config["variables"].items():
                # Get the data array
                if isinstance(data, xr.Dataset):
                    if var_name not in data:
                        continue
                    values = data[var_name]
                else:
                    values = data

                # Handle unit conversions if needed
                if "source_unit" in var_config and "target_unit" in var_config:
                    transform_key = f"{var_config['source_unit']}_to_{var_config['target_unit']}"
                    if transform_key in UNIT_TRANSFORMS:
                        values = UNIT_TRANSFORMS[transform_key](values)

                # Calculate min/max for valid values
                valid_values = values.values[~np.isnan(values.values)]
                if len(valid_values) == 0:
                    continue

                ranges[var_name] = {
                    "min": float(np.min(valid_values)),
                    "max": float(np.max(valid_values)),
                    "unit": var_config.get("target_unit") or var_config.get("unit")
                }

            return ranges

        except Exception as e:
            logger.error(f"Error calculating ranges: {str(e)}")
            return {}

    def _get_layer_urls(self, asset_paths) -> Dict[str, str]:
        """Create layer URLs for front-end access."""
        layers = {}
        for layer_type in ['image', 'data', 'contours']:
            path = getattr(asset_paths, layer_type, None)
            if path and path.exists():
                layers[layer_type] = self.get_full_url(
                    path.relative_to(self.path_manager.base_dir)
                )
        return layers

    def _update_metadata_file(self, region: str, dataset: str, date: datetime, 
                            layers: Dict[str, str], ranges: Dict[str, Dict[str, Any]]):
        """Update the metadata JSON file with new dataset information."""
        try:
            # Create date entry
            date_entry = {
                "date": date.strftime('%Y%m%d'),
                "layers": layers,
                "ranges": ranges
            }

            # Load or create metadata file
            metadata_path = self.path_manager.get_metadata_path()
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

            metadata["lastUpdated"] = datetime.now().isoformat()

            # Save metadata
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            logger.error(f"Error updating metadata file: {str(e)}")
            raise