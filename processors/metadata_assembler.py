import json
from datetime import datetime
from pathlib import Path
import logging
import xarray as xr
import numpy as np
from typing import Dict, Any

from config.settings import SOURCES, SERVER_URL
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self, path_manager):
        self.path_manager = path_manager

    def get_full_url(self, relative_path: str) -> str:
        """Convert relative path to full URL."""
        return f"{SERVER_URL}/{str(relative_path).replace('output/', '')}"

    def get_dataset_ranges(self, data_path: Path, dataset: str) -> Dict[str, Dict[str, Any]]:
        """Extract min/max values with units from dataset."""
        try:
            with xr.open_dataset(data_path) as ds:
                ranges = {}
                for var in SOURCES[dataset]['variables']:
                    data = ds[var]
                    
                    # Select first index for time/depth dimensions
                    for dim in ['time', 'depth', 'altitude']:
                        if dim in data.dims:
                            data = data.isel({dim: 0})

                    # Handle temperature conversion
                    if SOURCES[dataset]['type'] == 'sst':
                        data = data * 1.8 + 32
                        unit = 'fahrenheit'
                    else:
                        unit = getattr(data, 'units', 'unknown')

                    ranges[var] = {
                        'min': round(float(np.nanmin(data).item()), 2),
                        'max': round(float(np.nanmax(data).item()), 2),
                        'unit': unit
                    }
                
                return ranges

        except Exception as e:
            logger.error(f"Error getting dataset ranges: {str(e)}")
            logger.exception(e)
            return {}

    def assemble_metadata(self, date: datetime, dataset: str, region: str, asset_paths) -> Dict[str, Any]:
        """Update global metadata.json with new dataset information."""
        data_path = self.path_manager.get_data_path(date, dataset, region)
        ranges = self.get_dataset_ranges(data_path, dataset)
        
        # Collect available layers
        layers = {}
        for layer_type in ['image', 'data', 'contours']:
            path = getattr(asset_paths, layer_type, None)
            if path and path.exists():
                layers[layer_type] = self.get_full_url(
                    path.relative_to(self.path_manager.base_dir)
                )
        
        # Update global metadata directly
        self.update_global_metadata(region, dataset, date, asset_paths, ranges)
        
        return self.path_manager.output_dir / "metadata.json"

    def update_global_metadata(self, region: str, dataset: str, date: datetime, 
                             asset_paths, ranges: Dict[str, Dict[str, Any]]) -> None:
        """Update global metadata file with new dataset information."""
        global_metadata_path = self.path_manager.output_dir / "metadata.json"
        
        try:
            # Get layers
            layers = {}
            for layer_type in ['image', 'data', 'contours']:
                path = getattr(asset_paths, layer_type, None)
                if path and path.exists():
                    layers[layer_type] = self.get_full_url(
                        path.relative_to(self.path_manager.base_dir)
                    )

            if global_metadata_path.exists():
                with open(global_metadata_path) as f:
                    metadata = json.load(f)
            else:
                metadata = {"regions": [], "lastUpdated": datetime.now().isoformat()}

            region_entry = next((r for r in metadata["regions"] if r["id"] == region), None)
            if not region_entry:
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
                    "metadata": SOURCES[dataset]["metadata"],
                    "dates": []
                }
                region_entry["datasets"].append(dataset_entry)

            # Update dates
            date_str = date.strftime('%Y%m%d')
            dataset_entry["dates"] = [d for d in dataset_entry["dates"] if d["date"] != date_str]
            dataset_entry["dates"].append({
                "date": date_str,
                "layers": layers,
                "ranges": ranges
            })
            dataset_entry["dates"].sort(key=lambda x: x["date"], reverse=True)
            
            metadata["lastUpdated"] = datetime.now().isoformat()
            
            # Save updated metadata
            with open(global_metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            logger.error(f"Error updating global metadata: {str(e)}")
            logger.exception(e)
            raise