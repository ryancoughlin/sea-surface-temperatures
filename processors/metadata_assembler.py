import json
from datetime import datetime
from config.settings import SOURCES, SERVER_URL
import xarray as xr
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self, path_manager):
        self.path_manager = path_manager

    def get_full_url(self, relative_path: str) -> str:
        return f"{SERVER_URL}/{str(relative_path).replace('output/', '')}"

    def get_dataset_ranges(self, data_path: Path, dataset: str) -> dict:
        """Extract min/max values with units from dataset."""
        try:
            ds = xr.open_dataset(data_path)
            ranges = {}
            
            for var in SOURCES[dataset]['variables']:
                data = ds[var]
                
                # Force 2D by selecting first index of time and depth if they exist
                if 'time' in data.dims:
                    data = data.isel(time=0)
                if 'depth' in data.dims:
                    data = data.isel(depth=0)
                if 'altitude' in data.dims:
                    data = data.isel(altitude=0)
                
                # Get coordinates and mask to region
                lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
                lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
                
                # Calculate min/max based on data type
                if SOURCES[dataset]['type'] == 'sst':
                    # Convert to Fahrenheit
                    min_val = float(data.min() * 1.8 + 32)
                    max_val = float(data.max() * 1.8 + 32)
                    unit = 'fahrenheit'
                elif SOURCES[dataset]['type'] == 'currents':
                    min_val = float(data.min())
                    max_val = float(data.max())
                    unit = 'm/s'
                elif SOURCES[dataset]['type'] == 'chlorophyll':
                    min_val = float(data.min())
                    max_val = float(data.max())
                    unit = 'mg/m³'
                else:
                    min_val = float(data.min())
                    max_val = float(data.max())
                    unit = 'unknown'

                ranges[var] = {
                    'min': float(np.nanmin(data).item()),  # Convert to Python float
                    'max': float(np.nanmax(data).item()),  # Convert to Python float
                    'unit': unit
                }
                
            ds.close()
            logger.info(f"Extracted ranges for {dataset}: {ranges}")
            return ranges
            
        except Exception as e:
            logger.error(f"Error getting dataset ranges: {str(e)}")
            return {}

    def assemble_metadata(self, date: datetime, dataset: str, region: str, asset_paths) -> dict:
        """Enhanced metadata assembly with min/max values."""
        now = datetime.now()
        
        # Get data ranges
        data_path = self.path_manager.get_data_path(date, dataset, region)
        ranges = self.get_dataset_ranges(data_path, dataset)
        
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
                    "layers": layers,
                    "ranges": ranges
                }
            ]
        }
        
        with open(asset_paths.metadata, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        self.update_global_metadata(region, dataset, date, asset_paths, ranges)
        
        return metadata

    def update_global_metadata(self, region: str, dataset: str, date: datetime, asset_paths, ranges) -> None:
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
        # Remove existing entry for this date if it exists
        dataset_entry["dates"] = [d for d in dataset_entry["dates"] if d["date"] != date_str]
        
        # Add new entry
        dataset_entry["dates"].append({
            "date": date_str,
            "layers": layers,
            "ranges": ranges
        })
        
        # Sort dates in descending order (newest first)
        dataset_entry["dates"].sort(key=lambda x: x["date"], reverse=True)
        
        metadata["lastUpdated"] = datetime.now().isoformat()
        
        with open(global_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)