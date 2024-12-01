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

    def get_dataset_ranges(self, data_path: Path, dataset: str, processed_data: xr.DataArray = None) -> Dict[str, Dict[str, Any]]:
        """Extract standardized ranges for any dataset type."""
        try:
            if not data_path.exists():
                logger.error(f"Data file not found: {data_path}")
                return {}
            
            with xr.open_dataset(data_path) as ds:
                if SOURCES[dataset]['type'] == 'currents':
                    ranges = self._get_current_ranges(ds, dataset)
                elif SOURCES[dataset]['type'] == 'sst':
                    ranges = self._get_sst_ranges(ds, dataset)
                elif SOURCES[dataset]['type'] == 'waves':
                    ranges = self._get_waves_ranges(ds, dataset)
                elif SOURCES[dataset]['type'] == 'chlorophyll':
                    ranges = self._get_chlorophyll_ranges(ds, dataset)
                else:
                    ranges = self._get_default_ranges(ds, dataset)
            
            if not ranges or not any(ranges.values()):
                logger.warning(f"No valid ranges calculated for {dataset}")
                return {}
            
            return ranges
            
        except Exception as e:
            logger.error(f"Error getting dataset ranges: {str(e)}")
            logger.exception(e)
            return {}

    def _get_current_ranges(self, ds: xr.Dataset, dataset: str) -> Dict:
        """Get standardized ranges for current data."""
        try:
            # Get u and v components
            u_data = ds[SOURCES[dataset]['variables'][0]]
            v_data = ds[SOURCES[dataset]['variables'][1]]
            
            # Handle dimensions
            for dim in ['time', 'depth', 'altitude']:
                if dim in u_data.dims:
                    u_data = u_data.isel({dim: 0})
                    v_data = v_data.isel({dim: 0})
            
            # Calculate speed and direction from u/v components
            speed = np.sqrt(u_data**2 + v_data**2)
            direction = np.degrees(np.arctan2(v_data, u_data)) % 360
            
            # Get valid values only
            valid_speeds = speed.values[~np.isnan(speed.values)]
            valid_directions = direction.values[~np.isnan(direction.values)]
            
            if len(valid_speeds) == 0 or len(valid_directions) == 0:
                logger.warning("No valid current data found")
                return {}
            
            # Return only speed and direction ranges (not u/v)
            return {
                "speed": {
                    "min": round(float(np.min(valid_speeds)), 2),
                    "max": round(float(np.max(valid_speeds)), 2),
                    "unit": "m/s"
                },
                "direction": {
                    "min": round(float(np.min(valid_directions)), 1),
                    "max": round(float(np.max(valid_directions)), 1),
                    "unit": "degrees"
                }
            }
        except Exception as e:
            logger.error(f"Error calculating current ranges: {str(e)}")
            logger.exception(e)
            return {}

    def _get_sst_ranges(self, ds: xr.Dataset, dataset: str) -> Dict:
        """Get standardized ranges for SST data."""
        try:
            data = ds[SOURCES[dataset]['variables'][0]]
            
            # Handle dimensions
            for dim in ['time', 'depth']:
                if dim in data.dims:
                    data = data.isel({dim: 0})
            
            # Convert to Fahrenheit using source unit from settings
            source_unit = SOURCES[dataset].get('source_unit', 'C')  # Default to Celsius if not specified
            if source_unit == 'K':
                data = (data - 273.15) * 9/5 + 32  # Kelvin to Fahrenheit
            else:
                data = data * 9/5 + 32  # Celsius to Fahrenheit
            
            return {
                "temperature": {
                    "min": round(float(data.min()), 2),
                    "max": round(float(data.max()), 2),
                    "unit": "fahrenheit"
                }
            }
        except Exception as e:
            logger.error(f"Error calculating SST ranges: {str(e)}")
            return {}

    def _get_waves_ranges(self, ds: xr.Dataset, dataset: str) -> Dict:
        """Get standardized ranges for waves data."""
        try:
            height = ds['VHM0'].isel(time=0)
            direction = ds['VMDR'].isel(time=0)
            mean_period = ds['VTM10'].isel(time=0)
            peak_period = ds['VTPK'].isel(time=0)
            
            return {
                "height": {
                    "min": round(float(height.min()), 2),
                    "max": round(float(height.max()), 2),
                    "unit": "m"
                },
                "mean_period": {
                    "min": round(float(mean_period.min()), 1),
                    "max": round(float(mean_period.max()), 1),
                    "unit": "seconds"
                },
                "direction": {
                    "min": round(float(direction.min()), 1),
                    "max": round(float(direction.max()), 1),
                    "unit": "degrees"
                }
            }
        except Exception as e:
            logger.error(f"Error calculating wave ranges: {str(e)}")
            return {}

    def _get_chlorophyll_ranges(self, ds: xr.Dataset, dataset: str, processed_data: xr.DataArray = None) -> Dict:
        """Get standardized ranges for chlorophyll data."""
        try:
            if processed_data is not None:
                # Use the already processed data
                valid_data = processed_data.values[~np.isnan(processed_data.values)]
            else:
                # Fallback to raw data processing
                data = ds[SOURCES[dataset]['variables'][0]]
                for dim in ['time', 'altitude']:
                    if dim in data.dims:
                        data = data.isel({dim: 0})
                valid_data = data.values[~np.isnan(data.values)]
            
            if len(valid_data) == 0:
                logger.warning("No valid chlorophyll data found")
                return {}
            
            data_min = float(np.min(valid_data))
            data_max = float(np.max(valid_data))
            
            logger.info(f"[RANGES] Metadata min/max: {data_min:.4f} to {data_max:.4f}")
            
            return {
                "concentration": {
                    "min": round(data_min, 4),
                    "max": round(data_max, 4),
                    "unit": "mg/m³"
                }
            }
        except Exception as e:
            logger.error(f"Error calculating chlorophyll ranges: {str(e)}")
            return {}

    def _get_default_ranges(self, ds: xr.Dataset, dataset: str) -> Dict:
        """Get standardized ranges for other datasets."""
        try:
            ranges = {}
            for var in SOURCES[dataset]['variables']:
                data = ds[var]
                
                # Handle dimensions
                for dim in ['time', 'depth', 'altitude']:
                    if dim in data.dims:
                        data = data.isel({dim: 0})
                
                ranges[var] = {
                    "min": round(float(data.min()), 2),
                    "max": round(float(data.max()), 2),
                    "unit": getattr(data, 'units', 'unknown')
                }
            return ranges
        except Exception as e:
            logger.error(f"Error calculating default ranges: {str(e)}")
            return {}

    def assemble_metadata(self, date: datetime, dataset: str, region: str, 
                         asset_paths, data_path: Path = None) -> Dict[str, Any]:
        """Update global metadata.json with new dataset information."""
        # Use provided data_path if available, otherwise fall back to default
        data_path = data_path or self.path_manager.get_data_path(date, dataset, region)
        
        # Handle hourly PODAAC data differently
        if SOURCES[dataset].get('type') == 'podaac':
            # Get all hourly files for this date
            date_dir = data_path.parent
            hourly_files = sorted(date_dir.glob('*/*.nc'))  # e.g., 1542/*.nc
            
            if not hourly_files:
                logger.warning(f"No hourly files found for {dataset} on {date}")
                return self.path_manager.output_dir / "metadata.json"
            
            # Calculate ranges across all hourly files
            combined_ranges = {}
            for file in hourly_files:
                ranges = self.get_dataset_ranges(file, dataset)
                for key, value in ranges.items():
                    if key not in combined_ranges:
                        combined_ranges[key] = value.copy()
                    else:
                        combined_ranges[key]['min'] = min(combined_ranges[key]['min'], value['min'])
                        combined_ranges[key]['max'] = max(combined_ranges[key]['max'], value['max'])
            
            ranges = combined_ranges
        else:
            ranges = self.get_dataset_ranges(data_path, dataset)
        
        # Update global metadata directly
        self.update_global_metadata(region, dataset, date, asset_paths, ranges)
        
        return self.path_manager.output_dir / "metadata.json"

    def update_global_metadata(self, region: str, dataset: str, date: datetime, 
                             asset_paths, ranges: Dict[str, Dict[str, Any]]) -> None:
        """Update global metadata file with new dataset information."""
        try:
            # Get layers
            layers = {}
            if SOURCES[dataset].get('type') == 'podaac':
                # For PODAAC, include hourly subdirectories in layer paths
                date_str = date.strftime('%Y%m%d')
                base_dir = self.path_manager.base_dir
                
                # Get all hourly directories
                date_dir = Path(asset_paths.data).parent
                hour_dirs = sorted([d for d in date_dir.glob('*') if d.is_dir()])
                
                # Create hourly entries
                layers['hourly'] = {}
                for hour_dir in hour_dirs:
                    hour = hour_dir.name
                    hour_layers = {}
                    
                    for layer_type in ['image', 'data', 'contours']:
                        layer_path = hour_dir / f"{layer_type}.{SOURCES[dataset]['fileExtensions'][layer_type]}"
                        if layer_path.exists():
                            hour_layers[layer_type] = self.get_full_url(
                                layer_path.relative_to(base_dir)
                            )
                    
                    if hour_layers:
                        layers['hourly'][hour] = hour_layers
            else:
                # Handle non-PODAAC datasets as before
                for layer_type in ['image', 'data', 'contours']:
                    path = getattr(asset_paths, layer_type, None)
                    if path and path.exists():
                        layers[layer_type] = self.get_full_url(
                            path.relative_to(self.path_manager.base_dir)
                        )

            # Validate ranges
            if not ranges:
                logger.warning(f"No ranges provided for {dataset} in {region}")
                ranges = {}
            
            # Create date entry
            date_entry = {
                "date": date.strftime('%Y%m%d'),
                "layers": layers,
                "ranges": ranges or {}
            }

            # Update metadata file
            metadata_path = self.path_manager.output_dir / "metadata.json"
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
                    "metadata": SOURCES[dataset]["metadata"],
                    "dates": []
                }
                region_entry["datasets"].append(dataset_entry)

            # Update dates
            dataset_entry["dates"] = [d for d in dataset_entry["dates"] if d["date"] != date_entry["date"]]
            dataset_entry["dates"].append(date_entry)
            dataset_entry["dates"].sort(key=lambda x: x["date"], reverse=True)

            metadata["lastUpdated"] = datetime.now().isoformat()

            # Save metadata
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"Updated metadata for {dataset} in {region}")

        except Exception as e:
            logger.error(f"Error updating global metadata: {str(e)}")
            logger.exception(e)
            raise