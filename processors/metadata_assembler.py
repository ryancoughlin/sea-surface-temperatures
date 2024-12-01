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

    def assemble_metadata(self, data: xr.DataArray | xr.Dataset, dataset: str, region: str, date: datetime):
        """Update metadata for a dataset."""
        try:
            # Get dataset type
            dataset_type = SOURCES[dataset]['type']
            logger.info(f"Assembling metadata for {dataset} ({dataset_type}) in {region}")
            logger.info(f"Input data type: {type(data)}")
            
            if isinstance(data, xr.Dataset):
                logger.info(f"Dataset variables: {list(data.variables)}")
            elif isinstance(data, xr.DataArray):
                logger.info(f"DataArray dims: {data.dims}")
                logger.info(f"DataArray shape: {data.shape}")
            
            # Calculate ranges based on dataset type
            logger.info(f"Calculating ranges for {dataset_type}")
            if dataset_type == 'currents':
                ranges = self._get_current_ranges_from_data(data, dataset)
            elif dataset_type == 'sst':
                ranges = self._get_sst_ranges_from_data(data, dataset)
            elif dataset_type == 'waves':
                ranges = self._get_waves_ranges_from_data(data, dataset)
            elif dataset_type == 'chlorophyll':
                ranges = self._get_chlorophyll_ranges_from_data(data, dataset)
            else:
                logger.warning(f"Unknown dataset type: {dataset_type}")
                ranges = {}

            logger.info(f"Calculated ranges: {ranges}")

            # Get asset paths
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            
            # Update global metadata
            self.update_global_metadata(
                region=region,
                dataset=dataset,
                date=date,
                asset_paths=asset_paths,
                ranges=ranges
            )
            
            logger.info(f"Updated metadata for {dataset} in {region}")

        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            logger.exception(e)  # Log full traceback
            raise

    def update_global_metadata(self, region: str, dataset: str, date: datetime, 
                             asset_paths, ranges: Dict[str, Dict[str, Any]]) -> None:
        """Update global metadata file with new dataset information."""
        try:
            # Get layers
            layers = {}
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
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"Updated metadata for {dataset} in {region}")

        except Exception as e:
            logger.error(f"Error updating global metadata: {str(e)}")
            logger.exception(e)
            raise

    def _get_current_ranges_from_data(self, data: xr.DataArray, dataset: str) -> Dict:
        """Get standardized ranges for current data from DataArray."""
        try:
            # Calculate speed and direction from u/v components
            speed = np.sqrt(data[0]**2 + data[1]**2)
            direction = np.degrees(np.arctan2(data[1], data[0])) % 360
            
            # Get valid values only
            valid_speeds = speed.values[~np.isnan(speed.values)]
            valid_directions = direction.values[~np.isnan(direction.values)]
            
            if len(valid_speeds) == 0 or len(valid_directions) == 0:
                logger.warning("No valid current data found")
                return {}
            
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
            return {}

    def _get_sst_ranges_from_data(self, data: xr.DataArray, dataset: str) -> Dict:
        """Get standardized ranges for SST data from DataArray."""
        try:
            # Handle Dataset vs DataArray
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                sst_var = next(var for var in variables if 'sst' in var.lower() or 'temperature' in var.lower())
                data = data[sst_var]
            
            logger.info(f"Calculating SST ranges for {dataset}")
            logger.info(f"Data type: {type(data)}")
            logger.info(f"Data shape: {data.shape}")
            logger.info(f"Data dims: {data.dims}")
            
            # Convert to Fahrenheit using source unit from settings
            source_unit = SOURCES[dataset].get('source_unit', 'C')
            logger.info(f"Source unit: {source_unit}")
            
            if source_unit == 'K':
                data = (data - 273.15) * 9/5 + 32  # Kelvin to Fahrenheit
            else:
                data = data * 9/5 + 32  # Celsius to Fahrenheit
            
            valid_data = data.values[~np.isnan(data.values)]
            logger.info(f"Found {len(valid_data)} valid data points")
            
            if len(valid_data) == 0:
                logger.warning("No valid SST data found")
                return {}
            
            min_temp = float(np.min(valid_data))
            max_temp = float(np.max(valid_data))
            logger.info(f"Temperature range (F): {min_temp:.2f} to {max_temp:.2f}")
            
            return {
                "temperature": {
                    "min": round(min_temp, 2),
                    "max": round(max_temp, 2),
                    "unit": "fahrenheit"
                }
            }
        except Exception as e:
            logger.error(f"Error calculating SST ranges: {str(e)}")
            logger.exception(e)  # Log full traceback
            return {}

    def _get_waves_ranges_from_data(self, data: xr.DataArray, dataset: str) -> Dict:
        """Get standardized ranges for waves data from DataArray."""
        try:
            height = data['VHM0']
            direction = data['VMDR']
            mean_period = data['VTM10']
            peak_period = data['VTPK']
            
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

    def _get_chlorophyll_ranges_from_data(self, data: xr.DataArray, dataset: str) -> Dict:
        """Get standardized ranges for chlorophyll data from DataArray."""
        try:
            valid_data = data.values[~np.isnan(data.values)]
            if len(valid_data) == 0:
                logger.warning("No valid chlorophyll data found")
                return {}
            
            return {
                "concentration": {
                    "min": round(float(np.min(valid_data)), 4),
                    "max": round(float(np.max(valid_data)), 4),
                    "unit": "mg/m³"
                }
            }
        except Exception as e:
            logger.error(f"Error calculating chlorophyll ranges: {str(e)}")
            return {}