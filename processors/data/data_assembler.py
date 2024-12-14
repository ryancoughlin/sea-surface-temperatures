import json
from datetime import datetime
from pathlib import Path
import logging
import xarray as xr
import numpy as np
from typing import Dict, Any

from config.settings import SOURCES, SERVER_URL
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f, extract_variables

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
        """Get dataset configuration with validation.
        
        Args:
            dataset: Dataset identifier from settings
            
        Returns:
            Dict containing dataset configuration
            
        Raises:
            ValueError: If dataset not found in settings
        """
        if dataset not in SOURCES:
            raise ValueError(f"Dataset {dataset} not found in SOURCES")
        return SOURCES[dataset]

    def assemble_metadata(self, data: xr.DataArray | xr.Dataset, dataset: str, region: str, date: datetime):
        """Update metadata JSON with new dataset information."""
        try:
            # Calculate ranges for the dataset
            ranges = self._calculate_ranges(data, dataset)
            logger.info(f"Calculated ranges for {dataset}: {ranges}")

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
            logger.exception(e)
            raise

    def _calculate_ranges(self, data: xr.DataArray | xr.Dataset, dataset: str) -> Dict[str, Dict[str, Any]]:
        """Calculate ranges based on dataset type and unit precision settings."""
        try:
            dataset_config = self.get_dataset_config(dataset)  # Use the static method
            unit_precision = dataset_config.get('unit_precision', {})
            dataset_type = dataset_config['type']
            ranges = {}

            # Handle different dataset types
            if dataset_type in ['sst', 'potential_temperature']:
                ranges = self._get_temperature_ranges(data, dataset_config, unit_precision)
            elif dataset_type == 'currents':
                ranges = self._get_current_ranges(data, dataset_config, unit_precision)
            elif dataset_type == 'waves':
                ranges = self._get_wave_ranges(data, unit_precision)
            elif dataset_type == 'chlorophyll':
                ranges = self._get_chlorophyll_ranges(data, unit_precision)

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
                    "name": REGIONS[region]["name"],
                    "bounds": REGIONS[region]["bounds"],
                    "datasets": []
                }
                metadata["regions"].append(region_entry)

            # Find or create dataset entry
            dataset_entry = next((d for d in region_entry["datasets"] if d["id"] == dataset), None)
            if not dataset_entry:
                dataset_config = SOURCES[dataset]
                dataset_entry = {
                    "id": dataset,
                    "category": dataset_config["type"],
                    "name": dataset_config["name"],
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

    def _get_temperature_ranges(self, data: xr.DataArray | xr.Dataset, config: Dict, unit_precision: Dict) -> Dict:
        """Get temperature ranges with proper unit conversion."""
        if isinstance(data, xr.Dataset):
            temp_var = next(var for var in config['variables'] 
                          if 'sst' in var.lower() or 'temperature' in var.lower())
            data = data[temp_var]

        source_unit = config.get('source_unit', 'C')
        if source_unit == 'K':
            data = (data - 273.15) * 9/5 + 32
        elif source_unit == 'C':
            data = data * 9/5 + 32

        valid_data = data.values[~np.isnan(data.values)]
        if len(valid_data) == 0:
            return {}

        precision = unit_precision['temperature']['precision']
        return {
            "temperature": {
                "min": round(float(np.min(valid_data)), precision),
                "max": round(float(np.max(valid_data)), precision),
                "unit": unit_precision['temperature']['unit']
            }
        }

    def _get_current_ranges(self, data: xr.Dataset, config: Dict, unit_precision: Dict) -> Dict:
        """Get current speed and direction ranges."""
        u_var, v_var = config['variables']
        speed = np.sqrt(data[u_var]**2 + data[v_var]**2)
        direction = np.degrees(np.arctan2(data[v_var], data[u_var])) % 360

        ranges = {}
        valid_speeds = speed.values[~np.isnan(speed.values)]
        valid_directions = direction.values[~np.isnan(direction.values)]

        if len(valid_speeds) > 0:
            speed_precision = unit_precision['speed']['precision']
            ranges['speed'] = {
                'min': round(float(np.min(valid_speeds)), speed_precision),
                'max': round(float(np.max(valid_speeds)), speed_precision),
                'unit': unit_precision['speed']['unit']
            }

        if len(valid_directions) > 0:
            dir_precision = unit_precision['direction']['precision']
            ranges['direction'] = {
                'min': round(float(np.min(valid_directions)), dir_precision),
                'max': round(float(np.max(valid_directions)), dir_precision),
                'unit': unit_precision['direction']['unit']
            }

        return ranges

    def _get_wave_ranges(self, data: xr.Dataset, unit_precision: Dict) -> Dict:
        """Get wave measurement ranges."""
        ranges = {}
        for var_name, measure in [
            ('VHM0', 'height'),
            ('VMDR', 'direction'),
            ('VTM10', 'mean_period'),
            ('VTPK', 'peak_period')
        ]:
            if var_name in data and measure in unit_precision:
                valid_data = data[var_name].values[~np.isnan(data[var_name].values)]
                if len(valid_data) > 0:
                    precision = unit_precision[measure]['precision']
                    ranges[measure] = {
                        'min': round(float(np.min(valid_data)), precision),
                        'max': round(float(np.max(valid_data)), precision),
                        'unit': unit_precision[measure]['unit']
                    }
        return ranges

    def _get_chlorophyll_ranges(self, data: xr.DataArray | xr.Dataset, unit_precision: Dict) -> Dict:
        """Get chlorophyll concentration ranges."""
        if isinstance(data, xr.Dataset):
            data = data[data.variables[0]]

        valid_data = data.values[~np.isnan(data.values)]
        if len(valid_data) == 0:
            return {}

        precision = unit_precision['concentration']['precision']
        return {
            "concentration": {
                "min": round(float(np.min(valid_data)), precision),
                "max": round(float(np.max(valid_data)), precision),
                "unit": unit_precision['concentration']['unit']
            }
        }