from pathlib import Path
from datetime import datetime
import xarray as xr
import numpy as np
import logging
from typing import Dict, List, Union, Optional
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class WavesGeoJSONConverter(BaseGeoJSONConverter):
    """Converts wave data to GeoJSON format with available wave metrics"""
    
    def convert(self, data: Union[xr.Dataset, xr.DataArray], region: str, dataset: str, date: datetime) -> Path:
        """Convert wave data to GeoJSON format."""
        try:
            # Extract wave data from dataset
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                height_var = next(var for var, config in variables.items() if config['type'] == 'wave_height')
                direction_var = next((var for var, config in variables.items() if config['type'] == 'wave_direction'), None)
                
                wave_height = data[height_var]
                wave_direction = data[direction_var] if direction_var else None
            else:
                logger.warning("Received DataArray instead of Dataset - assuming wave height")
                wave_height = data
                wave_direction = None
            
            # Reduce dimensions
            wave_height = self._reduce_dimensions(wave_height)
            if wave_direction is not None:
                wave_direction = self._reduce_dimensions(wave_direction)
            
            # Get coordinates
            longitude, latitude = self.get_coordinate_names(wave_height)
            
            # Check which variables are available
            has_direction = wave_direction is not None
            has_mean_period = 'VTM10' in data.variables if isinstance(data, xr.Dataset) else False
            has_peak_period = 'VTPK' in data.variables if isinstance(data, xr.Dataset) else False
            
            # Load optional variables if available
            mean_period = data['VTM10'] if has_mean_period else None
            peak_period = data['VTPK'] if has_peak_period else None
            
            # Reduce dimensions for optional variables
            if mean_period is not None:
                mean_period = self._reduce_dimensions(mean_period)
            if peak_period is not None:
                peak_period = self._reduce_dimensions(peak_period)
            
            # Prepare data for feature generation
            lats = wave_height[latitude].values
            lons = wave_height[longitude].values
            height_values = wave_height.values
            direction_values = wave_direction.values if wave_direction is not None else None
            mean_period_values = mean_period.values if mean_period is not None else None
            peak_period_values = peak_period.values if peak_period is not None else None
            
            def property_generator(i: int, j: int) -> Optional[Dict]:
                height = float(height_values[i, j])
                if np.isnan(height):
                    return None
                
                # Build properties starting with wave height
                properties = {
                    "height": round(height, 2),
                    "units": {
                        "height": "m"
                    }
                }
                
                # Add other wave metrics if available
                if direction_values is not None:
                    dir_value = float(direction_values[i, j])
                    if not np.isnan(dir_value):
                        properties["direction"] = round(dir_value, 1)
                        properties["units"]["direction"] = "degrees"
                
                if mean_period_values is not None:
                    period_value = float(mean_period_values[i, j])
                    if not np.isnan(period_value):
                        properties["mean_period"] = round(period_value, 1)
                        properties["units"]["period"] = "seconds"
                        
                if peak_period_values is not None:
                    peak_value = float(peak_period_values[i, j])
                    if not np.isnan(peak_value):
                        properties["peak_period"] = round(peak_value, 1)
                
                return properties
            
            # Generate features using base class utility
            features = self._generate_features(lats, lons, property_generator)
            logger.info(f"Generated {len(features)} features")
            
            # Calculate ranges using base class utility
            data_dict = {
                "height": (height_values, "m")
            }
            
            if direction_values is not None:
                data_dict["direction"] = (direction_values, "degrees")
            if mean_period_values is not None:
                data_dict["mean_period"] = (mean_period_values, "seconds")
            if peak_period_values is not None:
                data_dict["peak_period"] = (peak_period_values, "seconds")
            
            ranges = self._calculate_ranges(data_dict)
            
            # Update metadata
            metadata = {
                "source": dataset,
                "variables": list(SOURCES[dataset]['variables']),
                "available_metrics": ["height"] + 
                    (["direction"] if has_direction else []) +
                    (["mean_period"] if has_mean_period else []) +
                    (["peak_period"] if has_peak_period else []),
                "units": {
                    "height": "m",
                    "direction": "degrees" if has_direction else None,
                    "period": "seconds" if has_mean_period or has_peak_period else None
                }
            }
            
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            logger.info(f"Saving waves GeoJSON to {output_path.name}")
            
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting waves to GeoJSON: {str(e)}", exc_info=True)
            raise