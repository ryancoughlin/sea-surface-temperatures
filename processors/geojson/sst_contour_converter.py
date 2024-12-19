from pathlib import Path
import logging
import datetime
import numpy as np
import matplotlib.pyplot as plt
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS
import xarray as xr
from typing import Union, Dict, Optional, Any

logger = logging.getLogger(__name__)

def clean_value(value):
    """Convert NaN or invalid values to null, otherwise return the float value."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return float(value)

class SSTContourConverter(BaseGeoJSONConverter):
    # Simplified temperature ranges with clear intervals
    TEMP_INTERVALS = [
        (0, 44, 2),    # Very cold
        (44, 54, 2),   # Cold
        (54, 60, 1),   # Transition
        (60, 75, 1),   # Prime
        (75, 90, 2)    # Warm
    ]
    
    KEY_TEMPERATURES = [60, 65, 70, 72]
    BREAK_THRESHOLDS = {'strong': 95, 'moderate': 85, 'weak': 0}

    def _generate_levels(self, min_temp, max_temp):
        """Generate temperature contour levels."""
        levels = []
        for start, end, interval in self.TEMP_INTERVALS:
            if max_temp >= start and min_temp <= end:
                range_start = max(start, np.floor(min_temp))
                range_end = min(end, np.ceil(max_temp))
                levels.extend(np.arange(range_start, range_end, interval))
        return np.unique(levels)

    def _process_gradient_data(self, gradient_data, segment):
        """Process gradient data for a contour segment."""
        if gradient_data is None:
            return None, None, 'weak'
            
        valid_gradients = gradient_data[~np.isnan(gradient_data)]
        if len(valid_gradients) == 0:
            return None, None, 'weak'
            
        avg_gradient = float(np.mean(valid_gradients))
        max_gradient = float(np.max(valid_gradients))
        
        # Determine break strength
        for strength, threshold in self.BREAK_THRESHOLDS.items():
            if avg_gradient > np.percentile(valid_gradients, threshold):
                return avg_gradient, max_gradient, strength
                
        return avg_gradient, max_gradient, 'weak'

    def _process_contour_properties(self, contour_data: Dict, gradient_data: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """Process SST-specific contour properties."""
        level = contour_data['level']
        segment = contour_data['segment']
        
        properties = {
            "value": clean_value(level),
            "unit": "fahrenheit",
            "path_length_nm": round(contour_data['path_length'] * 60, 1),
            "points": contour_data['points'],
            "is_closed": False,
            "is_key_temp": level in self.KEY_TEMPERATURES
        }
        
        # Add gradient information if available
        if gradient_data is not None:
            avg_gradient, max_gradient, strength = self._process_gradient_data(gradient_data, segment)
            if avg_gradient is not None:
                properties.update({
                    "avg_gradient": round(avg_gradient, 4),
                    "max_gradient": round(max_gradient, 4),
                    "strength": strength
                })
        
        return properties

    def convert(self, data: Union[xr.DataArray, xr.Dataset, Dict], region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to contour GeoJSON format."""
        try:
            # Extract temperature and gradient data
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                sst_var = next(var for var in variables if 'sst' in var.lower() or 'temperature' in var.lower())
                temp_data = data[sst_var]
                
                # Check for gradient data
                gradient_var = next((var for var in variables if 'gradient' in var.lower()), None)
                gradient_data = data[gradient_var] if gradient_var else None
            else:
                temp_data = data
                gradient_data = None
            
            # Get valid temperatures
            valid_temps = temp_data.values[~np.isnan(temp_data.values)]
            if len(valid_temps) == 0:
                return self._create_geojson([], date, None, None)
            
            min_temp = float(np.min(valid_temps))
            max_temp = float(np.max(valid_temps))

            logger.info(f"Processing SST data for {date} with min: {min_temp}, max: {max_temp}")   
            
            # Generate contours if we have sufficient data
            features = []
            if len(valid_temps) >= 10 and (max_temp - min_temp) >= 0.5:
                try:
                    levels = self._generate_levels(min_temp, max_temp)
                    
                    # Generate contours using base method
                    contour_data = self._generate_contours(
                        data=temp_data.values,
                        lons=temp_data.longitude.values,
                        lats=temp_data.latitude.values,
                        levels=levels,
                        min_points=10,
                        min_path_length=0.5
                    )
                    
                    # Create features from contour data
                    for data in contour_data:
                        properties = self._process_contour_properties(
                            data,
                            gradient_data=gradient_data.values if gradient_data is not None else None
                        )
                        
                        feature = self._create_geojson_feature(
                            segment=data['segment'],
                            properties=properties,
                            is_closed=False
                        )
                        features.append(feature)
                            
                except Exception as e:
                    logger.warning(f"Could not generate contours: {str(e)}")
            
            # Create and save GeoJSON
            geojson = self._create_geojson(features, date, min_temp, max_temp)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            return self.save_geojson(geojson, asset_paths.contours)
            
        except Exception as e:
            logger.error(f"Error converting data to contour GeoJSON: {str(e)}")
            raise

    def _create_geojson(self, features, date, min_temp, max_temp):
        """Create a GeoJSON object with consistent structure."""
        return {
            "type": "FeatureCollection",
            "features": features,
            "properties": {
                "date": date.strftime('%Y-%m-%d'),
                "value_range": {
                    "min": clean_value(min_temp),
                    "max": clean_value(max_temp)
                }
            }
        }