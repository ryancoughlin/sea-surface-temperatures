from pathlib import Path
import logging
import datetime
import numpy as np
import matplotlib.pyplot as plt
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
import xarray as xr
from typing import Union, Dict, Optional, Any
from shapely.geometry import LineString, mapping

logger = logging.getLogger(__name__)

def clean_value(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return float(value)

class SSTContourConverter(BaseGeoJSONConverter):
    TEMP_INTERVALS = [
        (0, 44, 2),    # Very cold
        (44, 54, 2),   # Cold
        (54, 60, 1),   # Transition
        (60, 75, 1),   # Prime
        (75, 90, 2)    # Warm
    ]
    
    KEY_TEMPERATURES = [60, 65, 70, 72]
    BREAK_THRESHOLDS = {'strong': 95, 'moderate': 85, 'weak': 0}

    def _generate_levels(self, min_temp: float, max_temp: float) -> np.ndarray:
        levels = []
        for start, end, interval in self.TEMP_INTERVALS:
            if max_temp >= start and min_temp <= end:
                range_start = max(start, np.floor(min_temp))
                range_end = min(end, np.ceil(max_temp))
                levels.extend(np.arange(range_start, range_end, interval))
        return np.unique(levels)

    def _classify_feature(self, level: float, gradient_data: Optional[np.ndarray] = None) -> Dict:
        strength = 'weak'
        if gradient_data is not None:
            valid_gradients = gradient_data[~np.isnan(gradient_data)]
            if len(valid_gradients) > 0:
                avg_gradient = float(np.mean(valid_gradients))
                for strength_level, threshold in self.BREAK_THRESHOLDS.items():
                    if avg_gradient > np.percentile(valid_gradients, threshold):
                        strength = strength_level
                        break
        
        return {
            "is_key_temp": level in self.KEY_TEMPERATURES,
            "strength": strength,
            "type": "temperature_break" if strength != 'weak' else "temperature_line"
        }

    def convert(self, data: Union[xr.DataArray, xr.Dataset, Dict], region: str, dataset: str, date: datetime) -> Path:
        try:
            # Handle standardized data format
            if isinstance(data, dict) and 'data' in data:
                data = data['data']
            
            # Extract temperature and gradient data
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                sst_var = next(var for var in variables if 'sst' in var.lower() or 'temperature' in var.lower())
                temp_data = data[sst_var]
                gradient_var = next((var for var in variables if 'gradient' in var.lower()), None)
                gradient_data = data[gradient_var] if gradient_var else None
            else:
                temp_data = data
                gradient_data = None
            
            # Get valid temperatures
            valid_temps = temp_data.values[~np.isnan(temp_data.values)]
            if len(valid_temps) == 0:
                logger.warning("No valid temperature data points found")
                return self._create_geojson([], date, None, None)
            
            min_temp = float(np.min(valid_temps))
            max_temp = float(np.max(valid_temps))
            logger.info(f"Processing SST data for {date} with min: {min_temp:.2f}, max: {max_temp:.2f}")   
            
            # Generate contours if we have sufficient data
            features = []
            if len(valid_temps) >= 10 and (max_temp - min_temp) >= 0.5:
                try:
                    levels = self._generate_levels(min_temp, max_temp)
                    
                    # Generate contours using matplotlib
                    fig, ax = plt.subplots(figsize=(10, 10))
                    contour_set = ax.contour(
                        temp_data.longitude.values,
                        temp_data.latitude.values,
                        temp_data.values,
                        levels=levels,
                        linestyles='solid',
                        linewidths=1.5,
                        colors='black'
                    )
                    plt.close(fig)
                    
                    # Create features from contour data
                    for level_idx, level in enumerate(contour_set.levels):
                        for segment in contour_set.allsegs[level_idx]:
                            # Calculate path length
                            path_length = float(LineString(segment).length)
                            
                            # Filter short segments
                            if path_length < 0.5 or len(segment) < 10:
                                continue
                                
                            coords = [[float(x), float(y)] for x, y in segment 
                                     if not (np.isnan(x) or np.isnan(y))]
                            
                            if len(coords) < 5:
                                continue
                            
                            # Classify feature and create properties
                            classification = self._classify_feature(
                                level,
                                gradient_data=gradient_data.values if gradient_data is not None else None
                            )
                            
                            feature = {
                                "type": "Feature",
                                "geometry": {
                                    "type": "LineString",
                                    "coordinates": coords
                                },
                                "properties": {
                                    "value": clean_value(level),
                                    "unit": "fahrenheit",
                                    "path_length_nm": round(path_length * 60, 1),
                                    "points": len(coords),
                                    "is_closed": False,
                                    "is_key_temp": classification['is_key_temp'],
                                    "strength": classification['strength'],
                                    "feature_type": classification['type']
                                }
                            }
                            features.append(feature)
                            
                except Exception as e:
                    logger.warning(f"Could not generate contours: {str(e)}")
            
            # Create and save GeoJSON
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "value_range": {
                        "min": clean_value(min_temp),
                        "max": clean_value(max_temp)
                    },
                    "key_temperatures": self.KEY_TEMPERATURES
                }
            }
            
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