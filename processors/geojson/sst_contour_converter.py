from pathlib import Path
import logging
import datetime
import numpy as np
import matplotlib.pyplot as plt
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS

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

    def _calculate_path_length(self, segment):
        """Calculate the length of a contour segment in degrees."""
        # Calculate differences between consecutive points
        point_differences = np.diff(segment, axis=0)
        
        # Calculate squared distances
        squared_distances = np.sum(point_differences**2, axis=1)
        
        # Sum up the distances to get total path length
        path_length = np.sum(np.sqrt(squared_distances))
        
        return path_length

    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to contour GeoJSON format."""
        try:
            # Load and prepare data
            ds = self.load_dataset(data_path)
            temp_var = ds[SOURCES[dataset]['variables'][0]]
            
            # Process temperature data - ensure 2D
            temp_data = temp_var
            for dim in ['time', 'depth', 'altitude']:
                if dim in temp_data.dims:
                    temp_data = temp_data.isel({dim: 0})
            
            # Convert to Fahrenheit using source unit from settings
            source_unit = SOURCES[dataset].get('source_unit', 'C')  # Default to Celsius if not specified
            if source_unit == 'K':
                temp_data = (temp_data - 273.15) * 9/5 + 32  # Kelvin to Fahrenheit
            else:
                temp_data = temp_data * 9/5 + 32  # Celsius to Fahrenheit
            
            # Get standardized coordinate names
            lon_var, lat_var = self.get_coordinate_names(temp_data)
            
            # Apply regional bounds
            bounds = REGIONS[region]['bounds']
            lon_mask = (temp_data[lon_var] >= bounds[0][0]) & (temp_data[lon_var] <= bounds[1][0])
            lat_mask = (temp_data[lat_var] >= bounds[0][1]) & (temp_data[lat_var] <= bounds[1][1])
            regional_temp = temp_data.where(lon_mask & lat_mask, drop=True)
            
            # Get valid temperatures
            valid_temps = regional_temp.values[~np.isnan(regional_temp.values)]
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
                    fig, ax = plt.subplots(figsize=(10, 10))
                    contour_set = ax.contour(
                        regional_temp[lon_var],
                        regional_temp[lat_var],
                        regional_temp.values,
                        levels=levels
                    )
                    plt.close(fig)
                    
                    # Create features from contours
                    for level_idx, level_value in enumerate(contour_set.levels):
                        for segment in contour_set.allsegs[level_idx]:
                            # Skip segments that are too short (less than 10 points)
                            if len(segment) < 10:
                                continue
                                
                            # Skip segments with small geographical extent
                            path_length = self._calculate_path_length(segment)
                            if path_length < 0.5:  # 0.5 degrees minimum length
                                continue
                            
                            features.append({
                                "type": "Feature",
                                "geometry": {
                                    "type": "LineString",
                                    "coordinates": [[float(x), float(y)] for x, y in segment]
                                },
                                "properties": {
                                    "value": clean_value(level_value),
                                    "unit": "fahrenheit",
                                    "is_key_temp": level_value in self.KEY_TEMPERATURES
                                }
                            })
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