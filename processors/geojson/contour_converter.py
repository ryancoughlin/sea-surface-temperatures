from pathlib import Path
import logging
import datetime
import numpy as np
from scipy.ndimage import gaussian_filter
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

class ContourConverter(BaseGeoJSONConverter):
    # Add class-level constants
    KEY_TEMPERATURES = [44, 48, 54, 60, 65, 70, 72, 74, 76]
    
    def _generate_temp_levels(self, min_temp: float, max_temp: float) -> np.ndarray:
        """Generate temperature contour levels based on data range and key temperatures."""
        # Create ranges with appropriate intervals
        below_50 = np.arange(np.floor(min_temp), 50, 2)
        mid_range = np.arange(50, 75, 1)
        above_75 = np.arange(75, np.ceil(max_temp) + 2, 2)
        
        return np.unique(np.concatenate([below_50, mid_range, above_75]))

    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to fishing-oriented contour GeoJSON format."""
        try:
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            data = self.select_time_slice(data)
            
            # Convert to Fahrenheit for fishing industry standard
            data = data * 1.8 + 32
            
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Get regional bounds and mask
            bounds = REGIONS[region]['bounds']
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Check if this dataset supports gradient magnitude
            has_gradient = (dataset == 'LEOACSPOSSTL3SnrtCDaily' and 
                          'sst_gradient_magnitude' in SOURCES[dataset]['variables'])
            
            if has_gradient:
                # Enhanced smoothing for major features
                smoothed_data = gaussian_filter(regional_data.values, sigma=2, mode='nearest')
                
                # Calculate gradients with directional components
                dy, dx = np.gradient(smoothed_data)
                gradient_magnitude = np.sqrt(dx**2 + dy**2)
                gradient_magnitude = gaussian_filter(gradient_magnitude, sigma=1.5)
                
                # Calculate break thresholds
                strong_break = np.nanpercentile(gradient_magnitude, 95)
                moderate_break = np.nanpercentile(gradient_magnitude, 85)
            else:
                smoothed_data = regional_data.values
                gradient_magnitude = None
                strong_break = None
                moderate_break = None
            
            # Generate temperature levels
            min_temp = np.floor(np.nanmin(smoothed_data))
            max_temp = np.ceil(np.nanmax(smoothed_data))
            base_levels = self._generate_temp_levels(min_temp, max_temp)
            
            # Generate contours
            fig, ax = plt.subplots(figsize=(10, 10))
            contour_set = ax.contour(
                regional_data[lon_name],
                regional_data[lat_name],
                smoothed_data,
                levels=base_levels
            )
            plt.close(fig)
            
            features = []
            for level_idx, level_value in enumerate(contour_set.levels):
                for segment in contour_set.allsegs[level_idx]:
                    if len(segment) < 10:
                        continue
                    
                    path_length = np.sum(np.sqrt(np.sum(np.diff(segment, axis=0)**2, axis=1)))
                    if path_length < 0.5:
                        continue
                    
                    # Calculate gradient properties only if available
                    if has_gradient:
                        x_indices = np.interp(segment[:, 0], regional_data[lon_name], np.arange(len(regional_data[lon_name])))
                        y_indices = np.interp(segment[:, 1], regional_data[lat_name], np.arange(len(regional_data[lat_name])))
                        x_indices = np.clip(x_indices.astype(int), 0, gradient_magnitude.shape[1]-1)
                        y_indices = np.clip(y_indices.astype(int), 0, gradient_magnitude.shape[0]-1)
                        
                        # Get gradient values and check for valid data
                        gradient_values = gradient_magnitude[y_indices, x_indices]
                        valid_gradients = gradient_values[~np.isnan(gradient_values)]
                        
                        if len(valid_gradients) > 0:
                            avg_gradient = float(np.mean(valid_gradients))
                            max_gradient = float(np.max(valid_gradients))
                            
                            break_strength = 'none'
                            if avg_gradient > strong_break:
                                break_strength = 'strong'
                            elif avg_gradient > moderate_break:
                                break_strength = 'moderate'
                        else:
                            avg_gradient = None
                            max_gradient = None
                            break_strength = 'none'
                    else:
                        avg_gradient = None
                        max_gradient = None
                        break_strength = 'none'
                    
                    # Skip non-significant features for gradient-enabled datasets
                    if has_gradient and break_strength == 'none' and level_value not in [60, 65, 70, 72]:
                        continue
                    
                    # Round coordinates to 3 decimal places
                    coords = [[round(float(x), 3), round(float(y), 3)] 
                             for i, (x, y) in enumerate(segment) 
                             if i % 2 == 0 and not (np.isnan(x) or np.isnan(y))]
                    
                    if len(coords) < 5:
                        continue
                    
                    # Base properties for all datasets
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coords
                        },
                        "properties": {
                            "value": clean_value(level_value),
                            "is_key_temp": level_value in self.KEY_TEMPERATURES
                        }
                    }
                    
                    # Add additional properties only for LEOACSPOSSTL3SnrtCDaily
                    if dataset == 'LEOACSPOSSTL3SnrtCDaily':
                        feature["properties"].update({
                            "gradient": clean_value(avg_gradient),
                            "max_gradient": clean_value(max_gradient),
                            "break_strength": break_strength,
                            "length_nm": clean_value(path_length * 60)
                        })
                    
                    features.append(feature)
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "bounds": {
                        "min_lon": clean_value(np.nanmin(regional_data[lon_name])),
                        "max_lon": clean_value(np.nanmax(regional_data[lon_name])),
                        "min_lat": clean_value(np.nanmin(regional_data[lat_name])),
                        "max_lat": clean_value(np.nanmax(regional_data[lat_name]))
                    },
                    "gradient_thresholds": {
                        "strong_break": clean_value(strong_break),
                        "moderate_break": clean_value(moderate_break)
                    },
                    "value_range": {
                        "min": clean_value(np.nanmin(regional_data)),
                        "max": clean_value(np.nanmax(regional_data))
                    }
                }
            }
            
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            self.save_geojson(geojson, asset_paths.contours)
            return asset_paths.contours
            
        except Exception as e:
            logger.error(f"Error converting data to contour GeoJSON: {str(e)}")
            raise