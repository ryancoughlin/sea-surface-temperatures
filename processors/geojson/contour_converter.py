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
    # Temperature ranges and their significance
    TEMP_RANGES = {
        'very_cold': {'min': 0, 'max': 44, 'interval': 2},
        'cold': {'min': 44, 'max': 54, 'interval': 2},
        'transition': {'min': 54, 'max': 60, 'interval': 1},
        'prime': {'min': 60, 'max': 75, 'interval': 1},
        'warm': {'min': 75, 'max': 90, 'interval': 2}
    }
    
    # Key fishing temperatures
    KEY_TEMPERATURES = [60, 65, 70, 72]  # Most significant for fishing
    
    # Break strength thresholds
    BREAK_THRESHOLDS = {
        'strong': 95,    # 95th percentile - bold lines
        'moderate': 85,  # 85th percentile - medium lines
        'weak': 0       # < 85th percentile - low opacity lines
    }
    
    def _generate_temp_levels(self, min_temp: float, max_temp: float) -> np.ndarray:
        """Generate temperature contour levels based on data range and key temperatures."""
        levels = []
        
        # Very cold waters (< 44°F)
        if min_temp < 44:
            levels.extend(np.arange(np.floor(min_temp), 44, 2))
            
        # Cold waters (44-54°F)
        levels.extend(np.arange(44, 54, 2))
        
        # Transition waters (54-60°F)
        levels.extend(np.arange(54, 60, 1))
        
        # Prime fishing temperatures (60-75°F)
        levels.extend(np.arange(60, 75, 1))
        
        # Warm waters (75°F+)
        if max_temp > 75:
            # Extend range to cover Gulf temps (up to 90°F if needed)
            levels.extend(np.arange(75, np.ceil(max_temp) + 2, 2))
        
        # Ensure unique values only
        return np.unique(levels)

    def _get_break_strength(self, avg_gradient, strong_break, moderate_break):
        """Classify break strength for styling."""
        if avg_gradient > strong_break:
            return 'strong'
        elif avg_gradient > moderate_break:
            return 'moderate'
        return 'weak'  # Instead of 'none'

    def _get_temp_range(self, temp: float) -> str:
        """Classify temperature into fishing-relevant ranges."""
        for range_name, range_info in self.TEMP_RANGES.items():
            if range_info['min'] <= temp < range_info['max']:
                return range_name
        return 'extreme'

    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to fishing-oriented contour GeoJSON format."""
        try:
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Force 2D data by selecting first index of time and depth (or z) if they exist
            if 'time' in data.dims:
                data = data.isel(time=0)
            if 'depth' in data.dims:
                data = data.isel(depth=0)
            elif 'z' in data.dims:
                data = data.isel(z=0)
            
            # Convert to Fahrenheit
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
                strong_break = np.nanpercentile(gradient_magnitude, self.BREAK_THRESHOLDS['strong'])
                moderate_break = np.nanpercentile(gradient_magnitude, self.BREAK_THRESHOLDS['moderate'])
            else:
                smoothed_data = regional_data.values
                gradient_magnitude = None
                strong_break = None
                moderate_break = None
            
            # Generate temperature levels based on actual data range
            min_temp = np.floor(np.nanmin(smoothed_data))
            max_temp = np.ceil(np.nanmax(smoothed_data))
            
            logger.debug(f"Temperature range: {min_temp}°F to {max_temp}°F")
            base_levels = self._generate_temp_levels(min_temp, max_temp)
            logger.debug(f"Generated contour levels: {base_levels}")
            
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
                    
                    # Initialize gradient variables
                    avg_gradient = None
                    max_gradient = None
                    break_strength = 'none'
                    
                    # Calculate gradient properties only for LEOACSPOSSTL3SnrtCDaily
                    if has_gradient:
                        x_indices = np.interp(segment[:, 0], regional_data[lon_name], np.arange(len(regional_data[lon_name])))
                        y_indices = np.interp(segment[:, 1], regional_data[lat_name], np.arange(len(regional_data[lat_name])))
                        x_indices = np.clip(x_indices.astype(int), 0, gradient_magnitude.shape[1]-1)
                        y_indices = np.clip(y_indices.astype(int), 0, gradient_magnitude.shape[0]-1)
                        
                        gradient_values = gradient_magnitude[y_indices, x_indices]
                        valid_gradients = gradient_values[~np.isnan(gradient_values)]
                        
                        if len(valid_gradients) > 0:
                            avg_gradient = float(np.mean(valid_gradients))
                            max_gradient = float(np.max(valid_gradients))
                            break_strength = self._get_break_strength(avg_gradient, strong_break, moderate_break)
                    
                    coords = [[float(x), float(y)] for i, (x, y) in enumerate(segment) 
                             if i % 2 == 0 and not (np.isnan(x) or np.isnan(y))]
                    
                    if len(coords) < 5:
                        continue
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coords
                        },
                        "properties": {
                            "value": clean_value(level_value),
                            "unit": "fahrenheit",
                            "gradient": clean_value(avg_gradient),
                            "max_gradient": clean_value(max_gradient),
                            "break_strength": break_strength,
                            "length_nm": clean_value(round(path_length * 60, 1)),
                            "is_key_temp": level_value in [60, 65, 70, 72]
                        }
                    }
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