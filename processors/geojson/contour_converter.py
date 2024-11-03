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
            
            # Enhanced smoothing for major features
            smoothed_data = gaussian_filter(regional_data.values, sigma=2, mode='nearest')
            
            # Calculate gradients with directional components
            dy, dx = np.gradient(smoothed_data)
            gradient_magnitude = np.sqrt(dx**2 + dy**2)
            gradient_direction = np.arctan2(dy, dx)
            
            # Apply additional smoothing to gradients
            gradient_magnitude = gaussian_filter(gradient_magnitude, sigma=1.5)
            
            # Calculate break thresholds
            strong_break = np.nanpercentile(gradient_magnitude, 95)  # Strong temperature break
            moderate_break = np.nanpercentile(gradient_magnitude, 85)  # Moderate break
            
            # Generate temperature levels with finer resolution in key ranges
            min_temp = np.floor(np.nanmin(smoothed_data))
            max_temp = np.ceil(np.nanmax(smoothed_data))
            
            # Create custom levels focusing on key fishing temperatures
            base_levels = np.concatenate([
                np.arange(min_temp, 50, 2),  # Wider spacing for cold water
                np.arange(50, 75, 1),        # Finer spacing for prime fishing temps
                np.arange(75, max_temp + 2, 2)  # Wider spacing for warm water
            ])
            
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
                    
                    # Calculate path characteristics
                    path_length = np.sum(np.sqrt(np.sum(np.diff(segment, axis=0)**2, axis=1)))
                    if path_length < 0.5:
                        continue
                    
                    # Sample gradients along contour
                    x_indices = np.interp(segment[:, 0], regional_data[lon_name], np.arange(len(regional_data[lon_name])))
                    y_indices = np.interp(segment[:, 1], regional_data[lat_name], np.arange(len(regional_data[lat_name])))
                    x_indices = np.clip(x_indices.astype(int), 0, gradient_magnitude.shape[1]-1)
                    y_indices = np.clip(y_indices.astype(int), 0, gradient_magnitude.shape[0]-1)
                    
                    # Calculate gradient statistics
                    avg_gradient = float(np.nanmean(gradient_magnitude[y_indices, x_indices]))
                    max_gradient = float(np.nanmax(gradient_magnitude[y_indices, x_indices]))
                    
                    # Classify break strength
                    break_strength = 'none'
                    if avg_gradient > strong_break:
                        break_strength = 'strong'
                    elif avg_gradient > moderate_break:
                        break_strength = 'moderate'
                    
                    # Skip non-significant features
                    if break_strength == 'none' and level_value not in [60, 65, 70, 72]:  # Key fishing temps
                        continue
                    
                    # Simplify coordinates while preserving important points
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
                            "length_nm": clean_value(path_length * 60),  # Convert to nautical miles
                            "is_key_temp": level_value in [60, 65, 70, 72]
                        }
                    }
                    features.append(feature)
            
            # Clean metadata values
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