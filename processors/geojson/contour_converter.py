from pathlib import Path
import logging
import datetime
import numpy as np
from scipy.ndimage import gaussian_filter
import matplotlib.pyplot as plt
from matplotlib.contour import QuadContourSet
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS
from scipy.interpolate import RectBivariateSpline

logger = logging.getLogger(__name__)

class ContourConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to contour line GeoJSON format."""
        try:
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            data = self.select_time_slice(data)
            
            # Convert Celsius to Fahrenheit
            data = data * 1.8 + 32
            
            # Create regular grid from the data points
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Get regional bounds and mask
            bounds = REGIONS[region]['bounds']
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Apply stronger smoothing to identify major features
            smoothed_data = gaussian_filter(regional_data.values, sigma=2, mode='nearest')
            
            # Calculate gradients with additional smoothing
            dy, dx = np.gradient(smoothed_data)
            gradient_magnitude = gaussian_filter(np.sqrt(dx**2 + dy**2), sigma=1.5)
            
            # More selective threshold - top 5% of gradients
            gradient_threshold = np.nanpercentile(gradient_magnitude, 95)
            
            # Create temperature levels
            min_temp = np.floor(np.nanmin(smoothed_data))
            max_temp = np.ceil(np.nanmax(smoothed_data))
            base_levels = np.arange(min_temp, max_temp + 2, 2)

            # Generate contours
            fig, ax = plt.subplots(figsize=(10, 10))
            contour_set = ax.contour(
                regional_data[lon_name],
                regional_data[lat_name],
                smoothed_data,
                levels=base_levels
            )
            plt.close(fig)

            # Process contours with stricter filtering
            features = []
            for level_idx, level_value in enumerate(contour_set.levels):
                for segment in contour_set.allsegs[level_idx]:
                    # Require longer minimum length
                    if len(segment) < 10:  # Increased from 5
                        continue
                    
                    path_length = np.sum(np.sqrt(np.sum(np.diff(segment, axis=0)**2, axis=1)))
                    # Increased minimum path length
                    if path_length < 0.5:  # Increased from 0.1
                        continue
                    
                    x_indices = np.interp(segment[:, 0], regional_data[lon_name], np.arange(len(regional_data[lon_name])))
                    y_indices = np.interp(segment[:, 1], regional_data[lat_name], np.arange(len(regional_data[lat_name])))
                    x_indices = np.clip(x_indices.astype(int), 0, gradient_magnitude.shape[1]-1)
                    y_indices = np.clip(y_indices.astype(int), 0, gradient_magnitude.shape[0]-1)
                    
                    # Calculate average gradient along the contour
                    avg_gradient = float(np.nanmean(gradient_magnitude[y_indices, x_indices]))
                    
                    # More strict break definition
                    is_break = (avg_gradient > gradient_threshold and path_length > 1.0)
                    
                    # Skip features that aren't breaks
                    if not is_break:
                        continue
                    
                    # Simplify coordinates to reduce noise
                    coords = [[float(x), float(y)] for i, (x, y) in enumerate(segment) 
                             if i % 2 == 0 and not (np.isnan(x) or np.isnan(y))]
                    
                    if len(coords) < 5:  # Ensure we still have enough points after simplification
                        continue
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coords
                        },
                        "properties": {
                            "value": float(level_value),
                            "unit": "fahrenheit",
                            "gradient": float(avg_gradient) if not np.isnan(avg_gradient) else 0.0,
                            "is_temp_break": bool(is_break)
                        }
                    }
                    features.append(feature)

            # Get asset paths and create GeoJSON
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "bounds": {
                        "min_lon": round(float(regional_data[lon_name].min()), 2),
                        "max_lon": round(float(regional_data[lon_name].max()), 2),
                        "min_lat": round(float(regional_data[lat_name].min()), 2),
                        "max_lat": round(float(regional_data[lat_name].max()), 2)
                    },
                    "value_range": {
                        "min": round(float(regional_data.min()), 2),
                        "max": round(float(regional_data.max()), 2)
                    },
                    "levels": [round(float(level), 2) for level in contour_set.levels]
                }
            }
            
            self.save_geojson(geojson, asset_paths.contours)
            return asset_paths.contours
        except Exception as e:
            logger.error(f"Error converting data to contour GeoJSON: {str(e)}")
            raise