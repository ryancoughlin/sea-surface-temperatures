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
            
            # Get coordinate names and mask data
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            bounds = REGIONS[region]['bounds']
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)

            # Check if we have valid data
            if regional_data.isnull().all():
                logger.warning(f"No valid data found for region {region}")
                return None

            # Get min/max values safely
            valid_data = regional_data.values[~np.isnan(regional_data.values)]
            if len(valid_data) == 0:
                logger.warning(f"No valid data points found for region {region}")
                return None

            min_temp = np.floor(np.nanmin(valid_data))
            max_temp = np.ceil(np.nanmax(valid_data))

            # Ensure we have a valid range
            if min_temp >= max_temp:
                logger.warning(f"Invalid temperature range: min={min_temp}, max={max_temp}")
                return None

            # Create levels with 2°F intervals
            levels = np.arange(min_temp, max_temp + 2, 2)
            
            # Light smoothing
            smoothed_data = gaussian_filter(regional_data.values, sigma=1.2)

            # Generate contours
            fig, ax = plt.subplots(figsize=(10, 10))
            
            contour_set = ax.contour(
                regional_data[lon_name],
                regional_data[lat_name],
                smoothed_data,
                levels=levels,
                corner_mask=True,
                linewidths=1,
                antialiased=True
            )
            plt.close(fig)

            # Process contours
            features = []
            for level_index, level_value in enumerate(contour_set.levels):
                for path in contour_set.collections[level_index].get_paths():
                    vertices = path.vertices
                    if len(vertices) < 3:
                        continue
                    
                    # Path simplification
                    simplified_coords = []
                    prev_point = None
                    min_distance = 0.03
                    max_distance = 0.3
                    
                    for lon, lat in vertices:
                        if prev_point is None:
                            simplified_coords.append([round(float(lon), 4), round(float(lat), 4)])
                            prev_point = (lon, lat)
                        else:
                            distance = np.sqrt((lon - prev_point[0])**2 + (lat - prev_point[1])**2)
                            if distance >= min_distance and distance <= max_distance:
                                simplified_coords.append([round(float(lon), 4), round(float(lat), 4)])
                                prev_point = (lon, lat)
                    
                    if len(simplified_coords) >= 4:
                        valid_path = True
                        for i in range(1, len(simplified_coords)):
                            dist = np.sqrt(
                                (simplified_coords[i][0] - simplified_coords[i-1][0])**2 +
                                (simplified_coords[i][1] - simplified_coords[i-1][1])**2
                            )
                            if dist > max_distance:
                                valid_path = False
                                break
                        
                        if valid_path:
                            feature = {
                                "type": "Feature",
                                "geometry": {
                                    "type": "LineString",
                                    "coordinates": simplified_coords
                                },
                                "properties": {
                                    "value": round(float(level_value), 1),
                                    "unit": "fahrenheit"
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