from pathlib import Path
import logging
import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.contour import QuadContourSet
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS

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

            # Generate contours using matplotlib
            fig, ax = plt.subplots(figsize=(10, 10))
            
            # Round min/max to nearest 2°F and create levels every 2°F
            min_temp = np.floor(float(regional_data.min()) / 2) * 2
            max_temp = np.ceil(float(regional_data.max()) / 2) * 2
            levels = np.arange(min_temp, max_temp + 2, 2)  # +2 to include max value
            
            contour_set: QuadContourSet = ax.contour(
                regional_data[lon_name],
                regional_data[lat_name],
                regional_data.values,
                levels=levels
            )
            plt.close(fig)

            # Convert to GeoJSON features
            features = []
            for level_index, level_value in enumerate(contour_set.levels):
                for path in contour_set.collections[level_index].get_paths():
                    vertices = path.vertices
                    if len(vertices) < 2:
                        continue
                        
                    coordinates = [[round(float(lon), 2), round(float(lat), 2)] 
                                 for lon, lat in vertices]
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coordinates
                        },
                        "properties": {
                            "value": round(float(level_value), 2),
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