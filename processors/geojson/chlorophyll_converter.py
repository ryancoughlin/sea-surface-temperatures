import numpy as np
import xarray as xr
from pathlib import Path
import logging
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS
import datetime

logger = logging.getLogger(__name__)

class ChlorophyllGeoJSONConverter(BaseGeoJSONConverter):
    """Converts chlorophyll data to GeoJSON for basic data display."""
    
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert chlorophyll data to GeoJSON format."""
        try:
            # Load and prepare data
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = self.normalize_dataset(ds, var_name)
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(data)
            
            # Mask to region
            bounds = REGIONS[region]['bounds']
            regional_data = data.where(
                (data[lon_name] >= bounds[0][0]) & 
                (data[lon_name] <= bounds[1][0]) &
                (data[lat_name] >= bounds[0][1]) & 
                (data[lat_name] <= bounds[1][1]),
                drop=True
            )
            
            # Create features for each point
            features = []
            for i in range(len(regional_data[lat_name])):
                for j in range(len(regional_data[lon_name])):
                    value = float(regional_data.values[i, j])
                    if not np.isnan(value):
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [
                                    float(regional_data[lon_name][j]),
                                    float(regional_data[lat_name][i])
                                ]
                            },
                            "properties": {
                                "value": value,
                                "unit": "mg/m³"
                            }
                        }
                        features.append(feature)
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "data_range": {
                        "min": float(regional_data.min()),
                        "max": float(regional_data.max())
                    }
                }
            }
            
            # Save and return
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            self.save_geojson(geojson, asset_paths.data)
            return asset_paths.data
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}")
            raise
