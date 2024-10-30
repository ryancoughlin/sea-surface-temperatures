from pathlib import Path
import json
import logging
import datetime
import numpy as np
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class SSTGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to GeoJSON format."""
        try:
            # Load the dataset using base class method
            ds = self.load_dataset(data_path)
            
            # Get the variable name from SOURCES
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Select first time slice if time dimension exists
            data = self.select_time_slice(data)
            
            # Get region bounds
            bounds = REGIONS[region]['bounds']
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Mask to region
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Create GeoJSON features with reduced resolution
            stride = 5  # Adjust based on needs
            features = []
            
            for i in range(0, regional_data.shape[0], stride):
                for j in range(0, regional_data.shape[1], stride):
                    try:
                        lon = float(regional_data[lon_name][j])
                        lat = float(regional_data[lat_name][i])
                        value = float(regional_data[i, j])
                        
                        if not np.isnan(value):
                            feature = self.create_feature(
                                lon, lat,
                                {
                                    "value": value,
                                    "unit": "celsius"
                                }
                            )
                            features.append(feature)
                    except Exception as e:
                        logger.warning(f"Error processing SST point ({i},{j}): {e}")
                        continue

            # Get asset paths from PathManager
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            
            # Create GeoJSON structure
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),  # Format datetime
                    "bounds": {
                        "min_lon": float(regional_data[lon_name].min()),
                        "max_lon": float(regional_data[lon_name].max()),
                        "min_lat": float(regional_data[lat_name].min()),
                        "max_lat": float(regional_data[lat_name].max())
                    },
                    "value_range": {
                        "min": float(regional_data.min()),
                        "max": float(regional_data.max())
                    }
                }
            }
            
            # Save using base class method
            self.save_geojson(geojson, asset_paths.data)
            return asset_paths.data
            
        except Exception as e:
            logger.error(f"Error converting SST data to GeoJSON: {str(e)}")
            raise
