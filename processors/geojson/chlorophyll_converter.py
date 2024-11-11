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
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert chlorophyll data to GeoJSON format."""
        try:
            # Load the netCDF data
            ds = self.load_dataset(data_path)
            
            # Get chlorophyll data and normalize dimensions
            var_name = SOURCES[dataset]['variables'][0]
            data = self.normalize_dataset(ds, var_name)
            
            # Get standardized coordinate names
            lon_name, lat_name = self.get_coordinate_names(data)
            
            # Get region bounds
            bounds = REGIONS[region]['bounds']
            
            # Mask to region
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            features = []
            for i in range(0, len(regional_data[lat_name]), 2):  # Decimation factor of 2
                for j in range(0, len(regional_data[lon_name]), 2):
                    try:
                        value = float(regional_data.values[i, j])
                        if not np.isnan(value) and value > 0:  # Chlorophyll should be positive
                            lon = float(regional_data[lon_name].values[j])
                            lat = float(regional_data[lat_name].values[i])
                            
                            feature = {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [lon, lat]
                                },
                                "properties": {
                                    "value": value,
                                    "unit": "mg/m³"
                                }
                            }
                            features.append(feature)
                            
                    except Exception as e:
                        logger.warning(f"Error processing chlorophyll point ({i},{j}): {e}")
                        continue
            
            # Create GeoJSON structure with metadata
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "source_type": SOURCES[dataset].get('source_type', 'unknown'),
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
            
            # Save and return
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            self.save_geojson(geojson, asset_paths.data)
            return asset_paths.data
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}")
            raise
