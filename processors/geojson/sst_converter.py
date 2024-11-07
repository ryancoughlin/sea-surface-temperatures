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
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Force 2D by selecting first index of time and depth if they exist
            if 'time' in data.dims:
                data = data.isel(time=0)
            if 'depth' in data.dims:
                data = data.isel(depth=0)
            
            # Get coordinates and mask to region
            lon_name, lat_name = self.get_coordinate_names(data)
            bounds = REGIONS[region]['bounds']
            
            # Mask to region and convert to Fahrenheit
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            data = data.where(lon_mask & lat_mask, drop=True) * 1.8 + 32
            
            # Create GeoJSON features
            features = []
            lats = data[lat_name].values
            lons = data[lon_name].values
            values = data.values
            
            for i in range(len(lats)):
                for j in range(len(lons)):
                    if not np.isnan(values[i, j]):
                        features.append({
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [float(lons[j]), float(lats[i])]
                            },
                            "properties": {
                                "value": float(values[i, j]),
                                "unit": "fahrenheit"
                            }
                        })
            
            return self.save_geojson({
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "source": dataset
                }
            }, self.path_manager.get_asset_paths(date, dataset, region).data)
            
        except Exception as e:
            logger.error(f"Error converting SST data to GeoJSON: {str(e)}")
            raise
