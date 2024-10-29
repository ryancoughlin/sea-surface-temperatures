import numpy as np
import xarray as xr
from pathlib import Path
import json
import logging
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class SSTGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert SST data to GeoJSON format."""
        try:
            # Load the netCDF data
            ds = xr.open_dataset(data_path)
            
            # Get the variable name directly from SOURCES
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Select first time slice if time dimension exists
            if 'time' in data.dims:
                data = data.isel(time=0)
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Create GeoJSON features with reduced resolution
            stride = 5  # Adjust based on needs
            features = []
            
            for i in range(0, data.shape[0], stride):
                for j in range(0, data.shape[1], stride):
                    lon = float(data[lon_name][j])
                    lat = float(data[lat_name][i])
                    value = float(data[i, j])
                    
                    if not np.isnan(value):
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [lon, lat]
                            },
                            "properties": {
                                "value": value,
                            }
                        }
                        features.append(feature)
            
            # Create GeoJSON structure
            geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            # Save to file
            output_path = self.generate_geojson_path(region, dataset, timestamp)
            with open(output_path, 'w') as f:
                json.dump(geojson, f)
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting SST data to GeoJSON: {str(e)}")
            raise
