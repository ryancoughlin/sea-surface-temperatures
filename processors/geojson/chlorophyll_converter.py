import xarray as xr
import numpy as np
import json
import logging
from pathlib import Path
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class ChlorophyllGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert chlorophyll data to GeoJSON format."""
        try:
            ds = xr.open_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            if 'time' in data.dims:
                data = data.isel(time=0)
            
            # Create GeoJSON features
            stride = 5  # Adjust based on needs
            features = []
            
            for i in range(0, data.shape[0], stride):
                for j in range(0, data.shape[1], stride):
                    lon = float(data.longitude[j])
                    lat = float(data.latitude[i])
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
                                "unit": "mg/m³"
                            }
                        }
                        features.append(feature)
            
            geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            output_path = self.generate_geojson_path(region, dataset, timestamp)
            with open(output_path, 'w') as f:
                json.dump(geojson, f)
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}")
            raise