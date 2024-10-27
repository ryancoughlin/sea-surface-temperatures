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
            ds = self.load_dataset(data_path)
            longitude, latitude = self.get_coordinates(ds)
            
            # Get chlorophyll data
            data = self.select_time_slice(ds['chlor_a'])
            if 'altitude' in data.dims:
                data = data.isel(altitude=0)
            
            features = []
            for i, lat in enumerate(latitude):
                for j, lon in enumerate(longitude):
                    value = float(data.values[i, j])
                    if not np.isnan(value) and value > 0:
                        properties = {"chlorophyll": value}
                        features.append(self.create_feature(lon, lat, properties))

            geojson_data = {
                "type": "FeatureCollection",
                "metadata": {
                    "title": "Chlorophyll Concentration",
                    "timestamp": timestamp,
                    "units": "mg/m³"
                },
                "features": features
            }

            output_path = self.generate_geojson_path(region, dataset, timestamp)
            self.save_geojson(geojson_data, output_path)
            return output_path

        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}")
            raise
