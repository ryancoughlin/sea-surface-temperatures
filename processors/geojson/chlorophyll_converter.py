import numpy as np
import xarray as xr
from pathlib import Path
import logging
from .base_converter import BaseGeoJSONConverter
import datetime
logger = logging.getLogger(__name__)

class ChlorophyllGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert chlorophyll data to GeoJSON format."""
        try:
            # Load the netCDF data
            ds = self.load_dataset(data_path)
            
            # Get chlorophyll data
            data = ds['chlor_a']
            
            # Select first time slice if time dimension exists
            data = self.select_time_slice(data)
            
            # Handle altitude dimension if present
            if 'altitude' in data.dims:
                data = data.squeeze('altitude')
            
            features = []
            for i in range(0, len(data.latitude), 2):  # Decimation factor of 2
                for j in range(0, len(data.longitude), 2):
                    try:
                        value = float(data.values[i, j])
                        if not np.isnan(value) and value > 0:  # Chlorophyll should be positive
                            lon = float(data.longitude.values[j])
                            lat = float(data.latitude.values[i])
                            
                            feature = self.create_feature(
                                lon, lat,
                                {
                                    "value": value,
                                    "unit": "mg/m³"
                                }
                            )
                            features.append(feature)
                            
                    except Exception as e:
                        logger.warning(f"Error processing chlorophyll point ({i},{j}): {e}")
                        continue
            
            # Save to GeoJSON
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            self.save_geojson({"type": "FeatureCollection", "features": features}, asset_paths.contours)
            return asset_paths.contours
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}")
            raise
