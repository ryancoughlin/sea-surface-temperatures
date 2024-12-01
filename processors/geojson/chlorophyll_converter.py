import numpy as np
import xarray as xr
from pathlib import Path
import logging
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
import datetime

logger = logging.getLogger(__name__)

class ChlorophyllGeoJSONConverter(BaseGeoJSONConverter):
    """Converts chlorophyll data to GeoJSON for basic data display."""
    
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert chlorophyll data to GeoJSON format."""
        try:
            # Store data_path for metadata assembler
            self.data_path = data_path
            
            # Load preprocessed data
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = self.normalize_dataset(ds, var_name)
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Create features from valid data points
            features = []
            valid_values = []
            
            for i in range(len(data[lat_name])):
                for j in range(len(data[lon_name])):
                    value = float(data.values[i, j])
                    if not np.isnan(value):
                        valid_values.append(value)
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [
                                    float(data[lon_name][j]),
                                    float(data[lat_name][i])
                                ]
                            },
                            "properties": {
                                "concentration": value,
                                "unit": "mg/m³"
                            }
                        }
                        features.append(feature)
            
            if len(valid_values) == 0:
                logger.warning("No valid chlorophyll data found")
                return self.save_geojson({"type": "FeatureCollection", "features": []}, 
                    self.path_manager.get_asset_paths(date, dataset, region).data)
            
            data_min = float(min(valid_values))
            data_max = float(max(valid_values))
            logger.info(f"[RANGES] GeoJSON data min/max: {data_min:.4f} to {data_max:.4f}")
            
            ranges = {
                "concentration": {
                    "min": data_min,
                    "max": data_max,
                    "unit": "mg/m³"
                }
            }
            
            metadata = {
                "valid_range": {
                    "min": data_min,
                    "max": data_max
                }
            }
            
            # Create and save GeoJSON
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata,
                processed_data=data  # Pass the preprocessed data
            )
            
            return self.save_geojson(
                geojson,
                self.path_manager.get_asset_paths(date, dataset, region).data
            )
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}")
            raise
