from pathlib import Path
import logging
import datetime
import numpy as np
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class SSTGeoJSONConverter(BaseGeoJSONConverter):
    def _mask_to_region(self, data, bounds, lon_name, lat_name):
        """Mask dataset to region bounds."""
        lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
        lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
        return data.where(lon_mask & lat_mask, drop=True)

    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to GeoJSON format."""
        try:
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Force 2D by selecting first index of time and depth
            for dim in ['time', 'depth']:
                if dim in data.dims:
                    data = data.isel({dim: 0})
            
            # Get coordinates and mask to region
            lon_name, lat_name = self.get_coordinate_names(data)
            bounds = REGIONS[region]['bounds']
            
            # Mask and convert to Fahrenheit
            data = self._mask_to_region(data, bounds, lon_name, lat_name)
            data = data * 1.8 + 32
            
            # Create features
            features = []
            for i in range(len(data[lat_name])):
                for j in range(len(data[lon_name])):
                    value = float(data.values[i, j])
                    if not np.isnan(value):
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
                                "temperature": round(value, 2),
                                "unit": "fahrenheit"
                            }
                        }
                        features.append(feature)
            
            ranges = {
                "temperature": {
                    "min": float(data.min()),
                    "max": float(data.max()),
                    "unit": "fahrenheit"
                }
            }
            
            metadata = {
                "source": dataset,
                "conversion": "celsius_to_fahrenheit"
            }
            
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            return self.save_geojson(
                geojson,
                self.path_manager.get_asset_paths(date, dataset, region).data
            )
            
        except Exception as e:
            logger.error(f"Error converting SST data to GeoJSON: {str(e)}")
            raise