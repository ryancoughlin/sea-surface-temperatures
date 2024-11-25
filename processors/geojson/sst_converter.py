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

    def _create_features(self, data, lats, lons):
        """Create GeoJSON features from gridded data."""
        # Use numpy operations instead of nested loops
        valid_mask = ~np.isnan(data)
        y_indices, x_indices = np.where(valid_mask)
        
        features = []
        for y, x in zip(y_indices, x_indices):
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lons[x]), float(lats[y])]
                },
                "properties": {
                    "value": round(float(data[y, x]), 2),
                }
            })
        return features

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
            features = self._create_features(
                data.values,
                data[lat_name].values,
                data[lon_name].values
            )
            
            # Save and return
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "source": dataset
                }
            }
            
            return self.save_geojson(
                geojson,
                self.path_manager.get_asset_paths(date, dataset, region).data
            )
            
        except Exception as e:
            logger.error(f"Error converting SST data to GeoJSON: {str(e)}")
            raise