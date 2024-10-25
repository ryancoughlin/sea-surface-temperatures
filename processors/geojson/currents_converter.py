import numpy as np
import xarray as xr
from pathlib import Path
import json
import logging
from .base_converter import BaseGeoJSONConverter

logger = logging.getLogger(__name__)

class CurrentsGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert currents data to GeoJSON format."""
        try:
            # Load the netCDF data
            ds = xr.open_dataset(data_path)
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
            
            # Extract u and v components
            u_data = ds['u_current'].isel(time=0)
            v_data = ds['v_current'].isel(time=0)
            
            # Calculate speed
            speed = np.sqrt(u_data**2 + v_data**2)
            
            # Create GeoJSON features with reduced resolution
            stride = 5  # Adjust based on needs
            features = []
            
            for i in range(0, speed.shape[0], stride):
                for j in range(0, speed.shape[1], stride):
                    lon = float(ds[lon_name][j])
                    lat = float(ds[lat_name][i])
                    u = float(u_data[i, j])
                    v = float(v_data[i, j])
                    spd = float(speed[i, j])
                    
                    if not np.isnan(spd):
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [lon, lat]
                            },
                            "properties": {
                                "speed": spd,
                                "direction": np.arctan2(v, u) * 180 / np.pi,
                                "u": u,
                                "v": v,
                                "unit": "m/s"
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
            
            logger.info(f"Generated GeoJSON file: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting currents data to GeoJSON: {str(e)}")
            raise
