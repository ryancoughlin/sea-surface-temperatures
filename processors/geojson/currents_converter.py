import xarray as xr
import numpy as np
import json
import logging
from pathlib import Path
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class CurrentsGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert currents data to GeoJSON format."""
        try:
            # Get dataset configuration
            dataset_config = SOURCES[dataset]
            export_config = dataset_config.get('export_geojson', {})
            decimation = export_config.get('decimation_factor', 4)
            vector_scale = export_config.get('vector_scale', 50)
            min_magnitude = export_config.get('min_magnitude', 0.1)

            # Load data
            ds = xr.open_dataset(data_path)
            bounds = REGIONS[region]['bounds']

            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'

            # Create regional subset
            ds_subset = ds.sel(
                **{lon_name: slice(bounds[0][0], bounds[1][0])},
                **{lat_name: slice(bounds[0][1], bounds[1][1])}
            )

            # Get u and v components for first time step
            u = ds_subset.u_current.isel(time=0)
            v = ds_subset.v_current.isel(time=0)

            # Get coordinate arrays
            lons = ds_subset[lon_name]
            lats = ds_subset[lat_name]

            # Create GeoJSON features
            features = []
            
            # Use numpy arrays for better performance
            u_array = u.values
            v_array = v.values
            lon_array = lons.values
            lat_array = lats.values

            # Iterate through coordinates with proper bounds checking
            for i in range(0, len(lon_array), decimation):
                for j in range(0, len(lat_array), decimation):
                    u_val = float(u_array[j, i])  # Note the order: [lat, lon]
                    v_val = float(v_array[j, i])
                    speed = float(np.sqrt(u_val**2 + v_val**2))
                    
                    # Skip points with speed below minimum threshold
                    if speed < min_magnitude:
                        continue

                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(lon_array[i]), float(lat_array[j])]
                        },
                        "properties": {
                            "u": u_val * vector_scale,
                            "v": v_val * vector_scale,
                            "speed": speed
                        }
                    }
                    features.append(feature)

            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "timestamp": timestamp,
                    "vector_scale": vector_scale,
                    "decimation_factor": decimation,
                    "min_magnitude": min_magnitude
                }
            }

            # Save to file
            output_path = self.generate_geojson_path(region, dataset, timestamp)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(geojson, f)

            logger.info(f"Saved currents GeoJSON to {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error converting currents to GeoJSON: {str(e)}")
            raise
