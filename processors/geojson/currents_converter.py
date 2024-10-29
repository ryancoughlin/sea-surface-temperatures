import xarray as xr
import numpy as np
import json
import logging
import datetime
from pathlib import Path
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class CurrentsGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert currents data to GeoJSON with enhanced vector properties."""
        try:
            # Load dataset using base class method
            ds = self.load_dataset(data_path)
            
            bounds = REGIONS[region]['bounds']
            dataset_config = SOURCES[dataset]
            export_config = dataset_config.get('export_geojson', {})
            
            # Configuration
            decimation = export_config.get('decimation_factor', 3)
            vector_scale = export_config.get('vector_scale', 50)
            min_magnitude = export_config.get('min_magnitude', 0.05)
            
            # Create regional subset
            ds_subset = ds.sel(
                longitude=slice(bounds[0][0], bounds[1][0]),
                latitude=slice(bounds[0][1], bounds[1][1]),
                time=ds.time[0]
            )
            
            # Get current components and calculate derived values
            u = ds_subset.u_current
            v = ds_subset.v_current
            speed = np.sqrt(u**2 + v**2)
            direction = np.degrees(np.arctan2(v, u)) % 360
            
            # Convert to numpy arrays
            u_array = u.values
            v_array = v.values
            lon_array = ds_subset.longitude.values
            lat_array = ds_subset.latitude.values
            speed_array = speed.values
            direction_array = direction.values
            
            features = []
            
            # Create streamline points
            for i in range(0, len(lon_array), decimation):
                for j in range(0, len(lat_array), decimation):
                    spd = float(speed_array[j, i])
                    
                    if np.isnan(spd) or spd < min_magnitude:
                        continue
                    
                    u_val = float(u_array[j, i])
                    v_val = float(v_array[j, i])
                    if np.isnan(u_val) or np.isnan(v_val):
                        continue
                    
                    u_norm = u_val / spd
                    v_norm = v_val / spd
                    direction_val = float(direction_array[j, i])
                    direction_val = None if np.isnan(direction_val) else direction_val

                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(lon_array[i]), float(lat_array[j])]
                        },
                        "properties": {
                            "u": u_val * vector_scale,
                            "v": v_val * vector_scale,
                            "u_norm": u_norm,
                            "v_norm": v_norm,
                            "speed": spd,
                            "direction": direction_val,
                            "speed_normalized": min(1.0, spd / 2.0),
                            "vector_magnitude": spd * vector_scale
                        }
                    }
                    features.append(feature)
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "vector_scale": vector_scale,
                    "decimation_factor": decimation,
                    "min_magnitude": min_magnitude,
                    "bounds": {
                        "min_lon": float(lon_array.min()),
                        "max_lon": float(lon_array.max()),
                        "min_lat": float(lat_array.min()),
                        "max_lat": float(lat_array.max())
                    },
                    "speed_range": {
                        "min": float(speed_array.min()),
                        "max": float(speed_array.max())
                    }
                }
            }
            
            # Get asset paths from PathManager
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            
            # Save using base class method
            self.save_geojson(geojson, asset_paths.contours)
            return asset_paths.contours
            
        except Exception as e:
            logger.error(f"Error converting currents to GeoJSON: {str(e)}")
            raise
