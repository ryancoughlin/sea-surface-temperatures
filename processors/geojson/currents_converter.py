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
            ds = self.load_dataset(data_path)
            
            bounds = REGIONS[region]['bounds']
            dataset_config = SOURCES[dataset]
            export_config = dataset_config.get('export_geojson', {})
            
            # Configuration
            decimation = export_config.get('decimation_factor', 3)
            vector_scale = export_config.get('vector_scale', 50)
            min_magnitude = export_config.get('min_magnitude', 0.05)
            
            # Get variable names from SOURCES config
            u_var, v_var = dataset_config['variables']
            
            # Normalize the dataset structure for both components
            u = self.normalize_dataset(ds, u_var)
            v = self.normalize_dataset(ds, v_var)
            
            # Get standardized coordinate names
            lon_name, lat_name = self.get_coordinate_names(u)
            
            # Mask to region
            lon_mask = (u[lon_name] >= bounds[0][0]) & (u[lon_name] <= bounds[1][0])
            lat_mask = (u[lat_name] >= bounds[0][1]) & (u[lat_name] <= bounds[1][1])
            
            u = u.where(lon_mask & lat_mask, drop=True)
            v = v.where(lon_mask & lat_mask, drop=True)
            
            # Calculate derived values
            speed = np.sqrt(u**2 + v**2)
            direction = np.degrees(np.arctan2(v, u)) % 360
            
            features = []
            for i in range(0, len(u[lat_name]), decimation):
                for j in range(0, len(u[lon_name]), decimation):
                    try:
                        spd = float(speed.values[i, j])
                        
                        if np.isnan(spd) or spd < min_magnitude:
                            continue
                        
                        u_val = float(u.values[i, j])
                        v_val = float(v.values[i, j])
                        if np.isnan(u_val) or np.isnan(v_val):
                            continue
                        
                        u_norm = u_val / spd if spd > 0 else 0
                        v_norm = v_val / spd if spd > 0 else 0
                        direction_val = float(direction.values[i, j])
                        
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [
                                    float(u[lon_name].values[j]), 
                                    float(u[lat_name].values[i])
                                ]
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
                    except Exception as e:
                        logger.warning(f"Error processing current point ({i},{j}): {e}")
                        continue
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "vector_scale": vector_scale,
                    "decimation_factor": decimation,
                    "min_magnitude": min_magnitude,
                    "bounds": {
                        "min_lon": float(u[lon_name].min()),
                        "max_lon": float(u[lon_name].max()),
                        "min_lat": float(u[lat_name].min()),
                        "max_lat": float(u[lat_name].max())
                    },
                    "speed_range": {
                        "min": float(speed.min()),
                        "max": float(speed.max())
                    }
                }
            }
            
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            self.save_geojson(geojson, asset_paths.data)
            return asset_paths.data
            
        except Exception as e:
            logger.error(f"Error converting currents to GeoJSON: {str(e)}")
            raise
