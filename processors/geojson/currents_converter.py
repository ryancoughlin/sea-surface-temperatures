import xarray as xr
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class CurrentsGeoJSONConverter(BaseGeoJSONConverter):
    """Converts ocean current data to GeoJSON format with speed and direction"""
    
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """
        Convert currents data to GeoJSON. Calculates and includes current speed and direction.
        Returns data in user-friendly units (m/s for speed, degrees for direction).
        """
        try:
            ds = self.load_dataset(data_path)
            
            # Log available variables
            available_vars = list(ds.variables.keys())
            logger.info(f"🔄 Converting currents data for {dataset}...")
            
            # Get u and v components from dataset config
            u_var, v_var = SOURCES[dataset]['variables']
            
            # Check if variables exist
            if u_var not in ds or v_var not in ds:
                logger.error(f"Missing required current components:")
                if u_var not in ds:
                    logger.error(f"   └── Missing U component: {u_var}")
                if v_var not in ds:
                    logger.error(f"   └── Missing V component: {v_var}")
                logger.error(f"   └── Available variables: {available_vars}")
                logger.error(f"   └── This may indicate an incomplete download or CMEMS service issue")
                raise KeyError(f"Required current components not found in dataset. Available: {available_vars}")
            
            # Load and normalize current components
            u = self.normalize_dataset(ds, u_var)
            v = self.normalize_dataset(ds, v_var)
            
            # Get coordinate names
            lon_name, lat_name = self.get_coordinate_names(u)
            
            # Calculate speed and direction
            speed = np.sqrt(u**2 + v**2)
            direction = np.degrees(np.arctan2(v, u)) % 360
            
            features = []
            for i in range(len(u[lat_name])):
                for j in range(len(u[lon_name])):
                    # Get values for this point
                    spd = float(speed.values[i, j])
                    u_val = float(u.values[i, j])
                    v_val = float(v.values[i, j])
                    
                    # Skip if any values are NaN
                    if np.isnan(spd) or np.isnan(u_val) or np.isnan(v_val):
                        continue
                    
                    direction_val = float(direction.values[i, j])
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                round(float(u[lon_name].values[j]), 4),
                                round(float(u[lat_name].values[i]), 4)
                            ]
                        },
                        "properties": {
                            "speed": round(spd, 2),
                            "direction": round(direction_val, 1),
                            "units": {
                                "speed": "m/s",
                                "direction": "degrees"
                            }
                        }
                    }
                    features.append(feature)
            
            logger.info(f"   └── Generated {len(features)} current vectors")
            
            # Calculate ranges for all variables
            ranges = {
                "speed": {
                    "min": float(speed.min()),
                    "max": float(speed.max()),
                    "unit": "m/s"
                },
                "direction": {
                    "min": float(direction.min()) % 360,
                    "max": float(direction.max()) % 360,
                    "unit": "degrees"
                }
            }
            
            metadata = {
                "available_variables": ["speed", "direction"]
            }
            
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            logger.info(f"   └── Saving GeoJSON to {output_path.name}")
            
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting currents to GeoJSON: {str(e)}")
            logger.error(f"Dataset path: {data_path}")
            if 'ds' in locals():
                logger.error(f"Dataset variables: {list(ds.variables.keys())}")
            raise
