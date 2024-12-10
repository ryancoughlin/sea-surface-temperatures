import xarray as xr
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class CurrentsGeoJSONConverter(BaseGeoJSONConverter):
    """Converts ocean current data to GeoJSON format with speed and direction"""
    
    def convert(self, data: xr.DataArray, region: str, dataset: str, date: datetime) -> Path:
        """
        Convert currents data to GeoJSON. Calculates and includes current speed and direction.
        Returns data in user-friendly units (m/s for speed, degrees for direction).
        """
        try:
            # Get u and v components from dataset config
            u_var, v_var = SOURCES[dataset]['variables']
            
            # Split data into u and v components using variable names
            u = data[u_var]  # Get u component by name
            v = data[v_var]  # Get v component by name
            
            # Reduce dimensions
            u = self._reduce_dimensions(u)
            v = self._reduce_dimensions(v)
            
            # Get coordinate names
            lon_name, lat_name = self.get_coordinate_names(u)
            
            # Calculate speed and direction
            speed = np.sqrt(u**2 + v**2)
            direction = np.degrees(np.arctan2(v, u)) % 360
            
            # Prepare data for feature generation
            lats = u[lat_name].values
            lons = u[lon_name].values
            speed_values = speed.values
            direction_values = direction.values
            u_values = u.values
            v_values = v.values
            
            def property_generator(i: int, j: int) -> Optional[Dict]:
                spd = float(speed_values[i, j])
                u_val = float(u_values[i, j])
                v_val = float(v_values[i, j])
                
                # Skip if any values are NaN
                if np.isnan(spd) or np.isnan(u_val) or np.isnan(v_val):
                    return None
                
                direction_val = float(direction_values[i, j])
                
                return {
                    "speed": round(spd, 2),
                    "direction": round(direction_val, 1),
                    "units": {
                        "speed": "m/s",
                        "direction": "degrees"
                    }
                }
            
            # Generate features using base class utility
            features = self._generate_features(lats, lons, property_generator)
            
            # Calculate ranges using base class utility
            data_dict = {
                "speed": (speed_values, "m/s"),
                "direction": (direction_values, "degrees")
            }
            
            ranges = self._calculate_ranges(data_dict)
            
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
            raise
