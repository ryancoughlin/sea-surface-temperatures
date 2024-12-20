import xarray as xr
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Union
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class CurrentsGeoJSONConverter(BaseGeoJSONConverter):
    """Converts ocean current data to GeoJSON format with speed and direction"""
    
    def convert(self, data: Union[xr.Dataset, xr.DataArray], region: str, dataset: str, date: datetime) -> Path:
        """Convert current data to GeoJSON format."""
        try:
            # Extract current components from dataset
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                u_var = next(var for var, config in variables.items() if config['type'] == 'current' and var.startswith('u'))
                v_var = next(var for var, config in variables.items() if config['type'] == 'current' and var.startswith('v'))
                logger.info(f"Using current variables: u={u_var}, v={v_var}")
                
                u_current = data[u_var]
                v_current = data[v_var]
            else:
                # Assume it's already the u component and v is not available
                logger.warning("Received DataArray instead of Dataset - assuming u component")
                u_current = data
                v_current = None
            
            # Reduce dimensions
            u_current = self._reduce_dimensions(u_current)
            if v_current is not None:
                v_current = self._reduce_dimensions(v_current)
            
            # Get coordinate names
            longitude, latitude = self.get_coordinate_names(u_current)
            
            # Calculate speed and direction
            if v_current is not None:
                logger.info("Calculating current speed and direction")
                speed = np.sqrt(u_current**2 + v_current**2)
                direction = np.degrees(np.arctan2(v_current, u_current)) % 360
            else:
                logger.warning("No v-component available - using only u magnitude")
                speed = abs(u_current)
                direction = np.zeros_like(u_current)
            
            # Prepare data for feature generation
            lats = u_current[latitude].values
            lons = u_current[longitude].values
            speed_values = speed.values
            direction_values = direction.values
            u_values = u_current.values
            v_values = v_current.values if v_current is not None else np.zeros_like(u_values)
            
            def property_generator(i: int, j: int) -> Optional[Dict]:
                spd = float(speed_values[i, j])
                u_val = float(u_values[i, j])
                v_val = float(v_values[i, j])
                
                # Skip if any values are NaN
                if np.isnan(spd) or np.isnan(u_val) or np.isnan(v_val):
                    return None
                
                direction_val = float(direction_values[i, j])
                
                return {
                    "speed": round(spd, 3),
                    "direction": round(direction_val, 1),
                    "components": {
                        "u": round(u_val, 3),
                        "v": round(v_val, 3)
                    },
                    "units": {
                        "speed": "m/s",
                        "direction": "degrees"
                    }
                }
            
            # Generate features using base class utility
            features = self._generate_features(lats, lons, property_generator)
            logger.info(f"Generated {len(features)} features")
            
            # Calculate ranges using base class utility
            data_dict = {
                "speed": (speed_values, "m/s"),
                "direction": (direction_values, "degrees"),
                "u": (u_values, "m/s"),
                "v": (v_values, "m/s")
            }
            
            ranges = self._calculate_ranges(data_dict)
            
            # Update metadata
            metadata = {
                "source": dataset,
                "variables": list(SOURCES[dataset]['variables']),
                "available_components": ["u", "v"] if v_current is not None else ["u"],
                "units": {
                    "speed": "m/s",
                    "direction": "degrees",
                    "components": "m/s"
                }
            }
            
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            logger.info(f"Saving currents GeoJSON to {output_path.name}")
            
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting currents to GeoJSON: {str(e)}", exc_info=True)
            raise
