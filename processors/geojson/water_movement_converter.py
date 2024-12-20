from pathlib import Path
import logging
import datetime
import numpy as np
import xarray as xr
from typing import Dict, List
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class WaterMovementConverter(BaseGeoJSONConverter):
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert water movement data to GeoJSON format with current speed, direction and SSH."""
        try:
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            # Get the raw variables
            ssh = data['sea_surface_height'].values if 'sea_surface_height' in data else None
            u_current = data['uo'].values
            v_current = data['vo'].values
            
            # Create features list
            features = []
            
            # Generate a feature for each point
            for i in range(len(lats)):
                for j in range(len(lons)):
                    # Get values at this point
                    u = float(u_current[i, j])
                    v = float(v_current[i, j])
                    
                    # Skip if currents are invalid
                    if np.isnan(u) or np.isnan(v):
                        continue
                        
                    # Calculate current speed and direction
                    speed = float(np.sqrt(u**2 + v**2))
                    direction = float(np.degrees(np.arctan2(v, u)))
                    
                    # Create properties
                    properties = {
                        "current_speed": round(speed, 3),
                        "current_direction": round(direction, 1),
                        "current_speed_unit": "m/s",
                        "current_direction_unit": "degrees"
                    }
                    
                    # Add SSH if available
                    if ssh is not None:
                        ssh_value = float(ssh[i, j])
                        if not np.isnan(ssh_value):
                            properties["ssh"] = round(ssh_value, 3)
                            properties["ssh_unit"] = "m"
                    
                    # Create the feature
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(lons[j]), float(lats[i])]
                        },
                        "properties": properties
                    }
                    
                    features.append(feature)
            
            # Create the GeoJSON
            geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            # Save and return
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting water movement data: {str(e)}")
            raise