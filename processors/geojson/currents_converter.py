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
        """Convert currents data to GeoJSON with enhanced vector properties."""
        try:
            # Load and configure
            ds = xr.open_dataset(data_path)
            bounds = REGIONS[region]['bounds']
            dataset_config = SOURCES[dataset]
            export_config = dataset_config.get('export_geojson', {})
            
            # Configuration
            decimation = export_config.get('decimation_factor', 3)  # Increased density
            vector_scale = export_config.get('vector_scale', 50)
            min_magnitude = export_config.get('min_magnitude', 0.05)  # Lower threshold
            
            # Create regional subset
            ds_subset = ds.sel(
                longitude=slice(bounds[0][0], bounds[1][0]),
                latitude=slice(bounds[0][1], bounds[1][1]),
                time=ds.time[0]
            )
            
            # Get current components
            u = ds_subset.u_current
            v = ds_subset.v_current
            
            # Calculate derived values
            speed = np.sqrt(u**2 + v**2)
            direction = np.degrees(np.arctan2(v, u)) % 360  # 0-360 degrees
            
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
                    
                    # Skip NaN values or values below threshold
                    if np.isnan(spd) or spd < min_magnitude:
                        continue
                    
                    # Ensure no NaN values in vector components
                    u_val = float(u_array[j, i])
                    v_val = float(v_array[j, i])
                    if np.isnan(u_val) or np.isnan(v_val):
                        continue
                    
                    # Calculate normalized vector components
                    u_norm = u_val / spd
                    v_norm = v_val / spd
                    
                    # Convert any remaining NaN values to null for JSON compatibility
                    direction_val = float(direction_array[j, i])
                    direction_val = None if np.isnan(direction_val) else direction_val

                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(lon_array[i]), float(lat_array[j])]
                        },
                        "properties": {
                            # Vector components for arrow rendering
                            "u": u_val * vector_scale,
                            "v": v_val * vector_scale,
                            
                            # Normalized components for streamline calculation
                            "u_norm": u_norm,
                            "v_norm": v_norm,
                            
                            # Additional properties for visualization
                            "speed": spd,
                            "direction": direction_val,
                            
                            # Animation helpers
                            "speed_normalized": min(1.0, spd / 2.0),  # Normalize 0-1
                            "vector_magnitude": spd * vector_scale
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
            
            # Save to file
            output_path = self.generate_geojson_path(region, dataset, timestamp)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(geojson, f)
                
            logger.info(f"Saved enhanced currents GeoJSON to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting currents to GeoJSON: {str(e)}")
            raise
