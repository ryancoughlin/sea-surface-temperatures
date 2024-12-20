from pathlib import Path
import logging
import datetime
import numpy as np
import matplotlib.pyplot as plt
from .base_converter import BaseGeoJSONConverter
import xarray as xr
from shapely.geometry import LineString

logger = logging.getLogger(__name__)

def clean_value(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return float(value)

class WaterMovementContourConverter(BaseGeoJSONConverter):
    """Creates contour lines showing water movement patterns."""
    
    def _generate_levels(self, min_ssh: float, max_ssh: float) -> np.ndarray:
        """Generate contour levels based on data statistics."""
        # Calculate mean and range
        ssh_mean = (max_ssh + min_ssh) / 2
        ssh_range = max_ssh - min_ssh
        
        # Use range/4 as our effective "standard deviation"
        ssh_std = ssh_range / 4
        
        # Generate levels based on deviations from mean
        base_levels = np.array([
            ssh_mean - ssh_std,
            ssh_mean - 0.5 * ssh_std,
            ssh_mean,
            ssh_mean + 0.5 * ssh_std,
            ssh_mean + ssh_std
        ])
        
        # Only use levels within our data range
        levels = base_levels[(base_levels >= min_ssh) & (base_levels <= max_ssh)]
        return levels

    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert water movement data to contour GeoJSON format."""
        try:
            # Get SSH data
            ssh = data['sea_surface_height'].values
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            # Get valid SSH range
            valid_ssh = ssh[~np.isnan(ssh)]
            if len(valid_ssh) == 0:
                logger.warning("⚠️  No valid SSH data points found")
                return self._create_geojson([], date, None, None)
            
            min_ssh = float(np.min(valid_ssh))
            max_ssh = float(np.max(valid_ssh))
            
            # Generate contours if we have sufficient data
            features = []
            if len(valid_ssh) >= 10 and (max_ssh - min_ssh) >= 0.05:
                try:
                    levels = self._generate_levels(min_ssh, max_ssh)
                    logger.info(f"   ├── Using {len(levels)} contour levels")
                    
                    # Generate contours
                    fig, ax = plt.subplots(figsize=(10, 10))
                    contour_set = ax.contour(
                        lons, lats, ssh,
                        levels=levels,
                        linestyles='-',
                        linewidths=2.0,
                        colors='black'
                    )
                    plt.close(fig)
                    
                    # Process contours
                    valid_segments = 0
                    for level_idx, level in enumerate(contour_set.levels):
                        segments = contour_set.allsegs[level_idx]
                        for segment in segments:
                            if len(segment) >= 3:
                                coords = [[float(x), float(y)] for x, y in segment 
                                         if not (np.isnan(x) or np.isnan(y))]
                                
                                if len(coords) >= 3:
                                    path_length = float(LineString(coords).length)
                                    if path_length >= 0.1:
                                        valid_segments += 1
                                        features.append({
                                            "type": "Feature",
                                            "geometry": {
                                                "type": "LineString",
                                                "coordinates": coords
                                            },
                                            "properties": {
                                                "value": clean_value(level),
                                                "unit": "meters",
                                                "path_length_nm": round(path_length * 60, 1),
                                                "lineStyle": "solid"
                                            }
                                        })                    
                except Exception as e:
                    logger.error(f"❌ Failed to generate contours: {str(e)}")
                    return self._create_geojson([], date, min_ssh, max_ssh)
            else:
                logger.warning(f"❌  Insufficient data for contours: {len(valid_ssh)} points, range: {max_ssh - min_ssh:.3f}m")
            
            # Create and save GeoJSON
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "value_range": {
                        "min": clean_value(min_ssh),
                        "max": clean_value(max_ssh)
                    }
                }
            }
            
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            return self.save_geojson(geojson, asset_paths.contours)
            
        except Exception as e:
            logger.error(f"❌ Failed to create water movement contours: {str(e)}")
            raise

    def _create_geojson(self, features, date, min_ssh, max_ssh):
        """Create a GeoJSON object with consistent structure."""
        return {
            "type": "FeatureCollection",
            "features": features,
            "properties": {
                "date": date.strftime('%Y-%m-%d'),
                "value_range": {
                    "min": clean_value(min_ssh),
                    "max": clean_value(max_ssh)
                }
            }
        }