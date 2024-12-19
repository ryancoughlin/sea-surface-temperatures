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

class OceanDynamicsContourConverter(BaseGeoJSONConverter):
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
        logger.info(f"Using {len(levels)} SSH contour levels:")
        for level in levels:
            logger.info(f"  Level: {level:.3f}m ({(level - ssh_mean)/ssh_std:.1f} std)")
        return levels

    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        try:
            # Log input data structure
            logger.info("Dataset variables:")
            for var in data.variables:
                logger.info(f"  - {var}: {data[var].shape}")

            # Get SSH data
            ssh = data['sea_surface_height'].values
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            # Log data shapes and ranges
            logger.info(f"Data ranges:")
            logger.info(f"  Longitude: {np.min(lons):.3f} to {np.max(lons):.3f}")
            logger.info(f"  Latitude: {np.min(lats):.3f} to {np.max(lats):.3f}")
            logger.info(f"  SSH shape: {ssh.shape}")
            
            # Check for NaN values
            nan_count = np.isnan(ssh).sum()
            total_points = ssh.size
            logger.info(f"NaN analysis:")
            logger.info(f"  Total points: {total_points}")
            logger.info(f"  NaN points: {nan_count}")
            logger.info(f"  Valid points: {total_points - nan_count}")
            
            # Get valid SSH range
            valid_ssh = ssh[~np.isnan(ssh)]
            if len(valid_ssh) == 0:
                logger.warning("No valid SSH data points found")
                return self._create_geojson([], date, None, None)
            
            min_ssh = float(np.min(valid_ssh))
            max_ssh = float(np.max(valid_ssh))
            logger.info(f"SSH value range: {min_ssh:.3f}m to {max_ssh:.3f}m")
            
            # Log histogram information
            hist, bins = np.histogram(valid_ssh, bins=20)
            logger.info("SSH distribution (20 bins):")
            for i, (start, end, count) in enumerate(zip(bins[:-1], bins[1:], hist)):
                logger.info(f"  Bin {i+1}: {start:.3f}m to {end:.3f}m: {count} points")
            
            # Generate contours if we have sufficient data
            if len(valid_ssh) >= 10 and (max_ssh - min_ssh) >= 0.05:
                try:
                    levels = self._generate_levels(min_ssh, max_ssh)
                    
                    # Generate contours
                    fig, ax = plt.subplots(figsize=(10, 10))
                    try:
                        contour_set = ax.contour(
                            lons, lats, ssh,
                            levels=levels,
                            linestyles='-',  # Explicit solid line style
                            linewidths=2.0,  # Thicker lines for major trends
                            colors='black'
                        )
                        logger.info("Successfully created contour set")
                    except Exception as ce:
                        logger.error(f"Failed to create contours: {str(ce)}")
                        raise
                    finally:
                        plt.close(fig)
                    
                    # Process contours
                    features = []
                    for level_idx, level in enumerate(contour_set.levels):
                        segments = contour_set.allsegs[level_idx]
                        logger.info(f"Level {level:.3f}m:")
                        logger.info(f"  - Found {len(segments)} segments")
                        valid_segments = 0
                        
                        for segment in segments:
                            # Only keep segments with enough points
                            if len(segment) < 3:
                                continue
                                
                            coords = [[float(x), float(y)] for x, y in segment 
                                     if not (np.isnan(x) or np.isnan(y))]
                            
                            if len(coords) < 3:
                                continue
                                
                            # Keep only longer segments for major trends
                            path_length = float(LineString(coords).length)
                            if path_length < 0.1:  # Increased minimum length
                                continue
                                
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
                                    "lineStyle": "solid"  # Explicitly mark as solid
                                }
                            })
                        logger.info(f"  - Kept {valid_segments} valid segments")
                    
                    logger.info(f"Final feature count: {len(features)}")
                    
                except Exception as e:
                    logger.error(f"Error in contour generation: {str(e)}")
                    logger.exception(e)
                    return self._create_geojson([], date, min_ssh, max_ssh)
            else:
                logger.warning(f"Insufficient data: {len(valid_ssh)} points, range: {max_ssh - min_ssh:.3f}m")
            
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
            logger.error(f"Error converting SSH data to contour GeoJSON: {str(e)}")
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