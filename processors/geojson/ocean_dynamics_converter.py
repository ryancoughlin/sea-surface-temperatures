from pathlib import Path
import logging
import datetime
import numpy as np
from typing import Optional, Dict, Tuple, List
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
import xarray as xr
from scipy.ndimage import maximum_filter, minimum_filter
import matplotlib.pyplot as plt


logger = logging.getLogger(__name__)

def clean_value(value):
    """Convert NaN or invalid values to null, otherwise return the float value."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return float(value)

class OceanDynamicsGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert ocean dynamics data to GeoJSON format with raw values."""
        try:
            logger.info(f"Starting ocean dynamics conversion for {dataset} in {region}")
            
            # Extract variables from merged dataset
            if not isinstance(data, xr.Dataset):
                raise ValueError(f"Expected xarray Dataset, got {type(data)}")
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            # Extract variables based on SOURCES configuration
            source_config = SOURCES[dataset]
            altimetry_vars = source_config['source_datasets']['altimetry']['variables']
            currents_vars = source_config['source_datasets']['currents']['variables']
            
            # Get SSH data
            ssh_var = next(var for var in altimetry_vars.keys())
            ssh = data[ssh_var].values if ssh_var in data else None
            
            # Get current components
            u_var = next(var for var, config in currents_vars.items() if config['type'] == 'current' and var.startswith('u'))
            v_var = next(var for var, config in currents_vars.items() if config['type'] == 'current' and var.startswith('v'))
            
            u_current = data[u_var].values
            v_current = data[v_var].values
            
            # Calculate derived fields
            current_magnitude = np.sqrt(u_current**2 + v_current**2)
            current_direction = np.degrees(np.arctan2(v_current, u_current))
            
            # Calculate SSH gradients if SSH data is available
            if ssh is not None:
                ssh_dx, ssh_dy = np.gradient(ssh)
                ssh_gradient = np.sqrt(ssh_dx**2 + ssh_dy**2)
            else:
                ssh_gradient = None
            
            # Create features list
            features = []
            valid_values = {
                'current_speed': [],
                'current_direction': [],
                'u_velocity': [],
                'v_velocity': []
            }
            
            if ssh is not None:
                valid_values['ssh'] = []
                valid_values['ssh_gradient'] = []
            
            # Generate point features
            for i in range(len(lats)):
                for j in range(len(lons)):
                    u_val = float(u_current[i, j])
                    v_val = float(v_current[i, j])
                    
                    # Skip if current values are NaN
                    if np.isnan(u_val) or np.isnan(v_val):
                        continue
                        
                    magnitude = float(current_magnitude[i, j])
                    direction = float(current_direction[i, j])
                    
                    # Store valid values for range calculation
                    valid_values['current_speed'].append(magnitude)
                    valid_values['current_direction'].append(direction)
                    valid_values['u_velocity'].append(u_val)
                    valid_values['v_velocity'].append(v_val)
                    
                    # Create feature properties
                    properties = {
                        "current_speed": round(magnitude, 3),
                        "current_speed_unit": "m/s",
                        "current_direction": round(direction, 1),
                        "current_direction_unit": "degrees",
                        "u_velocity": round(u_val, 3),
                        "v_velocity": round(v_val, 3),
                        "velocity_unit": "m/s"
                    }
                    
                    # Add SSH data if available
                    if ssh is not None:
                        ssh_value = float(ssh[i, j])
                        if not np.isnan(ssh_value):
                            properties["ssh"] = round(ssh_value, 3)
                            properties["ssh_unit"] = "m"
                            valid_values['ssh'].append(ssh_value)
                            
                            if ssh_gradient is not None:
                                gradient_value = float(ssh_gradient[i, j])
                                if not np.isnan(gradient_value):
                                    properties["ssh_gradient"] = round(gradient_value, 4)
                                    valid_values['ssh_gradient'].append(gradient_value)
                    
                    # Create feature
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(lons[j]), float(lats[i])]
                        },
                        "properties": properties
                    }
                    features.append(feature)
            
            if not features:
                logger.warning("No valid ocean dynamics data found")
                empty_geojson = {"type": "FeatureCollection", "features": []}
                return self.save_geojson(
                    empty_geojson,
                    self.path_manager.get_asset_paths(date, dataset, region).data
                )
            
            # Calculate ranges
            ranges = {}
            for var_name, values in valid_values.items():
                if values:
                    ranges[var_name] = {
                        "min": float(min(values)),
                        "max": float(max(values)),
                        "unit": "m" if var_name == "ssh" else 
                               "m/s" if var_name in ["current_speed", "u_velocity", "v_velocity"] else
                               "degrees" if var_name == "current_direction" else
                               "m/degree"
                    }
            
            # Add metadata
            metadata = {
                "source": dataset,
                "variables": {
                    "altimetry": list(altimetry_vars.keys()),
                    "currents": list(currents_vars.keys())
                }
            }
            
            # Create standardized GeoJSON
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            # Save and return path
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            logger.info(f"   └── Saving ocean dynamics GeoJSON to {output_path.name}")
            
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting ocean dynamics data to GeoJSON: {str(e)}")
            raise

class OceanDynamicsContourConverter(BaseGeoJSONConverter):
    """Converter for SSH contours in ocean dynamics data."""
    
    # Define contour intervals optimized for fishing
    SSH_INTERVALS = [
        (-2.0, -0.5, 0.25),   # Strong upwelling zones
        (-0.5, -0.2, 0.1),    # Moderate upwelling (finer resolution)
        (-0.2, 0.2, 0.05),    # Transition zones (finest resolution)
        (0.2, 0.5, 0.1),      # Moderate downwelling
        (0.5, 2.0, 0.25)      # Strong downwelling zones
    ]
    
    # Key levels for fishing (transition zones and strong gradients)
    KEY_LEVELS = [-0.5, -0.2, 0.0, 0.2, 0.5]
    
    BREAK_THRESHOLDS = {'strong': 95, 'moderate': 85, 'weak': 0}

    def _generate_levels(self, min_ssh: float, max_ssh: float) -> np.ndarray:
        """Generate SSH contour levels."""
        levels = []
        for start, end, interval in self.SSH_INTERVALS:
            if max_ssh >= start and min_ssh <= end:
                range_start = max(start, np.floor(min_ssh))
                range_end = min(end, np.ceil(max_ssh))
                levels.extend(np.arange(range_start, range_end, interval))
        return np.unique(levels)

    def _process_gradient_data(self, gradient_data: np.ndarray, segment: np.ndarray) -> Tuple[Optional[float], Optional[float], str]:
        """Process gradient data for a contour segment."""
        if gradient_data is None:
            return None, None, 'weak'
            
        valid_gradients = gradient_data[~np.isnan(gradient_data)]
        if len(valid_gradients) == 0:
            return None, None, 'weak'
            
        avg_gradient = float(np.mean(valid_gradients))
        max_gradient = float(np.max(valid_gradients))
        
        # Determine break strength
        for strength, threshold in self.BREAK_THRESHOLDS.items():
            if avg_gradient > np.percentile(valid_gradients, threshold):
                return avg_gradient, max_gradient, strength
                
        return avg_gradient, max_gradient, 'weak'

    def _calculate_path_length(self, segment: np.ndarray) -> float:
        """Calculate the length of a contour segment in degrees."""
        point_differences = np.diff(segment, axis=0)
        squared_distances = np.sum(point_differences**2, axis=1)
        return float(np.sum(np.sqrt(squared_distances)))

    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert SSH data to contour GeoJSON format."""
        try:
            logger.info(f"Starting SSH contour conversion for {dataset} in {region}")
            
            # Extract SSH data from merged dataset
            if not isinstance(data, xr.Dataset):
                raise ValueError(f"Expected xarray Dataset, got {type(data)}")
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(data)
            
            # Get SSH variable name from config
            source_config = SOURCES[dataset]
            altimetry_vars = source_config['source_datasets']['altimetry']['variables']
            ssh_var = next(var for var in altimetry_vars.keys())
            
            # Extract SSH values
            if ssh_var not in data:
                logger.warning(f"SSH variable {ssh_var} not found in dataset")
                return self._create_geojson([], date, None, None)
                
            ssh = data[ssh_var].values
            
            # Get valid SSH values
            valid_ssh = ssh[~np.isnan(ssh)]
            if len(valid_ssh) == 0:
                logger.warning("No valid SSH data found")
                return self._create_geojson([], date, None, None)
            
            min_ssh = float(np.min(valid_ssh))
            max_ssh = float(np.max(valid_ssh))
            
            logger.info(f"Processing SSH contours for {date} with min: {min_ssh}, max: {max_ssh}")
            
            # Calculate SSH gradients for break strength
            ssh_dx, ssh_dy = np.gradient(ssh)
            gradient_magnitude = np.sqrt(ssh_dx**2 + ssh_dy**2)
            
            features = []
            if len(valid_ssh) >= 10 and (max_ssh - min_ssh) >= 0.05:  # At least 5cm difference
                try:
                    # Generate contours
                    levels = self._generate_levels(min_ssh, max_ssh)
                    fig, ax = plt.subplots(figsize=(10, 10))
                    contour_set = ax.contour(
                        data[lon_name],
                        data[lat_name],
                        ssh,
                        levels=levels
                    )
                    plt.close(fig)
                    
                    # Create features from contours
                    for level_idx, level_value in enumerate(contour_set.levels):
                        for segment in contour_set.allsegs[level_idx]:
                            # Skip segments that are too short
                            if len(segment) < 10:
                                continue
                                
                            # Skip segments with small geographical extent
                            path_length = self._calculate_path_length(segment)
                            if path_length < 0.1:  # 0.1 degrees minimum length
                                continue
                            
                            # Process gradient data
                            avg_gradient, max_gradient, strength = self._process_gradient_data(
                                gradient_magnitude, segment
                            )
                            
                            gradient_info = {}
                            if avg_gradient is not None:
                                gradient_info.update({
                                    "avg_gradient": round(avg_gradient, 4),
                                    "max_gradient": round(max_gradient, 4),
                                    "strength": strength
                                })
                            
                            feature = {
                                "type": "Feature",
                                "geometry": {
                                    "type": "LineString",
                                    "coordinates": [[float(x), float(y)] for x, y in segment]
                                },
                                "properties": {
                                    "value": clean_value(level_value),
                                    "unit": "meters",
                                    "is_key_level": level_value in self.KEY_LEVELS,
                                    **gradient_info
                                }
                            }
                            features.append(feature)
                            
                except Exception as e:
                    logger.warning(f"Could not generate contours: {str(e)}")
            
            # Create and save GeoJSON
            geojson = self._create_geojson(features, date, min_ssh, max_ssh)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            return self.save_geojson(geojson, asset_paths.contours)
            
        except Exception as e:
            logger.error(f"Error converting SSH to contour GeoJSON: {str(e)}")
            raise

    def _create_geojson(self, features: List[Dict], date: datetime, min_ssh: Optional[float], max_ssh: Optional[float]) -> Dict:
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