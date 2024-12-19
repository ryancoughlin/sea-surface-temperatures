from pathlib import Path
import logging
import datetime
import numpy as np
from typing import Optional, Dict, Tuple, List, Any, Final
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
import xarray as xr
from scipy.ndimage import maximum_filter, minimum_filter, gaussian_filter
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

# Constants for SSH contours
SSH_INTERVALS: Final[List[Tuple[float, float, float]]] = [
    (-2.0, -0.5, 0.25),   # Strong upwelling zones
    (-0.5, -0.2, 0.1),    # Moderate upwelling (finer resolution)
    (-0.2, 0.2, 0.05),    # Transition zones (finest resolution)
    (0.2, 0.5, 0.1),      # Moderate downwelling
    (0.5, 2.0, 0.25)      # Strong downwelling zones
]

KEY_LEVELS: Final[List[float]] = [-0.5, -0.2, 0.0, 0.2, 0.5]

BREAK_THRESHOLDS: Final[Dict[str, int]] = {
    'strong': 95,
    'moderate': 85,
    'weak': 0
}

MIN_CONTOUR_POINTS: Final[int] = 10
MIN_PATH_LENGTH: Final[float] = 0.1
MIN_SSH_DIFFERENCE: Final[float] = 0.05

def clean_value(value: Any) -> Optional[float]:
    """Convert NaN or invalid values to null, otherwise return the float value.
    
    Args:
        value: Value to clean
        
    Returns:
        Optional[float]: Cleaned value or None if invalid
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return float(value)

class WaterMovementConverter(BaseGeoJSONConverter):
    """Converts water movement data (currents, temperatures, etc.) to GeoJSON format."""
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert water movement data to GeoJSON format."""
        try:
            logger.info(f"Starting water movement conversion for {dataset} in {region}")
            
            if not isinstance(data, xr.Dataset):
                raise ValueError(f"Expected xarray Dataset, got {type(data)}")
            
            # Extract coordinates and variables
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            variables = self._extract_variables(data, dataset)
            features = self._generate_features(variables, lons, lats)
            
            if not features:
                logger.warning("No valid water movement data found")
                return self._save_empty_geojson(date, dataset, region)
            
            # Calculate ranges and create metadata
            ranges = self._calculate_ranges(variables)
            metadata = self._create_metadata(dataset, variables)
            
            # Create and save GeoJSON
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            logger.info(f"   └── Saving water movement GeoJSON to {output_path.name}")
            
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting water movement data to GeoJSON: {str(e)}")
            raise
            
    def _extract_variables(self, data: xr.Dataset, dataset: str) -> Dict[str, np.ndarray]:
        """Extract and process variables from dataset."""
        source_config = SOURCES[dataset]
        
        # Get variable names
        ssh_var = next(iter(source_config['source_datasets']['altimetry']['variables']))
        currents_vars = source_config['source_datasets']['currents']['variables']
        u_var = next(var for var, config in currents_vars.items() 
                    if config['type'] == 'current' and var.startswith('u'))
        v_var = next(var for var, config in currents_vars.items() 
                    if config['type'] == 'current' and var.startswith('v'))
        
        # Extract data
        ssh = data[ssh_var].values if ssh_var in data else None
        u_current = data[u_var].values
        v_current = data[v_var].values
        
        # Calculate derived fields
        current_magnitude = np.sqrt(u_current**2 + v_current**2)
        current_direction = np.degrees(np.arctan2(v_current, u_current))
        
        # Calculate SSH gradients and curvature for fishing features
        ssh_gradient = None
        ssh_curvature = None
        if ssh is not None:
            # Calculate gradients
            ssh_dx, ssh_dy = np.gradient(ssh)
            ssh_gradient = np.sqrt(ssh_dx**2 + ssh_dy**2)
            
            # Calculate curvature (second derivatives)
            ssh_dxx, _ = np.gradient(ssh_dx)
            _, ssh_dyy = np.gradient(ssh_dy)
            ssh_curvature = np.abs(ssh_dxx + ssh_dyy)
            
            # Smooth fields for better feature detection
            ssh_gradient = gaussian_filter(ssh_gradient, sigma=1.0)
            ssh_curvature = gaussian_filter(ssh_curvature, sigma=1.0)
        
        return {
            'ssh': ssh,
            'ssh_gradient': ssh_gradient,
            'ssh_curvature': ssh_curvature,
            'u_current': u_current,
            'v_current': v_current,
            'current_magnitude': current_magnitude,
            'current_direction': current_direction
        }
        
    def _generate_features(
        self,
        variables: Dict[str, np.ndarray],
        lons: np.ndarray,
        lats: np.ndarray
    ) -> List[Dict]:
        """Generate GeoJSON features from variables."""
        features = []
        valid_values = {
            'current_speed': [],
            'current_direction': [],
            'u_velocity': [],
            'v_velocity': []
        }
        
        if variables['ssh'] is not None:
            valid_values['ssh'] = []
            valid_values['ssh_gradient'] = []
        
        for i in range(len(lats)):
            for j in range(len(lons)):
                feature = self._create_point_feature(variables, i, j, lons[j], lats[i], valid_values)
                if feature:
                    features.append(feature)
                    
        return features
        
    def _create_point_feature(
        self,
        variables: Dict[str, np.ndarray],
        i: int,
        j: int,
        lon: float,
        lat: float,
        valid_values: Dict[str, List[float]]
    ) -> Optional[Dict]:
        """Create a GeoJSON feature for a single point with fishing-relevant properties."""
        u_val = float(variables['u_current'][i, j])
        v_val = float(variables['v_current'][i, j])
        
        if np.isnan(u_val) or np.isnan(v_val):
            return None
            
        magnitude = float(variables['current_magnitude'][i, j])
        direction = float(variables['current_direction'][i, j])
        
        # Store valid values
        valid_values['current_speed'].append(magnitude)
        valid_values['current_direction'].append(direction)
        valid_values['u_velocity'].append(u_val)
        valid_values['v_velocity'].append(v_val)
        
        properties = {
            "current_speed": round(magnitude, 3),
            "current_speed_unit": "m/s",
            "current_direction": round(direction, 1),
            "current_direction_unit": "degrees",
            "u_velocity": round(u_val, 3),
            "v_velocity": round(v_val, 3),
            "velocity_unit": "m/s"
        }
        
        # Add SSH and fishing-relevant data
        if variables['ssh'] is not None:
            ssh_value = float(variables['ssh'][i, j])
            if not np.isnan(ssh_value):
                properties.update({
                    "ssh": round(ssh_value, 3),
                    "ssh_unit": "m",
                })
                valid_values['ssh'].append(ssh_value)
                
                # Add gradient and curvature for feature detection
                if variables['ssh_gradient'] is not None:
                    gradient_value = float(variables['ssh_gradient'][i, j])
                    curvature_value = float(variables['ssh_curvature'][i, j])
                    
                    if not np.isnan(gradient_value) and not np.isnan(curvature_value):
                        properties.update({
                            "ssh_gradient": round(gradient_value, 4),
                            "ssh_curvature": round(curvature_value, 4),
                            "feature_type": self._classify_ocean_feature(
                                ssh_value,
                                gradient_value,
                                curvature_value,
                                magnitude
                            )
                        })
                        valid_values['ssh_gradient'].append(gradient_value)
        
        return {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },
            "properties": properties
        }
        
    def _classify_ocean_feature(
        self,
        ssh: float,
        gradient: float,
        curvature: float,
        current_speed: float
    ) -> str:
        """Classify ocean features relevant for fishing.
        
        Args:
            ssh: Sea surface height value
            gradient: SSH gradient magnitude
            curvature: SSH curvature magnitude
            current_speed: Current speed
            
        Returns:
            str: Feature classification
        """
        # Thresholds for feature classification
        HIGH_GRADIENT = 0.00015  # High SSH gradient
        HIGH_CURVATURE = 0.0001  # High SSH curvature
        STRONG_CURRENT = 0.5     # Strong current (m/s)
        
        if ssh < -0.2 and gradient > HIGH_GRADIENT:
            return "upwelling_zone"
        elif ssh > 0.2 and gradient > HIGH_GRADIENT:
            return "convergence_zone"
        elif curvature > HIGH_CURVATURE and current_speed > STRONG_CURRENT:
            return "current_break"
        elif abs(ssh) < 0.1 and gradient > HIGH_GRADIENT:
            return "frontal_zone"
        else:
            return "background"
        
    def _calculate_ranges(self, variables: Dict[str, np.ndarray]) -> Dict[str, Dict[str, Any]]:
        """Calculate ranges for all variables."""
        ranges = {}
        
        # Define variable configurations
        var_configs = {
            'current_speed': {'unit': 'm/s'},
            'u_velocity': {'unit': 'm/s'},
            'v_velocity': {'unit': 'm/s'},
            'current_direction': {'unit': 'degrees'},
            'ssh': {'unit': 'm'},
            'ssh_gradient': {'unit': 'm/degree'}
        }
        
        for var_name, config in var_configs.items():
            if var_name in variables and variables[var_name] is not None:
                valid_data = variables[var_name][~np.isnan(variables[var_name])]
                if len(valid_data) > 0:
                    ranges[var_name] = {
                        'min': float(np.min(valid_data)),
                        'max': float(np.max(valid_data)),
                        'unit': config['unit']
                    }
        
        return ranges
        
    def _create_metadata(self, dataset: str, variables: Dict[str, np.ndarray]) -> Dict[str, Any]:
        """Create metadata for the GeoJSON."""
        source_config = SOURCES[dataset]
        return {
            "source": dataset,
            "variables": {
                "altimetry": list(source_config['source_datasets']['altimetry']['variables'].keys()),
                "currents": list(source_config['source_datasets']['currents']['variables'].keys())
            }
        }
        
    def _save_empty_geojson(self, date: datetime, dataset: str, region: str) -> Path:
        """Save an empty GeoJSON when no valid data is found."""
        empty_geojson = {"type": "FeatureCollection", "features": []}
        return self.save_geojson(
            empty_geojson,
            self.path_manager.get_asset_paths(date, dataset, region).data
        )