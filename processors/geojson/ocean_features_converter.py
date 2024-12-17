from pathlib import Path
import logging
import datetime
import numpy as np
from typing import Optional, Dict, List, Tuple
from .base_converter import BaseGeoJSONConverter
import xarray as xr
from scipy.ndimage import maximum_filter, minimum_filter, gaussian_filter
from config.settings import SOURCES
logger = logging.getLogger(__name__)

class OceanFeaturesConverter(BaseGeoJSONConverter):
    """Detects and converts ocean features (eddies, extrema, etc.) to GeoJSON format."""
    
    def _detect_eddies(self, ssh: np.ndarray, u_current: np.ndarray, v_current: np.ndarray,
                      lons: np.ndarray, lats: np.ndarray, neighborhood_size: int = 10) -> List[Dict]:
        """Detect eddies using SSH and current data."""
        features = []
        
        # Calculate vorticity (curl of velocity field)
        dvdx = np.gradient(v_current, axis=1)
        dudy = np.gradient(u_current, axis=0)
        vorticity = dvdx - dudy
        
        # Calculate SSH gradients
        ssh_dx, ssh_dy = np.gradient(ssh)
        ssh_grad_magnitude = np.sqrt(ssh_dx**2 + ssh_dy**2)
        
        # Smooth vorticity field
        vorticity_smooth = gaussian_filter(vorticity, sigma=1.0)
        
        # Thresholds
        vorticity_threshold = float(np.std(vorticity_smooth) * 1.5)
        grad_threshold = float(np.percentile(ssh_grad_magnitude[~np.isnan(ssh_grad_magnitude)], 75))
        
        # Find local maxima/minima in vorticity field
        max_filtered = maximum_filter(vorticity_smooth, size=neighborhood_size)
        min_filtered = minimum_filter(vorticity_smooth, size=neighborhood_size)
        
        # Detect cyclonic eddies (negative vorticity)
        cyclonic_centers = (vorticity_smooth == min_filtered) & (vorticity_smooth < -vorticity_threshold)
        cyclonic_points = np.where(cyclonic_centers & (ssh_grad_magnitude > grad_threshold))
        
        for y, x in zip(*cyclonic_points):
            if not np.isnan(ssh[y, x]):
                radius = float(self._estimate_eddy_radius(ssh, x, y))
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [float(lons[x]), float(lats[y])]
                    },
                    'properties': {
                        'feature_type': 'cyclonic_eddy',
                        'ssh': float(ssh[y, x]),
                        'vorticity': float(vorticity_smooth[y, x]),
                        'radius_degrees': radius,
                        'description': 'Cyclonic Eddy (counterclockwise)',
                        'display_text': 'Cyclonic Eddy'
                    }
                })
        
        # Detect anticyclonic eddies (positive vorticity)
        anticyclonic_centers = (vorticity_smooth == max_filtered) & (vorticity_smooth > vorticity_threshold)
        anticyclonic_points = np.where(anticyclonic_centers & (ssh_grad_magnitude > grad_threshold))
        
        for y, x in zip(*anticyclonic_points):
            if not np.isnan(ssh[y, x]):
                radius = float(self._estimate_eddy_radius(ssh, x, y))
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [float(lons[x]), float(lats[y])]
                    },
                    'properties': {
                        'feature_type': 'anticyclonic_eddy',
                        'ssh': float(ssh[y, x]),
                        'vorticity': float(vorticity_smooth[y, x]),
                        'radius_degrees': radius,
                        'description': 'Anticyclonic Eddy (clockwise)',
                        'display_text': 'Anticyclonic Eddy'
                    }
                })
        
        return features
    
    def _estimate_eddy_radius(self, ssh: np.ndarray, center_x: int, center_y: int, 
                            max_radius: int = 20) -> float:
        """Estimate eddy radius by analyzing SSH gradient."""
        center_value = ssh[center_y, center_x]
        max_gradient = 0
        radius = 1
        
        for r in range(1, min(max_radius, min(ssh.shape) // 2)):
            y_slice = slice(max(0, center_y - r), min(ssh.shape[0], center_y + r + 1))
            x_slice = slice(max(0, center_x - r), min(ssh.shape[1], center_x + r + 1))
            
            ring = ssh[y_slice, x_slice]
            gradient = np.abs(ring - center_value).mean()
            
            if gradient > max_gradient:
                max_gradient = gradient
                radius = r
            elif gradient < max_gradient * 0.5:  # Gradient dropped significantly
                break
        
        return radius
    
    def _find_extrema(self, ssh: np.ndarray, lons: np.ndarray, lats: np.ndarray, 
                     neighborhood_size: int = 10) -> List[Dict]:
        """Find significant SSH extrema."""
        features = []
        
        # Use maximum/minimum filters to find local extrema
        max_filtered = maximum_filter(ssh, size=neighborhood_size)
        min_filtered = minimum_filter(ssh, size=neighborhood_size)
        
        # Find global extrema
        global_max = float(np.nanmax(ssh))
        global_min = float(np.nanmin(ssh))
        
        # Find locations of maxima
        maxima = (ssh == max_filtered) & (ssh > np.percentile(ssh[~np.isnan(ssh)], 95))
        max_points = np.where(maxima)
        
        for y, x in zip(*max_points):
            ssh_value = float(ssh[y, x])
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(lons[x]), float(lats[y])]
                },
                'properties': {
                    'feature_type': 'ssh_maximum',
                    'value': ssh_value,
                    'is_global_max': bool(np.isclose(ssh_value, global_max)),
                    'description': f'High SSH: {ssh_value:.2f}m',
                    'display_text': f'High SSH\n{ssh_value:.2f}m'
                }
            })
        
        # Find locations of minima
        minima = (ssh == min_filtered) & (ssh < np.percentile(ssh[~np.isnan(ssh)], 5))
        min_points = np.where(minima)
        
        for y, x in zip(*min_points):
            ssh_value = float(ssh[y, x])
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(lons[x]), float(lats[y])]
                },
                'properties': {
                    'feature_type': 'ssh_minimum',
                    'value': ssh_value,
                    'is_global_min': bool(np.isclose(ssh_value, global_min)),
                    'description': f'Low SSH: {ssh_value:.2f}m',
                    'display_text': f'Low SSH\n{ssh_value:.2f}m'
                }
            })
        
        return features
    
    def _find_upwelling_zones(self, ssh: np.ndarray, lons: np.ndarray, lats: np.ndarray,
                             neighborhood_size: int = 20) -> List[Dict]:
        """Find significant upwelling/downwelling zones."""
        features = []
        
        # Calculate SSH gradients
        ssh_dx, ssh_dy = np.gradient(ssh)
        gradient_magnitude = np.sqrt(ssh_dx**2 + ssh_dy**2)
        
        # Smooth SSH for zone detection
        ssh_smooth = gaussian_filter(ssh, sigma=2.0)
        
        # Find strong upwelling zones (significant negative SSH)
        upwelling = (ssh_smooth < -0.5) & (gradient_magnitude > np.percentile(gradient_magnitude[~np.isnan(gradient_magnitude)], 75))
        up_points = np.where(upwelling)
        
        # Group nearby points and find centers
        processed_points = set()
        
        for y, x in zip(*up_points):
            if (y, x) not in processed_points:
                # Find local center
                y_start, y_end = max(0, y - neighborhood_size//2), min(ssh.shape[0], y + neighborhood_size//2)
                x_start, x_end = max(0, x - neighborhood_size//2), min(ssh.shape[1], x + neighborhood_size//2)
                
                region = ssh_smooth[y_start:y_end, x_start:x_end]
                if region.size > 0:
                    local_min_idx = np.unravel_index(np.argmin(region), region.shape)
                    center_y = y_start + local_min_idx[0]
                    center_x = x_start + local_min_idx[1]
                    
                    ssh_value = float(ssh[center_y, center_x])
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(lons[center_x]), float(lats[center_y])]
                        },
                        'properties': {
                            'feature_type': 'upwelling_zone',
                            'ssh': ssh_value,
                            'strength': 'strong' if ssh_value < -0.8 else 'moderate',
                            'description': f'Upwelling Zone: {ssh_value:.2f}m',
                            'display_text': 'Upwelling\nZone'
                        }
                    })
                    
                    # Mark region as processed
                    for dy in range(-neighborhood_size//2, neighborhood_size//2):
                        for dx in range(-neighborhood_size//2, neighborhood_size//2):
                            py, px = y + dy, x + dx
                            if 0 <= py < ssh.shape[0] and 0 <= px < ssh.shape[1]:
                                processed_points.add((py, px))
        
        # Find strong downwelling zones (significant positive SSH)
        downwelling = (ssh_smooth > 0.5) & (gradient_magnitude > np.percentile(gradient_magnitude[~np.isnan(gradient_magnitude)], 75))
        down_points = np.where(downwelling)
        
        processed_points.clear()
        
        for y, x in zip(*down_points):
            if (y, x) not in processed_points:
                # Find local center
                y_start, y_end = max(0, y - neighborhood_size//2), min(ssh.shape[0], y + neighborhood_size//2)
                x_start, x_end = max(0, x - neighborhood_size//2), min(ssh.shape[1], x + neighborhood_size//2)
                
                region = ssh_smooth[y_start:y_end, x_start:x_end]
                if region.size > 0:
                    local_max_idx = np.unravel_index(np.argmax(region), region.shape)
                    center_y = y_start + local_max_idx[0]
                    center_x = x_start + local_max_idx[1]
                    
                    ssh_value = float(ssh[center_y, center_x])
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(lons[center_x]), float(lats[center_y])]
                        },
                        'properties': {
                            'feature_type': 'downwelling_zone',
                            'ssh': ssh_value,
                            'strength': 'strong' if ssh_value > 0.8 else 'moderate',
                            'description': f'Downwelling Zone: {ssh_value:.2f}m',
                            'display_text': 'Downwelling\nZone'
                        }
                    })
                    
                    # Mark region as processed
                    for dy in range(-neighborhood_size//2, neighborhood_size//2):
                        for dx in range(-neighborhood_size//2, neighborhood_size//2):
                            py, px = y + dy, x + dx
                            if 0 <= py < ssh.shape[0] and 0 <= px < ssh.shape[1]:
                                processed_points.add((py, px))
        
        return features

    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert ocean dynamics data to feature GeoJSON format."""
        try:
            # Get variables based on SOURCES configuration
            source_config = SOURCES[dataset]
            altimetry_vars = source_config['source_datasets']['altimetry']['variables']
            currents_vars = source_config['source_datasets']['currents']['variables']
            
            # Get SSH variable name
            ssh_var = next(var for var in altimetry_vars.keys())
            
            # Get current variable names
            u_var = next(var for var, config in currents_vars.items() if config['type'] == 'current' and var.startswith('u'))
            v_var = next(var for var, config in currents_vars.items() if config['type'] == 'current' and var.startswith('v'))
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            # Extract variables from merged dataset
            ssh = data[ssh_var].values
            u_current = data[u_var].values
            v_current = data[v_var].values
            
            # Initialize features list
            features = []
            
            # Detect eddies
            features.extend(self._detect_eddies(ssh, u_current, v_current, lons, lats))
            
            # Find SSH extrema
            features.extend(self._find_extrema(ssh, lons, lats))
            
            # Find upwelling/downwelling zones
            features.extend(self._find_upwelling_zones(ssh, lons, lats))
            
            # Add metadata
            metadata = {
                "source": dataset,
                "feature_types": [
                    "cyclonic_eddy",
                    "anticyclonic_eddy",
                    "ssh_maximum",
                    "ssh_minimum",
                    "upwelling_zone",
                    "downwelling_zone"
                ]
            }
            
            # Create standardized GeoJSON
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=None,
                metadata=metadata
            )
            
            # Save and return path
            output_path = self.path_manager.get_asset_paths(date, dataset, region).features
            logger.info(f"   └── Saving ocean features GeoJSON to {output_path.name}")
            
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting ocean features to GeoJSON: {str(e)}")
            raise 