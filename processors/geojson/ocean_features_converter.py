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
    
    def _calculate_thresholds(self, ssh: np.ndarray, u_current: np.ndarray, v_current: np.ndarray, 
                               lons: np.ndarray, lats: np.ndarray) -> Dict:
        """Calculate dynamic thresholds based on data characteristics."""
        # Grid spacing in km
        grid_spacing = 111.0 * np.mean(np.diff(lons))  # km per grid point
        
        # SSH thresholds based on data statistics
        ssh_std = np.nanstd(ssh)
        ssh_mean = np.nanmean(ssh)
        
        thresholds = {
            'ssh': {
                'min_amplitude': ssh_std * 0.5,  # Reduced from 0.7 to 0.5
                'max_amplitude': ssh_std * 2.0,  # 2x STD for maximum
                'high_thresh': ssh_mean + (ssh_std * 0.5),  # Reduced from 0.7 to 0.5
                'low_thresh': ssh_mean - (ssh_std * 0.5)   # Reduced from 0.7 to 0.5
            },
            'size': {
                'min_radius_points': max(2, int(10/grid_spacing)),  # Reduced from 15km to 10km
                'max_radius_points': int(150/grid_spacing)          # Increased from 100km to 150km
            },
            'velocity': {
                'min_speed': 0.1,  # Reduced from 0.2 to 0.1 m/s
                'max_speed': 1.5   # Increased from 1.0 to 1.5 m/s
            }
        }
        
        # Calculate current statistics
        current_magnitude = np.sqrt(u_current**2 + v_current**2)
        current_std = np.nanstd(current_magnitude)
        current_mean = np.nanmean(current_magnitude)
        
        # Adjust velocity thresholds based on data
        thresholds['velocity']['min_speed'] = min(0.1, current_mean * 0.3)  # More lenient speed threshold
        
        logger.info(f"[THRESHOLDS] Grid spacing: {grid_spacing:.2f} km/point")
        logger.info(f"[THRESHOLDS] SSH - Mean: {ssh_mean:.3f}, STD: {ssh_std:.3f}")
        logger.info(f"[THRESHOLDS] Current - Mean: {current_mean:.3f}, STD: {current_std:.3f}")
        logger.info(f"[THRESHOLDS] Size - Min: {thresholds['size']['min_radius_points']} points ({10:.1f}km)")
        logger.info(f"[THRESHOLDS] Size - Max: {thresholds['size']['max_radius_points']} points ({150:.1f}km)")
        logger.info(f"[THRESHOLDS] Current - Min: {thresholds['velocity']['min_speed']:.2f} m/s")
        
        return thresholds
    
    def _detect_eddies(self, ssh: np.ndarray, u_current: np.ndarray, v_current: np.ndarray,
                      lons: np.ndarray, lats: np.ndarray) -> List[Dict]:
        """
        Detect ocean eddies using dynamic thresholds based on data characteristics.
        """
        features = []
        
        # Calculate dynamic thresholds
        thresholds = self._calculate_thresholds(ssh, u_current, v_current, lons, lats)
        
        # Replace NaN values with interpolation for smoother detection
        ssh_filled = ssh.copy()
        mask = np.isnan(ssh_filled)
        ssh_filled[mask] = np.interp(np.flatnonzero(mask), 
                                   np.flatnonzero(~mask), 
                                   ssh_filled[~mask])
        
        # Step 1: Find potential eddy centers
        ssh_smooth = gaussian_filter(ssh_filled, sigma=1.0)
        
        # Find extrema meeting amplitude criteria
        max_filtered = maximum_filter(ssh_smooth, size=3)
        min_filtered = minimum_filter(ssh_smooth, size=3)
        
        # Identify potential eddy centers using dynamic thresholds
        ssh_max = (ssh_smooth == max_filtered) & (ssh_smooth > thresholds['ssh']['high_thresh'])
        ssh_min = (ssh_smooth == min_filtered) & (ssh_smooth < thresholds['ssh']['low_thresh'])
        
        max_points = np.where(ssh_max)
        min_points = np.where(ssh_min)
        
        logger.info(f"[EDDY DETECTION] Initial centers - High: {len(max_points[0])}, Low: {len(min_points[0])}")
        
        # Calculate speed and vorticity
        speed = np.sqrt(u_current**2 + v_current**2)
        dx = np.gradient(lons) * 111000  # meters
        dy = np.gradient(lats) * 111000
        dvdx = np.gradient(v_current, dx, axis=1)
        dudy = np.gradient(u_current, dy, axis=0)
        vorticity = dvdx - dudy
        
        # Process anticyclonic (clockwise) candidates
        anticyclonic_count = 0
        for y, x in zip(*max_points):
            if not (0 <= y < ssh.shape[0] and 0 <= x < ssh.shape[1]):
                continue
                
            radius = self._estimate_fishing_radius(
                ssh_smooth, speed, x, y,
                thresholds['size']['min_radius_points'],
                thresholds['size']['max_radius_points'],
                min_speed=thresholds['velocity']['min_speed']
            )
            
            if radius and self._validate_fishing_eddy(
                ssh_smooth, vorticity, speed, x, y, radius,
                anticyclonic=True,
                min_speed=thresholds['velocity']['min_speed']
            ):
                anticyclonic_count += 1
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [float(lons[x]), float(lats[y])]
                    },
                    'properties': {
                        'feature_type': 'anticyclonic_eddy',
                        'ssh': float(ssh[y, x]),
                        'radius_km': float(radius * 111.0 * np.mean(np.diff(lons))),
                        'speed_knots': float(np.mean(speed[y-radius:y+radius, x-radius:x+radius]) * 1.944),
                        'description': 'Clockwise eddy - Good for tuna/mahi',
                        'display_text': f'Clockwise\n{int(radius * 111.0 * np.mean(np.diff(lons)))}km'
                    }
                })
        
        # Process cyclonic candidates with same thresholds
        cyclonic_count = 0
        for y, x in zip(*min_points):
            if not (0 <= y < ssh.shape[0] and 0 <= x < ssh.shape[1]):
                continue
                
            radius = self._estimate_fishing_radius(
                ssh_smooth, speed, x, y,
                thresholds['size']['min_radius_points'],
                thresholds['size']['max_radius_points'],
                min_speed=thresholds['velocity']['min_speed']
            )
            
            if radius and self._validate_fishing_eddy(
                ssh_smooth, vorticity, speed, x, y, radius,
                anticyclonic=False,
                min_speed=thresholds['velocity']['min_speed']
            ):
                cyclonic_count += 1
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [float(lons[x]), float(lats[y])]
                    },
                    'properties': {
                        'feature_type': 'cyclonic_eddy',
                        'ssh': float(ssh[y, x]),
                        'radius_km': float(radius * 111.0 * np.mean(np.diff(lons))),
                        'speed_knots': float(np.mean(speed[y-radius:y+radius, x-radius:x+radius]) * 1.944),
                        'description': 'Counterclockwise eddy - Good for bait/feeding',
                        'display_text': f'Counterclockwise\n{int(radius * 111.0 * np.mean(np.diff(lons)))}km'
                    }
                })
        
        logger.info(f"[EDDY DETECTION] Found {anticyclonic_count} fishable anticyclonic and {cyclonic_count} cyclonic eddies")
        return features
    
    def _estimate_fishing_radius(self, ssh: np.ndarray, speed: np.ndarray, x: int, y: int, 
                               min_radius: int, max_radius: int, min_speed: float) -> Optional[int]:
        """Estimate eddy radius based on fishing-relevant criteria."""
        center_value = ssh[y, x]
        best_radius = None
        max_gradient = 0
        
        for r in range(min_radius, max_radius):
            if y-r < 0 or y+r >= ssh.shape[0] or x-r < 0 or x+r >= ssh.shape[1]:
                break
                
            # Check SSH gradient
            ring = ssh[y-r:y+r, x-r:x+r]
            gradient = np.abs(np.mean(ring) - center_value)
            
            # Check current speed
            ring_speed = speed[y-r:y+r, x-r:x+r]
            mean_speed = np.mean(ring_speed)
            
            if gradient > max_gradient and mean_speed > min_speed:
                max_gradient = gradient
                best_radius = r
        
        return best_radius
    
    def _validate_fishing_eddy(self, ssh: np.ndarray, vorticity: np.ndarray, speed: np.ndarray,
                             x: int, y: int, radius: int, anticyclonic: bool, min_speed: float) -> bool:
        """Validate eddy based on fishing-relevant criteria."""
        # Extract region around potential eddy with proper bounds checking
        y_slice = slice(max(0, y-radius), min(ssh.shape[0], y+radius))
        x_slice = slice(max(0, x-radius), min(ssh.shape[1], x+radius))
        
        region_ssh = ssh[y_slice, x_slice]
        region_vorticity = vorticity[y_slice, x_slice]
        region_speed = speed[y_slice, x_slice]
        
        # Skip if region is too small
        if region_ssh.size < 4:  # Reduced from 9 to 4 (2x2 grid)
            return False
            
        # 1. Check current speed using 50th percentile (median)
        speed_threshold = np.nanpercentile(region_speed, 50)  # Reduced from 75th to 50th percentile
        if speed_threshold < min_speed:
            return False
            
        # 2. Check rotation coherence with scaled thresholds
        vort_mean = np.nanmean(region_vorticity)
        vort_std = np.nanstd(region_vorticity)
        
        # Scale threshold based on local variability
        vort_threshold = vort_std * 0.15  # Reduced from 0.25 to 0.15
        
        if anticyclonic:
            rotation_check = vort_mean < -vort_threshold * 0.5  # Reduced threshold by half
        else:
            rotation_check = vort_mean > vort_threshold * 0.5   # Reduced threshold by half
            
        if not rotation_check:
            return False
            
        # 3. Check SSH structure relative to local variability
        ssh_center = ssh[y, x]
        ssh_edge_mean = np.nanmean(region_ssh[0:1, :])  # Use edge values
        ssh_gradient = np.abs(ssh_center - ssh_edge_mean)
        
        if ssh_gradient < np.nanstd(region_ssh) * 0.3:  # Reduced from 0.5 to 0.3
            return False
            
        # 4. Check shape with more lenient criteria
        if not self._check_shape(region_ssh):
            return False
            
        return True
    
    def _check_shape(self, region: np.ndarray) -> bool:
        """Check if the region has a roughly circular shape using improved metrics."""
        if region.size < 4:  # Reduced minimum size
            return False
            
        # Calculate aspect ratio
        y_size, x_size = region.shape
        aspect_ratio = max(y_size, x_size) / min(y_size, x_size)
        
        # Calculate compactness using the ratio of values within 1.5 std of the mean
        region_mean = np.nanmean(region)
        region_std = np.nanstd(region)
        within_bounds = np.sum(np.abs(region - region_mean) <= 1.5 * region_std)  # Increased from 1.0 to 1.5
        compactness = within_bounds / region.size
        
        return aspect_ratio < 3.0 and compactness > 0.5  # More lenient criteria
    
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
            
            logger.info(f"[EDDY DETECTION] SSH shape: {ssh.shape}")
            logger.info(f"[EDDY DETECTION] Current shapes: {u_current.shape}, {v_current.shape}")
            
            # Calculate some statistics for debugging
            logger.info(f"[EDDY DETECTION] SSH range: {np.nanmin(ssh):.3f} to {np.nanmax(ssh):.3f}")
            logger.info(f"[EDDY DETECTION] U current range: {np.nanmin(u_current):.3f} to {np.nanmax(u_current):.3f}")
            logger.info(f"[EDDY DETECTION] V current range: {np.nanmin(v_current):.3f} to {np.nanmax(v_current):.3f}")
            
            # Initialize features list
            features = []
            
            # Detect eddies with debug info
            eddy_features = self._detect_eddies(ssh, u_current, v_current, lons, lats)
            logger.info(f"[EDDY DETECTION] Found {len(eddy_features)} eddies")
            features.extend(eddy_features)
            
            # Find SSH extrema
            ssh_features = self._find_extrema(ssh, lons, lats)
            logger.info(f"[EDDY DETECTION] Found {len(ssh_features)} SSH extrema")
            features.extend(ssh_features)
            
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