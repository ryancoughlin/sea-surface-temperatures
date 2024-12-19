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

class FishingSpotConverter(BaseGeoJSONConverter):
    """Detects and converts potential fishing spots to GeoJSON format."""
    
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
                'min_amplitude': ssh_std * 0.75,  # Increased from 0.3
                'max_amplitude': ssh_std * 2.0,
                'high_thresh': ssh_mean + (ssh_std * 0.75),  # Increased from 0.3
                'low_thresh': ssh_mean - (ssh_std * 0.75)    # Increased from 0.3
            },
            'size': {
                'min_radius_points': max(3, int(20/grid_spacing)),  # Increased minimum size
                'max_radius_points': int(150/grid_spacing)
            },
            'velocity': {
                'min_speed': 0.1,  # Increased from 0.05
                'max_speed': 1.5
            }
        }
        
        return thresholds
    
    def _detect_eddies(self, ssh: np.ndarray, u_current: np.ndarray, v_current: np.ndarray,
                      lons: np.ndarray, lats: np.ndarray) -> List[Dict]:
        """
        Detect eddies using basic oceanographic principles:
        1. Calculate vorticity from u,v currents
        2. Find SSH extrema
        3. Match vorticity with SSH patterns
        """
        features = []
        
        # Skip if too many NaN values
        if np.sum(np.isnan(ssh)) > 0.5 * ssh.size:
            logger.warning("Too many NaN values in SSH data")
            return features
            
        # 1. Calculate vorticity (simple finite difference)
        dx = 111000 * np.cos(np.radians(np.mean(lats))) * np.gradient(lons)  # meters
        dy = 111000 * np.gradient(lats)  # meters
        
        dvdx = np.gradient(v_current, axis=1) / dx[None, :]
        dudy = np.gradient(u_current, axis=0) / dy[:, None]
        vorticity = dvdx - dudy
        
        # 2. Find SSH extrema
        ssh_smooth = gaussian_filter(ssh, sigma=1)
        ssh_max = maximum_filter(ssh_smooth, size=3)
        ssh_min = minimum_filter(ssh_smooth, size=3)
        
        # 3. Find eddy centers
        # Anticyclonic: SSH max + positive vorticity
        anticyclonic_centers = ((ssh_smooth == ssh_max) & 
                              (vorticity > np.nanpercentile(vorticity, 75)))
        
        # Cyclonic: SSH min + negative vorticity
        cyclonic_centers = ((ssh_smooth == ssh_min) & 
                           (vorticity < np.nanpercentile(vorticity, 25)))
        
        # Process anticyclonic eddies
        for y, x in zip(*np.where(anticyclonic_centers)):
            vort_value = float(vorticity[y, x])
            strength = 'Strong' if vort_value > np.nanpercentile(vorticity, 90) else 'Moderate'
            
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(lons[x]), float(lats[y])]
                },
                'properties': {
                    'feature_type': 'clockwise_eddy',
                    'fishing_type': 'Warm Eddy',
                    'strength': strength,
                    'target_species': ['Mahi-mahi', 'Marlin', 'Tuna'],
                    'fishing_notes': 'Warm water eddy. Fish the edges for best results. Look for bait schools and birds.',
                    'vorticity': vort_value,
                    'display_text': f'{strength} Warm Eddy'
                }
            })
        
        # Process cyclonic eddies
        for y, x in zip(*np.where(cyclonic_centers)):
            vort_value = float(vorticity[y, x])
            strength = 'Strong' if abs(vort_value) > np.nanpercentile(abs(vorticity), 90) else 'Moderate'
            
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(lons[x]), float(lats[y])]
                },
                'properties': {
                    'feature_type': 'counterclockwise_eddy',
                    'fishing_type': 'Cool Eddy',
                    'strength': strength,
                    'target_species': ['Yellowfin Tuna', 'Bigeye Tuna', 'Swordfish'],
                    'fishing_notes': 'Cool water eddy. Concentrate on edges where bait gathers. Good for deep dropping.',
                    'vorticity': vort_value,
                    'display_text': f'{strength} Cool Eddy'
                }
            })
        
        return features
        
    def _estimate_radius(self, ssh: np.ndarray, x: int, y: int, lons: np.ndarray, lats: np.ndarray) -> float:
        """Estimate eddy radius using SSH gradient."""
        center = ssh[y, x]
        max_radius = 20  # grid points
        
        for r in range(2, max_radius):
            if y-r < 0 or y+r >= ssh.shape[0] or x-r < 0 or x+r >= ssh.shape[1]:
                break
                
            ring = ssh[y-r:y+r, x-r:x+r]
            if np.any(np.isnan(ring)):
                continue
                
            gradient = np.abs(np.mean(ring) - center)
            if gradient > np.nanstd(ssh) * 0.5:
                return r * 111.0 * np.cos(np.radians(lats[y])) * np.mean(np.diff(lons))
                
        return 0
    
    def _validate_ssh(self, ssh: np.ndarray, x: int, y: int, cyclonic: bool, window: int = 3) -> bool:
        """
        Validate eddy center using SSH.
        Cyclonic eddies should have SSH minimum
        Anticyclonic eddies should have SSH maximum
        """
        if not (0 <= y < ssh.shape[0] - window and 0 <= x < ssh.shape[1] - window):
            return False
            
        region = ssh[y-window:y+window+1, x-window:x+window+1]
        center = ssh[y, x]
        
        if np.isnan(center) or np.all(np.isnan(region)):
            return False
            
        if cyclonic:
            # Should be local minimum
            return np.all(center <= region[~np.isnan(region)])
        else:
            # Should be local maximum
            return np.all(center >= region[~np.isnan(region)])
    
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
        
        # Check for sufficient valid data
        valid_points = np.sum(~np.isnan(region_ssh))
        if valid_points < 0.8 * region_ssh.size:  # Require at least 80% valid data
            return False
        
        # 1. Check current speed using mean of top 25% speeds
        valid_speeds = region_speed[~np.isnan(region_speed)]
        if len(valid_speeds) == 0:
            return False
            
        speed_threshold = np.nanpercentile(valid_speeds, 75)
        if speed_threshold < min_speed:
            return False
        
        # 2. Check rotation coherence with scaled thresholds
        valid_vorticity = region_vorticity[~np.isnan(region_vorticity)]
        if len(valid_vorticity) == 0:
            return False
            
        vort_mean = np.nanmean(valid_vorticity)
        vort_std = np.nanstd(valid_vorticity)
        
        # Require strong and consistent rotation
        if anticyclonic:
            rotation_check = (vort_mean < 0 and 
                            np.sum(valid_vorticity < 0) > 0.7 * len(valid_vorticity))
        else:
            rotation_check = (vort_mean > 0 and 
                            np.sum(valid_vorticity > 0) > 0.7 * len(valid_vorticity))
            
        if not rotation_check:
            return False
        
        # 3. Check SSH structure relative to local variability
        ssh_center = ssh[y, x]
        if np.isnan(ssh_center):
            return False
            
        # Calculate edge values properly
        edge_values = np.concatenate([
            region_ssh[0, :],  # Top edge
            region_ssh[-1, :],  # Bottom edge
            region_ssh[:, 0],  # Left edge
            region_ssh[:, -1]  # Right edge
        ])
        edge_values = edge_values[~np.isnan(edge_values)]
        
        if len(edge_values) == 0:
            return False
            
        ssh_edge_mean = np.mean(edge_values)
        ssh_gradient = np.abs(ssh_center - ssh_edge_mean)
        
        if ssh_gradient < np.nanstd(region_ssh) * 0.5:  # Increased from 0.2 to 0.5
            return False
        
        # 4. Check circularity using edge points
        if not self._check_circularity(region_ssh):
            return False
        
        return True
        
    def _check_circularity(self, region: np.ndarray) -> bool:
        """Check if the region is roughly circular using edge points."""
        # Get edge points
        edges = np.concatenate([
            region[0, :],  # Top edge
            region[-1, :],  # Bottom edge
            region[:, 0],  # Left edge
            region[:, -1]  # Right edge
        ])
        
        valid_edges = edges[~np.isnan(edges)]
        if len(valid_edges) < 8:  # Need minimum points for meaningful check
            return False
            
        # Calculate aspect ratio
        y_size, x_size = region.shape
        aspect_ratio = max(y_size, x_size) / min(y_size, x_size)
        
        # Calculate distances from center to each valid point
        center_y, center_x = region.shape[0] // 2, region.shape[1] // 2
        y_indices, x_indices = np.where(~np.isnan(region))
        
        if len(y_indices) == 0:
            return False
            
        # Calculate distances one dimension at a time
        y_dists = y_indices - center_y
        x_dists = x_indices - center_x
        distances = np.sqrt(y_dists**2 + x_dists**2)
        
        # Circularity criterion: low relative standard deviation and aspect ratio
        return (np.std(distances) / np.mean(distances) < 0.3 and aspect_ratio < 1.5)
    
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
                     neighborhood_size: int = 40) -> List[Dict]:
        features = []
        
        max_filtered = maximum_filter(ssh, size=neighborhood_size)
        min_filtered = minimum_filter(ssh, size=neighborhood_size)
        
        global_max = float(np.nanmax(ssh))
        global_min = float(np.nanmin(ssh))
        
        # Calculate SSH statistics
        ssh_mean = float(np.nanmean(ssh))
        ssh_std = float(np.nanstd(ssh))
        
        maxima = (ssh == max_filtered) & (ssh > np.percentile(ssh[~np.isnan(ssh)], 98))
        max_points = np.where(maxima)
        
        processed_regions = set()
        
        for y, x in zip(*max_points):
            region_key = (y // (neighborhood_size//2), x // (neighborhood_size//2))
            if region_key in processed_regions:
                continue
                
            ssh_value = float(ssh[y, x])
            strength = 'Strong' if ssh_value > (ssh_mean + ssh_std * 1.5) else 'Moderate'
            
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(lons[x]), float(lats[y])]
                },
                'properties': {
                    'feature_type': 'high_ssh',
                    'fishing_type': 'Temperature Break',
                    'strength': strength,
                    'target_species': ['Mahi-mahi', 'Tuna', 'Billfish'],
                    'fishing_notes': 'Warm water concentration. Look for bait schools and temperature breaks.',
                    'value': ssh_value,
                    'display_text': f'{strength} Temp Break'
                }
            })
            
            for dy in range(-1, 2):
                for dx in range(-1, 2):
                    processed_regions.add((
                        (y // (neighborhood_size//2)) + dy,
                        (x // (neighborhood_size//2)) + dx
                    ))
        
        minima = (ssh == min_filtered) & (ssh < np.percentile(ssh[~np.isnan(ssh)], 2))
        min_points = np.where(minima)
        
        processed_regions.clear()
        
        for y, x in zip(*min_points):
            region_key = (y // (neighborhood_size//2), x // (neighborhood_size//2))
            if region_key in processed_regions:
                continue
                
            ssh_value = float(ssh[y, x])
            strength = 'Strong' if ssh_value < (ssh_mean - ssh_std * 1.5) else 'Moderate'
            
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(lons[x]), float(lats[y])]
                },
                'properties': {
                    'feature_type': 'low_ssh',
                    'fishing_type': 'Cool Water Zone',
                    'strength': strength,
                    'target_species': ['Tuna', 'Wahoo', 'Kingfish'],
                    'fishing_notes': 'Cool water area. Good for finding bait concentrations.',
                    'value': ssh_value,
                    'display_text': f'{strength} Cool Zone'
                }
            })
            
            for dy in range(-1, 2):
                for dx in range(-1, 2):
                    processed_regions.add((
                        (y // (neighborhood_size//2)) + dy,
                        (x // (neighborhood_size//2)) + dx
                    ))
        
        return features
    
    def _find_upwelling_zones(self, ssh: np.ndarray, lons: np.ndarray, lats: np.ndarray,
                             neighborhood_size: int = 40) -> List[Dict]:
        features = []
        
        ssh_dx, ssh_dy = np.gradient(ssh)
        gradient_magnitude = np.sqrt(ssh_dx**2 + ssh_dy**2)
        
        ssh_smooth = gaussian_filter(ssh, sigma=2.0)
        
        strong_thresh = np.percentile(gradient_magnitude[~np.isnan(gradient_magnitude)], 90)
        
        upwelling = (ssh_smooth < -0.75) & (gradient_magnitude > strong_thresh)
        up_points = np.where(upwelling)
        
        processed_points = set()
        
        for y, x in zip(*up_points):
            region_key = (y // (neighborhood_size//2), x // (neighborhood_size//2))
            if region_key in processed_points:
                continue
                
            y_start = max(0, y - neighborhood_size//2)
            y_end = min(ssh.shape[0], y + neighborhood_size//2)
            x_start = max(0, x - neighborhood_size//2)
            x_end = min(ssh.shape[1], x + neighborhood_size//2)
            
            region = ssh_smooth[y_start:y_end, x_start:x_end]
            if region.size > 0:
                local_min_idx = np.unravel_index(np.argmin(region), region.shape)
                center_y = y_start + local_min_idx[0]
                center_x = x_start + local_min_idx[1]
                
                ssh_value = float(ssh[center_y, center_x])
                if ssh_value < -0.75:
                    strength = 'Strong' if ssh_value < -1.0 else 'Moderate'
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(lons[center_x]), float(lats[center_y])]
                        },
                        'properties': {
                            'feature_type': 'upwelling',
                            'fishing_type': 'Nutrient Rich Zone',
                            'strength': strength,
                            'target_species': ['Tuna', 'Billfish', 'Wahoo'],
                            'fishing_notes': 'Nutrient-rich water rising from deep. Prime area for bait and predators.',
                            'ssh': ssh_value,
                            'display_text': f'{strength} Upwelling'
                        }
                    })
                
                for dy in range(-2, 3):
                    for dx in range(-2, 3):
                        processed_points.add((
                            (center_y // (neighborhood_size//2)) + dy,
                            (center_x // (neighborhood_size//2)) + dx
                        ))
        
        downwelling = (ssh_smooth > 0.75) & (gradient_magnitude > strong_thresh)
        down_points = np.where(downwelling)
        
        processed_points.clear()
        
        for y, x in zip(*down_points):
            region_key = (y // (neighborhood_size//2), x // (neighborhood_size//2))
            if region_key in processed_points:
                continue
                
            y_start = max(0, y - neighborhood_size//2)
            y_end = min(ssh.shape[0], y + neighborhood_size//2)
            x_start = max(0, x - neighborhood_size//2)
            x_end = min(ssh.shape[1], x + neighborhood_size//2)
            
            region = ssh_smooth[y_start:y_end, x_start:x_end]
            if region.size > 0:
                local_max_idx = np.unravel_index(np.argmax(region), region.shape)
                center_y = y_start + local_max_idx[0]
                center_x = x_start + local_max_idx[1]
                
                ssh_value = float(ssh[center_y, center_x])
                if ssh_value > 0.75:
                    strength = 'Strong' if ssh_value > 1.0 else 'Moderate'
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(lons[center_x]), float(lats[center_y])]
                        },
                        'properties': {
                            'feature_type': 'downwelling',
                            'fishing_type': 'Convergence Zone',
                            'strength': strength,
                            'target_species': ['Mahi-mahi', 'Marlin', 'Flying Fish'],
                            'fishing_notes': 'Surface waters converging. Look for floating debris and bait.',
                            'ssh': ssh_value,
                            'display_text': f'{strength} Convergence'
                        }
                    })
                
                for dy in range(-2, 3):
                    for dx in range(-2, 3):
                        processed_points.add((
                            (center_y // (neighborhood_size//2)) + dy,
                            (center_x // (neighborhood_size//2)) + dx
                        ))
        
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