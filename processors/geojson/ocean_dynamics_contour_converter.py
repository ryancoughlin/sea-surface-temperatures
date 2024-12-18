from pathlib import Path
import logging
import datetime
import numpy as np
from typing import Optional, Dict, List, Tuple
from .base_converter import BaseGeoJSONConverter
import xarray as xr
import matplotlib.pyplot as plt
from shapely.geometry import LineString, Polygon, mapping
from shapely.ops import linemerge

logger = logging.getLogger(__name__)

class OceanDynamicsContourConverter(BaseGeoJSONConverter):
    def __init__(self, path_manager, data_assembler):
        super().__init__(path_manager, data_assembler)
        # Parameters for feature detection
        self.ENDPOINT_TOLERANCE = 0.001
        self.MIN_EDDY_POINTS = 8
        self.MIN_EDDY_SIZE = 0.1
        
        # Relative thresholds (percentiles)
        self.STRONG_THRESH = 0.2   # Bottom 20% = strong upwelling
        self.WEAK_THRESH = 0.4     # 20-40% = weak upwelling
        self.CONVERGENCE_THRESH = 0.6  # Top 40% = convergence
    
    def _connect_segments(self, segments: List[np.ndarray]) -> List[np.ndarray]:
        """Connect segments that share endpoints within tolerance."""
        if not segments:
            return []
            
        def endpoints_match(seg1, seg2):
            start1, end1 = seg1[0], seg1[-1]
            start2, end2 = seg2[0], seg2[-1]
            return (np.linalg.norm(end1 - start2) < self.ENDPOINT_TOLERANCE or
                   np.linalg.norm(end1 - end2) < self.ENDPOINT_TOLERANCE or
                   np.linalg.norm(start1 - start2) < self.ENDPOINT_TOLERANCE or
                   np.linalg.norm(start1 - end2) < self.ENDPOINT_TOLERANCE)
        
        def merge_segments(seg1, seg2):
            start1, end1 = seg1[0], seg1[-1]
            start2, end2 = seg2[0], seg2[-1]
            
            if np.linalg.norm(end1 - start2) < self.ENDPOINT_TOLERANCE:
                return np.vstack((seg1, seg2))
            elif np.linalg.norm(end1 - end2) < self.ENDPOINT_TOLERANCE:
                return np.vstack((seg1, seg2[::-1]))
            elif np.linalg.norm(start1 - start2) < self.ENDPOINT_TOLERANCE:
                return np.vstack((seg1[::-1], seg2))
            else:  # start1 - end2
                return np.vstack((seg2, seg1))
        
        # Try to connect segments
        connected = []
        remaining = segments.copy()
        
        while remaining:
            current = remaining.pop(0)
            merged = True
            while merged:
                merged = False
                for i, other in enumerate(remaining):
                    if endpoints_match(current, other):
                        current = merge_segments(current, other)
                        remaining.pop(i)
                        merged = True
                        break
            connected.append(current)
        
        return connected
    
    def _calculate_thresholds(self, ssh_values: np.ndarray) -> Dict[str, float]:
        """Calculate absolute thresholds based on data distribution."""
        valid_ssh = ssh_values[~np.isnan(ssh_values)]
        percentiles = np.percentile(valid_ssh, 
                                  [self.STRONG_THRESH * 100, 
                                   self.WEAK_THRESH * 100,
                                   self.CONVERGENCE_THRESH * 100])
        
        return {
            "strong": float(percentiles[0]),
            "weak": float(percentiles[1]),
            "convergence": float(percentiles[2])
        }
    
    def _classify_ocean_feature(self, ssh_value: float, thresholds: Dict[str, float]) -> Dict:
        """Classify ocean features based on SSH values."""
        if ssh_value <= thresholds["strong"]:
            return {
                "feature_type": "strong_upwelling",
                "description": "Strong upwelling - Nutrient-rich cold water",
                "fishing_relevance": "High potential for fish activity"
            }
        elif ssh_value <= thresholds["weak"]:
            return {
                "feature_type": "weak_upwelling",
                "description": "Weak upwelling - Mixing zone",
                "fishing_relevance": "Moderate potential for fish activity"
            }
        elif ssh_value >= thresholds["convergence"]:
            return {
                "feature_type": "convergence",
                "description": "Convergence zone - Warm water accumulation",
                "fishing_relevance": "Check for temperature breaks"
            }
        else:
            return {
                "feature_type": "transition",
                "description": "Transition zone",
                "fishing_relevance": "Monitor for changes"
            }
    
    def _is_closed_contour(self, segment: np.ndarray) -> Tuple[bool, Optional[str]]:
        """Check if a segment forms a closed contour and classify eddy type."""
        if len(segment) < self.MIN_EDDY_POINTS:
            return False, None
            
        start, end = segment[0], segment[-1]
        if np.linalg.norm(start - end) > self.ENDPOINT_TOLERANCE:
            return False, None
            
        # Calculate size
        lon_range = np.max(segment[:, 0]) - np.min(segment[:, 0])
        lat_range = np.max(segment[:, 1]) - np.min(segment[:, 1])
        size = max(lon_range, lat_range)
        
        if size < self.MIN_EDDY_SIZE:
            return False, None
            
        # Classify eddy size
        if size > 0.5:
            eddy_size = "large"
        elif size > 0.2:
            eddy_size = "medium"
        else:
            eddy_size = "small"
            
        return True, eddy_size
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        try:
            logger.info(f"Converting ocean dynamics data for region: {region}")
            logger.info(f"Dataset: {dataset}")
            logger.info(f"Date: {date}")
            logger.info(f"Data variables: {list(data.variables)}")
            # 1. Get SSH data
            ssh = data['sea_surface_height'].values
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            valid_ssh = ssh[~np.isnan(ssh)]
            if len(valid_ssh) == 0:
                return self.save_empty_geojson(date, dataset, region)
            
            min_val = float(np.min(valid_ssh))
            max_val = float(np.max(valid_ssh))
            
            logger.info(f"Processing SSH data range: {min_val:.3f} to {max_val:.3f}")
            
            # 2. Calculate thresholds based on data distribution
            thresholds = self._calculate_thresholds(ssh)
            logger.info(f"Feature thresholds: strong={thresholds['strong']:.3f}, "
                       f"weak={thresholds['weak']:.3f}, "
                       f"convergence={thresholds['convergence']:.3f}")
            
            # 3. Generate contour levels with more detail
            num_levels = 12  # Increase number of levels
            levels = np.linspace(min_val, max_val, num_levels)
            
            # 4. Generate contours
            fig, ax = plt.subplots()
            contour_set = ax.contour(
                lons, lats, ssh, 
                levels=levels,
                linestyles='solid',
                linewidths=1
            )
            plt.close(fig)
            
            # 5. Process and convert to GeoJSON features
            features = []
            for level_idx, level in enumerate(contour_set.levels):
                segments = [np.array(seg) for seg in contour_set.allsegs[level_idx] if len(seg) >= 3]
                if not segments:
                    continue
                    
                logger.info(f"Processing level {level:.3f} with {len(segments)} segments")
                connected_segments = self._connect_segments(segments)
                logger.info(f"Connected into {len(connected_segments)} features")
                
                for segment in connected_segments:
                    # Check for eddies
                    is_closed, eddy_size = self._is_closed_contour(segment)
                    
                    # Get ocean feature classification
                    feature_info = self._classify_ocean_feature(float(level), thresholds)
                    
                    # Create geometry
                    if is_closed:
                        geom = mapping(Polygon(segment))
                        feature_info["feature_type"] = f"{eddy_size}_eddy"
                        feature_info["description"] = f"{eddy_size.capitalize()} eddy - Rotating water mass"
                        feature_info["fishing_relevance"] = "Check edges for fish aggregation"
                    else:
                        geom = mapping(LineString(segment))
                    
                    # Create feature with enhanced properties
                    feature = {
                        "type": "Feature",
                        "geometry": geom,
                        "properties": {
                            "ssh_value": float(level),
                            "unit": "meters",
                            **feature_info,
                            "length_nm": float(LineString(segment).length) * 60,  # Convert degrees to nautical miles
                            "points": len(segment)
                        }
                    }
                    features.append(feature)
            
            # Log summary
            eddy_count = sum(1 for f in features if "eddy" in f["properties"]["feature_type"])
            logger.info(f"Generated {len(features)} features including {eddy_count} eddies")
            
            # 6. Save as GeoJSON with enhanced metadata
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges={
                    "ssh": {"min": min_val, "max": max_val, "unit": "m"},
                    "thresholds": thresholds
                },
                metadata={
                    "feature_types": {
                        "strong_upwelling": "Nutrient-rich areas, high potential for fish",
                        "weak_upwelling": "Mixing zones, moderate fishing potential",
                        "convergence": "Warm water accumulation, check for temperature breaks",
                        "transition": "Changing conditions, monitor for activity",
                        "eddy": "Rotating water mass, check edges for fish"
                    }
                }
            )
            
            output_path = self.path_manager.get_asset_paths(date, dataset, region).contours
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting SSH contours: {str(e)}")
            raise