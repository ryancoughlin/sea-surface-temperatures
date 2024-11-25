import numpy as np
import xarray as xr
from pathlib import Path
import logging
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS
import datetime
from scipy.ndimage import gaussian_filter
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

class ChlorophyllContourConverter(BaseGeoJSONConverter):
    """Converts chlorophyll data to contours highlighting bloom areas."""
    
    def _calculate_contour_levels(self, data: np.ndarray) -> np.ndarray:
        """Calculate contour levels focusing on bloom detection."""
        valid_data = data[~np.isnan(data)]
        
        if len(valid_data) == 0:
            return np.array([])
        
        # Calculate key percentiles
        p75 = np.percentile(valid_data, 75)  # Background threshold
        p90 = np.percentile(valid_data, 90)  # Potential bloom
        p95 = np.percentile(valid_data, 95)  # Definite bloom
        
        return np.array([p75, p90, p95])

    def _classify_feature(self, value: float, percentiles: dict) -> dict:
        """Classify if this is a bloom feature."""
        if value >= percentiles['p95']:
            return {
                "is_bloom": True,
                "type": "major_bloom",
                "description": "Major bloom area"
            }
        elif value >= percentiles['p90']:
            return {
                "is_bloom": True,
                "type": "bloom",
                "description": "Bloom area"
            }
        else:
            return {
                "is_bloom": False,
                "type": "boundary",
                "description": "Productivity boundary"
            }

    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert chlorophyll data focusing on bloom identification."""
        try:
            # Load and prepare data
            ds = self.load_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = self.normalize_dataset(ds, var_name)
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(data)
            
            # Mask to region
            bounds = REGIONS[region]['bounds']
            regional_data = data.where(
                (data[lon_name] >= bounds[0][0]) & 
                (data[lon_name] <= bounds[1][0]) &
                (data[lat_name] >= bounds[0][1]) & 
                (data[lat_name] <= bounds[1][1]),
                drop=True
            )
            
            # Smooth data to focus on significant features
            smoothed_data = gaussian_filter(regional_data.values, sigma=1.5)
            
            # Calculate levels
            levels = self._calculate_contour_levels(smoothed_data)
            percentiles = {
                'p75': levels[0],
                'p90': levels[1],
                'p95': levels[2]
            }
            
            # Generate contours
            fig, ax = plt.subplots(figsize=(10, 10))
            contour_set = ax.contour(
                regional_data[lon_name],
                regional_data[lat_name],
                smoothed_data,
                levels=levels
            )
            plt.close(fig)
            
            features = []
            for level_idx, level_value in enumerate(contour_set.levels):
                for segment in contour_set.allsegs[level_idx]:
                    path_length = np.sum(np.sqrt(np.sum(np.diff(segment, axis=0)**2, axis=1)))
                    
                    # Only keep longer segments for blooms
                    min_length = 0.5 if level_value >= percentiles['p90'] else 1.0
                    if path_length < min_length:
                        continue
                    
                    coords = [[float(x), float(y)] for x, y in segment 
                             if not (np.isnan(x) or np.isnan(y))]
                    
                    if len(coords) < 5:
                        continue
                    
                    classification = self._classify_feature(level_value, percentiles)
                    
                    # Only include features we care about
                    if not classification['is_bloom'] and level_value < percentiles['p90']:
                        continue
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coords
                        },
                        "properties": {
                            "value": float(level_value),
                            "unit": "mg/m³",
                            "is_bloom": classification['is_bloom'],
                            "feature_type": classification['type'],
                            "description": classification['description'],
                            "length_nm": round(path_length * 60, 1)
                        }
                    }
                    features.append(feature)
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "bloom_thresholds": {
                        "bloom": float(percentiles['p90']),
                        "major_bloom": float(percentiles['p95'])
                    }
                }
            }
            
            # Save and return
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            self.save_geojson(geojson, asset_paths.contours)
            return asset_paths.contours
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to contours: {str(e)}")
            raise