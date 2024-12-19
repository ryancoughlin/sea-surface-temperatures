import numpy as np
import xarray as xr
from pathlib import Path
import logging
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
import datetime
from scipy.ndimage import gaussian_filter
import matplotlib.pyplot as plt
from typing import Union, Dict

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

    def convert(self, data: Union[xr.DataArray, xr.Dataset, Dict], region: str, dataset: str, date: datetime) -> Path:
        """Convert chlorophyll data focusing on bloom identification."""
        try:
            # Handle standardized data format
            if isinstance(data, dict) and 'data' in data:
                data = data['data']
            
            # If we have a Dataset, get the first data variable
            if isinstance(data, xr.Dataset):
                data = data[list(data.data_vars)[0]]
            
            # Ensure we have a DataArray
            if not isinstance(data, xr.DataArray):
                raise ValueError(f"Expected xarray.DataArray, got {type(data)}")
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Convert to numpy array and ensure float type
            data_values = data.values.astype(np.float64)
            
            # Log data ranges before smoothing
            valid_mask = ~np.isnan(data_values)
            valid_data = data_values[valid_mask]
            
            if len(valid_data) == 0:
                logger.warning("No valid data points found for contours")
                return self.save_empty_geojson(date, dataset, region)
                
            logger.info(f"[RANGES] Contour data min/max before smoothing: {valid_data.min():.4f} to {valid_data.max():.4f}")
            
            # Smooth data to focus on significant features
            smoothed_data = gaussian_filter(data_values, sigma=1.5)
            
            # Calculate levels
            levels = self._calculate_contour_levels(smoothed_data)
            
            if len(levels) == 0:
                logger.warning("No valid contour levels calculated")
                return self.save_empty_geojson(date, dataset, region)
                
            percentiles = {
                'p75': levels[0],
                'p90': levels[1],
                'p95': levels[2]
            }
            
            logger.info(f"[RANGES] Contour levels: p75={levels[0]:.4f}, p90={levels[1]:.4f}, p95={levels[2]:.4f}")
            
            # Generate contours
            fig, ax = plt.subplots(figsize=(10, 10))
            contour_set = ax.contour(
                data[lon_name].values,
                data[lat_name].values,
                smoothed_data,
                levels=levels,
                linestyles='solid',
                linewidths=1.5,  # Increased for better visibility
                colors='black'  # Consistent color for all contours
            )
            plt.close(fig)
            
            features = []
            for level_idx, level_value in enumerate(contour_set.levels):
                for segment in contour_set.allsegs[level_idx]:
                    # Calculate path length for all features
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
                    
                    # Create standardized feature
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coords
                        },
                        "properties": {
                            "value": float(level_value),
                            "unit": "mg/m³",
                            "path_length_nm": round(path_length * 60, 1),  # Convert to nautical miles
                            "points": len(coords),
                            "is_closed": False,  # Chlorophyll contours are not typically closed
                            "is_bloom": classification['is_bloom'],
                            "feature_type": classification['type'],
                            "description": classification['description']
                        }
                    }
                    features.append(feature)
            
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges={
                    "bloom_thresholds": {
                        "bloom": float(percentiles['p90']),
                        "major_bloom": float(percentiles['p95'])
                    }
                },
                metadata={}
            )
            
            # Save and return
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            return self.save_geojson(geojson, asset_paths.contours)
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to contours: {str(e)}")
            raise