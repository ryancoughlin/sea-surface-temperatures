from pathlib import Path
import logging
import datetime
import numpy as np
import matplotlib.pyplot as plt
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
import xarray as xr
from typing import Dict, List
from shapely.geometry import LineString
from skimage import measure 

logger = logging.getLogger(__name__)

def clean_value(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return float(value)

class SSTContourConverter(BaseGeoJSONConverter):
    TEMP_INTERVALS = [
        (0, 44, 2),    # Very cold
        (44, 54, 2),   # Cold
        (54, 60, 1),   # Transition
        (60, 75, 1),   # Prime
        (75, 90, 2)    # Warm
    ]
    
    KEY_TEMPERATURES = [60, 65, 70, 72]
    BREAK_THRESHOLDS = {'strong': 95, 'moderate': 85, 'weak': 0}

    def _generate_levels(self, min_temp: float, max_temp: float) -> np.ndarray:
        levels = []
        for start, end, interval in self.TEMP_INTERVALS:
            if max_temp >= start and min_temp <= end:
                range_start = max(start, np.floor(min_temp))
                range_end = min(end, np.ceil(max_temp))
                levels.extend(np.arange(range_start, range_end, interval))
        return np.unique(levels)

    def _classify_feature(self, level: float, gradient_data: Optional[np.ndarray] = None) -> Dict:
        strength = 'weak'
        if gradient_data is not None:
            valid_gradients = gradient_data[~np.isnan(gradient_data)]
            if len(valid_gradients) > 0:
                avg_gradient = float(np.mean(valid_gradients))
                for strength_level, threshold in self.BREAK_THRESHOLDS.items():
                    if avg_gradient > np.percentile(valid_gradients, threshold):
                        strength = strength_level
                        break
        
        return {
            "is_key_temp": level in self.KEY_TEMPERATURES,
            "strength": strength,
            "type": "temperature_break" if strength != 'weak' else "temperature_line"
        }

    def _prepare_data(self, data: xr.Dataset, dataset: str) -> xr.Dataset:
        """Prepare dataset for contour conversion."""
        source_config = SOURCES[dataset]
        sst_var = next(iter(source_config['variables']))
        
        return xr.Dataset({
            'sst': data[sst_var].squeeze()
        })
    
    def _create_contours(self, data: xr.Dataset) -> List[Dict]:
        """Create GeoJSON contour features from SST data."""
        features = []
        
        # Get coordinates and data
        lons = data['longitude'].values
        lats = data['latitude'].values
        sst_values = data['sst'].values
        
        # Calculate valid temperature range
        valid_temps = sst_values[~np.isnan(sst_values)]
        if len(valid_temps) == 0:
            logger.warning("No valid temperature data points found")
            return []
        
        min_temp = float(np.min(valid_temps))
        max_temp = float(np.max(valid_temps))
        
        # Generate contour levels
        levels = self._generate_levels(min_temp, max_temp)
        
        # Generate contours for each level
        for level in levels:
            contours = measure.find_contours(sst_values, level=level)
            
            # Convert contours to GeoJSON
            for contour in contours:
                # Map array indices to coordinates
                coords = []
                for point in contour:
                    lat_idx, lon_idx = point
                    if 0 <= lat_idx < len(lats) and 0 <= lon_idx < len(lons):
                        coords.append([float(lons[int(lon_idx)]), float(lats[int(lat_idx)])])
                
                if len(coords) > 2:  # Minimum points for a valid line
                    # Calculate path length
                    path_length = float(LineString(coords).length)
                    
                    # Filter short segments
                    if path_length < 0.5:
                        continue
                    
                    # Classify feature
                    classification = self._classify_feature(level)
                    
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'LineString',
                            'coordinates': coords
                        },
                        'properties': {
                            'temperature': float(level),
                            'path_length_nm': round(path_length * 60, 1),
                            'is_key_temp': classification['is_key_temp'],
                            'strength': classification['strength'],
                            'feature_type': classification['type']
                        }
                    })
                    
        return features
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to contour GeoJSON format."""
        try:
            logger.info(f"Converting SST data to contour GeoJSON for {dataset} in {region}")
            
            # Keep as Dataset throughout processing
            processed_data = self._prepare_data(data, dataset)
            
            # Convert to GeoJSON at the end
            features = self._create_contours(processed_data)
            
            # Create GeoJSON object
            geojson = {
                'type': 'FeatureCollection',
                'features': features,
                'properties': {
                    'date': date.isoformat(),
                    'region': region,
                    'dataset': dataset
                }
            }
            
            # Save and return path
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            return self.save_geojson(geojson, asset_paths.contours)
            
        except Exception as e:
            logger.error(f"❌ Failed to convert SST contour data: {str(e)}")
            raise

    def _create_geojson(self, features, date, min_temp, max_temp):
        """Create a GeoJSON object with consistent structure."""
        return {
            "type": "FeatureCollection",
            "features": features,
            "properties": {
                "date": date.strftime('%Y-%m-%d'),
                "value_range": {
                    "min": clean_value(min_temp),
                    "max": clean_value(max_temp)
                }
            }
        }