import numpy as np
import xarray as xr
from pathlib import Path
import logging
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from processors.data.data_utils import extract_variables
import datetime
from typing import Union, Dict, Optional, List

logger = logging.getLogger(__name__)

class ChlorophyllGeoJSONConverter(BaseGeoJSONConverter):
    """Converts chlorophyll data to GeoJSON for basic data display."""
    
    def _create_features(self, data: xr.Dataset) -> List[Dict]:
        """Create GeoJSON features from chlorophyll data."""
        features = []
        
        # Validate data
        if len(data['chlor_a'].values[~np.isnan(data['chlor_a'].values)]) == 0:
            logger.warning("No valid chlorophyll data points found")
            return features
        
        try:
            # Stack lat/lon coordinates to get all points
            stacked = data['chlor_a'].stack(points=['latitude', 'longitude'])
            
            # Create features for valid points
            for point in stacked.where(~np.isnan(stacked), drop=True):
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [
                            float(point['longitude'].values),
                            float(point['latitude'].values)
                        ]
                    },
                    'properties': {
                        'concentration': float(point.values)
                    }
                })

            return features
            
        except Exception as e:
            logger.error(f"Error creating chlorophyll features: {str(e)}")
            raise
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Dict:
        """Convert chlorophyll data to GeoJSON format."""
        try:
            logger.info(f"Converting chlorophyll data to GeoJSON for {dataset} in {region}")
            
            # Use chlor_a directly - it's the standard chlorophyll variable
            if 'chlor_a' not in data:
                raise ValueError("Required variable 'chlor_a' not found in dataset")
            
            processed_data = xr.Dataset({'chlor_a': data['chlor_a'].squeeze()})
            
            # Convert to GeoJSON
            features = self._create_features(processed_data)
            
            return {
                'type': 'FeatureCollection',
                'features': features,
                'properties': {
                    'date': date.isoformat(),
                    'region': region,
                    'dataset': dataset
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to convert chlorophyll data: {str(e)}")
            raise
