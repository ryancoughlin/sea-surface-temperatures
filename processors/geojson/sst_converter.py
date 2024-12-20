import xarray as xr
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from processors.data.data_utils import convert_temperature_to_f

logger = logging.getLogger(__name__)

class SSTGeoJSONConverter(BaseGeoJSONConverter):
    def _prepare_data(self, data: xr.Dataset, dataset: str) -> xr.Dataset:
        """Prepare dataset for conversion."""
        source_config = SOURCES[dataset]
        sst_var = next(iter(source_config['variables']))
        
        return xr.Dataset({
            'sst': data[sst_var].squeeze()
        })
    
    def _create_features(self, data: xr.Dataset) -> List[Dict]:
        """Create GeoJSON features from SST data."""
        features = []
        
        # Get coordinates
        lons = data['longitude'].values
        lats = data['latitude'].values
        
        # Create features for each point
        for i in range(len(lats)):
            for j in range(len(lons)):
                if not np.isnan(data['sst'].values[i, j]):
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(lons[j]), float(lats[i])]
                        },
                        'properties': {
                            'temperature': float(data['sst'].values[i, j])
                        }
                    })
                    
        return features
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Dict:
        """Convert SST data to GeoJSON format."""
        try:
            logger.info(f"Converting SST data to GeoJSON for {dataset} in {region}")
            
            # Keep as Dataset throughout processing
            processed_data = self._prepare_data(data, dataset)
            
            # Convert to GeoJSON at the end
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
            logger.error(f"❌ Failed to convert SST data: {str(e)}")
            raise