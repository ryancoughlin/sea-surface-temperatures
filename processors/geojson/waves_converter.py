import xarray as xr
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class WavesGeoJSONConverter(BaseGeoJSONConverter):
    def _prepare_data(self, data: xr.Dataset) -> xr.Dataset:
        """Prepare dataset for conversion."""
        # Keep as Dataset, just rename variables
        prepared = data.copy()
        prepared = prepared.rename({'VHM0': 'height', 'VMDR': 'direction'})
        return prepared
    
    def _create_features(self, data: xr.Dataset) -> List[Dict]:
        """Create GeoJSON features from wave data."""
        features = []
        
        # Get coordinates
        lons = data['longitude'].values
        lats = data['latitude'].values
        
        # Create features for each point
        for i in range(len(lats)):
            for j in range(len(lons)):
                if not np.isnan(data['height'].values[i, j]):
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(lons[j]), float(lats[i])]
                        },
                        'properties': {
                            'height': float(data['height'].values[i, j]),
                            'direction': float(data['direction'].values[i, j])
                        }
                    })
                    
        return features
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert wave data to GeoJSON format."""
        try:
            logger.info(f"Converting waves data to GeoJSON for {dataset} in {region}")
            
            # Keep as Dataset throughout processing
            processed_data = self._prepare_data(data)
            
            # Convert to GeoJSON at the end
            features = self._create_features(processed_data)
            
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
            return self.save_geojson(geojson, asset_paths.data)
            
        except Exception as e:
            logger.error(f"Error processing wave data: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            raise