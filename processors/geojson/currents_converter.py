import logging
import numpy as np
import xarray as xr
from typing import Dict, List
from datetime import datetime
from pathlib import Path
from .base_converter import BaseGeoJSONConverter

logger = logging.getLogger(__name__)

class CurrentsGeoJSONConverter(BaseGeoJSONConverter):
    """Converts ocean current data to GeoJSON format."""
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert current data to GeoJSON format."""
        try:
            logger.info(f"Converting currents data to GeoJSON for {dataset} in {region}")
            
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
            logger.error(f"❌ Failed to convert currents data: {str(e)}")
            raise
            
    def _prepare_data(self, data: xr.Dataset) -> xr.Dataset:
        """Prepare dataset for conversion."""
        return xr.Dataset({
            'u': data['uo'].squeeze(),
            'v': data['vo'].squeeze()
        })
        
    def _create_features(self, data: xr.Dataset) -> List[Dict]:
        """Create GeoJSON features from current data."""
        features = []
        
        # Get coordinates
        lons = data['longitude'].values
        lats = data['latitude'].values
        
        # Calculate vector properties
        magnitude = np.sqrt(data['u']**2 + data['v']**2)
        direction = np.arctan2(data['v'], data['u'])
        
        # Create features for each point
        for i in range(len(lats)):
            for j in range(len(lons)):
                if not np.isnan(magnitude.values[i, j]):
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(lons[j]), float(lats[i])]
                        },
                        'properties': {
                            'magnitude': float(magnitude.values[i, j]),
                            'direction': float(direction.values[i, j]),
                            'u': float(data['u'].values[i, j]),
                            'v': float(data['v'].values[i, j])
                        }
                    })
                    
        return features
