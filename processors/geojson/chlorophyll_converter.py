import numpy as np
import xarray as xr
from pathlib import Path
import logging
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
import datetime
from typing import Union, Dict, Optional

logger = logging.getLogger(__name__)

class ChlorophyllGeoJSONConverter(BaseGeoJSONConverter):
    """Converts chlorophyll data to GeoJSON for basic data display."""
    
    def convert(self, data: Union[xr.Dataset, xr.DataArray], region: str, dataset: str, date: datetime) -> Path:
        """Convert chlorophyll data to GeoJSON format."""
        try:
            # Extract chlorophyll data
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                chl_var = next(var for var, config in variables.items() if config['type'] == 'chlorophyll')
                chl_data = data[chl_var]
            else:
                chl_data = data
            
            # Generate features from chlorophyll data
            def property_generator(i: int, j: int) -> Optional[Dict]:
                value = float(chl_data.values[i, j])
                if np.isnan(value):
                    return None
                    
                return {
                    "concentration": round(value, 4),
                    "unit": "mg/m³"
                }
            
            # Generate features using base class utility
            features = self._generate_features(
                chl_data.latitude.values,
                chl_data.longitude.values,
                property_generator
            )
            logger.info(f"Generated {len(features)} features")
            
            # Calculate ranges
            ranges = self._calculate_ranges({
                "concentration": (chl_data.values, "mg/m³")
            })
            
            # Create GeoJSON with metadata
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata={
                    "source": dataset,
                    "variables": list(SOURCES[dataset]['variables']),
                    "unit": "mg/m³",
                    "valid_range": ranges["concentration"]
                }
            )
            
            # Save and return
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}", exc_info=True)
            raise
