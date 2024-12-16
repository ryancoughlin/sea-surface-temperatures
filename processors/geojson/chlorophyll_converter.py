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
            logger.info(f"Converting chlorophyll data type: {type(data)}")
            
            # Handle Dataset vs DataArray and extract chlorophyll data
            if isinstance(data, xr.Dataset):
                # Get the main chlorophyll variable
                variables = SOURCES[dataset]['variables']
                chl_var = next(var for var, config in variables.items() if config['type'] == 'chlorophyll')
                logger.info(f"Using chlorophyll variable: {chl_var}")
                chl_data = data[chl_var]
            else:
                chl_data = data
            
            # Reduce dimensions (time, depth)
            chl_data = self._reduce_dimensions(chl_data)
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(chl_data)
            logger.info(f"Using coordinates: lon={lon_name}, lat={lat_name}")
            
            # Prepare data for feature generation
            lats = chl_data[lat_name].values
            lons = chl_data[lon_name].values
            chl_values = chl_data.values
            
            def property_generator(i: int, j: int) -> Optional[Dict]:
                value = float(chl_values[i, j])
                if np.isnan(value):
                    return None
                    
                return {
                    "concentration": round(value, 4),
                    "unit": "mg/m³"
                }
            
            # Generate features using base class utility
            features = self._generate_features(lats, lons, property_generator)
            logger.info(f"Generated {len(features)} features")
            
            # Calculate ranges using base class utility
            data_dict = {
                "concentration": (chl_values, "mg/m³")
            }
            ranges = self._calculate_ranges(data_dict)
            
            # Update metadata
            metadata = {
                "source": dataset,
                "variables": list(SOURCES[dataset]['variables']),
                "unit": "mg/m³",
                "valid_range": ranges["concentration"]
            }
            
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting chlorophyll data to GeoJSON: {str(e)}", exc_info=True)
            raise
