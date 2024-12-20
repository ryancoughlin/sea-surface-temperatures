from pathlib import Path
import logging
import datetime
import numpy as np
from typing import Optional, Dict, Tuple, Union
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from processors.data.data_utils import convert_temperature_to_f
import xarray as xr

logger = logging.getLogger(__name__)

class SSTGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data: Union[xr.Dataset, xr.DataArray], region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to GeoJSON format."""
        try:
            # Handle Dataset vs DataArray and extract SST data
            if isinstance(data, xr.Dataset):
                # Get the main SST variable
                variables = SOURCES[dataset]['variables']
                sst_var = next(var for var in variables if 'sst' in var.lower() or 'temperature' in var.lower())
                temp_data = data[sst_var]
                
                # Check for gradient data
                gradient_var = next((var for var in variables if 'gradient' in var.lower()), None)
                gradient_data = data[gradient_var] if gradient_var else None
            else:
                temp_data = data
                gradient_data = None
            
            # Reduce dimensions (time, depth)
            temp_data = self._reduce_dimensions(temp_data)
            if gradient_data is not None:
                gradient_data = self._reduce_dimensions(gradient_data)
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(temp_data)
            
            # Convert to Fahrenheit using source unit from settings
            source_unit = SOURCES[dataset].get('source_unit', 'C')  # Default to Celsius if not specified
            temp_data = convert_temperature_to_f(temp_data, source_unit=source_unit)
            
            # Prepare data for feature generation
            lats = temp_data[lat_name].values
            lons = temp_data[lon_name].values
            temp_values = temp_data.values
            gradient_values = gradient_data.values if gradient_data is not None else None
            
            def property_generator(i: int, j: int) -> Optional[Dict]:
                temp_value = float(temp_values[i, j])
                if np.isnan(temp_value):
                    return None
                    
                properties = {
                    "temperature": round(temp_value, 2),
                    "unit": "fahrenheit"
                }
                
                # Add gradient information if available
                if gradient_values is not None:
                    gradient_value = float(gradient_values[i, j])
                    if not np.isnan(gradient_value):
                        properties["gradient"] = round(gradient_value, 4)
                
                return properties
            
            # Generate features using base class utility
            features = self._generate_features(lats, lons, property_generator)
            
            # Calculate ranges using base class utility
            data_dict = {
                "temperature": (temp_values, "fahrenheit")
            }
            if gradient_values is not None:
                data_dict["gradient"] = (gradient_values, "magnitude")
            
            ranges = self._calculate_ranges(data_dict)
            
            # Update metadata
            metadata = {
                "source": dataset,
                "conversion": f"{source_unit.lower()}_to_fahrenheit",
                "variables": list(SOURCES[dataset]['variables']),
                "has_gradient": gradient_data is not None
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
            logger.error(f"Error converting SST data to GeoJSON: {str(e)}")
            raise