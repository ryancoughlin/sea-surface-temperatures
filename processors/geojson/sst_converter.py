from pathlib import Path
import logging
import datetime
import numpy as np
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f
import xarray as xr

logger = logging.getLogger(__name__)

class SSTGeoJSONConverter(BaseGeoJSONConverter):
    def _mask_to_region(self, data, bounds, lon_name, lat_name):
        """Mask dataset to region bounds."""
        lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
        lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
        return data.where(lon_mask & lat_mask, drop=True)

    def convert(self, data: xr.DataArray | xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
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
            
            # Force 2D by selecting first index of time and depth
            for dim in ['time', 'depth']:
                if dim in temp_data.dims:
                    temp_data = temp_data.isel({dim: 0})
                if gradient_data is not None and dim in gradient_data.dims:
                    gradient_data = gradient_data.isel({dim: 0})
            
            # Get coordinates and mask to region
            lon_name, lat_name = self.get_coordinate_names(temp_data)
            bounds = REGIONS[region]['bounds']
            
            # Mask and convert to Fahrenheit using source unit from settings
            temp_data = self._mask_to_region(temp_data, bounds, lon_name, lat_name)
            source_unit = SOURCES[dataset].get('source_unit', 'C')  # Default to Celsius if not specified
            temp_data = convert_temperature_to_f(temp_data, source_unit=source_unit)
            
            if gradient_data is not None:
                gradient_data = self._mask_to_region(gradient_data, bounds, lon_name, lat_name)
            
            # Create features
            features = []
            lats = temp_data[lat_name].values
            lons = temp_data[lon_name].values
            temp_values = temp_data.values
            gradient_values = gradient_data.values if gradient_data is not None else None
            
            for i in range(len(lats)):
                for j in range(len(lons)):
                    temp_value = float(temp_values[i, j])
                    if np.isnan(temp_value):
                        continue
                        
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                float(lons[j]),
                                float(lats[i])
                            ]
                        },
                        "properties": {
                            "temperature": round(temp_value, 2),
                            "unit": "fahrenheit"
                        }
                    }
                    
                    # Add gradient information if available
                    if gradient_values is not None:
                        gradient_value = float(gradient_values[i, j])
                        if not np.isnan(gradient_value):
                            feature["properties"]["gradient"] = round(gradient_value, 4)
                    
                    features.append(feature)
            
            # Calculate ranges from valid data
            valid_temps = temp_values[~np.isnan(temp_values)]
            ranges = {
                "temperature": {
                    "min": float(np.min(valid_temps)) if len(valid_temps) > 0 else None,
                    "max": float(np.max(valid_temps)) if len(valid_temps) > 0 else None,
                    "unit": "fahrenheit"
                }
            }
            
            # Add gradient ranges if available
            if gradient_values is not None:
                valid_gradients = gradient_values[~np.isnan(gradient_values)]
                if len(valid_gradients) > 0:
                    ranges["gradient"] = {
                        "min": float(np.min(valid_gradients)),
                        "max": float(np.max(valid_gradients)),
                        "unit": "magnitude"
                    }
            
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