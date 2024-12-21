import xarray as xr
import numpy as np
import logging
from shapely.geometry import Point
from cartopy.feature import NaturalEarthFeature
from typing import Optional

logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        # Initialize land mask data
        self.land = NaturalEarthFeature('physical', 'land', '10m')
        self.land_geoms = list(self.land.geometries())

    def _convert_temperature(self, data: xr.Dataset, var_name: str) -> xr.DataArray:
        """Convert temperature from Celsius to Fahrenheit using xarray operations.
        
        Args:
            data: Dataset containing the temperature variable
            var_name: Name of the temperature variable
            
        Returns:
            DataArray with temperature in Fahrenheit
        """
        temp_data = data[var_name]
        
        # Check current units and skip if already in Fahrenheit
        current_units = temp_data.attrs.get('units', 'C')
        if current_units.upper() == 'F':
            logger.info(f"Variable {var_name} already in Fahrenheit")
            return temp_data
            
        # Convert using xarray's where to handle NaN values properly
        converted = xr.where(
            np.isnan(temp_data),
            temp_data,  # Keep NaN values as is
            temp_data * 1.8 + 32
        )
        
        # Preserve all original metadata and update only unit-related attributes
        converted.attrs = temp_data.attrs.copy()
        converted.attrs.update({
            'units': 'F',
            'original_units': current_units,
            'conversion_applied': 'C_to_F',
            'conversion_date': str(np.datetime64('now')),
            'conversion_formula': 'T(°F) = T(°C) × 1.8 + 32'
        })
        
        # Ensure we preserve the coordinate reference system if present
        if 'grid_mapping' in temp_data.attrs:
            converted.attrs['grid_mapping'] = temp_data.attrs['grid_mapping']
        
        return converted

    def _get_land_mask(self, data: xr.Dataset) -> np.ndarray:
        """Create a boolean mask for land areas using a simplified approach.
        
        Args:
            data: Dataset containing lat/lon coordinates
            
        Returns:
            Boolean mask where True indicates water, False indicates land
        """
        # Get coordinate names
        lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
        lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
        logger.info(f"Using coordinates: {lon_name}, {lat_name}")
        
        # Get the chlorophyll data
        chl_var = 'chlor_a' if 'chlor_a' in data else next(var for var in data.data_vars if 'CHL' in var.upper())
        values = data[chl_var].values
        
        # Simple mask: NaN or very high values typically indicate land
        water_mask = ~(np.isnan(values) | (values > 20))  # Values > 20 mg/m³ are unrealistic for ocean
        
        logger.info("Land mask generation complete")
        return water_mask

    def _clean_chlorophyll(self, data: xr.Dataset) -> xr.Dataset:
        """Clean chlorophyll data by removing land points and invalid values.
        
        Args:
            data: Dataset containing chlorophyll data
                
        Returns:
            Cleaned dataset with land masked and invalid values removed
        """
        try:            
            # Create copy to avoid modifying input
            cleaned_data = data.copy()
            
            # Get chlorophyll variable (chlor_a is standard name)
            chl_var = 'chlor_a' if 'chlor_a' in data else next(var for var in data.data_vars if 'CHL' in var.upper())
            
            # Get water mask and apply transformations
            values = data[chl_var].values
            
            # Remove invalid values (negative, zero, or unrealistically high)
            values = np.where(values <= 0, np.nan, values)  # Remove negative/zero values
            values = np.where(values > 20, np.nan, values)  # Remove unrealistic values
            
            cleaned_data[chl_var].values = values
            
            return cleaned_data
                
        except Exception as e:
            logger.error(f"Failed to clean chlorophyll data: {str(e)}")
            raise

    def preprocess_dataset(self, data: xr.Dataset | xr.DataArray, dataset_type: str = None) -> xr.Dataset:
        """Preprocess dataset by flattening time dimension, removing depth, and applying type-specific cleaning.
        
        Args:
            data: Dataset containing the variable
            dataset_type: Type of dataset (e.g., 'chlorophyll', 'sst', etc.)
            
        Returns:
            xr.Dataset: Preprocessed dataset with time and depth dimensions removed and cleaning applied
        """
        data = data.to_dataset() if isinstance(data, xr.DataArray) else data
        
        if 'time' in data.dims:
            data = data.isel(time=0)

        if 'depth' in data.dims:
            data = data.isel(depth=0, drop=True)

        if 'altitude' in data.dims:
            data = data.isel(altitude=0, drop=True)

        # Apply type-specific cleaning
        if dataset_type == 'chlorophyll':
            data = self._clean_chlorophyll(data)
        elif dataset_type == 'sst':
            # Convert all temperature variables to Fahrenheit
            temp_vars = [var for var in data.data_vars 
                        if 'temperature' in var.lower() 
                        or var in ['analysed_sst', 'sea_surface_temperature', 'thetao']]
            for var_name in temp_vars:
                logger.info(f"Converting temperature variable {var_name}")
                data[var_name] = self._convert_temperature(data, var_name)

        return data