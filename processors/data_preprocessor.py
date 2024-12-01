import xarray as xr
import logging
from pathlib import Path
from processors.data_cleaners.land_masker import LandMasker
from config.settings import SOURCES
from config.regions import REGIONS
import numpy as np

logger = logging.getLogger(__name__)

class DataPreprocessor:
    """Handles data preprocessing operations"""
    
    def __init__(self):
        self.land_masker = LandMasker()
        
    def preprocess_dataset(self, data: xr.DataArray | xr.Dataset, dataset: str, region: str) -> xr.DataArray | xr.Dataset:
        """
        Preprocess dataset with standard operations
        Args:
            data: Input xarray DataArray or Dataset
            dataset: Dataset identifier
            region: Region identifier
        Returns:
            Preprocessed xarray DataArray or Dataset
        """
        dataset_type = SOURCES[dataset]['type']
        variables = SOURCES[dataset]['variables']
        
        # Handle multi-variable datasets
        if isinstance(data, xr.Dataset):
            return self._preprocess_multi_variable(data, variables, dataset, region)
        
        # Handle single variable data
        return self._preprocess_single_variable(data, dataset, region)
        
    def _preprocess_multi_variable(self, data: xr.Dataset, variables: list, dataset: str, region: str) -> xr.Dataset:
        """Preprocess dataset with multiple variables."""
        # Handle dimensions for all variables
        for var in variables:
            for dim in ['time', 'altitude', 'depth']:
                if dim in data[var].dims:
                    data[var] = data[var].isel({dim: 0})
        
        # Get coordinate names from first variable (they should be the same for all)
        first_var = variables[0]
        lon_name = 'longitude' if 'longitude' in data[first_var].coords else 'lon'
        lat_name = 'latitude' if 'latitude' in data[first_var].coords else 'lat'
        bounds = REGIONS[region]['bounds']
        
        # Apply bounds to all variables
        for var in variables:
            data[var] = data[var].where(
                (data[lon_name] >= bounds[0][0]) & 
                (data[lon_name] <= bounds[1][0]) &
                (data[lat_name] >= bounds[0][1]) & 
                (data[lat_name] <= bounds[1][1])
            )
        
        # Apply land masking for all variables if needed
        dataset_type = SOURCES[dataset]['type']
        if dataset_type in ['currents', 'waves']:  # These need land masking
            for var in variables:
                data[var] = self.land_masker.mask_land(data[var])
        
        # Log ranges for each variable
        for var in variables:
            valid_data = data[var].values[~np.isnan(data[var].values)]
            if len(valid_data) > 0:
                logger.info(f"[RANGES] {var} min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
            else:
                logger.warning(f"No valid data for {var} after preprocessing")
        
        return data
        
    def _preprocess_single_variable(self, data: xr.DataArray, dataset: str, region: str) -> xr.DataArray:
        """Preprocess single variable dataset."""
        # Handle dimensions
        for dim in ['time', 'altitude', 'depth']:
            if dim in data.dims:
                data = data.isel({dim: 0})
        
        # Get coordinate names
        lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
        lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
        bounds = REGIONS[region]['bounds']
        
        # Apply bounds
        data = data.where(
            (data[lon_name] >= bounds[0][0]) & 
            (data[lon_name] <= bounds[1][0]) &
            (data[lat_name] >= bounds[0][1]) & 
            (data[lat_name] <= bounds[1][1])
        )
        
        # Apply type-specific processing
        dataset_type = SOURCES[dataset]['type']
        
        # Apply land masking for ocean data
        if dataset_type in ['sst', 'chlorophyll']:
            data = self.land_masker.mask_land(data)
        
        # Log ranges
        valid_data = data.values[~np.isnan(data.values)]
        if len(valid_data) > 0:
            logger.info(f"[RANGES] Processed data min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
        else:
            logger.warning("No valid data after preprocessing")
        
        return data