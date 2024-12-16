import xarray as xr
import logging
from pathlib import Path
from processors.data_cleaners.land_masker import LandMasker
from config.settings import SOURCES
import numpy as np
from typing import Union, Dict

logger = logging.getLogger(__name__)

class DataPreprocessor:
    """Handles data preprocessing operations"""
    
    def __init__(self):
        self.land_masker = LandMasker()
        
    def preprocess_dataset(self, data: Union[xr.DataArray, xr.Dataset, Dict], dataset: str, region: str) -> Union[xr.DataArray, xr.Dataset, Dict]:
        """
        Preprocess dataset with standard operations
        Args:
            data: Input xarray DataArray, Dataset, or combined data dictionary
            dataset: Dataset identifier
            region: Region identifier (used for logging)
        Returns:
            Preprocessed xarray DataArray, Dataset, or combined data dictionary
        """
        dataset_config = SOURCES[dataset]
        dataset_type = dataset_config['type']
        
        # Handle combined data
        if isinstance(data, dict) and dataset_config.get('source_type') == 'combined_view':
            processed_data = {}
            for source_name, source_data in data.items():
                source_config = dataset_config['source_datasets'][source_name]
                source_type = dataset_config['type']  # Use parent dataset type
                
                if isinstance(source_data['data'], xr.Dataset):
                    processed = self._preprocess_multi_variable(
                        source_data['data'],
                        source_data['variables'],
                        source_type
                    )
                else:
                    processed = self._preprocess_single_variable(
                        source_data['data'],
                        source_type
                    )
                
                processed_data[source_name] = {
                    'data': processed,
                    'variables': source_data['variables'],
                    'config': source_data['config']
                }
            return processed_data
        
        # Handle regular datasets
        variables = dataset_config['variables']
        
        # Handle multi-variable datasets
        if isinstance(data, xr.Dataset):
            return self._preprocess_multi_variable(data, variables, dataset_type)
        
        # Handle single variable data
        return self._preprocess_single_variable(data, dataset_type)
    
    def _handle_dimensions(self, data: Union[xr.DataArray, xr.Dataset], dims: list = None) -> Union[xr.DataArray, xr.Dataset]:
        """Handle dimension selection for data"""
        if dims is None:
            dims = ['time', 'altitude', 'depth']
            
        if isinstance(data, xr.Dataset):
            for var in data.data_vars:
                for dim in dims:
                    if dim in data[var].dims:
                        data[var] = data[var].isel({dim: 0})
        else:
            for dim in dims:
                if dim in data.dims:
                    data = data.isel({dim: 0})
                    
        return data
    
    def _apply_land_mask(self, data: Union[xr.DataArray, xr.Dataset], dataset_type: str) -> Union[xr.DataArray, xr.Dataset]:
        """Apply land masking based on dataset type"""
        if isinstance(data, xr.Dataset):
            if dataset_type in ['currents', 'waves']:
                for var in data.data_vars:
                    data[var] = self.land_masker.mask_land(data[var])
        else:
            if dataset_type in ['sst', 'chlorophyll']:
                data = self.land_masker.mask_land(data)
        return data
    
    def _log_data_ranges(self, data: Union[xr.DataArray, xr.Dataset], variables: list = None):
        """Log data ranges for monitoring"""
        if isinstance(data, xr.Dataset):
            for var in variables or data.data_vars:
                valid_data = data[var].values[~np.isnan(data[var].values)]
                if len(valid_data) > 0:
                    logger.info(f"[RANGES] {var} min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
                else:
                    logger.warning(f"No valid data for {var} after preprocessing")
        else:
            valid_data = data.values[~np.isnan(data.values)]
            if len(valid_data) > 0:
                logger.info(f"[RANGES] Processed data min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
            else:
                logger.warning("No valid data after preprocessing")
    
    def _preprocess_multi_variable(self, data: xr.Dataset, variables: list, dataset_type: str) -> xr.Dataset:
        """Preprocess dataset with multiple variables."""
        # Handle dimensions
        data = self._handle_dimensions(data)
        
        # Apply land masking
        data = self._apply_land_mask(data, dataset_type)
        
        # Log ranges
        self._log_data_ranges(data, variables)
        
        return data
        
    def _preprocess_single_variable(self, data: xr.DataArray, dataset_type: str) -> xr.DataArray:
        """Preprocess single variable dataset."""
        # Handle dimensions
        data = self._handle_dimensions(data)
        
        # Apply land masking
        data = self._apply_land_mask(data, dataset_type)
        
        # Log ranges
        self._log_data_ranges(data)
        
        return data