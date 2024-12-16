import xarray as xr
import logging
from pathlib import Path
from processors.data_cleaners.land_masker import LandMasker
from config.settings import SOURCES
import numpy as np
from typing import Union, Dict
from .dataset_standardizer import standardize_dataset

logger = logging.getLogger(__name__)

class DataPreprocessor:
    """Handles data preprocessing operations"""
    
    def __init__(self):
        self.land_masker = LandMasker()
        
    def preprocess_dataset(self, data: Union[xr.DataArray, xr.Dataset, Dict], dataset: str, region: str) -> xr.Dataset:
        """
        Preprocess dataset with standard operations
        Args:
            data: Input data in any supported format
            dataset: Dataset identifier
            region: Region identifier (used for logging)
        Returns:
            Preprocessed xarray Dataset
        """
        try:
            # First standardize the dataset format
            standardized = standardize_dataset(data, dataset)
            logger.info(f"Standardized dataset type: {type(standardized)}")
            
            # Apply preprocessing operations
            preprocessed = self._preprocess_data(standardized, dataset)
            
            # Log data ranges for monitoring
            self._log_data_ranges(preprocessed)
            
            return preprocessed
            
        except Exception as e:
            logger.error(f"Error preprocessing dataset {dataset}: {str(e)}")
            raise
    
    def _preprocess_data(self, data: xr.Dataset, dataset: str) -> xr.Dataset:
        """Preprocess standardized dataset."""
        # Handle dimensions
        data = self._handle_dimensions(data)
        
        # Apply land masking based on dataset type
        dataset_type = SOURCES[dataset]['type']
        if dataset_type in ['currents', 'waves']:
            for var in data.data_vars:
                data[var] = self.land_masker.mask_land(data[var])
        elif dataset_type in ['sst', 'chlorophyll']:
            for var in data.data_vars:
                data[var] = self.land_masker.mask_land(data[var])
        
        return data
    
    def _handle_dimensions(self, data: xr.Dataset, dims: list = None) -> xr.Dataset:
        """Handle dimension selection for data"""
        if dims is None:
            dims = ['time', 'altitude', 'depth']
            
        for var in data.data_vars:
            for dim in dims:
                if dim in data[var].dims:
                    data[var] = data[var].isel({dim: 0})
                    
        return data
    
    def _log_data_ranges(self, data: xr.Dataset):
        """Log data ranges for monitoring"""
        for var in data.data_vars:
            valid_data = data[var].values[~np.isnan(data[var].values)]
            if len(valid_data) > 0:
                logger.info(f"[RANGES] {var} min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
            else:
                logger.warning(f"No valid data for {var} after preprocessing")