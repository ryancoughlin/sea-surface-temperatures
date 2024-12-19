import xarray as xr
import logging
from pathlib import Path
from processors.data_cleaners.land_masker import LandMasker
from config.settings import SOURCES
import numpy as np
from typing import Union, Dict
from .data_utils import standardize_dataset

logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        self.land_masker = LandMasker()
    
    def preprocess_dataset(self, data: Union[xr.DataArray, xr.Dataset, Dict], dataset: str, region: str) -> xr.Dataset:
        try:
            # Standardize the dataset format
            standardized = standardize_dataset(data, dataset)
            logger.info(f"Standardized dataset type: {type(standardized)}")
            
            # Handle dimensions and apply land masking
            preprocessed = self._preprocess_data(standardized, dataset)
            
            # Log data ranges
            self._log_data_ranges(preprocessed)
            
            return preprocessed
            
        except Exception as e:
            logger.error(f"Error preprocessing dataset {dataset}: {str(e)}")
            raise
    
    def _preprocess_data(self, data: xr.Dataset, dataset: str) -> xr.Dataset:
        # Handle dimensions
        data = self._handle_dimensions(data)
        
        # Apply land masking based on dataset type
        dataset_type = SOURCES[dataset]['type']
        if dataset_type in ['sst', 'chlorophyll', 'currents', 'waves']:
            for var in data.data_vars:
                data[var] = self.land_masker.mask_land(data[var])
        
        return data
    
    def _handle_dimensions(self, data: xr.Dataset) -> xr.Dataset:
        dims_to_reduce = ['time', 'altitude', 'depth']
        
        for var in data.data_vars:
            for dim in dims_to_reduce:
                if dim in data[var].dims:
                    data[var] = data[var].isel({dim: 0})
        
        return data
    
    def _log_data_ranges(self, data: xr.Dataset):
        for var in data.data_vars:
            values = data[var].values
            valid_values = values[~np.isnan(values)]
            if len(valid_values) > 0:
                logger.info(f"Variable {var} range: {valid_values.min():.4f} to {valid_values.max():.4f}")
            else:
                logger.warning(f"No valid data found for variable {var}")