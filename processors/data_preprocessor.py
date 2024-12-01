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
        
    def preprocess_dataset(self, data: xr.DataArray, dataset: str, region: str) -> xr.DataArray:
        """
        Preprocess dataset with standard operations
        Args:
            data: Input xarray DataArray
            dataset: Dataset identifier
            region: Region identifier
        Returns:
            Preprocessed xarray DataArray
        """
        # 1. Handle dimensions
        for dim in ['time', 'altitude', 'depth']:
            if dim in data.dims:
                data = data.isel({dim: 0})
        
        # 2. Mask to region
        lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
        lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
        bounds = REGIONS[region]['bounds']
        
        data = data.where(
            (data[lon_name] >= bounds[0][0]) & 
            (data[lon_name] <= bounds[1][0]) &
            (data[lat_name] >= bounds[0][1]) & 
            (data[lat_name] <= bounds[1][1]),
            drop=True
        )
        
        # 3. Apply type-specific processing
        dataset_type = SOURCES[dataset]['type']
        if dataset_type in ['chlorophyll', 'sst']:  # Both need land masking
            data = self.land_masker.mask_land(data)
        
        # Log data ranges
        valid_data = data.values[~np.isnan(data.values)]
        if len(valid_data) > 0:
            logger.info(f"[RANGES] Processed data min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
        else:
            logger.warning("No valid data after preprocessing")
        
        return data