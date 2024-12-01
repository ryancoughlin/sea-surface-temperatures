import xarray as xr
import logging
from pathlib import Path
from processors.data_cleaners.land_masker import LandMasker
from config.settings import SOURCES
from config.regions import REGIONS
import numpy as np

logger = logging.getLogger(__name__)

class DataPreprocessor:
    """Handles data preprocessing operations like land masking"""
    
    def __init__(self):
        self.land_masker = LandMasker()
        
    def preprocess_dataset(self, data: xr.DataArray, dataset: str, region: str) -> xr.DataArray:
        """
        Preprocess dataset based on its type
        Args:
            data: Input xarray DataArray
            dataset: Dataset identifier
            region: Region identifier
        Returns:
            Preprocessed xarray DataArray
        """
        dataset_type = SOURCES[dataset]['type']
        
        if dataset_type == 'chlorophyll':
            logger.info(f"Preprocessing chlorophyll data: {dataset}")
            return self.preprocess_chlorophyll(data, region)
            
        return data
    
    def preprocess_chlorophyll(self, data: xr.DataArray, region: str) -> xr.DataArray:
        """Preprocess chlorophyll data into one clean dataset for all processors."""
        # Handle dimensions
        for dim in ['time', 'altitude']:
            if dim in data.dims:
                data = data.isel({dim: 0})
        
        # Mask to region
        lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
        lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
        bounds = REGIONS[region]['bounds']
        
        regional_data = data.where(
            (data[lon_name] >= bounds[0][0]) & 
            (data[lon_name] <= bounds[1][0]) &
            (data[lat_name] >= bounds[0][1]) & 
            (data[lat_name] <= bounds[1][1]),
            drop=True
        )
        
        # Apply land masking
        masked_data = self.land_masker.mask_land(regional_data)
        
        # Log the ranges of the final preprocessed data
        valid_data = masked_data.values[~np.isnan(masked_data.values)]
        if len(valid_data) > 0:
            logger.info(f"[RANGES] Preprocessed data min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
        else:
            logger.warning("No valid data after preprocessing")
        
        return masked_data
    
    def save_preprocessed(self, data: xr.DataArray, output_path: Path) -> Path:
        """
        Save preprocessed data to NetCDF file
        Args:
            data: Preprocessed xarray DataArray
            output_path: Path to save preprocessed data
        Returns:
            Path to saved preprocessed file
        """
        data.to_netcdf(output_path)
        return output_path 