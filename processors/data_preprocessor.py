import xarray as xr
import logging
from pathlib import Path
from processors.data_cleaners.land_masker import LandMasker
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class DataPreprocessor:
    """Handles data preprocessing operations like land masking"""
    
    def __init__(self):
        self.land_masker = LandMasker()
        
    def preprocess_dataset(self, data: xr.DataArray, dataset: str) -> xr.DataArray:
        """
        Preprocess dataset based on its type
        Args:
            data: Input xarray DataArray
            dataset: Dataset identifier
        Returns:
            Preprocessed xarray DataArray
        """
        dataset_type = SOURCES[dataset]['type']
        
        if dataset_type == 'chlorophyll':
            logger.info(f"Applying land masking to chlorophyll data: {dataset}")
            return self.land_masker.mask_land(data)
            
        return data
    
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