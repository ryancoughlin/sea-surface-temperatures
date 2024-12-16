import xarray as xr
import logging
from typing import Union, Dict
import numpy as np
from processors.data_cleaners.land_masker import LandMasker
from config.settings import SOURCES

logger = logging.getLogger(__name__)

def standardize_dataset(data: Union[xr.Dataset, Dict], dataset: str, region: str) -> xr.Dataset:
    """
    Standardize and preprocess dataset for the pipeline.
    Handles both single datasets and combined datasets.
    """
    try:
        logger.info(f"Standardizing dataset: {dataset}")
        logger.info(f"Input data type: {type(data)}")
        
        # Handle combined datasets (e.g., Ocean_Dynamics_Combined)
        if isinstance(data, dict) and any(k in data for k in ['altimetry', 'currents']):
            logger.info("Processing combined dataset")
            components = []
            for name, component in data.items():
                if isinstance(component, xr.Dataset):
                    ds = component
                elif isinstance(component, dict) and 'data' in component:
                    ds = component['data']
                else:
                    raise ValueError(f"Invalid component format for {name}")
                    
                components.append(_preprocess_component(ds, dataset))
            
            return xr.merge(components)
            
        # Handle single dataset
        if isinstance(data, dict) and 'data' in data:
            data = data['data']
        
        if not isinstance(data, xr.Dataset):
            raise ValueError(f"Expected xarray Dataset, got {type(data)}")
            
        return _preprocess_component(data, dataset)
        
    except Exception as e:
        logger.error(f"Error standardizing dataset {dataset}: {str(e)}")
        raise

def _preprocess_component(ds: xr.Dataset, dataset: str) -> xr.Dataset:
    """Preprocess a single dataset component."""
    # Handle dimensions
    ds = _handle_dimensions(ds)
    
    # Standardize coordinates
    ds = _standardize_coordinates(ds)
    
    # Apply land masking based on dataset type
    ds = _apply_land_mask(ds, dataset)
    
    # Log data ranges for monitoring
    _log_data_ranges(ds)
    
    return ds

def _handle_dimensions(ds: xr.Dataset, dims_to_reduce: list = None) -> xr.Dataset:
    """Handle dimension selection and reduction."""
    if dims_to_reduce is None:
        dims_to_reduce = ['time', 'depth', 'altitude']
        
    for dim in dims_to_reduce:
        if dim in ds.dims:
            ds = ds.isel({dim: 0})
            logger.info(f"Reduced {dim} dimension")
            
    return ds

def _standardize_coordinates(ds: xr.Dataset) -> xr.Dataset:
    """Standardize coordinate names."""
    logger.info(f"Original coordinates: {list(ds.coords)}")
    
    # Handle lat/latitude and lon/longitude
    if 'lat' in ds.coords or 'lat' in ds.dims:
        ds = ds.rename({'lat': 'latitude'})
    if 'lon' in ds.coords or 'lon' in ds.dims:
        ds = ds.rename({'lon': 'longitude'})
    
    logger.info(f"Standardized coordinates: {list(ds.coords)}")
    return ds

def _apply_land_mask(ds: xr.Dataset, dataset: str) -> xr.Dataset:
    """Apply land masking based on dataset type."""
    dataset_type = SOURCES[dataset]['type']
    land_masker = LandMasker()
    
    if dataset_type in ['currents', 'waves', 'sst', 'chlorophyll']:
        for var in ds.data_vars:
            ds[var] = land_masker.mask_land(ds[var])
            
    return ds

def _log_data_ranges(ds: xr.Dataset):
    """Log data ranges for monitoring."""
    for var in ds.data_vars:
        valid_data = ds[var].values[~np.isnan(ds[var].values)]
        if len(valid_data) > 0:
            logger.info(f"[RANGES] {var} min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
        else:
            logger.warning(f"No valid data for {var} after preprocessing") 