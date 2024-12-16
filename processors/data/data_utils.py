import xarray as xr
import numpy as np
import logging
from typing import Union, Dict, List, Optional, Tuple
from config.settings import SOURCES
from processors.data_cleaners.land_masker import LandMasker

logger = logging.getLogger(__name__)

def validate_coordinates(ds: xr.Dataset) -> List[str]:
    """Validate required coordinates exist."""
    missing = []
    for coord in ['longitude', 'latitude']:
        if coord not in ds.coords and coord not in ds.dims:
            missing.append(coord)
    return missing

def standardize_coordinates(data: Union[xr.Dataset, xr.DataArray]) -> Union[xr.Dataset, xr.DataArray]:
    """Standardize coordinate names."""
    # Handle lat/latitude and lon/longitude
    renames = {}
    if 'lat' in data.coords or 'lat' in data.dims:
        renames['lat'] = 'latitude'
    if 'lon' in data.coords or 'lon' in data.dims:
        renames['lon'] = 'longitude'
    
    if renames:
        data = data.rename(renames)
        
    return data

def reduce_dimensions(data: Union[xr.Dataset, xr.DataArray], 
                     dims_to_reduce: Optional[List[str]] = None) -> Union[xr.Dataset, xr.DataArray]:
    """Reduce extra dimensions by selecting first index."""
    if dims_to_reduce is None:
        dims_to_reduce = ['time', 'depth', 'altitude']
        
    for dim in dims_to_reduce:
        if dim in data.dims:
            data = data.isel({dim: 0})
            logger.info(f"Reduced {dim} dimension")
    
    return data

def ensure_data_type(data: Union[xr.Dataset, xr.DataArray, Dict], 
                    dataset_id: str) -> Tuple[xr.Dataset, List[str]]:
    """Ensure data is in correct format and type."""
    try:
        # Handle dictionary input
        if isinstance(data, dict):
            if 'data' in data:
                data = data['data']
            elif any(k in data for k in ['altimetry', 'currents']):
                # Handle combined datasets
                components = []
                for name, component in data.items():
                    if isinstance(component, dict) and 'data' in component:
                        components.append(component['data'])
                    elif isinstance(component, (xr.Dataset, xr.DataArray)):
                        components.append(component)
                return xr.merge(components), []
        
        # Convert DataArray to Dataset if needed
        if isinstance(data, xr.DataArray):
            data = data.to_dataset()
        
        # Validate we have a Dataset
        if not isinstance(data, xr.Dataset):
            raise TypeError(f"Expected xarray Dataset, got {type(data)}")
        
        # Add source information
        data.attrs['source_dataset'] = dataset_id
        
        return data, validate_coordinates(data)
        
    except Exception as e:
        logger.error(f"Error ensuring data type: {str(e)}")
        raise

def apply_land_mask(data: Union[xr.Dataset, xr.DataArray], 
                   dataset: str) -> Union[xr.Dataset, xr.DataArray]:
    """Apply land masking based on dataset type."""
    dataset_type = SOURCES[dataset]['type']
    land_masker = LandMasker()
    
    if dataset_type in ['currents', 'waves', 'sst', 'chlorophyll']:
        if isinstance(data, xr.Dataset):
            for var in data.data_vars:
                data[var] = land_masker.mask_land(data[var])
        else:
            data = land_masker.mask_land(data)
    
    return data

def log_data_ranges(data: Union[xr.Dataset, xr.DataArray], name: str = ""):
    """Log data ranges for monitoring."""
    if isinstance(data, xr.Dataset):
        for var in data.data_vars:
            _log_variable_range(data[var], f"{name}:{var}" if name else var)
    else:
        _log_variable_range(data, name or "data")

def _log_variable_range(da: xr.DataArray, name: str):
    """Log range for a single variable."""
    valid_data = da.values[~np.isnan(da.values)]
    if len(valid_data) > 0:
        logger.info(f"[RANGES] {name} min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
    else:
        logger.warning(f"No valid data for {name}")

def standardize_dataset(data: Union[xr.Dataset, xr.DataArray, Dict], 
                       dataset: str,
                       region: str = None) -> xr.Dataset:
    """
    Main entry point for data standardization.
    Returns a standardized xarray Dataset with consistent coordinates and types.
    
    Args:
        data: Input data in any supported format
        dataset: Dataset identifier
        region: Optional region identifier for logging
    
    Returns:
        Standardized xarray Dataset
    """
    try:
        logger.info(f"Standardizing dataset: {dataset}" + (f" for {region}" if region else ""))
        
        # 1. Ensure correct data type and structure
        data, missing_coords = ensure_data_type(data, dataset)
        if missing_coords:
            raise ValueError(f"Missing coordinates: {missing_coords}")
        
        # 2. Standardize coordinates
        data = standardize_coordinates(data)
        
        # 3. Reduce extra dimensions
        data = reduce_dimensions(data)
        
        # 4. Apply land masking
        data = apply_land_mask(data, dataset)
        
        # 5. Log data ranges
        log_data_ranges(data, dataset)
        
        return data
        
    except Exception as e:
        logger.error(f"Error standardizing {dataset}: {str(e)}")
        raise 