import xarray as xr
from typing import Union, Dict
import logging
import numpy as np

logger = logging.getLogger(__name__)

def standardize_coordinates(ds: xr.Dataset) -> xr.Dataset:
    """
    Standardize coordinate names and handle depth selection.
    """
    logger.info(f"Original coordinates and dims: coords={list(ds.coords)}, dims={list(ds.dims)}")
    
    # If depth is present, select only the first level
    if 'depth' in ds.dims:
        logger.info("Found depth dimension, selecting first level")
        ds = ds.isel(depth=0)
    
    # Handle lat/latitude and lon/longitude if needed
    if 'lat' in ds.coords or 'lat' in ds.dims:
        ds = ds.rename({'lat': 'latitude'})
    if 'lon' in ds.coords or 'lon' in ds.dims:
        ds = ds.rename({'lon': 'longitude'})
    
    # Verify required coordinates exist
    missing_coords = []
    if 'longitude' not in ds.coords and 'longitude' not in ds.dims:
        missing_coords.append('longitude')
    if 'latitude' not in ds.coords and 'latitude' not in ds.dims:
        missing_coords.append('latitude')
        
    if missing_coords:
        available_coords = list(ds.coords) + list(ds.dims)
        raise ValueError(f"Missing required coordinates {missing_coords}. Available: {available_coords}")
    
    logger.info(f"Final coordinates and dims: coords={list(ds.coords)}, dims={list(ds.dims)}")
    return ds

def standardize_dataset(data: Union[xr.Dataset, xr.DataArray, Dict], dataset_id: str) -> xr.Dataset:
    """
    Convert input data to standardized xarray Dataset.
    Returns a standardized xarray Dataset with consistent coordinates.
    """
    try:
        logger.info(f"Starting standardization for {dataset_id}")
        logger.info(f"Input data type: {type(data)}")
        
        if isinstance(data, xr.Dataset):
            logger.info("Processing xarray Dataset")
            ds = data
            
        elif isinstance(data, xr.DataArray):
            logger.info("Converting xarray DataArray to Dataset")
            ds = data.to_dataset()
            
        elif isinstance(data, dict):
            if 'data' not in data:
                raise ValueError(f"No 'data' key found in dictionary for dataset {dataset_id}")
                
            logger.info(f"Processing data from dictionary for {dataset_id}")
            data_component = data['data']
            
            if isinstance(data_component, xr.Dataset):
                ds = data_component
            elif isinstance(data_component, xr.DataArray):
                ds = data_component.to_dataset()
            else:
                raise ValueError(f"Data component is not an xarray type: {type(data_component)}")
        else:
            raise TypeError(f"Unsupported data type {type(data)} for dataset {dataset_id}")
        
        # Standardize coordinates
        ds = standardize_coordinates(ds)
        
        # Add source information
        ds.attrs['source_dataset'] = dataset_id
        
        # Log final state
        logger.info(f"Standardized dataset info:")
        logger.info(f"- Variables: {list(ds.data_vars)}")
        logger.info(f"- Coordinates: {list(ds.coords)}")
        logger.info(f"- Dimensions: {list(ds.dims)}")
        
        # Return the standardized dataset directly
        return ds
        
    except Exception as e:
        logger.error(f"Error standardizing dataset {dataset_id}: {str(e)}")
        raise