import xarray as xr
import numpy as np
import logging
from typing import Union, Dict, List, Optional, Tuple
from config.settings import SOURCES
from processors.data_cleaners.land_masker import LandMasker

logger = logging.getLogger(__name__)

# Coordinate handling functions
def get_coordinate_names(dataset) -> tuple[str, str]:
    """Get standardized longitude and latitude coordinate names."""
    lon_patterns = ['lon', 'longitude', 'x']
    lat_patterns = ['lat', 'latitude', 'y']
    
    lon_name = None
    lat_name = None
    
    for var in dataset.coords:
        var_lower = var.lower()
        if any(pattern in var_lower for pattern in lon_patterns):
            lon_name = var
        elif any(pattern in var_lower for pattern in lat_patterns):
            lat_name = var
            
    if not lon_name or not lat_name:
        raise ValueError("Could not identify coordinate variables")
        
    return lon_name, lat_name

def validate_coordinates(ds: xr.Dataset) -> List[str]:
    """Validate required coordinates exist."""
    missing = []
    for coord in ['longitude', 'latitude']:
        if coord not in ds.coords and coord not in ds.dims:
            missing.append(coord)
    return missing

def standardize_coordinates(data: Union[xr.Dataset, xr.DataArray]) -> Union[xr.Dataset, xr.DataArray]:
    """Standardize coordinate names to longitude/latitude."""
    renames = {}
    if 'lat' in data.coords or 'lat' in data.dims:
        renames['lat'] = 'latitude'
    if 'lon' in data.coords or 'lon' in data.dims:
        renames['lon'] = 'longitude'
    
    if renames:
        data = data.rename(renames)
    return data

# Data processing functions
def extract_variables(data: Union[xr.Dataset, xr.DataArray], dataset: str) -> Tuple[Union[xr.Dataset, xr.DataArray], List[str]]:
    """Extract variables from dataset based on configuration."""
    # First try to get config directly
    dataset_config = SOURCES.get(dataset)
    
    # If not found, check if it's a component of a combined dataset
    if not dataset_config:
        for source_config in SOURCES.values():
            if source_config.get('source_type') == 'combined_view':
                for component_info in source_config['source_datasets'].values():
                    if component_info['dataset_id'] == dataset:
                        dataset_config = {'variables': component_info['variables']}
                        break
                if dataset_config:
                    break
    
    if not dataset_config:
        raise ValueError(f"Dataset {dataset} not found in configuration")
        
    variables = dataset_config['variables']
    var_names = list(variables.keys())

    if isinstance(data, xr.Dataset):
        if len(var_names) > 1:
            processed_data = data[var_names]
            logger.info(f"Loaded variables: {', '.join(var_names)}")
            return processed_data, var_names
        else:
            var_name = var_names[0]
            processed_data = data[var_name]
            logger.info(f"Loaded variable: {var_name}")
            return processed_data, [var_name]
    return data, var_names

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

def ensure_data_type(data: Union[xr.Dataset, xr.DataArray], 
                    dataset_id: str) -> Tuple[xr.Dataset, List[str]]:
    """Ensure data is in correct format and type."""
    try:
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

# Data transformation functions
def convert_temperature_to_f(data: xr.DataArray, source_unit: str = None) -> xr.DataArray:
    """Convert temperature data to Fahrenheit."""
    if source_unit is None:
        if np.max(data) > 100:  # Assuming Kelvin if max temp is over 100
            source_unit = 'K'
        else:  # Assuming Celsius otherwise
            source_unit = 'C'
        
    if source_unit == 'C':
        return data * 9/5 + 32
    elif source_unit == 'K':
        return (data - 273.15) * 9/5 + 32
    else:
        raise ValueError("Unsupported temperature unit. Use 'C' for Celsius or 'K' for Kelvin.")

def interpolate_dataset(ds: xr.Dataset, factor: int = 1.4, method: str = 'linear') -> xr.Dataset:
    """Interpolate dataset to higher resolution."""
    lon_name, lat_name = get_coordinate_names(ds)
    
    new_lats = np.linspace(
        float(ds[lat_name].min()), 
        float(ds[lat_name].max()), 
        int(len(ds[lat_name]) * factor)
    )
    new_lons = np.linspace(
        float(ds[lon_name].min()), 
        float(ds[lon_name].max()), 
        int(len(ds[lon_name]) * factor)
    )

    return ds.interp(
        {lat_name: new_lats, lon_name: new_lons},
        method=method,
        kwargs={'fill_value': None}
    )

# Data cleaning functions
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

# Logging functions
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
        logger.info("data", da)
        logger.warning(f"No valid data for {name}")

# Main entry point
def standardize_dataset(data: Union[xr.Dataset, xr.DataArray], 
                       dataset: str,
                       region: str = None) -> xr.Dataset:
    """
    Main entry point for data standardization.
    Returns a standardized xarray Dataset with consistent coordinates and types.
    """
    try:
        logger.info(f"Standardizing dataset: {dataset}" + (f" for {region}" if region else ""))
        
        # 1. Ensure correct data type and structure
        data, missing_coords = ensure_data_type(data, dataset)
        if missing_coords:
            raise ValueError(f"Missing coordinates: {missing_coords}")
        
        # 2. Remove depth coordinate and variable if present
        if 'depth' in data.coords:
            data = data.drop_vars('depth')
        if 'depth' in data.variables:
            data = data.drop_vars('depth')
        
        # 3. Standardize coordinates
        data = standardize_coordinates(data)
        
        # 4. Reduce extra dimensions
        data = reduce_dimensions(data)
        
        # 5. Apply land masking
        data = apply_land_mask(data, dataset)
        
        # 6. Log data ranges
        log_data_ranges(data, dataset)
        
        return data
        
    except Exception as e:
        logger.error(f"Error standardizing {dataset}: {str(e)}")
        raise