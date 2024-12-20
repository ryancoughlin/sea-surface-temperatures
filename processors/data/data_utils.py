import xarray as xr
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from config.settings import SOURCES

logger = logging.getLogger(__name__)

def get_coordinate_names(dataset: xr.Dataset) -> tuple[str, str]:
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

def convert_temperature_to_f(data: xr.Dataset, source_unit: str = None) -> xr.Dataset:
    """Convert temperature data to Fahrenheit."""
    if source_unit is None:
        first_var = next(iter(data.data_vars))
        if np.max(data[first_var]) > 100:  # Assuming Kelvin if max temp is over 100
            source_unit = 'K'
        else:  # Assuming Celsius otherwise
            source_unit = 'C'
    
    for var in data.data_vars:
        if source_unit == 'C':
            data[var] = data[var] * 9/5 + 32
        elif source_unit == 'K':
            data[var] = (data[var] - 273.15) * 9/5 + 32
        else:
            raise ValueError("Unsupported temperature unit. Use 'C' for Celsius or 'K' for Kelvin.")
    
    return data