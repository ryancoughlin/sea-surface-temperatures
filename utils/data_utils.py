import numpy as np
import xarray as xr
from typing import Tuple
from scipy.ndimage import zoom

def convert_temperature_to_f(data: xr.DataArray, source_unit: str = None) -> xr.DataArray:
    """Convert temperature data to Fahrenheit.
    
    Args:
        data: Temperature data array
        source_unit: Source unit ('C', 'K', or None for auto-detection)
    
    Returns:
        Temperature data in Fahrenheit
    """
    # Auto-detect unit if not specified
    if source_unit is None:
        if np.max(data) > 100:  # Assuming Kelvin if max temp is over 100
            source_unit = 'K'
        else:  # Assuming Celsius otherwise
            source_unit = 'C'
        
    # Convert to Fahrenheit
    if source_unit == 'C':
        return data * 9/5 + 32
    elif source_unit == 'K':
        return (data - 273.15) * 9/5 + 32
    else:
        raise ValueError("Unsupported temperature unit. Use 'C' for Celsius or 'K' for Kelvin.")


def interpolate_data(data: xr.DataArray, factor: int = 2) -> np.ndarray:
    """
    Interpolate gridded data while preserving coordinates.
    
    Args:
        data: Input xarray DataArray
        factor: Interpolation factor (2 = double resolution)
    Returns:
        Interpolated numpy array matching original dimensions
    """
    
    # Handle NaN values before interpolation
    filled_data = np.nan_to_num(data.values, nan=0.0)
    
    # Use zoom for interpolation (preserves grid structure)
    return zoom(filled_data, factor, order=1)
