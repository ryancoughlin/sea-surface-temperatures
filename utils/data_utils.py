import numpy as np
import xarray as xr
from typing import Tuple
from scipy.ndimage import zoom
from scipy.interpolate import griddata

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


def interpolate_currents(u_data: xr.DataArray, v_data: xr.DataArray, 
                        target_points: Tuple[np.ndarray, np.ndarray]) -> Tuple[np.ndarray, np.ndarray]:
    """
    Interpolate current vector components (u,v) to new grid points.
    
    Args:
        u_data: U component of current velocity
        v_data: V component of current velocity
        target_points: Tuple of (x_points, y_points) meshgrid arrays for target locations
    
    Returns:
        Tuple of interpolated (u, v) arrays
    """
    # Get coordinate names
    lon_name = 'longitude' if 'longitude' in u_data.coords else 'lon'
    lat_name = 'latitude' if 'latitude' in u_data.coords else 'lat'
    
    # Create source coordinate meshgrid
    x_src, y_src = np.meshgrid(u_data[lon_name], u_data[lat_name])
    
    # Handle NaN values in source data
    u_valid = ~np.isnan(u_data.values)
    v_valid = ~np.isnan(v_data.values)
    valid_mask = u_valid & v_valid
    
    # Flatten arrays for interpolation
    points = np.column_stack((x_src[valid_mask].ravel(), y_src[valid_mask].ravel()))
    u_values = u_data.values[valid_mask].ravel()
    v_values = v_data.values[valid_mask].ravel()
    
    # Interpolate each component
    x_target, y_target = target_points
    xi = np.column_stack((x_target.ravel(), y_target.ravel()))
    
    u_interp = griddata(points, u_values, xi, method='linear')
    v_interp = griddata(points, v_values, xi, method='linear')
    
    # Reshape back to grid
    u_interp = u_interp.reshape(x_target.shape)
    v_interp = v_interp.reshape(y_target.shape)
    
    return u_interp, v_interp
