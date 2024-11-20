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

def calculate_wave_energy(height: xr.DataArray, period: xr.DataArray) -> xr.DataArray:
    """
    Calculate wave energy density.
    
    Args:
        height: Significant wave height (m)
        period: Wave period (s)
    
    Returns:
        Wave energy density in kJ/m²
    """
    rho = 1025  # Seawater density (kg/m³)
    g = 9.81    # Gravitational acceleration (m/s²)
    
    # Wave energy density formula: E = (1/16) * ρ * g * H² * T
    energy = (1/16) * rho * g * height**2 * period
    
    # Convert to kJ/m²
    return energy / 1000

def calculate_optimal_fishing_zones(
    height: xr.DataArray,
    period: xr.DataArray,
    direction: xr.DataArray
) -> xr.DataArray:
    """
    Calculate fishing condition scores based on wave parameters.
    
    Args:
        height: Significant wave height (m)
        period: Wave period (s)
        direction: Wave direction (degrees)
    
    Returns:
        Fishing condition score (0-1)
    """
    # Define ideal conditions
    ideal_height = 1.0  # meters
    ideal_period = 8.0  # seconds
    
    # Calculate scores for each parameter
    height_score = np.exp(-(height - ideal_height)**2 / 2)
    period_score = np.exp(-(period - ideal_period)**2 / 4)
    
    # Combine scores (weighted average)
    total_score = (0.6 * height_score + 0.4 * period_score)
    
    return total_score

def calculate_wave_steepness(height: xr.DataArray, period: xr.DataArray) -> xr.DataArray:
    """
    Calculate wave steepness (height/wavelength ratio).
    
    Args:
        height: Significant wave height (m)
        period: Wave period (s)
    
    Returns:
        Wave steepness (dimensionless ratio)
    """
    g = 9.81  # gravitational acceleration (m/s²)
    wavelength = (g * period**2) / (2 * np.pi)
    return height / wavelength

def interpolate_dataset(ds: xr.Dataset, factor: int = 1.4, method: str = 'linear') -> xr.Dataset:
    """
    Interpolates all variables in a dataset to a higher resolution grid using bilinear interpolation.
    
    Args:
        ds (xr.Dataset): Input dataset containing multiple variables
        factor (int): Factor by which to increase resolution
        method (str): Interpolation method ('linear', 'cubic', 'nearest')
        
    Returns:
        xr.Dataset: Interpolated dataset with all variables
    """
    # Get coordinate names
    lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
    lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
    
    # Create higher resolution coordinate grids
    lons = ds[lon_name].values
    lats = ds[lat_name].values
    
    new_lons = np.linspace(lons.min(), lons.max(), len(lons) * factor)
    new_lats = np.linspace(lats.min(), lats.max(), len(lats) * factor)
    
    # Create meshgrids for original and new coordinates
    lon_mesh, lat_mesh = np.meshgrid(lons, lats)
    new_lon_mesh, new_lat_mesh = np.meshgrid(new_lons, new_lats)
    
    # Initialize dictionary for interpolated data variables
    interpolated_data = {}
    
    # Interpolate each variable
    for var_name, var in ds.data_vars.items():
        if len(var.dims) >= 2:  # Only interpolate 2D or higher arrays
            # Handle time dimension if present
            if 'time' in var.dims:
                times = var.time
                interpolated_time_series = []
                
                for t in range(len(times)):
                    values = var.isel(time=t).values
                    interpolated = griddata(
                        (lon_mesh.ravel(), lat_mesh.ravel()),
                        values.ravel(),
                        (new_lon_mesh, new_lat_mesh),
                        method=method
                    )
                    interpolated_time_series.append(interpolated)
                
                interpolated_data[var_name] = xr.DataArray(
                    np.stack(interpolated_time_series),
                    dims=['time', lat_name, lon_name],
                    coords={
                        'time': times,
                        lon_name: new_lons,
                        lat_name: new_lats
                    }
                )
            else:
                values = var.values
                interpolated = griddata(
                    (lon_mesh.ravel(), lat_mesh.ravel()),
                    values.ravel(),
                    (new_lon_mesh, new_lat_mesh),
                    method=method
                )
                
                interpolated_data[var_name] = xr.DataArray(
                    interpolated,
                    dims=[lat_name, lon_name],
                    coords={
                        lon_name: new_lons,
                        lat_name: new_lats
                    }
                )
    
    return xr.Dataset(interpolated_data)
