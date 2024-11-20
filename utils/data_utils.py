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
    """Interpolates dataset using efficient chunked operations"""
    # Get coordinate names
    lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
    lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
    
    # Create new coordinate arrays with double the points
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

    # Perform interpolation directly with xarray
    return ds.interp(
        {lat_name: new_lats, lon_name: new_lons},
        method=method,
        kwargs={'fill_value': None}
    )
