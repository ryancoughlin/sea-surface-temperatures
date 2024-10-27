import numpy as np
import xarray as xr
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


def interpolate_data(data: xr.DataArray, factor: int = 2) -> xr.DataArray:
    """Interpolate data to add more points."""
    
    latitude = data.coords['latitude']
    longitude = data.coords['longitude']
    
    new_lat = np.linspace(latitude.min(), latitude.max(), len(latitude) * factor)
    new_lon = np.linspace(longitude.min(), longitude.max(), len(longitude) * factor)
    
    interpolated_data = data.interp(
        latitude=new_lat, 
        longitude=new_lon, 
        method='linear'  # Changed to linear temporarily
    )
    
    return interpolated_data
