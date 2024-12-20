import xarray as xr
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from config.settings import SOURCES
from processors.data_cleaners.land_masker import LandMasker

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

def extract_variables(data: xr.Dataset, dataset: str) -> Tuple[xr.Dataset, List[str]]:
    """Extract variables from dataset based on configuration."""
    dataset_config = SOURCES.get(dataset)
    
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

    # Always return a Dataset with selected variables
    processed_data = data[var_names]
    if not isinstance(processed_data, xr.Dataset):
        processed_data = processed_data.to_dataset()
    
    return processed_data, var_names

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