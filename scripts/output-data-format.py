import xarray as xr
import pandas as pd
from typing import Dict, Any
import numpy as np
from tabulate import tabulate

def analyze_dataset(ds: xr.Dataset, dataset_name: str) -> Dict[str, Any]:
    """Analyze an xarray Dataset and return key information."""
    
    # Basic dataset info
    info = {
        "Dataset Name": dataset_name,
        "Dimensions": dict(ds.sizes),  # Changed from ds.dims to ds.sizes
        "Variables": list(ds.data_vars),
        "Coordinates": list(ds.coords),
    }
    
    # Spatial resolution
    if all(dim in ds.dims for dim in ['latitude', 'longitude']):
        lat_res = abs(np.diff(ds.latitude.values).mean())
        lon_res = abs(np.diff(ds.longitude.values).mean())
        info["Spatial Resolution"] = {
            "Latitude": f"{lat_res:.4f}°",
            "Longitude": f"{lon_res:.4f}°"
        }
    
    # Data ranges
    info["Data Ranges"] = {}
    for var in ds.data_vars:
        try:
            var_data = ds[var].values
            info["Data Ranges"][var] = {
                "min": float(np.nanmin(var_data)),
                "max": float(np.nanmax(var_data)),
                "dtype": str(var_data.dtype)
            }
        except:
            continue
    
    # Memory usage
    info["Memory Usage"] = {
        var: f"{ds[var].nbytes / 1e6:.2f} MB" 
        for var in ds.data_vars
    }
    
    # Key attributes
    info["Global Attributes"] = {
        k: v for k, v in ds.attrs.items() 
        if k in ['title', 'summary', 'Conventions', 'source']
    }
    
    return info

def display_dataset_info(info: Dict[str, Any], md_file):
    """Display dataset information in markdown format matching README style."""
    
    md_file.write(f"# {info['Dataset Name']}\n\n")
    
    md_file.write("| **Component** | **Description** |\n")
    md_file.write("| ------------- | --------------- |\n")
    
    # Dimensions
    dims_str = ", ".join([f"`{k}: {v}`" for k, v in info['Dimensions'].items()])
    md_file.write(f"| **Dimensions** | {dims_str} |\n")
    
    # Grid Density
    total_points = " x ".join([str(v) for v in info['Dimensions'].values() if isinstance(v, (int, float))])
    md_file.write(f"| **Grid Density** | Resolution of roughly {total_points} points |\n")
    
    # Spatial Resolution
    if "Spatial Resolution" in info:
        md_file.write("| **Grid Spacing** | - **Latitude Spacing**: " + 
                     f"~{info['Spatial Resolution']['Latitude']} |\n")
        md_file.write(f"| | - **Longitude Spacing**: " + 
                     f"~{info['Spatial Resolution']['Longitude']} |\n")
    
    # Coordinates
    md_file.write("| **Coordinates** |")
    for coord in info['Coordinates']:
        md_file.write(f" - `{coord}` |\n| |")
    md_file.write("\n")
    
    # Data Variables
    md_file.write("| **Data Variables** |")
    for var in info['Variables']:
        var_info = info['Data Ranges'].get(var, {})
        dtype = var_info.get('dtype', '')
        md_file.write(f" - `{var}`: {dtype} |\n| |")
    md_file.write("\n")
    
    # Array Storage
    md_file.write("| **Array Storage** | Arrays are stored in a row-major order (C-style) layout, "
                 "optimized for efficient processing and visualization tasks |\n")
    
    # Attributes
    if info["Global Attributes"]:
        md_file.write("| **Attributes** |")
        for k, v in info["Global Attributes"].items():
            md_file.write(f" - `{k}`: {v} |\n| |")
        md_file.write("\n")
    
    md_file.write("\n---\n\n")

def analyze_oceanographic_dataset(file_path: str, dataset_name: str, md_file):
    """Main function to analyze oceanographic datasets."""
    try:
        ds = xr.open_dataset(file_path)
        info = analyze_dataset(ds, dataset_name)
        display_dataset_info(info, md_file)
        ds.close()
    except Exception as e:
        md_file.write(f"## Error analyzing dataset {dataset_name}\n\n")
        md_file.write(f"```\n{str(e)}\n```\n\n")

# Example usage:
if __name__ == "__main__":
    output_file = "dataset_analysis.md"
    
    datasets = {
        "SST": "./leo_sst.nc",
        "Ocean Currents": "./currents.nc",
        "Chlorophyll": "./chlorophyll.nc"
    }
    
    with open(output_file, 'w') as md_file:
        md_file.write("# Oceanographic Dataset Analysis\n\n")
        md_file.write("*Generated analysis of oceanographic datasets*\n\n")
        
        for name, path in datasets.items():
            analyze_oceanographic_dataset(path, name, md_file)
