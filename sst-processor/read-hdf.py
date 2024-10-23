from pyhdf.SD import SD, SDC
import numpy as np
from pathlib import Path

def print_hdf_structure(hdf_file):
    """Print HDF4 file structure"""
    datasets = hdf_file.datasets()
    
    print("\nDatasets in file:")
    for idx, (name, info) in enumerate(datasets.items()):
        print(f"\nDataset {idx + 1}: {name}")
        print(f"Shape: {info[0]}")
        print(f"Type: {info[1]}")
        try:
            data = hdf_file.select(name)
            print("First few values:", data[0:1])
        except Exception as e:
            print(f"Could not read values: {e}")
        print("-" * 40)

def get_hdf_bounds(file_path):
    """Extract min/max lat/lon bounds from HDF file"""
    try:
        hdf = SD(str(file_path), SDC.READ)
        print_hdf_structure(hdf)
        
        # Common variations of latitude/longitude names
        lat_options = ['lat', 'latitude', 'Latitude', 'LAT']
        lon_options = ['lon', 'longitude', 'Longitude', 'LON']
        
        lat = None
        lon = None
        
        datasets = hdf.datasets()
        for lat_name in lat_options:
            if lat_name in datasets:
                lat = hdf.select(lat_name)[:]
                break
                
        for lon_name in lon_options:
            if lon_name in datasets:
                lon = hdf.select(lon_name)[:]
                break
        
        if lat is not None and lon is not None:
            bounds = {
                'minLat': float(np.min(lat)),
                'maxLat': float(np.max(lat)),
                'minLon': float(np.min(lon)),
                'maxLon': float(np.max(lon))
            }
            hdf.end()
            return bounds
        else:
            print("Could not find latitude/longitude datasets")
            hdf.end()
            return None
            
    except Exception as e:
        print(f"Error opening file: {e}")
        return None

def main():
    data_dir = Path('./data')
    
    hdf_files = list(data_dir.glob('*.hdf'))
    if not hdf_files:
        print(f"No HDF files found in {data_dir}")
        return
        
    for hdf_file in hdf_files:
        print(f"\nAnalyzing {hdf_file.name}")
        bounds = get_hdf_bounds(hdf_file)
        
        if bounds:
            print("\nCoordinate Bounds:")
            print(f"  Latitude:  {bounds['minLat']:.2f} to {bounds['maxLat']:.2f}")
            print(f"  Longitude: {bounds['minLon']:.2f} to {bounds['maxLon']:.2f}")

if __name__ == '__main__':
    main()
