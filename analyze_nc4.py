import xarray as xr
import numpy as np

def analyze_nc4(file_path):
    with xr.open_dataset(file_path) as ds:
        print("Dataset Info:")
        print(ds.info())
        
        print("\nVariables:")
        for var_name, var in ds.variables.items():
            print(f"\n{var_name}:")
            print(f"  Shape: {var.shape}")
            print(f"  Dtype: {var.dtype}")
            if 'units' in var.attrs:
                print(f"  Units: {var.attrs['units']}")
            if len(var.shape) > 0:
                print(f"  Min value: {var.values.min()}")
                print(f"  Max value: {var.values.max()}")
                print(f"  Has NaN: {np.isnan(var.values).any()}")

        if 'sst' in ds:
            sst = ds.sst
            print("\nSST Variable Details:")
            print(f"Shape: {sst.shape}")
            print(f"Dimensions: {sst.dims}")
            print(f"Coordinates:")
            for dim, coord in sst.coords.items():
                print(f"  {dim}: {coord.values.min()} to {coord.values.max()}")
            
            # Check for fill values or missing data flags
            if '_FillValue' in sst.attrs:
                print(f"Fill Value: {sst.attrs['_FillValue']}")
            
            # Print some actual data values
            print("\nSample SST values:")
            print(sst.values.flatten()[:10])

if __name__ == "__main__":
    nc4_filepath = './data/capecod.nc4'
    analyze_nc4(nc4_filepath)
