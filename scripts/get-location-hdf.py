# For HDF5:
import h5py

def get_hdf5_bounds(file_path):
    with h5py.File(file_path, 'r') as f:
        # List all datasets
        print("Available datasets:")
        f.visit(print)
        
        # Extract bounds from specific dataset
        # dataset = f['your_dataset_name']
        # bounds = [dataset.min(), dataset.max()]
        
        return bounds

# For HDF4:
from pyhdf.SD import SD, SDC

def get_hdf4_bounds(file_path):
    hdf = SD(file_path, SDC.READ)
    # List datasets
    datasets = hdf.datasets()
    
    print("Available datasets:")
    for idx, sds in enumerate(datasets.keys()):
        print(f"{idx}: {sds}")
    
    return bounds