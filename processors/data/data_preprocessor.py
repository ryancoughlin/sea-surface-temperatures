import xarray as xr
import logging

logger = logging.getLogger(__name__)

class DataPreprocessor:
    def preprocess_dataset(self, data: xr.Dataset | xr.DataArray) -> xr.Dataset:
        """Preprocess dataset by handling dimensions and removing NaN values."""
        # Convert DataArray to Dataset if needed
        if isinstance(data, xr.DataArray):
            data = data.to_dataset()
            
        # 1. Select first index for time/altitude/depth dimensions
        dims_to_reduce = {dim: 0 for dim in ['time', 'altitude', 'depth'] if dim in data.dims}
        if dims_to_reduce:
            data = data.isel(dims_to_reduce)
            
        # 2. Standardize coordinates using rename
        renames = {}
        for coord in data.coords:
            if coord == 'lat':
                renames[coord] = 'latitude'
            elif coord == 'lon':
                renames[coord] = 'longitude'
        if renames:
            data = data.rename(renames)
        
        # 3. Drop NaN values for data variables
        data = data.dropna(dim='latitude', how='all', subset=data.data_vars)
        data = data.dropna(dim='longitude', how='all', subset=data.data_vars)
        
        return data