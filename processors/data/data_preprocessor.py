import xarray as xr
import logging

logger = logging.getLogger(__name__)

class DataPreprocessor:
    def preprocess_dataset(self, data: xr.Dataset | xr.DataArray) -> xr.Dataset:
        """Preprocess dataset by flattening time dimension and removing depth.
        
        Args:
            data: Input data as xarray Dataset or DataArray
            
        Returns:
            xr.Dataset: Preprocessed dataset with time and depth dimensions removed
        """
        data = data.to_dataset() if isinstance(data, xr.DataArray) else data
        
        if 'time' in data.dims:
            data = data.isel(time=0)

        if 'depth' in data.dims:
            data = data.isel(depth=0, drop=True)

        if 'altitude' in data.dims:
            data = data.isel(altitude=0, drop=True)

        return data