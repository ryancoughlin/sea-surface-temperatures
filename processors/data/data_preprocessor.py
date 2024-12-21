import xarray as xr
import numpy as np
import logging
from shapely.geometry import Point
from cartopy.feature import NaturalEarthFeature

logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        # Initialize land mask data
        self.land = NaturalEarthFeature('physical', 'land', '10m')
        self.land_geoms = list(self.land.geometries())

    def _get_land_mask(self, data: xr.Dataset) -> np.ndarray:
        """Create a boolean mask for land areas.
        
        Args:
            data: Dataset containing lat/lon coordinates
            
        Returns:
            Boolean mask where True indicates water, False indicates land
        """
        # Get coordinate names
        lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
        lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
        
        # Create meshgrid of coordinates
        lons, lats = np.meshgrid(data[lon_name], data[lat_name])
        
        # Initialize mask
        water_mask = np.ones_like(lons, dtype=bool)
        
        # Check each point
        for i in range(lons.shape[0]):
            for j in range(lons.shape[1]):
                point = Point(lons[i,j], lats[i,j])
                if any(geom.contains(point) for geom in self.land_geoms):
                    water_mask[i,j] = False
                    
        return water_mask

    def _clean_chlorophyll(self, data: xr.Dataset) -> xr.Dataset:
        """Clean chlorophyll data by removing land points and invalid values.
        
        Args:
            data: Dataset containing chlorophyll data
                
        Returns:
            Cleaned dataset with land masked and invalid values removed
        """
        try:
            # Create copy to avoid modifying input
            cleaned_data = data.copy()
            
            # Get chlorophyll variable (chlor_a is standard name)
            chl_var = 'chlor_a' if 'chlor_a' in data else next(var for var in data.data_vars if 'CHL' in var.upper())
            
            # Get water mask
            water_mask = self._get_land_mask(data)
            
            # Apply water mask and transformations
            values = data[chl_var].values
            values = np.where(~water_mask, np.nan, values)  # Mask land
            values = np.where(values <= 0, np.nan, values)  # Remove negative/zero values
            
            cleaned_data[chl_var].values = values
            
            logger.info(f"Cleaned chlorophyll data: masked land and invalid values")
            return cleaned_data
                
        except Exception as e:
            logger.error(f"Failed to clean chlorophyll data: {str(e)}")
            raise

    def preprocess_dataset(self, data: xr.Dataset | xr.DataArray, dataset_type: str = None) -> xr.Dataset:
        """Preprocess dataset by flattening time dimension, removing depth, and applying type-specific cleaning.
        
        Args:
            data: Input data as xarray Dataset or DataArray
            dataset_type: Type of dataset (e.g., 'chlorophyll', 'sst', etc.)
            
        Returns:
            xr.Dataset: Preprocessed dataset with time and depth dimensions removed and cleaning applied
        """
        data = data.to_dataset() if isinstance(data, xr.DataArray) else data
        
        if 'time' in data.dims:
            data = data.isel(time=0)

        if 'depth' in data.dims:
            data = data.isel(depth=0, drop=True)

        if 'altitude' in data.dims:
            data = data.isel(altitude=0, drop=True)

        # Apply type-specific cleaning
        if dataset_type == 'chlorophyll':
            logger.info("🧹 Applying chlorophyll-specific cleaning")
            data = self._clean_chlorophyll(data)

        return data