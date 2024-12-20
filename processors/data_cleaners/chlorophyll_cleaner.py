import xarray as xr
import numpy as np
from pathlib import Path
import cartopy.feature as cfeature
from shapely.geometry import Point
from cartopy.feature import NaturalEarthFeature
import logging

logger = logging.getLogger(__name__)

class ChlorophyllCleaner:
    """Handles chlorophyll-specific data cleaning."""
    
    def __init__(self):
        # Load land geometry once
        self.land = NaturalEarthFeature('physical', 'land', '10m')
        self.land_geoms = list(self.land.geometries())
    
    def _is_over_land(self, lon: float, lat: float) -> bool:
        """Check if a point is over land."""
        point = Point(lon, lat)
        return any(geom.contains(point) for geom in self.land_geoms)
    
    def clean(self, data: xr.Dataset) -> xr.Dataset:
        """Clean chlorophyll data.
        
        Args:
            data: Dataset containing chlorophyll data
            
        Returns:
            Cleaned dataset
        """
        try:
            # Create copy to avoid modifying input
            cleaned_data = data.copy()
            
            # Get chlorophyll variable
            chl_var = next(var for var in data.data_vars if 'CHL' in var)
            
            # Apply log transform
            values = data[chl_var].values
            values = np.where(values <= 0, np.nan, values)
            values = np.where(~np.isnan(values), np.log10(values), values)
            
            cleaned_data[chl_var].values = values
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Failed to clean chlorophyll data: {str(e)}")
            raise