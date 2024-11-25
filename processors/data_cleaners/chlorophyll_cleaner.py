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
    
    def clean(self, data: xr.DataArray) -> xr.DataArray:
        """Clean chlorophyll data by masking out land and extreme values."""
        try:
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Create land mask
            lons = data[lon_name].values
            lats = data[lat_name].values
            mask = np.zeros(data.shape, dtype=bool)
            
            # Create land mask using vectorized operations where possible
            for i in range(len(lats)):
                for j in range(len(lons)):
                    if self._is_over_land(lons[j], lats[i]):
                        mask[i, j] = True
            
            # Apply masks for both land and extreme values
            cleaned_data = data.where(
                (~mask) &  # Not over land
                (data > 0.01) &  # Remove extremely low values
                (data <= 15.0),  # Remove extremely high values
                drop=False
            )
            
            logger.info("Applied land mask and value constraints to chlorophyll data")
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Error cleaning chlorophyll data: {str(e)}")
            raise 