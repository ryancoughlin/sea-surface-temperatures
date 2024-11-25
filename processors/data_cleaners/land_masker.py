import xarray as xr
import numpy as np
from pathlib import Path
import cartopy.feature as cfeature
from shapely.geometry import Point, MultiPolygon
from cartopy.feature import NaturalEarthFeature
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class LandMasker:
    """Handles masking data points that fall over land."""
    
    def __init__(self):
        self._land_geoms = None
        self._cached_mask = {}  # Cache masks by grid shape and bounds
    
    @property
    def land_geoms(self) -> MultiPolygon:
        """Lazy load land geometries."""
        if self._land_geoms is None:
            land = NaturalEarthFeature('physical', 'land', '10m')
            self._land_geoms = list(land.geometries())
        return self._land_geoms

    def _create_land_mask(self, lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
        """Create a land mask for the given coordinate grid."""
        # Create cache key from grid shape and bounds
        cache_key = (
            lons.shape, lats.shape,
            float(lons.min()), float(lons.max()),
            float(lats.min()), float(lats.max())
        )
        
        # Return cached mask if available
        if cache_key in self._cached_mask:
            return self._cached_mask[cache_key]
            
        # Create new mask
        mask = np.zeros((len(lats), len(lons)), dtype=bool)
        
        # Vectorized point creation and checking
        for i in range(len(lats)):
            for j in range(len(lons)):
                point = Point(lons[j], lats[i])
                if any(geom.contains(point) for geom in self.land_geoms):
                    mask[i, j] = True
        
        # Cache the mask
        self._cached_mask[cache_key] = mask
        return mask

    def mask_land(self, data: xr.DataArray) -> xr.DataArray:
        """Mask out data points that fall over land."""
        try:
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Get coordinate arrays
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            # Create or get cached land mask
            mask = self._create_land_mask(lons, lats)
            
            # Apply land mask
            masked_data = data.where(~mask, drop=False)
            
            logger.info(f"Applied land mask to data array of shape {data.shape}")
            return masked_data
            
        except Exception as e:
            logger.error(f"Error applying land mask: {str(e)}")
            raise 