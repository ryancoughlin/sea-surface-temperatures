import xarray as xr
import numpy as np
from pathlib import Path
from shapely.geometry import Point, box
from shapely.prepared import prep
from cartopy.feature import NaturalEarthFeature
import logging
from typing import Optional, Dict
from rtree import index

logger = logging.getLogger(__name__)

class LandMasker:
    def __init__(self):
        self.land_feature = NaturalEarthFeature('physical', 'land', '50m')
        self.land_geoms = list(self.land_feature.geometries())
        self.prepared_land = prep(self.land_feature.geometries().__next__())
        
        # Build R-tree index for faster intersection tests
        self.idx = index.Index()
        for i, geom in enumerate(self.land_geoms):
            self.idx.insert(i, geom.bounds)
            
        self._cache = {}
        
    def _create_land_mask(self, lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
        """Create a land mask for the given coordinates."""
        try:
            # Create cache key
            cache_key = (lons.tobytes(), lats.tobytes())
            if cache_key in self._cache:
                return self._cache[cache_key]
            
            # Create coordinate meshgrid
            lon_mesh, lat_mesh = np.meshgrid(lons, lats)
            mask = np.zeros(lon_mesh.shape, dtype=bool)
            
            # Check each point
            for i in range(mask.shape[0]):
                for j in range(mask.shape[1]):
                    point = Point(lon_mesh[i, j], lat_mesh[i, j])
                    
                    # First check if point intersects with any land geometry's bounding box
                    intersects = False
                    for idx in self.idx.intersection(point.bounds):
                        if self.land_geoms[idx].contains(point):
                            intersects = True
                            break
                    
                    mask[i, j] = intersects
                    
            # Cache result
            self._cache[cache_key] = mask
            return mask
            
        except Exception as e:
            logger.error(f"Error creating land mask: {str(e)}")
            raise
            
    def mask_land(self, data: xr.Dataset | xr.DataArray) -> xr.Dataset | xr.DataArray:
        """Apply land mask to dataset.
        
        This method accepts both Dataset (preferred) and DataArray inputs to maintain
        compatibility with different data sources. When possible, pass a Dataset.
        
        Args:
            data: Dataset (preferred) or DataArray to mask
            
        Returns:
            Masked data of same type as input
        """
        try:
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(data)
            lons = data[lon_name].values
            lats = data[lat_name].values
            
            # Create mask
            mask = self._create_land_mask(lons, lats)
            
            # Handle Dataset (preferred) or DataArray
            if isinstance(data, xr.Dataset):
                masked_data = data.copy()
                for var in data.data_vars:
                    masked_data[var].values = np.where(mask, np.nan, data[var].values)
            else:
                # Legacy support for DataArray
                masked_data = data.copy()
                masked_data.values = np.where(mask, np.nan, data.values)
                
            return masked_data
            
        except Exception as e:
            logger.error(f"Failed to apply land mask: {str(e)}")
            raise
        
    def get_coordinate_names(self, data: xr.Dataset | xr.DataArray) -> tuple:
        """Get standardized coordinate names from dataset.
        
        This method accepts both Dataset (preferred) and DataArray inputs to maintain
        compatibility with different data sources. When possible, pass a Dataset.
        
        Args:
            data: Dataset (preferred) or DataArray to get coordinates from
            
        Returns:
            Tuple of (longitude_name, latitude_name)
        """
        lon_patterns = ['lon', 'longitude', 'x']
        lat_patterns = ['lat', 'latitude', 'y']
        
        lon_name = None
        lat_name = None
        
        # Handle Dataset (preferred) or DataArray coordinates
        coords = data.coords
        for var in coords:
            var_lower = var.lower()
            if any(pattern in var_lower for pattern in lon_patterns):
                lon_name = var
            elif any(pattern in var_lower for pattern in lat_patterns):
                lat_name = var
                
        if not lon_name or not lat_name:
            raise ValueError("Could not identify coordinate variables")
            
        return lon_name, lat_name