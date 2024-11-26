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
    """Handles masking data points that fall over land using efficient spatial indexing."""
    
    def __init__(self):
        self._land_geoms = None
        self._prepared_land = None
        self._spatial_index = None
        self._cached_mask = {}  # Cache masks by grid shape and bounds
    
    def _init_spatial_index(self):
        """Initialize spatial index for land geometries."""
        if self._spatial_index is None:
            # Get land geometries if not already loaded
            if self._land_geoms is None:
                land = NaturalEarthFeature('physical', 'land', '10m')
                self._land_geoms = list(land.geometries())
            
            # Create spatial index
            idx = index.Index()
            for i, geom in enumerate(self._land_geoms):
                idx.insert(i, geom.bounds)
            
            self._spatial_index = idx
            self._prepared_land = [prep(geom) for geom in self._land_geoms]

    def _create_land_mask(self, lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
        """Create a land mask for the given coordinate grid using spatial indexing."""
        # Create cache key from grid shape and bounds
        cache_key = (
            lons.shape, lats.shape,
            float(lons.min()), float(lons.max()),
            float(lats.min()), float(lats.max())
        )
        
        # Return cached mask if available
        if cache_key in self._cached_mask:
            return self._cached_mask[cache_key]
            
        # Initialize spatial index if needed
        self._init_spatial_index()
        
        # Create mask array
        mask = np.zeros((len(lats), len(lons)), dtype=bool)
        
        # Create a grid of points
        lon_grid, lat_grid = np.meshgrid(lons, lats)
        
        # Process in chunks for memory efficiency
        chunk_size = 1000
        for i in range(0, len(lats), chunk_size):
            for j in range(0, len(lons), chunk_size):
                # Get chunk bounds
                i_end = min(i + chunk_size, len(lats))
                j_end = min(j + chunk_size, len(lons))
                
                # Create bounding box for chunk
                chunk_box = box(
                    lons[j], lats[i],
                    lons[min(j_end, len(lons)-1)], 
                    lats[min(i_end, len(lats)-1)]
                )
                
                # Find potential intersecting land geometries
                potential_geoms = list(self._spatial_index.intersection(chunk_box.bounds))
                
                if not potential_geoms:
                    continue
                
                # Check points in chunk
                for ii in range(i, i_end):
                    for jj in range(j, j_end):
                        point = Point(lon_grid[ii, jj], lat_grid[ii, jj])
                        
                        # Check only against potentially intersecting geometries
                        for idx in potential_geoms:
                            if self._prepared_land[idx].contains(point):
                                mask[ii, jj] = True
                                break
        
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

            return masked_data
            
        except Exception as e:
            logger.error(f"Error applying land mask: {str(e)}")
            raise