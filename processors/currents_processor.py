from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
import xarray as xr
import json
from datetime import datetime
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from typing import Tuple

logger = logging.getLogger(__name__)

class CurrentsProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Tuple[Path, None]:
        """Generate ocean currents visualization with readable arrow coverage."""
        try:
            # Load data
            ds = xr.open_dataset(data_path)
            bounds = REGIONS[region]['bounds']
            
            # Subset data
            ds_subset = ds.sel(
                longitude=slice(bounds[0][0], bounds[1][0]),
                latitude=slice(bounds[0][1], bounds[1][1]),
                time=ds.time[0]
            )
            
            # Get current components
            u = ds_subset.u_current
            v = ds_subset.v_current
            
            # Create masked figure and axes
            fig, ax = self.create_masked_axes(region)
            
            # Create grid
            lon_grid, lat_grid = np.meshgrid(ds_subset.longitude, ds_subset.latitude)
            
            # Add stride to reduce density
            stride = 4  # Show every 4th point
            
            # Plot arrows
            ax.quiver(
                lon_grid[::stride, ::stride],
                lat_grid[::stride, ::stride],
                u.values[::stride, ::stride],
                v.values[::stride, ::stride],
                color='white',
                scale=8,
                width=0.004,
                headwidth=6,
                headlength=7,
                headaxislength=6,
                alpha=0.8,
                transform=ccrs.PlateCarree()
            )
            
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            raise

    def process_current_data(self, ds, bounds):
        """Process current data into useful formats."""
        # 1. Extract our region
        region_mask = (
            (ds.lon >= bounds[0][0]) & (ds.lon <= bounds[1][0]) &
            (ds.lat >= bounds[0][1]) & (ds.lat <= bounds[1][1])
        )
        
        # 2. Calculate derived values
        speed = np.sqrt(ds.u**2 + ds.v**2)  # magnitude of current
        direction = np.arctan2(ds.v, ds.u)   # direction in radians
        
        # 3. Create a clean data structure
        currents_data = {
            'lon': ds.lon.where(region_mask, drop=True),
            'lat': ds.lat.where(region_mask, drop=True),
            'u': ds.u.where(region_mask, drop=True),
            'v': ds.v.where(region_mask, drop=True),
            'speed': speed.where(region_mask, drop=True),
            'direction': direction.where(region_mask, drop=True)
        }
  
        return currents_data

    def plot_currents(self, data, ax):
        """Plot currents using processed data."""
        # Create grid for plotting
        lon, lat = np.meshgrid(data['lon'], data['lat'])
        
        # Plot speed as background color
        speed = data['speed'].squeeze().T
        contour = ax.contourf(
            lon, lat, speed,
            levels=np.linspace(0, speed.max().item(), 21),
            cmap='viridis',
            extend='both'
        )
        
        # Add arrows - subsample for clarity
        stride = max(1, min(speed.shape) // 20)  # adjust based on zoom level
        ax.quiver(
            lon[::stride, ::stride],
            lat[::stride, ::stride],
            data['u'].squeeze().values[::stride, ::stride].T,
            data['v'].squeeze().values[::stride, ::stride].T,
            scale=20,
            width=0.003,
            color='white',
            alpha=0.6
        )
        
        return contour
