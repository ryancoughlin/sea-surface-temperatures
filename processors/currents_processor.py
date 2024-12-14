from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
import cartopy.crs as ccrs
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from typing import Tuple, Optional, Dict, NamedTuple
from datetime import datetime
from matplotlib.colors import LinearSegmentedColormap

logger = logging.getLogger(__name__)

class CurrentsProcessor(BaseImageProcessor):
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[Path, Optional[Dict]]:
        """Generate currents visualization using preprocessed data."""
        try:
            # Get velocity components
            u_var, v_var = SOURCES[dataset]['variables']
            u_data = data[u_var]
            v_data = data[v_var]
            
            # Compute magnitude on the proper grid
            magnitude = xr.DataArray(
                np.sqrt(u_data**2 + v_data**2),
                coords={'latitude': data.latitude, 'longitude': data.longitude},
                dims=['latitude', 'longitude']
            )
            
            # Get valid data statistics
            valid_data = magnitude.values[~np.isnan(magnitude.values)]
            if len(valid_data) == 0:
                raise ValueError("No valid current data for visualization")
            
            # Calculate dynamic ranges using percentiles
            vmin = float(np.percentile(valid_data, 1))
            vmax = float(np.percentile(valid_data, 99))
            
            # Calculate threshold for significant currents (5th percentile)
            magnitude_threshold = float(np.percentile(valid_data, 5))
        
            # Compute spatial gradients
            grad_x, grad_y = np.gradient(magnitude.values)
            gradient_magnitude = np.sqrt(grad_x**2 + grad_y**2)
            
            # Create interest mask for areas with significant currents or gradients
            interest_mask = (magnitude.values > magnitude_threshold) | (gradient_magnitude > magnitude_threshold / 2)
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
            
            # Create colormap
            cmap = LinearSegmentedColormap.from_list('ocean_currents', 
                                                    SOURCES[dataset]['color_scale'], 
                                                    N=256)
            
            # Plot background current magnitude
            mesh = ax.pcolormesh(
                data.longitude,
                data.latitude,
                magnitude,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                shading='gouraud',
                vmin=vmin,
                vmax=vmax,
                zorder=1
            )
            
            # Calculate arrow density based on grid resolution
            nx, ny = len(data.longitude), len(data.latitude)
            skip = max(1, min(nx, ny) // 25)  # Aim for roughly 25 arrows in smallest dimension
            
            # Create proper coordinate grids for quiver plot
            lon_mesh, lat_mesh = np.meshgrid(data.longitude[::skip], data.latitude[::skip])
            
            # Prepare masked velocity components
            u_masked = np.where(interest_mask, u_data, np.nan)[::skip, ::skip]
            v_masked = np.where(interest_mask, v_data, np.nan)[::skip, ::skip]
            
            # Add quiver plot with masked velocities
            ax.quiver(
                lon_mesh,
                lat_mesh,
                u_masked,
                v_masked,
                transform=ccrs.PlateCarree(),
                color='white',
                scale=25,
                scale_units='width',
                width=0.0008,
                headwidth=3,
                headaxislength=2.5,
                headlength=2.5,
                alpha=0.6,
                pivot='middle',
                zorder=2
            )
            
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            if isinstance(data, xr.Dataset):
                logger.error(f"Data dimensions: {data.dims}")
                logger.error(f"Variables: {list(data.variables)}")
            raise
