from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
import cartopy.crs as ccrs
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from typing import Tuple, Optional, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

class CurrentsProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, date: datetime) -> Tuple[Path, Optional[Dict]]:
        """Generate currents visualization while emphasizing moving water and eddies."""
        try:
            # Load data
            ds = xr.open_dataset(data_path)
            
            # Get velocity components from SOURCES config
            u_var, v_var = SOURCES[dataset]['variables']
            u_data = ds[u_var].isel(time=0, depth=0)  # Eastward velocity
            v_data = ds[v_var].isel(time=0, depth=0)  # Northward velocity

            # Compute magnitude of currents
            magnitude = np.sqrt(u_data**2 + v_data**2)

            # Get actual min/max values from valid data for colormap
            valid_data = magnitude.values[~np.isnan(magnitude.values)]
            vmin = float(valid_data.min())
            vmax = float(valid_data.max())
            
            logger.info(f"Current speed range - min: {vmin:.2f} m/s, max: {vmax:.2f} m/s")

            # Calculate threshold for eddy detection (10th percentile)
            magnitude_threshold = float(np.percentile(valid_data, 5))
            logger.info(f"Eddy detection threshold: {magnitude_threshold:.2f} m/s")

            # Compute spatial gradient of magnitude to detect eddies and changes
            grad_x, grad_y = np.gradient(magnitude.values)
            gradient_magnitude = np.sqrt(grad_x**2 + grad_y**2)
            
            # Define areas of interest: moving water or strong gradients
            interest_mask = (magnitude.values > magnitude_threshold) | (gradient_magnitude > magnitude_threshold / 2)

            # Create figure and axes using base processor method
            fig, ax = self.create_axes(region)

            # Plot background current magnitude as colormesh
            colors = ['#B1C2D8', '#89CFF0', '#4682B4', '#0047AB', '#00008B', '#000033']
            current_cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list('ocean_currents', colors, N=256)

            mesh = ax.pcolormesh(
                ds['longitude'],
                ds['latitude'],
                magnitude,
                transform=ccrs.PlateCarree(),
                cmap=current_cmap,
                shading='gouraud',
                vmin=vmin,
                vmax=vmax,
                zorder=1
            )

            # Plot arrows for significant areas
            ax.quiver(
                ds['longitude'],
                ds['latitude'],
                u_data.where(interest_mask),
                v_data.where(interest_mask),
                transform=ccrs.PlateCarree(),
                color='white',
                scale=20,
                scale_units='width',
                width=0.001,
                headwidth=3.4,
                headaxislength=4,
                headlength=3,
                alpha=0.5,
                pivot='middle',
                zorder=2
            )

            return self.save_image(fig, region, dataset, date), None

        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            logger.error(f"Data dimensions: {ds.dims}")
            logger.error(f"Variables: {list(ds.variables)}")
            raise
