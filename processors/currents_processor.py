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
from matplotlib.colors import LinearSegmentedColormap

logger = logging.getLogger(__name__)

class CurrentsProcessor(BaseImageProcessor):
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[Path, Optional[Dict]]:
        """Generate currents visualization while emphasizing moving water and eddies."""
        try:
            # Get velocity components from SOURCES config
            u_var, v_var = SOURCES[dataset]['variables']
            u_data = data[u_var]  # Eastward velocity
            v_data = data[v_var]  # Northward velocity
            
            # Handle dimensions
            for dim in ['time', 'depth']:
                if dim in u_data.dims:
                    u_data = u_data.isel({dim: 0})
                if dim in v_data.dims:
                    v_data = v_data.isel({dim: 0})

            # Log data ranges before processing
            logger.info(f"Raw u_data range: {float(u_data.min().values):.4f} to {float(u_data.max().values):.4f}")
            logger.info(f"Raw v_data range: {float(v_data.min().values):.4f} to {float(v_data.max().values):.4f}")

            # Compute magnitude of currents
            magnitude = np.sqrt(u_data**2 + v_data**2)

            # Get actual min/max values from valid data for colormap
            valid_data = magnitude.values[~np.isnan(magnitude.values)]
            if len(valid_data) == 0:
                logger.error("No valid current data after preprocessing")
                raise ValueError("No valid current data after preprocessing")
            
            # Calculate dynamic ranges
            vmin = float(np.percentile(valid_data, 1))  # 1st percentile
            vmax = float(np.percentile(valid_data, 99))  # 99th percentile
            logger.info(f"Current magnitude range: {vmin:.4f} to {vmax:.4f}")
            
            # Calculate threshold for eddy detection (5th percentile)
            magnitude_threshold = float(np.percentile(valid_data, 5))

            # Compute spatial gradient of magnitude to detect eddies and changes
            grad_x, grad_y = np.gradient(magnitude.values)
            gradient_magnitude = np.sqrt(grad_x**2 + grad_y**2)
            
            # Define areas of interest: moving water or strong gradients
            interest_mask = (magnitude.values > magnitude_threshold) | (gradient_magnitude > magnitude_threshold / 2)
            logger.info(f"Interest points found: {np.sum(interest_mask)}")

            # Create figure and axes using base processor method
            fig, ax = self.create_axes(region)

            # Create colormap from color scale
            cmap = LinearSegmentedColormap.from_list('ocean_currents', SOURCES[dataset]['color_scale'], N=256)

            # Plot background current magnitude as colormesh
            mesh = ax.pcolormesh(
                data['longitude'],
                data['latitude'],
                magnitude,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                shading='gouraud',
                vmin=vmin,
                vmax=vmax,
                zorder=1
            )

            # Plot arrows for significant areas
            ax.quiver(
                data['longitude'],
                data['latitude'],
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
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            raise
