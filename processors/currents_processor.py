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

            # Reduce density of arrows by taking every nth point
            skip = 3  # Adjust this value to change arrow density (higher = fewer arrows)
            
            # Option 1: Thin, minimal arrows
            ax.quiver(
                data['longitude'][::skip],
                data['latitude'][::skip],
                u_data.where(interest_mask)[::skip],
                v_data.where(interest_mask)[::skip],
                transform=ccrs.PlateCarree(),
                color='white',
                scale=25,
                scale_units='width',
                width=0.0008,  # Thinner arrows
                headwidth=3,    # Smaller head
                headaxislength=2.5,
                headlength=2.5,
                alpha=0.6,
                pivot='middle',
                zorder=2
            )

            # Uncomment one of these alternative styles:
            
            # Option 2: Classic arrows with better visibility
            # ax.quiver(
            #     data['longitude'][::skip],
            #     data['latitude'][::skip],
            #     u_data.where(interest_mask)[::skip],
            #     v_data.where(interest_mask)[::skip],
            #     transform=ccrs.PlateCarree(),
            #     color='white',
            #     scale=22,
            #     scale_units='width',
            #     width=0.001,
            #     headwidth=4,
            #     headaxislength=3,
            #     headlength=3.5,
            #     alpha=0.7,
            #     pivot='middle',
            #     zorder=2
            # )

            # Option 3: Modern, streamlined arrows
            # ax.quiver(
            #     data['longitude'][::skip],
            #     data['latitude'][::skip],
            #     u_data.where(interest_mask)[::skip],
            #     v_data.where(interest_mask)[::skip],
            #     transform=ccrs.PlateCarree(),
            #     color='white',
            #     scale=30,
            #     scale_units='width',
            #     width=0.0005,  # Very thin arrows
            #     headwidth=2.5,  # Compact head
            #     headaxislength=2,
            #     headlength=2,
            #     alpha=0.8,
            #     pivot='middle',
            #     zorder=2
            # )

            return self.save_image(fig, region, dataset, date), None

        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            raise
