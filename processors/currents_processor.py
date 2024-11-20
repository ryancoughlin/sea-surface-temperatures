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
        """Generate currents visualization with smooth transitions."""
        try:
            # Load and process data
            ds = xr.open_dataset(data_path)
            
            # Get velocity components
            u_data = ds[SOURCES[dataset]['variables'][0]].squeeze()
            v_data = ds[SOURCES[dataset]['variables'][1]].squeeze()
            
            # Calculate magnitude
            magnitude = np.sqrt(u_data**2 + v_data**2)
            
            # Get coordinates and bounds
            lon_name = 'longitude' if 'longitude' in magnitude.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in magnitude.coords else 'lat'
            bounds = REGIONS[region]['bounds']
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
        
            # Create custom colormap
            colors = [
                '#ffffff', '#cce6ff', '#66b3ff', '#0080ff',
                '#0059b3', '#003366', '#001a33', '#000d1a'
            ]
            n_bins = 256
            custom_cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list('custom_currents', colors, N=n_bins)
            
            # Plot magnitude with pcolormesh using custom colormap
            mesh = ax.pcolormesh(
                magnitude[lon_name],
                magnitude[lat_name],
                magnitude.values,
                transform=ccrs.PlateCarree(),
                cmap=custom_cmap,
                shading='gouraud',
                vmin=0,
                vmax=4.0,
                zorder=1
            )

            # Calculate normalized vectors for quiver plot
            stride = 3  # Adjust based on data density
            u_norm = u_data[::stride, ::stride] / magnitude[::stride, ::stride]
            v_norm = v_data[::stride, ::stride] / magnitude[::stride, ::stride]
            
            # Replace NaNs with zeros
            u_norm = np.nan_to_num(u_norm, nan=0.0)
            v_norm = np.nan_to_num(v_norm, nan=0.0)

            # Plot arrows
            ax.quiver(
                magnitude[lon_name][::stride],
                magnitude[lat_name][::stride],
                u_norm, v_norm,
                transform=ccrs.PlateCarree(),
                alpha=0.4,
                color='black',
                scale=70,
                scale_units='width',
                units='width',
                width=0.001,
                headwidth=4,
                headlength=3,
                headaxislength=3,
                minshaft=0.3,
                pivot='middle',
                zorder=2
            )

            # Land mask is handled in save_image() in base processor
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            raise
