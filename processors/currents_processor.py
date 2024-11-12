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
from utils.data_utils import interpolate_currents

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
        
            # Convert hex #D8DEE1 to RGB (216,222,225) then to 0-1 range
            colors = [
                '#ffffff',  # 0.0 m/s - Pure white for no current
                '#cce6ff',  # 0.5 m/s - Very light blue
                '#66b3ff',  # 1.0 m/s - Light blue
                '#0080ff',  # 1.5 m/s - Medium blue
                '#0059b3',  # 2.0 m/s - Darker blue
                '#003366',  # 2.5 m/s - Deep blue
                '#001a33',  # 3.0 m/s - Very deep blue
                '#000d1a'   # 3.5-4.0 m/s - Almost black blue
            ]
            n_bins = 256  # Number of color gradations
            custom_cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list('custom_currents', colors, N=n_bins)
            
            # Calculate the maximum magnitude for scaling
            max_magnitude = np.sqrt(u_data**2 + v_data**2).max()

            # Create a grid of points for the arrows
            x_points, y_points = np.meshgrid(
                np.linspace(magnitude[lon_name].min(), magnitude[lon_name].max(), 60),
                np.linspace(magnitude[lat_name].min(), magnitude[lat_name].max(), 60)
            )

            # Interpolate u and v data to the new grid points
            u_interp, v_interp = interpolate_currents(u_data, v_data, (x_points, y_points))

            # Calculate local magnitudes for normalization
            local_magnitudes = np.sqrt(u_interp**2 + v_interp**2)
            local_magnitudes = np.where(local_magnitudes == 0, np.nan, local_magnitudes)
            
            # Normalize vectors to unit length
            u_norm = np.where(np.isnan(local_magnitudes), 0, u_interp/local_magnitudes)
            v_norm = np.where(np.isnan(local_magnitudes), 0, v_interp/local_magnitudes)

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

            # Adjust quiver parameters for better visibility
            ax.quiver(
                x_points, y_points, u_norm, v_norm,
                transform=ccrs.PlateCarree(),
                alpha=0.4,
                color='black',
                scale=50,          # Increased scale to make arrows shorter
                scale_units='width',
                units='width',
                width=0.001,
                headwidth=4,       # Slightly reduced head width
                headlength=3,      # Reduced head length
                headaxislength=3,  # Reduced head axis length
                minshaft=0.5,      # Reduced minimum shaft length
                pivot='middle',
                zorder=2
            )

            # Land mask is handled in save_image() in base processor
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            raise
