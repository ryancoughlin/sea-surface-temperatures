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
from utils.data_utils import interpolate_data

logger = logging.getLogger(__name__)

class CurrentsProcessor(BaseImageProcessor):
    def convert_to_knots(self, speed_ms: np.ndarray) -> np.ndarray:
        """Convert speed from m/s to knots."""
        return speed_ms * 1.94384
    
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
            fig, ax = self.create_masked_axes(region)
        
            # Convert hex #D8DEE1 to RGB (216,222,225) then to 0-1 range
            colors = [
                (216/255, 222/255, 225/255, 1),  # #D8DEE1 with 0 alpha
                (0.1, 0.1, 0.5, 1),    # Deep blue for slow currents
                (0, 0.5, 1, 1),        # Medium blue
                (0, 1, 1, 1),          # Cyan
                (1, 1, 0, 1),          # Yellow
                (1, 0.5, 0, 1),        # Orange
                (1, 0, 0, 1)           # Red for fast currents
            ]
            n_bins = 256  # Number of color gradations
            custom_cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list('custom_currents', colors, N=n_bins)
            
            # Interpolate magnitude data
            interpolated_data = interpolate_data(magnitude, factor=2)
            
            # Plot magnitude with pcolormesh
            mesh = ax.pcolormesh(
                magnitude[lon_name],
                magnitude[lat_name],
                magnitude.values,  # Use original data for coordinates
                transform=ccrs.PlateCarree(),
                cmap=custom_cmap,
                shading='gouraud',
                vmin=0,
                vmax=4.0,
                zorder=1
            )
            
            # Add streamlines using original resolution
            ax.streamplot(
                magnitude[lon_name],
                magnitude[lat_name],
                u_data,
                v_data,
                transform=ccrs.PlateCarree(),
                color=('#000000', 0.5),
                density=3,
                linewidth=2,
                arrowsize=3,
                arrowstyle='->',
                minlength=0.1,
                integration_direction='forward',
                zorder=2
            )
            
            # Land mask is handled in save_image() in base processor
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            raise
