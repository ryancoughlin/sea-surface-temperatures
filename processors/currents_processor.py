from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
from scipy.ndimage import gaussian_filter
from scipy.interpolate import RectBivariateSpline
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
import xarray as xr
import cartopy.crs as ccrs
from typing import Tuple, Optional, Dict
from scipy.interpolate import griddata
import cartopy.feature as cfeature
from datetime import datetime

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
        
            colors = [
                (0, 0, 0, 0),          # Transparent for zero
                (0.1, 0.1, 0.5, 1),    # Deep blue for slow currents
                (0, 0.5, 1, 1),        # Medium blue
                (0, 1, 1, 1),          # Cyan
                (1, 1, 0, 1),          # Yellow
                (1, 0.5, 0, 1),        # Orange
                (1, 0, 0, 1)           # Red for fast currents
            ]
            n_bins = 256  # Number of color gradations
            custom_cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list('custom_currents', colors, N=n_bins)
            
            mesh = ax.pcolormesh(
                magnitude[lon_name],
                magnitude[lat_name],
                magnitude.values,
                transform=ccrs.PlateCarree(),
                cmap=custom_cmap,
                shading='gouraud',
                vmin=0,
                vmax=4.0,
                zorder=1,
                edgecolors='none'     # Remove cell edges
            )
            
            # Add dense streamlines for current direction
            # Increase density and reduce arrow size for better flow visualization
            ax.streamplot(
                magnitude[lon_name],
                magnitude[lat_name],
                u_data,
                v_data,
                transform=ccrs.PlateCarree(),
                color=('#000000', 0.5),           # White arrows for visibility
                density=3,               # Increase density of streamlines
                linewidth=2,          # Thinner lines
                arrowsize=3,          # Smaller arrows
                arrowstyle='->',        # Simple arrow style
                minlength=0.1,          # Allow shorter streamlines
                integration_direction='forward',  # Follow flow direction
                zorder=2,
            )

            # Explicitly turn off all grids
            ax.grid(False, which='both')  # Turn off both major and minor grids
            
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            raise
