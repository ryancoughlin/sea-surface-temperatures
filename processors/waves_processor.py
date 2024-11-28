from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
import cartopy.crs as ccrs
from matplotlib.colors import LinearSegmentedColormap
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class WavesProcessor(BaseImageProcessor):
    """Processor for generating wave height visualizations."""
    
    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Path:
        """Generate wave height visualization in feet."""
        try:
            # Load data efficiently
            ds = xr.open_dataset(data_path, chunks={'time': 1})
            height = ds['VHM0'].isel(time=0).load()
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(height)
            bounds = REGIONS[region]['bounds']
            
            # Create figure
            fig, ax = self.create_axes(region)
            
            # Convert to feet
            height_ft = height * 3.28084
            
            # Get color scale configuration
            color_config = SOURCES[dataset]['color_scale']
            cmap = LinearSegmentedColormap.from_list('wave_heights', color_config['colors'], N=color_config['N'])
            
            # Create levels based on config
            vmin = color_config['vmin']
            vmax = color_config['vmax']
            if vmin == "auto":
                vmin = float(height_ft.min())
            if vmax == "auto":
                vmax = float(height_ft.max())
            
            levels = np.linspace(vmin, vmax, color_config['N'])
            
            # Create contour plot
            contourf = ax.contourf(
                height_ft[lon_name],
                height_ft[lat_name],
                height_ft.values,
                levels=levels,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                alpha=0.9,
                zorder=1,
                extend=color_config.get('extend', 'neither')
            )
            
            return self.save_image(fig, region, dataset, date)
            
        except Exception as e:
            logger.error(f"Error processing wave data: {str(e)}")
            raise