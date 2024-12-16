from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
import cartopy.crs as ccrs
from matplotlib.colors import LinearSegmentedColormap
from .base_visualizer import BaseVisualizer
from config.settings import SOURCES
from config.regions import REGIONS
from typing import Tuple, Optional, Dict

logger = logging.getLogger(__name__)

class WavesVisualizer(BaseVisualizer):
    """Processor for generating wave height visualizations."""
    
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: str) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate wave height visualization in feet."""
        try:
            # Get wave height data
            height = data['VHM0']
            
            # Handle dimensions
            for dim in ['time', 'depth']:
                if dim in height.dims:
                    height = height.isel({dim: 0})
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(height)
            bounds = REGIONS[region]['bounds']
            
            # Expand data near coastlines
            height = self.expand_coastal_data(height)
            
            # Log data info
            logger.info(f"Wave height dimensions: {height.dims}")
            logger.info(f"Wave height coordinates: {list(height.coords)}")
            
            # Create figure
            fig, ax = self.create_axes(region)
            
            # Convert to feet
            height_ft = height * 3.28084
            
            # Create colormap from color scale
            cmap = LinearSegmentedColormap.from_list('wave_heights', SOURCES[dataset]['color_scale'], N=91)
            
            # Calculate dynamic ranges from valid data
            valid_data = height_ft.values[~np.isnan(height_ft.values)]
            if len(valid_data) == 0:
                logger.error("No valid wave height data")
                raise ValueError("No valid wave height data")
                
            vmin = float(np.percentile(valid_data, 1))  # 1st percentile
            vmax = float(np.percentile(valid_data, 99))  # 99th percentile
            logger.info(f"Wave height range (ft): {vmin:.2f} to {vmax:.2f}")
            
            # Create levels for contour plot
            levels = np.linspace(vmin, vmax, 91)  # 90 intervals
            
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
                extend='both'
            )
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error processing wave data: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            logger.error(f"Coordinates: {list(data.coords)}")
            raise