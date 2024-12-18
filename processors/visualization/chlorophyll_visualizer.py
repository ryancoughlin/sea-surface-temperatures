import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
import cartopy.crs as ccrs
import matplotlib.colors as mcolors
from pathlib import Path
from .base_visualizer import BaseVisualizer
from config.settings import SOURCES
from typing import Dict, Optional, Tuple
from matplotlib.colors import LinearSegmentedColormap
import cartopy.feature as cfeature

logger = logging.getLogger(__name__)

class ChlorophyllVisualizer(BaseVisualizer):
    def generate_image(self, data: xr.DataArray | xr.Dataset, region: str, dataset: str, date: str) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate chlorophyll visualization."""
        try:
            logger.info(f"Processing chlorophyll data for {region}")
            
            # Handle Dataset vs DataArray
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                chl_var = next(var for var, config in variables.items() if config['type'] == 'chlorophyll')
                data = data[chl_var]
            
            # Force 2D data
            if 'time' in data.dims:
                data = data.isel(time=0)
            if 'depth' in data.dims:
                data = data.isel(depth=0)
            
            # Convert to float64 to ensure numpy compatibility
            data = data.astype(np.float64)
            
            # Get valid data for ranges
            valid_data = data.values[~np.isnan(data.values)]
            
            if len(valid_data) == 0:
                raise ValueError("No valid chlorophyll data points found")

            # Apply coastal buffer to fill gaps
            logger.info("Applying coastal buffer to fill data gaps")
            buffered_data = self.expand_coastal_data(data)
            
            # Create figure and plot
            fig, ax = self.create_axes(region)
            
            # Create colormap from color scale
            cmap = LinearSegmentedColormap.from_list('chlorophyll', SOURCES[dataset]['color_scale'], N=1024)
            
            # Use actual min/max for visualization
            vmin = float(valid_data.min())
            vmax = float(valid_data.max())
            
            norm = mcolors.LogNorm(vmin=max(vmin, 0.01), vmax=vmax)  # Ensure positive values for log scale
            
            # Plot data with smooth interpolation
            mesh = ax.pcolormesh(
                buffered_data["longitude"],
                buffered_data["latitude"],
                buffered_data.values,
                transform=ccrs.PlateCarree(),
                norm=norm,
                cmap=cmap,
                shading='gouraud',  # Smooth interpolation
                rasterized=True,
                zorder=1
            )
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error processing chlorophyll data: {str(e)}")
            raise
