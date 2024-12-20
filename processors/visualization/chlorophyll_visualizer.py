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
from datetime import datetime
from processors.data.data_utils import extract_variables

logger = logging.getLogger(__name__)

class ChlorophyllVisualizer(BaseVisualizer):
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate chlorophyll visualization."""
        try:
            logger.info(f"🎨 Creating chlorophyll visualization for {dataset} in {region}")
            
            # Use chlor_a directly - it's the standard chlorophyll variable
            if 'chlor_a' not in data:
                raise ValueError("Required variable 'chlor_a' not found in dataset")
            
            processed_data = xr.Dataset({'chlor_a': data['chlor_a'].squeeze()})
            expanded_data = self.expand_coastal_data(processed_data)
            
            # Create figure
            fig, ax = self.create_axes(region)
            
            # Plot chlorophyll layer
            self._plot_chlorophyll(ax, expanded_data['chlor_a'])
            logger.info("   └── Added chlorophyll layer")
            
            return fig, None
            
        except Exception as e:
            logger.error(f"❌ Failed to create chlorophyll visualization: {str(e)}")
            raise
        
    def _plot_chlorophyll(self, ax: plt.Axes, chl_data: xr.DataArray) -> None:
        """Plot chlorophyll field using pcolormesh."""
        valid_data = chl_data.values[~np.isnan(chl_data.values)]
        if len(valid_data) == 0:
            raise ValueError("No valid chlorophyll data points found")
            
        vmin, vmax = float(np.nanmin(valid_data)), float(np.nanmax(valid_data))
        
        ax.pcolormesh(
            chl_data['longitude'],
            chl_data['latitude'],
            chl_data.values,
            transform=ccrs.PlateCarree(),
            cmap='viridis',
            shading='gouraud',
            vmin=vmin,
            vmax=vmax,
            rasterized=True
        )
