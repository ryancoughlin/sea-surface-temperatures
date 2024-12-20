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
from datetime import datetime

logger = logging.getLogger(__name__)

class ChlorophyllVisualizer(BaseVisualizer):
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate chlorophyll visualization."""
        try:
            logger.info(f"🎨 Creating chlorophyll visualization for {dataset} in {region}")
            
            # Use chlor_a directly - it's the standard chlorophyll variable
            if 'chlor_a' not in data:
                raise ValueError("Required variable 'chlor_a' not found in dataset")
            
            processed_data = xr.Dataset({'chlor_a': data['chlor_a']})
            expanded_data = self.expand_coastal_data(processed_data)
            
            # Create figure
            fig, ax = self.create_axes(region)
            
            # Plot chlorophyll layer
            self._plot_chlorophyll(ax, expanded_data['chlor_a'], dataset)
            logger.info("   └── Added chlorophyll layer")
            
            return fig, None
            
        except Exception as e:
            logger.error(f"❌ Failed to create chlorophyll visualization: {str(e)}")
            raise
        
    def _plot_chlorophyll(self, ax: plt.Axes, chl_data: xr.DataArray, dataset: str) -> None:
        """Plot chlorophyll field using pcolormesh."""
        valid_data = chl_data.values[~np.isnan(chl_data.values)]
        
        cmap = LinearSegmentedColormap.from_list('chlorophyll', SOURCES[dataset]['color_scale'], N=1024)

        vmin = float(valid_data.min())
        vmax = float(valid_data.max())

        norm = mcolors.LogNorm(vmin=max(vmin, 0.01), vmax=vmax)

        ax.pcolormesh(
            chl_data['longitude'],
            chl_data['latitude'],
            chl_data.values,
            transform=ccrs.PlateCarree(),
            cmap=cmap,
            norm=norm,
            shading='gouraud',
            rasterized=True
        )
