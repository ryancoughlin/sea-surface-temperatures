from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import logging
import cartopy.crs as ccrs
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from .base_visualizer import BaseVisualizer

from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

class SSTVisualizer(BaseVisualizer):
    def generate_image(self, data: xr.DataArray | xr.Dataset, region: str, dataset: str, date: str) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate SST visualization."""
        try:
            # Handle Dataset vs DataArray
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                sst_var = next(var for var in variables if 'sst' in var.lower() or 'temperature' in var.lower())
                data = data[sst_var]

            
            # Convert temperature using source unit from settings
            source_unit = SOURCES[dataset].get('source_unit', 'C')
            data = convert_temperature_to_f(data, source_unit=source_unit)
            expanded_data = self.expand_coastal_data(data)
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
            
            # Create colormap from color scale
            cmap = LinearSegmentedColormap.from_list('sst_detailed', SOURCES[dataset]['color_scale'], N=1024)
            
            # Calculate dynamic ranges from valid data
            valid_data = expanded_data.values[~np.isnan(expanded_data.values)]
            vmin = float(np.percentile(valid_data, 1))  # 1st percentile
            vmax = float(np.percentile(valid_data, 99))  # 99th percentile

            mesh = ax.pcolormesh(
                expanded_data['longitude'],
                expanded_data['latitude'],
                expanded_data.values,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                shading='gouraud',
                vmin=vmin,
                vmax=vmax,
                rasterized=True,
                zorder=1
            )
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
