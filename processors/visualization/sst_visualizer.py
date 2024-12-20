from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
import cartopy.crs as ccrs
from .base_visualizer import BaseVisualizer
from config.settings import SOURCES
from typing import Tuple, Optional, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

class SSTVisualizer(BaseVisualizer):
    """Creates visualizations of sea surface temperature data."""
    
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate SST visualization."""
        try:
            logger.info(f"🎨 Creating SST visualization for {dataset} in {region}")
            
            # Keep as Dataset throughout processing
            processed_data = self._prepare_data(data, dataset)
            expanded_data = self.expand_coastal_data(processed_data)
            
            # Create figure
            fig, ax = self.create_axes(region)
            
            # Plot layers - only convert to DataArray at plot time
            self._plot_sst(ax, expanded_data['sst'])
            logger.info("   └── Added SST layer")
            
            return fig, None
            
        except Exception as e:
            logger.error(f"❌ Failed to create SST visualization: {str(e)}")
            raise
            
    def _prepare_data(self, data: xr.Dataset, dataset: str) -> xr.Dataset:
        """Prepare dataset for visualization."""
        source_config = SOURCES[dataset]
        sst_var = next(iter(source_config['variables']))
        
        # Create new Dataset with required variables
        return xr.Dataset({
            'sst': data[sst_var]
        })
        
    def _plot_sst(self, ax: plt.Axes, sst_data: xr.DataArray) -> None:
        """Plot SST field using pcolormesh."""
        valid_data = sst_data.values[~np.isnan(sst_data.values)]
        vmin, vmax = float(np.nanmin(valid_data)), float(np.nanmax(valid_data))
        
        ax.pcolormesh(
            sst_data['longitude'],
            sst_data['latitude'],
            sst_data.values,
            transform=ccrs.PlateCarree(),
            cmap='RdYlBu_r',
            shading='gouraud',
            vmin=vmin,
            vmax=vmax,
            rasterized=True
        )
