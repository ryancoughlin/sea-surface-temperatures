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
from matplotlib.colors import LinearSegmentedColormap

logger = logging.getLogger(__name__)

class OceanDynamicsVisualizer(BaseVisualizer):
    """Visualizer for combined ocean dynamics data (sea surface height and currents)."""
    
    def _create_ssh_colormap(self):
        """Create a diverging colormap for SSH."""
        colors = ['#053061', '#2166ac', '#4393c3', '#92c5de', '#d1e5f0',
                 '#f7f7f7', '#fddbc7', '#f4a582', '#d6604d', '#b2182b']
        return LinearSegmentedColormap.from_list('ssh_colormap', colors, N=1024)
    
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate visualization combining sea surface height and currents."""
        try:
            # Get variable names from config
            source_config = SOURCES[dataset]
            ssh_var = next(iter(source_config['source_datasets']['altimetry']['variables']))
            u_var = next(var for var, cfg in source_config['source_datasets']['currents']['variables'].items() 
                        if cfg['type'] == 'current' and var.startswith('u'))
            v_var = next(var for var, cfg in source_config['source_datasets']['currents']['variables'].items() 
                        if cfg['type'] == 'current' and var.startswith('v'))
            
            # Create figure with map
            fig, ax = self.create_axes(region)
            
            # 1. Plot SSH background
            ssh_data = self.expand_coastal_data(data[ssh_var])
            ax.pcolormesh(
                ssh_data['longitude'],
                ssh_data['latitude'],
                ssh_data.values,
                transform=ccrs.PlateCarree(),
                cmap=self._create_ssh_colormap(),
                shading='gouraud',
                alpha=0.8,
                rasterized=True,
                zorder=1
            )
            
            # 2. Prepare current data
            skip = 1  # Reduced skip for more arrows
            
            # Get coordinates
            lons = data.longitude.values[::skip]
            lats = data.latitude.values[::skip]
            
            # Create meshgrid
            lon_mesh, lat_mesh = np.meshgrid(lons, lats)
            
            # Get current components and subsample
            u_plot = data[u_var].values[::skip, ::skip]
            v_plot = data[v_var].values[::skip, ::skip]
            
            # Calculate current magnitude for scaling
            magnitude = np.sqrt(u_plot**2 + v_plot**2)
            
            # Normalize vectors for consistent arrow size
            magnitude_nonzero = np.maximum(magnitude, 1e-10)  # Avoid division by zero
            u_norm = u_plot / magnitude_nonzero
            v_norm = v_plot / magnitude_nonzero
            
            # Plot arrows - smaller fancy arrows with triangle heads
            ax.quiver(
                lon_mesh,
                lat_mesh,
                u_norm,
                v_norm,
                magnitude,
                transform=ccrs.PlateCarree(),
                cmap='RdBu_r',
                scale=60,  # Even smaller arrows
                scale_units='width',
                width=0.002,  # Thinner arrows
                headwidth=5,  # Triangle head shape
                headlength=5,
                headaxislength=4.5,
                alpha=0.7,
                pivot='middle',
                zorder=2
            )
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error generating ocean dynamics visualization: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            raise