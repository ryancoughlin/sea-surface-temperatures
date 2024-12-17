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
            
            # Extract data arrays
            ssh_data = data[ssh_var]
            u_data = data[u_var]
            v_data = data[v_var]
            
            # Fill coastal gaps
            ssh_expanded = self.expand_coastal_data(ssh_data)
            
            # Create figure with map
            fig, ax = self.create_axes(region)
            
            # Plot SSH background with smooth interpolation
            ssh_mesh = ax.pcolormesh(
                ssh_expanded['longitude'],
                ssh_expanded['latitude'],
                ssh_expanded.values,
                transform=ccrs.PlateCarree(),
                cmap=self._create_ssh_colormap(),
                shading='gouraud',
                alpha=0.8,
                rasterized=True,
                zorder=1
            )
            
            # Calculate target number of arrows based on figure size
            fig_width, fig_height = fig.get_size_inches()
            target_arrows = int(min(fig_width, fig_height) * 8)  # 8 arrows per inch
            
            # Calculate skip factor to achieve target arrow count
            nx, ny = len(data['longitude']), len(data['latitude'])
            skip = max(1, int(np.sqrt((nx * ny) / target_arrows)))
            
            # Create evenly spaced grid for arrows
            lon_slice = slice(skip//2, -1, skip)
            lat_slice = slice(skip//2, -1, skip)
            
            # Create coordinate grids for quiver
            lon_mesh, lat_mesh = np.meshgrid(data['longitude'][lon_slice], 
                                           data['latitude'][lat_slice])
            
            # Calculate current magnitude for coloring
            current_mag = np.sqrt(u_data**2 + v_data**2)
            
            # Normalize vectors for consistent arrow size
            u_plot = u_data[lat_slice, lon_slice]
            v_plot = v_data[lat_slice, lon_slice]
            mag_plot = current_mag[lat_slice, lon_slice]
            
            # Only plot arrows where we have valid data
            valid_mask = ~(np.isnan(u_plot) | np.isnan(v_plot))
            
            if valid_mask.any():
                quiver = ax.quiver(
                    lon_mesh[valid_mask],
                    lat_mesh[valid_mask],
                    u_plot[valid_mask],
                    v_plot[valid_mask],
                    mag_plot[valid_mask],
                    transform=ccrs.PlateCarree(),
                    cmap='RdBu_r',
                    scale=30,
                    width=0.004,
                    headwidth=4,
                    headlength=5,
                    alpha=0.7,
                    zorder=2
                )
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error generating ocean dynamics visualization: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            raise