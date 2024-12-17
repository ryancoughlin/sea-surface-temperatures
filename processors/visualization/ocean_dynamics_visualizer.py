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
from scipy.interpolate import griddata

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
            source_config = SOURCES[dataset]
            
            # Get SSH data
            ssh_var = next(iter(source_config['source_datasets']['altimetry']['variables']))
            ssh_data = data[ssh_var]
            
            # Get current data
            u_var = next(var for var, cfg in source_config['source_datasets']['currents']['variables'].items() 
                        if cfg['type'] == 'current' and var.startswith('u'))
            v_var = next(var for var, cfg in source_config['source_datasets']['currents']['variables'].items() 
                        if cfg['type'] == 'current' and var.startswith('v'))
            
            u_data = data[u_var]
            v_data = data[v_var]
            
            fig, ax = self.create_axes(region)
            
            # Expand coastal data to reduce gaps
            ssh_expanded = self.expand_coastal_data(ssh_data)
            
            # Calculate ranges from valid data
            valid_data = ssh_expanded.values[~np.isnan(ssh_expanded.values)]
            vmin = float(np.percentile(valid_data, 1))
            vmax = float(np.percentile(valid_data, 99))
            
            # Plot SSH with simplified settings matching SST visualizer
            ssh_plot = ax.pcolormesh(
                ssh_expanded['longitude'],
                ssh_expanded['latitude'],
                ssh_expanded.values,
                transform=ccrs.PlateCarree(),
                cmap=self._create_ssh_colormap(),
                vmin=vmin,
                vmax=vmax,
                shading='gouraud',
                alpha=0.8,
                rasterized=True,
                zorder=1
            )
            
            # Process current data separately
            skip = 1
            u_plot = u_data[::skip, ::skip]
            v_plot = v_data[::skip, ::skip]
            
            magnitude = np.sqrt(u_plot**2 + v_plot**2)
            magnitude_nonzero = np.maximum(magnitude, 1e-10)
            u_norm = u_plot / magnitude_nonzero
            v_norm = v_plot / magnitude_nonzero
            
            quiver_lons = u_data.longitude[::skip]
            quiver_lats = u_data.latitude[::skip]
            quiver_lon_mesh, quiver_lat_mesh = np.meshgrid(quiver_lons, quiver_lats)
            
            ax.quiver(
                quiver_lon_mesh,
                quiver_lat_mesh,
                u_norm,
                v_norm,
                magnitude,
                transform=ccrs.PlateCarree(),
                cmap='RdBu_r',
                scale=100,
                scale_units='width',
                width=0.001,
                headwidth=3.6,
                headlength=3.6,
                headaxislength=3.5,
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