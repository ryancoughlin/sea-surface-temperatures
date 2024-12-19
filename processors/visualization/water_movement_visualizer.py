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

class WaterMovementVisualizer(BaseVisualizer):
    """Creates visualizations of water movement patterns."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.arrow_spacing = 25  # Number of arrows in smallest dimension
        
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
            u_data = data['uo'].squeeze()
            v_data = data['vo'].squeeze()
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
            
            # Expand coastal data using base visualizer method
            expanded_data = self.expand_coastal_data(ssh_data)
            
            # Calculate value range from valid data
            valid_data = expanded_data.values[~np.isnan(expanded_data.values)]
            vmin = float(np.nanmin(valid_data))
            vmax = float(np.nanmax(valid_data))
            
            # Plot SSH using pcolormesh
            mesh = ax.pcolormesh(
                expanded_data['longitude'],
                expanded_data['latitude'],
                expanded_data.values,
                transform=ccrs.PlateCarree(),
                cmap=self._create_ssh_colormap(),
                shading='gouraud',
                vmin=vmin,
                vmax=vmax,
                rasterized=True,
                zorder=1
            )
            
            # Prepare current vectors
            nx, ny = len(u_data.longitude), len(u_data.latitude)
            skip = max(1, min(nx, ny) // self.arrow_spacing)
            
            # Create coordinate grids for quiver
            lon_mesh, lat_mesh = np.meshgrid(u_data.longitude[::skip], u_data.latitude[::skip])
            
            # Compute magnitude and mask
            magnitude = np.sqrt(u_data**2 + v_data**2)
            threshold = float(np.percentile(magnitude.values[~np.isnan(magnitude.values)], 5))
            mask = magnitude.values > threshold
            
            # Prepare masked velocities
            u_masked = np.where(mask, u_data.values, np.nan)[::skip, ::skip]
            v_masked = np.where(mask, v_data.values, np.nan)[::skip, ::skip]
            
            # Normalize vectors
            mag_subset = np.sqrt(u_masked**2 + v_masked**2)
            mag_subset = np.maximum(mag_subset, 1e-10)
            u_norm = u_masked / mag_subset
            v_norm = v_masked / mag_subset
            
            # Plot arrows
            ax.quiver(
                lon_mesh,
                lat_mesh,
                u_norm,
                v_norm,
                transform=ccrs.PlateCarree(),
                color='white',
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
            logger.error(f"Error generating water movement visualization: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            raise