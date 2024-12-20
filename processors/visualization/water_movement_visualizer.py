from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
import cartopy.crs as ccrs
from .base_visualizer import BaseVisualizer
from config.settings import SOURCES
from typing import Tuple, Optional, Dict, Final, List
from datetime import datetime
from matplotlib.colors import LinearSegmentedColormap
from scipy.interpolate import griddata

logger = logging.getLogger(__name__)

# Visualization constants
ARROW_SETTINGS: Final[Dict] = {
    'color': 'white',
    'scale': 100,
    'scale_units': 'width',
    'width': 0.001,
    'headwidth': 3.6,
    'headlength': 3.6,
    'headaxislength': 3.5,
    'alpha': 0.7,
    'pivot': 'middle',
    'zorder': 2
}

SSH_COLORS: Final[List[str]] = [
    '#053061', '#2166ac', '#4393c3', '#92c5de', '#d1e5f0',
    '#f7f7f7', '#fddbc7', '#f4a582', '#d6604d', '#b2182b'
]

class WaterMovementVisualizer(BaseVisualizer):
    """Creates visualizations of water movement patterns."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.arrow_spacing = 25  # Number of arrows in smallest dimension
        self._ssh_cmap = LinearSegmentedColormap.from_list('ssh_colormap', SSH_COLORS, N=1024)
    
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate visualization combining sea surface height and currents."""
        try:
            logger.info(f"🎨 Creating water movement visualization for {dataset} in {region}")
            
            # Keep as Dataset throughout processing
            processed_data = self._prepare_data(data, dataset)
            expanded_data = self.expand_coastal_data(processed_data)
            
            # Create figure
            fig, ax = self.create_axes(region)
            
            # Plot layers - only convert to DataArray at plot time
            self._plot_ssh(ax, expanded_data['ssh'])
            logger.info("   ├── Added sea surface height layer")
            
            self._plot_currents(ax, expanded_data['uo'], expanded_data['vo'])
            logger.info("   └── Added current vectors")
            
            return fig, None
            
        except Exception as e:
            logger.error(f"❌ Failed to create water movement visualization: {str(e)}")
            raise
            
    def _prepare_data(self, data: xr.Dataset, dataset: str) -> xr.Dataset:
        """Prepare dataset for visualization."""
        source_config = SOURCES[dataset]
        ssh_var = next(iter(source_config['source_datasets']['altimetry']['variables']))
        
        # Create new Dataset with required variables
        return xr.Dataset({
            'ssh': data[ssh_var],
            'uo': data['uo'].squeeze(),
            'vo': data['vo'].squeeze()
        })
        
    def _plot_ssh(self, ax: plt.Axes, ssh_data: xr.DataArray) -> None:
        """Plot SSH field using pcolormesh."""
        valid_data = ssh_data.values[~np.isnan(ssh_data.values)]
        vmin, vmax = float(np.nanmin(valid_data)), float(np.nanmax(valid_data))
        
        ax.pcolormesh(
            ssh_data['longitude'],
            ssh_data['latitude'],
            ssh_data.values,
            transform=ccrs.PlateCarree(),
            cmap=self._ssh_cmap,
            shading='gouraud',
            vmin=vmin,
            vmax=vmax,
            rasterized=True,
            zorder=1
        )
        
    def _plot_currents(self, ax: plt.Axes, u_data: xr.DataArray, v_data: xr.DataArray) -> None:
        """Plot current vectors using quiver."""
        # Calculate spacing
        nx, ny = len(u_data.longitude), len(u_data.latitude)
        skip = max(1, min(nx, ny) // self.arrow_spacing)
        
        # Create grid
        lon_mesh, lat_mesh = np.meshgrid(u_data.longitude[::skip], u_data.latitude[::skip])
        
        # Process vectors
        magnitude = np.sqrt(u_data**2 + v_data**2)
        threshold = float(np.percentile(magnitude.values[~np.isnan(magnitude.values)], 5))
        mask = magnitude.values > threshold
        
        u_masked = np.where(mask, u_data.values, np.nan)[::skip, ::skip]
        v_masked = np.where(mask, v_data.values, np.nan)[::skip, ::skip]
        
        # Normalize
        mag_subset = np.sqrt(u_masked**2 + v_masked**2)
        mag_subset = np.maximum(mag_subset, 1e-10)
        u_norm = u_masked / mag_subset
        v_norm = v_masked / mag_subset
        
        # Plot
        ax.quiver(
            lon_mesh,
            lat_mesh,
            u_norm,
            v_norm,
            transform=ccrs.PlateCarree(),
            **ARROW_SETTINGS
        )