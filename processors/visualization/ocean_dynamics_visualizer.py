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
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ARROW_DENSITY = 30  # Number of arrows along each dimension
    
    def _create_ssh_colormap(self):
        """Create a diverging colormap for SSH."""
        colors = ['#053061', '#2166ac', '#4393c3', '#92c5de', '#d1e5f0',
                 '#f7f7f7', '#fddbc7', '#f4a582', '#d6604d', '#b2182b']
        return LinearSegmentedColormap.from_list('ssh_colormap', colors, N=1024)
    
    def _downsample_vectors(self, lons: np.ndarray, lats: np.ndarray, 
                          u: np.ndarray, v: np.ndarray) -> Tuple[np.ndarray, ...]:
        """Downsample vector data for quiver plots using grid subsampling."""
        # Log input data structure
        logger.info(f"Original grid dimensions: {u.shape}")
        logger.info(f"Longitude range: {lons.min():.3f} to {lons.max():.3f}")
        logger.info(f"Latitude range: {lats.min():.3f} to {lats.max():.3f}")
        logger.info(f"Current velocities range - U: {float(u.min()):.3f} to {float(u.max()):.3f}, V: {float(v.min()):.3f} to {float(v.max()):.3f}")
        
        # Calculate stride for even sampling
        lat_stride = max(1, len(lats) // self.ARROW_DENSITY)
        lon_stride = max(1, len(lons) // self.ARROW_DENSITY)
        
        # Subsample the grid using strides
        ds_lats = lats[::lat_stride]
        ds_lons = lons[::lon_stride]
        ds_u = u[::lat_stride, ::lon_stride]
        ds_v = v[::lat_stride, ::lon_stride]
        
        # Log results
        logger.info(f"Downsampled grid dimensions: {ds_u.shape}")
        logger.info(f"Points reduced from {u.size} to {ds_u.size}")
        logger.info(f"Stride sizes - lat: {lat_stride}, lon: {lon_stride}")
        logger.info(f"Downsampled velocities range - U: {float(ds_u.min()):.3f} to {float(ds_u.max()):.3f}, V: {float(ds_v.min()):.3f} to {float(ds_v.max()):.3f}")
        
        return ds_lons, ds_lats, ds_u, ds_v
    
    def _normalize_vectors(self, u: np.ndarray, v: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Normalize vectors and calculate magnitudes."""
        magnitude = np.sqrt(u**2 + v**2)
        magnitude_nonzero = np.maximum(magnitude, 1e-10)
        u_norm = u / magnitude_nonzero
        v_norm = v / magnitude_nonzero
        return u_norm, v_norm, magnitude
    
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate visualization combining sea surface height and currents."""
        try:
            source_config = SOURCES[dataset]
            
            # Log dataset structure
            logger.info(f"Dataset variables: {list(data.variables)}")
            logger.info(f"Dataset dimensions: {data.dims}")
            
            # Get SSH data
            ssh_var = next(iter(source_config['source_datasets']['altimetry']['variables']))
            ssh_data = data[ssh_var]
            
            # Get current data
            u_data = data['uo'].squeeze()
            v_data = data['vo'].squeeze()
            
            # Log the raw data structure
            logger.info(f"Raw current data shapes - U: {u_data.shape}, V: {v_data.shape}")
            logger.info(f"Raw current ranges - U: {float(u_data.min()):.3f} to {float(u_data.max()):.3f}")
            logger.info(f"Raw current ranges - V: {float(v_data.min()):.3f} to {float(v_data.max()):.3f}")
            
            fig, ax = self.create_axes(region)
            
            # Plot SSH first
            ssh_expanded = self.expand_coastal_data(ssh_data)
            valid_data = ssh_expanded.values[~np.isnan(ssh_expanded.values)]
            vmin = float(np.percentile(valid_data, 1))
            vmax = float(np.percentile(valid_data, 99))
            
            ssh_plot = ax.pcolormesh(
                ssh_expanded['longitude'],
                ssh_expanded['latitude'],
                ssh_expanded.values,
                transform=ccrs.PlateCarree(),
                cmap=self._create_ssh_colormap(),
                vmin=vmin,
                vmax=vmax,
                shading='gouraud',
                alpha=1,
                rasterized=True,
                zorder=1
            )
            
            # Create meshgrid from raw coordinates
            lon_mesh, lat_mesh = np.meshgrid(u_data.longitude.values, u_data.latitude.values)
            
            # Normalize vectors and get magnitude
            u_norm, v_norm, magnitude = self._normalize_vectors(u_data.values, v_data.values)
            
            # Plot normalized currents colored by magnitude
            ax.quiver(
                lon_mesh,
                lat_mesh,
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