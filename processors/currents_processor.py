from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
from scipy.ndimage import gaussian_filter
from scipy.interpolate import RectBivariateSpline
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
import xarray as xr
import cartopy.crs as ccrs
from typing import Tuple, Optional, Dict
from scipy.interpolate import griddata
import cartopy.feature as cfeature
from datetime import datetime
import cmocean  # Import cmocean for specialized oceanographic colormaps

logger = logging.getLogger(__name__)

class CurrentsProcessor(BaseImageProcessor):
    def convert_to_knots(self, speed_ms: np.ndarray) -> np.ndarray:
        """Convert speed from m/s to knots."""
        return speed_ms * 1.94384
    
    def smooth_field(self, data: np.ndarray, lon: np.ndarray, lat: np.ndarray, 
                    smoothing_factor: float = 0.5) -> np.ndarray:
        """Smooth the field using spline interpolation."""
        # Create spline interpolator
        spline = RectBivariateSpline(
            lat, lon, data,
            kx=3, ky=3,  # Cubic spline
            s=smoothing_factor  # Smoothing factor
        )
        
        # Create higher resolution grid for smoother output
        lon_fine = np.linspace(lon.min(), lon.max(), len(lon) * 2)
        lat_fine = np.linspace(lat.min(), lat.max(), len(lat) * 2)
        
        # Evaluate spline on fine grid
        return spline(lat_fine, lon_fine)
    
    def interpolate_field(self, data: xr.DataArray, lon: np.ndarray, lat: np.ndarray, 
                         scale_factor: int = 4) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Interpolate field using RectBivariateSpline for smooth geospatial interpolation.
        Optimized for evenly spaced gridded ocean data.
        """
        # Ensure arrays are 1D for interpolation
        lon_1d = lon.ravel()
        lat_1d = lat.ravel()
        
        # Create high-resolution grid (4x original resolution)
        lon_hr = np.linspace(lon_1d.min(), lon_1d.max(), len(lon_1d) * scale_factor)
        lat_hr = np.linspace(lat_1d.min(), lat_1d.max(), len(lat_1d) * scale_factor)
        
        # Create spline interpolator with parameters optimized for ocean currents
        spline = RectBivariateSpline(
            lat_1d, lon_1d, 
            data.values,
            kx=3,  # Cubic spline for smooth interpolation
            ky=3,  # Cubic spline for smooth interpolation
            s=0.001  # Very small smoothing factor for accurate representation
        )
        
        # Create meshgrid for interpolation
        lon_mesh, lat_mesh = np.meshgrid(lon_hr, lat_hr)
        
        # Evaluate spline on high-res grid
        interpolated = spline.ev(lat_mesh, lon_mesh)
        
        return interpolated, lon_hr, lat_hr
    
    def generate_image(self, data_path: Path, region: str, dataset: str, date: datetime) -> Tuple[Path, Optional[Dict]]:
        """Generate currents visualization with smooth transitions."""
        try:
            # Load and process data
            ds = xr.open_dataset(data_path)
            
            # Get velocity components
            u_data = ds[SOURCES[dataset]['variables'][0]].squeeze()
            v_data = ds[SOURCES[dataset]['variables'][1]].squeeze()
            
            # Calculate magnitude
            magnitude = np.sqrt(u_data**2 + v_data**2)
            
            # Get coordinates and bounds
            lon_name = 'longitude' if 'longitude' in magnitude.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in magnitude.coords else 'lat'
            bounds = REGIONS[region]['bounds']
            
            # Create figure and axes
            fig, ax = plt.subplots(
                figsize=(10, 10),
                subplot_kw={'projection': ccrs.PlateCarree()}
            )
            
            # Plot current magnitude using speed colormap
            mesh = ax.pcolormesh(
                magnitude[lon_name],
                magnitude[lat_name],
                magnitude.values,
                transform=ccrs.PlateCarree(),
                cmap=cmocean.cm.speed,  # Specialized colormap for current speed
                shading='gouraud',
                vmin=0,
                vmax=2.0,
                zorder=1
            )
            
            # Add dense streamlines for current direction
            # Increase density and reduce arrow size for better flow visualization
            ax.streamplot(
                magnitude[lon_name],
                magnitude[lat_name],
                u_data,
                v_data,
                transform=ccrs.PlateCarree(),
                color=('#ffffff', 0.5),           # White arrows for visibility
                density=3,               # Increase density of streamlines
                linewidth=0.5,          # Thinner lines
                arrowsize=0.8,          # Smaller arrows
                arrowstyle='->',        # Simple arrow style
                minlength=0.1,          # Allow shorter streamlines
                integration_direction='forward',  # Follow flow direction
                zorder=2,
            )
            
            # Set extent
            ax.set_extent([
                bounds[0][0],
                bounds[1][0],
                bounds[0][1],
                bounds[1][1]
            ], crs=ccrs.PlateCarree())
            
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            raise
