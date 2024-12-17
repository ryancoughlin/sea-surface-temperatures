from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
import cartopy.crs as ccrs
from matplotlib.colors import LinearSegmentedColormap
from .base_visualizer import BaseVisualizer
from config.settings import SOURCES
from config.regions import REGIONS
from typing import Tuple, Optional, Dict

logger = logging.getLogger(__name__)

class WavesVisualizer(BaseVisualizer):
    """Processor for generating wave height and direction visualizations."""
    
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: str) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate wave visualization showing height (pcolormesh) and direction (streamlines)."""
        try:
            # Get wave data
            height = data['VHM0']  # Significant wave height
            direction = data.get('VMDR')  # Mean wave direction
            
            # Handle dimensions
            for dim in ['time', 'depth']:
                if dim in height.dims:
                    height = height.isel({dim: 0})
                    if direction is not None:
                        direction = direction.isel({dim: 0})
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(height)
            
            # Expand data near coastlines
            height = self.expand_coastal_data(height)
            
            # Create figure
            fig, ax = self.create_axes(region)
            
            # Convert height to feet
            height_ft = height * 3.28084
            
            # Create colormap from color scale
            cmap = LinearSegmentedColormap.from_list('wave_heights', 
                                                    SOURCES[dataset]['color_scale'], 
                                                    N=256)  # Increased color resolution
            
            # Calculate dynamic ranges from valid data
            valid_data = height_ft.values[~np.isnan(height_ft.values)]
            if len(valid_data) == 0:
                logger.error("No valid wave height data")
                raise ValueError("No valid wave height data")
                
            vmin = float(np.min(valid_data))
            vmax = float(np.max(valid_data))
            logger.info(f"Wave height range (ft): {vmin:.2f} to {vmax:.2f}")
            
            # Create pcolormesh for wave heights
            mesh = ax.pcolormesh(
                height_ft[lon_name],
                height_ft[lat_name],
                height_ft.values,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
                alpha=1,
                zorder=1,
                shading='gouraud'
            )
            
            # Add wave direction if available
            if direction is not None:
                # Convert direction from meteorological to mathematical convention
                # and then to u,v components
                dir_rad = np.deg2rad(270 - direction)  # Convert to math convention
                
                # Calculate u and v components
                u = np.cos(dir_rad)
                v = np.sin(dir_rad)
                
                # Subsample grid for clearer visualization
                stride = 5
                
                # Create streamplot for wave direction
                ax.streamplot(
                    direction[lon_name][::stride],
                    direction[lat_name][::stride],
                    u[::stride, ::stride],
                    v[::stride, ::stride],
                    transform=ccrs.PlateCarree(),
                    density=1.5,
                    linewidth=1.2,
                    color='white',
                    arrowsize=1,
                    zorder=2
                )
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error processing wave data: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            logger.error(f"Coordinates: {list(data.coords)}")
            raise