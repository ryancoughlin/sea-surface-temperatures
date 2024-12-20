import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
import cartopy.crs as ccrs
from matplotlib.colors import LinearSegmentedColormap
from .base_visualizer import BaseVisualizer
from config.settings import SOURCES
from typing import Tuple, Optional, Dict, Any, Final

logger = logging.getLogger(__name__)

# Constants
METERS_TO_FEET: Final[float] = 3.28084
DIRECTION_STRIDE: Final[int] = 5
STREAMLINE_DENSITY: Final[float] = 1.5
STREAMLINE_WIDTH: Final[float] = 0.5
STREAMLINE_ARROW_SIZE: Final[float] = 0.5
WAVE_HEIGHT_VAR: Final[str] = 'VHM0'
WAVE_DIRECTION_VAR: Final[str] = 'VMDR'

class WavesVisualizer(BaseVisualizer):
    """Visualizes ocean wave characteristics including significant wave height and mean direction."""
    
    def generate_image(
        self, 
        data: xr.Dataset, 
        region: str, 
        dataset: str, 
        date: str
    ) -> Tuple[plt.Figure, Optional[Dict[str, Any]]]:
        """Generate wave visualization combining height and direction data."""
        try:
            # Get coordinates and create figure
            longitude, latitude = self.get_coordinate_names(data)
            fig, ax = self.create_axes(region)
            
            # Convert height to feet and prepare colormap
            height = data[WAVE_HEIGHT_VAR] * METERS_TO_FEET
            cmap = LinearSegmentedColormap.from_list(
                'wave_heights', 
                SOURCES[dataset]['color_scale'], 
                N=256
            )
            
            # Calculate data range
            valid_data = height.values[~np.isnan(height.values)]
            if len(valid_data) == 0:
                logger.error("No valid wave height data found in dataset")
                raise ValueError("No valid wave height data")
                
            vmin = float(np.min(valid_data))
            vmax = float(np.max(valid_data))
            logger.info(f"Wave height range (ft): {vmin:.2f} to {vmax:.2f}")
            
            # Create pcolormesh for wave heights
            mesh = ax.pcolormesh(
                data[longitude],
                data[latitude],
                height.values,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
                alpha=0.9,
                zorder=1,
                shading='auto'
            )
            
            self._add_direction_streamlines(ax, data, longitude, latitude)
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error saving visualization: {str(e)}")
            raise
            
    def _add_direction_streamlines(
        self,
        ax: plt.Axes,
        data: xr.Dataset,
        longitude: str,
        latitude: str,
        stride: int = DIRECTION_STRIDE
    ) -> None:
        """Add wave direction streamlines to the plot."""
        # Convert direction from meteorological to mathematical convention
        dir_rad = np.deg2rad(270 - data[WAVE_DIRECTION_VAR])
        
        # Calculate u and v components
        u = np.cos(dir_rad)
        v = np.sin(dir_rad)
        
        # Create streamplot
        ax.streamplot(
            data[longitude][::stride],
            data[latitude][::stride],
            u[::stride, ::stride],
            v[::stride, ::stride],
            transform=ccrs.PlateCarree(),
            density=STREAMLINE_DENSITY,
            linewidth=STREAMLINE_WIDTH,
            color='white',
            arrowsize=STREAMLINE_ARROW_SIZE,
            zorder=2
        )