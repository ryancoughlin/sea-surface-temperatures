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
    """Visualizes ocean wave characteristics including significant wave height and mean direction.
    
    Processes CMEMS wave data to create visualizations showing:
    - Significant wave height using pcolormesh
    - Wave direction using streamlines (when available)
    """
    
    def generate_image(
        self, 
        data: xr.Dataset, 
        region: str, 
        dataset: str, 
        date: str
    ) -> Tuple[plt.Figure, Optional[Dict[str, Any]]]:
        """Generate wave visualization combining height and direction data.
        
        Args:
            data: Dataset containing wave height ('VHM0') and optional direction ('VMDR')
            region: Geographic region identifier
            dataset: Source dataset identifier
            date: Date string for the visualization
            
        Returns:
            tuple: (matplotlib figure, optional metadata dictionary)
            
        Raises:
            ValueError: If no valid wave height data is found
            KeyError: If required variables are missing from dataset
        """
        try:
            # Get wave data
            try:
                height = data[WAVE_HEIGHT_VAR]  # Significant wave height
                direction = data.get(WAVE_DIRECTION_VAR)  # Optional mean wave direction
            except KeyError as e:
                logger.error(f"Required variable missing from dataset: {e}")
                raise
            
            # Handle dimensions
            for dim in ['time', 'depth']:
                if dim in height.dims:
                    try:
                        height = height.isel({dim: 0})
                        if direction is not None:
                            direction = direction.isel({dim: 0})
                    except ValueError as e:
                        logger.error(f"Error selecting {dim} dimension: {e}")
                        raise
            
            # Get coordinates and create figure
            lon_name, lat_name = self.get_coordinate_names(height)
            height = self.expand_coastal_data(height)
            fig, ax = self.create_axes(region)
            
            # Convert height to feet and prepare colormap
            height_ft = height * METERS_TO_FEET
            cmap = LinearSegmentedColormap.from_list(
                'wave_heights', 
                SOURCES[dataset]['color_scale'], 
                N=256
            )
            
            # Calculate data range
            valid_data = height_ft.values[~np.isnan(height_ft.values)]
            if len(valid_data) == 0:
                logger.error("No valid wave height data found in dataset")
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
                alpha=0.9,  # Standard alpha for consistency
                zorder=1,
                shading='auto'  # Standard shading for consistency
            )
            
            # Add wave direction if available
            if direction is not None:
                self._add_direction_streamlines(ax, direction, lon_name, lat_name)
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error processing wave data: {str(e)}")
            logger.error(f"Data dimensions: {data.dims}")
            logger.error(f"Variables: {list(data.variables)}")
            raise
            
    def _add_direction_streamlines(
        self,
        ax: plt.Axes,
        direction: xr.DataArray,
        lon_name: str,
        lat_name: str,
        stride: int = DIRECTION_STRIDE
    ) -> None:
        """Add wave direction streamlines to the plot.
        
        Args:
            ax: Matplotlib axes to plot on
            direction: Wave direction data array
            lon_name: Name of longitude coordinate
            lat_name: Name of latitude coordinate
            stride: Subsampling factor for direction data
        """
        # Convert direction from meteorological to mathematical convention
        dir_rad = np.deg2rad(270 - direction)
        
        # Calculate u and v components
        u = np.cos(dir_rad)
        v = np.sin(dir_rad)
        
        # Create streamplot
        ax.streamplot(
            direction[lon_name][::stride],
            direction[lat_name][::stride],
            u[::stride, ::stride],
            v[::stride, ::stride],
            transform=ccrs.PlateCarree(),
            density=STREAMLINE_DENSITY,
            linewidth=STREAMLINE_WIDTH,
            color='white',
            arrowsize=STREAMLINE_ARROW_SIZE,
            zorder=2
        )