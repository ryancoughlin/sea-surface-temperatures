from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
import cartopy.crs as ccrs
from matplotlib.colors import LinearSegmentedColormap
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS


logger = logging.getLogger(__name__)

class WavesProcessor(BaseImageProcessor):
    def __init__(self, path_manager=None):
        super().__init__(path_manager)

    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Path:
        """Generate wave visualization showing height and direction."""
        try:
            # Load data with chunking
            ds = xr.open_dataset(data_path, chunks={'time': 1})
            
            # Extract and process data efficiently
            height = ds['VHM0'].isel(time=0).load()  # Explicit loading
            direction = ds['VMDR'].isel(time=0).load()
            
            # Cache coordinates
            lon_name, lat_name = self.get_coordinate_names(height)
            lons = height[lon_name].values
            lats = height[lat_name].values
            
            # Downsample for quiver plot
            stride = self._calculate_optimal_stride(lons.size)
            
            # Get coordinates and mask to region
            bounds = REGIONS[region]['bounds']
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
            
            # Convert input data from meters to feet
            height_ft = height * 3.28084

            # Create detailed color gradient - 3 colors per foot
            colors = [
                # 0-1 ft (calm)
                '#053061', '#0a3666', '#0f3d6c',
                # 1-2 ft
                '#164270', '#1c4785', '#234d91',
                # 2-3 ft
                '#2c5ea0', '#3165a6', '#366dad',
                # 3-4 ft
                '#3d77bb', '#417fc0', '#4687c4',
                # 4-5 ft
                '#4b8bc2', '#5293c7', '#599bcc',
                # 5-6 ft
                '#5EA1CF', '#67aad3', '#70b2d7',
                # 6-7 ft
                '#73B3D8', '#7cbbdd', '#85c3e1',
                # 7-8 ft
                '#88C4E2', '#91cce6', '#9ad4ea',
                # 8-9 ft
                '#9DD6EC', '#a6def0', '#afe5f4',
                # 9-10 ft
                '#B2E5F4', '#bae7f3', '#c1e9f2',
                # 10-12 ft (moderate)
                '#c6dbef', '#cdddf0', '#d3dff1',
                # 12-14 ft
                '#d9e6f2', '#e0e9f3', '#e7ecf4',
                # 14-16 ft
                '#e5eef4', '#edf1f6', '#f0f5f7',
                # 16-18 ft (transition)
                '#f2f2f1', '#f3efeb', '#f5ebe6',
                # 18-20 ft
                '#f4e7df', '#f3e3d9', '#f3e0d4',
                # 20-22 ft (building)
                '#f2d9c8', '#f1d1bc', '#f0c5ac',
                # 22-24 ft
                '#ecb399', '#e8a086', '#e48d73',
                # 24-26 ft (large)
                '#dd7960', '#d66552', '#d15043',
                # 26-28 ft
                '#cb3e36', '#c52828', '#bf1f1f',
                # >28 ft (extreme)
                '#b81717', '#b01010', '#a80808'
            ]
            
            # Create levels in thirds of feet
            feet_levels = np.arange(0, 30, 1/3)  # Creates levels every 1/3 foot
            
            # Plot wave height contours using feet
            cmap = LinearSegmentedColormap.from_list('wave_heights', colors)

            contourf = ax.contourf(
                height_ft[lon_name],
                height_ft[lat_name],
                height_ft.values,
                levels=feet_levels,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                alpha=0.9,
                zorder=1,
                extend='both'
            )
                        
            # Plot direction arrows on top
            # u = -np.sin(np.deg2rad(direction[::stride, ::stride]))
            # v = -np.cos(np.deg2rad(direction[::stride, ::stride]))
            
            # ax.quiver(
            #     height[lon_name][::stride],
            #     height[lat_name][::stride],
            #     u, v,
            #     transform=ccrs.PlateCarree(),
            #     color='black',
            #     alpha=0.5,
            #     scale=35,
            #     width=0.003,
            #     headwidth=3,
            #     headlength=4,
            #     headaxislength=3,
            #     zorder=3
            # )
            
            return self.save_image(fig, region, dataset, date)
            
        except Exception as e:
            logger.error(f"Error processing wave data: {str(e)}")
            raise

    # def _calculate_optimal_stride(self, data_size: int) -> int:
    #     """Calculate a simple stride value to avoid overcrowding arrows.
    #     Returns larger stride for larger datasets."""
    #     if data_size > 500:
    #         return 8
    #     elif data_size > 200:
    #         return 4
    #     return 2