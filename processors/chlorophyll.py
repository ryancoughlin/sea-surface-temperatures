import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
import cartopy.crs as ccrs
import matplotlib.colors as mcolors
from pathlib import Path
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from typing import Tuple
from matplotlib.colors import LinearSegmentedColormap
import cartopy.feature as cfeature

logger = logging.getLogger(__name__)

class ChlorophyllProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Tuple[Path, None]:
        """Generate chlorophyll visualization."""
        try:
            # 1. Load and prepare data
            logger.info(f"Processing chlorophyll data for {region}")
            ds = xr.open_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # 2. Handle dimensions
            if 'time' in data.dims:
                data = data.isel(time=0)
            if 'altitude' in data.dims:
                data = data.isel(altitude=0)
            
            # 3. Get coordinates and mask to region
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            bounds = REGIONS[region]['bounds']
            
            # 4. Mask to region
            regional_data = data.where(
                (data[lon_name] >= bounds[0][0]) & 
                (data[lon_name] <= bounds[1][0]) &
                (data[lat_name] >= bounds[0][1]) & 
                (data[lat_name] <= bounds[1][1]),
                drop=True
            )
            
            # Log regional data stats
            valid_regional = regional_data.values[~np.isnan(regional_data.values)]
            logger.info(f"Valid regional data range: {valid_regional.min()} to {valid_regional.max()}")
            
            # 5. Expand coastal data for smoother visualization
            expanded_data = self.expand_coastal_data(regional_data, buffer_size=4)

            # 6. Create figure and plot
            fig, ax = self.create_axes(region)
            
            # Get color scale configuration
            color_config = SOURCES[dataset]['color_scale']
            cmap = LinearSegmentedColormap.from_list('chlorophyll', color_config['colors'], N=color_config['N'])
            
            # Create color normalization based on config
            if color_config.get('norm') == 'log':
                norm = mcolors.LogNorm(vmin=color_config['vmin'], vmax=color_config['vmax'])
            else:
                norm = None
                vmin = color_config['vmin']
                vmax = color_config['vmax']
                if vmin == "auto":
                    vmin = float(expanded_data.min())
                if vmax == "auto":
                    vmax = float(expanded_data.max())
            
            # Plot data with smooth interpolation
            mesh = ax.pcolormesh(
                expanded_data[lon_name],
                expanded_data[lat_name],
                expanded_data.values,
                transform=ccrs.PlateCarree(),
                norm=norm,
                vmin=None if norm else vmin,
                vmax=None if norm else vmax,
                cmap=cmap,
                shading='gouraud',  # Smooth interpolation
                rasterized=True,
                zorder=1
            )
            
            # Add land mask
            land = cfeature.NaturalEarthFeature('physical', 'land', '10m')
            ax.add_feature(land, facecolor='#B1C2D8', zorder=2)
            
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing chlorophyll data: {str(e)}")
            raise
