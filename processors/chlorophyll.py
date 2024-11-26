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
    CHLOROPHYLL_COLORS = [
        '#B1C2D8', '#A3B9D3', '#96B1CF', '#88A8CA', '#7AA0C5',  # Clear water
        '#6C98C0', '#5F8FBB', '#5187B6', '#437FB0', '#3577AB',  # Low chlorophyll
        '#2EAB87', '#37B993', '#40C79F', '#49D5AB', '#52E3B7',  # Transition to greens
        '#63E8B8', '#75EDB9', '#86F3BA', '#98F8BB', '#A9FDBB',  # Greens (moderate)
        '#C1F5A3', '#DAFD8B', '#F2FF73', '#FFF75B', '#FFE742',  # Lime yellows
        '#FFD629', '#FFC611', '#FFB600', '#FFA500', '#FF9400',  # Yellow-orange
        '#FF8300', '#FF7200', '#FF6100', '#FF5000', '#FF3F00'   # High chlorophyll
    ]
    
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
            
            # 5. Expand coastal data for smoother visualization
            expanded_data = self.expand_coastal_data(regional_data, buffer_size=4)

            # 6. Create figure and plot
            fig, ax = self.create_axes(region)
            
            # Create high-resolution colormap
            cmap = LinearSegmentedColormap.from_list('chlorophyll', self.CHLOROPHYLL_COLORS, N=1024)
            
            # Plot data with smooth interpolation
            mesh = ax.pcolormesh(
                expanded_data[lon_name],
                expanded_data[lat_name],
                expanded_data.values,
                transform=ccrs.PlateCarree(),
                norm=mcolors.LogNorm(vmin=0.05, vmax=15.0),
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
