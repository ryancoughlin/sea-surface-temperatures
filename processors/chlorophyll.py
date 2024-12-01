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
            # Load preprocessed data
            logger.info(f"Processing chlorophyll data for {region}")
            ds = xr.open_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Get valid data for ranges
            valid_data = data.values[~np.isnan(data.values)]
            logger.info(f"[RANGES] Image data min/max: {valid_data.min():.4f} to {valid_data.max():.4f}")
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'

            # Create figure and plot
            fig, ax = self.create_axes(region)
            
            # Create colormap from color scale
            cmap = LinearSegmentedColormap.from_list('chlorophyll', SOURCES[dataset]['color_scale'], N=1024)
            
            # Use actual min/max for visualization
            vmin = float(valid_data.min())
            vmax = float(valid_data.max())
            
            norm = mcolors.LogNorm(vmin=vmin, vmax=vmax)
            
            # Plot data with smooth interpolation
            mesh = ax.pcolormesh(
                data[lon_name],
                data[lat_name],
                data.values,
                transform=ccrs.PlateCarree(),
                norm=norm,
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
