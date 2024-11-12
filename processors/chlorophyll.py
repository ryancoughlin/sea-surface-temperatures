import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
from pathlib import Path
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import interpolate_data
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
from typing import Tuple

logger = logging.getLogger(__name__)

class ChlorophyllProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Tuple[Path, None]:
        """Generate chlorophyll visualization."""
        try:
            # Load data with debug logging
            logger.info(f"Processing chlorophyll data for {region}")
            ds = xr.open_dataset(data_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Handle dimensions
            if 'time' in data.dims:
                data = data.isel(time=0)
            if 'altitude' in data.dims:
                data = data.isel(altitude=0)
            
            # Get bounds and coordinates
            bounds = REGIONS[region]['bounds']
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Check if we have valid coordinates
            if data[lon_name].size == 0 or data[lat_name].size == 0:
                logger.warning("Empty coordinate dimensions found")
                raise ValueError("Dataset has empty coordinate dimensions")
            
            # Mask to region
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Check if we have any data after masking
            if regional_data.size == 0:
                logger.warning("No data available in the specified region")
                fig, ax = plt.subplots(figsize=(10, 8))
                ax.text(0.5, 0.5, 'No Data Available', 
                       horizontalalignment='center',
                       verticalalignment='center',
                       transform=ax.transAxes)
                ax.axis('off')
            else:
                # Interpolate data for higher resolution
                interpolated_data = interpolate_data(regional_data, factor=2)
                
                # Create masked figure and axes
                fig, ax = self.create_masked_axes(region)
                
                # Plot data with proper transform
                mesh = ax.pcolormesh(
                    regional_data[lon_name],
                    regional_data[lat_name],
                    regional_data.values,  # Use original data for coordinates
                    transform=ccrs.PlateCarree(),
                    cmap=SOURCES[dataset]['color_scale'],
                    shading='gouraud',
                    zorder=1
                )
                
            return self.save_image(fig, region, dataset, date), None
            
        except Exception as e:
            logger.error(f"Error processing chlorophyll data: {str(e)}")
            raise
