from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import logging
import cartopy.crs as ccrs
import numpy as np
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f, interpolate_data

logger = logging.getLogger(__name__)

class SSTProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Path:
        """Generate SST visualization."""
        try:
            # Get paths and create directories
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            asset_paths.image.parent.mkdir(parents=True, exist_ok=True)

            # Load and process data
            ds = xr.open_dataset(data_path)
            data = ds[SOURCES[dataset]['variables'][0]]
            
            # Force 2D data by selecting first index of time and depth if they exist
            if 'time' in data.dims:
                data = data.isel(time=0)
            if 'depth' in data.dims:
                data = data.isel(depth=0)
            
            # Get coordinates and bounds
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            bounds = REGIONS[region]['bounds']
            
            # Mask to region
            regional_data = data.where(
                (data[lon_name] >= bounds[0][0]) & 
                (data[lon_name] <= bounds[1][0]) &
                (data[lat_name] >= bounds[0][1]) & 
                (data[lat_name] <= bounds[1][1]), 
                drop=True
            )
            
            # Convert temperature
            regional_data = convert_temperature_to_f(regional_data)
            
            # Interpolate data for higher resolution
            interpolated_data = interpolate_data(regional_data, factor=2)
            
            # Create figure and axes
            fig, ax = self.create_masked_axes(region)
            
            # Plot with higher resolution
            mesh = ax.pcolormesh(
                regional_data[lon_name],
                regional_data[lat_name],
                regional_data.values,  # Use original data for coordinates
                transform=ccrs.PlateCarree(),
                cmap=SOURCES[dataset]['color_scale'],
                shading='gouraud',
                vmin=36,
                vmax=88,
                zorder=1
            )
            
            return self.save_image(fig, region, dataset, date)
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
