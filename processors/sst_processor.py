from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import logging
import cartopy.crs as ccrs
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f

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
            
            # Force 2D data
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
            
            # Convert temperature and expand coastal data
            regional_data = convert_temperature_to_f(regional_data)
            expanded_data = self.expand_coastal_data(regional_data, buffer_size=3)
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
            
            # Get color scale configuration
            color_config = SOURCES[dataset]['color_scale']
            cmap = LinearSegmentedColormap.from_list('sst_detailed', color_config['colors'], N=color_config['N'])
            
            # Use configured vmin/vmax or calculate from data
            vmin = color_config['vmin']
            vmax = color_config['vmax']
            if vmin == "auto":
                vmin = float(expanded_data.min())
            if vmax == "auto":
                vmax = float(expanded_data.max())

            mesh = ax.pcolormesh(
                expanded_data[lon_name],
                expanded_data[lat_name],
                expanded_data.values,
                transform=ccrs.PlateCarree(),
                cmap=cmap,
                shading='gouraud',
                vmin=vmin,
                vmax=vmax,
                rasterized=True,
                zorder=1
            )
            
            return self.save_image(fig, region, dataset, date)
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
