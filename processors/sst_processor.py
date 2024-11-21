from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import logging
import cartopy.crs as ccrs
import numpy as np
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f
from matplotlib.colors import LinearSegmentedColormap
from scipy.ndimage import gaussian_filter

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
            
            # Create custom colormap for SST
            colors = [
                '#081d58', '#0d2167', '#122b76', '#173584', '#1c3f93',
                '#2149a1', '#2653b0', '#2b5dbe', '#3067cd', '#3571db',
                '#3a7bea', '#4185f8', '#41b6c4', '#46c0cd', '#4bcad6',
                '#50d4df', '#55dde8', '#5ae7f1', '#7fcdbb', '#8ed7c4',
                '#9de1cd', '#acebd6', '#bbf5df', '#c7e9b4', '#d6edb8',
                '#e5f1bc', '#f4f5c0', '#fef396', '#fec44f', '#fdb347',
                '#fca23f', '#fb9137', '#fa802f', '#f96f27', '#f85e1f',
                '#f74d17'
            ]
            # Create high-resolution colormap
            cmap = LinearSegmentedColormap.from_list('sst_detailed', colors, N=1024)
            
            # Calculate dynamic range
            vmin = max(40, float(expanded_data.min()))
            vmax = min(88, float(expanded_data.max()))
            
            # Plot with enhanced detail (removed gaussian_filter)
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
