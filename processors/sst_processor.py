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
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

class SSTProcessor(BaseImageProcessor):
    def generate_image(self, data: xr.DataArray | xr.Dataset, region: str, dataset: str, date: str) -> Tuple[Path, Optional[Dict]]:
        """Generate SST visualization."""
        try:
            # Get paths and create directories
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            asset_paths.image.parent.mkdir(parents=True, exist_ok=True)

            # Handle Dataset vs DataArray
            if isinstance(data, xr.Dataset):
                variables = SOURCES[dataset]['variables']
                sst_var = next(var for var in variables if 'sst' in var.lower() or 'temperature' in var.lower())
                data = data[sst_var]

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
            
            # Convert temperature using source unit from settings
            source_unit = SOURCES[dataset].get('source_unit', 'C')  # Default to Celsius if not specified
            regional_data = convert_temperature_to_f(regional_data, source_unit=source_unit)
            expanded_data = self.expand_coastal_data(regional_data)
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
            
            # Create colormap from color scale
            cmap = LinearSegmentedColormap.from_list('sst_detailed', SOURCES[dataset]['color_scale'], N=1024)
            
            # Calculate dynamic ranges from valid data
            valid_data = expanded_data.values[~np.isnan(expanded_data.values)]
            vmin = float(np.percentile(valid_data, 1))  # 1st percentile
            vmax = float(np.percentile(valid_data, 99))  # 99th percentile

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
            
            image_path = self.save_image(fig, region, dataset, date)
            return image_path, None
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
