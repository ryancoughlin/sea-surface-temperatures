from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.settings import OUTPUT_DIR
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f, interpolate_data
import cartopy.crs as ccrs
import cartopy.feature as cfeature

logger = logging.getLogger(__name__)

class SSTProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Generate SST image for a specific region."""
        try:
            # Load data
            logger.info(f"Processing SST data for {region}")
            ds = xr.open_dataset(data_path)
            
            # Get variable name from settings
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Select first time slice if time dimension exists
            if 'time' in data.dims:
                logger.debug("Selecting first time slice from 3D data")
                data = data.isel(time=0)
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Get region bounds
            bounds = REGIONS[region]['bounds']
            
            # Mask to region using detected coordinate names
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Convert temperature to Fahrenheit
            regional_data = convert_temperature_to_f(regional_data)
            
            # Create masked figure and axes
            fig, ax = self.create_masked_axes(region)
            
            # Plot data
            contour = ax.contourf(
                regional_data[lon_name],
                regional_data[lat_name],
                regional_data,
                levels=70,
                cmap=SOURCES[dataset]['color_scale'],
                extend='both',
                vmin=32,
                vmax=88,
                transform=ccrs.PlateCarree()
            )
            
            return self.save_image(fig, region, dataset, timestamp)
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
