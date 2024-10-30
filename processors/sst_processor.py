from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
from .base_processor import BaseImageProcessor
from config.settings import SOURCES, IMAGE_SETTINGS
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f
from utils.path_manager import PathManager
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import json
from datetime import datetime
from typing import Dict

logger = logging.getLogger(__name__)

class SSTProcessor(BaseImageProcessor):
    def __init__(self, path_manager: PathManager):
        super().__init__(path_manager)

    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Path:
        """Generate SST image and contours."""
        try:
            # Replace current path construction with PathManager
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            
            # Use asset_paths.image instead of constructing path
            image_path = asset_paths.image
            image_path.parent.mkdir(parents=True, exist_ok=True)
            
            contour_path = asset_paths.contours if SOURCES[dataset]['type'] == 'sst' else None

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
            
            # Plot filled contours
            filled_contours = ax.contourf(
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
            
            # Save using base class method that properly handles zero padding
            return self.save_image(fig, region, dataset, date)
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
